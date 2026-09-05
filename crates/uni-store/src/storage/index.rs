// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::types::{FilterExpr, Scalar};
use anyhow::{Result, anyhow};
use arrow_array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteMode};
use lance::index::DatasetIndexExt;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::{UniId, Vid};

/// Convert a UniId to hex string for filter pushdown.
fn uid_to_hex(uid: &UniId) -> String {
    uid.as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

pub struct UidIndex {
    uri: String,
}

impl UidIndex {
    pub fn new(base_uri: &str, label: &str) -> Self {
        let uri = format!("{}/indexes/uni_id_to_vid/{}/index.lance", base_uri, label);
        Self { uri }
    }

    /// Open the backing dataset.
    ///
    /// Deliberately **private**. This type is raw-Lance inside, which is fine —
    /// so is `backend::lance`. What is not fine is handing an
    /// `Arc<lance::Dataset>` across a crate boundary: that was the last such
    /// leak outside `uni-store`, and it made every consumer reimplement the
    /// index's own scan. Consumers get domain methods
    /// ([`Self::resolve_uids`], [`Self::resolve_all_vids`]) instead.
    async fn open(&self) -> Result<Arc<Dataset>> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(Arc::new(ds))
    }

    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_uid", ArrowDataType::FixedSizeBinary(32), false),
            Field::new("_vid", ArrowDataType::UInt64, false),
            Field::new("_uid_hex", ArrowDataType::Utf8, false), // hex-encoded _uid for filtering
            // MVCC version of the mapping (review C3). A UID can have multiple
            // rows after delete+reinsert (a new vid at a higher version);
            // lookups take the highest `_version` so resolution is deterministic
            // instead of the old non-deterministic `limit(1)` / last-row-wins.
            Field::new("_version", ArrowDataType::UInt64, false),
        ]))
    }

    /// Append UID→vid mappings stamped with `version` (review C3).
    ///
    /// `version` should be the flush's MVCC version so that, across flushes, a
    /// re-created vertex (new vid, same content-derived UID) writes a row that
    /// deterministically outranks the stale mapping — [`Self::get_vid`] /
    /// [`Self::resolve_uids`] take the highest `_version`.
    ///
    /// Note on deletes: a vertex deleted without re-creation leaves its old
    /// mapping in the index (the flush delete path carries only `(vid, version)`,
    /// not the props/ext_id needed to recompute the UID, so it cannot write a
    /// UID-keyed tombstone). This is sound because the index is NOT the
    /// authoritative liveness gate — every consumer re-verifies the resolved vid
    /// against live storage (MERGE scans the vertex table with a tombstone-aware
    /// two-pass check; fork-promote re-validates via a Cypher liveness MATCH).
    pub async fn write_mapping_versioned(
        &self,
        mappings: &[(UniId, Vid)],
        version: u64,
    ) -> Result<()> {
        // Upgrade a pre-`_version` table in place so the append schema matches.
        self.migrate_schema_if_legacy().await?;

        let schema = Self::get_arrow_schema();

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        let mut vids = Vec::with_capacity(mappings.len());
        let mut uid_hex_builder = StringBuilder::new();

        for (uid, vid) in mappings {
            uid_builder.append_value(uid.as_bytes()).unwrap();
            vids.push(vid.as_u64());
            uid_hex_builder.append_value(uid_to_hex(uid));
        }

        let uid_array = uid_builder.finish();
        let vid_array = UInt64Array::from(vids);
        let uid_hex_array = uid_hex_builder.finish();
        let version_array = UInt64Array::from(vec![version; mappings.len()]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(uid_array),
                Arc::new(vid_array),
                Arc::new(uid_hex_array),
                Arc::new(version_array),
            ],
        )?;

        // Retried: a losing Lance commit is retryable.
        crate::storage::manager::write_dataset_with_lance_conflict_retry(
            &self.uri,
            schema,
            vec![batch],
            WriteMode::Append,
        )
        .await?;
        self.ensure_uid_hex_index().await?;
        Ok(())
    }

    /// Back-compat shim for ad-hoc single inserts that don't carry a flush
    /// version. Such inserts write one mapping per UID and so are not subject to
    /// the multi-row-per-UID determinism problem; version 0 is sufficient (and
    /// is outranked by any real flush write of the same UID).
    pub async fn write_mapping(&self, mappings: &[(UniId, Vid)]) -> Result<()> {
        self.write_mapping_versioned(mappings, 0).await
    }

    /// Upgrade a legacy (pre-`_version`) index table — the original
    /// `_uid`/`_vid`/`_uid_hex` schema — to include `_version`, defaulting
    /// existing rows to `0`. One-time and cheap (one small table per label).
    /// A no-op when the table is absent (a fresh write creates the new schema)
    /// or already migrated. The BTree `_uid_hex` index is recreated by the
    /// `ensure_uid_hex_index` call that follows the append in the write path.
    async fn migrate_schema_if_legacy(&self) -> Result<()> {
        let ds = match Dataset::open(&self.uri).await {
            Ok(ds) => ds,
            Err(_) => return Ok(()), // doesn't exist yet — created fresh with the new schema
        };
        if ds.schema().field("_version").is_some() {
            return Ok(()); // already migrated
        }

        // Read every legacy row and re-emit it under the new schema with
        // `_version = 0`, then overwrite the table.
        let mut stream = ds.scan().try_into_stream().await?;
        let new_schema = Self::get_arrow_schema();
        let mut migrated = Vec::new();
        while let Some(b) = stream.try_next().await? {
            let n = b.num_rows();
            let uid = b
                .column_by_name("_uid")
                .ok_or(anyhow!("legacy uid index missing _uid"))?
                .clone();
            let vid = b
                .column_by_name("_vid")
                .ok_or(anyhow!("legacy uid index missing _vid"))?
                .clone();
            let hex = b
                .column_by_name("_uid_hex")
                .ok_or(anyhow!("legacy uid index missing _uid_hex"))?
                .clone();
            let versions = Arc::new(UInt64Array::from(vec![0u64; n]));
            migrated.push(RecordBatch::try_new(
                new_schema.clone(),
                vec![uid, vid, hex, versions],
            )?);
        }

        // Retried: a losing Lance commit is retryable.
        crate::storage::manager::write_dataset_with_lance_conflict_retry(
            &self.uri,
            new_schema,
            migrated,
            WriteMode::Overwrite,
        )
        .await?;
        Ok(())
    }

    /// Create a BTree scalar index on `_uid_hex` for O(log N) lookups.
    ///
    /// Fail-open by design: without the index a UID lookup falls back to a scan,
    /// which is slower but not wrong, and this runs on the write path — refusing
    /// the write would be the worse failure. What changed is that both ways it
    /// can fail are now recorded (#233, Tier 2). The build discarded its error
    /// with `.ok()` and no log whatsoever, so a store could carry no index with
    /// nothing anywhere saying so.
    ///
    /// The open is also no longer read as "absent". Only a genuinely missing
    /// dataset means "no UIDs registered yet"; permissions, a corrupt manifest
    /// or IO used to reach the same `Ok(())`, which is the fail-open shape this
    /// class is about — the same distinction `get_vid` below already draws.
    pub async fn ensure_uid_hex_index(&self) -> Result<()> {
        let mut ds = match Dataset::open(&self.uri).await {
            Ok(ds) => ds,
            Err(e) => {
                let err = anyhow::Error::from(e);
                if !crate::store_utils::is_dataset_not_found(&err) {
                    crate::storage::record_default_index_failure(&self.uri, "_uid_hex", &err);
                }
                // Nothing is written yet, so there is nothing to index. The
                // append path calls this again after it writes.
                return Ok(());
            }
        };

        if let Err(e) = ds
            .create_index(
                &["_uid_hex"],
                lance_index::IndexType::Scalar,
                Some("idx_uid_hex".to_string()),
                &lance_index::scalar::ScalarIndexParams::default(),
                true, // replace if exists
            )
            .await
        {
            crate::storage::record_default_index_failure(&self.uri, "_uid_hex", &e);
        }

        Ok(())
    }

    pub async fn get_vid(&self, uid: &UniId) -> Result<Option<Vid>> {
        // Only an absent dataset means "no UID registered". Any other failure —
        // permissions, a corrupt manifest, IO — used to read the same way, so a
        // broken index was indistinguishable from an empty one and MERGE went on
        // to create a duplicate (#233).
        let ds = match self.open().await {
            Ok(ds) => ds,
            Err(e) if crate::store_utils::is_dataset_not_found(&e) => return Ok(None),
            Err(e) => return Err(e),
        };

        // Use filter pushdown on _uid_hex for O(log N) or better lookup
        let hex = uid_to_hex(uid);
        let filter = FilterExpr::equals("_uid_hex", Scalar::Str(hex)).to_sql()?;

        // Deterministic resolution (review C3): a UID may have multiple rows
        // after delete+reinsert. Take the highest `_version` instead of the old
        // non-deterministic `limit(1)`. `_version` is absent on a not-yet-
        // migrated legacy table, in which case every row is treated as version 0.
        let has_version = ds.schema().field("_version").is_some();
        let project: &[&str] = if has_version {
            &["_vid", "_version"]
        } else {
            &["_vid"]
        };
        let mut scanner = ds.scan();
        scanner.filter(&filter)?;
        scanner.project(project)?;
        let mut stream = scanner.try_into_stream().await?;

        let mut best_vid: Option<Vid> = None;
        let mut best_version: u64 = 0;

        while let Some(batch) = stream.try_next().await? {
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid column type"))?;
            let ver_col = batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            for i in 0..batch.num_rows() {
                let v = ver_col.map_or(0, |c| if c.is_null(i) { 0 } else { c.value(i) });
                if best_vid.is_none() || v >= best_version {
                    best_version = v;
                    best_vid = Some(Vid::from(vid_col.value(i)));
                }
            }
        }

        Ok(best_vid)
    }

    /// Every `(uid, vid, version)` row registered for any of `uids`, in scan
    /// order.
    ///
    /// The single place this index's `_uid_hex IN (…)` scan is expressed. Both
    /// public resolvers fold this; `uni-fork` used to hand-roll a second copy
    /// against a borrowed `Arc<Dataset>`, complete with its own hex encoder and
    /// its own literal quoting.
    ///
    /// `_version` is absent on a not-yet-migrated legacy table, in which case
    /// every row is reported as version 0.
    async fn scan_uid_rows(ds: &Dataset, uids: &[UniId]) -> Result<Vec<(UniId, Vid, u64)>> {
        let filter =
            FilterExpr::one_of("_uid_hex", uids.iter().map(|u| Scalar::Str(uid_to_hex(u))))
                .to_sql()?;

        let has_version = ds.schema().field("_version").is_some();
        let project: &[&str] = if has_version {
            &["_uid_hex", "_vid", "_version"]
        } else {
            &["_uid_hex", "_vid"]
        };
        let mut scanner = ds.scan();
        scanner.filter(&filter)?;
        scanner.project(project)?;
        let mut stream = scanner.try_into_stream().await?;

        // Reverse map hex -> UniId so each row resolves without re-hashing.
        let hex_to_uid: HashMap<String, UniId> =
            uids.iter().map(|uid| (uid_to_hex(uid), *uid)).collect();

        let mut rows = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let uid_hex_col = batch
                .column_by_name("_uid_hex")
                .ok_or(anyhow!("Missing _uid_hex column"))?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or(anyhow!("Invalid _uid_hex column type"))?;
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid column type"))?;
            let ver_col = batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            for i in 0..batch.num_rows() {
                if uid_hex_col.is_null(i) {
                    continue;
                }
                if let Some(&uid) = hex_to_uid.get(uid_hex_col.value(i)) {
                    let version = ver_col.map_or(0, |c| if c.is_null(i) { 0 } else { c.value(i) });
                    rows.push((uid, Vid::from(vid_col.value(i)), version));
                }
            }
        }
        Ok(rows)
    }

    /// The single best vid per UID: highest `_version` wins.
    ///
    /// A missing index dataset resolves to "nothing registered" rather than an
    /// error; a *scan* failure still propagates, so a transient read fault
    /// cannot masquerade as an empty index.
    pub async fn resolve_uids(&self, uids: &[UniId]) -> Result<HashMap<UniId, Vid>> {
        if uids.is_empty() {
            return Ok(HashMap::new());
        }
        let ds = match self.open().await {
            Ok(ds) => ds,
            Err(e) if crate::store_utils::is_dataset_not_found(&e) => {
                return Ok(HashMap::new());
            }
            Err(e) => return Err(e),
        };

        // Deterministic resolution (review C3): keep the highest-`_version` vid
        // per UID rather than the old last-row-wins.
        let mut best: HashMap<UniId, (u64, Vid)> = HashMap::new();
        for (uid, vid, version) in Self::scan_uid_rows(&ds, uids).await? {
            match best.get(&uid) {
                Some(&(bv, _)) if bv >= version => {}
                _ => {
                    best.insert(uid, (version, vid));
                }
            }
        }
        Ok(best.into_iter().map(|(uid, (_, vid))| (uid, vid)).collect())
    }

    /// **Every** vid registered for each UID, not just the winner.
    ///
    /// The index is shared across primary and fork branches and is append-only,
    /// so one UID can carry several vids. [`Self::resolve_uids`] collapses them
    /// to one, which loses the fork-vs-primary disambiguation that fork diff and
    /// promote need — they must test each candidate against primary's view.
    ///
    /// Unlike [`Self::resolve_uids`], a missing index dataset is an error here:
    /// the caller treats it as "cannot answer" and falls back, rather than
    /// silently concluding that no vids are registered.
    pub async fn resolve_all_vids(&self, uids: &[UniId]) -> Result<HashMap<UniId, Vec<Vid>>> {
        if uids.is_empty() {
            return Ok(HashMap::new());
        }
        let ds = self.open().await?;
        let mut out: HashMap<UniId, Vec<Vid>> = HashMap::new();
        for (uid, vid, _) in Self::scan_uid_rows(&ds, uids).await? {
            out.entry(uid).or_default().push(vid);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatchIterator;
    use lance::dataset::WriteParams;
    use tempfile::TempDir;

    fn test_uid(counter: u8) -> UniId {
        let mut bytes = [0u8; 32];
        bytes[0] = counter;
        UniId::from_bytes(bytes)
    }

    /// Deterministic resolution after delete+reinsert: the highest-`_version`
    /// mapping wins regardless of write order (review C3). The old `limit(1)` /
    /// last-row-wins could return the stale vid.
    #[tokio::test]
    async fn test_get_vid_picks_highest_version() {
        let dir = TempDir::new().unwrap();
        let index = UidIndex::new(dir.path().to_str().unwrap(), "Person");

        // Original mapping (low version), then a re-create at a higher version.
        let uid = test_uid(1);
        index
            .write_mapping_versioned(&[(uid, Vid::new(100))], 5)
            .await
            .unwrap();
        index
            .write_mapping_versioned(&[(uid, Vid::new(200))], 9)
            .await
            .unwrap();
        assert_eq!(index.get_vid(&uid).await.unwrap(), Some(Vid::new(200)));

        // Write-order independence: higher version written first, lower second.
        let uid2 = test_uid(2);
        index
            .write_mapping_versioned(&[(uid2, Vid::new(11))], 20)
            .await
            .unwrap();
        index
            .write_mapping_versioned(&[(uid2, Vid::new(10))], 12)
            .await
            .unwrap();
        assert_eq!(index.get_vid(&uid2).await.unwrap(), Some(Vid::new(11)));

        // resolve_uids agrees with get_vid.
        let map = index.resolve_uids(&[uid, uid2]).await.unwrap();
        assert_eq!(map.get(&uid), Some(&Vid::new(200)));
        assert_eq!(map.get(&uid2), Some(&Vid::new(11)));
    }

    /// A pre-`_version` (3-column) index table is read correctly as-is and is
    /// migrated in place on the next write, after which the higher-version
    /// re-create wins (review C3).
    #[tokio::test]
    async fn test_legacy_table_migrates_on_write() {
        let dir = TempDir::new().unwrap();
        let index = UidIndex::new(dir.path().to_str().unwrap(), "Person");

        // Hand-build the OLD 3-column schema and write a legacy row.
        let legacy_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_uid", ArrowDataType::FixedSizeBinary(32), false),
            Field::new("_vid", ArrowDataType::UInt64, false),
            Field::new("_uid_hex", ArrowDataType::Utf8, false),
        ]));
        let uid = test_uid(7);
        let mut uid_b = FixedSizeBinaryBuilder::new(32);
        uid_b.append_value(uid.as_bytes()).unwrap();
        let mut hex_b = StringBuilder::new();
        hex_b.append_value(uid_to_hex(&uid));
        let legacy_batch = RecordBatch::try_new(
            legacy_schema.clone(),
            vec![
                Arc::new(uid_b.finish()),
                Arc::new(UInt64Array::from(vec![100u64])),
                Arc::new(hex_b.finish()),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(std::iter::once(Ok(legacy_batch)), legacy_schema);
        Dataset::write(
            Box::new(reader),
            &index.uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Reads against the legacy (no `_version`) table still work (treated as
        // version 0).
        assert_eq!(index.get_vid(&uid).await.unwrap(), Some(Vid::new(100)));

        // A re-create at a higher version triggers migration and then wins.
        index
            .write_mapping_versioned(&[(uid, Vid::new(200))], 9)
            .await
            .unwrap();
        let ds = Dataset::open(&index.uri).await.unwrap();
        assert!(
            ds.schema().field("_version").is_some(),
            "table should have been migrated to include _version"
        );
        assert_eq!(index.get_vid(&uid).await.unwrap(), Some(Vid::new(200)));
    }
}

#[cfg(test)]
mod uid_hex_index_failure_tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tempfile::TempDir;

    /// Total of `uni_default_index_build_failures_total` recorded while `body` ran.
    fn failures_during(body: impl FnOnce()) -> u64 {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // The global recorder slot is one-shot and another test in this binary
        // may hold it, so record against a local one.
        metrics::with_local_recorder(&recorder, body);
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(ckey, _, _, _)| ckey.key().name() == "uni_default_index_build_failures_total")
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(v) => v,
                _ => 0,
            })
            .sum()
    }

    /// An index that was never written is not a failure.
    ///
    /// This is the arm that must stay quiet: the write path calls this before
    /// anything is on disk, so counting here would report a failure on every
    /// healthy store and make the counter useless.
    #[tokio::test]
    async fn a_missing_dataset_is_not_recorded_as_a_failure() {
        let dir = TempDir::new().unwrap();
        let index = UidIndex::new(dir.path().to_str().unwrap(), "Person");

        let mut result = None;
        let counted = failures_during(|| {
            result = Some(futures::executor::block_on(index.ensure_uid_hex_index()));
        });

        assert!(result.unwrap().is_ok(), "a missing dataset is not an error");
        assert_eq!(counted, 0, "nothing failed, so nothing may be counted");
    }

    /// An open that fails for any *other* reason is recorded.
    ///
    /// Both arms used to reach the same `Ok(())` through `Err(_) => return`, so
    /// a corrupt manifest or a permissions failure was indistinguishable from
    /// "no UIDs registered yet" — the fail-open shape #233 is about. The call
    /// still succeeds, because this runs on the write path and a degraded index
    /// must not become a failed insert; the difference is that it is now
    /// countable.
    #[tokio::test]
    async fn an_unexpected_open_failure_is_recorded() {
        let dir = TempDir::new().unwrap();
        let index = UidIndex::new(dir.path().to_str().unwrap(), "Person");

        // A corrupt manifest, which is one of the cases the old `Err(_)` arm
        // read as "absent". Measured, not assumed: an empty directory and a
        // `_versions` written as a *file* both classify as not-found, so those
        // would not exercise this arm. A junk manifest inside `_versions/`
        // yields `LanceError(IO)`, which does not.
        let uri = std::path::Path::new(dir.path())
            .join("indexes/uni_id_to_vid/Person/index.lance/_versions");
        std::fs::create_dir_all(&uri).unwrap();
        std::fs::write(uri.join("1.manifest"), b"not-a-manifest").unwrap();

        let mut result = None;
        let counted = failures_during(|| {
            result = Some(futures::executor::block_on(index.ensure_uid_hex_index()));
        });

        assert!(
            result.unwrap().is_ok(),
            "the build stays fail-open: a missing index is slower, not wrong"
        );
        assert_eq!(
            counted, 1,
            "a corrupt dataset must be counted, not read as `absent`"
        );
    }
}
