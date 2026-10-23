// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Rewinding storage to an explicit snapshot.
//!
//! A snapshot manifest records the MVCC high-water mark, which is all a
//! time-travel read needs: it filters rows to `_version <= hwm`. A restore
//! cannot work that way. Flushed writes after the snapshot changed stored rows
//! in place (a vertex delete flips `_deleted` and raises `_version` on the row
//! the snapshot read), and they created tables and index datasets the snapshot
//! never had. So an explicit snapshot records a [`RestorePoint`]: the storage
//! version of every table and index dataset, each pinned by a tag so that
//! version cleanup keeps it. A restore rolls every table back to that version
//! and empties the ones created since.
//!
//! The catalog is rewound too: the restore point records the whole catalog
//! (labels, edge types, properties, indexes, constraints, the schemaless
//! edge-type registry) and the restore reinstates it. Storage goes first, as
//! for any catalog change: every table is back at its snapshot version, or
//! dropped when the snapshot did not have it, before the snapshot's catalog
//! is installed, and each property table is then widened to what that catalog
//! declares. A catalog column storage lacks fails every read, while a storage
//! column the catalog does not declare is harmless; dropping the tables
//! created since also removes columns whose type a later declaration changed.
//! Renames are catalog-only, so rewinding the catalog undoes them.
//!
//! A restore point recorded before the catalog was has no catalog, and such a
//! snapshot is refused by [`crate::Writer::restore_snapshot`]. Only an
//! interrupted restore of one, found at open, is completed the old way: data
//! only, the current catalog kept and storage widened to it.
//!
//! In-memory state (L0, adjacency caches, the VID-to-labels index) is the
//! writer's concern; see [`crate::Writer::restore_snapshot`].

// Rust guideline compliant

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use tracing::{info, warn};
use uni_common::core::schema::{IndexDefinition, SchemaElementState};

use crate::snapshot::manager::{RestoreIntent, RestorePoint};
use crate::storage::manager::StorageManager;
use uni_common::core::snapshot::SnapshotManifest;

/// The tag pinning the versions a [`RestorePoint`] records.
#[must_use]
pub fn restore_point_tag(snapshot_id: &str) -> String {
    format!("uni_snapshot_{snapshot_id}")
}

/// Record the storage version of every table and index dataset, pinned so
/// cleanup keeps it, together with the catalog.
///
/// The caller must hold off writers, compaction and index builds
/// ([`StorageManager::pause_index_builds`]), so that the versions read here
/// are the ones the snapshot's flush produced and no build changes an
/// artifact or its catalog record in between. The catalog is read first: DDL
/// widens storage before it saves the catalog, so every column the catalog
/// read here declares is already in the tables read after it.
///
/// # Errors
///
/// When a table version cannot be read or pinned.
pub async fn capture_restore_point(
    storage: &StorageManager,
    snapshot_id: &str,
) -> Result<RestorePoint> {
    let catalog = (*storage.schema_manager().schema()).clone();
    let backend = storage.backend();
    let tag = restore_point_tag(snapshot_id);
    let mut tables = BTreeMap::new();
    for table in backend.table_names().await? {
        let Some(version) = backend.get_table_version(&table).await? else {
            continue;
        };
        backend
            .tag_table_version(&table, &tag, version)
            .await
            .with_context(|| format!("pin table '{table}' for snapshot {snapshot_id}"))?;
        tables.insert(table, version);
    }
    let index_datasets = index_datasets::capture(storage, &tag).await?;
    Ok(RestorePoint {
        snapshot_id: snapshot_id.to_string(),
        tag,
        tables,
        index_datasets,
        indexes: catalog.indexes.clone(),
        catalog: Some(catalog),
    })
}

/// Remove the pins a [`RestorePoint`] holds, so version cleanup may reclaim
/// the versions only the snapshot kept.
///
/// Idempotent: a tag, table or index dataset already gone is skipped.
///
/// # Errors
///
/// When a tag that exists cannot be removed. Every pin is attempted before
/// the first failure is returned.
pub async fn release_restore_point(storage: &StorageManager, point: &RestorePoint) -> Result<()> {
    let backend = storage.backend();
    let mut failures = Vec::new();
    for table in point.tables.keys() {
        if let Err(e) = backend.delete_table_tag(table, &point.tag).await {
            failures.push(format!("{table}: {e:#}"));
        }
    }
    if let Err(e) = index_datasets::release(storage, point).await {
        failures.push(format!("index datasets: {e:#}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "snapshot {}: {} pin(s) could not be removed: {}",
            point.snapshot_id,
            failures.len(),
            failures.join("; ")
        ))
    }
}

/// The labels and edge types whose indexes must be rebuilt after rewinding to
/// `point`, given the catalog's current definitions.
///
/// An index keeps a matching artifact through the rewind only if the snapshot
/// had it with the same configuration, built at the same time: the rewind
/// restores the artifact as it was then. Anything declared, rebuilt or first
/// built after the snapshot gets an artifact the rewind removed or replaced.
#[must_use]
pub fn indexes_needing_rebuild(current: &[IndexDefinition], point: &RestorePoint) -> Vec<String> {
    let mut out = BTreeSet::new();
    for def in current {
        let kept = point.indexes.iter().any(|old| {
            old.name() == def.name()
                && old.same_config_as(def)
                && old.metadata().last_built_at == def.metadata().last_built_at
        });
        if !kept {
            out.insert(def.label().to_string());
        }
    }
    out.into_iter().collect()
}

/// Rewind every table, index dataset and the catalog to `point`.
///
/// Idempotent, so an interrupted restore converges when it is run again:
/// a table the snapshot had is rolled back to its recorded version, and one
/// created since is dropped, as the snapshot did not have it (the restore
/// refuses while forks exist, so no fork branch lives inside one). Then the
/// snapshot's catalog is installed, its edge-endpoint
/// verifications recorded after `snapshot_created_at` withdrawn, and each
/// property table widened to the properties it declares. For a legacy point
/// without a catalog the current catalog is kept and storage widened to it.
///
/// A rewound table can still hold rows the snapshot does not: a bulk load
/// publishes table by table without the flush lock, so the versions recorded
/// can include rows above the snapshot's ceiling, or rows of a load the
/// snapshot's manifest lists as unpublished. Those rows are deleted
/// (`drop_rows_outside_the_snapshot`).
///
/// The caller must hold off writers, compaction and index builds.
///
/// # Errors
///
/// When a table cannot be rolled back or dropped, widening fails, or the
/// catalog cannot be saved. Every table is attempted before the first
/// failure is returned, and the catalog is installed only once every table
/// is rewound.
pub async fn rewind_storage(
    storage: &StorageManager,
    point: &RestorePoint,
    snapshot: &SnapshotManifest,
) -> Result<()> {
    let snapshot_created_at = snapshot.created_at;
    let backend = storage.backend();
    let mut failures = Vec::new();
    let current = backend.table_names().await?;
    for table in &current {
        let result = match point.tables.get(table) {
            Some(&version) => match backend.get_table_version(table).await {
                Ok(Some(now)) if now == version => Ok(()),
                Ok(_) => backend.rollback_table(table, version).await,
                Err(e) => Err(e),
            },
            None => backend.drop_table(table).await,
        };
        if let Err(e) = result {
            failures.push(format!("{table}: {e:#}"));
        }
        // Test-only seam (no-op without the `failpoints` feature): a crash
        // with some tables rewound and others not.
        fail::fail_point!("restore::after-table");
    }
    for table in point.tables.keys() {
        if !current.contains(table) {
            // Dropped after the snapshot, which DDL never does; nothing is
            // left to roll back, so the restored label reads as empty.
            warn!(
                table = %table,
                snapshot = %point.snapshot_id,
                "restore: table recorded by the snapshot no longer exists; not recreated"
            );
        }
    }
    if failures.is_empty()
        && let Err(e) = drop_rows_outside_the_snapshot(storage, point, snapshot).await
    {
        failures.push(format!("rows outside the snapshot: {e:#}"));
    }
    if let Err(e) = index_datasets::rewind(storage, point).await {
        failures.push(format!("index datasets: {e:#}"));
    }
    backend.clear_cache();
    if !failures.is_empty() {
        return Err(anyhow!(
            "restore of snapshot {} could not rewind {} table(s): {}",
            point.snapshot_id,
            failures.len(),
            failures.join("; ")
        ));
    }
    match &point.catalog {
        Some(catalog) => install_catalog(storage, catalog, snapshot_created_at).await,
        None => rewiden_declared_properties(storage).await,
    }
}

/// Deletes, from every rewound table that carries `_version`, the rows a
/// reader pinned to `snapshot` never sees: above its ceiling, or of a bulk
/// load its manifest lists as unpublished.
///
/// # Errors
///
/// When a table's schema cannot be read or the delete fails.
async fn drop_rows_outside_the_snapshot(
    storage: &StorageManager,
    point: &RestorePoint,
    snapshot: &SnapshotManifest,
) -> Result<()> {
    use crate::backend::types::{CmpOp, FilterExpr, Scalar, cols};
    let mut outside = vec![FilterExpr::compare(
        cols::VERSION,
        CmpOp::Gt,
        Scalar::UInt(snapshot.version_high_water_mark),
    )];
    if !snapshot.unpublished_bulk_versions.is_empty() {
        outside.push(FilterExpr::one_of(
            cols::VERSION,
            snapshot
                .unpublished_bulk_versions
                .iter()
                .map(|v| Scalar::UInt(*v)),
        ));
    }
    let outside = FilterExpr::any_of(outside);
    let backend = storage.backend();
    for table in point.tables.keys() {
        let Some(schema) = backend.get_table_schema(table).await? else {
            continue;
        };
        if schema.field_with_name(cols::VERSION).is_err() {
            continue;
        }
        if backend.count_rows(table, Some(&outside)).await? == 0 {
            continue;
        }
        warn!(
            table = %table,
            snapshot = %point.snapshot_id,
            "restore: deleting rows the snapshot does not contain"
        );
        backend
            .delete_rows(table, &outside)
            .await
            .with_context(|| format!("delete rows outside snapshot from '{table}'"))?;
    }
    Ok(())
}

/// Reinstate `catalog` as the store's catalog, after storage was rewound.
///
/// The installed catalog takes a `schema_version` above both the live one
/// and the snapshot's, so every plan cached against either is discarded.
/// Verifications recorded after `snapshot_created_at` are withdrawn, since
/// they checked tables the rewind replaced. Storage is widened to the
/// catalog's properties before the catalog is saved and synced, so the
/// persisted catalog never declares a column storage lacks.
///
/// # Errors
///
/// When widening fails or the catalog cannot be saved or synced.
async fn install_catalog(
    storage: &StorageManager,
    catalog: &uni_common::core::schema::Schema,
    snapshot_created_at: DateTime<Utc>,
) -> Result<()> {
    let sm = storage.schema_manager();
    let live = sm.schema();
    let mut restored = catalog.clone();
    restored.schema_version = live
        .schema_version
        .max(catalog.schema_version)
        .wrapping_add(1);
    // What the running binary and this open recorded, not catalog shape.
    restored.writer_features = live.writer_features.clone();
    restored.trusted_unmarked_manifest = live.trusted_unmarked_manifest.clone();
    sm.replace_schema(restored);
    sm.distrust_verifications_after(snapshot_created_at)?;
    rewiden_declared_properties(storage).await?;
    sm.save().await?;
    sync_catalog_file(storage).await
}

/// Sync the catalog file on a local store, so a restore whose intent is
/// cleared afterwards cannot reopen to the pre-restore catalog over rewound
/// tables.
async fn sync_catalog_file(storage: &StorageManager) -> Result<()> {
    let Some(file) = storage.schema_manager().local_file().map(Path::to_path_buf) else {
        return Ok(());
    };
    tokio::task::spawn_blocking(move || crate::runtime::wal::sync_file_and_parent(&file))
        .await
        .map_err(|e| anyhow!("sync catalog after restore: task failed: {e}"))?
        .context("sync catalog after restore")
}

/// Give every property table a column for each declared property it lacks.
///
/// A table rolled back to before a property was declared lacks its column,
/// and a catalog column storage lacks fails every read. Widening also promotes
/// values held as schemaless data, as the declaration did.
async fn rewiden_declared_properties(storage: &StorageManager) -> Result<()> {
    let schema = storage.schema_manager().schema();
    let backend = storage.backend();
    for (entity, props) in &schema.properties {
        let declared: Vec<(String, uni_common::DataType)> = props
            .iter()
            .filter(|(_, meta)| matches!(meta.state, SchemaElementState::Active))
            .map(|(name, meta)| (name.clone(), meta.r#type.clone()))
            .collect();
        let mut missing: Vec<(String, uni_common::DataType)> = Vec::new();
        for table in storage.property_tables_for(entity) {
            let Some(stored) = backend.get_table_schema(&table).await? else {
                continue;
            };
            for (name, ty) in &declared {
                if stored.field_with_name(name).is_err() && !missing.iter().any(|(n, _)| n == name)
                {
                    missing.push((name.clone(), ty.clone()));
                }
            }
        }
        if !missing.is_empty() {
            storage
                .widen_for_declared_properties(entity, &missing)
                .await
                .with_context(|| format!("widen '{entity}' after restore"))?;
        }
    }
    Ok(())
}

/// Complete a restore a crash interrupted, before anything reads the store.
///
/// Returns `true` when there was one. Rewinds storage again (idempotent),
/// publishes the manifest the restore prepared, rebuilds the indexes it
/// listed, and clears the intent. Reloads the VID-to-labels index, which the
/// manager read before the rewind.
///
/// # Errors
///
/// When the intent names a snapshot with no restore point, or any step fails;
/// the intent is then kept, so the next open retries.
pub async fn recover_interrupted_restore(storage: &StorageManager) -> Result<bool> {
    let sm = storage.snapshot_manager();
    let Some(intent) = sm.load_restore_intent().await? else {
        return Ok(false);
    };
    let point = sm
        .load_restore_point(&intent.snapshot_id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "interrupted restore of snapshot {} cannot be completed: its restore point is \
                 missing",
                intent.snapshot_id
            )
        })?;
    warn!(
        snapshot = %intent.snapshot_id,
        "completing a snapshot restore interrupted by a crash"
    );
    // Background compaction is already running at this point of the open.
    let _paused = storage.pause_compaction().await?;
    let snapshot = sm
        .load_snapshot(&intent.snapshot_id)
        .await
        .with_context(|| {
            format!(
                "interrupted restore of snapshot {} cannot be completed: its manifest is \
                 unreadable",
                intent.snapshot_id
            )
        })?;
    rewind_storage(storage, &point, &snapshot).await?;
    sm.set_latest_snapshot(&intent.manifest_id).await?;
    finish(storage, &intent).await?;
    storage.reload_vid_labels_index().await?;
    Ok(true)
}

/// The steps after the manifest is published: rebuild the listed indexes, then
/// clear the intent.
///
/// # Errors
///
/// When an index rebuild fails or the intent cannot be cleared.
pub async fn finish(storage: &StorageManager, intent: &RestoreIntent) -> Result<()> {
    rebuild_indexes(storage, &intent.rebuild_indexes_for).await?;
    storage.snapshot_manager().clear_restore_intent().await?;
    info!(snapshot = %intent.snapshot_id, "snapshot restore complete");
    Ok(())
}

#[cfg(feature = "lance-backend")]
async fn rebuild_indexes(storage: &StorageManager, labels: &[String]) -> Result<()> {
    if labels.is_empty() {
        return Ok(());
    }
    let manager = crate::storage::index_manager::IndexManager::new(
        storage.base_path(),
        storage.schema_manager_arc(),
    )
    .with_backend(storage.backend_arc());
    for label in labels {
        manager
            .rebuild_indexes_for_label(label)
            .await
            .with_context(|| format!("rebuild indexes of '{label}' after restore"))?;
    }
    Ok(())
}

#[cfg(not(feature = "lance-backend"))]
async fn rebuild_indexes(_storage: &StorageManager, labels: &[String]) -> Result<()> {
    if labels.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "indexes of {labels:?} must be rebuilt after restore, which needs the Lance backend"
        ))
    }
}

/// The Lance datasets under `indexes/` that hold index content outside the
/// tables themselves (UID map, inverted, sparse and JSON indexes).
#[cfg(feature = "lance-backend")]
mod index_datasets {
    use std::collections::BTreeMap;

    use anyhow::{Context, Result};
    use futures::TryStreamExt;
    use lance::dataset::builder::DatasetBuilder;
    use object_store::ObjectStoreExt;
    use object_store::path::Path as ObjectStorePath;

    use crate::snapshot::manager::RestorePoint;
    use crate::storage::manager::StorageManager;
    use crate::store_utils::{DEFAULT_TIMEOUT, list_with_timeout};

    /// Every index dataset, as a path relative to the store root.
    async fn list(storage: &StorageManager) -> Result<Vec<String>> {
        let store = storage.store();
        let prefix = ObjectStorePath::from("indexes");
        let metas = list_with_timeout(&store, Some(&prefix), DEFAULT_TIMEOUT).await?;
        let mut out: Vec<String> = metas
            .iter()
            .filter_map(|m| {
                let path = m.location.as_ref();
                let (root, rest) = path.split_once("/_versions/")?;
                // A branch of a dataset lives in its `tree/` directory and is
                // not a dataset of its own.
                (rest.ends_with(".manifest") && !root.contains("/tree/")).then(|| root.to_string())
            })
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    fn uri(storage: &StorageManager, rel: &str) -> String {
        format!("{}/{rel}", storage.base_path().trim_end_matches('/'))
    }

    /// Record and pin the current version of every index dataset.
    pub(super) async fn capture(
        storage: &StorageManager,
        tag: &str,
    ) -> Result<BTreeMap<String, u64>> {
        let mut out = BTreeMap::new();
        for rel in list(storage).await? {
            let uri = uri(storage, &rel);
            let dataset = lance::Dataset::open(&uri)
                .await
                .with_context(|| format!("open index dataset {uri}"))?;
            let version = dataset.version().version;
            dataset
                .tags()
                .create(tag, version)
                .await
                .with_context(|| format!("pin index dataset {uri} v{version}"))?;
            out.insert(rel, version);
        }
        Ok(out)
    }

    /// Remove the restore point's tag from each index dataset it pinned.
    pub(super) async fn release(storage: &StorageManager, point: &RestorePoint) -> Result<()> {
        let present = list(storage).await?;
        for rel in point.index_datasets.keys() {
            if !present.contains(rel) {
                continue;
            }
            let uri = uri(storage, rel);
            let dataset = lance::Dataset::open(&uri)
                .await
                .with_context(|| format!("open index dataset {uri}"))?;
            match dataset.tags().delete(&point.tag).await {
                Ok(()) | Err(lance::Error::RefNotFound { .. }) => {}
                Err(e) => {
                    return Err(anyhow::Error::new(e))
                        .with_context(|| format!("unpin index dataset {uri}"));
                }
            }
        }
        Ok(())
    }

    /// Roll each recorded index dataset back, and delete those created since.
    pub(super) async fn rewind(storage: &StorageManager, point: &RestorePoint) -> Result<()> {
        for rel in list(storage).await? {
            let uri = uri(storage, &rel);
            match point.index_datasets.get(&rel) {
                Some(&version) => {
                    let current = lance::Dataset::open(&uri)
                        .await
                        .with_context(|| format!("open index dataset {uri}"))?;
                    if current.version().version == version {
                        continue;
                    }
                    let mut at = DatasetBuilder::from_uri(&uri)
                        .with_version(version)
                        .load()
                        .await
                        .with_context(|| format!("open index dataset {uri} at v{version}"))?;
                    at.restore()
                        .await
                        .with_context(|| format!("restore index dataset {uri} to v{version}"))?;
                }
                None => {
                    // Created after the snapshot, so it indexes only rows the
                    // rewind removed. Whatever index it belongs to is rebuilt
                    // or recreated on the next write.
                    let store = storage.store();
                    let prefix = ObjectStorePath::from(format!("{rel}/"));
                    let paths: Vec<ObjectStorePath> = store
                        .list(Some(&prefix))
                        .map_ok(|m| m.location)
                        .try_collect()
                        .await
                        .with_context(|| format!("list index dataset {rel}"))?;
                    for path in paths {
                        store
                            .delete(&path)
                            .await
                            .with_context(|| format!("delete {path}"))?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Without the Lance backend there are no index datasets.
#[cfg(not(feature = "lance-backend"))]
mod index_datasets {
    use std::collections::BTreeMap;

    use anyhow::Result;

    use crate::snapshot::manager::RestorePoint;
    use crate::storage::manager::StorageManager;

    pub(super) async fn capture(
        _storage: &StorageManager,
        _tag: &str,
    ) -> Result<BTreeMap<String, u64>> {
        Ok(BTreeMap::new())
    }

    pub(super) async fn rewind(_storage: &StorageManager, _point: &RestorePoint) -> Result<()> {
        Ok(())
    }

    pub(super) async fn release(_storage: &StorageManager, _point: &RestorePoint) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_common::core::schema::{IndexMetadata, ScalarIndexConfig, ScalarIndexType};

    fn scalar(name: &str, label: &str, built: Option<i64>) -> IndexDefinition {
        IndexDefinition::Scalar(ScalarIndexConfig {
            name: name.into(),
            label: label.into(),
            properties: vec!["p".into()],
            index_type: ScalarIndexType::BTree,
            where_clause: None,
            metadata: IndexMetadata {
                last_built_at: built.map(|s| chrono::DateTime::from_timestamp(s, 0).unwrap()),
                ..IndexMetadata::default()
            },
        })
    }

    fn point(indexes: Vec<IndexDefinition>) -> RestorePoint {
        RestorePoint {
            snapshot_id: "s".into(),
            tag: restore_point_tag("s"),
            tables: BTreeMap::new(),
            index_datasets: BTreeMap::new(),
            indexes,
            catalog: None,
        }
    }

    #[test]
    fn an_index_unchanged_since_the_snapshot_is_kept() {
        let p = point(vec![scalar("i", "A", Some(10))]);
        assert!(indexes_needing_rebuild(&[scalar("i", "A", Some(10))], &p).is_empty());
    }

    #[test]
    fn an_index_declared_or_rebuilt_after_the_snapshot_is_rebuilt() {
        let p = point(vec![scalar("i", "A", Some(10))]);
        let current = [
            scalar("i", "A", Some(20)),
            scalar("j", "B", None),
            scalar("k", "B", Some(5)),
        ];
        assert_eq!(indexes_needing_rebuild(&current, &p), vec!["A", "B"]);
    }
}
