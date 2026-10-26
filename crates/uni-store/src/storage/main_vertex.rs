// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Main vertex table for unified vertex storage.
//!
//! This module implements the main `vertices` table as described in STORAGE_DESIGN.md.
//! The main table contains all vertices in the graph with:
//! - `_vid`: Internal vertex ID (primary key)
//! - `_uid`: Content-addressed unique ID (SHA3-256 hash)
//! - `ext_id`: Optional external/user-provided ID (globally unique)
//! - `labels`: List of label names (OpenCypher multi-label)
//! - `props_json`: All properties as JSONB blob
//! - `_deleted`: Soft-delete flag
//! - `_version`: MVCC version
//! - `_created_at`: Creation timestamp
//! - `_updated_at`: Update timestamp

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::{FilterExpr, Scalar, ScalarIndexType, ScanRequest};
use crate::runtime::counters::QueryCounters;
use crate::storage::arrow_convert::build_timestamp_column_from_vid_map;
use crate::storage::bulk_gate::IntoRowVisibility;
use anyhow::{Result, anyhow};
use arrow_array::builder::{
    FixedSizeBinaryBuilder, LargeBinaryBuilder, ListBuilder, StringBuilder,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{UniId, Vid};

/// Maximum `ext_id`s per `ext_id IN (...)` chunk in
/// [`MainVertexDataset::find_by_ext_ids_counted`].
///
/// Mirrors `MAX_VIDS_PER_CHUNK` on the vertex read path and `MAX_EIDS_PER_CHUNK`
/// on the edge one: an unbounded `IN` list inflates the scan request and stops
/// the scalar index earning its keep, while a bounded one preserves the indexed
/// lookup at any batch size.
const MAX_EXT_IDS_PER_CHUNK: usize = 10_000;

/// Main vertex dataset for the unified `vertices` table.
///
/// This table contains all vertices regardless of label, providing:
/// - Fast ID-based lookups without knowing the label
/// - Global ext_id uniqueness enforcement
/// - Multi-label storage with labels as a list column
#[derive(Debug)]
pub struct MainVertexDataset {
    _base_uri: String,
}

use super::with_version_bound;

/// A vertex's properties from its `props_json` blob, with their types.
///
/// The blob is a CypherValue map, decoded straight into `Value`s. These
/// readers used to pass it through `serde_json::Value`, which turned a
/// temporal into a string; the flush of a relabeled vertex then wrote that
/// string back as the property's value, losing the type for good (a NULL in
/// a declared column, a string in a schemaless one).
///
/// # Errors
/// Returns an error if the blob does not decode or is not a map.
fn decode_props_blob(bytes: &[u8]) -> Result<Properties> {
    match uni_common::cypher_value_codec::decode(bytes)
        .map_err(|e| anyhow!("Failed to decode CypherValue: {}", e))?
    {
        uni_common::Value::Map(map) => Ok(map),
        other => Err(anyhow!("props_json is not a map: {:?}", other)),
    }
}

/// Extract a label vector from one row of a `List<Utf8>` labels column.
///
/// Returns `None` when the row's values are not a `StringArray`. Nulls within
/// the list are skipped.
fn list_array_to_labels(list_arr: &arrow_array::ListArray, row: usize) -> Option<Vec<String>> {
    let values = list_arr.value(row);
    let str_arr = values.as_any().downcast_ref::<arrow_array::StringArray>()?;
    let labels: Vec<String> = (0..str_arr.len())
        .filter_map(|i| {
            if str_arr.is_null(i) {
                None
            } else {
                Some(str_arr.value(i).to_string())
            }
        })
        .collect();
    Some(labels)
}

impl MainVertexDataset {
    /// Create a new MainVertexDataset.
    pub fn new(base_uri: &str) -> Self {
        Self {
            _base_uri: base_uri.to_string(),
        }
    }

    /// Get the Arrow schema for the main vertices table.
    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_uid", DataType::FixedSizeBinary(32), true),
            Field::new("ext_id", DataType::Utf8, true),
            Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("props_json", DataType::LargeBinary, true),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new(
                "_created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ]))
    }

    /// Get the table name for the main vertices table.
    pub fn table_name() -> &'static str {
        table_names::main_vertex_table_name()
    }

    /// Compute the UniId (content-addressed hash) for a vertex.
    fn compute_vertex_uid(labels: &[String], ext_id: Option<&str>, props: &Properties) -> UniId {
        let mut hasher = Sha3_256::new();

        // Hash labels (sorted for consistency)
        let mut sorted_labels = labels.to_vec();
        sorted_labels.sort();
        for label in &sorted_labels {
            hasher.update(label.as_bytes());
            hasher.update(b"\0");
        }

        // Hash ext_id if present
        if let Some(ext_id) = ext_id {
            hasher.update(b"ext_id:");
            hasher.update(ext_id.as_bytes());
            hasher.update(b"\0");
        }

        // Hash properties (sorted by key for deterministic hashing)
        let mut sorted_keys: Vec<_> = props.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            if key == "ext_id" {
                continue; // Already handled above
            }
            if let Some(val) = props.get(key) {
                hasher.update(key.as_bytes());
                hasher.update(b":");
                hasher.update(val.to_string().as_bytes());
                hasher.update(b"\0");
            }
        }

        let result = hasher.finalize();
        UniId::from_bytes(result.into())
    }

    /// Build a record batch for the main vertices table.
    ///
    /// # Arguments
    /// * `vertices` - List of (vid, labels, properties, deleted, version) tuples
    /// * `created_at` - Optional map of Vid -> nanoseconds since epoch
    /// * `updated_at` - Optional map of Vid -> nanoseconds since epoch
    pub fn build_record_batch(
        vertices: &[(Vid, Vec<String>, Properties, bool, u64)],
        created_at: Option<&HashMap<Vid, i64>>,
        updated_at: Option<&HashMap<Vid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = Self::get_arrow_schema();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        // _vid column
        let vids: Vec<u64> = vertices.iter().map(|(v, _, _, _, _)| v.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(vids)));

        // _uid column
        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for (_, labels, props, _, _) in vertices.iter() {
            let ext_id = props.get("ext_id").and_then(|v| v.as_str());
            let uid = Self::compute_vertex_uid(labels, ext_id, props);
            uid_builder.append_value(uid.as_bytes())?;
        }
        columns.push(Arc::new(uid_builder.finish()));

        // ext_id column
        let mut ext_id_builder = StringBuilder::new();
        for (_, _, props, _, _) in vertices.iter() {
            if let Some(ext_id_val) = props.get("ext_id").and_then(|v| v.as_str()) {
                ext_id_builder.append_value(ext_id_val);
            } else {
                ext_id_builder.append_null();
            }
        }
        columns.push(Arc::new(ext_id_builder.finish()));

        // labels column (List<String>)
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        for (_, labels, _, _, _) in vertices.iter() {
            let values_builder = labels_builder.values();
            for label in labels {
                values_builder.append_value(label);
            }
            labels_builder.append(true);
        }
        columns.push(Arc::new(labels_builder.finish()));

        // props_json column (JSONB binary encoding).
        //
        // Encoded straight from the property map, for the reasons on the edge
        // sibling in `main_edge.rs` (#228): the previous
        // `serde_json::to_value(props)` round trip had no representation for a
        // non-finite `Float` and wrote `Null` in its place, and its
        // `unwrap_or(json!({}))` turned a serialization failure into *every*
        // property on the row silently disappearing rather than an error.
        let mut props_json_builder = LargeBinaryBuilder::new();
        for (_, _, props, _, _) in vertices.iter() {
            let uni_val = uni_common::Value::Map(props.clone());
            props_json_builder.append_value(uni_common::cypher_value_codec::encode(&uni_val));
        }
        columns.push(Arc::new(props_json_builder.finish()));

        // _deleted column
        let deleted: Vec<bool> = vertices.iter().map(|(_, _, _, d, _)| *d).collect();
        columns.push(Arc::new(BooleanArray::from(deleted)));

        // _version column
        let versions: Vec<u64> = vertices.iter().map(|(_, _, _, _, v)| *v).collect();
        columns.push(Arc::new(UInt64Array::from(versions)));

        // _created_at and _updated_at columns using shared builder
        let vids = vertices.iter().map(|(v, _, _, _, _)| *v);
        columns.push(build_timestamp_column_from_vid_map(
            vids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_vid_map(vids, updated_at));

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Write a batch to the main vertices table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    /// Race-safe under async-flush concurrent writes — see
    /// `crate::storage::manager::write_batch_with_lance_conflict_retry`.
    pub async fn write_batch(backend: &dyn StorageBackend, batch: RecordBatch) -> Result<()> {
        let table_name = table_names::main_vertex_table_name();
        crate::storage::manager::write_batch_with_lance_conflict_retry(backend, table_name, batch)
            .await
    }

    /// Ensure default indexes exist on the main vertices table.
    ///
    /// Checks for existing indexes before creating to avoid expensive
    /// full-table rebuilds on every flush (LanceDB replaces indexes on create).
    pub async fn ensure_default_indexes(backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::main_vertex_table_name();
        // A table that does not exist has nothing to index (#182). Reachable on
        // a tombstone-only flush, where the full-row write that would have
        // created it was skipped; `list_indexes` opens the dataset and would
        // error. Without this the #182 fix merely moves the failure one line.
        if !backend.table_exists(table_name).await? {
            return Ok(());
        }
        let indices = backend.list_indexes(table_name).await?;

        let has_index = |col: &str| {
            indices
                .iter()
                .any(|idx| idx.columns.contains(&col.to_string()))
        };

        for (column, idx_type) in [
            ("_vid", ScalarIndexType::BTree),
            ("ext_id", ScalarIndexType::BTree),
            ("_uid", ScalarIndexType::BTree),
            ("labels", ScalarIndexType::LabelList),
        ] {
            if has_index(column) {
                continue;
            }
            log::info!("Creating {} index on main_vertices", column);
            if let Err(e) = backend
                .create_scalar_index(table_name, &[column], idx_type, None)
                .await
            {
                crate::storage::record_default_index_failure(table_name, column, &e);
            }
        }

        Ok(())
    }

    /// Query the main vertices table for a vertex by ext_id.
    ///
    /// Returns the Vid if found, None otherwise.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///   Pass `None` for writer uniqueness checks (global visibility).
    ///   Pass `Some(hwm)` for query-time snapshot isolation.
    pub async fn find_by_ext_id(
        backend: &dyn StorageBackend,
        ext_id: &str,
        version: impl IntoRowVisibility,
    ) -> Result<Option<Vid>> {
        let version = version.into_row_visibility();
        Self::find_by_ext_id_counted(backend, ext_id, &version, None).await
    }

    /// [`Self::find_by_ext_id`], carrying a query's counters into the backend.
    ///
    /// Same reason as the main-edge siblings: the backend never sees a
    /// [`QueryContext`](crate::QueryContext), so `ScanRequest::counters` is the
    /// only route by which this scan reaches `scans_reported`. Without it the
    /// stats callback is never even attached, so the scan is unobservable
    /// rather than merely uncounted.
    pub async fn find_by_ext_id_counted(
        backend: &dyn StorageBackend,
        ext_id: &str,
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<Option<Vid>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        // MVCC (review C2): scan tombstones too — the highest-version row wins,
        // and a deleted winner yields `None`. Filtering `_deleted = false` here
        // (and taking `value(0)` with no version comparison, as the old code
        // did) would let an OLDER live version resurrect a vertex whose tombstone
        // is the true winner.
        let filter = with_version_bound(
            FilterExpr::equals("ext_id", Scalar::Str(ext_id.to_string())),
            &version,
        );

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "_vid".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ])
                    .with_counters(counters.cloned()),
            )
            .await?;

        let mut best_vid: Option<Vid> = None;
        let mut best_version: u64 = 0;
        let mut best_deleted = false;

        for batch in results {
            let vid_col = batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
            let ver_col = batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
            let deleted_col = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

            if let (Some(vid_arr), Some(ver_arr)) = (vid_col, ver_col) {
                for i in 0..batch.num_rows() {
                    let v = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };
                    if v >= best_version {
                        best_version = v;
                        best_deleted = deleted_col.is_some_and(|d| d.value(i));
                        best_vid = Some(Vid::from(vid_arr.value(i)));
                    }
                }
            }
        }

        if best_deleted {
            return Ok(None);
        }
        let Some(vid) = best_vid else {
            return Ok(None);
        };
        let holders =
            Self::current_ext_id_holders(backend, &[(ext_id.to_string(), vid)], &version, counters)
                .await?;
        Ok(holders.into_values().next())
    }

    /// Keep each `(ext_id, vid)` whose vid still holds that `ext_id`.
    ///
    /// The `ext_id` lookups pick the highest-version row *carrying* the
    /// `ext_id`, but a vertex that later took another `ext_id`, removed it, or
    /// was rewritten without it has newer rows that do not carry it, so the
    /// lookup alone kept reporting the freed `ext_id` as taken: reassigning it
    /// was refused permanently, flushed or not. This reads each candidate's
    /// newest row overall (tombstones included) and keeps the pair only when
    /// that row is live and still carries the `ext_id`.
    ///
    /// Under the uniqueness invariant the highest-version row carrying an
    /// `ext_id` belongs to its latest assignee, so checking that one vid is
    /// enough: anyone who took the `ext_id` after it was freed wrote a newer
    /// row carrying it.
    async fn current_ext_id_holders(
        backend: &dyn StorageBackend,
        candidates: &[(String, Vid)],
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<HashMap<String, Vid>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();
        // Newest row per vid: (version, deleted, ext_id).
        let mut latest: HashMap<Vid, (u64, bool, Option<String>)> = HashMap::new();
        for chunk in candidates.chunks(MAX_EXT_IDS_PER_CHUNK) {
            let filter = with_version_bound(
                FilterExpr::one_of("_vid", chunk.iter().map(|(_, v)| Scalar::UInt(v.as_u64()))),
                &version,
            );
            let results = backend
                .scan(
                    ScanRequest::all(table_name)
                        .with_filter(filter)
                        .with_columns(vec![
                            "_vid".to_string(),
                            "ext_id".to_string(),
                            "_version".to_string(),
                            "_deleted".to_string(),
                        ])
                        .with_counters(counters.cloned()),
                )
                .await?;
            for batch in results {
                let vid_col = batch
                    .column_by_name("_vid")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
                let ext_col = batch
                    .column_by_name("ext_id")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());
                let ver_col = batch
                    .column_by_name("_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
                let deleted_col = batch
                    .column_by_name("_deleted")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());
                let (Some(vid_arr), Some(ver_arr)) = (vid_col, ver_col) else {
                    continue;
                };
                for i in 0..batch.num_rows() {
                    let vid = Vid::from(vid_arr.value(i));
                    let v = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };
                    // `>=`: the later row wins a tie, as in the lookups.
                    if latest.get(&vid).is_none_or(|(best, ..)| v >= *best) {
                        let ext = ext_col
                            .filter(|c| !c.is_null(i))
                            .map(|c| c.value(i).to_string());
                        latest.insert(vid, (v, deleted_col.is_some_and(|d| d.value(i)), ext));
                    }
                }
            }
        }
        Ok(candidates
            .iter()
            .filter(|(ext_id, vid)| {
                latest
                    .get(vid)
                    .is_some_and(|(_, deleted, ext)| !deleted && ext.as_deref() == Some(ext_id))
            })
            .cloned()
            .collect())
    }

    /// The batched counterpart of [`Self::find_by_ext_id`].
    ///
    /// Batch insert probed one `ext_id` at a time to enforce global uniqueness,
    /// so a batch of N vertices issued N scans of the main vertex table before
    /// writing anything (#220). This collapses them into one chunked
    /// `ext_id IN (...)` lookup, which keeps the scalar index in play.
    ///
    /// Version selection matches the singular form exactly, per `ext_id`: the
    /// highest `_version` wins with tombstones included in the contest, and an
    /// `ext_id` whose winner is a tombstone is absent from the map — the
    /// batched spelling of its `Ok(None)`. Ties resolve to the later row, as
    /// they do there.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails. Note that an absent table is
    /// `Ok`, not an error, matching the singular form — but a failed *scan*
    /// must stay an error here: this backs a uniqueness constraint, and a probe
    /// that reads as "no duplicate" on an I/O failure admits the very duplicate
    /// it exists to reject (#233).
    pub async fn find_by_ext_ids_counted(
        backend: &dyn StorageBackend,
        ext_ids: &[String],
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<HashMap<String, Vid>> {
        let version = version.into_row_visibility();
        let mut found: HashMap<String, Vid> = HashMap::new();
        if ext_ids.is_empty() {
            return Ok(found);
        }

        let table_name = table_names::main_vertex_table_name();
        if !backend.table_exists(table_name).await? {
            return Ok(found);
        }

        // Winner per ext_id: (version, deleted, vid). Tombstones stay in the
        // map until the end so a lower-version live row cannot displace them.
        let mut best: HashMap<String, (u64, bool, Vid)> = HashMap::new();
        for chunk in ext_ids.chunks(MAX_EXT_IDS_PER_CHUNK) {
            let filter = with_version_bound(
                FilterExpr::one_of("ext_id", chunk.iter().map(|e| Scalar::Str(e.clone()))),
                &version,
            );
            let results = backend
                .scan(
                    ScanRequest::all(table_name)
                        .with_filter(filter)
                        .with_columns(vec![
                            "_vid".to_string(),
                            "ext_id".to_string(),
                            "_version".to_string(),
                            "_deleted".to_string(),
                        ])
                        .with_counters(counters.cloned()),
                )
                .await?;

            for batch in results {
                let vid_col = batch
                    .column_by_name("_vid")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
                let ext_col = batch
                    .column_by_name("ext_id")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());
                let ver_col = batch
                    .column_by_name("_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
                let deleted_col = batch
                    .column_by_name("_deleted")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

                let (Some(vid_arr), Some(ext_arr), Some(ver_arr)) = (vid_col, ext_col, ver_col)
                else {
                    continue;
                };

                for i in 0..batch.num_rows() {
                    if ext_arr.is_null(i) || vid_arr.is_null(i) {
                        continue;
                    }
                    let key = ext_arr.value(i).to_string();
                    let v = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };
                    // `>=` keeps the singular form's tie-breaking, where the
                    // later row wins.
                    if best.get(&key).is_none_or(|(best_v, ..)| v >= *best_v) {
                        best.insert(
                            key,
                            (
                                v,
                                deleted_col.is_some_and(|d| d.value(i)),
                                Vid::from(vid_arr.value(i)),
                            ),
                        );
                    }
                }
            }
        }

        let candidates: Vec<(String, Vid)> = best
            .into_iter()
            .filter(|(_, (_, deleted, _))| !deleted)
            .map(|(ext_id, (_, _, vid))| (ext_id, vid))
            .collect();
        if candidates.is_empty() {
            return Ok(found);
        }
        found = Self::current_ext_id_holders(backend, &candidates, &version, counters).await?;
        Ok(found)
    }

    /// Check if an ext_id already exists in the main vertices table.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn ext_id_exists(
        backend: &dyn StorageBackend,
        ext_id: &str,
        version: impl IntoRowVisibility,
    ) -> Result<bool> {
        let version = version.into_row_visibility();
        Ok(Self::find_by_ext_id(backend, ext_id, &version)
            .await?
            .is_some())
    }

    /// Find labels for a vertex by VID in the main vertices table.
    ///
    /// Returns the list of labels if found, None otherwise.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn find_labels_by_vid(
        backend: &dyn StorageBackend,
        vid: Vid,
        version: impl IntoRowVisibility,
    ) -> Result<Option<Vec<String>>> {
        let version = version.into_row_visibility();
        Self::find_labels_by_vid_counted(backend, vid, &version, None).await
    }

    /// [`Self::find_labels_by_vid`], carrying a query's counters into the backend.
    ///
    /// Same reason as the main-edge siblings: the backend never sees a
    /// [`QueryContext`](crate::QueryContext), so `ScanRequest::counters` is the
    /// only route by which this scan reaches `scans_reported`. Without it the
    /// stats callback is never even attached, so the scan is unobservable
    /// rather than merely uncounted.
    pub async fn find_labels_by_vid_counted(
        backend: &dyn StorageBackend,
        vid: Vid,
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<Option<Vec<String>>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        // MVCC (review C2): the highest-version row wins, tombstones included,
        // and a deleted winner yields `None`. Filtering `_deleted = false` (and
        // taking `value(0)` with no version comparison, as the old code did)
        // would let an OLDER live version resurrect a deleted vertex.
        let filter = with_version_bound(
            FilterExpr::equals("_vid", Scalar::UInt(vid.as_u64())),
            &version,
        );

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "labels".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ])
                    .with_counters(counters.cloned()),
            )
            .await?;

        let mut best_labels: Option<Vec<String>> = None;
        let mut best_version: u64 = 0;
        let mut best_deleted = false;

        for batch in results {
            let labels_col = batch
                .column_by_name("labels")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>());
            let ver_col = batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
            let deleted_col = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

            if let (Some(list_arr), Some(ver_arr)) = (labels_col, ver_col) {
                for i in 0..batch.num_rows() {
                    let v = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };
                    if v >= best_version {
                        best_version = v;
                        best_deleted = deleted_col.is_some_and(|d| d.value(i));
                        best_labels = if best_deleted {
                            Some(Vec::new())
                        } else {
                            Some(list_array_to_labels(list_arr, i).unwrap_or_default())
                        };
                    }
                }
            }
        }

        if best_deleted {
            return Ok(None);
        }
        Ok(best_labels)
    }

    /// Batch-fetch properties for multiple VIDs from the main vertices table.
    ///
    /// Returns a HashMap mapping VIDs to their parsed properties.
    /// Non-deleted vertices are returned with properties from props_json.
    /// This is used for schemaless vertex scans via DataFusion.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails or JSON parsing fails.
    pub async fn find_batch_props_by_vids(
        backend: &dyn StorageBackend,
        vids: &[Vid],
        version: impl IntoRowVisibility,
    ) -> Result<HashMap<Vid, Properties>> {
        let version = version.into_row_visibility();
        Self::find_batch_props_by_vids_counted(backend, vids, &version, None).await
    }

    /// [`Self::find_batch_props_by_vids`], carrying a query's counters into the backend.
    ///
    /// Same reason as the main-edge siblings: the backend never sees a
    /// [`QueryContext`](crate::QueryContext), so `ScanRequest::counters` is the
    /// only route by which this scan reaches `scans_reported`. Without it the
    /// stats callback is never even attached, so the scan is unobservable
    /// rather than merely uncounted.
    pub async fn find_batch_props_by_vids_counted(
        backend: &dyn StorageBackend,
        vids: &[Vid],
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<HashMap<Vid, Properties>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();

        if vids.is_empty() || !backend.table_exists(table_name).await? {
            return Ok(HashMap::new());
        }

        // Build IN clause for VIDs.
        //
        // MVCC (mirrors `find_props_by_vid`): the scan must see deletion
        // tombstones — the highest-`_version` row per vid wins, and a deleted
        // winner drops the vid. Filtering `_deleted = false` here would let an
        // OLDER live version resurrect a vertex whose tombstone lives only in the
        // main table (review C2), and last-row-wins with no `_version` ranking
        // returned stale props depending on physical scan order.
        let filter = with_version_bound(
            FilterExpr::one_of("_vid", vids.iter().map(|v| Scalar::UInt(v.as_u64()))),
            &version,
        );

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "_vid".to_string(),
                        "props_json".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ])
                    .with_counters(counters.cloned()),
            )
            .await?;

        // Per-vid highest-version winner: (version, deleted, props).
        let mut best: HashMap<Vid, (u64, bool, Properties)> = HashMap::new();

        for batch in results {
            let vid_col = batch.column_by_name("_vid");
            let props_col = batch.column_by_name("props_json");
            let ver_col = batch.column_by_name("_version");
            let deleted_col = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

            if let (Some(vid_arr), Some(props_arr), Some(ver_arr)) = (
                vid_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()),
                ver_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            ) {
                for i in 0..batch.num_rows() {
                    if vid_arr.is_null(i) {
                        continue;
                    }
                    let vid = Vid::new(vid_arr.value(i));
                    let version = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };

                    // Keep the highest-version row for this vid (>= so a later
                    // equal-version row still wins, matching find_props_by_vid).
                    if best.get(&vid).is_some_and(|(bv, _, _)| version < *bv) {
                        continue;
                    }
                    let deleted = deleted_col.is_some_and(|d| d.value(i));
                    let props: Properties =
                        if deleted || props_arr.is_null(i) || props_arr.value(i).is_empty() {
                            Properties::new()
                        } else {
                            decode_props_blob(props_arr.value(i))?
                        };
                    best.insert(vid, (version, deleted, props));
                }
            }
        }

        // Drop tombstone winners; return live winners' props.
        let props_map = best
            .into_iter()
            .filter(|(_, (_, deleted, _))| !*deleted)
            .map(|(vid, (_, _, props))| (vid, props))
            .collect();

        Ok(props_map)
    }

    /// Find properties for a vertex by VID in the main vertices table.
    ///
    /// Returns the props_json parsed into a Properties HashMap if found.
    /// This is used as a fallback for unknown/schemaless labels.
    ///
    /// MVCC: the scan must see deletion tombstones — the highest-version row
    /// wins, and a deleted winner yields `None`. Filtering `_deleted = false`
    /// in the scan would let an OLDER live version resurrect a vertex whose
    /// tombstone lives only in the main table (schemaless deletes have no
    /// per-label table to record them).
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    ///
    /// # Errors
    ///
    /// Returns an error if the table query fails or JSON parsing fails.
    pub async fn find_props_by_vid(
        backend: &dyn StorageBackend,
        vid: Vid,
        version: impl IntoRowVisibility,
    ) -> Result<Option<Properties>> {
        let version = version.into_row_visibility();
        Self::find_props_by_vid_counted(backend, vid, &version, None).await
    }

    /// [`Self::find_props_by_vid`], carrying a query's counters into the backend.
    ///
    /// Same reason as the main-edge siblings: the backend never sees a
    /// [`QueryContext`](crate::QueryContext), so `ScanRequest::counters` is the
    /// only route by which this scan reaches `scans_reported`. Without it the
    /// stats callback is never even attached, so the scan is unobservable
    /// rather than merely uncounted.
    pub async fn find_props_by_vid_counted(
        backend: &dyn StorageBackend,
        vid: Vid,
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<Option<Properties>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        let filter = with_version_bound(
            FilterExpr::equals("_vid", Scalar::UInt(vid.as_u64())),
            &version,
        );

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "props_json".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ])
                    .with_counters(counters.cloned()),
            )
            .await?;

        // Find the row with highest version (latest), tombstones included.
        let mut best_props: Option<Properties> = None;
        let mut best_version: u64 = 0;
        let mut best_deleted = false;

        for batch in results {
            let props_col = batch.column_by_name("props_json");
            let version_col = batch.column_by_name("_version");
            let deleted_col = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

            if let (Some(props_arr), Some(ver_arr)) = (
                props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()),
                version_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            ) {
                for i in 0..batch.num_rows() {
                    let version = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };

                    if version >= best_version {
                        best_version = version;
                        best_deleted = deleted_col.is_some_and(|d| d.value(i));
                        if best_deleted || props_arr.is_null(i) || props_arr.value(i).is_empty() {
                            best_props = Some(Properties::new());
                        } else {
                            best_props = Some(decode_props_blob(props_arr.value(i))?);
                        }
                    }
                }
            }
        }

        if best_deleted {
            return Ok(None);
        }
        Ok(best_props)
    }

    /// Batch-fetch labels for multiple VIDs from the main vertices table.
    ///
    /// # Arguments
    /// * `version` - Optional version high water mark for snapshot isolation.
    pub async fn find_batch_labels_by_vids(
        backend: &dyn StorageBackend,
        vids: &[Vid],
        version: impl IntoRowVisibility,
    ) -> Result<HashMap<Vid, Vec<String>>> {
        let version = version.into_row_visibility();
        Self::find_batch_labels_by_vids_counted(backend, vids, &version, None).await
    }

    /// [`Self::find_batch_labels_by_vids`], carrying a query's counters into the backend.
    ///
    /// Same reason as the main-edge siblings: the backend never sees a
    /// [`QueryContext`](crate::QueryContext), so `ScanRequest::counters` is the
    /// only route by which this scan reaches `scans_reported`. Without it the
    /// stats callback is never even attached, so the scan is unobservable
    /// rather than merely uncounted.
    pub async fn find_batch_labels_by_vids_counted(
        backend: &dyn StorageBackend,
        vids: &[Vid],
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<HashMap<Vid, Vec<String>>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();

        if vids.is_empty() || !backend.table_exists(table_name).await? {
            return Ok(HashMap::new());
        }

        // Build IN clause for VIDs. Same MVCC discipline as
        // `find_batch_props_by_vids`: version-max winner per vid, tombstones
        // included (a deleted winner drops the vid). `_deleted = false` filtering
        // + last-row-wins previously resurrected deleted vertices' labels.
        let filter = with_version_bound(
            FilterExpr::one_of("_vid", vids.iter().map(|v| Scalar::UInt(v.as_u64()))),
            &version,
        );

        let results = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "_vid".to_string(),
                        "labels".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ])
                    .with_counters(counters.cloned()),
            )
            .await?;

        // Per-vid highest-version winner: (version, deleted, labels).
        let mut best: HashMap<Vid, (u64, bool, Vec<String>)> = HashMap::new();

        for batch in results {
            let vid_col = batch.column_by_name("_vid");
            let labels_col = batch.column_by_name("labels");
            let ver_col = batch.column_by_name("_version");
            let deleted_col = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());

            if let (Some(vid_arr), Some(labels_arr), Some(ver_arr)) = (
                vid_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                labels_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>()),
                ver_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            ) {
                for i in 0..batch.num_rows() {
                    if vid_arr.is_null(i) {
                        continue;
                    }
                    let vid = Vid::new(vid_arr.value(i));
                    let version = if ver_arr.is_null(i) {
                        0
                    } else {
                        ver_arr.value(i)
                    };

                    if best.get(&vid).is_some_and(|(bv, _, _)| version < *bv) {
                        continue;
                    }
                    let deleted = deleted_col.is_some_and(|d| d.value(i));
                    let labels = if deleted {
                        Vec::new()
                    } else {
                        list_array_to_labels(labels_arr, i).unwrap_or_default()
                    };
                    best.insert(vid, (version, deleted, labels));
                }
            }
        }

        let label_map = best
            .into_iter()
            .filter(|(_, (_, deleted, _))| !*deleted)
            .map(|(vid, (_, _, labels))| (vid, labels))
            .collect();

        Ok(label_map)
    }

    /// Latest properties of every live flushed vertex that carries `label`.
    ///
    /// This is the only flushed copy of a schemaless value: an undeclared
    /// label has no table of its own, and an undeclared property of a
    /// declared label has no column of its own. A vertex counts only if its
    /// latest row (within `version`) is live and still carries `label`. A
    /// label filter alone would also match an older row of a vertex that has
    /// since been relabeled away from `label`, so the candidates it finds are
    /// re-read with all their rows. Superseded rows are never decoded.
    ///
    /// # Errors
    ///
    /// Returns an error if a scan fails or a `props_json` blob does not decode.
    pub async fn find_label_props_counted(
        backend: &dyn StorageBackend,
        label: &str,
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<HashMap<Vid, Properties>> {
        Ok(
            Self::find_label_latest_rows(backend, label, version, counters)
                .await?
                .into_iter()
                .filter(|(_, row)| row.is_live_with(label))
                .map(|(vid, row)| (vid, row.props))
                .collect(),
        )
    }

    /// The latest row (within `version`) of every flushed vertex that has
    /// *ever* carried `label`.
    ///
    /// Unlike [`Self::find_label_props_counted`] this keeps the vertices whose
    /// latest row is a tombstone or no longer carries `label`, so a caller
    /// reconciling another copy of the label's rows can retire them too (see
    /// `StorageManager::backfill_declared_label`). Properties are decoded only
    /// for a latest row that is live and carries `label`; every other row
    /// comes back with empty properties.
    ///
    /// # Errors
    ///
    /// Returns an error if a scan fails or a `props_json` blob does not decode.
    pub async fn find_label_latest_rows(
        backend: &dyn StorageBackend,
        label: &str,
        version: impl IntoRowVisibility,
        counters: Option<&Arc<QueryCounters>>,
    ) -> Result<HashMap<Vid, MainVertexRow>> {
        let version = version.into_row_visibility();
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(HashMap::new());
        }

        let candidates = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(with_version_bound(
                        FilterExpr::array_contains("labels", Scalar::Str(label.to_string())),
                        &version,
                    ))
                    .with_columns(vec!["_vid".to_string()])
                    .with_counters(counters.cloned()),
            )
            .await?;
        let mut vids: Vec<u64> = Vec::new();
        for batch in &candidates {
            if let Some(arr) = batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                vids.extend(arr.iter().flatten());
            }
        }
        vids.sort_unstable();
        vids.dedup();
        if vids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(with_version_bound(
                        FilterExpr::one_of("_vid", vids.into_iter().map(Scalar::UInt)),
                        &version,
                    ))
                    .with_columns(vec![
                        "_vid".to_string(),
                        "labels".to_string(),
                        "props_json".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                        "_created_at".to_string(),
                        "_updated_at".to_string(),
                    ])
                    .with_counters(counters.cloned()),
            )
            .await?;

        // Per-vid highest-version winner, as (version, batch, row). `>=` lets
        // a later equal-version row win, as in `find_batch_props_by_vids`.
        let mut best: HashMap<Vid, (u64, usize, usize)> = HashMap::new();
        for (b, batch) in rows.iter().enumerate() {
            let (Some(vid_arr), Some(ver_arr)) = (
                batch
                    .column_by_name("_vid")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                batch
                    .column_by_name("_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            ) else {
                continue;
            };
            for i in 0..batch.num_rows() {
                if vid_arr.is_null(i) {
                    continue;
                }
                let vid = Vid::new(vid_arr.value(i));
                let version = if ver_arr.is_null(i) {
                    0
                } else {
                    ver_arr.value(i)
                };
                if best.get(&vid).is_some_and(|(bv, _, _)| version < *bv) {
                    continue;
                }
                best.insert(vid, (version, b, i));
            }
        }

        let timestamp = |batch: &RecordBatch, column: &str, i: usize| {
            batch
                .column_by_name(column)
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .filter(|a| a.is_valid(i))
                .map(|a| a.value(i))
        };

        let mut latest = HashMap::with_capacity(best.len());
        for (vid, (version, b, i)) in best {
            let batch = &rows[b];
            let deleted = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                .is_some_and(|d| d.is_valid(i) && d.value(i));
            let labels = batch
                .column_by_name("labels")
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>())
                .and_then(|l| list_array_to_labels(l, i))
                .unwrap_or_default();
            let mut row = MainVertexRow {
                version,
                deleted,
                labels,
                props: Properties::new(),
                created_at: timestamp(batch, "_created_at", i),
                updated_at: timestamp(batch, "_updated_at", i),
            };
            if row.is_live_with(label)
                && let Some(props_arr) = batch
                    .column_by_name("props_json")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>())
                && !props_arr.is_null(i)
                && !props_arr.value(i).is_empty()
            {
                row.props = decode_props_blob(props_arr.value(i))?;
            }
            latest.insert(vid, row);
        }

        Ok(latest)
    }
}

/// A vertex's latest row in the main `vertices` table, as returned by
/// [`MainVertexDataset::find_label_latest_rows`].
#[derive(Debug, Clone)]
pub struct MainVertexRow {
    /// MVCC version of the row.
    pub version: u64,
    /// Whether the row is a tombstone.
    pub deleted: bool,
    /// The vertex's labels as of this row.
    pub labels: Vec<String>,
    /// The vertex's properties; empty unless the row is live and carries the
    /// label it was looked up by.
    pub props: Properties,
    /// `_created_at`, in nanoseconds since the epoch.
    pub created_at: Option<i64>,
    /// `_updated_at`, in nanoseconds since the epoch.
    pub updated_at: Option<i64>,
}

impl MainVertexRow {
    /// Whether this row is live and still carries `label`.
    #[must_use]
    pub fn is_live_with(&self, label: &str) -> bool {
        !self.deleted && self.labels.iter().any(|l| l == label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    #[test]
    fn test_main_vertex_schema() {
        let schema = MainVertexDataset::get_arrow_schema();
        assert_eq!(schema.fields().len(), 9);
        assert!(schema.field_with_name("_vid").is_ok());
        assert!(schema.field_with_name("_uid").is_ok());
        assert!(schema.field_with_name("ext_id").is_ok());
        assert!(schema.field_with_name("labels").is_ok());
        assert!(schema.field_with_name("props_json").is_ok());
        assert!(schema.field_with_name("_deleted").is_ok());
        assert!(schema.field_with_name("_version").is_ok());
        assert!(schema.field_with_name("_created_at").is_ok());
        assert!(schema.field_with_name("_updated_at").is_ok());
    }

    #[test]
    fn test_build_record_batch() {
        use uni_common::Value;
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("ext_id".to_string(), Value::String("user_001".to_string()));

        let vertices = vec![(Vid::new(1), vec!["Person".to_string()], props, false, 1u64)];

        let batch = MainVertexDataset::build_record_batch(&vertices, None, None).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);

        // Check ext_id was extracted
        let ext_id_col = batch.column_by_name("ext_id").unwrap();
        let ext_id_arr = ext_id_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ext_id_arr.value(0), "user_001");
    }

    #[test]
    fn test_compute_vertex_uid_deterministic() {
        use uni_common::Value;
        let labels = vec!["Person".to_string()];
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let uid1 = MainVertexDataset::compute_vertex_uid(&labels, None, &props);
        let uid2 = MainVertexDataset::compute_vertex_uid(&labels, None, &props);
        assert_eq!(uid1, uid2, "Same inputs should produce same UID");
    }

    #[test]
    fn test_compute_vertex_uid_label_order_independence() {
        use uni_common::Value;
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let labels_ab = vec!["Admin".to_string(), "Person".to_string()];
        let labels_ba = vec!["Person".to_string(), "Admin".to_string()];

        let uid1 = MainVertexDataset::compute_vertex_uid(&labels_ab, None, &props);
        let uid2 = MainVertexDataset::compute_vertex_uid(&labels_ba, None, &props);
        assert_eq!(uid1, uid2, "Label order should not affect UID");
    }

    #[test]
    fn test_compute_vertex_uid_different_props_different_uid() {
        use uni_common::Value;
        let labels = vec!["Person".to_string()];

        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("Bob".to_string()));

        let uid1 = MainVertexDataset::compute_vertex_uid(&labels, None, &props1);
        let uid2 = MainVertexDataset::compute_vertex_uid(&labels, None, &props2);
        assert_ne!(
            uid1, uid2,
            "Different properties should produce different UIDs"
        );
    }

    /// MVCC regression (review C2): a deletion tombstone written at a higher
    /// version must win over the older live row. `find_by_ext_id` /
    /// `ext_id_exists` / `find_labels_by_vid` previously filtered
    /// `_deleted = false` and took `value(0)` with no version comparison, so the
    /// older live version resurrected a deleted vertex.
    #[tokio::test]
    async fn test_vertex_key_reads_respect_tombstone_winner() {
        use crate::backend::lance::LanceDbBackend;
        use uni_common::Value;

        let dir = tempfile::TempDir::new().unwrap();
        let be = LanceDbBackend::connect(dir.path().to_str().unwrap(), None)
            .await
            .unwrap();
        let backend: &dyn StorageBackend = &be;

        let mut props = HashMap::new();
        props.insert("ext_id".to_string(), Value::String("u1".to_string()));
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        // v1: live row.
        let live = MainVertexDataset::build_record_batch(
            &[(
                Vid::new(1),
                vec!["Person".to_string()],
                props.clone(),
                false,
                1u64,
            )],
            None,
            None,
        )
        .unwrap();
        MainVertexDataset::write_batch(backend, live).await.unwrap();

        // Sanity: visible while live.
        assert_eq!(
            MainVertexDataset::find_by_ext_id(backend, "u1", None)
                .await
                .unwrap(),
            Some(Vid::new(1))
        );
        assert!(
            MainVertexDataset::find_labels_by_vid(backend, Vid::new(1), None)
                .await
                .unwrap()
                .is_some()
        );

        // v2: deletion tombstone at a higher version — the winning row.
        let dead = MainVertexDataset::build_record_batch(
            &[(Vid::new(1), vec!["Person".to_string()], props, true, 2u64)],
            None,
            None,
        )
        .unwrap();
        MainVertexDataset::write_batch(backend, dead).await.unwrap();

        assert_eq!(
            MainVertexDataset::find_by_ext_id(backend, "u1", None)
                .await
                .unwrap(),
            None,
            "deleted (highest-version) winner must not be resurrected via ext_id"
        );
        assert!(
            !MainVertexDataset::ext_id_exists(backend, "u1", None)
                .await
                .unwrap(),
            "ext_id_exists must report absent once the winner is a tombstone"
        );
        assert_eq!(
            MainVertexDataset::find_labels_by_vid(backend, Vid::new(1), None)
                .await
                .unwrap(),
            None,
            "deleted winner must not return labels"
        );
    }
}
