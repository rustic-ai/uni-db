// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! LSM-style delta dataset for accumulating edge mutations before compaction.

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::{FilterExpr, Scalar, ScalarIndexType, ScanRequest};
use crate::storage::arrow_convert::build_timestamp_column;
use crate::storage::property_builder::PropertyColumnBuilder;
use crate::storage::value_codec::CrdtDecodeMode;
use anyhow::{Result, anyhow};
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, UInt8Array, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema, TimeUnit};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;
use uni_common::DataType;
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::Schema;

/// Default maximum number of rows allowed in in-memory compaction operations.
/// Set to 5 million rows to prevent OOM. For larger datasets, use chunked compaction.
pub const DEFAULT_MAX_COMPACTION_ROWS: usize = 5_000_000;

/// Estimated memory footprint per L1Entry in bytes (conservative estimate).
/// Each entry has: src_vid (8), dst_vid (8), eid (8), op (1), version (8),
/// properties (variable, ~64 avg), timestamps (16), overhead (~32) = ~145 bytes.
pub const ENTRY_SIZE_ESTIMATE: usize = 145;

/// Check whether loading `row_count` rows into memory would exceed `max_rows`.
///
/// Returns an error with a human-readable message including the estimated memory
/// footprint. Used by both Lance and LanceDB scan paths.
pub fn check_oom_guard(
    row_count: usize,
    max_rows: usize,
    entity_name: &str,
    qualifier: &str,
) -> Result<()> {
    if row_count > max_rows {
        let estimated_bytes = row_count * ENTRY_SIZE_ESTIMATE;
        return Err(anyhow!(
            "Table for {}_{} has {} rows (estimated {:.2} GB in memory), exceeding max_compaction_rows limit of {}. \
            Use chunked compaction or increase the limit. See issue #143.",
            entity_name,
            qualifier,
            row_count,
            estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
            max_rows
        ));
    }
    Ok(())
}

/// Operation type stored in the delta (L1) log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// Edge was inserted.
    Insert = 0,
    /// Edge was soft-deleted.
    Delete = 1,
}

/// A single entry in the L1 (sorted run) delta dataset.
#[derive(Clone, Debug)]
pub struct L1Entry {
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub eid: Eid,
    pub op: Op,
    pub version: u64,
    pub properties: Properties,
    /// Timestamp when the edge was created (nanoseconds since epoch).
    pub created_at: Option<i64>,
    /// Timestamp when the edge was last updated (nanoseconds since epoch).
    pub updated_at: Option<i64>,
}

/// LSM-style delta dataset for a single edge type and direction.
///
/// Stores L1 sorted runs that accumulate edge mutations before compaction
/// merges them into the base CSR.
#[derive(Debug)]
pub struct DeltaDataset {
    edge_type: String,
    direction: String, // "fwd" or "bwd"
    /// Lance branch for branched reads. `None` = primary.
    #[cfg_attr(not(feature = "lance-backend"), allow(dead_code))]
    branch: Option<String>,
}

impl DeltaDataset {
    /// Create a new `DeltaDataset` for the given edge type and direction.
    ///
    /// `_base_uri` is ignored: physical path resolution belongs to the storage
    /// backend; this type holds only the table's logical identity.
    pub fn new(_base_uri: &str, edge_type: &str, direction: &str) -> Self {
        Self {
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
            branch: None,
        }
    }

    /// Construct a delta dataset that reads from a Lance branch.
    pub fn new_branched(
        base_uri: &str,
        edge_type: &str,
        direction: &str,
        branch: impl Into<String>,
    ) -> Self {
        let mut ds = Self::new(base_uri, edge_type, direction);
        ds.branch = Some(branch.into());
        ds
    }

    /// Build the Arrow schema for this delta table using the given graph schema.
    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("src_vid", arrow_schema::DataType::UInt64, false),
            Field::new("dst_vid", arrow_schema::DataType::UInt64, false),
            Field::new("eid", arrow_schema::DataType::UInt64, false),
            Field::new("op", arrow_schema::DataType::UInt8, false), // 0=INSERT, 1=DELETE
            Field::new("_version", arrow_schema::DataType::UInt64, false),
            // New timestamp columns per STORAGE_DESIGN.md
            Field::new(
                "_created_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ];

        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            let mut sorted_props: Vec<_> = type_props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            for (name, meta) in sorted_props {
                fields.push(Field::new(name, meta.r#type.to_arrow(), meta.nullable));
            }
        }

        // Add overflow_json column for non-schema properties (JSONB binary format)
        fields.push(Field::new(
            "overflow_json",
            arrow_schema::DataType::LargeBinary,
            true,
        ));

        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    /// Serialize `entries` into an Arrow `RecordBatch` using the given graph schema.
    pub fn build_record_batch(&self, entries: &[L1Entry], schema: &Schema) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;

        let mut src_vids = Vec::with_capacity(entries.len());
        let mut dst_vids = Vec::with_capacity(entries.len());
        let mut eids = Vec::with_capacity(entries.len());
        let mut ops = Vec::with_capacity(entries.len());
        let mut versions = Vec::with_capacity(entries.len());

        for entry in entries {
            src_vids.push(entry.src_vid.as_u64());
            dst_vids.push(entry.dst_vid.as_u64());
            eids.push(entry.eid.as_u64());
            ops.push(entry.op as u8);
            versions.push(entry.version);
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(src_vids)),
            Arc::new(UInt64Array::from(dst_vids)),
            Arc::new(UInt64Array::from(eids)),
            Arc::new(UInt8Array::from(ops)),
            Arc::new(UInt64Array::from(versions)),
        ];

        // Build _created_at and _updated_at columns using shared builder
        columns.push(build_timestamp_column(entries.iter().map(|e| e.created_at)));
        columns.push(build_timestamp_column(entries.iter().map(|e| e.updated_at)));

        // Derive deleted flags from Op for property column building
        // Tombstones (Op::Delete) are logically deleted and should use default values
        let deleted_flags: Vec<bool> = entries.iter().map(|e| e.op == Op::Delete).collect();

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.edge_type, entries.len())
            .with_deleted(&deleted_flags)
            .build(|i| &entries[i].properties)?;

        columns.extend(prop_columns);

        // Build overflow_json column for non-schema properties
        let overflow_column = self.build_overflow_json_column(entries, schema)?;
        columns.push(overflow_column);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Build the overflow_json column containing properties not in schema.
    fn build_overflow_json_column(&self, entries: &[L1Entry], schema: &Schema) -> Result<ArrayRef> {
        crate::storage::property_builder::build_overflow_json_column(
            entries.len(),
            &self.edge_type,
            schema,
            |i| &entries[i].properties,
            &[],
        )
    }

    /// Sort entries by direction key (src_vid for fwd, dst_vid for bwd) then by version.
    fn sort_entries(&self, entries: &mut [L1Entry]) {
        let is_fwd = self.direction == "fwd";
        entries.sort_by(|a, b| {
            let key_a = if is_fwd { a.src_vid } else { a.dst_vid };
            let key_b = if is_fwd { b.src_vid } else { b.dst_vid };
            key_a.cmp(&key_b).then(a.version.cmp(&b.version))
        });
    }

    fn parse_batch(&self, batch: &RecordBatch, schema: &Schema) -> Result<Vec<L1Entry>> {
        let src_vids = batch
            .column_by_name("src_vid")
            .ok_or(anyhow!("Missing src_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid src_vid type"))?;
        let dst_vids = batch
            .column_by_name("dst_vid")
            .ok_or(anyhow!("Missing dst_vid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid dst_vid type"))?;
        let eids = batch
            .column_by_name("eid")
            .ok_or(anyhow!("Missing eid"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid eid type"))?;
        let ops = batch
            .column_by_name("op")
            .ok_or(anyhow!("Missing op"))?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or(anyhow!("Invalid op type"))?;
        let versions = batch
            .column_by_name("_version")
            .ok_or(anyhow!("Missing _version"))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(anyhow!("Invalid _version type"))?;

        // Try to read timestamp columns (may not exist in old data)
        let created_at_col = batch.column_by_name("_created_at").and_then(|c| {
            c.as_any()
                .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        });
        let updated_at_col = batch.column_by_name("_updated_at").and_then(|c| {
            c.as_any()
                .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        });

        // Prepare property columns
        let mut prop_cols = Vec::new();
        if let Some(type_props) = schema.properties.get(&self.edge_type) {
            for (name, meta) in type_props {
                if let Some(col) = batch.column_by_name(name) {
                    prop_cols.push((name, meta.r#type.clone(), col));
                }
            }
        }

        let mut entries = Vec::with_capacity(batch.num_rows());

        for i in 0..batch.num_rows() {
            let op = match ops.value(i) {
                0 => Op::Insert,
                1 => Op::Delete,
                _ => continue, // Unknown op
            };

            let properties = self.extract_properties(&prop_cols, i)?;

            // Read timestamps if present
            let read_ts = |col: Option<&PrimitiveArray<TimestampNanosecondType>>| {
                col.and_then(|c| (!c.is_null(i)).then(|| c.value(i)))
            };
            let created_at = read_ts(created_at_col);
            let updated_at = read_ts(updated_at_col);

            entries.push(L1Entry {
                src_vid: Vid::from(src_vids.value(i)),
                dst_vid: Vid::from(dst_vids.value(i)),
                eid: Eid::from(eids.value(i)),
                op,
                version: versions.value(i),
                properties,
                created_at,
                updated_at,
            });
        }
        Ok(entries)
    }

    /// Extract properties from columns for a single row.
    fn extract_properties(
        &self,
        prop_cols: &[(&String, DataType, &ArrayRef)],
        row: usize,
    ) -> Result<Properties> {
        let mut properties = Properties::new();
        for (name, dtype, col) in prop_cols {
            if col.is_null(row) {
                continue;
            }
            let val = Self::value_from_column(col.as_ref(), dtype, row)?;
            properties.insert(name.to_string(), uni_common::Value::from(val));
        }
        Ok(properties)
    }

    /// Decode an Arrow column value to JSON with lenient CRDT error handling.
    fn value_from_column(
        col: &dyn arrow_array::Array,
        dtype: &uni_common::DataType,
        row: usize,
    ) -> Result<serde_json::Value> {
        crate::storage::value_codec::value_from_column(col, dtype, row, CrdtDecodeMode::Lenient)
    }

    /// Returns the filter column name based on direction ("src_vid" for fwd, "dst_vid" for bwd).
    fn filter_column(&self) -> &'static str {
        if self.direction == "fwd" {
            "src_vid"
        } else {
            "dst_vid"
        }
    }

    // ========================================================================
    // Backend-agnostic Methods
    // ========================================================================

    /// Open or create a delta table via the storage backend.
    pub async fn open_or_create(
        &self,
        backend: &dyn StorageBackend,
        schema: &Schema,
    ) -> Result<()> {
        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);
        let arrow_schema = self.get_arrow_schema(schema)?;
        backend
            .open_or_create_table(&table_name, arrow_schema)
            .await
    }

    /// Write a run to a delta table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    /// Race-safe under async-flush — see
    /// `crate::storage::manager::write_batch_with_lance_conflict_retry`.
    pub async fn write_run(&self, backend: &dyn StorageBackend, batch: RecordBatch) -> Result<()> {
        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);
        crate::storage::manager::write_batch_with_lance_conflict_retry(backend, &table_name, batch)
            .await
    }

    /// Build a partial-column RecordBatch for Lance `MergeInsert`.
    /// Includes the join key (`eid`), the system columns (`src_vid`,
    /// `dst_vid`, `op=0/Insert`, `_version`, `_updated_at`), and ONLY
    /// the schema-defined property columns whose name appears in
    /// `touched_keys`. If any of the entry's properties are NOT in the
    /// label schema, `overflow_json` is regenerated with all overflow
    /// properties present in the entry (the JSONB blob is one column;
    /// it must be rewritten in full because we can't merge JSON
    /// fragments at the storage layer).
    ///
    /// Used by Round-12 §A: edge SETs route through this builder so the
    /// per-edge-type delta tables receive only the touched schema
    /// columns; untouched columns retain their previous-version value
    /// via Lance's MVCC `WhenMatched::UpdateAll` semantics.
    pub fn build_partial_record_batch(
        &self,
        entries: &[L1Entry],
        touched_keys: &std::collections::HashSet<String>,
        schema: &Schema,
    ) -> Result<RecordBatch> {
        // Source schema: src_vid, dst_vid, eid, op, _version, _updated_at,
        // touched schema cols, overflow_json (when any overflow key was
        // touched).
        let mut fields: Vec<Field> = vec![
            Field::new("src_vid", arrow_schema::DataType::UInt64, false),
            Field::new("dst_vid", arrow_schema::DataType::UInt64, false),
            Field::new("eid", arrow_schema::DataType::UInt64, false),
            Field::new("op", arrow_schema::DataType::UInt8, false),
            Field::new("_version", arrow_schema::DataType::UInt64, false),
            Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ];

        let type_props = schema.properties.get(&self.edge_type);
        let mut sorted_touched_props: Vec<(&String, &uni_common::core::schema::PropertyMeta)> =
            if let Some(tp) = type_props {
                tp.iter()
                    .filter(|(name, _)| touched_keys.contains(*name))
                    .collect()
            } else {
                Vec::new()
            };
        sorted_touched_props.sort_by_key(|(name, _)| *name);

        for (name, meta) in &sorted_touched_props {
            fields.push(Field::new(*name, meta.r#type.to_arrow(), meta.nullable));
        }

        // Determine if any touched key is a non-schema (overflow) prop —
        // if so, regenerate overflow_json.
        let schema_prop_names: std::collections::HashSet<&String> =
            type_props.map(|tp| tp.keys().collect()).unwrap_or_default();
        let any_overflow_touched = touched_keys.iter().any(|k| !schema_prop_names.contains(k));
        if any_overflow_touched {
            fields.push(Field::new(
                "overflow_json",
                arrow_schema::DataType::LargeBinary,
                true,
            ));
        }

        let arrow_schema = Arc::new(ArrowSchema::new(fields));

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        let src_vids: Vec<u64> = entries.iter().map(|e| e.src_vid.as_u64()).collect();
        let dst_vids: Vec<u64> = entries.iter().map(|e| e.dst_vid.as_u64()).collect();
        let eids: Vec<u64> = entries.iter().map(|e| e.eid.as_u64()).collect();
        let ops: Vec<u8> = entries.iter().map(|e| e.op as u8).collect();
        let versions: Vec<u64> = entries.iter().map(|e| e.version).collect();
        columns.push(Arc::new(UInt64Array::from(src_vids)));
        columns.push(Arc::new(UInt64Array::from(dst_vids)));
        columns.push(Arc::new(UInt64Array::from(eids)));
        columns.push(Arc::new(UInt8Array::from(ops)));
        columns.push(Arc::new(UInt64Array::from(versions)));
        columns.push(build_timestamp_column(entries.iter().map(|e| e.updated_at)));

        let default_deleted = vec![false; entries.len()];
        for (name, meta) in &sorted_touched_props {
            let extractor =
                crate::storage::arrow_convert::PropertyExtractor::new(name, &meta.r#type);
            let col = extractor.build_column(entries.len(), &default_deleted, |i| {
                entries[i].properties.get(*name)
            })?;
            columns.push(col);
        }

        if any_overflow_touched {
            let overflow_column = crate::storage::property_builder::build_overflow_json_column(
                entries.len(),
                &self.edge_type,
                schema,
                |i| &entries[i].properties,
                &[],
            )?;
            columns.push(overflow_column);
        }

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// MergeInsert a partial-column batch via Lance. Join key is `eid`.
    /// Matched rows have `WhenMatched::UpdateAll` applied; unmatched
    /// source rows are dropped (a partial SET can only update an edge
    /// that was previously CREATEd — the full-row Append path landed
    /// the original row).
    pub async fn merge_insert_partial_run(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
    ) -> Result<()> {
        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);
        crate::storage::manager::merge_insert_batch_with_lance_conflict_retry(
            backend,
            &table_name,
            batch,
            &["eid"],
        )
        .await
    }

    /// Ensure a BTree index exists on the 'eid' column.
    pub async fn ensure_eid_index(&self, backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);
        let indices = backend.list_indexes(&table_name).await?;

        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"eid".to_string()))
        {
            log::info!(
                "Creating eid BTree index for edge type '{}'",
                self.edge_type
            );
            if let Err(e) = backend
                .create_scalar_index(&table_name, &["eid"], ScalarIndexType::BTree, None)
                .await
            {
                crate::storage::record_default_index_failure(&table_name, "eid", &e);
            }
        }

        Ok(())
    }

    /// Get the table name for this delta dataset.
    pub fn table_name(&self) -> String {
        table_names::delta_table_name(&self.edge_type, &self.direction)
    }

    /// Scan all entries from the backend table.
    ///
    /// Returns an empty vector if the table doesn't exist.
    pub async fn scan_all_backend(
        &self,
        backend: &dyn StorageBackend,
        schema: &Schema,
    ) -> Result<Vec<L1Entry>> {
        self.scan_all_backend_with_limit(backend, schema, DEFAULT_MAX_COMPACTION_ROWS)
            .await
    }

    /// Scan all entries from the backend table with a configurable row limit to prevent OOM.
    pub async fn scan_all_backend_with_limit(
        &self,
        backend: &dyn StorageBackend,
        schema: &Schema,
        max_rows: usize,
    ) -> Result<Vec<L1Entry>> {
        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(vec![]);
        }

        let row_count = backend.count_rows(&table_name, None).await?;
        check_oom_guard(row_count, max_rows, &self.edge_type, &self.direction)?;

        info!(
            edge_type = %self.edge_type,
            direction = %self.direction,
            row_count,
            estimated_bytes = row_count * ENTRY_SIZE_ESTIMATE,
            "Starting delta scan for compaction (backend)"
        );

        let batches = backend.scan(ScanRequest::all(&table_name)).await?;

        let mut entries = Vec::new();
        for batch in batches {
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        self.sort_entries(&mut entries);

        Ok(entries)
    }

    /// Replace the delta table with a new batch (atomic replacement).
    ///
    /// This is used during compaction to clear the delta table after merging into L2.
    pub async fn replace(&self, backend: &dyn StorageBackend, batch: RecordBatch) -> Result<()> {
        let table_name = self.table_name();
        let arrow_schema = batch.schema();
        backend
            .replace_table_atomic(&table_name, vec![batch], arrow_schema)
            .await
    }

    /// Delete every delta row at or below a version high-water-mark.
    ///
    /// Compaction uses this to clear ONLY the deltas it actually merged into L2
    /// — the ones present at read time, whose max `_version` is `hwm`. Unlike a
    /// full table wipe ([`Self::replace`] with an empty batch), this preserves rows a
    /// concurrent flush appended after the compaction read them: those carry a
    /// strictly higher `_version` (flush versions are monotonic and a flush's
    /// deltas land atomically), so `_version <= hwm` never matches them. This is
    /// what makes the clear safe without depending on an instantaneous
    /// `flush_in_progress` check. (review H11)
    pub async fn delete_up_to_version(&self, backend: &dyn StorageBackend, hwm: u64) -> Result<()> {
        let table_name = self.table_name();
        if !backend.table_exists(&table_name).await? {
            return Ok(());
        }
        backend
            .delete_rows(&table_name, &FilterExpr::version_at_most(hwm))
            .await
    }

    /// Read delta entries for a specific vertex ID.
    ///
    /// Returns an empty vector if the table doesn't exist or no entries match.
    pub async fn read_deltas(
        &self,
        backend: &dyn StorageBackend,
        vid: Vid,
        schema: &Schema,
        version_hwm: Option<u64>,
    ) -> Result<Vec<L1Entry>> {
        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(vec![]);
        }

        let base_filter = FilterExpr::equals(self.filter_column(), Scalar::UInt(vid.as_u64()));

        // Add version filtering if snapshot is active
        let final_filter = match version_hwm {
            Some(hwm) => FilterExpr::all([base_filter, FilterExpr::version_at_most(hwm)]),
            None => base_filter,
        };

        let batches = backend
            .scan(ScanRequest::all(&table_name).with_filter(final_filter))
            .await?;

        let mut entries = Vec::new();
        for batch in batches {
            let mut batch_entries = self.parse_batch(&batch, schema)?;
            entries.append(&mut batch_entries);
        }

        Ok(entries)
    }

    /// Read delta entries for multiple vertex IDs in a single batch query.
    ///
    /// Returns a HashMap mapping each vid to its delta entries.
    /// VIDs with no delta entries will not be in the map.
    pub async fn read_deltas_batch(
        &self,
        backend: &dyn StorageBackend,
        vids: &[Vid],
        schema: &Schema,
        version_hwm: Option<u64>,
    ) -> Result<HashMap<Vid, Vec<L1Entry>>> {
        if vids.is_empty() {
            return Ok(HashMap::new());
        }

        let table_name = table_names::delta_table_name(&self.edge_type, &self.direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(HashMap::new());
        }

        let mut filter = FilterExpr::one_of(
            self.filter_column(),
            vids.iter().map(|v| Scalar::UInt(v.as_u64())),
        );

        // Add version filtering if snapshot is active
        if let Some(hwm) = version_hwm {
            filter = FilterExpr::all([filter, FilterExpr::version_at_most(hwm)]);
        }

        let batches = backend
            .scan(ScanRequest::all(&table_name).with_filter(filter))
            .await?;

        // Parse all batches and group by direction key VID
        let is_fwd = self.direction == "fwd";
        let mut result: HashMap<Vid, Vec<L1Entry>> = HashMap::new();
        for batch in batches {
            let entries = self.parse_batch(&batch, schema)?;
            for entry in entries {
                let vid = if is_fwd { entry.src_vid } else { entry.dst_vid };
                result.entry(vid).or_default().push(entry);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(
        clippy::assertions_on_constants,
        reason = "Validating configuration constants intentionally"
    )]
    fn test_constants_are_reasonable() {
        // Verify DEFAULT_MAX_COMPACTION_ROWS is set to 5 million
        assert_eq!(DEFAULT_MAX_COMPACTION_ROWS, 5_000_000);

        // Verify ENTRY_SIZE_ESTIMATE is reasonable (should be between 100-300 bytes)
        assert!(ENTRY_SIZE_ESTIMATE >= 100, "Entry size estimate too low");
        assert!(ENTRY_SIZE_ESTIMATE <= 300, "Entry size estimate too high");

        // Verify that 5M entries at the estimated size fits in reasonable memory
        let estimated_gb =
            (DEFAULT_MAX_COMPACTION_ROWS * ENTRY_SIZE_ESTIMATE) as f64 / (1024.0 * 1024.0 * 1024.0);
        assert!(
            estimated_gb < 1.0,
            "5M entries should fit in under 1GB with current estimate"
        );
    }

    #[test]
    fn test_memory_estimate_formatting() {
        // Test that our GB formatting works correctly
        let row_count = 10_000_000;
        let estimated_bytes = row_count * ENTRY_SIZE_ESTIMATE;
        let estimated_gb = estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        // Should be around 1.35 GB for 10M rows
        assert!(
            estimated_gb > 1.0 && estimated_gb < 2.0,
            "10M rows should be 1-2 GB"
        );
    }

    #[test]
    fn test_check_oom_guard_below_limit() {
        let result = check_oom_guard(1_000_000, 5_000_000, "KNOWS", "fwd");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_oom_guard_at_limit() {
        let result = check_oom_guard(5_000_000, 5_000_000, "KNOWS", "fwd");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_oom_guard_above_limit() {
        let result = check_oom_guard(5_000_001, 5_000_000, "KNOWS", "fwd");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("KNOWS_fwd"), "Error should name the entity");
        assert!(msg.contains("5000001"), "Error should state the row count");
        assert!(msg.contains("GB"), "Error should show GB estimate");
        assert!(msg.contains("issue #143"), "Error should reference issue");
    }

    #[test]
    fn test_op_values() {
        assert_eq!(Op::Insert as u8, 0);
        assert_eq!(Op::Delete as u8, 1);
    }

    fn entry(eid: u64, version: u64) -> L1Entry {
        L1Entry {
            src_vid: Vid::new(1),
            dst_vid: Vid::new(2),
            eid: Eid::new(eid),
            op: Op::Insert,
            version,
            properties: Properties::new(),
            created_at: None,
            updated_at: None,
        }
    }

    /// H11: compaction must clear ONLY the deltas it read (those at or below the
    /// high-water-mark), so a delta a concurrent flush appended with a higher
    /// `_version` after the read survives instead of being wiped by a full
    /// table replace.
    #[tokio::test]
    async fn delete_up_to_version_preserves_newer_deltas() -> Result<()> {
        use crate::backend::lance::LanceDbBackend;

        let dir = tempfile::tempdir()?;
        let uri = dir.path().to_str().unwrap();
        let be = LanceDbBackend::connect(uri, None).await?;
        let backend: &dyn StorageBackend = &be;
        let schema = Schema::default();
        let ds = DeltaDataset::new(uri, "KNOWS", "fwd");

        // Compaction-visible deltas at versions 1 and 2 (hwm = 2).
        let merged = ds.build_record_batch(&[entry(10, 1), entry(11, 2)], &schema)?;
        ds.write_run(backend, merged).await?;

        // A concurrent flush appends a NEWER delta (version 3) after the read.
        let newer = ds.build_record_batch(&[entry(12, 3)], &schema)?;
        ds.write_run(backend, newer).await?;

        // Clear what compaction merged (hwm = 2). The version-3 row must remain.
        ds.delete_up_to_version(backend, 2).await?;

        let remaining = ds.scan_all_backend(backend, &schema).await?;
        assert_eq!(
            remaining.len(),
            1,
            "only the concurrently-appended version-3 delta should remain"
        );
        assert_eq!(remaining[0].eid, Eid::new(12));
        assert_eq!(remaining[0].version, 3);
        Ok(())
    }
}
