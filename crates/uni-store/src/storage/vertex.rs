// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::ScalarIndexType;
use crate::storage::arrow_convert::build_timestamp_column_from_vid_map;
use crate::storage::property_builder::PropertyColumnBuilder;
use anyhow::{Result, anyhow};
use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema, TimeUnit};
use sha3::{Digest, Sha3_256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{UniId, Vid};
use uni_common::core::schema::Schema;

/// A per-label vertex dataset handle.
///
/// Builds the Arrow record batches for a label's vertex table and derives
/// vertex identity. The on-disk data is read and written exclusively through
/// the [`StorageBackend`]; this type no longer
/// opens a raw `lance::Dataset`, so the on-disk path lives in exactly one place
/// (the backend) and cannot drift.
pub struct VertexDataset {
    label: String,
    #[cfg_attr(not(feature = "lance-backend"), allow(dead_code))]
    _label_id: u16,
}

impl VertexDataset {
    /// Construct a per-label vertex dataset handle.
    ///
    /// `base_uri` is accepted for call-site stability but unused — the on-disk
    /// data is reached through the `StorageBackend`, not a path constructed here.
    pub fn new(_base_uri: &str, label: &str, label_id: u16) -> Self {
        Self {
            label: label.to_string(),
            _label_id: label_id,
        }
    }

    /// Compute UniId from vertex content.
    /// Canonical form: sorted JSON of (label, ext_id, properties)
    pub fn compute_vertex_uid(label: &str, ext_id: Option<&str>, properties: &Properties) -> UniId {
        let mut hasher = Sha3_256::new();

        // Include label
        hasher.update(label.as_bytes());
        hasher.update(b"\x00"); // separator

        // Include ext_id if present
        if let Some(eid) = ext_id {
            hasher.update(eid.as_bytes());
        }
        hasher.update(b"\x00");

        // Include sorted properties for determinism
        let mut sorted_props: Vec<_> = properties.iter().collect();
        sorted_props.sort_by_key(|(k, _)| *k);
        for (key, value) in sorted_props {
            hasher.update(key.as_bytes());
            hasher.update(b"=");
            hasher.update(value.to_string().as_bytes());
            hasher.update(b"\x00");
        }

        let hash: [u8; 32] = hasher.finalize().into();
        UniId::from_bytes(hash)
    }

    /// Build a record batch from vertices with optional timestamp metadata.
    ///
    /// If timestamps are not provided, they default to None (null).
    pub fn build_record_batch(
        &self,
        vertices: &[(Vid, Vec<String>, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
    ) -> Result<RecordBatch> {
        self.build_record_batch_with_timestamps(vertices, deleted, versions, schema, None, None)
    }

    /// Build a record batch with explicit timestamp metadata.
    ///
    /// # Arguments
    /// * `vertices` - Vertex ID, labels, and properties triples
    /// * `deleted` - Deletion flags per vertex
    /// * `versions` - Version numbers per vertex
    /// * `schema` - Database schema
    /// * `created_at` - Optional map of Vid -> nanoseconds since epoch
    /// * `updated_at` - Optional map of Vid -> nanoseconds since epoch
    pub fn build_record_batch_with_timestamps(
        &self,
        vertices: &[(Vid, Vec<String>, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
        created_at: Option<&HashMap<Vid, i64>>,
        updated_at: Option<&HashMap<Vid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        let vids: Vec<u64> = vertices.iter().map(|(v, _, _)| v.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(vids)));

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for (_vid, _labels, props) in vertices.iter() {
            let ext_id = props.get("ext_id").and_then(|v| v.as_str());
            let uid = Self::compute_vertex_uid(&self.label, ext_id, props);
            uid_builder.append_value(uid.as_bytes())?;
        }
        columns.push(Arc::new(uid_builder.finish()));

        columns.push(Arc::new(BooleanArray::from(deleted.to_vec())));
        columns.push(Arc::new(UInt64Array::from(versions.to_vec())));

        // Build ext_id column (extracted from properties as dedicated column)
        let mut ext_id_builder = StringBuilder::new();
        for (_vid, _labels, props) in vertices.iter() {
            if let Some(ext_id_val) = props.get("ext_id").and_then(|v| v.as_str()) {
                ext_id_builder.append_value(ext_id_val);
            } else {
                ext_id_builder.append_null();
            }
        }
        columns.push(Arc::new(ext_id_builder.finish()));

        // Build _labels column (List<Utf8>)
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        for (_vid, labels, _props) in vertices.iter() {
            let values = labels_builder.values();
            for lbl in labels {
                values.append_value(lbl);
            }
            labels_builder.append(true);
        }
        columns.push(Arc::new(labels_builder.finish()));

        // Build _created_at and _updated_at columns using shared builder
        let vids = vertices.iter().map(|(v, _, _)| *v);
        columns.push(build_timestamp_column_from_vid_map(
            vids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_vid_map(vids, updated_at));

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.label, vertices.len())
            .with_deleted(deleted)
            .build(|i| &vertices[i].2)?;

        columns.extend(prop_columns);

        // Build overflow_json column for non-schema properties
        let overflow_column = self.build_overflow_json_column(vertices, schema)?;
        columns.push(overflow_column);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Build the overflow_json column containing properties not in schema.
    fn build_overflow_json_column(
        &self,
        vertices: &[(Vid, Vec<String>, Properties)],
        schema: &Schema,
    ) -> Result<ArrayRef> {
        crate::storage::property_builder::build_overflow_json_column(
            vertices.len(),
            &self.label,
            schema,
            |i| &vertices[i].2,
            &["ext_id"],
        )
    }

    pub fn get_arrow_schema(&self, schema: &Schema) -> Result<Arc<ArrowSchema>> {
        let mut fields = vec![
            Field::new("_vid", arrow_schema::DataType::UInt64, false),
            Field::new("_uid", arrow_schema::DataType::FixedSizeBinary(32), true),
            Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            Field::new("_version", arrow_schema::DataType::UInt64, false),
            // New metadata columns per STORAGE_DESIGN.md
            Field::new("ext_id", arrow_schema::DataType::Utf8, true),
            Field::new(
                "_labels",
                arrow_schema::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow_schema::DataType::Utf8,
                    true,
                ))),
                true,
            ),
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

        if let Some(label_props) = schema.properties.get(&self.label) {
            let mut sorted_props: Vec<_> = label_props.iter().collect();
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

    // ========================================================================
    // Backend-agnostic Methods
    // ========================================================================

    /// Open or create a vertex table via the storage backend.
    pub async fn open_or_create(
        &self,
        backend: &dyn StorageBackend,
        schema: &Schema,
    ) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        let arrow_schema = self.get_arrow_schema(schema)?;
        backend
            .open_or_create_table(&table_name, arrow_schema)
            .await
    }

    /// Write a batch to a vertex table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    /// Race-safe under async-flush — see
    /// `crate::storage::manager::write_batch_with_lance_conflict_retry`.
    pub async fn write_batch(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
        _schema: &Schema,
    ) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        crate::storage::manager::write_batch_with_lance_conflict_retry(backend, &table_name, batch)
            .await
    }

    /// Build a *partial-column* RecordBatch for Lance `MergeInsert`. The
    /// batch includes `_vid` (join key), `_deleted`, `_version`,
    /// `_updated_at`, and ONLY the schema-defined property columns whose
    /// name appears in `touched_keys`. Untouched columns (including
    /// vector embeddings, overflow JSON, ext_id, _labels, _uid,
    /// _created_at) are omitted from the source — Lance leaves their
    /// target values at the previous version.
    pub fn build_partial_record_batch(
        &self,
        vertices: &[(Vid, Properties)],
        versions: &[u64],
        touched_keys: &HashSet<String>,
        schema: &Schema,
        updated_at: Option<&HashMap<Vid, i64>>,
    ) -> Result<RecordBatch> {
        let mut fields: Vec<arrow_schema::Field> = vec![
            arrow_schema::Field::new("_vid", arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            arrow_schema::Field::new("_version", arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ];

        let label_props = schema.properties.get(&self.label);
        let mut sorted_touched_props: Vec<(&String, &uni_common::core::schema::PropertyMeta)> =
            if let Some(lp) = label_props {
                lp.iter()
                    .filter(|(name, _)| touched_keys.contains(*name))
                    .collect()
            } else {
                Vec::new()
            };
        sorted_touched_props.sort_by_key(|(name, _)| *name);

        for (name, meta) in &sorted_touched_props {
            fields.push(arrow_schema::Field::new(
                *name,
                meta.r#type.to_arrow(),
                meta.nullable,
            ));
        }

        let arrow_schema = Arc::new(ArrowSchema::new(fields));

        let vids: Vec<u64> = vertices.iter().map(|(v, _)| v.as_u64()).collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());
        columns.push(Arc::new(UInt64Array::from(vids)));
        columns.push(Arc::new(BooleanArray::from(vec![false; vertices.len()])));
        columns.push(Arc::new(UInt64Array::from(versions.to_vec())));

        let vids_iter = vertices.iter().map(|(v, _)| *v);
        columns.push(build_timestamp_column_from_vid_map(vids_iter, updated_at));

        // Property columns: for each touched, schema-known property,
        // build the column from each row's Properties map. Rows whose
        // map doesn't contain the key get a NULL — Lance treats that
        // as "don't change this column on this row" only if the source
        // schema OMITS the column. Since we're sending a uniform
        // sub-schema across all rows, NULLs in the column do represent
        // an intentional "set to null" assignment for that row.
        //
        // Caller responsibility: a row in the partial batch SHOULD
        // contain all keys the SET touched on that row. If it doesn't,
        // we still emit NULL (semantically a null assignment for that
        // row, which is the SET-to-null Cypher semantic anyway).
        let default_deleted = vec![false; vertices.len()];
        for (name, meta) in &sorted_touched_props {
            let extractor =
                crate::storage::arrow_convert::PropertyExtractor::new(name, &meta.r#type);
            let col = extractor.build_column(vertices.len(), &default_deleted, |i| {
                vertices[i].1.get(*name)
            })?;
            columns.push(col);
        }

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// MergeInsert a partial-column batch via Lance. The source schema
    /// must be a subset of the target's schema. Used by the flush path
    /// when `UniConfig::partial_lance_writes` is on.
    pub async fn merge_insert_batch(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
    ) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        crate::storage::manager::merge_insert_batch_with_lance_conflict_retry(
            backend,
            &table_name,
            batch,
            &["_vid"],
        )
        .await
    }

    /// MergeInsert a tombstone batch into this label's table.
    ///
    /// Separate from [`Self::merge_insert_batch`] because the two callers
    /// differ in what a missing table means. That one serves partial SETs,
    /// where the row must already exist, so a missing table is a fault. This
    /// one serves tombstones, where an unmatched row is a no-op — and a missing
    /// table is the degenerate case of that (#182): a label whose vertices were
    /// created and deleted inside one flush window never had its table written.
    pub async fn merge_insert_tombstone_batch(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
    ) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        crate::storage::manager::merge_insert_batch_tolerating_missing_table(
            backend,
            &table_name,
            batch,
            &["_vid"],
        )
        .await
    }

    /// Build a partial-column RecordBatch marking VIDs as deleted. Used
    /// by the per-label DELETE flush path to skip the wide-row tombstone
    /// Append. Schema mirrors
    /// `MainVertexDataset::build_tombstone_partial_batch`.
    pub fn build_tombstone_partial_batch(
        &self,
        tombstones: &[(Vid, u64)],
        updated_at: Option<&HashMap<Vid, i64>>,
    ) -> Result<RecordBatch> {
        let fields = vec![
            arrow_schema::Field::new("_vid", arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new("_deleted", arrow_schema::DataType::Boolean, false),
            arrow_schema::Field::new("_version", arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
        ];
        let arrow_schema = Arc::new(ArrowSchema::new(fields));

        let vids: Vec<u64> = tombstones.iter().map(|(v, _)| v.as_u64()).collect();
        let deleted: Vec<bool> = vec![true; tombstones.len()];
        let versions: Vec<u64> = tombstones.iter().map(|(_, v)| *v).collect();
        let vids_iter = tombstones.iter().map(|(v, _)| *v);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vids)),
            Arc::new(BooleanArray::from(deleted)),
            Arc::new(UInt64Array::from(versions)),
            build_timestamp_column_from_vid_map(vids_iter, updated_at),
        ];

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Ensure default scalar indexes exist on system columns (_vid, _uid, ext_id).
    pub async fn ensure_default_indexes(&self, backend: &dyn StorageBackend) -> Result<()> {
        let table_name = table_names::vertex_table_name(&self.label);
        // Nothing to index on a table that does not exist (#182). Reachable on
        // a tombstone-only flush for this label, where the full-row write that
        // would have created it was skipped.
        if !backend.table_exists(&table_name).await? {
            return Ok(());
        }
        let indices = backend.list_indexes(&table_name).await?;

        let has_index = |col: &str| {
            indices
                .iter()
                .any(|idx| idx.columns.contains(&col.to_string()))
        };

        for column in &["_vid", "_uid", "ext_id"] {
            if has_index(column) {
                continue;
            }
            log::info!("Creating {} BTree index for label '{}'", column, self.label);
            if let Err(e) = backend
                .create_scalar_index(&table_name, &[*column], ScalarIndexType::BTree, None)
                .await
            {
                log::warn!(
                    "Failed to create {} index for '{}': {}",
                    column,
                    self.label,
                    e
                );
            }
        }

        Ok(())
    }

    /// Get the table name for this vertex dataset.
    pub fn table_name(&self) -> String {
        table_names::vertex_table_name(&self.label)
    }

    /// Replace a vertex table's contents atomically.
    ///
    /// Used by compaction to rewrite the table with merged data.
    pub async fn replace(
        &self,
        backend: &dyn StorageBackend,
        batch: RecordBatch,
        schema: &Schema,
    ) -> Result<()> {
        let table_name = self.table_name();
        let arrow_schema = self.get_arrow_schema(schema)?;
        backend
            .replace_table_atomic(&table_name, vec![batch], arrow_schema)
            .await
    }
}
