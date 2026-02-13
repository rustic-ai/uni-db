// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::lancedb::LanceDbStore;
use crate::storage::arrow_convert::build_timestamp_column_from_vid_map;
use crate::storage::property_builder::PropertyColumnBuilder;
use anyhow::{Result, anyhow};
use arrow_array::builder::{FixedSizeBinaryBuilder, StringBuilder};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema, TimeUnit};
use lance::dataset::Dataset;
use lancedb::Table;
use lancedb::index::Index as LanceDbIndex;
use lancedb::index::scalar::BTreeIndexBuilder;
use sha3::{Digest, Sha3_256};
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{UniId, Vid};
use uni_common::core::schema::Schema;

pub struct VertexDataset {
    uri: String,
    label: String,
    _label_id: u16,
}

impl VertexDataset {
    pub fn new(base_uri: &str, label: &str, label_id: u16) -> Self {
        let uri = format!("{}/vertices/{}", base_uri, label);
        Self {
            uri,
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

    pub async fn open(&self) -> Result<Arc<Dataset>> {
        self.open_at(None).await
    }

    pub async fn open_at(&self, version: Option<u64>) -> Result<Arc<Dataset>> {
        let mut ds = Dataset::open(&self.uri).await?;
        if let Some(v) = version {
            ds = ds.checkout_version(v).await?;
        }
        Ok(Arc::new(ds))
    }

    pub async fn open_raw(&self) -> Result<Dataset> {
        let ds = Dataset::open(&self.uri).await?;
        Ok(ds)
    }

    /// Build a record batch from vertices with optional timestamp metadata.
    ///
    /// If timestamps are not provided, they default to None (null).
    pub fn build_record_batch(
        &self,
        vertices: &[(Vid, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
    ) -> Result<RecordBatch> {
        self.build_record_batch_with_timestamps(vertices, deleted, versions, schema, None, None)
    }

    /// Build a record batch with explicit timestamp metadata.
    ///
    /// # Arguments
    /// * `vertices` - Vertex ID and properties pairs
    /// * `deleted` - Deletion flags per vertex
    /// * `versions` - Version numbers per vertex
    /// * `schema` - Database schema
    /// * `created_at` - Optional map of Vid -> microseconds since epoch
    /// * `updated_at` - Optional map of Vid -> microseconds since epoch
    pub fn build_record_batch_with_timestamps(
        &self,
        vertices: &[(Vid, Properties)],
        deleted: &[bool],
        versions: &[u64],
        schema: &Schema,
        created_at: Option<&std::collections::HashMap<uni_common::core::id::Vid, i64>>,
        updated_at: Option<&std::collections::HashMap<uni_common::core::id::Vid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = self.get_arrow_schema(schema)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        let vids: Vec<u64> = vertices.iter().map(|(v, _)| v.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(vids)));

        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for (_vid, props) in vertices.iter() {
            let ext_id = props.get("ext_id").and_then(|v| v.as_str());
            let uid = Self::compute_vertex_uid(&self.label, ext_id, props);
            uid_builder.append_value(uid.as_bytes())?;
        }
        columns.push(Arc::new(uid_builder.finish()));

        columns.push(Arc::new(BooleanArray::from(deleted.to_vec())));
        columns.push(Arc::new(UInt64Array::from(versions.to_vec())));

        // Build ext_id column (extracted from properties as dedicated column)
        let mut ext_id_builder = StringBuilder::new();
        for (_vid, props) in vertices.iter() {
            if let Some(ext_id_val) = props.get("ext_id").and_then(|v| v.as_str()) {
                ext_id_builder.append_value(ext_id_val);
            } else {
                ext_id_builder.append_null();
            }
        }
        columns.push(Arc::new(ext_id_builder.finish()));

        // Build _created_at and _updated_at columns using shared builder
        let vids = vertices.iter().map(|(v, _)| *v);
        columns.push(build_timestamp_column_from_vid_map(
            vids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_vid_map(vids, updated_at));

        // Build property columns using shared builder
        let prop_columns = PropertyColumnBuilder::new(schema, &self.label, vertices.len())
            .with_deleted(deleted)
            .build(|i| &vertices[i].1)?;

        columns.extend(prop_columns);

        // Build overflow_json column for non-schema properties
        let overflow_column = self.build_overflow_json_column(vertices, schema)?;
        columns.push(overflow_column);

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Build the overflow_json column containing properties not in schema.
    ///
    /// This method identifies properties that are not defined in the label's schema
    /// and serializes them as a JSON blob. Properties in schema are stored as typed
    /// columns, while overflow properties are stored in this JSON column.
    fn build_overflow_json_column(
        &self,
        vertices: &[(Vid, Properties)],
        schema: &Schema,
    ) -> Result<ArrayRef> {
        use arrow_array::builder::LargeBinaryBuilder;
        use std::collections::HashMap;

        let schema_props = schema.properties.get(&self.label);
        let mut builder = LargeBinaryBuilder::new();

        for (_vid, props) in vertices {
            let mut overflow_props = HashMap::new();

            // Collect non-schema properties (skip ext_id, it's a system column)
            for (key, value) in props {
                if key == "ext_id" {
                    continue;
                }
                if !schema_props.is_some_and(|sp| sp.contains_key(key)) {
                    overflow_props.insert(key.clone(), value.clone());
                }
            }

            // Serialize to JSONB binary (or null if no overflow)
            if overflow_props.is_empty() {
                builder.append_null();
            } else {
                let jsonb = {
                    let json_val = serde_json::to_value(&overflow_props)
                        .map_err(|e| anyhow!("Failed to serialize overflow properties: {}", e))?;
                    let uni_val: uni_common::Value = json_val.into();
                    uni_common::cypher_value_codec::encode(&uni_val)
                };
                builder.append_value(&jsonb);
            }
        }

        Ok(Arc::new(builder.finish()))
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
                "_created_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
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
    // LanceDB-based Methods
    // ========================================================================

    /// Open a vertex table using LanceDB.
    ///
    /// This is the preferred method for new code as it enables DataFusion queries.
    pub async fn open_lancedb(&self, store: &LanceDbStore) -> Result<Table> {
        store.open_vertex_table(&self.label).await
    }

    /// Open or create a vertex table using LanceDB.
    pub async fn open_or_create_lancedb(
        &self,
        store: &LanceDbStore,
        schema: &Schema,
    ) -> Result<Table> {
        let arrow_schema = self.get_arrow_schema(schema)?;
        store
            .open_or_create_vertex_table(&self.label, arrow_schema)
            .await
    }

    /// Write a batch to a LanceDB vertex table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    pub async fn write_batch_lancedb(
        &self,
        store: &LanceDbStore,
        batch: RecordBatch,
        _schema: &Schema,
    ) -> Result<Table> {
        let table_name = LanceDbStore::vertex_table_name(&self.label);

        if store.table_exists(&table_name).await? {
            let table = store.open_table(&table_name).await?;
            store.append_to_table(&table, vec![batch]).await?;
            Ok(table)
        } else {
            store.create_table(&table_name, vec![batch]).await
        }
    }

    /// Ensure default scalar indexes exist on system columns (_vid, _uid, ext_id) using LanceDB.
    ///
    /// LanceDB uses BTree indexes for scalar columns.
    pub async fn ensure_default_indexes_lancedb(&self, table: &Table) -> Result<()> {
        let indices = table
            .list_indices()
            .await
            .map_err(|e| anyhow!("Failed to list indices: {}", e))?;

        // Ensure _vid index
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"_vid".to_string()))
        {
            log::info!(
                "Creating _vid BTree index for label '{}' via LanceDB",
                self.label
            );
            if let Err(e) = table
                .create_index(&["_vid"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!(
                    "Failed to create _vid index for '{}' via LanceDB: {}",
                    self.label,
                    e
                );
            }
        }

        // Ensure _uid index
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"_uid".to_string()))
        {
            log::info!(
                "Creating _uid BTree index for label '{}' via LanceDB",
                self.label
            );
            if let Err(e) = table
                .create_index(&["_uid"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!(
                    "Failed to create _uid index for '{}' via LanceDB: {}",
                    self.label,
                    e
                );
            }
        }

        // Ensure ext_id index for fast external ID lookups
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"ext_id".to_string()))
        {
            log::info!(
                "Creating ext_id BTree index for label '{}' via LanceDB",
                self.label
            );
            if let Err(e) = table
                .create_index(
                    &["ext_id"],
                    LanceDbIndex::BTree(BTreeIndexBuilder::default()),
                )
                .execute()
                .await
            {
                log::warn!(
                    "Failed to create ext_id index for '{}' via LanceDB: {}",
                    self.label,
                    e
                );
            }
        }

        Ok(())
    }

    /// Get the LanceDB table name for this vertex dataset.
    pub fn lancedb_table_name(&self) -> String {
        LanceDbStore::vertex_table_name(&self.label)
    }

    /// Replace a vertex table's contents using LanceDB.
    ///
    /// This drops the existing table and creates a new one with the provided
    /// data. Used by compaction to rewrite the table with merged data.
    pub async fn replace_lancedb(
        &self,
        store: &LanceDbStore,
        batch: RecordBatch,
        schema: &Schema,
    ) -> Result<Table> {
        let table_name = self.lancedb_table_name();
        let arrow_schema = self.get_arrow_schema(schema)?;
        store
            .replace_table_or_empty(&table_name, vec![batch], arrow_schema)
            .await
    }
}
