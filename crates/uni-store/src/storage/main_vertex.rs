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
//! - `props_json`: All properties as JSON blob
//! - `_deleted`: Soft-delete flag
//! - `_version`: MVCC version
//! - `_created_at`: Creation timestamp
//! - `_updated_at`: Update timestamp

use crate::lancedb::LanceDbStore;
use crate::storage::arrow_convert::build_timestamp_column_from_vid_map;
use anyhow::{Result, anyhow};
use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use lancedb::Table;
use lancedb::index::Index as LanceDbIndex;
use lancedb::index::scalar::BTreeIndexBuilder;
use lancedb::query::{ExecutableQuery, QueryBase};
use sha3::{Digest, Sha3_256};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{UniId, Vid};

/// Main vertex dataset for the unified `vertices` table.
///
/// This table contains all vertices regardless of label, providing:
/// - Fast ID-based lookups without knowing the label
/// - Global ext_id uniqueness enforcement
/// - Multi-label storage with labels as a list column
pub struct MainVertexDataset {
    _base_uri: String,
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
            Field::new("props_json", DataType::Utf8, true),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new(
                "_created_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "_updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]))
    }

    /// Get the table name for the main vertices table.
    pub fn table_name() -> &'static str {
        "vertices"
    }

    /// Open the main vertices table.
    ///
    /// Returns the LanceDB table handle for querying vertices.
    pub async fn open_table(store: &LanceDbStore) -> Result<Table> {
        store
            .open_table(Self::table_name())
            .await
            .map_err(|e| anyhow!("Failed to open main vertices table: {}", e))
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
    /// * `created_at` - Optional map of Vid -> microseconds since epoch
    /// * `updated_at` - Optional map of Vid -> microseconds since epoch
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

        // props_json column
        let mut props_json_builder = StringBuilder::new();
        for (_, _, props, _, _) in vertices.iter() {
            let json = serde_json::to_string(props).unwrap_or_else(|_| "{}".to_string());
            props_json_builder.append_value(&json);
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
    pub async fn write_batch_lancedb(store: &LanceDbStore, batch: RecordBatch) -> Result<Table> {
        let table_name = Self::table_name();

        if store.table_exists(table_name).await? {
            let table = store.open_table(table_name).await?;
            store.append_to_table(&table, vec![batch]).await?;
            Ok(table)
        } else {
            store.create_table(table_name, vec![batch]).await
        }
    }

    /// Ensure default indexes exist on the main vertices table.
    pub async fn ensure_default_indexes_lancedb(table: &Table) -> Result<()> {
        let indices = table
            .list_indices()
            .await
            .map_err(|e| anyhow!("Failed to list indices: {}", e))?;

        // Ensure _vid index (primary key)
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"_vid".to_string()))
        {
            log::info!("Creating _vid BTree index for main vertices table");
            if let Err(e) = table
                .create_index(&["_vid"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!("Failed to create _vid index for main vertices: {}", e);
            }
        }

        // Ensure ext_id index (unique lookup)
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"ext_id".to_string()))
        {
            log::info!("Creating ext_id BTree index for main vertices table");
            if let Err(e) = table
                .create_index(
                    &["ext_id"],
                    LanceDbIndex::BTree(BTreeIndexBuilder::default()),
                )
                .execute()
                .await
            {
                log::warn!("Failed to create ext_id index for main vertices: {}", e);
            }
        }

        // Ensure _uid index
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"_uid".to_string()))
        {
            log::info!("Creating _uid BTree index for main vertices table");
            if let Err(e) = table
                .create_index(&["_uid"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!("Failed to create _uid index for main vertices: {}", e);
            }
        }

        Ok(())
    }

    /// Query the main vertices table for a vertex by ext_id.
    ///
    /// Returns the Vid if found, None otherwise.
    pub async fn find_by_ext_id(store: &LanceDbStore, ext_id: &str) -> Result<Option<Vid>> {
        let table_name = Self::table_name();

        if !store.table_exists(table_name).await? {
            return Ok(None);
        }

        let table = store.open_table(table_name).await?;
        let query = format!("ext_id = '{}'", ext_id.replace('\'', "''"));

        let batches = table
            .query()
            .only_if(query)
            .select(lancedb::query::Select::Columns(vec!["_vid".to_string()]))
            .execute()
            .await
            .map_err(|e| anyhow!("Query failed: {}", e))?;

        use futures::TryStreamExt;
        let results: Vec<RecordBatch> = batches.try_collect().await?;

        for batch in results {
            if batch.num_rows() > 0
                && let Some(vid_col) = batch.column_by_name("_vid")
                && let Some(vid_arr) = vid_col.as_any().downcast_ref::<UInt64Array>()
            {
                return Ok(Some(Vid::from(vid_arr.value(0))));
            }
        }

        Ok(None)
    }

    /// Check if an ext_id already exists in the main vertices table.
    pub async fn ext_id_exists(store: &LanceDbStore, ext_id: &str) -> Result<bool> {
        Ok(Self::find_by_ext_id(store, ext_id).await?.is_some())
    }

    /// Find labels for a vertex by VID in the main vertices table.
    ///
    /// Returns the list of labels if found, None otherwise.
    pub async fn find_labels_by_vid(store: &LanceDbStore, vid: Vid) -> Result<Option<Vec<String>>> {
        let table_name = Self::table_name();

        if !store.table_exists(table_name).await? {
            return Ok(None);
        }

        let table = store.open_table(table_name).await?;
        let query = format!("_vid = {}", vid.as_u64());

        let batches = table
            .query()
            .only_if(query)
            .select(lancedb::query::Select::Columns(vec!["labels".to_string()]))
            .execute()
            .await
            .map_err(|e| anyhow!("Query failed: {}", e))?;

        use futures::TryStreamExt;
        let results: Vec<RecordBatch> = batches.try_collect().await?;

        for batch in results {
            if batch.num_rows() > 0
                && let Some(labels_col) = batch.column_by_name("labels")
                && let Some(list_arr) = labels_col.as_any().downcast_ref::<arrow_array::ListArray>()
            {
                // Labels is a List<Utf8> column
                let values = list_arr.value(0);
                if let Some(str_arr) = values.as_any().downcast_ref::<arrow_array::StringArray>() {
                    let labels: Vec<String> = (0..str_arr.len())
                        .filter_map(|i| {
                            if str_arr.is_null(i) {
                                None
                            } else {
                                Some(str_arr.value(i).to_string())
                            }
                        })
                        .collect();
                    return Ok(Some(labels));
                }
            }
        }

        Ok(None)
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
        let mut props = HashMap::new();
        props.insert("name".to_string(), serde_json::json!("Alice"));
        props.insert("ext_id".to_string(), serde_json::json!("user_001"));

        let vertices = vec![(Vid::new(1), vec!["Person".to_string()], props, false, 1u64)];

        let batch = MainVertexDataset::build_record_batch(&vertices, None, None).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);

        // Check ext_id was extracted
        let ext_id_col = batch.column_by_name("ext_id").unwrap();
        let ext_id_arr = ext_id_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(ext_id_arr.value(0), "user_001");
    }
}
