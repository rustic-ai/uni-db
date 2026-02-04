// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Main edge table for unified edge storage.
//!
//! This module implements the main `edges` table as described in STORAGE_DESIGN.md.
//! The main table contains all edges in the graph with:
//! - `_eid`: Internal edge ID (primary key)
//! - `src_vid`: Source vertex ID
//! - `dst_vid`: Destination vertex ID
//! - `type`: Edge type name
//! - `props_json`: All properties as JSON blob
//! - `_deleted`: Soft-delete flag
//! - `_version`: MVCC version
//! - `_created_at`: Creation timestamp
//! - `_updated_at`: Update timestamp

use crate::lancedb::LanceDbStore;
use crate::storage::arrow_convert::build_timestamp_column_from_eid_map;
use anyhow::{Result, anyhow};
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use futures::TryStreamExt;
use lancedb::Table;
use lancedb::index::Index as LanceDbIndex;
use lancedb::index::scalar::BTreeIndexBuilder;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};

/// Main edge dataset for the unified `edges` table.
///
/// This table contains all edges regardless of type, providing:
/// - Fast ID-based lookups without knowing the edge type
/// - Unified traversal queries
pub struct MainEdgeDataset {
    _base_uri: String,
}

impl MainEdgeDataset {
    /// Create a new MainEdgeDataset.
    pub fn new(base_uri: &str) -> Self {
        Self {
            _base_uri: base_uri.to_string(),
        }
    }

    /// Get the Arrow schema for the main edges table.
    pub fn get_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("_eid", DataType::UInt64, false),
            Field::new("src_vid", DataType::UInt64, false),
            Field::new("dst_vid", DataType::UInt64, false),
            Field::new("type", DataType::Utf8, false),
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

    /// Get the table name for the main edges table.
    pub fn table_name() -> &'static str {
        "edges"
    }

    /// Build a record batch for the main edges table.
    ///
    /// # Arguments
    /// * `edges` - List of (eid, src_vid, dst_vid, edge_type, properties, deleted, version) tuples
    /// * `created_at` - Optional map of Eid -> microseconds since epoch
    /// * `updated_at` - Optional map of Eid -> microseconds since epoch
    pub fn build_record_batch(
        edges: &[(Eid, Vid, Vid, String, Properties, bool, u64)],
        created_at: Option<&HashMap<Eid, i64>>,
        updated_at: Option<&HashMap<Eid, i64>>,
    ) -> Result<RecordBatch> {
        let arrow_schema = Self::get_arrow_schema();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        // _eid column
        let eids: Vec<u64> = edges
            .iter()
            .map(|(e, _, _, _, _, _, _)| e.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(eids)));

        // src_vid column
        let src_vids: Vec<u64> = edges
            .iter()
            .map(|(_, s, _, _, _, _, _)| s.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(src_vids)));

        // dst_vid column
        let dst_vids: Vec<u64> = edges
            .iter()
            .map(|(_, _, d, _, _, _, _)| d.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(dst_vids)));

        // type column
        let mut type_builder = StringBuilder::new();
        for (_, _, _, edge_type, _, _, _) in edges.iter() {
            type_builder.append_value(edge_type);
        }
        columns.push(Arc::new(type_builder.finish()));

        // props_json column
        let mut props_json_builder = StringBuilder::new();
        for (_, _, _, _, props, _, _) in edges.iter() {
            let json = serde_json::to_string(props).unwrap_or_else(|_| "{}".to_string());
            props_json_builder.append_value(&json);
        }
        columns.push(Arc::new(props_json_builder.finish()));

        // _deleted column
        let deleted: Vec<bool> = edges.iter().map(|(_, _, _, _, _, d, _)| *d).collect();
        columns.push(Arc::new(BooleanArray::from(deleted)));

        // _version column
        let versions: Vec<u64> = edges.iter().map(|(_, _, _, _, _, _, v)| *v).collect();
        columns.push(Arc::new(UInt64Array::from(versions)));

        // _created_at and _updated_at columns using shared builder
        let eids = edges.iter().map(|(e, _, _, _, _, _, _)| *e);
        columns.push(build_timestamp_column_from_eid_map(
            eids.clone(),
            created_at,
        ));
        columns.push(build_timestamp_column_from_eid_map(eids, updated_at));

        RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!(e))
    }

    /// Write a batch to the main edges table.
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

    /// Ensure default indexes exist on the main edges table.
    pub async fn ensure_default_indexes_lancedb(table: &Table) -> Result<()> {
        let indices = table
            .list_indices()
            .await
            .map_err(|e| anyhow!("Failed to list indices: {}", e))?;

        // Ensure _eid index (primary key)
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"_eid".to_string()))
        {
            log::info!("Creating _eid BTree index for main edges table");
            if let Err(e) = table
                .create_index(&["_eid"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!("Failed to create _eid index for main edges: {}", e);
            }
        }

        // Ensure src_vid index for outgoing traversal
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"src_vid".to_string()))
        {
            log::info!("Creating src_vid BTree index for main edges table");
            if let Err(e) = table
                .create_index(
                    &["src_vid"],
                    LanceDbIndex::BTree(BTreeIndexBuilder::default()),
                )
                .execute()
                .await
            {
                log::warn!("Failed to create src_vid index for main edges: {}", e);
            }
        }

        // Ensure dst_vid index for incoming traversal
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"dst_vid".to_string()))
        {
            log::info!("Creating dst_vid BTree index for main edges table");
            if let Err(e) = table
                .create_index(
                    &["dst_vid"],
                    LanceDbIndex::BTree(BTreeIndexBuilder::default()),
                )
                .execute()
                .await
            {
                log::warn!("Failed to create dst_vid index for main edges: {}", e);
            }
        }

        // Ensure type index for edge type filtering
        if !indices
            .iter()
            .any(|idx| idx.columns.contains(&"type".to_string()))
        {
            log::info!("Creating type BTree index for main edges table");
            if let Err(e) = table
                .create_index(&["type"], LanceDbIndex::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
            {
                log::warn!("Failed to create type index for main edges: {}", e);
            }
        }

        Ok(())
    }

    /// Query the main edges table for an edge by eid.
    pub async fn find_by_eid(
        store: &LanceDbStore,
        eid: Eid,
    ) -> Result<Option<(Vid, Vid, String, Properties)>> {
        let table_name = Self::table_name();

        if !store.table_exists(table_name).await? {
            return Ok(None);
        }

        let table = store.open_table(table_name).await?;
        let query = format!("_eid = {}", eid.as_u64());

        let batches = table
            .query()
            .only_if(query)
            .execute()
            .await
            .map_err(|e| anyhow!("Query failed: {}", e))?;

        use futures::TryStreamExt;
        let results: Vec<RecordBatch> = batches.try_collect().await?;

        for batch in results {
            if batch.num_rows() > 0 {
                let src_vid_col = batch.column_by_name("src_vid");
                let dst_vid_col = batch.column_by_name("dst_vid");
                let type_col = batch.column_by_name("type");
                let props_col = batch.column_by_name("props_json");

                if let (Some(src), Some(dst), Some(typ), Some(props)) =
                    (src_vid_col, dst_vid_col, type_col, props_col)
                    && let (Some(src_arr), Some(dst_arr), Some(type_arr), Some(props_arr)) = (
                        src.as_any().downcast_ref::<UInt64Array>(),
                        dst.as_any().downcast_ref::<UInt64Array>(),
                        typ.as_any().downcast_ref::<arrow_array::StringArray>(),
                        props.as_any().downcast_ref::<arrow_array::StringArray>(),
                    )
                {
                    let src_vid = Vid::from(src_arr.value(0));
                    let dst_vid = Vid::from(dst_arr.value(0));
                    let edge_type = type_arr.value(0).to_string();
                    let props_json = props_arr.value(0);
                    let properties: Properties =
                        serde_json::from_str(props_json).unwrap_or_default();

                    return Ok(Some((src_vid, dst_vid, edge_type, properties)));
                }
            }
        }

        Ok(None)
    }

    /// Open the main edges table.
    ///
    /// Returns None if the table doesn't exist yet.
    pub async fn open_table(store: &LanceDbStore) -> Result<Option<Table>> {
        let table_name = Self::table_name();

        if !store.table_exists(table_name).await? {
            return Ok(None);
        }

        let table = store.open_table(table_name).await?;
        Ok(Some(table))
    }

    /// Execute a query on the main edges table.
    ///
    /// Returns empty vec if table doesn't exist.
    async fn execute_query(
        store: &LanceDbStore,
        filter: &str,
        columns: Option<Vec<&str>>,
    ) -> Result<Vec<RecordBatch>> {
        let Some(table) = Self::open_table(store).await? else {
            return Ok(Vec::new());
        };

        let mut query = table.query();
        query = query.only_if(filter);

        if let Some(cols) = columns {
            query = query.select(Select::Columns(
                cols.into_iter().map(String::from).collect(),
            ));
        }

        let batches = query
            .execute()
            .await
            .map_err(|e| anyhow!("Query failed: {}", e))?;

        batches.try_collect().await.map_err(Into::into)
    }

    /// Extract EIDs from record batches.
    fn extract_eids(batches: &[RecordBatch]) -> Vec<Eid> {
        let mut eids = Vec::new();
        for batch in batches {
            if let Some(eid_col) = batch.column_by_name("_eid")
                && let Some(eid_arr) = eid_col.as_any().downcast_ref::<UInt64Array>()
            {
                for i in 0..eid_arr.len() {
                    if !eid_arr.is_null(i) {
                        eids.push(Eid::new(eid_arr.value(i)));
                    }
                }
            }
        }
        eids
    }

    /// Find all non-deleted EIDs from the main edges table.
    pub async fn find_all_eids(store: &LanceDbStore) -> Result<Vec<Eid>> {
        let batches = Self::execute_query(store, "_deleted = false", Some(vec!["_eid"])).await?;
        Ok(Self::extract_eids(&batches))
    }

    /// Find EIDs by type name in the main edges table.
    pub async fn find_eids_by_type_name(store: &LanceDbStore, type_name: &str) -> Result<Vec<Eid>> {
        let filter = format!(
            "_deleted = false AND type = '{}'",
            type_name.replace('\'', "''")
        );
        let batches = Self::execute_query(store, &filter, Some(vec!["_eid"])).await?;
        Ok(Self::extract_eids(&batches))
    }

    /// Find properties for an edge by EID in the main edges table.
    ///
    /// Returns the props_json parsed into a Properties HashMap if found.
    /// This is used as a fallback for unknown/schemaless edge types.
    pub async fn find_props_by_eid(store: &LanceDbStore, eid: Eid) -> Result<Option<Properties>> {
        let filter = format!("_eid = {} AND _deleted = false", eid.as_u64());
        let batches =
            Self::execute_query(store, &filter, Some(vec!["props_json", "_version"])).await?;

        if batches.is_empty() {
            return Ok(None);
        }

        // Find the row with highest version (latest)
        let mut best_props: Option<Properties> = None;
        let mut best_version: u64 = 0;

        for batch in &batches {
            let props_col = batch.column_by_name("props_json");
            let version_col = batch.column_by_name("_version");

            if let (Some(props_arr), Some(ver_arr)) = (
                props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>()),
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
                        best_props = Some(Self::parse_props_json(props_arr, i)?);
                    }
                }
            }
        }

        Ok(best_props)
    }

    /// Parse props_json from a StringArray at the given index.
    fn parse_props_json(arr: &arrow_array::StringArray, idx: usize) -> Result<Properties> {
        if arr.is_null(idx) || arr.value(idx).is_empty() {
            return Ok(Properties::new());
        }
        serde_json::from_str(arr.value(idx))
            .map_err(|e| anyhow!("Failed to parse props_json: {}", e))
    }

    /// Find edge type name by EID in the main edges table.
    pub async fn find_type_by_eid(store: &LanceDbStore, eid: Eid) -> Result<Option<String>> {
        let filter = format!("_eid = {} AND _deleted = false", eid.as_u64());
        let batches = Self::execute_query(store, &filter, Some(vec!["type"])).await?;

        for batch in batches {
            if batch.num_rows() > 0
                && let Some(type_col) = batch.column_by_name("type")
                && let Some(type_arr) = type_col.as_any().downcast_ref::<arrow_array::StringArray>()
                && !type_arr.is_null(0)
            {
                return Ok(Some(type_arr.value(0).to_string()));
            }
        }

        Ok(None)
    }

    /// Find edge data (eid, src_vid, dst_vid, props) by type name in the main edges table.
    ///
    /// Returns all non-deleted edges with the given type name.
    pub async fn find_edges_by_type_name(
        store: &LanceDbStore,
        type_name: &str,
    ) -> Result<Vec<(Eid, Vid, Vid, Properties)>> {
        let filter = format!(
            "_deleted = false AND type = '{}'",
            type_name.replace('\'', "''")
        );
        // Fetch all columns for edge data
        let batches = Self::execute_query(store, &filter, None).await?;

        let mut edges = Vec::new();
        for batch in &batches {
            Self::extract_edges_from_batch(batch, &mut edges)?;
        }

        Ok(edges)
    }

    /// Extract edge data from a record batch.
    fn extract_edges_from_batch(
        batch: &RecordBatch,
        edges: &mut Vec<(Eid, Vid, Vid, Properties)>,
    ) -> Result<()> {
        let eid_col = batch.column_by_name("_eid");
        let src_col = batch.column_by_name("src_vid");
        let dst_col = batch.column_by_name("dst_vid");
        let props_col = batch.column_by_name("props_json");

        if let (Some(eid_arr), Some(src_arr), Some(dst_arr), Some(props_arr)) = (
            eid_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            src_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            dst_col.and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>()),
        ) {
            for i in 0..batch.num_rows() {
                if eid_arr.is_null(i) || src_arr.is_null(i) || dst_arr.is_null(i) {
                    continue;
                }

                let eid = Eid::new(eid_arr.value(i));
                let src_vid = Vid::new(src_arr.value(i));
                let dst_vid = Vid::new(dst_arr.value(i));
                let props = Self::parse_props_json(props_arr, i)?;

                edges.push((eid, src_vid, dst_vid, props));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_main_edge_schema() {
        let schema = MainEdgeDataset::get_arrow_schema();
        assert_eq!(schema.fields().len(), 9);
        assert!(schema.field_with_name("_eid").is_ok());
        assert!(schema.field_with_name("src_vid").is_ok());
        assert!(schema.field_with_name("dst_vid").is_ok());
        assert!(schema.field_with_name("type").is_ok());
        assert!(schema.field_with_name("props_json").is_ok());
        assert!(schema.field_with_name("_deleted").is_ok());
        assert!(schema.field_with_name("_version").is_ok());
        assert!(schema.field_with_name("_created_at").is_ok());
        assert!(schema.field_with_name("_updated_at").is_ok());
    }

    #[test]
    fn test_build_record_batch() {
        let mut props = HashMap::new();
        props.insert("weight".to_string(), serde_json::json!(0.5));

        let edges = vec![(
            Eid::new(1),
            Vid::new(1),
            Vid::new(2),
            "KNOWS".to_string(),
            props,
            false,
            1u64,
        )];

        let batch = MainEdgeDataset::build_record_batch(&edges, None, None).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);
    }
}
