// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::lancedb::LanceDbStore;
use anyhow::{Result, anyhow};
use arrow_array::{ListArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::dataset::Dataset;
use lancedb::Table;
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};

/// Extract adjacency data (neighbors, edge IDs) from a single row of a RecordBatch.
///
/// Returns `None` if the batch is empty or columns are missing.
fn extract_adjacency_from_batch(batch: &RecordBatch) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }

    let neighbors_list = batch
        .column_by_name("neighbors")
        .ok_or(anyhow!("Missing neighbors"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or(anyhow!("Invalid neighbors type"))?;

    let edge_ids_list = batch
        .column_by_name("edge_ids")
        .ok_or(anyhow!("Missing edge_ids"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or(anyhow!("Invalid edge_ids type"))?;

    let neighbors_array = neighbors_list.value(0);
    let neighbors_uint64 = neighbors_array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(anyhow!("Invalid neighbors inner type"))?;

    let edge_ids_array = edge_ids_list.value(0);
    let edge_ids_uint64 = edge_ids_array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or(anyhow!("Invalid edge_ids inner type"))?;

    let neighbors: Vec<Vid> = (0..neighbors_uint64.len())
        .map(|i| Vid::from(neighbors_uint64.value(i)))
        .collect();

    let eids: Vec<Eid> = (0..edge_ids_uint64.len())
        .map(|i| Eid::from(edge_ids_uint64.value(i)))
        .collect();

    Ok(Some((neighbors, eids)))
}

pub struct AdjacencyDataset {
    uri: String,
    edge_type: String,
    direction: String,
}

impl AdjacencyDataset {
    pub fn new(base_uri: &str, edge_type: &str, label: &str, direction: &str) -> Self {
        let uri = format!(
            "{}/adjacency/{}_{}_{}",
            base_uri, direction, edge_type, label
        );
        Self {
            uri,
            edge_type: edge_type.to_string(),
            direction: direction.to_string(),
        }
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

    pub fn get_arrow_schema(&self) -> Arc<ArrowSchema> {
        let fields = vec![
            Field::new("src_vid", ArrowDataType::UInt64, false),
            // neighbors: list<uint64>
            Field::new(
                "neighbors",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
            // edge_ids: list<uint64>
            Field::new(
                "edge_ids",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::UInt64, true))),
                false,
            ),
        ];

        Arc::new(ArrowSchema::new(fields))
    }

    pub async fn read_adjacency(&self, vid: Vid) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        self.read_adjacency_at(vid, None).await
    }

    pub async fn read_adjacency_at(
        &self,
        vid: Vid,
        version: Option<u64>,
    ) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        let ds = match self.open_at(version).await {
            Ok(ds) => ds,
            Err(_) => return Ok(None),
        };

        let mut stream = ds
            .scan()
            .filter(&format!("src_vid = {}", vid.as_u64()))?
            .try_into_stream()
            .await?;

        if let Some(batch) = stream.try_next().await? {
            return extract_adjacency_from_batch(&batch);
        }

        Ok(None)
    }

    // ========================================================================
    // LanceDB-based Methods
    // ========================================================================

    /// Read adjacency data for a vertex from LanceDB.
    ///
    /// Returns `None` if the table doesn't exist or no data for the vertex.
    pub async fn read_adjacency_lancedb(
        &self,
        store: &LanceDbStore,
        vid: Vid,
    ) -> Result<Option<(Vec<Vid>, Vec<Eid>)>> {
        let table = match self.open_lancedb(store).await {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };

        use lancedb::query::{ExecutableQuery, QueryBase};

        let query = table.query().only_if(format!("src_vid = {}", vid.as_u64()));
        let stream = query.execute().await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        for batch in batches {
            if let Some(result) = extract_adjacency_from_batch(&batch)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }

    /// Open an adjacency table using LanceDB.
    pub async fn open_lancedb(&self, store: &LanceDbStore) -> Result<Table> {
        store
            .open_adjacency_table(&self.edge_type, &self.direction)
            .await
    }

    /// Open or create an adjacency table using LanceDB.
    pub async fn open_or_create_lancedb(&self, store: &LanceDbStore) -> Result<Table> {
        let arrow_schema = self.get_arrow_schema();
        store
            .open_or_create_adjacency_table(&self.edge_type, &self.direction, arrow_schema)
            .await
    }

    /// Write a chunk to a LanceDB adjacency table.
    ///
    /// Creates the table if it doesn't exist, otherwise appends to it.
    pub async fn write_chunk_lancedb(
        &self,
        store: &LanceDbStore,
        batch: RecordBatch,
    ) -> Result<Table> {
        let table_name = LanceDbStore::adjacency_table_name(&self.edge_type, &self.direction);

        if store.table_exists(&table_name).await? {
            let table = store.open_table(&table_name).await?;
            store.append_to_table(&table, vec![batch]).await?;
            Ok(table)
        } else {
            store.create_table(&table_name, vec![batch]).await
        }
    }

    /// Get the LanceDB table name for this adjacency dataset.
    pub fn lancedb_table_name(&self) -> String {
        LanceDbStore::adjacency_table_name(&self.edge_type, &self.direction)
    }

    /// Replace an adjacency table's contents using LanceDB.
    ///
    /// This drops the existing table and creates a new one with the provided
    /// data. Used by compaction to rewrite the table with merged data.
    pub async fn replace_lancedb(&self, store: &LanceDbStore, batch: RecordBatch) -> Result<Table> {
        let table_name = self.lancedb_table_name();
        let arrow_schema = self.get_arrow_schema();
        store
            .replace_table_or_empty(&table_name, vec![batch], arrow_schema)
            .await
    }
}
