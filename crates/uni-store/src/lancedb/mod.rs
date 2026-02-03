// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! LanceDB integration module.
//!
//! This module provides a wrapper around LanceDB for Uni's storage layer.
//! LanceDB provides:
//! - Built-in DataFusion query engine
//! - Automatic scalar indexing (BTREE, BITMAP)
//! - Vector search with IVF_PQ
//! - Full-text search with BM25

use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_array::RecordBatchIterator;
use arrow_schema::Schema as ArrowSchema;
use lancedb::Table;
use lancedb::connection::Connection;
use std::sync::Arc;

/// Wrapper around LanceDB connection for Uni storage.
///
/// This provides a unified interface for all table operations,
/// replacing direct Lance Dataset usage.
pub struct LanceDbStore {
    connection: Connection,
    base_uri: String,
}

impl LanceDbStore {
    /// Connect to a LanceDB database at the given URI.
    ///
    /// Supported URIs:
    /// - `/path/to/database` - local filesystem
    /// - `s3://bucket/path` - AWS S3
    /// - `gs://bucket/path` - Google Cloud Storage
    pub async fn connect(uri: &str) -> Result<Self> {
        let connection = lancedb::connect(uri)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to connect to LanceDB at {}: {}", uri, e))?;

        Ok(Self {
            connection,
            base_uri: uri.to_string(),
        })
    }

    /// Get the base URI for this store.
    pub fn base_uri(&self) -> &str {
        &self.base_uri
    }

    /// List all table names in the database.
    pub async fn table_names(&self) -> Result<Vec<String>> {
        self.connection
            .table_names()
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to list tables: {}", e))
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, name: &str) -> Result<bool> {
        let tables = self.table_names().await?;
        Ok(tables.contains(&name.to_string()))
    }

    /// Open an existing table.
    pub async fn open_table(&self, name: &str) -> Result<Table> {
        self.connection
            .open_table(name)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to open table '{}': {}", name, e))
    }

    /// Create a new table with initial data.
    ///
    /// If the table already exists, this will fail.
    pub async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<Table> {
        if batches.is_empty() {
            return Err(anyhow!(
                "Cannot create table '{}' with empty data. Use create_empty_table instead.",
                name
            ));
        }

        let schema = batches[0].schema();
        let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        self.connection
            .create_table(name, batch_iter)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create table '{}': {}", name, e))
    }

    /// Create a new empty table with a schema.
    pub async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<Table> {
        self.connection
            .create_empty_table(name, schema)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create empty table '{}': {}", name, e))
    }

    /// Open a table, creating it if it doesn't exist.
    pub async fn open_or_create_table(
        &self,
        name: &str,
        schema: Arc<ArrowSchema>,
    ) -> Result<Table> {
        if self.table_exists(name).await? {
            self.open_table(name).await
        } else {
            self.create_empty_table(name, schema).await
        }
    }

    /// Drop a table by name.
    pub async fn drop_table(&self, name: &str) -> Result<()> {
        self.connection
            .drop_table(name, &[])
            .await
            .map_err(|e| anyhow!("Failed to drop table '{}': {}", name, e))
    }

    /// Drop all tables in the database.
    pub async fn drop_all_tables(&self) -> Result<()> {
        self.connection
            .drop_all_tables(&[])
            .await
            .map_err(|e| anyhow!("Failed to drop all tables: {}", e))
    }

    /// Append data to an existing table.
    pub async fn append_to_table(&self, table: &Table, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();
        let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        table
            .add(batch_iter)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to append to table: {}", e))?;

        Ok(())
    }

    // ========================================================================
    // Main Unified Table Operations
    // ========================================================================

    /// Get the table name for the main vertices table.
    ///
    /// The main vertices table contains all vertices regardless of label,
    /// enabling fast ID-based lookups without knowing the label.
    pub fn main_vertex_table_name() -> &'static str {
        "vertices"
    }

    /// Get the table name for the main edges table.
    ///
    /// The main edges table contains all edges regardless of type,
    /// enabling fast ID-based lookups without knowing the edge type.
    pub fn main_edge_table_name() -> &'static str {
        "edges"
    }

    /// Open the main vertices table.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist.
    pub async fn open_main_vertex_table(&self) -> Result<Table> {
        self.open_table(Self::main_vertex_table_name()).await
    }

    /// Open the main edges table.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist.
    pub async fn open_main_edge_table(&self) -> Result<Table> {
        self.open_table(Self::main_edge_table_name()).await
    }

    /// Check if the main vertices table exists.
    pub async fn main_vertex_table_exists(&self) -> Result<bool> {
        self.table_exists(Self::main_vertex_table_name()).await
    }

    /// Check if the main edges table exists.
    pub async fn main_edge_table_exists(&self) -> Result<bool> {
        self.table_exists(Self::main_edge_table_name()).await
    }

    // ========================================================================
    // Per-Label Vertex Table Operations
    // ========================================================================

    /// Get the table name for a vertex label.
    pub fn vertex_table_name(label: &str) -> String {
        format!("vertices_{}", label)
    }

    /// Open or create a vertex table for a label.
    pub async fn open_or_create_vertex_table(
        &self,
        label: &str,
        schema: Arc<ArrowSchema>,
    ) -> Result<Table> {
        let table_name = Self::vertex_table_name(label);
        self.open_or_create_table(&table_name, schema).await
    }

    /// Open a vertex table for a label.
    pub async fn open_vertex_table(&self, label: &str) -> Result<Table> {
        let table_name = Self::vertex_table_name(label);
        self.open_table(&table_name).await
    }

    /// Check if a vertex table exists for a label.
    pub async fn vertex_table_exists(&self, label: &str) -> Result<bool> {
        let table_name = Self::vertex_table_name(label);
        self.table_exists(&table_name).await
    }

    // ========================================================================
    // Delta Table Operations (Edge Mutations)
    // ========================================================================

    /// Get the table name for edge deltas.
    pub fn delta_table_name(edge_type: &str, direction: &str) -> String {
        format!("deltas_{}_{}", edge_type, direction)
    }

    /// Open or create a delta table.
    pub async fn open_or_create_delta_table(
        &self,
        edge_type: &str,
        direction: &str,
        schema: Arc<ArrowSchema>,
    ) -> Result<Table> {
        let table_name = Self::delta_table_name(edge_type, direction);
        self.open_or_create_table(&table_name, schema).await
    }

    /// Open a delta table.
    pub async fn open_delta_table(&self, edge_type: &str, direction: &str) -> Result<Table> {
        let table_name = Self::delta_table_name(edge_type, direction);
        self.open_table(&table_name).await
    }

    // ========================================================================
    // Adjacency Table Operations
    // ========================================================================

    /// Get the table name for adjacency data.
    pub fn adjacency_table_name(edge_type: &str, direction: &str) -> String {
        format!("adjacency_{}_{}", edge_type, direction)
    }

    /// Open or create an adjacency table.
    pub async fn open_or_create_adjacency_table(
        &self,
        edge_type: &str,
        direction: &str,
        schema: Arc<ArrowSchema>,
    ) -> Result<Table> {
        let table_name = Self::adjacency_table_name(edge_type, direction);
        self.open_or_create_table(&table_name, schema).await
    }

    /// Open an adjacency table.
    pub async fn open_adjacency_table(&self, edge_type: &str, direction: &str) -> Result<Table> {
        let table_name = Self::adjacency_table_name(edge_type, direction);
        self.open_table(&table_name).await
    }

    // ========================================================================
    // Version/Rollback Operations (for bulk load abort)
    // ========================================================================

    /// Get the current version of a table.
    ///
    /// Returns `None` if the table does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the table exists but version query fails.
    pub async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        if !self.table_exists(table_name).await? {
            return Ok(None);
        }
        let table = self.open_table(table_name).await?;
        let version = table
            .version()
            .await
            .map_err(|e| anyhow!("Failed to get version for table '{}': {}", table_name, e))?;
        Ok(Some(version))
    }

    /// Roll back a table to a specific version.
    ///
    /// This uses LanceDB's checkout and restore APIs to create a new version
    /// that matches the state at `target_version`.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be opened or rollback fails.
    pub async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()> {
        let table = self.open_table(table_name).await?;
        table.checkout(target_version).await.map_err(|e| {
            anyhow!(
                "Failed to checkout version {} for '{}': {}",
                target_version,
                table_name,
                e
            )
        })?;
        table.restore().await.map_err(|e| {
            anyhow!(
                "Failed to restore table '{}' to version {}: {}",
                table_name,
                target_version,
                e
            )
        })?;
        Ok(())
    }

    /// Replace a table's contents with new data.
    ///
    /// This is the LanceDB equivalent of `WriteMode::Overwrite`. It drops the
    /// existing table and creates a new one with the provided data.
    ///
    /// # Errors
    ///
    /// Returns an error if table operations fail.
    pub async fn replace_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<Table> {
        // Drop existing table if it exists
        if self.table_exists(name).await? {
            self.drop_table(name).await?;
        }

        // Create new table with data
        self.create_table(name, batches).await
    }

    /// Replace a table's contents, creating an empty table if batches are empty.
    ///
    /// Unlike `replace_table`, this handles empty data gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if table operations fail.
    pub async fn replace_table_or_empty(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<Table> {
        // Drop existing table if it exists
        if self.table_exists(name).await? {
            self.drop_table(name).await?;
        }

        // Create new table with data or empty
        if batches.is_empty() {
            self.create_empty_table(name, schema).await
        } else {
            self.create_table(name, batches).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_connect_and_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let uri = temp_dir.path().to_str().unwrap();

        // Connect to LanceDB
        let store = LanceDbStore::connect(uri).await.unwrap();

        // Create a simple schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create empty table
        let _table = store
            .create_empty_table("test_table", schema.clone())
            .await
            .unwrap();

        // Verify table exists
        assert!(store.table_exists("test_table").await.unwrap());

        // List tables
        let tables = store.table_names().await.unwrap();
        assert!(tables.contains(&"test_table".to_string()));
    }

    #[tokio::test]
    async fn test_create_table_with_data() {
        let temp_dir = TempDir::new().unwrap();
        let uri = temp_dir.path().to_str().unwrap();

        let store = LanceDbStore::connect(uri).await.unwrap();

        // Create a batch with data
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        // Create table with data
        let table = store.create_table("users", vec![batch]).await.unwrap();

        // Verify row count
        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_vertex_table_operations() {
        let temp_dir = TempDir::new().unwrap();
        let uri = temp_dir.path().to_str().unwrap();

        let store = LanceDbStore::connect(uri).await.unwrap();

        // Verify table naming convention
        assert_eq!(LanceDbStore::vertex_table_name("Person"), "vertices_Person");

        // Create a vertex-like schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Open or create vertex table
        let table = store
            .open_or_create_vertex_table("Person", schema)
            .await
            .unwrap();

        // Verify table was created
        assert!(store.vertex_table_exists("Person").await.unwrap());

        // Verify row count is 0 for empty table
        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_append_to_table() {
        use arrow_array::UInt64Array;

        let temp_dir = TempDir::new().unwrap();
        let uri = temp_dir.path().to_str().unwrap();

        let store = LanceDbStore::connect(uri).await.unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Create initial batch
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let table = store.create_table("test", vec![batch1]).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 2);

        // Append more data
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![3, 4, 5])),
                Arc::new(Int64Array::from(vec![300, 400, 500])),
            ],
        )
        .unwrap();

        store.append_to_table(&table, vec![batch2]).await.unwrap();

        // Verify total row count
        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 5);
    }
}
