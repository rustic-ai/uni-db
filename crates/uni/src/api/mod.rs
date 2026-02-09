// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub mod builder;
pub mod bulk;
pub mod impl_query;
pub mod query_builder;
pub mod schema;
pub mod session;
pub mod sync;
pub mod transaction;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tracing::info;
use uni_common::core::snapshot::SnapshotManifest;
use uni_common::{CloudStorageConfig, UniConfig};
use uni_common::{Result, UniError};
use uni_store::cloud::build_cloud_store;

use uni_common::core::schema::SchemaManager;
use uni_store::runtime::id_allocator::IdAllocator;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::wal::WriteAheadLog;
use uni_store::storage::manager::StorageManager;

use tokio::sync::RwLock;
use uni_store::runtime::writer::Writer;

use crate::shutdown::ShutdownHandle;

/// Main entry point for Uni embedded database.
///
/// Handles storage, schema, and query execution. It coordinates the various
/// subsystems including the storage engine, query executor, and graph algorithms.
///
/// # Examples
///
/// ## Local Usage
/// ```no_run
/// use uni_db::Uni;
///
/// #[tokio::main]
/// async fn main() -> Result<(), uni_db::UniError> {
///     let db = Uni::open("./my_db")
///         .build()
///         .await?;
///
///     // Run a query
///     let results = db.query("MATCH (n) RETURN count(n)").await?;
///     println!("Count: {:?}", results);
///     Ok(())
/// }
/// ```
///
/// ## Hybrid Storage (S3 + Local)
/// Store bulk data in S3 (or GCS/Azure) but keep WAL and Metadata local for low latency.
///
/// ```no_run
/// use uni_db::Uni;
///
/// #[tokio::main]
/// async fn main() -> Result<(), uni_db::UniError> {
///     // Requires `object_store` features enabled (aws, gcp, azure)
///     let db = Uni::open("./local_meta")
///         .hybrid("./local_meta", "s3://my-bucket/graph-data")
///         .build()
///         .await?;
///     Ok(())
/// }
/// ```
pub struct Uni {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) schema: Arc<SchemaManager>,
    pub(crate) properties: Arc<PropertyManager>,
    pub(crate) writer: Option<Arc<RwLock<Writer>>>,
    pub(crate) config: UniConfig,
    pub(crate) procedure_registry: Arc<uni_query::ProcedureRegistry>,
    pub(crate) shutdown_handle: Arc<ShutdownHandle>,
}

impl Uni {
    /// Open or create a database at the given path.
    ///
    /// If the database does not exist, it will be created.
    ///
    /// # Arguments
    ///
    /// * `uri` - Local path or object store URI.
    ///
    /// # Returns
    ///
    /// A [`UniBuilder`] to configure and build the database instance.
    pub fn open(uri: impl Into<String>) -> UniBuilder {
        UniBuilder::new(uri.into())
    }

    /// Open an existing database at the given path. Fails if it does not exist.
    pub fn open_existing(uri: impl Into<String>) -> UniBuilder {
        let mut builder = UniBuilder::new(uri.into());
        builder.create_if_missing = false;
        builder
    }

    /// Create a new database at the given path. Fails if it already exists.
    pub fn create(uri: impl Into<String>) -> UniBuilder {
        let mut builder = UniBuilder::new(uri.into());
        builder.fail_if_exists = true;
        builder
    }

    /// Create a temporary database that is deleted when dropped.
    ///
    /// Useful for tests and short-lived processing.
    /// Note: Currently uses a temporary directory on the filesystem.
    pub fn temporary() -> UniBuilder {
        let temp_dir = std::env::temp_dir().join(format!("uni_mem_{}", uuid::Uuid::new_v4()));
        UniBuilder::new(temp_dir.to_string_lossy().to_string())
    }

    /// Open an in-memory database (alias for temporary).
    pub fn in_memory() -> UniBuilder {
        Self::temporary()
    }

    /// Open a point-in-time view of the database at the given snapshot.
    ///
    /// This returns a new `Uni` instance that is pinned to the specified snapshot state.
    /// The returned instance is read-only. Used internally by Cypher `VERSION AS OF` / `TIMESTAMP AS OF`.
    pub(crate) async fn at_snapshot(&self, snapshot_id: &str) -> Result<Uni> {
        let manifest = self
            .storage
            .snapshot_manager()
            .load_snapshot(snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        let pinned_storage = Arc::new(self.storage.pinned(manifest));

        let prop_manager = Arc::new(PropertyManager::new(
            pinned_storage.clone(),
            self.schema.clone(),
            self.properties.cache_size(),
        ));

        // Create a shutdown handle for this read-only snapshot view
        // (no background tasks, but needed for consistency)
        let shutdown_handle = Arc::new(ShutdownHandle::new(Duration::from_secs(30)));

        Ok(Uni {
            storage: pinned_storage,
            schema: self.schema.clone(),
            properties: prop_manager,
            writer: None,
            config: self.config.clone(),
            procedure_registry: self.procedure_registry.clone(),
            shutdown_handle,
        })
    }

    /// Get configuration
    pub fn config(&self) -> &UniConfig {
        &self.config
    }

    /// Returns the procedure registry for registering test procedures.
    pub fn procedure_registry(&self) -> &Arc<uni_query::ProcedureRegistry> {
        &self.procedure_registry
    }

    /// Get current schema (read-only snapshot)
    pub fn get_schema(&self) -> uni_common::core::schema::Schema {
        self.schema.schema()
    }

    /// Create a bulk writer for efficient data loading.
    pub fn bulk_writer(&self) -> bulk::BulkWriterBuilder<'_> {
        bulk::BulkWriterBuilder::new(self)
    }

    /// Create a session builder for scoped query context.
    pub fn session(&self) -> session::SessionBuilder<'_> {
        session::SessionBuilder::new(self)
    }

    /// Get schema manager
    #[doc(hidden)]
    pub fn schema_manager(&self) -> Arc<SchemaManager> {
        self.schema.clone()
    }

    #[doc(hidden)]
    pub fn writer(&self) -> Option<Arc<RwLock<Writer>>> {
        self.writer.clone()
    }

    #[doc(hidden)]
    pub fn storage(&self) -> Arc<StorageManager> {
        self.storage.clone()
    }

    /// Flush all uncommitted changes to persistent storage (L1).
    ///
    /// This forces a write of the current in-memory buffer (L0) to columnar files.
    /// It also creates a new snapshot.
    pub async fn flush(&self) -> Result<()> {
        if let Some(writer_lock) = &self.writer {
            let mut writer = writer_lock.write().await;
            writer
                .flush_to_l1(None)
                .await
                .map(|_| ())
                .map_err(UniError::Internal)
        } else {
            Err(UniError::ReadOnly {
                operation: "flush".to_string(),
            })
        }
    }

    /// Create a named point-in-time snapshot of the database.
    ///
    /// This flushes current changes and records the state.
    /// Returns the snapshot ID.
    pub async fn create_snapshot(&self, name: Option<&str>) -> Result<String> {
        if let Some(writer_lock) = &self.writer {
            let mut writer = writer_lock.write().await;
            writer
                .flush_to_l1(name.map(|s| s.to_string()))
                .await
                .map_err(UniError::Internal)
        } else {
            Err(UniError::ReadOnly {
                operation: "create_snapshot".to_string(),
            })
        }
    }

    /// Create a persisted named snapshot that can be retrieved later.
    pub async fn create_named_snapshot(&self, name: &str) -> Result<String> {
        if name.is_empty() {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Snapshot name cannot be empty"
            )));
        }

        let snapshot_id = self.create_snapshot(Some(name)).await?;

        self.storage
            .snapshot_manager()
            .save_named_snapshot(name, &snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        Ok(snapshot_id)
    }

    /// List all available snapshots.
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>> {
        let sm = self.storage.snapshot_manager();
        let ids = sm.list_snapshots().await.map_err(UniError::Internal)?;
        let mut manifests = Vec::new();
        for id in ids {
            if let Ok(m) = sm.load_snapshot(&id).await {
                manifests.push(m);
            }
        }
        Ok(manifests)
    }

    /// Restore the database to a specific snapshot.
    ///
    /// **Note**: This currently requires a restart or re-opening of Uni to fully take effect
    /// as it only updates the latest pointer.
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.storage
            .snapshot_manager()
            .set_latest_snapshot(snapshot_id)
            .await
            .map_err(UniError::Internal)
    }

    /// Check if a label exists in the schema.
    pub async fn label_exists(&self, name: &str) -> Result<bool> {
        Ok(self.schema.schema().labels.get(name).is_some_and(|l| {
            matches!(
                l.state,
                uni_common::core::schema::SchemaElementState::Active
            )
        }))
    }

    /// Check if an edge type exists in the schema.
    pub async fn edge_type_exists(&self, name: &str) -> Result<bool> {
        Ok(self.schema.schema().edge_types.get(name).is_some_and(|e| {
            matches!(
                e.state,
                uni_common::core::schema::SchemaElementState::Active
            )
        }))
    }

    /// Get all distinct labels that have at least one active vertex.
    /// This is done by querying the database directly for all distinct labels,
    /// rather than relying on the schema which may not be updated yet in a
    /// schemaless database.
    async fn get_distinct_labels(&self) -> Result<Vec<String>> {
        // Query: MATCH (n) RETURN DISTINCT labels(n) AS labels
        // This returns all unique label combinations on vertices
        let query = "MATCH (n) RETURN DISTINCT labels(n) AS labels";
        let result = self.query(query).await?;

        let mut all_labels = std::collections::HashSet::new();
        for row in &result.rows {
            if let Ok(labels_list) = row.get::<Vec<String>>("labels") {
                for label in labels_list {
                    all_labels.insert(label);
                }
            }
        }

        Ok(all_labels.into_iter().collect())
    }

    /// Get all label names.
    /// Returns only labels that have at least one active vertex.
    /// This aligns with OpenCypher semantics where labels are implicitly managed
    /// by vertex lifecycle - labels appear when first vertex is created and
    /// disappear when last vertex is deleted.
    pub async fn list_labels(&self) -> Result<Vec<String>> {
        self.get_distinct_labels().await
    }

    /// Get all edge type names.
    pub async fn list_edge_types(&self) -> Result<Vec<String>> {
        Ok(self
            .schema
            .schema()
            .edge_types
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e.state,
                    uni_common::core::schema::SchemaElementState::Active
                )
            })
            .map(|(name, _)| name.clone())
            .collect())
    }

    /// Get detailed information about a label.
    pub async fn get_label_info(
        &self,
        name: &str,
    ) -> Result<Option<crate::api::schema::LabelInfo>> {
        let schema = self.schema.schema();
        if schema.labels.contains_key(name) {
            let count = if let Ok(ds) = self.storage.vertex_dataset(name) {
                if let Ok(raw) = ds.open_raw().await {
                    raw.count_rows(None)
                        .await
                        .map_err(|e| UniError::Internal(anyhow::anyhow!(e)))?
                } else {
                    0
                }
            } else {
                0
            };

            let mut properties = Vec::new();
            if let Some(props) = schema.properties.get(name) {
                for (prop_name, prop_meta) in props {
                    let is_indexed = schema.indexes.iter().any(|idx| match idx {
                        uni_common::core::schema::IndexDefinition::Vector(v) => {
                            v.label == name && v.property == *prop_name
                        }
                        uni_common::core::schema::IndexDefinition::Scalar(s) => {
                            s.label == name && s.properties.contains(prop_name)
                        }
                        uni_common::core::schema::IndexDefinition::FullText(f) => {
                            f.label == name && f.properties.contains(prop_name)
                        }
                        uni_common::core::schema::IndexDefinition::Inverted(inv) => {
                            inv.label == name && inv.property == *prop_name
                        }
                        uni_common::core::schema::IndexDefinition::JsonFullText(j) => {
                            j.label == name
                        }
                        _ => false,
                    });

                    properties.push(crate::api::schema::PropertyInfo {
                        name: prop_name.clone(),
                        data_type: format!("{:?}", prop_meta.r#type),
                        nullable: prop_meta.nullable,
                        is_indexed,
                    });
                }
            }

            let mut indexes = Vec::new();
            for idx in schema.indexes.iter().filter(|i| i.label() == name) {
                use uni_common::core::schema::IndexDefinition;
                let (idx_type, idx_props) = match idx {
                    IndexDefinition::Vector(v) => ("VECTOR", vec![v.property.clone()]),
                    IndexDefinition::Scalar(s) => ("SCALAR", s.properties.clone()),
                    IndexDefinition::FullText(f) => ("FULLTEXT", f.properties.clone()),
                    IndexDefinition::Inverted(inv) => ("INVERTED", vec![inv.property.clone()]),
                    IndexDefinition::JsonFullText(j) => ("JSON_FTS", vec![j.column.clone()]),
                    _ => continue,
                };

                indexes.push(crate::api::schema::IndexInfo {
                    name: idx.name().to_string(),
                    index_type: idx_type.to_string(),
                    properties: idx_props,
                    status: "ONLINE".to_string(), // TODO: Check actual status
                });
            }

            let mut constraints = Vec::new();
            for c in &schema.constraints {
                if let uni_common::core::schema::ConstraintTarget::Label(l) = &c.target
                    && l == name
                {
                    let (ctype, cprops) = match &c.constraint_type {
                        uni_common::core::schema::ConstraintType::Unique { properties } => {
                            ("UNIQUE", properties.clone())
                        }
                        uni_common::core::schema::ConstraintType::Exists { property } => {
                            ("EXISTS", vec![property.clone()])
                        }
                        uni_common::core::schema::ConstraintType::Check { expression } => {
                            ("CHECK", vec![expression.clone()])
                        }
                        _ => ("UNKNOWN", vec![]),
                    };

                    constraints.push(crate::api::schema::ConstraintInfo {
                        name: c.name.clone(),
                        constraint_type: ctype.to_string(),
                        properties: cprops,
                        enabled: c.enabled,
                    });
                }
            }

            Ok(Some(crate::api::schema::LabelInfo {
                name: name.to_string(),
                count,
                properties,
                indexes,
                constraints,
            }))
        } else {
            Ok(None)
        }
    }

    /// Manually trigger compaction for a specific label.
    ///
    /// Compaction merges multiple L1 files into larger files to improve read performance.
    pub async fn compact_label(
        &self,
        label: &str,
    ) -> Result<uni_store::compaction::CompactionStats> {
        self.storage
            .compact_label(label)
            .await
            .map_err(UniError::Internal)
    }

    /// Manually trigger compaction for a specific edge type.
    pub async fn compact_edge_type(
        &self,
        edge_type: &str,
    ) -> Result<uni_store::compaction::CompactionStats> {
        self.storage
            .compact_edge_type(edge_type)
            .await
            .map_err(UniError::Internal)
    }

    /// Wait for any ongoing compaction to complete.
    ///
    /// Useful for tests or ensuring consistent performance before benchmarks.
    pub async fn wait_for_compaction(&self) -> Result<()> {
        self.storage
            .wait_for_compaction()
            .await
            .map_err(UniError::Internal)
    }

    /// Bulk insert vertices for a given label.
    ///
    /// This is a low-level API intended for bulk loading and benchmarking.
    /// Each properties map should contain all property values as JSON.
    ///
    /// Returns the allocated VIDs in the same order as the input.
    pub async fn bulk_insert_vertices(
        &self,
        label: &str,
        properties_list: Vec<uni_common::Properties>,
    ) -> Result<Vec<uni_common::core::id::Vid>> {
        let schema = self.schema.schema();
        // Validate label exists in schema
        schema
            .labels
            .get(label)
            .ok_or_else(|| UniError::LabelNotFound {
                label: label.to_string(),
            })?;
        if let Some(writer_lock) = &self.writer {
            let mut writer = writer_lock.write().await;

            if properties_list.is_empty() {
                return Ok(Vec::new());
            }

            // NEW: Batch allocate VIDs (single call)
            let vids = writer
                .allocate_vids(properties_list.len())
                .await
                .map_err(UniError::Internal)?;

            // NEW: Use optimized batch insert
            let _props = writer
                .insert_vertices_batch(vids.clone(), properties_list, vec![label.to_string()])
                .await
                .map_err(UniError::Internal)?;

            Ok(vids)
        } else {
            Err(UniError::ReadOnly {
                operation: "bulk_insert_vertices".to_string(),
            })
        }
    }

    /// Bulk insert edges for a given edge type using pre-allocated VIDs.
    ///
    /// This is a low-level API intended for bulk loading and benchmarking.
    /// Each tuple is (src_vid, dst_vid, properties).
    pub async fn bulk_insert_edges(
        &self,
        edge_type: &str,
        edges: Vec<(
            uni_common::core::id::Vid,
            uni_common::core::id::Vid,
            uni_common::Properties,
        )>,
    ) -> Result<()> {
        let schema = self.schema.schema();
        let edge_meta =
            schema
                .edge_types
                .get(edge_type)
                .ok_or_else(|| UniError::EdgeTypeNotFound {
                    edge_type: edge_type.to_string(),
                })?;
        let type_id = edge_meta.id;

        if let Some(writer_lock) = &self.writer {
            let mut writer = writer_lock.write().await;

            for (src_vid, dst_vid, props) in edges {
                let eid = writer.next_eid(type_id).await.map_err(UniError::Internal)?;
                writer
                    .insert_edge(src_vid, dst_vid, type_id, eid, props)
                    .await
                    .map_err(UniError::Internal)?;
            }

            Ok(())
        } else {
            Err(UniError::ReadOnly {
                operation: "bulk_insert_edges".to_string(),
            })
        }
    }

    /// Get the status of background index rebuild tasks.
    ///
    /// Returns all tracked index rebuild tasks, including pending, in-progress,
    /// completed, and failed tasks. Use this to monitor progress of async
    /// index rebuilds started via `BulkWriter::commit()` with `async_indexes(true)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let status = db.index_rebuild_status().await?;
    /// for task in status {
    ///     println!("Label: {}, Status: {:?}", task.label, task.status);
    /// }
    /// ```
    pub async fn index_rebuild_status(&self) -> Result<Vec<uni_store::storage::IndexRebuildTask>> {
        let manager = uni_store::storage::IndexRebuildManager::new(
            self.storage.clone(),
            self.schema.clone(),
            self.config.index_rebuild.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        Ok(manager.status())
    }

    /// Retry failed index rebuild tasks.
    ///
    /// Resets failed tasks back to pending state and returns the task IDs
    /// that will be retried. Tasks that have exceeded their retry limit
    /// will not be retried.
    ///
    /// # Returns
    ///
    /// A vector of task IDs that were scheduled for retry.
    pub async fn retry_index_rebuilds(&self) -> Result<Vec<String>> {
        let manager = uni_store::storage::IndexRebuildManager::new(
            self.storage.clone(),
            self.schema.clone(),
            self.config.index_rebuild.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        let retried = manager.retry_failed().await.map_err(UniError::Internal)?;

        // Start background worker to process the retried tasks
        if !retried.is_empty() {
            let manager = std::sync::Arc::new(manager);
            let handle = manager.start_background_worker(self.shutdown_handle.subscribe());
            self.shutdown_handle.track_task(handle);
        }

        Ok(retried)
    }

    /// Force rebuild indexes for a specific label.
    ///
    /// # Arguments
    ///
    /// * `label` - The vertex label to rebuild indexes for.
    /// * `async_` - If true, rebuild in background; if false, block until complete.
    ///
    /// # Returns
    ///
    /// When `async_` is true, returns the task ID for tracking progress.
    /// When `async_` is false, returns None after indexes are rebuilt.
    pub async fn rebuild_indexes(&self, label: &str, async_: bool) -> Result<Option<String>> {
        if async_ {
            let manager = uni_store::storage::IndexRebuildManager::new(
                self.storage.clone(),
                self.schema.clone(),
                self.config.index_rebuild.clone(),
            )
            .await
            .map_err(UniError::Internal)?;

            let task_ids = manager
                .schedule(vec![label.to_string()])
                .await
                .map_err(UniError::Internal)?;

            let manager = std::sync::Arc::new(manager);
            let handle = manager.start_background_worker(self.shutdown_handle.subscribe());
            self.shutdown_handle.track_task(handle);

            Ok(task_ids.into_iter().next())
        } else {
            let idx_mgr = uni_store::storage::IndexManager::new(
                self.storage.base_path(),
                self.schema.clone(),
                self.storage.lancedb_store_arc(),
            );
            idx_mgr
                .rebuild_indexes_for_label(label)
                .await
                .map_err(UniError::Internal)?;
            Ok(None)
        }
    }

    /// Check if an index is currently being rebuilt for a label.
    ///
    /// Returns true if there is a pending or in-progress index rebuild task
    /// for the specified label.
    pub async fn is_index_building(&self, label: &str) -> Result<bool> {
        let manager = uni_store::storage::IndexRebuildManager::new(
            self.storage.clone(),
            self.schema.clone(),
            self.config.index_rebuild.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        Ok(manager.is_index_building(label))
    }

    /// Shutdown the database gracefully, flushing pending data and stopping background tasks.
    ///
    /// This method flushes any pending data and waits for all background tasks to complete
    /// (with a timeout). After calling this method, the database instance should not be used.
    pub async fn shutdown(self) -> Result<()> {
        // Flush pending data
        if let Some(ref writer) = self.writer {
            let mut w = writer.write().await;
            if let Err(e) = w.flush_to_l1(None).await {
                tracing::error!("Error flushing during shutdown: {}", e);
            }
        }

        self.shutdown_handle
            .shutdown_async()
            .await
            .map_err(UniError::Internal)
    }
}

impl Drop for Uni {
    fn drop(&mut self) {
        self.shutdown_handle.shutdown_blocking();
        tracing::debug!("Uni dropped, shutdown signal sent");
    }
}

/// Builder for configuring and opening a `Uni` database instance.
#[must_use = "builders do nothing until .build() is called"]
pub struct UniBuilder {
    uri: String,
    config: UniConfig,
    schema_file: Option<PathBuf>,
    hybrid_remote_url: Option<String>,
    cloud_config: Option<CloudStorageConfig>,
    create_if_missing: bool,
    fail_if_exists: bool,
}

impl UniBuilder {
    /// Creates a new builder for the given URI.
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            config: UniConfig::default(),
            schema_file: None,
            hybrid_remote_url: None,
            cloud_config: None,
            create_if_missing: true,
            fail_if_exists: false,
        }
    }

    /// Load schema from JSON file on initialization.
    pub fn schema_file(mut self, path: impl AsRef<Path>) -> Self {
        self.schema_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Configure hybrid storage with a local path for WAL/IDs and a remote URL for data.
    ///
    /// This allows fast local writes and metadata operations while storing bulk data
    /// in object storage (e.g., S3, GCS, Azure Blob Storage).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let db = Uni::open("./local_meta")
    ///     .hybrid("./local_meta", "s3://my-bucket/graph-data")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn hybrid(mut self, local_path: impl AsRef<Path>, remote_url: &str) -> Self {
        self.uri = local_path.as_ref().to_string_lossy().to_string();
        self.hybrid_remote_url = Some(remote_url.to_string());
        self
    }

    /// Configure cloud storage with explicit credentials.
    ///
    /// Use this method when you need fine-grained control over cloud storage
    /// credentials instead of relying on environment variables.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use uni_common::CloudStorageConfig;
    ///
    /// let config = CloudStorageConfig::S3 {
    ///     bucket: "my-bucket".to_string(),
    ///     region: Some("us-east-1".to_string()),
    ///     endpoint: Some("http://localhost:4566".to_string()), // LocalStack
    ///     access_key_id: Some("test".to_string()),
    ///     secret_access_key: Some("test".to_string()),
    ///     session_token: None,
    ///     virtual_hosted_style: false,
    /// };
    ///
    /// let db = Uni::open("./local_meta")
    ///     .hybrid("./local_meta", "s3://my-bucket/data")
    ///     .cloud_config(config)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn cloud_config(mut self, config: CloudStorageConfig) -> Self {
        self.cloud_config = Some(config);
        self
    }

    /// Configure database options using `UniConfig`.
    pub fn config(mut self, config: UniConfig) -> Self {
        self.config = config;
        self
    }

    /// Set maximum adjacency cache size in bytes.
    pub fn cache_size(mut self, bytes: usize) -> Self {
        self.config.cache_size = bytes;
        self
    }

    /// Set query parallelism (number of worker threads).
    pub fn parallelism(mut self, n: usize) -> Self {
        self.config.parallelism = n;
        self
    }

    /// Open the database (async).
    pub async fn build(self) -> Result<Uni> {
        let uri = self.uri.clone();
        let is_remote_uri = uri.contains("://");
        let is_hybrid = self.hybrid_remote_url.is_some();

        if is_hybrid && is_remote_uri {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Hybrid mode requires a local path as primary URI, found: {}",
                uri
            )));
        }

        let (storage_uri, data_store, local_store_opt) = if is_hybrid {
            let remote_url = self.hybrid_remote_url.as_ref().unwrap();

            // Remote Store (Data) - use explicit cloud_config if provided
            let remote_store: Arc<dyn ObjectStore> = if let Some(cloud_cfg) = &self.cloud_config {
                build_cloud_store(cloud_cfg).map_err(UniError::Internal)?
            } else {
                let url = url::Url::parse(remote_url).map_err(|e| {
                    UniError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        e.to_string(),
                    ))
                })?;
                let (os, _path) =
                    object_store::parse_url(&url).map_err(|e| UniError::Internal(e.into()))?;
                Arc::from(os)
            };

            // Local Store (WAL, IDs)
            let path = PathBuf::from(&uri);
            if path.exists() {
                if self.fail_if_exists {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Database already exists at {}",
                        uri
                    )));
                }
            } else {
                if !self.create_if_missing {
                    return Err(UniError::NotFound { path: path.clone() });
                }
                std::fs::create_dir_all(&path).map_err(UniError::Io)?;
            }

            let local_store = Arc::new(
                LocalFileSystem::new_with_prefix(&path).map_err(|e| UniError::Io(e.into()))?,
            );

            // For hybrid, storage_uri is the remote URL (since StorageManager loads datasets from there)
            // But we must provide the correct store to other components manually.
            (
                remote_url.clone(),
                remote_store,
                Some(local_store as Arc<dyn ObjectStore>),
            )
        } else if is_remote_uri {
            // Remote Only - use explicit cloud_config if provided
            let remote_store: Arc<dyn ObjectStore> = if let Some(cloud_cfg) = &self.cloud_config {
                build_cloud_store(cloud_cfg).map_err(UniError::Internal)?
            } else {
                let url = url::Url::parse(&uri).map_err(|e| {
                    UniError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        e.to_string(),
                    ))
                })?;
                let (os, _path) =
                    object_store::parse_url(&url).map_err(|e| UniError::Internal(e.into()))?;
                Arc::from(os)
            };

            (uri.clone(), remote_store, None)
        } else {
            // Local Only
            let path = PathBuf::from(&uri);
            let storage_path = path.join("storage");

            if path.exists() {
                if self.fail_if_exists {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Database already exists at {}",
                        uri
                    )));
                }
            } else {
                if !self.create_if_missing {
                    return Err(UniError::NotFound { path: path.clone() });
                }
                std::fs::create_dir_all(&path).map_err(UniError::Io)?;
            }

            // Ensure storage directory exists
            if !storage_path.exists() {
                std::fs::create_dir_all(&storage_path).map_err(UniError::Io)?;
            }

            let store = Arc::new(
                LocalFileSystem::new_with_prefix(&path).map_err(|e| UniError::Io(e.into()))?,
            );
            (
                storage_path.to_string_lossy().to_string(),
                store.clone() as Arc<dyn ObjectStore>,
                Some(store as Arc<dyn ObjectStore>),
            )
        };

        let schema_obj_path = object_store::path::Path::from("schema.json");

        // Load schema (SchemaManager::load creates a default if missing)
        // Schema is always in data_store (Remote or Local)
        let schema_manager = Arc::new(
            SchemaManager::load_from_store(data_store.clone(), &schema_obj_path)
                .await
                .map_err(UniError::Internal)?,
        );

        // Initialize storage
        // Note: StorageManager re-creates the store from `storage_uri` internally.
        // For Hybrid/Remote, this works because `storage_uri` is a URL.
        // For Local, `storage_uri` is a path.
        let storage = StorageManager::new_with_config(
            &storage_uri,
            schema_manager.clone(),
            self.config.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        let storage = Arc::new(storage);

        // Create shutdown handle
        let shutdown_handle = Arc::new(ShutdownHandle::new(Duration::from_secs(30)));

        // Start background compaction with shutdown signal
        let compaction_handle = storage
            .clone()
            .start_background_compaction(shutdown_handle.subscribe());
        shutdown_handle.track_task(compaction_handle);

        // Initialize property manager
        let prop_cache_capacity = self.config.cache_size / 1024;

        let prop_manager = Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            prop_cache_capacity,
        ));

        // Determine start version and WAL high water mark from latest snapshot
        let latest_snapshot = storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?;

        let start_version = latest_snapshot
            .as_ref()
            .map(|s| s.version_high_water_mark + 1)
            .unwrap_or(0);

        let wal_high_water_mark = latest_snapshot
            .as_ref()
            .map(|s| s.wal_high_water_mark)
            .unwrap_or(0);

        // Setup WAL and IdAllocator
        let id_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());
        let wal_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());

        let allocator = Arc::new(
            IdAllocator::new(
                id_store,
                object_store::path::Path::from("id_allocator.json"),
                1000,
            )
            .await
            .map_err(UniError::Internal)?,
        );

        let wal = if !self.config.wal_enabled {
            // WAL disabled by config
            None
        } else if is_remote_uri && !is_hybrid {
            // Remote-only WAL (ObjectStoreWal)
            Some(Arc::new(WriteAheadLog::new(
                wal_store,
                object_store::path::Path::from("wal"),
            )))
        } else if is_hybrid || !is_remote_uri {
            // Local WAL (using local_store)
            // Even if local_store uses ObjectStore trait, it maps to FS.
            Some(Arc::new(WriteAheadLog::new(
                wal_store,
                object_store::path::Path::from("wal"),
            )))
        } else {
            None
        };

        let writer = Arc::new(RwLock::new(
            Writer::new_with_config(
                storage.clone(),
                schema_manager.clone(),
                start_version,
                self.config.clone(),
                wal,
                Some(allocator),
            )
            .await
            .map_err(UniError::Internal)?,
        ));

        // Replay WAL to restore any uncommitted mutations from previous session
        // Only replay mutations with LSN > wal_high_water_mark to avoid double-applying
        {
            let w = writer.read().await;
            let replayed = w
                .replay_wal(wal_high_water_mark)
                .await
                .map_err(UniError::Internal)?;
            if replayed > 0 {
                info!("WAL recovery: replayed {} mutations", replayed);
            }
        }

        // Start background flush checker for time-based auto-flush
        if let Some(interval) = self.config.auto_flush_interval {
            let writer_clone = writer.clone();
            let mut shutdown_rx = shutdown_handle.subscribe();

            let handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            let mut w = writer_clone.write().await;
                            if let Err(e) = w.check_flush().await {
                                tracing::warn!("Background flush check failed: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            tracing::info!("Auto-flush shutting down, performing final flush");
                            let mut w = writer_clone.write().await;
                            let _ = w.flush_to_l1(None).await;
                            break;
                        }
                    }
                }
            });

            shutdown_handle.track_task(handle);
        }

        Ok(Uni {
            storage,
            schema: schema_manager,
            properties: prop_manager,
            writer: Some(writer),
            config: self.config,
            procedure_registry: Arc::new(uni_query::ProcedureRegistry::new()),
            shutdown_handle,
        })
    }

    /// Open the database (blocking)
    pub fn build_sync(self) -> Result<Uni> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        rt.block_on(self.build())
    }
}
