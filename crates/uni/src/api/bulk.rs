// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bulk loading API for high-throughput data ingestion.
//!
//! This module provides `BulkWriter` for efficiently loading large amounts of
//! vertices and edges while deferring index updates until commit time.
//!
//! ## Async Index Building
//!
//! By default, `commit()` blocks until all indexes are rebuilt. For large datasets,
//! you can enable async index building to return immediately while indexes are
//! built in the background:
//!
//! ```ignore
//! let stats = db.bulk_writer()
//!     .async_indexes(true)
//!     .build()?
//!     .insert_vertices(...)
//!     .await?
//!     .commit()
//!     .await?;
//!
//! // Data is queryable immediately (may use full scans)
//! // Check index status later:
//! let status = db.index_rebuild_status().await?;
//! ```

use crate::api::Uni;
use anyhow::{Result, anyhow};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot, SnapshotManifest};
use uni_common::{Properties, UniError};
use uni_store::storage::delta::{L1Entry, Op};
use uni_store::storage::main_edge::MainEdgeDataset;
use uni_store::storage::main_vertex::MainVertexDataset;
use uni_store::storage::{IndexManager, IndexRebuildManager};
use uuid::Uuid;

/// Builder for configuring a bulk writer.
pub struct BulkWriterBuilder<'a> {
    db: &'a Uni,
    config: BulkConfig,
    progress_callback: Option<Box<dyn Fn(BulkProgress) + Send>>,
}

impl<'a> BulkWriterBuilder<'a> {
    /// Create a new bulk writer builder with default configuration.
    pub fn new(db: &'a Uni) -> Self {
        Self {
            db,
            config: BulkConfig::default(),
            progress_callback: None,
        }
    }

    /// Set whether to defer vector index building until commit.
    pub fn defer_vector_indexes(mut self, defer: bool) -> Self {
        self.config.defer_vector_indexes = defer;
        self
    }

    /// Set whether to defer scalar index building until commit.
    pub fn defer_scalar_indexes(mut self, defer: bool) -> Self {
        self.config.defer_scalar_indexes = defer;
        self
    }

    /// Set the batch size for buffering before flush.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set a progress callback for monitoring bulk load progress.
    pub fn on_progress<F: Fn(BulkProgress) + Send + 'static>(mut self, f: F) -> Self {
        self.progress_callback = Some(Box::new(f));
        self
    }

    /// Build indexes asynchronously after commit.
    ///
    /// When enabled, `commit()` returns immediately after data is written,
    /// and indexes are rebuilt in the background. The data is queryable
    /// immediately but queries may use full scans until indexes are ready.
    ///
    /// Use `Uni::index_rebuild_status()` to check progress.
    ///
    /// Default: `false` (blocking index rebuild)
    pub fn async_indexes(mut self, async_: bool) -> Self {
        self.config.async_indexes = async_;
        self
    }

    /// Set whether to validate constraints during bulk load.
    ///
    /// When enabled (default), BulkWriter validates NOT NULL, UNIQUE, and CHECK
    /// constraints before each flush, matching the behavior of regular Writer.
    /// Set to `false` for trusted data sources to improve performance.
    ///
    /// Default: `true`
    pub fn validate_constraints(mut self, validate: bool) -> Self {
        self.config.validate_constraints = validate;
        self
    }

    /// Set the maximum buffer size before triggering a checkpoint flush.
    ///
    /// When the in-memory buffer exceeds this size, a checkpoint is triggered
    /// to flush data to storage. This allows bulk loading of arbitrarily large
    /// datasets while controlling memory usage.
    ///
    /// Default: 1 GB (1_073_741_824 bytes)
    pub fn max_buffer_size_bytes(mut self, size: usize) -> Self {
        self.config.max_buffer_size_bytes = size;
        self
    }

    /// Build the bulk writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is not writable.
    pub fn build(self) -> Result<BulkWriter<'a>> {
        if self.db.writer.is_none() {
            return Err(anyhow!("BulkWriter requires a writable database instance"));
        }

        Ok(BulkWriter {
            db: self.db,
            config: self.config,
            progress_callback: self.progress_callback,
            stats: BulkStats::default(),
            start_time: Instant::now(),
            pending_vertices: HashMap::new(),
            pending_edges: HashMap::new(),
            touched_labels: HashSet::new(),
            touched_edge_types: HashSet::new(),
            initial_table_versions: HashMap::new(),
            buffer_size_bytes: 0,
            committed: false,
        })
    }
}

/// Configuration for bulk loading operations.
pub struct BulkConfig {
    /// Whether to defer vector index building until commit.
    pub defer_vector_indexes: bool,
    /// Whether to defer scalar index building until commit.
    pub defer_scalar_indexes: bool,
    /// Number of rows to buffer before flushing to storage.
    pub batch_size: usize,
    /// Whether to build indexes asynchronously after commit.
    pub async_indexes: bool,
    /// Whether to validate constraints (NOT NULL, UNIQUE, CHECK) during bulk load.
    ///
    /// Default: `true`. Set to `false` to skip validation for trusted data sources.
    pub validate_constraints: bool,
    /// Maximum buffer size in bytes before triggering a checkpoint flush.
    ///
    /// Default: 1 GB (1_073_741_824 bytes). When buffer size exceeds this limit,
    /// a checkpoint is triggered to flush data to storage while continuing to
    /// accept new data.
    pub max_buffer_size_bytes: usize,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: 10_000,
            async_indexes: false,
            validate_constraints: true,
            max_buffer_size_bytes: 1_073_741_824, // 1 GB
        }
    }
}

#[derive(Debug, Clone)]
pub struct BulkProgress {
    pub phase: BulkPhase,
    pub rows_processed: usize,
    pub total_rows: Option<usize>,
    pub current_label: Option<String>,
    pub elapsed: Duration,
}

#[derive(Debug, Clone)]
pub enum BulkPhase {
    Inserting,
    RebuildingIndexes { label: String },
    Finalizing,
}

#[derive(Debug, Clone, Default)]
pub struct BulkStats {
    pub vertices_inserted: usize,
    pub edges_inserted: usize,
    pub indexes_rebuilt: usize,
    pub duration: Duration,
    pub index_build_duration: Duration,
    /// Task IDs for async index rebuilds (populated when `async_indexes` is true).
    pub index_task_ids: Vec<String>,
    /// True if index building was deferred to background (async mode).
    pub indexes_pending: bool,
}

/// Edge data for bulk insertion.
///
/// Contains source/destination vertex IDs and properties.
#[derive(Debug, Clone)]
pub struct EdgeData {
    /// Source vertex ID.
    pub src_vid: Vid,
    /// Destination vertex ID.
    pub dst_vid: Vid,
    /// Edge properties.
    pub properties: Properties,
}

impl EdgeData {
    /// Create new edge data.
    pub fn new(src_vid: Vid, dst_vid: Vid, properties: Properties) -> Self {
        Self {
            src_vid,
            dst_vid,
            properties,
        }
    }
}

/// Bulk writer for high-throughput data ingestion.
///
/// Buffers vertices and edges, deferring index updates until commit.
/// Supports constraint validation, automatic checkpointing when buffer limits
/// are exceeded, and proper rollback via LanceDB version tracking.
///
/// Use `abort()` to discard uncommitted changes and roll back storage to its
/// pre-bulk-load state.
pub struct BulkWriter<'a> {
    db: &'a Uni,
    config: BulkConfig,
    progress_callback: Option<Box<dyn Fn(BulkProgress) + Send>>,
    stats: BulkStats,
    start_time: Instant,
    // Buffered data per label/type
    pending_vertices: HashMap<String, Vec<(Vid, Properties)>>,
    pending_edges: HashMap<String, Vec<L1Entry>>,
    // Track what was written (for index rebuild)
    touched_labels: HashSet<String>,
    touched_edge_types: HashSet<String>,
    // Track LanceDB table versions before bulk load started (for abort rollback)
    // Key: table name, Value: version before first write (None = table created during bulk load)
    initial_table_versions: HashMap<String, Option<u64>>,
    // Current buffer size in bytes (approximate)
    buffer_size_bytes: usize,
    committed: bool,
}

impl<'a> BulkWriter<'a> {
    /// Returns the current timestamp in microseconds since Unix epoch.
    fn get_current_timestamp_micros(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    /// Insert vertices in bulk.
    ///
    /// The vertices are buffered until `batch_size` is reached, then written to storage.
    /// When constraint validation is enabled, constraints are checked before each flush.
    /// When the buffer size exceeds `max_buffer_size_bytes`, a checkpoint is triggered.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The label is not found in the schema
    /// - Constraint validation fails (when enabled)
    /// - Storage write fails
    pub async fn insert_vertices(
        &mut self,
        label: &str,
        vertices: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<Vid>> {
        let schema = self.db.schema.schema();
        // Validate label exists in schema
        schema
            .labels
            .get(label)
            .ok_or_else(|| UniError::LabelNotFound {
                label: label.to_string(),
            })?;
        // Validate constraints before buffering (if enabled)
        if self.config.validate_constraints {
            self.validate_vertex_batch_constraints(label, &vertices)
                .await?;
        }

        // Allocate VIDs (batched for performance)
        let vids = {
            let writer = self.db.writer.as_ref().unwrap().read().await;
            writer
                .allocate_vids(vertices.len())
                .await
                .map_err(UniError::Internal)?
        };

        // Track buffer size and add to buffer
        let buffer = self.pending_vertices.entry(label.to_string()).or_default();
        for (i, props) in vertices.into_iter().enumerate() {
            self.buffer_size_bytes += Self::estimate_properties_size(&props);
            buffer.push((vids[i], props));
        }

        self.touched_labels.insert(label.to_string());

        // Check if we need to checkpoint based on buffer size
        if self.buffer_size_bytes >= self.config.max_buffer_size_bytes {
            self.checkpoint().await?;
        } else {
            // Otherwise, check batch size threshold for this label only
            self.check_flush_vertices(label).await?;
        }

        self.stats.vertices_inserted += vids.len();
        self.report_progress(
            BulkPhase::Inserting,
            self.stats.vertices_inserted,
            Some(label.to_string()),
        );

        Ok(vids)
    }

    /// Estimate the size of a properties map in bytes.
    fn estimate_properties_size(props: &Properties) -> usize {
        let mut size = 0;
        for (key, value) in props {
            size += key.len();
            size += Self::estimate_value_size(value);
        }
        size
    }

    /// Estimate the size of a value in bytes.
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::Null => 1,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 8,
            Value::String(s) => s.len(),
            Value::Bytes(b) => b.len(),
            Value::List(arr) => arr.iter().map(Self::estimate_value_size).sum::<usize>() + 8,
            Value::Map(obj) => {
                obj.iter()
                    .map(|(k, v)| k.len() + Self::estimate_value_size(v))
                    .sum::<usize>()
                    + 8
            }
            Value::Vector(v) => v.len() * 4,
            _ => 16, // Node, Edge, Path
        }
    }

    /// Validate constraints for a batch of vertices before insertion.
    ///
    /// Checks NOT NULL, UNIQUE, and CHECK constraints. For UNIQUE constraints,
    /// validates both within the batch and against already-buffered data.
    async fn validate_vertex_batch_constraints(
        &self,
        label: &str,
        vertices: &[Properties],
    ) -> Result<()> {
        let schema = self.db.schema.schema();

        // Check NOT NULL and CHECK constraints for each vertex
        if let Some(props_meta) = schema.properties.get(label) {
            for (idx, props) in vertices.iter().enumerate() {
                // NOT NULL constraints
                for (prop_name, meta) in props_meta {
                    if !meta.nullable && props.get(prop_name).is_none_or(|v| v.is_null()) {
                        return Err(anyhow!(
                            "NOT NULL constraint violation at row {}: property '{}' cannot be null for label '{}'",
                            idx,
                            prop_name,
                            label
                        ));
                    }
                }
            }
        }

        // Check explicit constraints (UNIQUE, CHECK)
        for constraint in &schema.constraints {
            if !constraint.enabled {
                continue;
            }
            match &constraint.target {
                uni_common::core::schema::ConstraintTarget::Label(l) if l == label => {}
                _ => continue,
            }

            match &constraint.constraint_type {
                uni_common::core::schema::ConstraintType::Unique {
                    properties: unique_props,
                } => {
                    // Check for duplicates within the batch
                    let mut seen_keys: HashSet<String> = HashSet::new();
                    for (idx, props) in vertices.iter().enumerate() {
                        let key = self.compute_unique_key(unique_props, props);
                        if let Some(k) = key
                            && !seen_keys.insert(k.clone())
                        {
                            return Err(anyhow!(
                                "UNIQUE constraint violation at row {}: duplicate key '{}' in batch",
                                idx,
                                k
                            ));
                        }
                    }

                    // Check against already-buffered data
                    if let Some(buffered) = self.pending_vertices.get(label) {
                        for (idx, props) in vertices.iter().enumerate() {
                            let key = self.compute_unique_key(unique_props, props);
                            if let Some(k) = key {
                                for (_, buffered_props) in buffered {
                                    let buffered_key =
                                        self.compute_unique_key(unique_props, buffered_props);
                                    if buffered_key.as_ref() == Some(&k) {
                                        return Err(anyhow!(
                                            "UNIQUE constraint violation at row {}: key '{}' conflicts with buffered data",
                                            idx,
                                            k
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
                uni_common::core::schema::ConstraintType::Exists { property } => {
                    for (idx, props) in vertices.iter().enumerate() {
                        if props.get(property).is_none_or(|v| v.is_null()) {
                            return Err(anyhow!(
                                "EXISTS constraint violation at row {}: property '{}' must exist",
                                idx,
                                property
                            ));
                        }
                    }
                }
                uni_common::core::schema::ConstraintType::Check { expression } => {
                    for (idx, props) in vertices.iter().enumerate() {
                        if !self.evaluate_check_expression(expression, props)? {
                            return Err(anyhow!(
                                "CHECK constraint '{}' violated at row {}: expression '{}' evaluated to false",
                                constraint.name,
                                idx,
                                expression
                            ));
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Compute a unique key string from properties for UNIQUE constraint checking.
    fn compute_unique_key(&self, unique_props: &[String], props: &Properties) -> Option<String> {
        let mut parts = Vec::new();
        for prop in unique_props {
            match props.get(prop) {
                Some(v) if !v.is_null() => parts.push(v.to_string()),
                _ => return None, // Missing property means can't enforce uniqueness
            }
        }
        Some(parts.join(":"))
    }

    /// Evaluate a simple CHECK constraint expression.
    fn evaluate_check_expression(&self, expression: &str, properties: &Properties) -> Result<bool> {
        let parts: Vec<&str> = expression.split_whitespace().collect();
        if parts.len() != 3 {
            // Complex expression - allow for now
            return Ok(true);
        }

        let prop_part = parts[0].trim_start_matches('(');
        let prop_name = if let Some(idx) = prop_part.find('.') {
            &prop_part[idx + 1..]
        } else {
            prop_part
        };

        let op = parts[1];
        let val_str = parts[2].trim_end_matches(')');

        let prop_val = match properties.get(prop_name) {
            Some(v) => v,
            None => return Ok(true), // Missing property passes CHECK
        };

        // Parse target value
        let target_val = if (val_str.starts_with('\'') && val_str.ends_with('\''))
            || (val_str.starts_with('"') && val_str.ends_with('"'))
        {
            Value::String(val_str[1..val_str.len() - 1].to_string())
        } else if let Ok(n) = val_str.parse::<i64>() {
            Value::Int(n)
        } else if let Ok(n) = val_str.parse::<f64>() {
            Value::Float(n)
        } else if let Ok(b) = val_str.parse::<bool>() {
            Value::Bool(b)
        } else {
            Value::String(val_str.to_string())
        };

        match op {
            "=" | "==" => Ok(prop_val == &target_val),
            "!=" | "<>" => Ok(prop_val != &target_val),
            ">" => self
                .compare_json_values(prop_val, &target_val)
                .map(|c| c > 0),
            "<" => self
                .compare_json_values(prop_val, &target_val)
                .map(|c| c < 0),
            ">=" => self
                .compare_json_values(prop_val, &target_val)
                .map(|c| c >= 0),
            "<=" => self
                .compare_json_values(prop_val, &target_val)
                .map(|c| c <= 0),
            _ => Ok(true), // Unknown operator - allow
        }
    }

    /// Compare two values, returning -1, 0, or 1.
    fn compare_json_values(&self, a: &Value, b: &Value) -> Result<i8> {
        match (a, b) {
            (Value::Int(n1), Value::Int(n2)) => Ok(n1.cmp(n2) as i8),
            (Value::Float(f1), Value::Float(f2)) => {
                if f1 < f2 {
                    Ok(-1)
                } else if f1 > f2 {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            (Value::Int(n), Value::Float(f)) => {
                let nf = *n as f64;
                if nf < *f {
                    Ok(-1)
                } else if nf > *f {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            (Value::Float(f), Value::Int(n)) => {
                let nf = *n as f64;
                if *f < nf {
                    Ok(-1)
                } else if *f > nf {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            (Value::String(s1), Value::String(s2)) => match s1.cmp(s2) {
                std::cmp::Ordering::Less => Ok(-1),
                std::cmp::Ordering::Greater => Ok(1),
                std::cmp::Ordering::Equal => Ok(0),
            },
            _ => Err(anyhow!(
                "Cannot compare incompatible types: {:?} vs {:?}",
                a,
                b
            )),
        }
    }

    /// Checkpoint: flush all pending data to storage.
    ///
    /// Called automatically when buffer size exceeds `max_buffer_size_bytes`.
    /// Flushes all buffered vertices and edges, then resets the buffer size counter.
    async fn checkpoint(&mut self) -> Result<()> {
        log::debug!(
            "Checkpoint triggered at {} bytes (limit: {})",
            self.buffer_size_bytes,
            self.config.max_buffer_size_bytes
        );

        // Flush all pending vertices
        let labels: Vec<String> = self.pending_vertices.keys().cloned().collect();
        for label in labels {
            self.flush_vertices_buffer(&label).await?;
        }

        // Flush all pending edges
        let edge_types: Vec<String> = self.pending_edges.keys().cloned().collect();
        for edge_type in edge_types {
            self.flush_edges_buffer(&edge_type).await?;
        }

        // Reset buffer size
        self.buffer_size_bytes = 0;

        Ok(())
    }

    // Helper to flush vertex buffer if full
    async fn check_flush_vertices(&mut self, label: &str) -> Result<()> {
        let should_flush = {
            if let Some(buf) = self.pending_vertices.get(label) {
                buf.len() >= self.config.batch_size
            } else {
                false
            }
        };

        if should_flush {
            self.flush_vertices_buffer(label).await?;
        }
        Ok(())
    }

    /// Flush vertex buffer to LanceDB storage.
    ///
    /// Records the initial table version before first write for rollback support.
    /// Writes to both per-label table and main vertices table.
    async fn flush_vertices_buffer(&mut self, label: &str) -> Result<()> {
        if let Some(vertices) = self.pending_vertices.remove(label) {
            if vertices.is_empty() {
                return Ok(());
            }

            // Record initial version for abort rollback (only once per table)
            let table_name = uni_store::lancedb::LanceDbStore::vertex_table_name(label);
            if !self.initial_table_versions.contains_key(&table_name) {
                let lancedb_store = self.db.storage.lancedb_store();
                let version = lancedb_store
                    .get_table_version(&table_name)
                    .await
                    .map_err(UniError::Internal)?;
                self.initial_table_versions.insert(table_name, version);
            }

            // Record main vertices table version for rollback
            let main_table_name =
                uni_store::lancedb::LanceDbStore::main_vertex_table_name().to_string();
            if !self.initial_table_versions.contains_key(&main_table_name) {
                let lancedb_store = self.db.storage.lancedb_store();
                let version = lancedb_store
                    .get_table_version(&main_table_name)
                    .await
                    .map_err(UniError::Internal)?;
                self.initial_table_versions
                    .insert(main_table_name.clone(), version);
            }

            let ds = self
                .db
                .storage
                .vertex_dataset(label)
                .map_err(UniError::Internal)?;
            let schema = self.db.schema.schema();

            let deleted = vec![false; vertices.len()];
            let versions = vec![1; vertices.len()]; // Version 1 for bulk load

            // Generate timestamps for this batch
            let now = self.get_current_timestamp_micros();
            let mut created_at: HashMap<Vid, i64> = HashMap::new();
            let mut updated_at: HashMap<Vid, i64> = HashMap::new();
            for (vid, _) in &vertices {
                created_at.insert(*vid, now);
                updated_at.insert(*vid, now);
            }

            // Build per-label and main-vertex entries from the 2-tuple input.
            // Both tables need labels attached; compute once per vertex.
            let labels = vec![label.to_string()];
            let vertices_with_labels: Vec<(Vid, Vec<String>, Properties)> = vertices
                .iter()
                .map(|(vid, props)| (*vid, labels.clone(), props.clone()))
                .collect();

            let batch = ds
                .build_record_batch_with_timestamps(
                    &vertices_with_labels,
                    &deleted,
                    &versions,
                    &schema,
                    Some(&created_at),
                    Some(&updated_at),
                )
                .map_err(UniError::Internal)?;

            // Write to per-label LanceDB table
            let lancedb_store = self.db.storage.lancedb_store();
            let table = ds
                .write_batch_lancedb(lancedb_store, batch, &schema)
                .await
                .map_err(UniError::Internal)?;

            // Create default scalar indexes (_vid, _uid) which are critical for basic function
            ds.ensure_default_indexes_lancedb(&table)
                .await
                .map_err(UniError::Internal)?;

            // Dual-write to main vertices table
            let main_vertices: Vec<(Vid, Vec<String>, Properties, bool, u64)> =
                vertices_with_labels
                    .into_iter()
                    .map(|(vid, lbls, props)| (vid, lbls, props, false, 1u64))
                    .collect();

            if !main_vertices.is_empty() {
                let main_batch = MainVertexDataset::build_record_batch(
                    &main_vertices,
                    Some(&created_at),
                    Some(&updated_at),
                )
                .map_err(UniError::Internal)?;

                let main_table = MainVertexDataset::write_batch_lancedb(lancedb_store, main_batch)
                    .await
                    .map_err(UniError::Internal)?;

                MainVertexDataset::ensure_default_indexes_lancedb(&main_table)
                    .await
                    .map_err(UniError::Internal)?;
            }
        }
        Ok(())
    }

    /// Insert edges in bulk.
    ///
    /// Edges are buffered until `batch_size` is reached, then written to storage.
    /// When the buffer size exceeds `max_buffer_size_bytes`, a checkpoint is triggered.
    /// Indexes are NOT updated during these writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the edge type is not found in the schema or if
    /// storage write fails.
    pub async fn insert_edges(
        &mut self,
        edge_type: &str,
        edges: Vec<EdgeData>,
    ) -> Result<Vec<Eid>> {
        let schema = self.db.schema.schema();
        let edge_meta =
            schema
                .edge_types
                .get(edge_type)
                .ok_or_else(|| UniError::EdgeTypeNotFound {
                    edge_type: edge_type.to_string(),
                })?;
        let type_id = edge_meta.id;

        // Allocate EIDs
        let mut eids = Vec::with_capacity(edges.len());
        {
            let writer = self.db.writer.as_ref().unwrap().read().await;
            for _ in 0..edges.len() {
                eids.push(writer.next_eid(type_id).await.map_err(UniError::Internal)?);
            }
        }

        // Convert to L1Entry format and track buffer size
        let now = self.get_current_timestamp_micros();
        let mut added_size = 0usize;
        let entries: Vec<L1Entry> = edges
            .into_iter()
            .enumerate()
            .map(|(i, edge)| {
                // Estimate size for buffer tracking (16 bytes for VIDs + 8 for EID + properties)
                added_size += 32 + Self::estimate_properties_size(&edge.properties);
                L1Entry {
                    src_vid: edge.src_vid,
                    dst_vid: edge.dst_vid,
                    eid: eids[i],
                    op: Op::Insert,
                    version: 1,
                    properties: edge.properties,
                    created_at: Some(now),
                    updated_at: Some(now),
                }
            })
            .collect();
        self.buffer_size_bytes += added_size;
        self.pending_edges
            .entry(edge_type.to_string())
            .or_default()
            .extend(entries);

        self.touched_edge_types.insert(edge_type.to_string());

        // Check if we need to checkpoint based on buffer size
        if self.buffer_size_bytes >= self.config.max_buffer_size_bytes {
            self.checkpoint().await?;
        } else {
            self.check_flush_edges(edge_type).await?;
        }

        self.stats.edges_inserted += eids.len();
        self.report_progress(
            BulkPhase::Inserting,
            self.stats.vertices_inserted + self.stats.edges_inserted,
            Some(edge_type.to_string()),
        );

        Ok(eids)
    }

    /// Check and flush edge buffer if full.
    async fn check_flush_edges(&mut self, edge_type: &str) -> Result<()> {
        let should_flush = self
            .pending_edges
            .get(edge_type)
            .is_some_and(|buf| buf.len() >= self.config.batch_size);

        if should_flush {
            self.flush_edges_buffer(edge_type).await?;
        }
        Ok(())
    }

    /// Flush edge buffer to delta datasets.
    ///
    /// Records initial table versions before first write for rollback support.
    /// Writes to both per-type delta tables and main edges table.
    #[expect(
        clippy::map_entry,
        reason = "async code between contains_key and insert"
    )]
    async fn flush_edges_buffer(&mut self, edge_type: &str) -> Result<()> {
        if let Some(entries) = self.pending_edges.remove(edge_type) {
            if entries.is_empty() {
                return Ok(());
            }

            let schema = self.db.schema.schema();
            let lancedb_store = self.db.storage.lancedb_store();

            // Record initial versions for abort rollback (FWD and BWD tables)
            let fwd_table_name =
                uni_store::lancedb::LanceDbStore::delta_table_name(edge_type, "fwd");
            if !self.initial_table_versions.contains_key(&fwd_table_name) {
                let version = lancedb_store
                    .get_table_version(&fwd_table_name)
                    .await
                    .map_err(UniError::Internal)?;
                self.initial_table_versions.insert(fwd_table_name, version);
            }
            let bwd_table_name =
                uni_store::lancedb::LanceDbStore::delta_table_name(edge_type, "bwd");
            if !self.initial_table_versions.contains_key(&bwd_table_name) {
                let version = lancedb_store
                    .get_table_version(&bwd_table_name)
                    .await
                    .map_err(UniError::Internal)?;
                self.initial_table_versions.insert(bwd_table_name, version);
            }

            // Record main edges table version for rollback
            let main_edge_table_name =
                uni_store::lancedb::LanceDbStore::main_edge_table_name().to_string();
            if !self
                .initial_table_versions
                .contains_key(&main_edge_table_name)
            {
                let version = lancedb_store
                    .get_table_version(&main_edge_table_name)
                    .await
                    .map_err(UniError::Internal)?;
                self.initial_table_versions
                    .insert(main_edge_table_name.clone(), version);
            }

            // Write to FWD delta (sorted by src_vid)
            let mut fwd_entries = entries.clone();
            fwd_entries.sort_by_key(|e| e.src_vid);
            let fwd_ds = self
                .db
                .storage
                .delta_dataset(edge_type, "fwd")
                .map_err(UniError::Internal)?;
            let fwd_batch = fwd_ds
                .build_record_batch(&fwd_entries, &schema)
                .map_err(UniError::Internal)?;
            let fwd_table = fwd_ds
                .write_run_lancedb(lancedb_store, fwd_batch)
                .await
                .map_err(UniError::Internal)?;
            fwd_ds
                .ensure_eid_index_lancedb(&fwd_table)
                .await
                .map_err(UniError::Internal)?;

            // Write to BWD delta (sorted by dst_vid)
            let mut bwd_entries = entries.clone();
            bwd_entries.sort_by_key(|e| e.dst_vid);
            let bwd_ds = self
                .db
                .storage
                .delta_dataset(edge_type, "bwd")
                .map_err(UniError::Internal)?;
            let bwd_batch = bwd_ds
                .build_record_batch(&bwd_entries, &schema)
                .map_err(UniError::Internal)?;
            let bwd_table = bwd_ds
                .write_run_lancedb(lancedb_store, bwd_batch)
                .await
                .map_err(UniError::Internal)?;
            bwd_ds
                .ensure_eid_index_lancedb(&bwd_table)
                .await
                .map_err(UniError::Internal)?;

            // Dual-write to main edges table
            let mut edge_created_at: HashMap<Eid, i64> = HashMap::new();
            let mut edge_updated_at: HashMap<Eid, i64> = HashMap::new();
            let main_edges: Vec<(Eid, Vid, Vid, String, Properties, bool, u64)> = entries
                .iter()
                .map(|e| {
                    let deleted = matches!(e.op, Op::Delete);
                    if let Some(ts) = e.created_at {
                        edge_created_at.insert(e.eid, ts);
                    }
                    if let Some(ts) = e.updated_at {
                        edge_updated_at.insert(e.eid, ts);
                    }
                    (
                        e.eid,
                        e.src_vid,
                        e.dst_vid,
                        edge_type.to_string(),
                        e.properties.clone(),
                        deleted,
                        e.version,
                    )
                })
                .collect();

            if !main_edges.is_empty() {
                let main_batch = MainEdgeDataset::build_record_batch(
                    &main_edges,
                    Some(&edge_created_at),
                    Some(&edge_updated_at),
                )
                .map_err(UniError::Internal)?;

                let main_table = MainEdgeDataset::write_batch_lancedb(lancedb_store, main_batch)
                    .await
                    .map_err(UniError::Internal)?;

                MainEdgeDataset::ensure_default_indexes_lancedb(&main_table)
                    .await
                    .map_err(UniError::Internal)?;
            }
        }
        Ok(())
    }

    /// Commit all pending data and rebuild indexes.
    ///
    /// Flushes remaining buffered data, rebuilds deferred indexes, and updates
    /// the snapshot manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing, index rebuilding, or snapshot update fails.
    pub async fn commit(mut self) -> Result<BulkStats> {
        // 1. Flush remaining vertex buffers
        let labels: Vec<String> = self.pending_vertices.keys().cloned().collect();
        for label in labels {
            self.flush_vertices_buffer(&label).await?;
        }

        // 2. Flush remaining edge buffers
        let edge_types: Vec<String> = self.pending_edges.keys().cloned().collect();
        for edge_type in edge_types {
            self.flush_edges_buffer(&edge_type).await?;
        }

        let index_start = Instant::now();

        // 3. Rebuild indexes for vertices
        if self.config.defer_vector_indexes || self.config.defer_scalar_indexes {
            let labels_to_rebuild: Vec<String> = self.touched_labels.iter().cloned().collect();

            if self.config.async_indexes && !labels_to_rebuild.is_empty() {
                // Async mode: schedule rebuilds in background
                let rebuild_manager = IndexRebuildManager::new(
                    self.db.storage.clone(),
                    self.db.schema.clone(),
                    self.db.config.index_rebuild.clone(),
                )
                .await
                .map_err(UniError::Internal)?;

                let task_ids = rebuild_manager
                    .schedule(labels_to_rebuild)
                    .await
                    .map_err(UniError::Internal)?;

                self.stats.index_task_ids = task_ids;
                self.stats.indexes_pending = true;

                // Start background worker if not already running
                // Note: The Uni instance should manage the IndexRebuildManager lifecycle
                // For now, we start a new worker for each bulk commit
                let manager = Arc::new(rebuild_manager);
                let handle = manager.start_background_worker(self.db.shutdown_handle.subscribe());
                self.db.shutdown_handle.track_task(handle);
            } else {
                // Sync mode: rebuild indexes blocking
                for label in &labels_to_rebuild {
                    self.report_progress(
                        BulkPhase::RebuildingIndexes {
                            label: label.clone(),
                        },
                        self.stats.vertices_inserted + self.stats.edges_inserted,
                        Some(label.clone()),
                    );
                    let idx_mgr = IndexManager::new(
                        self.db.storage.base_path(),
                        self.db.storage.schema_manager_arc(),
                        self.db.storage.lancedb_store_arc(),
                    );
                    idx_mgr
                        .rebuild_indexes_for_label(label)
                        .await
                        .map_err(UniError::Internal)?;
                    self.stats.indexes_rebuilt += 1;
                }
            }
        }

        self.stats.index_build_duration = index_start.elapsed();

        // 4. Update Snapshot
        self.report_progress(
            BulkPhase::Finalizing,
            self.stats.vertices_inserted + self.stats.edges_inserted,
            None,
        );

        // Load latest snapshot or create new
        let mut manifest = self
            .db
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?
            .unwrap_or_else(|| {
                SnapshotManifest::new(
                    Uuid::new_v4().to_string(),
                    self.db.schema.schema().schema_version,
                )
            });

        // Update Manifest
        let parent_id = manifest.snapshot_id.clone();
        manifest.parent_snapshot = Some(parent_id);
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.created_at = Utc::now();

        // Update counts and versions for touched labels (vertices)
        let lancedb_store = self.db.storage.lancedb_store();
        for label in &self.touched_labels {
            let table = lancedb_store
                .open_vertex_table(label)
                .await
                .map_err(UniError::Internal)?;
            let count = table
                .count_rows(None)
                .await
                .map_err(|e| UniError::Internal(anyhow::anyhow!("Count rows failed: {}", e)))?;

            let current_snap =
                manifest
                    .vertices
                    .entry(label.to_string())
                    .or_insert(LabelSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.count = count as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;
        }

        // Update counts and versions for touched edge types
        for edge_type in &self.touched_edge_types {
            if let Ok(table) = lancedb_store.open_delta_table(edge_type, "fwd").await {
                let count = table
                    .count_rows(None)
                    .await
                    .map_err(|e| UniError::Internal(anyhow::anyhow!("Count rows failed: {}", e)))?;

                let current_snap =
                    manifest
                        .edges
                        .entry(edge_type.to_string())
                        .or_insert(EdgeSnapshot {
                            version: 0,
                            count: 0,
                            lance_version: 0,
                        });
                current_snap.count = count as u64;
                // LanceDB tables don't expose Lance version directly
                current_snap.lance_version = 0;
            }
        }

        // Save Snapshot
        self.db
            .storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await
            .map_err(UniError::Internal)?;
        self.db
            .storage
            .snapshot_manager()
            .set_latest_snapshot(&manifest.snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        // Warm adjacency CSRs for all edge types written during bulk import
        // so that subsequent traversal queries can find the edges.
        let schema = self.db.storage.schema_manager().schema();
        for edge_type_name in &self.touched_edge_types {
            if let Some(meta) = schema.edge_types.get(edge_type_name.as_str()) {
                let type_id = meta.id;
                for &dir in uni_store::storage::direction::Direction::Both.expand() {
                    let _ = self.db.storage.warm_adjacency(type_id, dir, None).await;
                }
            }
        }

        self.committed = true;
        self.stats.duration = self.start_time.elapsed();
        Ok(self.stats)
    }

    /// Abort bulk loading and roll back all changes.
    ///
    /// Rolls back LanceDB tables to their pre-bulk-load versions using LanceDB's
    /// version API. Tables created during the bulk load are dropped. Buffered
    /// data that hasn't been flushed is discarded.
    ///
    /// # Errors
    ///
    /// Returns an error if rollback fails. The error message includes details
    /// about which tables failed to roll back.
    pub async fn abort(mut self) -> Result<()> {
        if self.committed {
            return Err(anyhow!("Cannot abort: bulk load already committed"));
        }

        // 1. Clear pending buffers (not yet flushed to storage)
        self.pending_vertices.clear();
        self.pending_edges.clear();
        self.buffer_size_bytes = 0;

        // 2. Roll back each modified table to its initial version
        let lancedb_store = self.db.storage.lancedb_store();
        let mut rollback_errors = Vec::new();
        let mut rolled_back_count = 0;
        let mut dropped_count = 0;

        for (table_name, initial_version) in &self.initial_table_versions {
            match initial_version {
                Some(version) => {
                    // Table existed before - rollback to initial version
                    match lancedb_store.rollback_table(table_name, *version).await {
                        Ok(()) => {
                            log::info!("Rolled back table '{}' to version {}", table_name, version);
                            rolled_back_count += 1;
                        }
                        Err(e) => {
                            rollback_errors.push(format!("{}: {}", table_name, e));
                        }
                    }
                }
                None => {
                    // Table was created during bulk load - drop it
                    match lancedb_store.drop_table(table_name).await {
                        Ok(()) => {
                            log::info!("Dropped table '{}' (created during bulk load)", table_name);
                            dropped_count += 1;
                        }
                        Err(e) => {
                            rollback_errors.push(format!("{}: {}", table_name, e));
                        }
                    }
                }
            }
        }

        // 3. Clear table cache to ensure next read picks up rolled-back state
        self.db.storage.clear_table_cache();

        if rollback_errors.is_empty() {
            log::info!(
                "Bulk load aborted successfully. Rolled back {} tables, dropped {} tables.",
                rolled_back_count,
                dropped_count
            );
            Ok(())
        } else {
            Err(anyhow!(
                "Bulk load abort had {} rollback errors: {}",
                rollback_errors.len(),
                rollback_errors.join("; ")
            ))
        }
    }

    fn report_progress(&self, phase: BulkPhase, rows: usize, label: Option<String>) {
        if let Some(cb) = &self.progress_callback {
            cb(BulkProgress {
                phase,
                rows_processed: rows,
                total_rows: None,
                current_label: label,
                elapsed: self.start_time.elapsed(),
            });
        }
    }
}
