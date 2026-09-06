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

use anyhow::{Result, anyhow};
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uni_common::UniConfig;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::SchemaManager;
use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot, SnapshotManifest};
use uni_common::{Properties, UniError};
use uni_plugin_host::shutdown::ShutdownHandle;
use uni_store::runtime::writer::Writer;
use uni_store::storage::delta::{L1Entry, Op};
use uni_store::storage::main_edge::MainEdgeDataset;
use uni_store::storage::main_vertex::MainVertexDataset;
use uni_store::storage::manager::StorageManager;
use uni_store::storage::{IndexManager, IndexRebuildManager};
use uuid::Uuid;

use crate::flush_intent;

/// Test-only fault-injection: when set, `flush_vertices_buffer` returns an error
/// *after* committing the per-label table but *before* the main table, exactly
/// reproducing the crash-in-the-middle scenario H9 guards against. Always
/// compiled (so integration tests across the crate boundary can arm it) but only
/// ever set by tests.
#[doc(hidden)]
pub static FAIL_AFTER_PERLABEL_WRITE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Concrete handle bundle injected into the bulk write path.
///
/// `BulkWriter`/`BulkWriterBuilder`/`StreamingAppender` need only field access
/// to the database's storage, writer, schema, shutdown coordinator, and config —
/// they call no methods on the owning `uni_db::Uni` inner state. The uni-db
/// driver constructs a `BulkBackend` from its `UniInner` fields and hands it in,
/// avoiding a dependency cycle.
#[derive(Clone)]
pub struct BulkBackend {
    /// Storage manager (datasets, backend, snapshot manager).
    pub storage: Arc<StorageManager>,
    /// Writer for ID allocation. `None` on a read-only database.
    pub writer: Option<Arc<Writer>>,
    /// Schema manager (labels, edge types, constraints, index metadata).
    pub schema: Arc<SchemaManager>,
    /// Shutdown coordinator — tracks background index-rebuild tasks.
    pub shutdown: Arc<ShutdownHandle>,
    /// Database configuration (e.g. index-rebuild tuning).
    pub config: UniConfig,
}

/// Trait for types that can be converted to property maps for bulk insertion.
///
/// Enables `insert_vertices` to accept both `Vec<HashMap<String, Value>>`
/// and `RecordBatch` (Arrow columnar data).
pub trait IntoArrow {
    /// Convert to a vector of property maps.
    ///
    /// # Errors
    ///
    /// Returns an error if a row cannot be converted (#233).
    fn into_property_maps(self) -> Result<Vec<HashMap<String, Value>>>;
}

impl IntoArrow for Vec<HashMap<String, Value>> {
    fn into_property_maps(self) -> Result<Vec<HashMap<String, Value>>> {
        Ok(self)
    }
}

impl IntoArrow for arrow_array::RecordBatch {
    fn into_property_maps(self) -> Result<Vec<HashMap<String, Value>>> {
        record_batch_to_property_maps(&self)
    }
}

/// Convert each row of an Arrow `RecordBatch` to a property map.
///
/// Columns become property keys; values are converted from Arrow types to Uni
/// [`Value`]s via `arrow_to_value`. Null values are omitted from the map.
///
/// # Errors
///
/// Returns an error if a non-null cell cannot be decoded to a [`Value`].
pub fn record_batch_to_property_maps(
    batch: &arrow_array::RecordBatch,
) -> Result<Vec<HashMap<String, Value>>> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let mut props = HashMap::with_capacity(schema.fields().len());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let value =
                uni_store::storage::arrow_convert::arrow_to_value(col.as_ref(), row_idx, None);
            // #233 Tier 1: `arrow_to_value` answers `Null` both for a genuine
            // null AND for a type it has no decoder arm for. Dropping every
            // null therefore dropped the whole column for an unsupported type
            // while `BulkStats` still reported a full ingest. Comparing
            // against the Arrow null bitmap separates the two.
            if value.is_null() && !col.is_null(row_idx) {
                return Err(anyhow::anyhow!(
                    "bulk ingest: column `{}` of type {} could not be decoded at row {row_idx}; \
                     silently dropping it would report a full ingest with the property missing \
                     from every row",
                    field.name(),
                    col.data_type()
                ));
            }
            if !value.is_null() {
                props.insert(field.name().clone(), value);
            }
        }
        rows.push(props);
    }
    Ok(rows)
}

/// Builder for configuring a bulk writer.
pub struct BulkWriterBuilder {
    backend: BulkBackend,
    config: BulkConfig,
    progress_callback: Option<Box<dyn Fn(BulkProgress) + Send>>,
}

impl BulkWriterBuilder {
    /// Create a bulk writer builder.
    ///
    /// Used by `Transaction::bulk_writer()` and `AppenderBuilder` — the
    /// Transaction already holds the session write guard, so the BulkWriter
    /// neither acquires nor releases one.
    pub fn new_unguarded(backend: BulkBackend) -> Self {
        Self {
            backend,
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
    pub fn build(self) -> Result<BulkWriter> {
        if self.backend.writer.is_none() {
            return Err(anyhow!("BulkWriter requires a writable database instance"));
        }

        Ok(BulkWriter {
            backend: self.backend,
            config: self.config,
            progress_callback: self.progress_callback,
            stats: BulkStats::default(),
            start_time: Instant::now(),
            pending_vertices: HashMap::new(),
            pending_edges: HashMap::new(),
            touched_labels: HashSet::new(),
            touched_edge_types: HashSet::new(),
            initial_table_versions: HashMap::new(),
            seen_unique_keys: HashMap::new(),
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
pub struct BulkWriter {
    backend: BulkBackend,
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
    // UNIQUE-constraint keys accepted over the WHOLE load (not just the current
    // buffer, which `flush_vertices_buffer` drains): without this, a duplicate
    // spanning two flushes slipped through. Keyed by constraint identity
    // (label + unique props), value = set of value-keys seen. (review H8)
    seen_unique_keys: HashMap<String, HashSet<String>>,
    // Current buffer size in bytes (approximate)
    buffer_size_bytes: usize,
    committed: bool,
}

/// Stable identity for a UNIQUE constraint's seen-key namespace: its label plus
/// its property list. `compute_unique_key` encodes only VALUES, so two different
/// UNIQUE constraints on the same label must not share a key namespace.
fn unique_set_key(label: &str, unique_props: &[String]) -> String {
    let mut s = String::from(label);
    for p in unique_props {
        s.push('\u{1f}'); // unit separator — won't appear in a label/prop name
        s.push_str(p);
    }
    s
}

impl BulkWriter {
    /// Returns a snapshot of the current bulk load statistics.
    /// Updated after each batch flush.
    pub fn stats(&self) -> &BulkStats {
        &self.stats
    }

    /// Returns the set of vertex labels that have been written to.
    pub fn touched_labels(&self) -> Vec<String> {
        self.touched_labels.iter().cloned().collect()
    }

    /// Returns the set of edge types that have been written to.
    pub fn touched_edge_types(&self) -> Vec<String> {
        self.touched_edge_types.iter().cloned().collect()
    }

    /// Returns the current timestamp in microseconds since Unix epoch.
    fn get_current_timestamp_micros() -> i64 {
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
        vertices: impl IntoArrow,
    ) -> Result<Vec<Vid>> {
        let vertices = vertices.into_property_maps()?;
        let schema = self.backend.schema.schema();
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
            let writer = self.backend.writer.as_ref().unwrap();
            writer
                .allocate_vids(vertices.len())
                .await
                .map_err(UniError::Internal)?
        };

        // Track buffer size and add to buffer
        let added_size: usize = vertices.iter().map(Self::estimate_properties_size).sum();
        self.buffer_size_bytes += added_size;
        self.pending_vertices
            .entry(label.to_string())
            .or_default()
            .extend(vids.iter().copied().zip(vertices));

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
        &mut self,
        label: &str,
        vertices: &[Properties],
    ) -> Result<()> {
        let schema = self.backend.schema.schema();

        // UNIQUE keys to commit to the writer-lifetime set once EVERY constraint
        // on this batch has passed. (review H8)
        let mut pending_unique_records: Vec<(String, Vec<String>)> = Vec::new();

        // Check NOT NULL and CHECK constraints for each vertex
        if let Some(props_meta) = schema.properties.get(label) {
            for (idx, props) in vertices.iter().enumerate() {
                // NOT NULL constraints
                for (prop_name, meta) in props_meta {
                    // Declared vector/multi-vector columns: enforce dimensions so a
                    // wrong-length vector fails the bulk write instead of being nulled
                    // by the Arrow converters at flush (issue #137).
                    if let Some(value) = props.get(prop_name)
                        && let Err(e) = meta.r#type.check_vector_dims(value)
                    {
                        return Err(anyhow!(
                            "vector dimension mismatch at row {}: property '{}' for label '{}': {}",
                            idx,
                            prop_name,
                            label,
                            e
                        ));
                    }
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

            // Unique and NodeKey share the uniqueness machinery; NodeKey adds a
            // NOT-NULL pre-check on every key property (a missing/null key is a
            // violation, whereas Unique simply skips enforcement for that row).
            match &constraint.constraint_type {
                ct if ct.unique_properties().is_some() => {
                    let unique_props = ct.unique_properties().expect("guarded by is_some");
                    let is_node_key =
                        matches!(ct, uni_common::core::schema::ConstraintType::NodeKey { .. });
                    let kind = if is_node_key { "NODE KEY" } else { "UNIQUE" };

                    // NodeKey NOT-NULL half.
                    if is_node_key {
                        for (idx, props) in vertices.iter().enumerate() {
                            for prop in unique_props {
                                if props.get(prop).is_none_or(|v| v.is_null()) {
                                    return Err(anyhow!(
                                        "NODE KEY constraint '{}' violated at row {}: property '{}' must exist and be non-null",
                                        constraint.name,
                                        idx,
                                        prop
                                    ));
                                }
                            }
                        }
                    }

                    // Check for duplicates within the batch
                    let mut seen_keys: HashSet<String> = HashSet::new();
                    for (idx, props) in vertices.iter().enumerate() {
                        let key = self.compute_unique_key(unique_props, props);
                        if let Some(k) = key
                            && !seen_keys.insert(k.clone())
                        {
                            return Err(anyhow!(
                                "{} constraint violation at row {}: duplicate key '{}' in batch",
                                kind,
                                idx,
                                k
                            ));
                        }
                    }

                    // Check against EVERY vertex accepted so far in this load,
                    // not just the current buffer (`pending_vertices` is drained
                    // on each flush, so a duplicate spanning two flushes used to
                    // slip through). (review H8)
                    let set_key = unique_set_key(label, unique_props);
                    let mut batch_keys = Vec::with_capacity(vertices.len());
                    for (idx, props) in vertices.iter().enumerate() {
                        if let Some(k) = self.compute_unique_key(unique_props, props) {
                            if self
                                .seen_unique_keys
                                .get(&set_key)
                                .is_some_and(|seen| seen.contains(&k))
                            {
                                return Err(anyhow!(
                                    "{} constraint violation at row {}: key '{}' conflicts with an already-loaded vertex",
                                    kind,
                                    idx,
                                    k
                                ));
                            }

                            // Also consult the main Writer's full write horizon
                            // (current L0 + pending-flush buffers + committed
                            // storage). The seen_unique_keys set only covers this
                            // bulk writer's lifetime, so a key committed on the
                            // main channel — flushed OR still in an unflushed L0
                            // buffer — was invisible and silently twinned.
                            // Delegates to the same lookup surface the
                            // single-vertex path uses. (findings uni-bulk[4] / D6)
                            if self
                                .unique_key_exists_full_horizon(label, unique_props, props)
                                .await?
                            {
                                return Err(anyhow!(
                                    "{} constraint violation at row {}: key '{}' conflicts with a committed row",
                                    kind,
                                    idx,
                                    k
                                ));
                            }
                            batch_keys.push(k);
                        }
                    }
                    pending_unique_records.push((set_key, batch_keys));
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
                        if !uni_common::core::check_constraint::evaluate(expression, props)? {
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

        // All constraints passed — commit the UNIQUE keys to the writer-lifetime
        // set so a later batch (after a buffer flush) still sees them. (review H8)
        for (set_key, keys) in pending_unique_records {
            self.seen_unique_keys
                .entry(set_key)
                .or_default()
                .extend(keys);
        }

        Ok(())
    }

    /// Compute a unique key string from properties for UNIQUE constraint checking.
    fn compute_unique_key(&self, unique_props: &[String], props: &Properties) -> Option<String> {
        // Build the key from length-prefixed, type-tagged codec bytes rather than
        // a lossy `Display` join. `to_string()` collided distinct values —
        // Int(1) vs Float(1.0) both render "1", ("x:y","z") vs ("x","y:z") both
        // render "x:y:z", and equal-length Bytes rendered alike. The codec tags
        // the type and the length prefix makes field boundaries unambiguous.
        // (finding uni-bulk[3])
        let mut buf: Vec<u8> = Vec::new();
        for prop in unique_props {
            let v = match props.get(prop) {
                Some(v) if !v.is_null() => v,
                _ => return None, // Missing property means can't enforce uniqueness
            };
            let enc = uni_common::cypher_value_codec::encode(v);
            buf.extend_from_slice(&(enc.len() as u64).to_le_bytes());
            buf.extend_from_slice(&enc);
        }
        // Hex so the (binary) key stays a String for the dedup sets.
        Some(
            buf.iter()
                .fold(String::with_capacity(buf.len() * 2), |mut s, b| {
                    use std::fmt::Write as _;
                    let _ = write!(s, "{b:02x}");
                    s
                }),
        )
    }

    /// Whether a committed row (L1/L2) already holds this UNIQUE key.
    ///
    /// Mirrors the storage step of the single-vertex
    /// `Writer::check_unique_constraint_multi`: build a typed `prop = value`
    /// filter over the unique properties and count live rows. Returns `false`
    /// when any unique property is missing/null (can't enforce), or when the
    /// label's vertex table isn't flushed yet.
    ///
    /// # Errors
    ///
    /// Propagates backend scan/count errors rather than failing open.
    /// Reports whether a UNIQUE key already exists on the main write channel.
    ///
    /// Delegates to [`Writer::unique_key_exists_full_horizon`] so the bulk path
    /// sees the same layers the single-vertex path does — the main Writer's
    /// current L0, its pending-flush buffers, and committed storage — closing the
    /// cross-channel window where a committed-but-unflushed key was invisible to
    /// the bulk loader (finding D6).
    ///
    /// Non-scalar or null key values are not representable as a constraint key
    /// here and return `false`, deferring to the in-load/L0 dedup checks rather
    /// than erroring — matching the prior storage-probe behavior.
    ///
    /// # Errors
    /// Returns an error if the Writer's horizon probe (e.g. a storage backend
    /// query) fails.
    async fn unique_key_exists_full_horizon(
        &self,
        label: &str,
        unique_props: &[String],
        props: &Properties,
    ) -> Result<bool> {
        let mut key_values = Vec::with_capacity(unique_props.len());
        for prop in unique_props {
            let val = match props.get(prop) {
                // Non-scalar unique keys aren't representable as a constraint
                // key; fall back to the in-load/L0 checks rather than error.
                Some(v @ (Value::String(_) | Value::Int(_) | Value::Float(_) | Value::Bool(_))) => {
                    v.clone()
                }
                _ => return Ok(false),
            };
            key_values.push((prop.clone(), val));
        }

        // The bulk vertex has no allocated VID at validation time (VIDs are
        // assigned later, in `insert_vertices`), so `exclude_vid = None` — there
        // is no self to exclude, and every genuine hit counts as a violation.
        // `tx_l0 = None`: a preexisting committed row lives in the main Writer's
        // L0, not the bulk transaction's L0, which the bulk path bypasses.
        let writer = self
            .backend
            .writer
            .as_ref()
            .expect("BulkWriter always holds a Writer (build() enforces it)");
        writer
            .unique_key_exists_full_horizon(label, &key_values, None, None)
            .await
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
        let should_flush = self
            .pending_vertices
            .get(label)
            .is_some_and(|buf| buf.len() >= self.config.batch_size);

        if should_flush {
            self.flush_vertices_buffer(label).await?;
        }
        Ok(())
    }

    /// Record a table's pre-load Lance version the first time it is touched.
    ///
    /// Subsequent calls for the same table are no-ops, preserving the version
    /// captured before the bulk load's first write (used for abort rollback).
    async fn record_initial_version(&mut self, table_name: &str) -> Result<()> {
        if self.initial_table_versions.contains_key(table_name) {
            return Ok(());
        }
        let version = self
            .backend
            .storage
            .backend()
            .get_table_version(table_name)
            .await
            .map_err(UniError::Internal)?;
        self.initial_table_versions
            .insert(table_name.to_string(), version);
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

            // Record initial versions for abort rollback (only once per table)
            let table_name = uni_store::backend::table_names::vertex_table_name(label);
            self.record_initial_version(&table_name).await?;
            self.record_initial_version(uni_store::backend::table_names::main_vertex_table_name())
                .await?;

            // Durably record the intent to mutate these tables BEFORE writing
            // any of them, so a crash between the per-label and main commits is
            // reconciled (rolled back) on reopen (H9).
            self.persist_active_intent().await?;

            let ds = self
                .backend
                .storage
                .vertex_dataset(label)
                .map_err(UniError::Internal)?;
            let schema = self.backend.schema.schema();

            let deleted = vec![false; vertices.len()];
            let versions = vec![1; vertices.len()]; // Version 1 for bulk load

            // Generate timestamps for this batch
            let now = Self::get_current_timestamp_micros();
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

            // Write to per-label table via backend
            let backend = self.backend.storage.backend();
            ds.write_batch(backend, batch, &schema)
                .await
                .map_err(UniError::Internal)?;

            // Create default scalar indexes (_vid, _uid) which are critical for basic function
            ds.ensure_default_indexes(backend)
                .await
                .map_err(UniError::Internal)?;

            // Test-only: simulate a crash between the per-label commit (done) and
            // the main-table commit (below). The intent marker is already durable,
            // so reopen recovery must roll the per-label table back.
            if FAIL_AFTER_PERLABEL_WRITE.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow!("injected failure after per-label vertex commit"));
            }

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

                MainVertexDataset::write_batch(backend, main_batch)
                    .await
                    .map_err(UniError::Internal)?;

                MainVertexDataset::ensure_default_indexes(backend)
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
        let schema = self.backend.schema.schema();
        // Validate the edge type exists, but its id is no longer needed:
        // `allocate_eids` does not partition by type.
        schema
            .edge_types
            .get(edge_type)
            .ok_or_else(|| UniError::EdgeTypeNotFound {
                edge_type: edge_type.to_string(),
            })?;

        // Allocate EIDs in one IdAllocator mutex acquisition.
        let eids = {
            let writer = self.backend.writer.as_ref().unwrap();
            writer
                .allocate_eids(edges.len())
                .await
                .map_err(UniError::Internal)?
        };

        // Convert to L1Entry format and track buffer size
        let now = Self::get_current_timestamp_micros();
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
    async fn flush_edges_buffer(&mut self, edge_type: &str) -> Result<()> {
        if let Some(entries) = self.pending_edges.remove(edge_type) {
            if entries.is_empty() {
                return Ok(());
            }

            // Record initial versions for abort rollback (FWD, BWD, and main).
            let fwd_table_name =
                uni_store::backend::table_names::delta_table_name(edge_type, "fwd");
            self.record_initial_version(&fwd_table_name).await?;
            let bwd_table_name =
                uni_store::backend::table_names::delta_table_name(edge_type, "bwd");
            self.record_initial_version(&bwd_table_name).await?;
            self.record_initial_version(uni_store::backend::table_names::main_edge_table_name())
                .await?;

            let schema = self.backend.schema.schema();
            let backend = self.backend.storage.backend();

            // Record the intent before writing any of the three edge datasets
            // (fwd delta, bwd delta, main edges) — see the vertex path (H9).
            self.persist_active_intent().await?;

            // Write to FWD delta (sorted by src_vid)
            let mut fwd_entries = entries.clone();
            fwd_entries.sort_by_key(|e| e.src_vid);
            let fwd_ds = self
                .backend
                .storage
                .delta_dataset(edge_type, "fwd")
                .map_err(UniError::Internal)?;
            let fwd_batch = fwd_ds
                .build_record_batch(&fwd_entries, &schema)
                .map_err(UniError::Internal)?;
            fwd_ds
                .write_run(backend, fwd_batch)
                .await
                .map_err(UniError::Internal)?;
            fwd_ds
                .ensure_eid_index(backend)
                .await
                .map_err(UniError::Internal)?;

            // Write to BWD delta (sorted by dst_vid)
            let mut bwd_entries = entries.clone();
            bwd_entries.sort_by_key(|e| e.dst_vid);
            let bwd_ds = self
                .backend
                .storage
                .delta_dataset(edge_type, "bwd")
                .map_err(UniError::Internal)?;
            let bwd_batch = bwd_ds
                .build_record_batch(&bwd_entries, &schema)
                .map_err(UniError::Internal)?;
            bwd_ds
                .write_run(backend, bwd_batch)
                .await
                .map_err(UniError::Internal)?;
            bwd_ds
                .ensure_eid_index(backend)
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

                MainEdgeDataset::write_batch(self.backend.storage.backend(), main_batch)
                    .await
                    .map_err(UniError::Internal)?;

                MainEdgeDataset::ensure_default_indexes(self.backend.storage.backend())
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

        // 3. Rebuild indexes for vertices.
        //
        // User-declared indexes are materialized ONLY here — the flush path only
        // builds the fixed _vid/_uid/ext_id indexes. So this block must run
        // regardless of the defer flags; gating it on `defer_* == true` meant a
        // load with both flags false (build eagerly) silently built no user
        // index at all. The defer flags only select the BACKGROUND (deferred)
        // path; an eager load rebuilds synchronously at commit.
        {
            let labels_to_rebuild: Vec<String> = self.touched_labels.iter().cloned().collect();
            let use_async = self.config.async_indexes
                && (self.config.defer_vector_indexes || self.config.defer_scalar_indexes);

            if use_async && !labels_to_rebuild.is_empty() {
                // Async mode: mark affected indexes as Stale before scheduling
                let schema = self.backend.schema.schema();
                for label in &labels_to_rebuild {
                    for idx in &schema.indexes {
                        if idx.label() == label.as_str() {
                            // #233 Tier 1: `let _ =` left the index reporting
                            // `Online` while this load has just grown the table
                            // and the rebuild is only SCHEDULED. The planner
                            // trusts `Online`, consults the stale index, and
                            // rows go missing from query results with no log
                            // and no metric. Unlike the background paths in
                            // `uni-store`, this one has a caller: `commit`
                            // returns `Result`, so it propagates.
                            self.backend
                                .schema
                                .update_index_metadata(idx.name(), |m| {
                                    m.status = uni_common::core::schema::IndexStatus::Stale;
                                })
                                .map_err(|e| {
                                    anyhow::anyhow!(
                                        "bulk commit: could not mark index `{}` Stale before \
                                         scheduling its rebuild; leaving it Online would make \
                                         the planner consult a stale index: {e}",
                                        idx.name()
                                    )
                                })?;
                        }
                    }
                }

                let rebuild_manager = IndexRebuildManager::new(
                    self.backend.storage.clone(),
                    self.backend.schema.clone(),
                    self.backend.config.index_rebuild.clone(),
                )
                .await
                .map_err(UniError::Internal)?;

                let task_ids = rebuild_manager
                    .schedule(labels_to_rebuild)
                    .await
                    .map_err(UniError::Internal)?;

                self.stats.index_task_ids = task_ids;
                self.stats.indexes_pending = true;

                let manager = Arc::new(rebuild_manager);
                let handle = manager.start_background_worker(self.backend.shutdown.subscribe());
                self.backend.shutdown.track_task(handle);
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
                        self.backend.storage.base_path(),
                        self.backend.storage.schema_manager_arc(),
                    )
                    .with_backend(self.backend.storage.backend_arc());
                    idx_mgr
                        .rebuild_indexes_for_label(label)
                        .await
                        .map_err(UniError::Internal)?;
                    self.stats.indexes_rebuilt += 1;

                    // Update index metadata after successful sync rebuild
                    let now = Utc::now();
                    let vtable_name = uni_store::backend::table_names::vertex_table_name(label);
                    let row_count = self
                        .backend
                        .storage
                        .backend()
                        .count_rows(&vtable_name, None)
                        .await
                        // #233: `.ok()` persisted `row_count_at_build = None`,
                        // and `index_rebuild.rs`'s growth trigger is gated on
                        // `if let Some(built_count)` — so that index never
                        // auto-rebuilt on growth again, silently, until some
                        // later build happened to write a count. Recorded so
                        // the lost trigger is visible.
                        .inspect_err(|e| {
                            tracing::error!(
                                table = %vtable_name,
                                error = %e,
                                "bulk commit: could not read the row count for a rebuilt index; \
                                 its growth-based auto-rebuild stays disabled until a later \
                                 build records one",
                            );
                        })
                        .ok()
                        .map(|c| c as u64);

                    let schema = self.backend.schema.schema();
                    for idx in &schema.indexes {
                        if idx.label() == label.as_str() {
                            // #233: unlike the Stale write above, failing to
                            // record a SUCCESSFUL build is conservative — the
                            // index stays `Stale` and the rebuild manager picks
                            // it up again — so this does not fail the commit.
                            // It is still recorded rather than dropped: the
                            // decision is that no index-status write goes
                            // unrecorded.
                            if let Err(e) =
                                self.backend.schema.update_index_metadata(idx.name(), |m| {
                                    m.status = uni_common::core::schema::IndexStatus::Online;
                                    m.last_built_at = Some(now);
                                    if let Some(count) = row_count {
                                        m.row_count_at_build = Some(count);
                                    }
                                })
                            {
                                tracing::error!(
                                    index = %idx.name(),
                                    error = %e,
                                    "bulk commit: could not record a successful index build; \
                                     the index keeps its previous status",
                                );
                            }
                        }
                    }
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
            .backend
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?
            .unwrap_or_else(|| {
                SnapshotManifest::new(
                    Uuid::new_v4().to_string(),
                    self.backend.schema.schema().schema_version,
                )
            });

        // Update Manifest
        let parent_id = manifest.snapshot_id.clone();
        manifest.parent_snapshot = Some(parent_id);
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.created_at = Utc::now();

        // Update counts and versions for touched labels (vertices)
        let backend = self.backend.storage.backend();
        for label in &self.touched_labels {
            let vtable_name = uni_store::backend::table_names::vertex_table_name(label);
            let count = backend
                .count_rows(&vtable_name, None)
                .await
                .map_err(UniError::Internal)?;

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
            let delta_name = uni_store::backend::table_names::delta_table_name(edge_type, "fwd");
            if let Ok(count) = backend.count_rows(&delta_name, None).await {
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

        // Save Snapshot. The manifest body is durable after `save_snapshot`;
        // only after that do we promote the recovery marker to `Committed`
        // (carrying the new snapshot id) so that a crash before the latest
        // pointer flip rolls *forward* to this snapshot rather than rolling the
        // committed tables back (H9).
        self.backend
            .storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await
            .map_err(UniError::Internal)?;
        if !self.initial_table_versions.is_empty() {
            let store = self.backend.storage.store();
            flush_intent::write_committed(
                &store,
                &self.initial_table_versions,
                &manifest.snapshot_id,
            )
            .await
            .map_err(UniError::Internal)?;
        }
        self.backend
            .storage
            .snapshot_manager()
            .set_latest_snapshot(&manifest.snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        // Save schema with updated index metadata
        self.backend
            .schema
            .save()
            .await
            .map_err(UniError::Internal)?;

        // Warm adjacency CSRs for all edge types written during bulk import
        // so that subsequent traversal queries can find the edges.
        let schema = self.backend.storage.schema_manager().schema();
        for edge_type_name in &self.touched_edge_types {
            if let Some(meta) = schema.edge_types.get(edge_type_name.as_str()) {
                let type_id = meta.id;
                for &dir in uni_store::storage::direction::Direction::Both.expand() {
                    // #233 P3: pure prewarm. `uni-query`'s
                    // `ensure_adjacency_warmed` warms lazily and PROPAGATES,
                    // so a failure here costs first-query latency and nothing
                    // else — which is why it stays fail-open. Logged at debug
                    // so an unexplained first-query cost is attributable.
                    if let Err(e) = self
                        .backend
                        .storage
                        .warm_adjacency(type_id, dir, None)
                        .await
                    {
                        tracing::debug!(
                            edge_type = type_id,
                            ?dir,
                            error = %e,
                            "bulk commit: adjacency prewarm failed; the first traversal will \
                             build it instead",
                        );
                    }
                }
            }
        }

        // The load is fully finalized — drop the recovery marker. (A crash
        // before this point leaves the `Committed` marker, which reopen
        // reconciliation rolls forward; a crash before the `Committed` write
        // leaves the `Active` marker, which it rolls back.)
        if !self.initial_table_versions.is_empty() {
            let store = self.backend.storage.store();
            flush_intent::clear(&store)
                .await
                .map_err(UniError::Internal)?;
        }

        self.committed = true;
        self.stats.duration = self.start_time.elapsed();
        Ok(self.stats.clone())
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
        let backend = self.backend.storage.backend();
        let mut rollback_errors = Vec::new();
        let mut rolled_back_count = 0;
        let mut dropped_count = 0;

        for (table_name, initial_version) in &self.initial_table_versions {
            match initial_version {
                Some(version) => {
                    // Table existed before - rollback to initial version
                    match backend.rollback_table(table_name, *version).await {
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
                    match backend.drop_table(table_name).await {
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

        // 3. Clear backend cache to ensure next read picks up rolled-back state
        self.backend.storage.backend().clear_cache();

        // 4. Drop the crash-recovery marker — this abort already did the
        // rollback, so reopen must not try to roll back again.
        let store = self.backend.storage.store();
        if let Err(e) = flush_intent::clear(&store).await {
            rollback_errors.push(format!("intent marker: {e}"));
        }

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

    /// Persist the crash-recovery intent marker (phase `Active`) with the
    /// current set of touched tables and their pre-load versions. Called before
    /// each multi-table flush writes anything.
    async fn persist_active_intent(&self) -> Result<()> {
        let store = self.backend.storage.store();
        flush_intent::write_active(&store, &self.initial_table_versions)
            .await
            .map_err(UniError::Internal)?;
        Ok(())
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
