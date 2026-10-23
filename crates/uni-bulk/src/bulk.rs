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
use uni_store::backend::table_names;
use uni_store::backend::traits::{StagedAppend, StagedFilesHold};
use uni_store::runtime::writer::Writer;
use uni_store::storage::bulk_gate::GateHold;
use uni_store::storage::delta::{L1Entry, Op};
use uni_store::storage::main_edge::MainEdgeDataset;
use uni_store::storage::main_vertex::MainVertexDataset;
use uni_store::storage::manager::StorageManager;
use uni_store::storage::{IndexManager, IndexRebuildManager};
use uuid::Uuid;

use crate::events::EventDelivery;
use crate::flush_intent;

/// Test-only fault-injection: when set, the bulk writer returns an error
/// *after* publishing the per-label table but *before* the main table, in the
/// publish step of `commit()`: the crash-in-the-middle scenario H9 guards
/// against. Always
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
    /// The database's background index-rebuild manager, its worker started
    /// on first use.
    ///
    /// A deferred (`async_indexes`) rebuild is queued here. `None` falls back
    /// to a manager of the load's own, whose worker is tracked by
    /// [`Self::shutdown`]; that worker loops until shutdown, so a database
    /// should always supply this to keep one worker however many loads run.
    pub rebuild_manager: Option<RebuildManagerSource>,
    /// Tracks a background task a load spawns -- the re-drive of a committed
    /// load whose publish could not be finished in `commit` -- so the
    /// database's shutdown waits for it, with whatever else the database must
    /// keep alive while the task writes (a temporary database's directory).
    /// `None` tracks it with [`Self::shutdown`] alone.
    pub track_task: Option<TaskTracker>,
    /// Where a load's trigger and CDC events go once it is visible, and what
    /// it asks before staging rows a synchronous before-trigger would see
    /// (see [`crate::events`]). `None` emits nothing and refuses nothing.
    pub events: Option<Arc<dyn crate::events::BulkEventSink>>,
}

/// Tracks a spawned task until the database shuts down; see
/// [`BulkBackend::track_task`].
pub type TaskTracker = Arc<dyn Fn(tokio::task::JoinHandle<()>) + Send + Sync>;

impl BulkBackend {
    /// Track `handle` as [`Self::track_task`] says: so the database's shutdown
    /// waits for it.
    fn track(&self, handle: tokio::task::JoinHandle<()>) {
        match &self.track_task {
            Some(track) => track(handle),
            None => self.shutdown.track_task(handle),
        }
    }
}

/// Yields a database's shared [`IndexRebuildManager`], creating it (and
/// starting its worker) on first use.
pub type RebuildManagerSource = Arc<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Arc<IndexRebuildManager>>> + Send>,
        > + Send
        + Sync,
>;

/// A bulk load's progress callback (see [`BulkWriterBuilder::on_progress`]).
type ProgressCallback = Box<dyn Fn(BulkProgress) + Send>;

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
            // `arrow_to_value` used to answer `Null` both for a genuine null and
            // for a type it had no decoder arm for, so this compared against the
            // Arrow null bitmap to tell them apart — reconstructing by hand a
            // distinction the return type could not carry. It carries it now.
            let value = uni_store::storage::arrow_convert::arrow_to_value(
                col.as_ref(),
                row_idx,
                None,
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "bulk ingest: column `{}` of type {} could not be decoded at row {row_idx}: \
                     {e}",
                    field.name(),
                    col.data_type()
                )
            })?;
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

    /// Load even though a synchronous `BeforeMutation` / `BeforeCommit`
    /// trigger subscribes to what the load writes, without running it.
    ///
    /// A bulk load's mutation events are emitted after the load is visible,
    /// past the point a before-trigger could veto it, so such a trigger cannot
    /// be honoured. By default the load is refused with
    /// `UniError::BulkLoadBlockedByTrigger` before the matching rows are
    /// staged. With `true` the load proceeds and those triggers are skipped
    /// for it; after-phase triggers and CDC still receive its events.
    ///
    /// Default: `false`.
    pub fn skip_before_triggers(mut self, skip: bool) -> Self {
        self.config.skip_before_triggers = skip;
        self
    }

    /// Build the bulk writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is not writable, and
    /// `UniError::BulkLoadUnsupported` if its storage backend cannot stage
    /// rows invisibly until commit (`StorageBackend::stages_invisibly`).
    pub fn build(self) -> Result<BulkWriter> {
        if self.backend.writer.is_none() {
            return Err(anyhow!("BulkWriter requires a writable database instance"));
        }
        // Without staging every write is visible as it lands, so the load
        // would be neither isolated nor atomic, and the only undo -- for an
        // abort or a crash -- would roll each table back by version, dropping
        // whatever other writers committed to it meanwhile.
        if !self.backend.storage.backend().stages_invisibly() {
            return Err(UniError::BulkLoadUnsupported {
                reason: "the storage backend cannot stage rows invisibly until commit, which \
                         a bulk load's isolation, abort and crash recovery require"
                    .to_string(),
            }
            .into());
        }

        let load_id = Uuid::new_v4().to_string();
        Ok(BulkWriter {
            backend: self.backend,
            config: self.config,
            progress_callback: self.progress_callback.map(std::sync::Mutex::new),
            stats: BulkStats {
                load_id: load_id.clone(),
                ..BulkStats::default()
            },
            start_time: Instant::now(),
            pending_vertices: HashMap::new(),
            pending_edges: HashMap::new(),
            touched_labels: HashSet::new(),
            touched_edge_types: HashSet::new(),
            initial_table_versions: HashMap::new(),
            staged: HashMap::new(),
            written_vids: HashMap::new(),
            load_vid_labels: std::collections::BTreeMap::new(),
            seen_unique_keys: HashMap::new(),
            buffer_size_bytes: 0,
            committed: false,
            write_version: None,
            version_reservation: None,
            load_id,
            staged_holds: HashMap::new(),
            external_endpoints: std::collections::BTreeMap::new(),
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
    /// Load past synchronous before-triggers instead of refusing; see
    /// [`BulkWriterBuilder::skip_before_triggers`]. Default: `false`.
    pub skip_before_triggers: bool,
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
            skip_before_triggers: false,
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
    /// The load's id (see [`BulkWriter::load_id`]).
    pub load_id: String,
    /// Steps after the commit point that failed without affecting the load.
    ///
    /// The load is applied whatever this holds; each entry names a step --
    /// an index build, the snapshot manifest, the schema save, the marker
    /// delete -- that recovery at the next open or the index rebuild worker
    /// completes. Empty when every step succeeded.
    pub warnings: Vec<String>,
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
/// Supports constraint validation and automatic checkpointing when buffer
/// limits are exceeded.
///
/// # Isolation
///
/// On a backend that stages writes (Lance — see
/// `StorageBackend::stages_invisibly`) every flush before `commit()` writes
/// data files that no table version references yet. Until `commit()` publishes
/// them, no reader, no compaction pass and no in-memory cache can observe the
/// load, so `abort()` (or dropping the writer) only has to discard files nobody
/// can reach. Earlier, flushes appended straight into the live tables and abort
/// restored each table's pre-load version, which exposed uncommitted rows to
/// every reader, let compaction fold uncommitted edges into L2 adjacency where
/// abort could not reach them, and discarded rows other writers had committed
/// to the same tables in the meantime.
///
/// A fork stages on its own branch of each table, so a fork's load is just as
/// invisible until `commit()`. A backend that cannot stage is refused when the
/// writer is built (`UniError::BulkLoadUnsupported`).
pub struct BulkWriter {
    backend: BulkBackend,
    config: BulkConfig,
    // Behind a mutex so the writer is `Sync` (the callback need only be
    // `Send`): every async method borrows `&self` across an await, so a
    // `!Sync` writer made every one of its futures `!Send`, and a load could
    // not run on a spawned task.
    progress_callback: Option<std::sync::Mutex<ProgressCallback>>,
    stats: BulkStats,
    start_time: Instant,
    // Buffered data per label/type
    pending_vertices: HashMap<String, Vec<(Vid, Properties)>>,
    pending_edges: HashMap<String, Vec<L1Entry>>,
    // Track what was written (for index rebuild)
    touched_labels: HashSet<String>,
    touched_edge_types: HashSet<String>,
    // Each published table's version before the publish (None = the table
    // did not exist), recorded inside `commit`: it bounds the search recovery
    // makes for a table already published.
    initial_table_versions: HashMap<String, Option<u64>>,
    // Rows staged per table, awaiting `publish`.
    staged: HashMap<String, Vec<StagedAppend>>,
    // VIDs written per label, registered in `VidLabelsIndex` once the rows are
    // visible — registering them earlier would outlive an abort.
    written_vids: HashMap<String, Vec<Vid>>,
    // The label of every vertex this load inserted, as runs of consecutive
    // VIDs (`start -> (end exclusive, label)`), so declared edge endpoints can
    // be checked against vertices the load has not published yet.
    load_vid_labels: std::collections::BTreeMap<u64, (u64, String)>,
    // UNIQUE-constraint keys accepted over the WHOLE load (not just the current
    // buffer, which `flush_vertices_buffer` drains): without this, a duplicate
    // spanning two flushes slipped through. Keyed by constraint identity
    // (label + unique props), value = set of value-keys seen. (review H8)
    seen_unique_keys: HashMap<String, HashSet<String>>,
    // Current buffer size in bytes (approximate)
    buffer_size_bytes: usize,
    committed: bool,
    /// MVCC version stamped on every row this writer produces, reserved from
    /// the live counter on first use. See [`BulkWriter::write_version`].
    write_version: Option<u64>,
    /// Keeps `write_version` recorded as not yet visible, so every version
    /// ceiling captured before the load is visible excludes it for good;
    /// released once the load is shown (or discarded). See
    /// [`L0Manager::reserve_bulk_version`](uni_store::runtime::l0_manager::L0Manager::reserve_bulk_version).
    version_reservation: Option<uni_store::runtime::l0_manager::BulkVersionReservation>,
    /// Names this load's crash-recovery marker, so concurrent loads each keep
    /// their own and never overwrite or delete another's.
    load_id: String,
    /// One hold per table this load staged into, keeping its staged files
    /// from version cleanup until they are published or discarded.
    staged_holds: HashMap<String, StagedFilesHold>,
    /// Per edge type, the endpoints of this load's edges that existed before
    /// the load, as checked when the edges were inserted; checked again once
    /// the load is published (see `crate::endpoint_check`).
    external_endpoints: std::collections::BTreeMap<String, crate::endpoint_check::TypeEndpoints>,
}

/// The order a commit publishes its tables in, and the commit record.
pub(crate) struct PublishPlan {
    /// (table, (edge type, direction) when it is a delta table)
    order: Vec<(String, Option<(String, &'static str)>)>,
    /// How many per-label vertex tables lead `order`.
    per_label_tables: usize,
    /// Every table's staged rows, as written into the commit marker. Empty
    /// for a load that staged nothing.
    pub(crate) records: Vec<flush_intent::StagedTablePublish>,
    /// Vertex labels the load wrote, sorted.
    labels: Vec<String>,
}

/// Which side of its commit point a commit ended on.
pub(crate) enum CommitPoint {
    /// The marker is durable (or the load needs none): the load is decided.
    Reached,
    /// The marker is not in the store and never will be: not applied.
    NotReached(anyhow::Error),
    /// The marker write failed and whether it landed cannot be told.
    Unknown(String),
}

/// Attempts at completing a committed load's publish in-process, after the
/// first attempt failed.
const COMPLETE_PUBLISH_ATTEMPTS: u32 = 3;
/// Backoff before the next attempt, multiplied by the attempt number.
const COMPLETE_PUBLISH_BACKOFF: Duration = Duration::from_millis(100);
/// First delay of the background re-drive of a committed load whose publish
/// `commit` could not finish; doubled after each failed attempt.
const REDRIVE_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Longest delay between two background re-drive attempts.
const REDRIVE_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Test seam for the commit-marker write: `return(terminal)` fails it as the
/// store refusing the write, `return(landed)` writes it and then reports a
/// transport failure, any other argument reports a transport failure without
/// writing. Inert without the `failpoints` feature.
fn injected_marker_fault() -> Option<String> {
    fail::fail_point!("bulk::commit::marker-write", |arg| Some(
        arg.unwrap_or_default()
    ));
    None
}

/// The store error [`injected_marker_fault`] stands for.
fn injected_store_error(fault: &str) -> anyhow::Error {
    let source: Box<dyn std::error::Error + Send + Sync> =
        format!("injected marker write fault ({fault})").into();
    if fault == "terminal" {
        anyhow::Error::new(object_store::Error::PermissionDenied {
            path: "bulk marker".to_string(),
            source,
        })
    } else {
        anyhow::Error::new(object_store::Error::Generic {
            store: "injected",
            source,
        })
    }
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
    /// The MVCC version stamped on every row this writer produces.
    ///
    /// Reserved from the live `L0Buffer::current_version` on first use and then
    /// cached, so one bulk load is one logical write.
    ///
    /// **Advancing the shared counter is the load-bearing half** (#266). Bulk
    /// rows go straight to Lance, bypassing L0, and used to carry a hard-coded
    /// `_version = 1` while the counter stayed where it was. A transaction
    /// opened afterwards on the same handle pins its L1 reads to
    /// `_version <= started_at_version`, so on a fresh database that pin was
    /// `<= 0` and every bulk row was invisible to it: `MATCH … SET` matched
    /// nothing, committed cleanly, and discarded the write with no error.
    /// Reopening masked it, because reopen re-reads the mark from the manifest.
    ///
    /// Drawing the version from the counter also removes a second hazard that
    /// had no symptom yet: a fixed `1` written into a database whose counter had
    /// already passed `1` produces rows that read as *older* than what is
    /// already stored.
    ///
    /// A read-only database has no `Writer` and therefore no counter to advance;
    /// it also cannot be written through this path, so the value is immaterial.
    fn write_version(&mut self) -> u64 {
        if let Some(v) = self.write_version {
            return v;
        }
        let v = match self.backend.writer.as_ref() {
            Some(writer) => {
                let reservation = writer.l0_manager.reserve_bulk_version();
                let v = reservation.version();
                self.version_reservation = Some(reservation);
                v
            }
            None => 1,
        };
        self.write_version = Some(v);
        v
    }

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
    /// - A synchronous before-trigger subscribes to the label's creates
    ///   (`UniError::BulkLoadBlockedByTrigger`; see
    ///   [`BulkWriterBuilder::skip_before_triggers`])
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

        // An empty insert is a no-op, and must not mark the label touched.
        // `flush_vertices_buffer` never creates `vertices_<label>` for an empty
        // buffer, but `commit` counts rows for every touched label with a hard
        // `?` -- so marking it here made a zero-row insert fail the whole commit
        // with `open table 'vertices_<label>'`. It also fed `labels_to_rebuild`,
        // scheduling an index rebuild for a label with no new rows. The label
        // check above still runs, so an empty insert against an unknown label
        // is still an error.
        if vertices.is_empty() {
            return Ok(Vec::new());
        }
        // Before anything of this label is staged.
        self.check_before_triggers(crate::events::BulkWriteKind::Vertex, label)?;

        // A property named after a storage column is refused whether or not
        // constraint validation is on: it is a storage collision, not a
        // constraint (see `RESERVED_PROPERTY_NAMES`).
        for props in &vertices {
            uni_common::core::schema::check_written_property_names(props.keys(), &schema)?;
        }

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
        self.record_load_vid_labels(&vids, label);

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

    /// Record a table's pre-publish Lance version the first time it is asked.
    ///
    /// Later calls for the same table are no-ops. The version bounds the
    /// search recovery makes for a table already published.
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

    /// Flush the vertex buffer for `label` to storage.
    ///
    /// Stages the per-label table's and the main vertices table's rows,
    /// invisible until [`Self::commit`] publishes them.
    async fn flush_vertices_buffer(&mut self, label: &str) -> Result<()> {
        let write_version = self.write_version();
        if let Some(vertices) = self.pending_vertices.remove(label) {
            if vertices.is_empty() {
                return Ok(());
            }

            let table_name = table_names::vertex_table_name(label);
            let main_table = table_names::main_vertex_table_name();

            let ds = self
                .backend
                .storage
                .vertex_dataset(label)
                .map_err(UniError::Internal)?;
            let schema = self.backend.schema.schema();

            let deleted = vec![false; vertices.len()];
            let versions = vec![write_version; vertices.len()];

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

            self.write_rows(&table_name, batch).await?;

            // Dual-write to main vertices table, at the load's version like the
            // per-label rows. A hard-coded `1` here (left over from before
            // #266) put the label-less copy of every bulk vertex under every
            // snapshot ceiling and outside the bulk visibility gate, which
            // hides a publishing load by its version.
            let main_vertices: Vec<(Vid, Vec<String>, Properties, bool, u64)> =
                vertices_with_labels
                    .into_iter()
                    .map(|(vid, lbls, props)| (vid, lbls, props, false, write_version))
                    .collect();

            let main_batch = MainVertexDataset::build_record_batch(
                &main_vertices,
                Some(&created_at),
                Some(&updated_at),
            )
            .map_err(UniError::Internal)?;
            self.write_rows(main_table, main_batch).await?;

            self.written_vids
                .entry(label.to_string())
                .or_default()
                .extend(main_vertices.iter().map(|(vid, ..)| *vid));
        }
        Ok(())
    }

    /// Hold `table`'s staged files against version cleanup, once per table.
    fn hold_staged(&mut self, table: &str) {
        if !self.staged_holds.contains_key(table) {
            let hold = self.backend.storage.backend().hold_staged_files(table);
            self.staged_holds.insert(table.to_string(), hold);
        }
    }

    /// Stage `batch` for `table`, invisible until `commit` publishes it.
    async fn write_rows(&mut self, table: &str, batch: arrow_array::RecordBatch) -> Result<()> {
        self.hold_staged(table);
        let backend = self.backend.storage.backend();
        let staged = uni_store::storage::manager::stage_batch(backend, table, batch)
            .await
            .map_err(UniError::Internal)?;
        self.staged
            .entry(table.to_string())
            .or_default()
            .push(staged);
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
    /// Returns `UniError::BulkLoadBlockedByTrigger` if a synchronous
    /// before-trigger subscribes to the type's creates (see
    /// [`BulkWriterBuilder::skip_before_triggers`]), and an error if the edge
    /// type is not found in the schema or if
    /// storage write fails.
    pub async fn insert_edges(
        &mut self,
        edge_type: &str,
        edges: Vec<EdgeData>,
    ) -> Result<Vec<Eid>> {
        let schema = self.backend.schema.schema();
        // Validate the edge type exists, but its id is no longer needed:
        // `allocate_eids` does not partition by type.
        let meta = schema
            .edge_types
            .get(edge_type)
            .ok_or_else(|| UniError::EdgeTypeNotFound {
                edge_type: edge_type.to_string(),
            })?;
        // Before anything of this edge type is staged or recorded.
        if !edges.is_empty() {
            self.check_before_triggers(crate::events::BulkWriteKind::Edge, edge_type)?;
        }
        // Declared endpoints are enforced here as on every other write path:
        // these rows go straight to storage, so nothing downstream checks
        // them, and the planner narrows on a type whose edges all conform.
        let external = self
            .check_edge_endpoints(edge_type, &meta.src_labels, &meta.dst_labels, &edges)
            .await?;
        if !(external.sources.is_empty() && external.targets.is_empty()) {
            let seen = self
                .external_endpoints
                .entry(edge_type.to_string())
                .or_default();
            seen.sources.extend(external.sources);
            seen.targets.extend(external.targets);
        }

        // Empty insert is a no-op -- see the note in `insert_vertices`. The edge
        // count loop in `commit` happens to tolerate a missing table (`if let
        // Ok(count)`) where the vertex loop does not, so this path did not fail
        // the same way; guarding both keeps `touched_edge_types` honest.
        if edges.is_empty() {
            return Ok(Vec::new());
        }

        // Unconditional, as for vertices.
        for edge in &edges {
            uni_common::core::schema::check_written_property_names(
                edge.properties.keys(),
                &schema,
            )?;
        }

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
        // Same reserved version as this writer's vertices (#266) — one bulk
        // load is one logical write, and the counter must end above it.
        let write_version = self.write_version();
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
                    version: write_version,
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

    /// Remember the label of the VIDs one `insert_vertices` call allocated.
    fn record_load_vid_labels(&mut self, vids: &[Vid], label: &str) {
        let mut run: Option<(u64, u64)> = None;
        for vid in vids.iter().map(|v| v.as_u64()) {
            run = match run {
                Some((start, end)) if end == vid => Some((start, end + 1)),
                Some((start, end)) => {
                    self.load_vid_labels.insert(start, (end, label.to_string()));
                    Some((vid, vid + 1))
                }
                None => Some((vid, vid + 1)),
            };
        }
        if let Some((start, end)) = run {
            self.load_vid_labels.insert(start, (end, label.to_string()));
        }
    }

    /// Labels of an edge endpoint: this load's own vertices first, then the
    /// database's.
    async fn endpoint_labels(&self, vid: Vid) -> Result<Vec<String>> {
        let raw = vid.as_u64();
        if let Some((_, (end, label))) = self.load_vid_labels.range(..=raw).next_back()
            && raw < *end
        {
            return Ok(vec![label.clone()]);
        }
        let Some(writer) = self.backend.writer.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(writer
            .get_vertex_labels(vid, None)
            .await?
            .unwrap_or_default())
    }

    /// Refuse an edge whose endpoint carries none of its type's declared labels.
    ///
    /// An empty declaration accepts any endpoint, as on the transactional
    /// write path.
    ///
    /// Returns the checked endpoints that are not this load's own vertices:
    /// they existed before the load, so their labels can change before the
    /// load is published, and [`Self::finalize`] checks them again.
    ///
    /// # Errors
    /// `UniError::EdgeEndpointViolation` for the first endpoint that fails.
    async fn check_edge_endpoints(
        &self,
        edge_type: &str,
        src_allowed: &[String],
        dst_allowed: &[String],
        edges: &[EdgeData],
    ) -> Result<crate::endpoint_check::TypeEndpoints> {
        let mut external = crate::endpoint_check::TypeEndpoints::default();
        if src_allowed.is_empty() && dst_allowed.is_empty() {
            return Ok(external);
        }
        let mut labels_of: HashMap<Vid, Vec<String>> = HashMap::new();
        for edge in edges {
            for (vid, allowed, endpoint) in [
                (edge.src_vid, src_allowed, "source"),
                (edge.dst_vid, dst_allowed, "target"),
            ] {
                if allowed.is_empty() {
                    continue;
                }
                if let std::collections::hash_map::Entry::Vacant(slot) = labels_of.entry(vid) {
                    slot.insert(self.endpoint_labels(vid).await?);
                }
                let labels = &labels_of[&vid];
                if !labels.iter().any(|l| allowed.contains(l)) {
                    return Err(UniError::EdgeEndpointViolation {
                        edge_type: edge_type.to_string(),
                        endpoint: endpoint.to_string(),
                        labels: labels.clone(),
                        allowed: allowed.to_vec(),
                    }
                    .into());
                }
                if !self.is_load_vertex(vid) {
                    if endpoint == "source" {
                        external.sources.insert(vid);
                    } else {
                        external.targets.insert(vid);
                    }
                }
            }
        }
        Ok(external)
    }

    /// Whether `vid` is a vertex this load inserted.
    fn is_load_vertex(&self, vid: Vid) -> bool {
        let raw = vid.as_u64();
        self.load_vid_labels
            .range(..=raw)
            .next_back()
            .is_some_and(|(_, (end, _))| raw < *end)
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

    /// Flush the edge buffer for `edge_type` to storage.
    ///
    /// Stages the main edges table's and both delta tables' rows, invisible
    /// until [`Self::commit`] publishes them.
    async fn flush_edges_buffer(&mut self, edge_type: &str) -> Result<()> {
        if let Some(entries) = self.pending_edges.remove(edge_type) {
            if entries.is_empty() {
                return Ok(());
            }

            let fwd_table_name = table_names::delta_table_name(edge_type, "fwd");
            let bwd_table_name = table_names::delta_table_name(edge_type, "bwd");
            let main_table = table_names::main_edge_table_name();

            let schema = self.backend.schema.schema();
            let storage = Arc::clone(&self.backend.storage);
            let backend = storage.backend();

            // Main edges FIRST, deltas second — the same order as
            // `Writer::flush_to_l1`. Background compaction folds whatever a
            // delta table holds into L2 adjacency and relies on every delta EID
            // already having its `main_edges` row (the properties it does not
            // carry). `publish` keeps the same order, which is what keeps a
            // pass from seeing an EID `main_edges` does not yet hold.
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

            let main_batch = MainEdgeDataset::build_record_batch(
                &main_edges,
                Some(&edge_created_at),
                Some(&edge_updated_at),
            )
            .map_err(UniError::Internal)?;
            self.write_rows(main_table, main_batch).await?;

            // FWD delta (sorted by src_vid), then BWD delta (sorted by dst_vid).
            for (direction, table_name) in [("fwd", &fwd_table_name), ("bwd", &bwd_table_name)] {
                let mut sorted = entries.clone();
                if direction == "fwd" {
                    sorted.sort_by_key(|e| e.src_vid);
                } else {
                    sorted.sort_by_key(|e| e.dst_vid);
                }
                let ds = self
                    .backend
                    .storage
                    .delta_dataset(edge_type, direction)
                    .map_err(UniError::Internal)?;
                let batch = ds
                    .build_record_batch(&sorted, &schema)
                    .map_err(UniError::Internal)?;
                if !self.staged_holds.contains_key(table_name.as_str()) {
                    self.staged_holds
                        .insert(table_name.clone(), backend.hold_staged_files(table_name));
                }
                let run = ds
                    .stage_run(backend, batch)
                    .await
                    .map_err(UniError::Internal)?;
                self.staged.entry(table_name.clone()).or_default().push(run);
            }
        }
        Ok(())
    }

    /// Decide the publish order and, when this load keeps a marker, record
    /// every table's staged rows for it.
    ///
    /// Vertex tables first (per-label, then main) so that an edge never
    /// becomes visible before its endpoints, then `main_edges`, then the delta
    /// tables: compaction folds a delta into L2 adjacency only on the strength
    /// of its `main_edges` row. Recording the pre-publish versions bounds the
    /// search recovery (and an in-process completion) makes for a table already
    /// published.
    async fn plan_publish(&mut self) -> Result<PublishPlan> {
        let mut labels: Vec<String> = self.touched_labels.iter().cloned().collect();
        labels.sort();
        let mut edge_types: Vec<String> = self.touched_edge_types.iter().cloned().collect();
        edge_types.sort();

        // (table, (edge type, direction) when it is a delta table)
        let mut order: Vec<(String, Option<(String, &'static str)>)> = labels
            .iter()
            .map(|l| (table_names::vertex_table_name(l), None))
            .collect();
        order.push((table_names::main_vertex_table_name().to_string(), None));
        order.push((table_names::main_edge_table_name().to_string(), None));
        for et in &edge_types {
            for dir in ["fwd", "bwd"] {
                order.push((
                    table_names::delta_table_name(et, dir),
                    Some((et.clone(), dir)),
                ));
            }
        }
        order.retain(|(table, _)| self.staged.contains_key(table));
        // The staged per-label tables lead `order`.
        let per_label_tables = labels
            .iter()
            .filter(|l| self.staged.contains_key(&table_names::vertex_table_name(l)))
            .count();

        // A fork's marker lives in the fork's own namespace and is completed
        // through the fork's branched backend, never against primary tables.
        // The versions recorded for a fork are the primary tables' (that is
        // what the fork backend reports); the fork's branch publish ignores
        // them and searches the branch's whole retained history instead.
        let mut records = Vec::with_capacity(order.len());
        let storage = Arc::clone(&self.backend.storage);
        let backend = storage.backend();
        for (table, _) in &order {
            self.record_initial_version(table).await?;
            let runs = self.staged.get(table).map_or(&[][..], Vec::as_slice);
            if let Some(staged) = backend
                .staged_record(table, runs)
                .map_err(UniError::Internal)?
            {
                records.push(flush_intent::StagedTablePublish {
                    table: table.clone(),
                    staged,
                });
            }
        }
        Ok(PublishPlan {
            order,
            per_label_tables,
            records,
            labels,
        })
    }

    /// Everything before the commit point. Nothing this load wrote is visible
    /// yet on a staging backend, so a failure here leaves the load unapplied.
    pub(crate) async fn prepare_commit(&mut self) -> Result<PublishPlan> {
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

        fail::fail_point!("bulk::commit::before-commit-point", |_| Err(anyhow!(
            "injected failure before the commit point"
        )));

        // 3. Persist the schema the load was written against. Essential, and
        // so before the commit point: a schema installed with `load_schema` is
        // in memory only, and its labels reach storage only through a save. A
        // load published under labels the stored schema lacks has rows no
        // reader resolves after a reopen.
        if self.saves_schema() {
            self.backend
                .schema
                .save()
                .await
                .map_err(UniError::Internal)?;
        }

        // 4. The publish order, and the record of every table's staged rows.
        self.plan_publish().await
    }

    /// Write the durable record that decides the load: from here on, the next
    /// open completes it rather than discarding it.
    ///
    /// It is the marker with every table's staged rows, in the primary's or
    /// the fork's own catalog namespace.
    pub(crate) async fn reach_commit_point(&self, plan: &PublishPlan) -> CommitPoint {
        if plan.records.is_empty() {
            return CommitPoint::Reached;
        }
        let store = flush_intent::Markers::for_storage(&self.backend.storage);
        let write = async {
            match injected_marker_fault() {
                None => {
                    flush_intent::write_publishing(
                        &store,
                        &self.load_id,
                        &self.initial_table_versions,
                        plan.records.clone(),
                        plan.labels.clone(),
                        self.write_version,
                        self.loaded_rows(),
                    )
                    .await
                }
                Some(fault) => {
                    if fault == "landed" {
                        flush_intent::write_publishing(
                            &store,
                            &self.load_id,
                            &self.initial_table_versions,
                            plan.records.clone(),
                            plan.labels.clone(),
                            self.write_version,
                            self.loaded_rows(),
                        )
                        .await?;
                    }
                    Err(injected_store_error(&fault))
                }
            }
        };
        self.decide_marker_write(write.await).await
    }

    /// Settle whether a marker write that may have failed reached the store.
    ///
    /// A failed write is read back: the marker present means the load is
    /// committed. Absent settles "not committed" only when the store refused
    /// the write outright (a terminal error); a timeout or transport error may
    /// still land, so absence then proves nothing, and neither does a failed
    /// read.
    async fn decide_marker_write(&self, written: anyhow::Result<()>) -> CommitPoint {
        let err = match written {
            Ok(()) => return CommitPoint::Reached,
            Err(e) => e,
        };
        let terminal = err
            .downcast_ref::<object_store::Error>()
            .is_some_and(uni_store::store_utils::is_terminal);
        let store = flush_intent::Markers::for_storage(&self.backend.storage);
        let read_back = async {
            fail::fail_point!("bulk::commit::marker-read", |_| Err(anyhow!(
                "injected marker read-back failure"
            )));
            flush_intent::read_marker(&store, &self.load_id).await
        };
        match read_back.await {
            Ok(Some(intent)) if flush_intent::marker_applies(&intent) => {
                tracing::warn!(
                    load_id = %self.load_id,
                    error = %err,
                    "bulk commit: writing the commit marker reported a failure, but the marker \
                     is in the store; the load is committed"
                );
                CommitPoint::Reached
            }
            Ok(_) if terminal => CommitPoint::NotReached(err),
            Ok(_) => CommitPoint::Unknown(format!(
                "writing the commit marker failed ({err:#}) and it is not in the store, but the \
                 failed write may still land"
            )),
            Err(read_err) => CommitPoint::Unknown(format!(
                "writing the commit marker failed ({err:#}) and reading it back failed too \
                 ({read_err:#})"
            )),
        }
    }

    /// Publish each table's staged rows from this process's own staging
    /// handles, in `plan.order`. The first attempt after the commit point.
    async fn publish_staged_runs(&mut self, plan: &PublishPlan) -> Result<()> {
        let backend = Arc::clone(&self.backend.storage);
        let backend = backend.backend();
        for (i, (table, delta)) in plan.order.iter().enumerate() {
            // Test-only: crash after the per-label vertex commit(s), before the
            // main vertices table — the H9 scenario, now at publish time.
            if i == plan.per_label_tables
                && plan.per_label_tables > 0
                && FAIL_AFTER_PERLABEL_WRITE.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(anyhow!("injected failure after per-label vertex commit"));
            }
            fail::fail_point!("bulk::commit::publish-table", |_| Err(anyhow!(
                "injected failure publishing '{table}'"
            )));
            let runs = self.staged.remove(table).unwrap_or_default();
            match delta {
                Some((edge_type, direction)) => {
                    self.backend
                        .storage
                        .delta_dataset(edge_type, direction)
                        .map_err(UniError::Internal)?
                        .publish_runs(backend, runs)
                        .await
                        .map_err(UniError::Internal)?;
                }
                None => backend
                    .publish_staged(table, runs)
                    .await
                    .map_err(UniError::Internal)?,
            }
        }
        Ok(())
    }

    /// Make every staged row visible. Runs after the commit point, so it is
    /// essential: it either finishes or the load is reported incomplete.
    ///
    /// The first attempt publishes from the staging handles. If it fails part
    /// way, the remaining tables are published from the commit record through
    /// the same exactly-once path recovery takes
    /// ([`StorageBackend::publish_staged_record`]), which recognizes a table
    /// the failed attempt already published and keeps every other writer's
    /// commits, retried a bounded number of times.
    ///
    /// [`StorageBackend::publish_staged_record`]: uni_store::backend::traits::StorageBackend::publish_staged_record
    async fn complete_publish(&mut self, plan: &PublishPlan) -> Result<()> {
        let first = match self.publish_staged_runs(plan).await {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        // Whatever the failed attempt did not consume is published from the
        // record below; the handles themselves describe nothing more.
        self.staged.clear();
        if plan.records.is_empty() {
            // Nothing was recorded to complete from.
            return Err(first);
        }
        let tables: std::collections::BTreeMap<String, Option<u64>> =
            flush_intent::table_map(&self.initial_table_versions);
        let first_failure = format!("{first:#}");
        let mut last = first;
        for attempt in 1..=COMPLETE_PUBLISH_ATTEMPTS {
            tracing::warn!(
                load_id = %self.load_id,
                attempt,
                error = %last,
                "bulk commit: publishing failed after the commit point; completing it from \
                 the commit record"
            );
            let result = async {
                fail::fail_point!("bulk::commit::complete-publish", |_| Err(anyhow!(
                    "injected failure completing the publish"
                )));
                flush_intent::publish_records(&self.backend.storage, &tables, &plan.records, |_| {
                    Ok(())
                })
                .await
            }
            .await;
            match result {
                Ok(published) => {
                    let earlier_retry = if attempt > 1 {
                        format!(" (retry {} failed: {last:#})", attempt - 1)
                    } else {
                        String::new()
                    };
                    self.stats.warnings.push(format!(
                        "publish: the first attempt failed ({first_failure}); completed \
                         in-process on retry {attempt}{earlier_retry}, which published \
                         {published} remaining table(s)"
                    ));
                    return Ok(());
                }
                Err(e) => last = e,
            }
            if attempt < COMPLETE_PUBLISH_ATTEMPTS {
                tokio::time::sleep(COMPLETE_PUBLISH_BACKOFF * attempt).await;
            }
        }
        Err(last)
    }

    /// Bring the published tables' fixed indexes and in-memory label index up
    /// to date with the rows this load made visible.
    async fn index_published_rows(&mut self) -> Result<()> {
        let backend = self.backend.storage.backend();

        let mut labels: Vec<String> = self.written_vids.keys().cloned().collect();
        labels.sort();

        // Register the labels of the rows just made visible (#269).
        //
        // `VidLabelsIndex` is populated at startup from the main vertex table
        // and thereafter only by `flush_to_l1`. Bulk rows go straight to Lance
        // and reach neither, so on the inserting handle every one of them
        // missed the index — and `get_batch_vertex_props` answers a miss by
        // scanning *every declared label* (#264). Correct, and a fan-out
        // proportional to the schema. A reopen hid it, because the startup
        // rebuild reads the table these rows are already in. Registering here
        // is what makes the inserting handle agree with a reopened one; doing
        // it only now, after the rows are visible, keeps an aborted load from
        // leaving entries behind. In memory and infallible, so it comes before
        // the index builds that can fail.
        for (label, vids) in self.written_vids.drain() {
            for vid in vids {
                self.backend
                    .storage
                    .update_vid_labels_index(vid, vec![label.clone()]);
            }
        }

        for label in &labels {
            // Create default scalar indexes (_vid, _uid), which are critical
            // for basic function.
            self.backend
                .storage
                .vertex_dataset(label)
                .map_err(UniError::Internal)?
                .ensure_default_indexes(backend)
                .await
                .map_err(UniError::Internal)?;
        }
        if !labels.is_empty() {
            MainVertexDataset::ensure_default_indexes(backend)
                .await
                .map_err(UniError::Internal)?;
        }

        let mut edge_types: Vec<String> = self.touched_edge_types.iter().cloned().collect();
        edge_types.sort();
        if !edge_types.is_empty() {
            MainEdgeDataset::ensure_default_indexes(backend)
                .await
                .map_err(UniError::Internal)?;
        }
        for edge_type in &edge_types {
            for dir in ["fwd", "bwd"] {
                self.backend
                    .storage
                    .delta_dataset(edge_type, dir)
                    .map_err(UniError::Internal)?
                    .ensure_eid_index(backend)
                    .await
                    .map_err(UniError::Internal)?;
            }
        }
        Ok(())
    }

    /// Rebuild (or schedule the rebuild of) the user indexes of every label
    /// the load wrote.
    ///
    /// User-declared indexes are materialized ONLY here — the flush path only
    /// builds the fixed _vid/_uid/ext_id indexes. So this must run regardless
    /// of the defer flags; gating it on `defer_* == true` meant a load with
    /// both flags false (build eagerly) silently built no user index at all.
    /// The defer flags only select the BACKGROUND (deferred) path; an eager
    /// load rebuilds synchronously at commit.
    async fn rebuild_user_indexes(&mut self) -> Result<()> {
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
                        // trusts `Online`, consults the stale index, and rows
                        // go missing from query results with no log and no
                        // metric. Reported, as every step after the commit
                        // point is; the commit marker is then kept, so the
                        // next open marks the index Stale itself.
                        self.backend
                            .schema
                            .update_index_metadata(idx.name(), |m| {
                                m.status = uni_common::core::schema::IndexStatus::Stale;
                            })
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "could not mark index `{}` Stale before scheduling its \
                                     rebuild; leaving it Online would make the planner consult \
                                     a stale index: {e}",
                                    idx.name()
                                )
                            })?;
                    }
                }
            }

            let manager = match &self.backend.rebuild_manager {
                Some(source) => source().await.map_err(UniError::Internal)?,
                None => {
                    let manager = Arc::new(
                        IndexRebuildManager::new(
                            self.backend.storage.clone(),
                            self.backend.schema.clone(),
                            self.backend.config.index_rebuild.clone(),
                        )
                        .await
                        .map_err(UniError::Internal)?,
                    );
                    let handle = Arc::clone(&manager)
                        .start_background_worker(self.backend.shutdown.subscribe());
                    self.backend.shutdown.track_task(handle);
                    manager
                }
            };

            let task_ids = manager
                .schedule(labels_to_rebuild)
                .await
                .map_err(UniError::Internal)?;

            self.stats.index_task_ids = task_ids;
            self.stats.indexes_pending = true;
            return Ok(());
        }

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
            if let Err(e) = idx_mgr.rebuild_indexes_for_label(label).await {
                // The label's indexes do not cover the rows just published:
                // keep the planner off them, and let the rebuild trigger pick
                // them up.
                self.mark_label_indexes_stale(label);
                return Err(e);
            }
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
                // #233: `.ok()` persisted `row_count_at_build = None`, and
                // `index_rebuild.rs`'s growth trigger is gated on
                // `if let Some(built_count)` — so that index never
                // auto-rebuilt on growth again, silently, until some later
                // build happened to write a count. Recorded so the lost
                // trigger is visible.
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
                    // #233: unlike the Stale write above, failing to record a
                    // SUCCESSFUL build is conservative — the index stays
                    // `Stale` and the rebuild manager picks it up again — so
                    // this does not fail the commit. It is still recorded
                    // rather than dropped: the decision is that no
                    // index-status write goes unrecorded.
                    if let Err(e) = self.backend.schema.update_index_metadata(idx.name(), |m| {
                        m.status = uni_common::core::schema::IndexStatus::Online;
                        m.last_built_at = Some(now);
                        if let Some(count) = row_count {
                            m.row_count_at_build = Some(count);
                        }
                    }) {
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
        Ok(())
    }

    /// Mark every user index on `label` `Stale` in memory. A failure means the
    /// index is gone, which leaves nothing to keep the planner off.
    fn mark_label_indexes_stale(&self, label: &str) {
        let schema = self.backend.schema.schema();
        for idx in schema.indexes.iter().filter(|i| i.label() == label) {
            if let Err(e) = self.backend.schema.update_index_metadata(idx.name(), |m| {
                m.status = uni_common::core::schema::IndexStatus::Stale;
            }) {
                tracing::warn!(index = %idx.name(), error = %e, "bulk commit: could not mark index Stale");
            }
        }
    }

    /// Flush L0 when it holds any row, before the load's snapshot manifest is
    /// written.
    ///
    /// A pinned snapshot reads L1 only, filtered to `_version <= ceiling`, and
    /// the load's manifest puts the ceiling at the load's version, which was
    /// reserved at the load's first write. Rows committed before then carry a
    /// lower version; left in L0 they were missing from the snapshot when it
    /// was taken and joined it once a later flush stored them, so a session
    /// pinned to it saw its content change. Flushed first, they are in the
    /// snapshot from the start. A no-op when L0 is empty, which is when no
    /// such row can exist.
    async fn flush_older_writes(&self) -> Result<()> {
        match (self.backend.writer.as_deref(), self.write_version) {
            (Some(writer), Some(_)) => flush_l0_if_buffered(writer).await,
            _ => Ok(()),
        }
    }

    /// Save a snapshot manifest recording the load's label and edge-type
    /// counts; returns its id.
    async fn save_load_snapshot(&self) -> Result<String> {
        fail::fail_point!("bulk::commit::save-snapshot", |_| Err(anyhow!(
            "injected snapshot manifest failure"
        )));
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
        // The load's rows carry `write_version`, which no flush recorded: a
        // ceiling below it hid the load from a session pinned here, and made
        // the next open restart the version counter below it, reusing it.
        // Rows committed with a lower version are in L1 already
        // (`flush_older_writes`), so the ceiling covers only stored rows.
        if let Some(version) = self.write_version {
            manifest.version_high_water_mark = manifest.version_high_water_mark.max(version);
        }
        // Recomputed, not inherited from the manifest read above: that one
        // may have been cut while this load was staged, and lists it.
        manifest.unpublished_bulk_versions = match self.backend.writer.as_deref() {
            Some(writer) => writer
                .l0_manager
                .unpublished_bulk_at_most(manifest.version_high_water_mark),
            None => Vec::new(),
        };

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

        self.backend
            .storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await
            .map_err(UniError::Internal)?;
        Ok(manifest.snapshot_id)
    }

    /// Check again, now that the load is published, the declared endpoints of
    /// its edges that point at vertices older than the load; see
    /// `crate::endpoint_check`. An edge type with an endpoint that lost its
    /// declared label meanwhile is recorded `Unverified` (saved) and listed in
    /// [`BulkStats::warnings`]. Returns whether the check completed.
    async fn recheck_external_endpoints(&mut self) -> bool {
        let storage = Arc::clone(&self.backend.storage);
        let types =
            crate::endpoint_check::conforming_types(&storage, self.external_endpoints.keys());
        let mut endpoints = std::mem::take(&mut self.external_endpoints);
        endpoints.retain(|name, _| types.contains(name));
        let checked = async {
            let found = crate::endpoint_check::downgrade_violating_types(
                &storage,
                self.backend.writer.as_deref(),
                &endpoints,
            )
            .await?;
            crate::endpoint_check::persist_downgrades(&storage, &found).await?;
            Ok::<_, anyhow::Error>(found)
        }
        .await;
        match checked {
            Ok(found) => {
                for d in found {
                    self.stats
                        .warnings
                        .push(format!("endpoints: {}", d.describe()));
                }
                true
            }
            Err(e) => {
                self.warn_step("endpoint re-check (marker kept for recovery)", e);
                false
            }
        }
    }

    /// Record a failed step after the commit point: it does not decide the
    /// load, which is applied, but the caller is told.
    fn warn_step(&mut self, step: &str, error: impl std::fmt::Display) {
        tracing::warn!(
            load_id = %self.load_id,
            step,
            error = %error,
            "bulk commit: a step after the commit point failed; the load is applied, and the \
             step is completed by recovery at the next open or by the index rebuild worker"
        );
        self.stats.warnings.push(format!("{step}: {error}"));
    }

    /// The steps after every row is visible. None decides the load, so a
    /// failure is reported in [`BulkStats::warnings`] and left to what
    /// completes it later:
    ///
    /// - default and user index builds: the index rebuild worker and the
    ///   index's `Stale` status (marked by recovery when the marker is kept);
    /// - the snapshot manifest (and the flush of older L0 rows before it) and
    ///   the latest pointer: the marker is kept, and recovery raises the
    ///   snapshot ceiling to the load's version at the next open;
    /// - the schema save after the index builds: index status only; the
    ///   schema itself was saved before the commit point;
    /// - the receipt and the marker delete: the marker is kept whenever an
    ///   earlier step that recovery repeats did not succeed, and recovery then
    ///   writes the receipt and deletes the marker at the next open.
    ///
    /// `events` is how the delivery of the load's events ended: interrupted by
    /// a shutdown, the marker is promoted with its events still pending and
    /// kept, so the next open delivers them again.
    async fn finalize(&mut self, plan: &PublishPlan, events: EventDelivery) {
        let events_interrupted = events == EventDelivery::Interrupted;
        let mut keep_marker = events_interrupted;

        // Unchecked, the marker is kept: recovery checks them at the next open.
        let endpoints_checked = self.recheck_external_endpoints().await;
        keep_marker |= !endpoints_checked;

        if let Err(e) = self.index_published_rows().await {
            self.warn_step("default indexes", e);
        }

        let index_start = Instant::now();
        if let Err(e) = self.rebuild_user_indexes().await {
            // The affected indexes are `Stale` in memory, persisted by the
            // schema save below; the rebuild worker picks them up.
            self.warn_step("index rebuild", e);
        }
        self.stats.index_build_duration = index_start.elapsed();

        // 4. Update Snapshot
        self.report_progress(
            BulkPhase::Finalizing,
            self.stats.vertices_inserted + self.stats.edges_inserted,
            None,
        );
        // A missing manifest leaves the snapshot ceiling below the load's
        // version, so the marker is kept: the next open raises it.
        let snapshot_id = match self.flush_older_writes().await {
            Err(e) => {
                keep_marker = true;
                self.warn_step("snapshot manifest (the flush before it)", e);
                None
            }
            Ok(()) => match self.save_load_snapshot().await {
                Ok(id) => Some(id),
                Err(e) => {
                    keep_marker = true;
                    self.warn_step("snapshot manifest", e);
                    None
                }
            },
        };

        let marker = !plan.records.is_empty();
        let store = flush_intent::Markers::for_storage(&self.backend.storage);
        // Every row is visible: promote the marker to `Committed`, so a crash
        // from here on publishes nothing at recovery and only moves the latest
        // pointer (when a manifest was saved).
        if marker
            && let Err(e) = flush_intent::write_committed(
                &store,
                &self.load_id,
                &self.initial_table_versions,
                snapshot_id.as_deref(),
                plan.labels.clone(),
                self.write_version,
                endpoints_checked,
                events_interrupted.then(|| self.loaded_rows()),
            )
            .await
        {
            self.warn_step("commit marker", e);
        }
        // Test seam: a crash after the `Committed` marker, before the latest
        // pointer flip.
        fail::fail_point!("bulk::commit::after-committed-marker");
        if let Some(id) = &snapshot_id
            && let Err(e) = self
                .backend
                .storage
                .snapshot_manager()
                .set_latest_snapshot(id)
                .await
        {
            keep_marker = true;
            self.warn_step("latest snapshot pointer", e);
        }

        // Save schema with updated index metadata
        let schema_saved = async {
            if !self.saves_schema() {
                return Ok(());
            }
            fail::fail_point!("bulk::commit::schema-save", |_| Err(anyhow!(
                "injected schema save failure"
            )));
            self.backend.schema.save().await
        }
        .await;
        if let Err(e) = schema_saved {
            keep_marker = true;
            self.warn_step("schema save", e);
        }

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

        // The load is finalized: record its receipt, then drop the marker.
        // The receipt first, so `bulk_load_applied` always finds one of the
        // two. A marker kept for a failed step is finished by recovery, which
        // writes the receipt itself.
        if marker && keep_marker {
            self.stats
                .warnings
                .push("commit marker kept: the next open completes the steps above".to_string());
            return;
        }
        if let Err(e) = flush_intent::write_receipt(&store, &self.load_id).await {
            if marker {
                self.warn_step("applied receipt (marker kept for recovery)", e);
            } else {
                self.warn_step("applied receipt", e);
            }
            return;
        }
        if marker && let Err(e) = flush_intent::clear(&store, &self.load_id).await {
            self.warn_step("commit marker delete", e);
        }
    }

    /// Commit the load: make every row visible, then build indexes and record
    /// a snapshot.
    ///
    /// # Commit point
    ///
    /// A load is decided by one durable write, its *commit point*: the marker
    /// that records every table's staged rows. The return value says which
    /// side of it the load ended on:
    ///
    /// - **`Ok`** — the load is applied and every row is visible. Steps after
    ///   the commit point that do not decide the load (default and user index
    ///   builds, the snapshot manifest and latest pointer, the index-status
    ///   schema save, the marker delete) may have failed; each is listed in
    ///   [`BulkStats::warnings`] and completed by recovery at the next open or
    ///   by the index rebuild worker.
    /// - **`Err(UniError::BulkCommitIncomplete)`** — the load is applied
    ///   (committed) but publishing it could not be finished in-process after
    ///   bounded retries. A background task keeps retrying it, with backoff,
    ///   until it completes or the database shuts down; the next open
    ///   completes it otherwise, exactly once. Until then none of it is
    ///   visible to this process's readers. **Do not retry it.**
    /// - **`Err(UniError::BulkCommitOutcomeUnknown)`** — writing the commit
    ///   marker failed in a way that may still have written it. Reopen, then
    ///   ask `Uni::bulk_load_applied` before retrying.
    /// - **any other `Err`** — the commit point was not reached: the load is
    ///   not applied, its staged files are discarded as by
    ///   [`Self::abort`], and nothing of it is visible now or after a reopen.
    ///   Retrying is safe.
    ///
    /// The typed errors arrive inside the returned `anyhow::Error`; use
    /// `err.downcast_ref::<UniError>()`.
    ///
    /// While the tables are being published, none of the load is visible to
    /// readers of this database handle: every row of a load carries one
    /// `_version`, which the storage manager's bulk visibility gate hides
    /// until the last table is published. Another process, or another handle
    /// on the same path, is not gated and can see a load part way.
    ///
    /// A load on a fork keeps its marker and receipt in the fork's catalog
    /// namespace and returns the same outcomes; the fork completes it when it
    /// is next opened, and `Session::bulk_load_applied` on the fork answers
    /// for it.
    ///
    /// Once the load is visible, and before indexes are finalized, `commit`
    /// announces it and delivers its create events to after-phase triggers
    /// and CDC through [`BulkBackend::events`], reading its rows back in
    /// chunks of at most [`crate::BULK_EVENT_CHUNK_ROWS`]; see
    /// [`crate::events`]. It waits while CDC has
    /// `MUTATION_STREAM_DEPTH` chunks undelivered. The delivery runs on a task
    /// of its own, so dropping this future once the load is visible does not
    /// cut it short; the dropped commit skips finalizing the load (index
    /// builds, snapshot), which the next open completes -- delivering the
    /// load's events a second time. A trigger registered after a label's rows
    /// were staged that would make the load refused (see
    /// [`BulkWriterBuilder::skip_before_triggers`]) is caught here, before the
    /// commit point, and the load is discarded.
    ///
    /// # Errors
    ///
    /// As listed above, and `UniError::BulkLoadBlockedByTrigger`.
    pub async fn commit(mut self) -> Result<BulkStats> {
        if let Err(e) = self.check_before_triggers_for_load() {
            self.discard_after_failed_commit().await;
            return Err(e);
        }
        let plan = match self.prepare_commit().await {
            Ok(plan) => plan,
            Err(e) => {
                self.discard_after_failed_commit().await;
                return Err(e);
            }
        };

        match self.reach_commit_point(&plan).await {
            CommitPoint::Reached => {}
            CommitPoint::NotReached(e) => {
                self.discard_after_failed_commit().await;
                return Err(e);
            }
            CommitPoint::Unknown(message) => {
                // The marker may exist, and if it does the next open publishes
                // from these staged files: they must not be discarded, and must
                // survive any cleanup this process runs.
                self.keep_staged_until_exit();
                tracing::error!(load_id = %self.load_id, %message, "bulk commit outcome unknown");
                return Err(UniError::BulkCommitOutcomeUnknown {
                    load_id: self.load_id.clone(),
                    message,
                }
                .into());
            }
        }

        // Hide the load from this process's readers until every table is
        // published: Lance commits each table on its own, and a reader between
        // two of them saw part of the load.
        let hold = self.hide_load().await;
        if let Err(e) = self.complete_publish(&plan).await {
            if plan.records.is_empty() {
                // Unreachable while the backend records whatever it staged;
                // kept so one that does not cannot claim a completion it has
                // no record to drive.
                self.keep_staged_until_exit();
                return Err(UniError::Internal(anyhow!(
                    "bulk load failed part way through publishing and keeps no record to \
                     complete it from: {e:#}"
                ))
                .into());
            }
            tracing::error!(
                load_id = %self.load_id,
                error = %e,
                "bulk commit: the load is committed but could not be published in-process; \
                 it stays hidden while a background task completes it (or the next open does)"
            );
            self.spawn_redrive(&plan, hold);
            return Err(UniError::BulkCommitIncomplete {
                load_id: self.load_id.clone(),
                message: format!("{e:#}"),
            }
            .into());
        }
        let visible = hold.is_some();
        show_load(&self.backend.storage, hold, &self.touched_edge_types);
        // Visible now: a ceiling captured from here on includes the load.
        self.version_reservation = None;
        // Every staged file is referenced by a table version now.
        self.staged_holds.clear();
        // Announce the load and stream its events to after-phase triggers and
        // CDC. Before the index finalization below, which can take long and
        // does not change what is visible.
        let events = if visible {
            self.publish_events().await
        } else {
            EventDelivery::Finished
        };

        self.finalize(&plan, events).await;

        self.committed = true;
        self.stats.duration = self.start_time.elapsed();
        Ok(self.stats.clone())
    }

    /// Discard the load after a failure before its commit point, as
    /// [`Self::abort`] does; the caller returns the original error.
    async fn discard_after_failed_commit(&mut self) {
        if let Err(e) = self.discard_load().await {
            tracing::warn!(
                load_id = %self.load_id,
                error = %e,
                "bulk commit failed before its commit point and discarding the load failed too; \
                 its unreferenced files are reclaimed by version cleanup"
            );
        }
    }

    /// Refuse to write `kind` rows under `name` when a synchronous
    /// before-trigger would see them, unless the caller opted out with
    /// [`BulkWriterBuilder::skip_before_triggers`].
    ///
    /// A load's events exist only once it is visible, past any veto, so such
    /// a trigger cannot be honoured; refusing before the rows are staged keeps
    /// the load from ever becoming visible behind its back.
    fn check_before_triggers(&self, kind: crate::events::BulkWriteKind, name: &str) -> Result<()> {
        if self.config.skip_before_triggers {
            return Ok(());
        }
        let Some(sink) = &self.backend.events else {
            return Ok(());
        };
        match sink.blocking_trigger(kind, name) {
            None => Ok(()),
            Some(trigger) => Err(UniError::BulkLoadBlockedByTrigger {
                trigger,
                target: match kind {
                    crate::events::BulkWriteKind::Vertex => format!("label '{name}'"),
                    crate::events::BulkWriteKind::Edge => format!("edge type '{name}'"),
                },
            }
            .into()),
        }
    }

    /// [`Self::check_before_triggers`] for everything the load wrote: a
    /// trigger registered after a label's rows were staged is caught here,
    /// before the commit point.
    fn check_before_triggers_for_load(&self) -> Result<()> {
        for label in &self.touched_labels {
            self.check_before_triggers(crate::events::BulkWriteKind::Vertex, label)?;
        }
        for edge_type in &self.touched_edge_types {
            self.check_before_triggers(crate::events::BulkWriteKind::Edge, edge_type)?;
        }
        Ok(())
    }

    /// Rows this load wrote: vertices plus edges.
    fn loaded_rows(&self) -> usize {
        self.stats.vertices_inserted + self.stats.edges_inserted
    }

    /// Hand the now-visible load to the event sink; see [`crate::events`].
    ///
    /// The sink runs on a task of its own, tracked like the re-drive, and
    /// this waits for it. Once the load is visible its events must be
    /// delivered whole: run inline, a caller dropping `commit()` part way
    /// would cut the delivery short -- before the announcement, a load CDC
    /// never hears of; mid-stream, the rest of the load lost to triggers and
    /// every CDC stream halted. Dropping `commit()` now drops only this wait.
    async fn publish_events(&self) -> EventDelivery {
        let (Some(sink), Some(write_version)) = (&self.backend.events, self.write_version) else {
            return EventDelivery::Finished;
        };
        let mut labels: Vec<String> = self.touched_labels.iter().cloned().collect();
        labels.sort();
        let mut edge_types: Vec<String> = self.touched_edge_types.iter().cloned().collect();
        edge_types.sort();
        let reader = crate::events::LoadEventReader::new(
            Arc::clone(&self.backend.storage),
            write_version,
            !labels.is_empty(),
            !edge_types.is_empty(),
        );
        let load = crate::events::VisibleLoad {
            load_id: self.load_id.clone(),
            write_version,
            labels,
            edge_types,
            rows: self.loaded_rows(),
            reader,
        };
        let sink = Arc::clone(sink);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let delivery = sink.publish(load).await;
            // The committer may have stopped waiting; the delivery stands.
            let _ = done_tx.send(delivery);
        });
        self.backend.track(handle);
        // A task that ended without answering panicked, or was aborted by the
        // runtime shutting down: the delivery did not finish.
        done_rx.await.unwrap_or(EventDelivery::Interrupted)
    }

    /// Hide this load's version from the storage manager's readers; `None`
    /// when nothing is staged (nothing would become visible).
    pub(crate) async fn hide_load(&self) -> Option<GateHold> {
        if self.staged.is_empty() {
            return None;
        }
        let version = self.write_version?;
        Some(self.backend.storage.bulk_gate().hide(version).await)
    }

    /// Hand a committed load whose publish `commit` could not finish to a
    /// background task that retries it until it completes or the database
    /// shuts down. The load stays hidden meanwhile.
    fn spawn_redrive(&mut self, plan: &PublishPlan, hold: Option<GateHold>) {
        let redrive = self.redrive(plan, hold);
        let shutdown = self.backend.shutdown.subscribe();
        let handle = tokio::spawn(redrive.run(shutdown));
        self.backend.track(handle);
    }

    /// The re-drive of this committed load, taking over what it still owns:
    /// the gate hold, the staged-file holds and the endpoints to re-check.
    pub(crate) fn redrive(&mut self, plan: &PublishPlan, hold: Option<GateHold>) -> Redrive {
        self.staged.clear();
        Redrive {
            version: self.write_version,
            storage: Arc::clone(&self.backend.storage),
            writer: self.backend.writer.clone(),
            external_endpoints: std::mem::take(&mut self.external_endpoints),
            markers: flush_intent::Markers::for_storage(&self.backend.storage),
            load_id: self.load_id.clone(),
            tables: flush_intent::table_map(&self.initial_table_versions),
            records: plan.records.clone(),
            labels: plan.labels.clone(),
            edge_types: self.touched_edge_types.iter().cloned().collect(),
            written_vids: std::mem::take(&mut self.written_vids),
            hold,
            version_reservation: self.version_reservation.take(),
            staged_holds: self.staged_holds.drain().map(|(_, h)| h).collect(),
            events: self.backend.events.clone(),
            load_rows: self.stats.vertices_inserted + self.stats.edges_inserted,
            vertex_labels: self.touched_labels.iter().cloned().collect(),
        }
    }

    /// Keep this load's staged files from cleanup for the rest of the process:
    /// the next open may publish from them.
    fn keep_staged_until_exit(&mut self) {
        self.staged.clear();
        for (_, hold) in self.staged_holds.drain() {
            hold.keep_until_exit();
        }
    }

    /// The id of this load.
    ///
    /// Stable for the writer's lifetime and recorded at the commit point, so
    /// after a crash or an ambiguous commit outcome
    /// `Uni::bulk_load_applied(id)` says whether the load was applied. Also in
    /// [`BulkStats::load_id`] and in the `BulkCommitIncomplete` /
    /// `BulkCommitOutcomeUnknown` errors.
    #[must_use]
    pub fn load_id(&self) -> &str {
        &self.load_id
    }

    /// Abort bulk loading and discard everything it wrote.
    ///
    /// Nothing this load wrote was ever visible, so abort discards its staged
    /// files and buffered rows and touches no table.
    ///
    /// # Errors
    ///
    /// Returns an error if the load is already committed. Failing to delete a
    /// staged file is not an error: an unreferenced file is unreachable, and
    /// version cleanup reclaims it.
    pub async fn abort(mut self) -> Result<()> {
        if self.committed {
            return Err(anyhow!("Cannot abort: bulk load already committed"));
        }
        self.discard_load().await
    }

    /// Discard everything this load wrote; see [`Self::abort`].
    async fn discard_load(&mut self) -> Result<()> {
        // 1. Clear pending buffers (not yet flushed to storage)
        self.pending_vertices.clear();
        self.pending_edges.clear();
        self.written_vids.clear();
        self.buffer_size_bytes = 0;

        let storage = Arc::clone(&self.backend.storage);
        let backend = storage.backend();

        // 2. Staged rows: never visible, so there is nothing to roll back.
        // Reclaiming their files is best effort — an unreferenced file is
        // unreachable, and the load is discarded whether or not the space comes
        // back now — so a failure is reported, not returned.
        let mut discarded = 0usize;
        for (table_name, runs) in self.staged.drain() {
            discarded += runs.iter().map(StagedAppend::rows).sum::<usize>();
            if let Err(e) = backend.discard_staged(&table_name, runs).await {
                tracing::warn!(
                    table = %table_name,
                    error = %e,
                    "bulk abort: could not delete staged files; they are unreferenced and \
                     will be reclaimed by version cleanup",
                );
            }
        }
        // Nothing staged is needed any more; cleanup may reclaim what is left.
        self.staged_holds.clear();
        // Nothing was visible, so nothing is rolled back -- the versions
        // recorded for the commit marker bound recovery's search, not an undo,
        // and restoring them would discard what other writers committed
        // meanwhile.
        tracing::info!(discarded, "bulk load aborted; discarded its staged rows");
        Ok(())
    }

    /// Whether this load persists the schema.
    ///
    /// Not on a fork: a fork's `SchemaManager` is `primary ⊕ overlay` but keeps
    /// primary's path, so a save would write the fork's merged view -- its
    /// fork-local labels included -- over primary's `catalog/schema.json`.
    /// Fork-local schema persists through the fork registry's overlay.
    fn saves_schema(&self) -> bool {
        self.backend.storage.fork_scope().is_none()
    }

    fn report_progress(&self, phase: BulkPhase, rows: usize, label: Option<String>) {
        if let Some(cb) = &self.progress_callback {
            // A callback that panicked poisoned the lock, not the callback.
            let cb = cb.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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

/// Flush `writer`'s L0 to L1 when any buffer holds a row: the current one or
/// one still being flushed. See [`BulkWriter::flush_older_writes`].
async fn flush_l0_if_buffered(writer: &Writer) -> Result<()> {
    let buffered = writer.l0_manager.get_current().read().mutation_count > 0
        || !writer.l0_manager.get_pending_flush().is_empty();
    if buffered {
        writer.flush_to_l1(None).await?;
    }
    Ok(())
}

/// Show a load hidden by [`BulkWriter::hide_load`], dropping -- under the
/// gate's lock, so no reader sees the load without them dropped -- the cached
/// adjacency CSRs of its edge types, which were built without its edges.
pub(crate) fn show_load(
    storage: &StorageManager,
    hold: Option<GateHold>,
    edge_types: &HashSet<String>,
) {
    let Some(hold) = hold else {
        return;
    };
    let schema = storage.schema_manager().schema();
    let ids: Vec<u32> = edge_types
        .iter()
        .filter_map(|name| schema.edge_types.get(name).map(|meta| meta.id))
        .collect();
    hold.release(|| {
        for &id in &ids {
            for &dir in uni_store::storage::direction::Direction::Both.expand() {
                storage.invalidate_adjacency_csr(id, dir);
            }
        }
    });
}

/// The background completion of a committed load whose publish `commit` could
/// not finish (it returned `BulkCommitIncomplete`).
///
/// Retries [`flush_intent::publish_records`] -- the exactly-once path recovery
/// takes at open -- with exponential backoff, keeping the load hidden until
/// every table is published. Then it does what recovery would: shows the load,
/// registers its vertices' labels, marks the labels' user indexes `Stale`,
/// writes the receipt and deletes the marker. It stops when the database shuts
/// down, leaving the load hidden and its marker for the next open.
pub(crate) struct Redrive {
    /// The load's `_version`, which the snapshot ceiling is raised to.
    version: Option<u64>,
    storage: Arc<StorageManager>,
    /// Flushes older L0 rows before the ceiling is raised, and resolves the
    /// endpoints' current labels.
    writer: Option<Arc<Writer>>,
    /// See [`BulkWriter::recheck_external_endpoints`].
    external_endpoints: std::collections::BTreeMap<String, crate::endpoint_check::TypeEndpoints>,
    markers: flush_intent::Markers,
    load_id: String,
    tables: std::collections::BTreeMap<String, Option<u64>>,
    records: Vec<flush_intent::StagedTablePublish>,
    labels: Vec<String>,
    edge_types: HashSet<String>,
    written_vids: HashMap<String, Vec<Vid>>,
    hold: Option<GateHold>,
    /// Released once the load is shown; see `BulkWriter::version_reservation`.
    version_reservation: Option<uni_store::runtime::l0_manager::BulkVersionReservation>,
    staged_holds: Vec<StagedFilesHold>,
    /// Receives the load's events once it is shown; see [`crate::events`].
    events: Option<Arc<dyn crate::events::BulkEventSink>>,
    /// Rows the load wrote, for its announcement.
    load_rows: usize,
    /// Labels the load wrote vertices under, for its announcement.
    vertex_labels: Vec<String>,
}

impl Redrive {
    /// Retry until the load is published, then finish it; or stop on the
    /// database's shutdown or compaction cancel.
    pub(crate) async fn run(self, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
        let cancel = self.storage.compaction_cancel_token().clone();
        let mut delay = REDRIVE_INITIAL_BACKOFF;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let outcome = tokio::select! {
                biased;
                _ = shutdown.recv() => None,
                () = cancel.cancelled() => None,
                r = async {
                    tokio::time::sleep(delay).await;
                    self.attempt().await
                } => Some(r),
            };
            match outcome {
                None => {
                    self.stop();
                    return;
                }
                Some(Ok(published)) => {
                    tracing::warn!(
                        load_id = %self.load_id,
                        attempt,
                        published,
                        "bulk load re-drive: publish completed in the background"
                    );
                    break;
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        load_id = %self.load_id,
                        attempt,
                        error = %e,
                        retry_in = ?(delay * 2).min(REDRIVE_MAX_BACKOFF),
                        "bulk load re-drive: publish attempt failed; the load stays hidden"
                    );
                    delay = (delay * 2).min(REDRIVE_MAX_BACKOFF);
                }
            }
        }
        self.finish().await;
    }

    async fn attempt(&self) -> Result<usize> {
        fail::fail_point!("bulk::redrive::attempt", |_| Err(anyhow!(
            "injected bulk re-drive failure"
        )));
        flush_intent::publish_records(&self.storage, &self.tables, &self.records, |_| Ok(())).await
    }

    /// Shut down part way: the load stays hidden, and its staged files stay
    /// protected from cleanup, for the next open to complete.
    fn stop(self) {
        tracing::info!(
            load_id = %self.load_id,
            "bulk load re-drive stopped by shutdown; the next open completes the load"
        );
        for hold in self.staged_holds {
            hold.keep_until_exit();
        }
    }

    async fn finish(mut self) {
        let visible = self.hold.is_some();
        show_load(&self.storage, self.hold.take(), &self.edge_types);
        self.version_reservation = None;
        // The load is visible from here: announce it and deliver its events,
        // as `BulkWriter::commit` does for a load it publishes itself.
        let mut events = EventDelivery::Finished;
        if visible && let (Some(sink), Some(write_version)) = (self.events.clone(), self.version) {
            let mut labels = std::mem::take(&mut self.vertex_labels);
            labels.sort();
            let mut edge_types: Vec<String> = self.edge_types.iter().cloned().collect();
            edge_types.sort();
            let reader = crate::events::LoadEventReader::new(
                Arc::clone(&self.storage),
                write_version,
                !labels.is_empty(),
                !edge_types.is_empty(),
            );
            let delivery = sink
                .publish(crate::events::VisibleLoad {
                    load_id: self.load_id.clone(),
                    write_version,
                    labels,
                    edge_types,
                    rows: self.load_rows,
                    reader,
                })
                .await;
            events = delivery;
        }
        for (label, vids) in std::mem::take(&mut self.written_vids) {
            for vid in vids {
                self.storage
                    .update_vid_labels_index(vid, vec![label.clone()]);
            }
        }
        // Every staged file is referenced by a table version now.
        self.staged_holds.clear();
        if events == EventDelivery::Interrupted {
            // Shut down part way through the delivery: the marker stays
            // `Active`, so the next open repeats the bookkeeping below and
            // delivers the load's events again.
            tracing::info!(
                load_id = %self.load_id,
                "bulk load re-drive: event delivery stopped by shutdown; the next open delivers \
                 the load's events again"
            );
            return;
        }
        // The steps below only finish the bookkeeping; the marker is kept on
        // any failure and the next open repeats them.
        let types =
            crate::endpoint_check::conforming_types(&self.storage, self.external_endpoints.keys());
        let mut endpoints = std::mem::take(&mut self.external_endpoints);
        endpoints.retain(|name, _| types.contains(name));
        let checked = async {
            let found = crate::endpoint_check::downgrade_violating_types(
                &self.storage,
                self.writer.as_deref(),
                &endpoints,
            )
            .await?;
            crate::endpoint_check::persist_downgrades(&self.storage, &found).await
        }
        .await;
        if let Err(e) = checked {
            tracing::warn!(load_id = %self.load_id, error = %e, "bulk load re-drive: checking the published edges' declared endpoints failed; the marker is kept for the next open, which checks them");
            return;
        }
        if let Err(e) = flush_intent::mark_indexes_stale(&self.storage, &self.labels).await {
            tracing::warn!(load_id = %self.load_id, error = %e, "bulk load re-drive: marking indexes Stale failed; the marker is kept for the next open");
            return;
        }
        // Rows committed before the load (lower versions) must be stored
        // before the ceiling covers them; see `BulkWriter::flush_older_writes`.
        if let Some(writer) = self.writer.as_deref()
            && let Err(e) = flush_l0_if_buffered(writer).await
        {
            tracing::warn!(load_id = %self.load_id, error = %e, "bulk load re-drive: flushing before raising the snapshot ceiling failed; the marker is kept for the next open");
            return;
        }
        if let Some(version) = self.version
            && let Err(e) = flush_intent::raise_snapshot_hwm(&self.storage, version).await
        {
            tracing::warn!(load_id = %self.load_id, error = %e, "bulk load re-drive: raising the snapshot ceiling failed; the marker is kept for the next open");
            return;
        }
        if let Err(e) = flush_intent::write_receipt(&self.markers, &self.load_id).await {
            tracing::warn!(load_id = %self.load_id, error = %e, "bulk load re-drive: writing the receipt failed; the marker is kept for the next open");
            return;
        }
        if let Err(e) = flush_intent::clear(&self.markers, &self.load_id).await {
            tracing::warn!(load_id = %self.load_id, error = %e, "bulk load re-drive: deleting the marker failed; the next open deletes it");
        }
    }
}
