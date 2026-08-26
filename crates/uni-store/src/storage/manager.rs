// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::StorageBackend;
#[cfg(feature = "lance-backend")]
use crate::backend::lance::LanceDbBackend;
use crate::backend::table_names;
use crate::backend::types::{FilterExpr, OptimizeReport, ScanRequest, VectorQueryOpts};
use crate::compaction::{CompactionStats, CompactionStatus, CompactionTask};
use crate::runtime::WorkingGraph;
use crate::runtime::context::QueryContext;
use crate::runtime::l0::L0Buffer;
use crate::storage::adjacency::AdjacencyDataset;
use crate::storage::compaction::{Compactor, SemanticCompactionReport};
use crate::storage::delta::{DeltaDataset, ENTRY_SIZE_ESTIMATE, Op};
use crate::storage::direction::Direction;
#[cfg(feature = "lance-backend")]
use crate::storage::edge::EdgeDataset;
#[cfg(feature = "lance-backend")]
use crate::storage::index::UidIndex;
#[cfg(feature = "lance-backend")]
use crate::storage::inverted_index::InvertedIndex;
use crate::storage::main_edge::MainEdgeDataset;
use crate::storage::main_vertex::MainVertexDataset;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use arrow_array::{Array, Float32Array, TimestampNanosecondArray, UInt64Array};
use object_store::ObjectStore;
#[cfg(feature = "lance-backend")]
use object_store::local::LocalFileSystem;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use uni_common::config::UniConfig;
#[cfg(feature = "lance-backend")]
use uni_common::core::id::UniId;
use uni_common::core::id::{Eid, Vid};
#[cfg(feature = "lance-backend")]
use uni_common::core::schema::IndexDefinition;
use uni_common::core::schema::{DistanceMetric, SchemaManager};
use uni_common::sync::acquire_mutex;

use crate::snapshot::manager::SnapshotManager;
use crate::storage::IndexManager;
use crate::storage::adjacency_manager::AdjacencyManager;
use crate::storage::resilient_store::ResilientObjectStore;

use uni_common::core::snapshot::SnapshotManifest;

use uni_common::graph::simple_graph::Direction as GraphDirection;

/// Edge state during subgraph loading - tracks version and deletion status.
struct EdgeState {
    neighbor: Vid,
    version: u64,
    deleted: bool,
}

pub struct StorageManager {
    base_uri: String,
    store: Arc<dyn ObjectStore>,
    schema_manager: Arc<SchemaManager>,
    snapshot_manager: Arc<SnapshotManager>,
    adjacency_manager: Arc<AdjacencyManager>,
    /// See `set_requires_adjacency_revalidation`. Defaults to `false`: the
    /// common case is the single handle that owns the writer.
    requires_adjacency_revalidation: bool,
    pub config: UniConfig,
    pub compaction_status: Arc<Mutex<CompactionStatus>>,
    /// Counter of in-flight `flush_to_l1` operations. Compaction skips
    /// delta-clear when this is non-zero to avoid wiping rows a flush is
    /// about to append. Counter (not bool) so multiple async flushes can
    /// be in flight concurrently.
    pub flush_in_progress: std::sync::atomic::AtomicUsize,
    /// Optional pinned snapshot for time-travel
    pinned_snapshot: Option<SnapshotManifest>,
    /// Optional row-version pin for transaction snapshot reads (C2).
    ///
    /// When set, L1 scans filter to `_version <= hwm` exactly like a pinned
    /// snapshot, but WITHOUT a manifest: a read-write transaction pins the
    /// version counter observed at begin (`SnapshotView.started_at_version`)
    /// so an L0→L1 flush completing mid-transaction cannot leak
    /// post-snapshot rows into its scans. Mutually exclusive with
    /// `pinned_snapshot`.
    pinned_version_hwm: Option<u64>,
    /// Keeps this view's version registered in the shared
    /// [`PinnedVersions`](crate::storage::adjacency_manager::PinnedVersions)
    /// registry so shadow-CSR GC cannot reclaim entries it still resolves.
    ///
    /// Only `pinned_at_version` sets it: `pinned()` and `at_fork` each build a
    /// *fresh* `AdjacencyManager`, so they read their own empty shadow and have
    /// nothing to protect. Released when the view drops.
    #[expect(
        dead_code,
        reason = "RAII guard: held solely so its Drop deregisters this view's pinned version. Never read."
    )]
    pin_guard: Option<crate::storage::adjacency_manager::PinGuard>,
    /// Optional fork scope for branch-aware reads (Phase 1 read-only).
    ///
    /// Mutually exclusive with `pinned_snapshot`: a single
    /// `StorageManager` is either pinned to a snapshot or scoped to a
    /// fork, never both. Phase 4's `pin_to_version` on a forked session
    /// builds a separate combined manager out of band; Phase 1 forbids
    /// mixing.
    fork_scope: Option<Arc<crate::fork::ForkScope>>,
    /// Pluggable storage backend.
    backend: Arc<dyn StorageBackend>,
    /// In-memory VID-to-labels index for O(1) label lookups.
    ///
    /// Always present: populated at startup via [`Self::rebuild_vid_labels_index`]
    /// and kept current at flush time. Traversal-time label predicates
    /// (`MATCH (a)-[r]->(b:B)`) read it to resolve labels for vertices that
    /// have aged out of L0 into Lance storage — notably on forks, whose data
    /// is flushed to Lance before branching.
    vid_labels_index: Arc<parking_lot::RwLock<crate::storage::vid_labels::VidLabelsIndex>>,
    /// Optional plugin registry for registry-dispatched CRDT merges on the
    /// durable paths (compaction, L0 flush).
    ///
    /// Behavior-preserving when absent: the durable merge helpers fall back to
    /// [`uni_crdt::Crdt::try_merge`] bit-for-bit when no
    /// [`uni_plugin::traits::crdt::CrdtKindProvider`] is registered. Threaded
    /// down to the writer's `L0Manager` (and hence each `L0Buffer`) and read by
    /// the [`crate::storage::compaction::Compactor`] via
    /// [`Self::plugin_registry`].
    plugin_registry: Option<Arc<uni_plugin::PluginRegistry>>,
}

/// RAII counter increment for `StorageManager.flush_in_progress`.
///
/// Acquired during the rotate phase of a flush (see
/// `runtime::writer::flush_l0_rotate`) and dropped when the full
/// rotate/stream/finalize pipeline completes. Compaction's delta-clear
/// gate skips while this counter is non-zero, so the counter must
/// reflect "flush has started, has not completed" — including any
/// async stream phase running on a spawned task.
pub struct FlushInProgressGuard {
    storage: Arc<StorageManager>,
}

impl FlushInProgressGuard {
    pub fn new(storage: &Arc<StorageManager>) -> Self {
        storage
            .flush_in_progress
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self {
            storage: storage.clone(),
        }
    }
}

impl Drop for FlushInProgressGuard {
    fn drop(&mut self) {
        // M-PANIC-IS-STOP: must not panic in Drop. Atomic op cannot fail.
        self.storage
            .flush_in_progress
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Whether a Lance error represents a commit conflict that retrying may
/// resolve. These fire under async-flush when ≥2 streams concurrently try
/// to create the same table OR when an Append races with a still-in-progress
/// Overwrite (create_table). See the Lance commit-conflict-resolver in
/// `lance-3.0.1/src/io/commit/conflict_resolver.rs`.
fn is_lance_conflict(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    msg.contains("Incompatible transaction") || msg.contains("conflict")
}

/// Runs `op` with exponential-backoff retry on Lance commit conflicts.
/// Up to 10 attempts (~10s worst case); backoff is 1ms, 2ms, 4ms, ...,
/// 512ms. Non-conflict errors return immediately. `op` is re-invoked each
/// attempt so it can re-check table existence and adjust strategy.
async fn retry_on_lance_conflict<F, Fut>(mut op: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    for attempt in 0u32..10 {
        match op().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if !is_lance_conflict(&e) || attempt == 9 {
                    return Err(e);
                }
                let backoff_ms = 1u64 << attempt;
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    unreachable!("retry loop exits via Ok or Err")
}

/// Combine a version high-water-mark predicate with an optional caller filter.
///
/// `hwm` is passed in rather than read off `StorageManager` because the vertex
/// and edge scan paths deliberately source it differently — see the call sites.
fn combine_hwm_filter(hwm: Option<u64>, extra: Option<&FilterExpr>) -> Option<FilterExpr> {
    match (hwm, extra) {
        (Some(hwm), Some(f)) => Some(FilterExpr::all([
            FilterExpr::version_at_most(hwm),
            f.clone(),
        ])),
        (Some(hwm), None) => Some(FilterExpr::version_at_most(hwm)),
        (None, Some(f)) => Some(f.clone()),
        (None, None) => None,
    }
}

/// Narrow `columns` to those that physically exist in the table's schema.
///
/// Returns `None` when the table has no readable schema — callers treat that
/// as "table absent" and return `Ok(None)`. An empty `Vec` is distinct: the
/// schema exists but none of the requested columns do, and the scan still runs.
async fn existing_columns(
    backend: &dyn StorageBackend,
    table_name: &str,
    columns: &[&str],
) -> Result<Option<Vec<String>>> {
    let Some(table_schema) = backend.get_table_schema(table_name).await? else {
        return Ok(None);
    };
    let table_field_names: HashSet<&str> = table_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    Ok(Some(
        columns
            .iter()
            .copied()
            .filter(|c| table_field_names.contains(c))
            .map(|s| s.to_string())
            .collect(),
    ))
}

/// Run a scan and concatenate its batches, or `Ok(None)` when it yields none.
///
/// Fail closed: a scan error (transient I/O, an unparsable filter, a
/// corrupt fragment) must propagate, never be silently mapped to
/// `Ok(None)`. Callers treat `Ok(None)` as "no rows" — e.g. the MERGE
/// fast path would create a duplicate node on a transient failure (review
/// bug #3a) — so an error here must surface as an error. A genuinely-
/// absent table is handled by the caller before it gets here.
async fn scan_concat(
    backend: &dyn StorageBackend,
    request: ScanRequest,
) -> Result<Option<arrow_array::RecordBatch>> {
    let batches = backend.scan(request).await?;
    if batches.is_empty() {
        Ok(None)
    } else {
        Ok(Some(arrow::compute::concat_batches(
            &batches[0].schema(),
            &batches,
        )?))
    }
}

/// What a merge-insert should do when its target table does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingTable {
    /// Fail. Correct when the caller's payload only makes sense against rows
    /// that must already be there — a partial SET can only update a row whose
    /// full-row Append has happened, so a missing table means that Append was
    /// lost and silence would hide it.
    Error,
    /// Succeed, having done nothing. Correct for a tombstone: "no such table"
    /// is the degenerate case of "no rows matched", and an unmatched tombstone
    /// is already defined as a no-op.
    SkipAsNoOp,
}

/// MergeInsert sibling of `write_batch_with_lance_conflict_retry`.
///
/// Source `batch` must contain the join columns in `on` plus any
/// columns to update. Matched rows have `WhenMatched::UpdateAll`
/// applied; unmatched source rows are dropped (partial writes never
/// INSERT). Returns an error if the target table does not exist —
/// see [`merge_insert_batch_tolerating_missing_table`] for the callers
/// where that is a no-op rather than a fault.
/// Retries on Lance commit conflicts via `retry_on_lance_conflict`.
/// RecordBatch clones are cheap (column data is Arc'd).
pub async fn merge_insert_batch_with_lance_conflict_retry(
    backend: &dyn crate::backend::StorageBackend,
    table_name: &str,
    batch: arrow_array::RecordBatch,
    on: &[&str],
) -> anyhow::Result<()> {
    merge_insert_batch_inner(backend, table_name, batch, on, MissingTable::Error).await
}

/// As [`merge_insert_batch_with_lance_conflict_retry`], but a missing target
/// table is a no-op rather than an error.
///
/// For tombstone writes (#182). `L0Buffer::delete_vertex` drops the vid from
/// `vertex_properties` and records a tombstone, so a vertex created and deleted
/// inside one flush window leaves the window with a tombstone and no live rows.
/// The flush then skips the full-row write that would have created the table,
/// and the tombstone has no target — but there is also nothing to tombstone, so
/// the correct answer is to do nothing rather than to fail a `flush()` the
/// caller may not even have invoked (`auto_flush_interval` fires on a timer).
pub async fn merge_insert_batch_tolerating_missing_table(
    backend: &dyn crate::backend::StorageBackend,
    table_name: &str,
    batch: arrow_array::RecordBatch,
    on: &[&str],
) -> anyhow::Result<()> {
    merge_insert_batch_inner(backend, table_name, batch, on, MissingTable::SkipAsNoOp).await
}

async fn merge_insert_batch_inner(
    backend: &dyn crate::backend::StorageBackend,
    table_name: &str,
    batch: arrow_array::RecordBatch,
    on: &[&str],
    missing: MissingTable,
) -> anyhow::Result<()> {
    // NOTE: `backend.merge_insert` already takes the per-table write lock
    // internally (the same mutex `lock_table_for_write` exposes), so it is
    // serialized against a compaction that holds that lock across its whole
    // scan → overwrite. Do NOT take `lock_table_for_write` here as well — that
    // would re-lock the same non-reentrant mutex and self-deadlock.
    retry_on_lance_conflict(|| async {
        // Inside the retry, not hoisted: a concurrent flush may create the table
        // between attempts, and this way the retry still merges. A `table_exists`
        // guard at the call site would instead widen a TOCTOU window outside the
        // write lock the backend takes internally.
        let exists = backend.table_exists(table_name).await?;
        if !exists {
            match missing {
                MissingTable::SkipAsNoOp => {
                    log::debug!(
                        "merge_insert target table '{}' does not exist; \
                         treating as a no-op",
                        table_name
                    );
                    return Ok(());
                }
                MissingTable::Error => anyhow::bail!(
                    "merge_insert target table '{}' does not exist; the full-row \
                     Append that creates it must precede any merge. Callers for \
                     which an unmatched merge is a no-op should use \
                     `merge_insert_batch_tolerating_missing_table`.",
                    table_name
                ),
            }
        }
        backend
            .merge_insert(table_name, on, vec![batch.clone()])
            .await
    })
    .await
}

/// Race-safe write: creates the table if missing, otherwise appends.
/// Each attempt re-checks `table_exists` and adjusts strategy: Append if
/// now-exists, Create if still-missing. Retries on Lance commit conflicts
/// via `retry_on_lance_conflict`.
///
/// Used by every dataset's `write_batch` helper to absorb the Lance
/// commit-conflict-resolver behavior. RecordBatch clones are cheap
/// (column data is Arc'd).
pub async fn write_batch_with_lance_conflict_retry(
    backend: &dyn crate::backend::StorageBackend,
    table_name: &str,
    batch: arrow_array::RecordBatch,
) -> anyhow::Result<()> {
    use crate::backend::types::WriteMode;
    retry_on_lance_conflict(|| async {
        let exists = backend.table_exists(table_name).await?;
        if exists {
            backend
                .write(table_name, vec![batch.clone()], WriteMode::Append)
                .await
        } else {
            backend.create_table(table_name, vec![batch.clone()]).await
        }
    })
    .await
}

/// Helper to manage compaction_in_progress flag
struct CompactionGuard {
    status: Arc<Mutex<CompactionStatus>>,
}

impl CompactionGuard {
    fn new(status: Arc<Mutex<CompactionStatus>>) -> Option<Self> {
        let mut s = acquire_mutex(&status, "compaction_status").ok()?;
        if s.compaction_in_progress {
            return None;
        }
        s.compaction_in_progress = true;
        Some(Self {
            status: status.clone(),
        })
    }
}

impl Drop for CompactionGuard {
    fn drop(&mut self) {
        // CRITICAL: Never panic in Drop - panicking in drop() = process ABORT.
        // See issue #18/#150. If the lock is poisoned, log and continue gracefully.
        match uni_common::sync::acquire_mutex(&self.status, "compaction_status") {
            Ok(mut s) => {
                s.compaction_in_progress = false;
                s.last_compaction = Some(std::time::SystemTime::now());
            }
            Err(e) => {
                // Lock is poisoned but we're in Drop - cannot panic.
                // Log the error and continue. System state may be inconsistent but at least
                // we don't abort the process.
                log::error!(
                    "CompactionGuard drop failed to acquire poisoned lock: {}. \
                     Compaction status may be inconsistent. Issue #18/#150",
                    e
                );
            }
        }
    }
}

/// Upper bound on candidates a **defaulted** refine pass may re-score
/// (`fetch_k * refine_factor`).
///
/// Refine cost scales with `k`: measured on SIFT-1M, `refine_factor = 20` costs
/// ~5% throughput at k=10 but 19% at k=100. Bounding the multiplier alone would
/// therefore get expensive exactly where result sets are large, so the *work* is
/// what is capped. An explicit `refine_factor` from the caller is honoured
/// verbatim and never clamped.
const REFINE_CANDIDATE_CAP: usize = 10_000;

impl StorageManager {
    /// Create a new StorageManager with a pre-configured backend.
    pub async fn new_with_backend(
        base_uri: &str,
        store: Arc<dyn ObjectStore>,
        backend: Arc<dyn StorageBackend>,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
    ) -> Result<Self> {
        let resilient_store: Arc<dyn ObjectStore> = Arc::new(ResilientObjectStore::new(
            store,
            config.object_store.clone(),
        ));

        let snapshot_manager = Arc::new(SnapshotManager::new(resilient_store.clone()));

        // Perform crash recovery for all known table patterns
        Self::recover_all_staging_tables(backend.as_ref(), &schema_manager).await?;

        let mut sm = Self {
            requires_adjacency_revalidation: false,
            base_uri: base_uri.to_string(),
            store: resilient_store,
            schema_manager,
            snapshot_manager,
            adjacency_manager: Arc::new(AdjacencyManager::new(config.cache_size)),
            config,
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: None,
            pinned_version_hwm: None,
            pin_guard: None,
            fork_scope: None,
            backend,
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                crate::storage::vid_labels::VidLabelsIndex::new(),
            )),
            plugin_registry: None,
        };

        // Rebuild VidLabelsIndex from persisted vertices. A failure leaves the
        // empty index in place; flush-time updates then repopulate it
        // incrementally, so reads degrade rather than break.
        if let Err(e) = sm.rebuild_vid_labels_index().await {
            warn!(
                "Failed to rebuild VidLabelsIndex on startup: {}. Falling back to storage queries.",
                e
            );
        }

        Ok(sm)
    }

    /// Create a new StorageManager with LanceDB integration.
    #[cfg(feature = "lance-backend")]
    pub async fn new(base_uri: &str, schema_manager: Arc<SchemaManager>) -> Result<Self> {
        Self::new_with_config(base_uri, schema_manager, UniConfig::default()).await
    }

    /// Create a new StorageManager with custom cache size.
    #[cfg(feature = "lance-backend")]
    pub async fn new_with_cache(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        adjacency_cache_size: usize,
    ) -> Result<Self> {
        let config = UniConfig {
            cache_size: adjacency_cache_size,
            ..Default::default()
        };
        Self::new_with_config(base_uri, schema_manager, config).await
    }

    /// Create a new StorageManager with custom configuration.
    #[cfg(feature = "lance-backend")]
    pub async fn new_with_config(
        base_uri: &str,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
    ) -> Result<Self> {
        let store = Self::build_store_from_uri(base_uri)?;
        Self::new_with_store_and_config(base_uri, store, schema_manager, config).await
    }

    /// Create a new StorageManager using an already-constructed object store.
    #[cfg(feature = "lance-backend")]
    pub async fn new_with_store_and_config(
        base_uri: &str,
        store: Arc<dyn ObjectStore>,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
    ) -> Result<Self> {
        Self::new_with_store_and_storage_options(base_uri, store, schema_manager, config, None)
            .await
    }

    /// Create a new StorageManager with LanceDB storage options.
    #[cfg(feature = "lance-backend")]
    pub async fn new_with_store_and_storage_options(
        base_uri: &str,
        store: Arc<dyn ObjectStore>,
        schema_manager: Arc<SchemaManager>,
        config: UniConfig,
        lancedb_storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let backend = Arc::new(LanceDbBackend::connect(base_uri, lancedb_storage_options).await?);
        Self::new_with_backend(base_uri, store, backend, schema_manager, config).await
    }

    /// Recover all staging tables for known table patterns.
    ///
    /// This runs on startup to handle crash recovery. It checks for staging tables
    /// for all vertex labels, adjacency tables, delta tables, and main tables.
    async fn recover_all_staging_tables(
        backend: &dyn StorageBackend,
        schema_manager: &SchemaManager,
    ) -> Result<()> {
        let schema = schema_manager.schema();

        // Recover main vertex and edge tables
        backend
            .recover_staging(table_names::main_vertex_table_name())
            .await?;
        backend
            .recover_staging(table_names::main_edge_table_name())
            .await?;

        // Recover per-label vertex tables
        for label in schema.labels.keys() {
            let name = table_names::vertex_table_name(label);
            backend.recover_staging(&name).await?;
        }

        // Recover adjacency and delta tables for each edge type and direction
        for edge_type in schema.edge_types.keys() {
            for direction in &["fwd", "bwd"] {
                // Recover delta tables
                let delta_name = table_names::delta_table_name(edge_type, direction);
                backend.recover_staging(&delta_name).await?;

                // Recover adjacency tables for each label
                for _label in schema.labels.keys() {
                    let adj_name = table_names::adjacency_table_name(edge_type, direction);
                    backend.recover_staging(&adj_name).await?;
                }
            }
        }

        Ok(())
    }

    #[cfg(feature = "lance-backend")]
    fn build_store_from_uri(base_uri: &str) -> Result<Arc<dyn ObjectStore>> {
        if base_uri.contains("://") {
            let parsed = url::Url::parse(base_uri).map_err(|e| anyhow!("Invalid base URI: {e}"))?;
            let (store, _path) = object_store::parse_url(&parsed)
                .map_err(|e| anyhow!("Failed to parse object store URL: {e}"))?;
            Ok(Arc::from(store))
        } else {
            // If local path, ensure it exists.
            std::fs::create_dir_all(base_uri)?;
            Ok(Arc::new(LocalFileSystem::new_with_prefix(base_uri)?))
        }
    }

    /// Filesystem root backing this manager's object store, when the store
    /// is a local filesystem (the non-`://` branch of
    /// `build_store_from_uri`). Used to fsync WAL segments after PUT —
    /// `object_store::LocalFileSystem` does not fsync on its own. `None`
    /// for remote/URL-based stores.
    pub fn local_fs_root(&self) -> Option<std::path::PathBuf> {
        if self.base_uri.contains("://") {
            None
        } else {
            Some(std::path::PathBuf::from(&self.base_uri))
        }
    }

    pub fn pinned(&self, snapshot: SnapshotManifest) -> Self {
        // Phase 4a: pinning a forked session is now supported. The
        // resulting StorageManager keeps `fork_scope` so reads continue
        // to route through the fork's Lance branches via `base_paths`,
        // and adds `pinned_snapshot` so writers / writers' read views
        // resolve at the snapshot's HWM. Writes are gated separately by
        // the session-level `is_pinned` check (`Session::tx` rejects
        // them via `UniError::ReadOnly`).
        Self {
            requires_adjacency_revalidation: self.requires_adjacency_revalidation,
            base_uri: self.base_uri.clone(),
            store: self.store.clone(),
            schema_manager: self.schema_manager.clone(),
            snapshot_manager: self.snapshot_manager.clone(),
            // Separate AdjacencyManager for snapshot isolation (Issue #73):
            // warm() will load only edges visible at the snapshot's HWM.
            // This prevents live DB's CSR (with all edges) from leaking into snapshots.
            adjacency_manager: Arc::new(AdjacencyManager::new(self.adjacency_manager.max_bytes())),
            config: self.config.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: Some(snapshot),
            pinned_version_hwm: None,
            pin_guard: None,
            fork_scope: self.fork_scope.clone(),
            backend: self.backend.clone(),
            // Deep-copy, not Arc-clone: a fork/pin must get its OWN label index
            // so its flushes/relabels don't mutate the parent's (review H1/L2),
            // mirroring the fresh `adjacency_manager` above. `VidLabelsIndex`
            // derives `Clone`; the snapshot is taken after flush-before-branch so
            // inherited labels (#99) are preserved.
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                self.vid_labels_index.read().clone(),
            )),
            plugin_registry: self.plugin_registry.clone(),
        }
    }

    /// Construct a clone of this `StorageManager` pinned to a row-version
    /// high-water mark (C2: transaction-level L1 pinning).
    ///
    /// Unlike [`Self::pinned`], this needs no `SnapshotManifest`: scans
    /// filter to `_version <= hwm` via [`Self::version_high_water_mark`].
    /// A read-write transaction builds one of these at begin with
    /// `SnapshotView.started_at_version`, so an L0→L1 flush completing
    /// mid-transaction cannot leak post-snapshot rows into its L1 scans
    /// (the L0 tier is pinned separately by the `SnapshotView`).
    ///
    /// Unlike [`Self::pinned`], the live `AdjacencyManager` is SHARED, not
    /// fresh: commits replay their edges into the live manager's overlay,
    /// which is the traversal path's only source for L0-resident edges — a
    /// fresh manager would make every unflushed edge invisible to the
    /// transaction. The cost is that the edge tier is not version-pinned
    /// (post-snapshot edges remain visible to traversals, exactly as before
    /// C2); edge reads are recorded in the OCC read-set, so a conflicting
    /// read-modify-write still aborts at commit.
    pub fn pinned_at_version(&self, hwm: u64) -> Self {
        // Register the pin for as long as this view lives, so shadow GC cannot
        // reclaim entries this reader still resolves. Dropped with the view.
        let pin_guard = Some(self.adjacency_manager.pinned_versions().pin(hwm));
        Self {
            requires_adjacency_revalidation: self.requires_adjacency_revalidation,
            base_uri: self.base_uri.clone(),
            store: self.store.clone(),
            schema_manager: self.schema_manager.clone(),
            snapshot_manager: self.snapshot_manager.clone(),
            adjacency_manager: self.adjacency_manager.clone(),
            config: self.config.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: None,
            pinned_version_hwm: Some(hwm),
            pin_guard,
            fork_scope: self.fork_scope.clone(),
            backend: self.backend.clone(),
            // Deep-copy, not Arc-clone: a fork/pin must get its OWN label index
            // so its flushes/relabels don't mutate the parent's (review H1/L2),
            // mirroring the fresh `adjacency_manager` above. `VidLabelsIndex`
            // derives `Clone`; the snapshot is taken after flush-before-branch so
            // inherited labels (#99) are preserved.
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                self.vid_labels_index.read().clone(),
            )),
            plugin_registry: self.plugin_registry.clone(),
        }
    }

    /// Construct a fork-scoped clone of this `StorageManager`.
    ///
    /// All reads through dataset factories *and* through `backend()`
    /// on the returned manager route through the fork's Lance branches
    /// via `base_paths`. The `AdjacencyManager` is fresh (per Issue
    /// #73 reasoning — same as `pinned`) to prevent primary's CSR from
    /// leaking into the fork. `fork_scope` and `pinned_snapshot` are
    /// mutually exclusive.
    ///
    /// The backend is wrapped in [`crate::backend::branched::BranchedBackend`]
    /// so that every `ScanRequest` constructed *anywhere* (PropertyManager,
    /// MainVertexDataset static methods, etc.) automatically picks up
    /// the fork's branch for tables the fork has branched. Untracked
    /// tables fall back to primary, matching Phase 1 read semantics.
    pub fn at_fork(&self, scope: Arc<crate::fork::ForkScope>) -> Self {
        self.at_fork_with_schema(scope, self.schema_manager.clone())
    }

    /// Variant of [`Self::at_fork`] that uses an explicit
    /// `merged_schema` for the fork's storage rather than primary's
    /// schema_manager. Used by `UniInner::at_fork` so that the
    /// fork-side strict-schema checks (in `uni-query` / `uni-store`'s
    /// writer) see fork-local labels and edge types added through
    /// `Session::fork_schema()`. Without this, those checks would
    /// route through primary's schema and reject fork-local labels.
    pub fn at_fork_with_schema(
        &self,
        scope: Arc<crate::fork::ForkScope>,
        merged_schema: Arc<SchemaManager>,
    ) -> Self {
        debug_assert!(
            self.pinned_snapshot.is_none(),
            "forking a pinned StorageManager is unsupported in Phase 1"
        );
        let branched_backend: Arc<dyn StorageBackend> = Arc::new(
            crate::backend::branched::BranchedBackend::new(self.backend.clone(), scope.clone()),
        );
        // Fork-scoped snapshot manager: a fork's flush publishes its manifest +
        // `latest` pointer under `catalog/forks/{fork_id}/`, never the primary's
        // global `catalog/latest` (review C1). Uses the raw object store, since
        // catalog metadata is not branched.
        let snapshot_manager = Arc::new(SnapshotManager::new_for_fork(
            self.store.clone(),
            scope.fork_id(),
        ));
        Self {
            requires_adjacency_revalidation: self.requires_adjacency_revalidation,
            base_uri: self.base_uri.clone(),
            store: self.store.clone(),
            schema_manager: merged_schema,
            snapshot_manager,
            adjacency_manager: Arc::new(AdjacencyManager::new(self.adjacency_manager.max_bytes())),
            config: self.config.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: None,
            pinned_version_hwm: None,
            pin_guard: None,
            fork_scope: Some(scope),
            backend: branched_backend,
            // Deep-copy, not Arc-clone: a fork/pin must get its OWN label index
            // so its flushes/relabels don't mutate the parent's (review H1/L2),
            // mirroring the fresh `adjacency_manager` above. `VidLabelsIndex`
            // derives `Clone`; the snapshot is taken after flush-before-branch so
            // inherited labels (#99) are preserved.
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                self.vid_labels_index.read().clone(),
            )),
            plugin_registry: self.plugin_registry.clone(),
        }
    }

    /// Borrow the plugin registry used for registry-dispatched CRDT merges.
    ///
    /// Returns `None` when no registry has been installed, in which case the
    /// durable merge paths fall back to native [`uni_crdt::Crdt::try_merge`].
    pub fn plugin_registry(&self) -> Option<&Arc<uni_plugin::PluginRegistry>> {
        self.plugin_registry.as_ref()
    }

    /// Install the plugin registry used for registry-dispatched CRDT merges.
    ///
    /// Called once at DB construction (before the manager is shared) so the
    /// same registry that backs `PropertyManager` also governs the compaction
    /// and L0-flush durable merge paths.
    pub fn set_plugin_registry(&mut self, registry: Arc<uni_plugin::PluginRegistry>) {
        self.plugin_registry = Some(registry);
    }

    /// Borrow the active fork scope, if any.
    pub fn fork_scope(&self) -> Option<&Arc<crate::fork::ForkScope>> {
        self.fork_scope.as_ref()
    }

    /// Phase 5a: query whether a fork-local index of `kind` exists
    /// for the `(label, column)` pair on the active fork scope.
    /// Returns `false` outside a fork or when no fork-local build of
    /// that kind has completed for the pair.
    ///
    /// The planner consults this to decide whether to emit a specific
    /// `FusedIndexScan` (returns `true`) or fall back to the inherited
    /// primary index via `base_paths` (returns `false`). A column can
    /// carry several kinds at once, so callers ask for the exact kind
    /// they intend to fuse. The lookup is a `DashMap::get` on
    /// `ForkScope` — O(1) and safe to call per query without caching
    /// above this layer.
    #[must_use]
    pub fn has_fork_index(
        &self,
        label: &str,
        column: &str,
        kind: crate::fork::ForkLocalIndexKind,
    ) -> bool {
        self.fork_scope
            .as_ref()
            .is_some_and(|s| s.has_fork_local_index(label, column, kind))
    }

    /// Base URI for this storage manager (the directory or remote
    /// prefix under which dataset directories live).
    pub fn base_uri(&self) -> &str {
        &self.base_uri
    }

    pub fn get_edge_version_by_id(&self, edge_type_id: u32) -> Option<u64> {
        let schema = self.schema_manager.schema();
        let name = schema.edge_type_name_by_id(edge_type_id)?;
        self.pinned_snapshot
            .as_ref()
            .and_then(|s| s.edges.get(name).map(|es| es.lance_version))
            // The flush path stamps `lance_version: 0` ("LanceDB tables don't
            // expose Lance version directly") — 0 is a stub sentinel, not a
            // real dataset version. Returning it would route adjacency reads
            // through `checkout_version(0)` (the empty initial version).
            .filter(|v| *v != 0)
    }

    /// Returns the version high-water mark from the pinned snapshot or the
    /// transaction-level version pin, if present.
    ///
    /// Used by the SCAN tier (vertex tables, property reads) to filter data
    /// by version: when set, only rows with
    /// `_version <= version_high_water_mark` are visible. The edge/adjacency
    /// path must use [`Self::snapshot_version_hwm`] instead.
    pub fn version_high_water_mark(&self) -> Option<u64> {
        self.pinned_snapshot
            .as_ref()
            .map(|s| s.version_high_water_mark)
            .or(self.pinned_version_hwm)
    }

    /// Version high-water mark from a manifest-pinned (time-travel) snapshot
    /// ONLY — never from a transaction-level version pin.
    ///
    /// The edge/adjacency read path switches to version-filtered CSR reads
    /// and skips the L0 overlays when a hwm is present. That is correct for
    /// time-travel (a snapshot is flushed state, with its own fresh
    /// `AdjacencyManager`), but a transaction pin shares the LIVE adjacency
    /// manager and needs live CSR + L0 overlays + its tx-L0 — filtering
    /// there would hide unflushed edges and poison the shared warm cache.
    /// The edge tier is deliberately not version-pinned for transactions
    /// (see [`Self::pinned_at_version`]).
    pub fn snapshot_version_hwm(&self) -> Option<u64> {
        self.pinned_snapshot
            .as_ref()
            .map(|s| s.version_high_water_mark)
    }

    /// Apply version filtering to a base filter expression.
    ///
    /// If a snapshot is pinned, conjoins `_version <= hwm` onto `base_filter`.
    /// Otherwise returns `base_filter` unchanged.
    pub fn apply_version_filter(&self, base_filter: FilterExpr) -> FilterExpr {
        match self.version_high_water_mark() {
            Some(hwm) => FilterExpr::all([base_filter, FilterExpr::version_at_most(hwm)]),
            None => base_filter,
        }
    }

    /// Build a filter expression that excludes soft-deleted rows and optionally
    /// includes a user-provided filter.
    ///
    /// The user filter stays [`FilterExpr::Raw`] — it is opaque text nothing in
    /// the engine parses, so only the visibility half can be structured.
    fn build_active_filter(user_filter: Option<&str>) -> FilterExpr {
        match user_filter {
            Some(expr) => {
                FilterExpr::all([FilterExpr::Raw(expr.to_string()), FilterExpr::not_deleted()])
            }
            None => FilterExpr::not_deleted(),
        }
    }

    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Get current compaction status.
    ///
    /// # Errors
    ///
    /// Returns error if the compaction status lock is poisoned (see issue #18/#150).
    pub fn compaction_status(
        &self,
    ) -> Result<CompactionStatus, uni_common::sync::LockPoisonedError> {
        let guard = uni_common::sync::acquire_mutex(&self.compaction_status, "compaction_status")?;
        Ok(guard.clone())
    }

    /// Optimize one table if it exists, folding what it did into `stats`.
    ///
    /// A table that does not exist is not counted: `tables_optimized` means
    /// "visited", and reporting a table that was never there is exactly the
    /// defect this replaced — `compact_label` used to return
    /// `files_compacted: 1` for a label with no table at all (#172).
    async fn optimize_into(&self, table: &str, stats: &mut CompactionStats) -> Result<()> {
        if self.backend.table_exists(table).await? {
            let report = self
                .backend
                .optimize_table(table, self.config.compaction.version_retention)
                .await?;
            stats.absorb(&report);
            self.backend.invalidate_cache(table);
        }
        Ok(())
    }

    /// Every table the schema owns: vertex tables, per-edge-type delta and
    /// adjacency tables in both directions, and the two main tables.
    ///
    /// Shared by `compact` and `execute_compaction` so the two cannot drift
    /// apart. They had: `compact` walked only vertex tables, so
    /// `uni.admin.compact()` reported success having never touched an edge.
    fn all_tables(schema: &uni_common::core::schema::Schema) -> Vec<String> {
        let mut tables: Vec<String> = schema
            .labels
            .keys()
            .map(|l| table_names::vertex_table_name(l))
            .collect();
        for edge_type in schema.edge_types.keys() {
            for dir in ["fwd", "bwd"] {
                tables.push(table_names::delta_table_name(edge_type, dir));
                tables.push(table_names::adjacency_table_name(edge_type, dir));
            }
        }
        tables.push(table_names::main_vertex_table_name().to_string());
        tables.push(table_names::main_edge_table_name().to_string());
        tables
    }

    pub async fn compact(&self) -> Result<CompactionStats> {
        let start = std::time::Instant::now();
        let schema = self.schema_manager.schema();
        let mut stats = CompactionStats::default();

        for table in Self::all_tables(&schema) {
            self.optimize_into(&table, &mut stats).await?;
        }

        stats.duration = start.elapsed();
        self.record_run(&stats)?;
        Ok(stats)
    }

    pub async fn compact_label(&self, label: &str) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let start = std::time::Instant::now();
        let mut stats = CompactionStats::default();
        self.optimize_into(&table_names::vertex_table_name(label), &mut stats)
            .await?;

        stats.duration = start.elapsed();
        self.record_run(&stats)?;
        Ok(stats)
    }

    pub async fn compact_edge_type(&self, edge_type: &str) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(self.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let start = std::time::Instant::now();
        let mut stats = CompactionStats::default();

        for dir in ["fwd", "bwd"] {
            self.optimize_into(&table_names::delta_table_name(edge_type, dir), &mut stats)
                .await?;
            self.optimize_into(
                &table_names::adjacency_table_name(edge_type, dir),
                &mut stats,
            )
            .await?;
        }

        stats.duration = start.elapsed();
        self.record_run(&stats)?;
        Ok(stats)
    }

    /// Fold a finished run into the lifetime counters.
    ///
    /// Called by every entry point. `total_compactions` used to move only on the
    /// background path, so a user driving compaction by hand saw it stay at
    /// zero forever.
    fn record_run(&self, stats: &CompactionStats) -> Result<()> {
        let mut status = acquire_mutex(&self.compaction_status, "compaction_status")?;
        status.total_compactions += 1;
        status.total_bytes_reclaimed += stats.bytes_reclaimed;
        Ok(())
    }

    pub async fn wait_for_compaction(&self) -> Result<()> {
        loop {
            let in_progress = {
                acquire_mutex(&self.compaction_status, "compaction_status")?.compaction_in_progress
            };
            if !in_progress {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    pub fn start_background_compaction(
        self: Arc<Self>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        if !self.config.compaction.enabled {
            return tokio::spawn(async {});
        }

        tokio::spawn(async move {
            // Use interval_at to delay the first tick. tokio::time::interval fires
            // immediately on the first tick, which can race with queries that run
            // right after database open. Delaying by the check_interval gives
            // initial queries time to complete before compaction modifies tables
            // (optimize(All) can GC index files that concurrent queries depend on).
            let start = tokio::time::Instant::now() + self.config.compaction.check_interval;
            let mut interval =
                tokio::time::interval_at(start, self.config.compaction.check_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = self.update_compaction_status().await {
                            log::error!("Failed to update compaction status: {}", e);
                            continue;
                        }

                        if let Some(task) = self.pick_compaction_task() {
                            log::info!("Triggering background compaction: {:?}", task);
                            if let Err(e) = Self::execute_compaction(Arc::clone(&self), task).await {
                                log::error!("Compaction failed: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::info!("Background compaction shutting down");
                        let _ = self.wait_for_compaction().await;
                        break;
                    }
                }
            }
        })
    }

    async fn update_compaction_status(&self) -> Result<()> {
        let schema = self.schema_manager.schema();
        let backend = self.backend.as_ref();
        let mut total_rows: usize = 0;
        let mut oldest_ts: Option<i64> = None;

        for name in schema.edge_types.keys() {
            for dir in ["fwd", "bwd"] {
                let tbl_name = table_names::delta_table_name(name, dir);
                if !backend.table_exists(&tbl_name).await? {
                    continue;
                }
                let row_count = backend.count_rows(&tbl_name, None).await.unwrap_or(0);
                if row_count == 0 {
                    continue;
                }
                total_rows += row_count;

                // Query oldest _created_at for age tracking
                let request =
                    ScanRequest::all(&tbl_name).with_columns(vec!["_created_at".to_string()]);
                let Ok(batches) = backend.scan(request).await else {
                    continue;
                };
                for batch in batches {
                    let Some(col) = batch
                        .column_by_name("_created_at")
                        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                    else {
                        continue;
                    };
                    for i in 0..col.len() {
                        if !col.is_null(i) {
                            let ts = col.value(i);
                            oldest_ts = Some(oldest_ts.map_or(ts, |prev| prev.min(ts)));
                        }
                    }
                }
            }
        }

        let oldest_l1_age = oldest_ts
            .and_then(|ts| {
                let created = UNIX_EPOCH + Duration::from_nanos(ts as u64);
                SystemTime::now().duration_since(created).ok()
            })
            .unwrap_or(Duration::ZERO);

        let mut status = acquire_mutex(&self.compaction_status, "compaction_status")?;
        // Note: l1_runs is managed by flush_to_l1 (increment) and execute_compaction
        // (reset). It counts flush generations, not delta table count.
        status.l1_rows = total_rows as u64;
        status.l1_estimated_bytes = status.l1_rows.saturating_mul(ENTRY_SIZE_ESTIMATE as u64);
        status.status_refreshes += 1;
        status.oldest_l1_age = oldest_l1_age;
        Ok(())
    }

    fn pick_compaction_task(&self) -> Option<CompactionTask> {
        let status = acquire_mutex(&self.compaction_status, "compaction_status").ok()?;

        if status.l1_runs >= self.config.compaction.max_l1_runs {
            return Some(CompactionTask::ByRunCount);
        }
        if status.l1_estimated_bytes >= self.config.compaction.max_l1_size_bytes {
            return Some(CompactionTask::BySize);
        }
        if status.oldest_l1_age >= self.config.compaction.max_l1_age
            && status.oldest_l1_age > Duration::ZERO
        {
            return Some(CompactionTask::ByAge);
        }

        None
    }

    /// Optimize a table via the backend, returning `true` on success.
    /// Optimize one table, downgrading a failure to a warning.
    ///
    /// `None` means the optimize did not run; `Some(report)` means it ran, and
    /// an all-zero report inside means it ran and found nothing to merge. The
    /// two are kept apart deliberately — collapsing them is how
    /// `files_compacted` came to count "calls that did not error" (#172).
    async fn try_optimize_table(
        backend: &dyn StorageBackend,
        table_name: &str,
        version_retention: std::time::Duration,
    ) -> Option<OptimizeReport> {
        match backend.optimize_table(table_name, version_retention).await {
            Ok(report) => Some(report),
            Err(e) => {
                log::warn!("Failed to optimize table {}: {}", table_name, e);
                None
            }
        }
    }

    /// Trigger L1 compaction asynchronously without blocking the caller.
    /// Safe to call frequently — CompactionGuard prevents concurrent runs.
    pub fn trigger_async_compaction(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = Self::execute_compaction(this, CompactionTask::ByRunCount).await {
                // "Compaction already in progress" is expected when called frequently
                log::debug!("Post-flush compaction skipped: {}", e);
            }
        });
    }

    pub(crate) async fn execute_compaction(
        this: Arc<Self>,
        _task: CompactionTask,
    ) -> Result<CompactionStats> {
        let start = std::time::Instant::now();
        let _guard = CompactionGuard::new(this.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;

        let schema = this.schema_manager.schema();
        let mut stats = CompactionStats::default();

        // ── Tier 2: Semantic compaction ──
        // Dedup vertices, merge CRDTs, consolidate L1→L2 deltas, clean tombstones
        let compactor = Compactor::new(Arc::clone(&this));
        // On failure the default report leaves `semantic_passes` at zero, which
        // correctly reads as "no CRDT merge count was measured" rather than
        // "no CRDT merges happened".
        let semantic = compactor.compact_all().await.unwrap_or_else(|e| {
            log::error!(
                "Semantic compaction failed (continuing with backend optimize): {}",
                e
            );
            SemanticCompactionReport::default()
        });
        stats.semantic_passes = semantic.vertex_passes;
        stats.crdt_merges = semantic.crdt_merges;

        // Re-warm adjacency CSR after semantic compaction
        let am = this.adjacency_manager();
        for info in &semantic.adjacency {
            let direction = match info.direction.as_str() {
                "fwd" => Direction::Outgoing,
                "bwd" => Direction::Incoming,
                _ => continue,
            };
            if let Some(etid) = schema.edge_type_id_unified_case_insensitive(&info.edge_type)
                && let Err(e) = am.warm(&this, etid, direction, None).await
            {
                log::warn!(
                    "Failed to re-warm adjacency for {}/{}: {}",
                    info.edge_type,
                    info.direction,
                    e
                );
            }
        }

        // ── Tier 3: Backend optimize ──
        // Same table set as `compact`, via the shared walk, so the two cannot
        // drift apart again.
        let backend = this.backend.as_ref();
        for table in Self::all_tables(&schema) {
            if let Some(report) =
                Self::try_optimize_table(backend, &table, this.config.compaction.version_retention)
                    .await
            {
                stats.absorb(&report);
                backend.invalidate_cache(&table);
            }
        }

        stats.duration = start.elapsed();
        this.record_run(&stats)?;
        {
            let mut status = acquire_mutex(&this.compaction_status, "compaction_status")?;
            status.l1_runs = 0; // Reset flush generation counter
        }

        Ok(stats)
    }

    /// Open a LanceDB table for a label.
    ///
    /// Invalidate cached table state (call after writes).
    pub fn invalidate_table_cache(&self, label: &str) {
        let name = table_names::vertex_table_name(label);
        self.backend.invalidate_cache(&name);
    }

    pub fn base_path(&self) -> &str {
        &self.base_uri
    }

    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    pub fn schema_manager_arc(&self) -> Arc<SchemaManager> {
        self.schema_manager.clone()
    }

    /// Returns the backing `Arc<SchemaManager>` by reference.
    ///
    /// Unlike [`Self::schema_manager`] (which derefs to `&SchemaManager`),
    /// this preserves the `Arc`'s pointer identity. A pinned transaction and
    /// the live session clone the *same* `schema_manager` `Arc`, while forks
    /// hold a distinct one — so this is the correct registry key for the
    /// projection store (see `uni-query`'s `projection_store::for_storage`).
    #[must_use]
    pub fn schema_manager_arc_ref(&self) -> &Arc<SchemaManager> {
        &self.schema_manager
    }

    /// Get the adjacency manager for the dual-CSR architecture.
    pub fn adjacency_manager(&self) -> Arc<AdjacencyManager> {
        Arc::clone(&self.adjacency_manager)
    }

    /// Warm the adjacency manager for a specific edge type and direction.
    ///
    /// Builds the Main CSR from L2 adjacency + L1 delta data in storage.
    /// Called lazily on first access per edge type or at startup.
    pub async fn warm_adjacency(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        self.adjacency_manager
            .warm(self, edge_type_id, direction, version)
            .await
    }

    /// Coalesced warm_adjacency() to prevent cache stampede (Issue #13).
    ///
    /// Uses double-checked locking to ensure only one concurrent warm() per
    /// (edge_type, direction) key. Subsequent callers wait for the first to complete.
    pub async fn warm_adjacency_coalesced(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
        version: Option<u64>,
    ) -> anyhow::Result<()> {
        self.adjacency_manager
            .warm_coalesced(self, edge_type_id, direction, version)
            .await
    }

    /// Marks this handle as one that does not own the database's writer.
    ///
    /// The adjacency CSR is complete-by-construction only for the handle whose
    /// writer produced the edges — its commits land in the manager's
    /// `active_overlay` directly. A handle without a writer receives no such
    /// updates, so it must revalidate the CSR against its source tables before
    /// each traversal (issue #168). Revalidation costs a Lance manifest read
    /// per source table per query — measured at ~0.9ms on a small traversal —
    /// so it is confined to the handles that actually need it.
    pub fn set_requires_adjacency_revalidation(&mut self, yes: bool) {
        self.requires_adjacency_revalidation = yes;
    }

    /// Whether this handle must revalidate the adjacency CSR before reads.
    #[must_use]
    pub fn requires_adjacency_revalidation(&self) -> bool {
        self.requires_adjacency_revalidation
    }

    /// Whether the cached adjacency CSR still matches its source tables.
    ///
    /// See `AdjacencyManager::csr_source_stamp` — a presence-only gate pinned a
    /// second, writer-less handle to the edge set it first traversed (#168).
    pub async fn adjacency_csr_is_fresh(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
    ) -> bool {
        self.adjacency_manager
            .csr_is_fresh(self, edge_type_id, direction)
            .await
    }

    /// Drops the cached adjacency CSR for this edge type and direction.
    pub fn invalidate_adjacency_csr(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
    ) {
        self.adjacency_manager
            .invalidate_csr(edge_type_id, direction);
    }

    /// Check whether the adjacency manager has a CSR for the given edge type and direction.
    pub fn has_adjacency_csr(
        &self,
        edge_type_id: u32,
        direction: crate::storage::direction::Direction,
    ) -> bool {
        self.adjacency_manager.has_csr(edge_type_id, direction)
    }

    /// Get neighbors at a specific version for snapshot queries.
    pub fn get_neighbors_at_version(
        &self,
        vid: uni_common::core::id::Vid,
        edge_type: u32,
        direction: crate::storage::direction::Direction,
        version: u64,
    ) -> Vec<(uni_common::core::id::Vid, uni_common::core::id::Eid)> {
        self.adjacency_manager
            .get_neighbors_at_version(vid, edge_type, direction, version)
    }

    /// Get the storage backend.
    pub fn backend(&self) -> &dyn StorageBackend {
        self.backend.as_ref()
    }

    /// Get the storage backend as an Arc.
    pub fn backend_arc(&self) -> Arc<dyn StorageBackend> {
        self.backend.clone()
    }

    /// Rebuild the VidLabelsIndex from the main vertex table.
    ///
    /// Always called on startup. On a fresh database (no vertex table yet) the
    /// index is left empty and filled incrementally by flush-time updates.
    async fn rebuild_vid_labels_index(&mut self) -> Result<()> {
        use crate::storage::vid_labels::VidLabelsIndex;

        let backend = self.backend.as_ref();
        let vtable = table_names::main_vertex_table_name();

        // Check if the table exists (fresh database)
        if !backend.table_exists(vtable).await? {
            self.vid_labels_index = Arc::new(parking_lot::RwLock::new(VidLabelsIndex::new()));
            return Ok(());
        }

        // Scan all non-deleted vertices and collect (VID, labels)
        let request = ScanRequest::all(vtable)
            .with_filter(FilterExpr::not_deleted())
            .with_limit(100_000);
        let batches = backend
            .scan(request)
            .await
            .map_err(|e| anyhow!("Failed to query main vertex table: {}", e))?;

        let mut index = VidLabelsIndex::new();
        for batch in batches {
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or_else(|| anyhow!("Missing _vid column"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("Invalid _vid column type"))?;

            let labels_col = batch
                .column_by_name("labels")
                .ok_or_else(|| anyhow!("Missing labels column"))?
                .as_any()
                .downcast_ref::<arrow_array::ListArray>()
                .ok_or_else(|| anyhow!("Invalid labels column type"))?;

            for row_idx in 0..batch.num_rows() {
                let vid = Vid::from(vid_col.value(row_idx));
                let labels_array = labels_col.value(row_idx);
                let labels_str_array = labels_array
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| anyhow!("Invalid labels array element type"))?;

                let labels: Vec<String> = (0..labels_str_array.len())
                    .map(|i| labels_str_array.value(i).to_string())
                    .collect();

                index.insert(vid, labels);
            }
        }

        self.vid_labels_index = Arc::new(parking_lot::RwLock::new(index));
        Ok(())
    }

    /// Get labels for a VID from the in-memory index.
    ///
    /// Returns `None` only when the VID is absent from the index (e.g. it was
    /// never persisted, or has been deleted).
    pub fn get_labels_from_index(&self, vid: Vid) -> Option<Vec<String>> {
        let index = self.vid_labels_index.read();
        index.get_labels(vid).map(|labels| labels.to_vec())
    }

    /// Distinct vertex label names physically present in flushed storage (the
    /// VID→labels index, which resolves labels for vertices aged out of L0 into
    /// Lance). The GraphCompute projection compares this against the declared
    /// schema to detect present-but-undeclared (schemaless) labels a whole-graph
    /// projection would otherwise silently omit (G11). Unflushed labels still in
    /// L0 are enumerated separately by the caller.
    pub fn physical_vertex_label_names(&self) -> Vec<String> {
        self.vid_labels_index
            .read()
            .labels()
            .map(str::to_owned)
            .collect()
    }

    /// Update the VID-to-labels mapping in the index.
    pub fn update_vid_labels_index(&self, vid: Vid, labels: Vec<String>) {
        let mut index = self.vid_labels_index.write();
        index.insert(vid, labels);
    }

    /// Remove a VID from the labels index.
    pub fn remove_from_vid_labels_index(&self, vid: Vid) {
        let mut index = self.vid_labels_index.write();
        index.remove_vid(vid);
    }

    pub async fn load_subgraph_cached(
        &self,
        start_vids: &[Vid],
        edge_types: &[u32],
        max_hops: usize,
        direction: GraphDirection,
        _l0: Option<Arc<RwLock<L0Buffer>>>,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new();

        let dir = match direction {
            GraphDirection::Outgoing => crate::storage::direction::Direction::Outgoing,
            GraphDirection::Incoming => crate::storage::direction::Direction::Incoming,
        };

        let neighbor_is_dst = matches!(direction, GraphDirection::Outgoing);

        // Initialize frontier
        let mut frontier: Vec<Vid> = start_vids.to_vec();
        let mut visited: HashSet<Vid> = HashSet::new();

        // Initialize start vids
        for &vid in start_vids {
            graph.add_vertex(vid);
        }

        for _hop in 0..max_hops {
            let mut next_frontier = HashSet::new();

            for &vid in &frontier {
                if visited.contains(&vid) {
                    continue;
                }
                visited.insert(vid);
                graph.add_vertex(vid);

                for &etype_id in edge_types {
                    // Warm adjacency with coalescing to prevent cache stampede (Issue #13).
                    // Manifest pin only: a tx version pin shares the LIVE adjacency
                    // manager — warming it filtered would poison the shared cache.
                    let edge_ver = self.snapshot_version_hwm();
                    self.adjacency_manager
                        .warm_coalesced(self, etype_id, dir, edge_ver)
                        .await?;

                    // Get neighbors from AdjacencyManager (Main CSR + overlay)
                    let edges = self.adjacency_manager.get_neighbors(vid, etype_id, dir);

                    for (neighbor_vid, eid) in edges {
                        graph.add_vertex(neighbor_vid);
                        if !visited.contains(&neighbor_vid) {
                            next_frontier.insert(neighbor_vid);
                        }

                        if neighbor_is_dst {
                            graph.add_edge(vid, neighbor_vid, eid, etype_id);
                        } else {
                            graph.add_edge(neighbor_vid, vid, eid, etype_id);
                        }
                    }
                }
            }
            frontier = next_frontier.into_iter().collect();

            // Early termination: if frontier is empty, no more vertices to explore
            if frontier.is_empty() {
                break;
            }
        }

        Ok(graph)
    }

    pub fn snapshot_manager(&self) -> &SnapshotManager {
        &self.snapshot_manager
    }

    pub fn index_manager(&self) -> IndexManager {
        IndexManager::new(&self.base_uri, self.schema_manager.clone())
            .with_backend(self.backend_arc())
    }

    // ========================================================================
    // Domain-level scan methods — encapsulate LanceDB queries for consumers
    // ========================================================================

    /// Scan a per-label vertex table. Returns `None` if the table doesn't exist.
    ///
    /// Internally opens the table, filters requested columns to those that
    /// physically exist, and applies the version HWM filter for snapshot isolation.
    pub async fn scan_vertex_table(
        &self,
        label: &str,
        columns: &[&str],
        additional_filter: Option<&FilterExpr>,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        self.scan_vertex_table_counted(label, columns, additional_filter, None)
            .await
    }

    /// [`Self::scan_vertex_table`], carrying a query's counters into the backend.
    ///
    /// The backend layer never sees a [`QueryContext`], so `ScanRequest` is the
    /// only channel by which a count taken at the point a branch scan *executes*
    /// (`LanceDbBackend::execute_scan_stream`) can be attributed to the query
    /// that caused it. Callers on a query path pass
    /// `graph_ctx.counters()`; everything else passes `None` and is not counted.
    pub async fn scan_vertex_table_counted(
        &self,
        label: &str,
        columns: &[&str],
        additional_filter: Option<&FilterExpr>,
        counters: Option<&Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        let backend = self.backend();
        let table_name = table_names::vertex_table_name(label);

        if !backend.table_exists(&table_name).await? {
            return Ok(None);
        }

        // Filter columns to those that exist in the table
        let Some(actual_columns) = existing_columns(backend, &table_name, columns).await? else {
            return Ok(None);
        };

        // Build filter with version HWM + optional additional filter
        let hwm = self.version_high_water_mark();
        // Counted at the point the ceiling is actually applied to a scan, not
        // from `pinned_snapshot.is_some()` — which would be a config read.
        //
        // Deliberately `snapshot_version_hwm`, the manifest-only pin, and not
        // `version_high_water_mark`: the latter is also `Some` for an ordinary
        // read-write transaction's version pin (`pinned_version_hwm`), so
        // counting off it would fire on every transactional read and make the
        // pinned-vs-live witness meaningless. The *filter* still uses the wider
        // value; only the count narrows.
        if self.snapshot_version_hwm().is_some()
            && let Some(c) = counters
        {
            c.add_snapshot_read();
        }
        let filter = combine_hwm_filter(hwm, additional_filter);

        let mut request = ScanRequest::all(&table_name)
            .with_columns(actual_columns)
            .with_counters(counters.cloned());
        if let Some(f) = filter {
            request = request.with_filter(f);
        }

        scan_concat(backend, request).await
    }

    /// Scan a delta table for an edge type + direction.
    /// Returns `None` if the table doesn't exist.
    pub async fn scan_delta_table(
        &self,
        edge_type: &str,
        direction: &str,
        columns: &[&str],
        additional_filter: Option<&FilterExpr>,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        // Edge path: manifest pin only. A transaction version pin must NOT
        // version-filter edge reads — the edge tier is not version-pinned
        // (the live AdjacencyManager + tx-L0 overlay carry unflushed and
        // in-transaction edges), so filtering here would hide a relationship
        // the same transaction just created (MERGE read-your-writes).
        let edge_hwm = self.snapshot_version_hwm();
        let backend = self.backend();
        let table_name = table_names::delta_table_name(edge_type, direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(None);
        }

        // Filter columns to those that exist
        let Some(actual_columns) = existing_columns(backend, &table_name, columns).await? else {
            return Ok(None);
        };

        let filter = combine_hwm_filter(edge_hwm, additional_filter);

        let mut request = ScanRequest::all(&table_name).with_columns(actual_columns);
        if let Some(f) = filter {
            request = request.with_filter(f);
        }

        scan_concat(backend, request).await
    }

    /// Scan the unified main vertex table. Returns `None` if table doesn't exist.
    ///
    /// Applies version HWM filter internally for snapshot isolation, combined
    /// with any caller-provided filter (label conditions, etc.).
    pub async fn scan_main_vertex_table(
        &self,
        columns: &[&str],
        filter: Option<&FilterExpr>,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        let backend = self.backend();
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        // Combine caller filter with version HWM for snapshot isolation
        let full_filter = combine_hwm_filter(self.version_high_water_mark(), filter);

        let request = ScanRequest::all(table_name)
            .with_columns(columns.iter().map(|s| s.to_string()).collect());
        let request = match full_filter {
            Some(f) => request.with_filter(f),
            None => request,
        };

        scan_concat(backend, request).await
    }

    /// Scan the main edge table as a stream. Returns `None` if table doesn't exist.
    pub async fn scan_main_edge_table_stream(
        &self,
        filter: Option<&FilterExpr>,
    ) -> Result<
        Option<
            std::pin::Pin<Box<dyn futures::Stream<Item = Result<arrow_array::RecordBatch>> + Send>>,
        >,
    > {
        let backend = self.backend();
        let table_name = table_names::main_edge_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        let mut request = ScanRequest::all(table_name);
        if let Some(f) = filter {
            request = request.with_filter(f.clone());
        }

        let stream = backend.scan_stream(request).await?;
        Ok(Some(stream))
    }

    /// Scan a per-label vertex table as a stream. Returns `None` if table doesn't exist.
    pub async fn scan_vertex_table_stream(
        &self,
        label: &str,
    ) -> Result<
        Option<
            std::pin::Pin<Box<dyn futures::Stream<Item = Result<arrow_array::RecordBatch>> + Send>>,
        >,
    > {
        let backend = self.backend();
        let table_name = table_names::vertex_table_name(label);

        if !backend.table_exists(&table_name).await? {
            return Ok(None);
        }

        let stream = backend.scan_stream(ScanRequest::all(&table_name)).await?;
        Ok(Some(stream))
    }

    /// Find a vertex VID by external ID. Uses pinned snapshot HWM if present.
    pub async fn find_vertex_by_ext_id(&self, ext_id: &str) -> Result<Option<Vid>> {
        MainVertexDataset::find_by_ext_id(self.backend(), ext_id, self.version_high_water_mark())
            .await
    }

    /// Map every live vertex that has an external id to its `ext_id`
    /// (`_vid` → `ext_id`).
    ///
    /// `ext_id` is folded into a vertex's content `_uid` but is stripped from
    /// query results, so the fork diff/promote engine can't recover it by
    /// re-hashing query rows — two vertices differing only by `ext_id` would
    /// collapse to one identity (review H4). This exposes the stored `ext_id`
    /// so the diff can fold it back into its recomputed UID. Reads through the
    /// (branched) backend, so a forked manager sees its own + inherited rows.
    /// Covers flushed (Lance) rows; `ext_id` is immutable so no version
    /// reconciliation is needed.
    pub async fn get_vertex_ext_ids(&self) -> Result<std::collections::HashMap<Vid, String>> {
        use arrow_array::StringArray;
        let backend = self.backend.as_ref();
        let vtable = table_names::main_vertex_table_name();
        let mut out = std::collections::HashMap::new();
        if !backend.table_exists(vtable).await? {
            return Ok(out);
        }
        let request = ScanRequest::all(vtable)
            .with_filter(FilterExpr::not_deleted())
            .with_columns(vec!["_vid".to_string(), "ext_id".to_string()]);
        let batches = backend
            .scan(request)
            .await
            .map_err(|e| anyhow!("get_vertex_ext_ids: {}", e))?;
        for batch in batches {
            let vids = batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow!("get_vertex_ext_ids: missing/invalid _vid column"))?;
            let exts = batch
                .column_by_name("ext_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| anyhow!("get_vertex_ext_ids: missing/invalid ext_id column"))?;
            for i in 0..batch.num_rows() {
                if exts.is_null(i) {
                    continue;
                }
                let ext = exts.value(i);
                if !ext.is_empty() {
                    out.insert(Vid::from(vids.value(i)), ext.to_string());
                }
            }
        }
        Ok(out)
    }

    /// Find labels for a vertex by VID. Uses pinned snapshot HWM if present.
    pub async fn find_vertex_labels_by_vid(&self, vid: Vid) -> Result<Option<Vec<String>>> {
        MainVertexDataset::find_labels_by_vid(self.backend(), vid, self.version_high_water_mark())
            .await
    }

    /// Find edges from the main edge table by type names, optionally pushing
    /// a bounded endpoint vid set into the scan (review perf #5).
    pub async fn find_edges_by_type_names(
        &self,
        type_names: &[&str],
        endpoint_filter: Option<(crate::storage::main_edge::EndpointSide, &[Vid])>,
    ) -> Result<Vec<(Eid, Vid, Vid, String, uni_common::Properties)>> {
        MainEdgeDataset::find_edges_by_type_names(self.backend(), type_names, endpoint_filter).await
    }

    /// Scan vertex candidates matching a filter. Returns VIDs where `_deleted = false`.
    pub async fn scan_vertex_candidates(
        &self,
        label: &str,
        filter: Option<&FilterExpr>,
    ) -> Result<Vec<Vid>> {
        let backend = self.backend();
        let table_name = table_names::vertex_table_name(label);

        if !backend.table_exists(&table_name).await? {
            return Ok(Vec::new());
        }

        let full_filter = match filter {
            Some(f) => FilterExpr::all([FilterExpr::not_deleted(), f.clone()]),
            None => FilterExpr::not_deleted(),
        };

        let request = ScanRequest::all(&table_name)
            .with_filter(full_filter)
            .with_columns(vec!["_vid".to_string()]);

        let batches = backend.scan(request).await?;

        let mut vids = Vec::new();
        for batch in batches {
            let vid_col = batch
                .column_by_name("_vid")
                .ok_or(anyhow!("Missing _vid"))?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or(anyhow!("Invalid _vid"))?;
            for i in 0..batch.num_rows() {
                vids.push(Vid::from(vid_col.value(i)));
            }
        }
        Ok(vids)
    }

    /// Construct a [`VertexDataset`] batch-builder for `label`.
    ///
    /// `VertexDataset` no longer opens on-disk data, so there is nothing to
    /// branch here — fork-scoped reads of vertex data go through the
    /// (branch-aware) `StorageBackend`.
    pub fn vertex_dataset(&self, label: &str) -> Result<VertexDataset> {
        let schema = self.schema_manager.schema();
        let label_meta = schema
            .labels
            .get(label)
            .ok_or_else(|| anyhow!("Label '{}' not found", label))?;
        Ok(VertexDataset::new(&self.base_uri, label, label_meta.id))
    }

    #[cfg(feature = "lance-backend")]
    pub fn edge_dataset(
        &self,
        edge_type: &str,
        src_label: &str,
        dst_label: &str,
    ) -> Result<EdgeDataset> {
        let key = format!("edges_{edge_type}");
        match self.fork_branch_for(&key) {
            Some(branch) => Ok(EdgeDataset::new_branched(
                &self.base_uri,
                edge_type,
                src_label,
                dst_label,
                branch,
            )),
            None => Ok(EdgeDataset::new(
                &self.base_uri,
                edge_type,
                src_label,
                dst_label,
            )),
        }
    }

    pub fn delta_dataset(&self, edge_type: &str, direction: &str) -> Result<DeltaDataset> {
        let key = format!("deltas_{edge_type}_{direction}");
        match self.fork_branch_for(&key) {
            Some(branch) => Ok(DeltaDataset::new_branched(
                &self.base_uri,
                edge_type,
                direction,
                branch,
            )),
            None => Ok(DeltaDataset::new(&self.base_uri, edge_type, direction)),
        }
    }

    pub fn adjacency_dataset(
        &self,
        edge_type: &str,
        label: &str,
        direction: &str,
    ) -> Result<AdjacencyDataset> {
        // The fork registers adjacency branches under the canonical table
        // name (`adjacency_{edge_type}_{direction}`), so the lookup key must
        // match it — not the historical `adjacency_{direction}_{edge_type}_
        // {label}`, which never resolved a branch. Adjacency is per-`(edge_
        // type, direction)`, not per-label. The canonical form is pinned by
        // `table_names::tests::adjacency_table_name_is_canonical`. (L8)
        let key = crate::backend::table_names::adjacency_table_name(edge_type, direction);
        match self.fork_branch_for(&key) {
            Some(branch) => Ok(AdjacencyDataset::new_branched(
                &self.base_uri,
                edge_type,
                label,
                direction,
                branch,
            )),
            None => Ok(AdjacencyDataset::new(
                &self.base_uri,
                edge_type,
                label,
                direction,
            )),
        }
    }

    /// Look up the branch name for a dataset under the active fork
    /// scope, if any. Returns `None` when not forked, or when the fork
    /// hasn't recorded a branch on this dataset yet (Phase 2 territory).
    fn fork_branch_for(&self, dataset_name: &str) -> Option<String> {
        self.fork_scope
            .as_ref()
            .and_then(|s| s.branch_for(dataset_name))
    }

    /// Get the main vertex dataset for unified vertex storage.
    ///
    /// The main vertex dataset contains all vertices regardless of label,
    /// enabling fast ID-based lookups without knowing the label.
    pub fn main_vertex_dataset(&self) -> MainVertexDataset {
        MainVertexDataset::new(&self.base_uri)
    }

    /// Get the main edge dataset for unified edge storage.
    ///
    /// The main edge dataset contains all edges regardless of type,
    /// enabling fast ID-based lookups without knowing the edge type.
    pub fn main_edge_dataset(&self) -> MainEdgeDataset {
        MainEdgeDataset::new(&self.base_uri)
    }

    #[cfg(feature = "lance-backend")]
    pub fn uid_index(&self, label: &str) -> Result<UidIndex> {
        Ok(UidIndex::new(&self.base_uri, label))
    }

    #[cfg(feature = "lance-backend")]
    pub async fn inverted_index(&self, label: &str, property: &str) -> Result<InvertedIndex> {
        let schema = self.schema_manager.schema();
        let config = schema
            .indexes
            .iter()
            .find_map(|idx| match idx {
                IndexDefinition::Inverted(cfg)
                    if cfg.label == label && cfg.property == property =>
                {
                    Some(cfg.clone())
                }
                _ => None,
            })
            .ok_or_else(|| anyhow!("Inverted index not found for {}.{}", label, property))?;

        InvertedIndex::new(&self.base_uri, config).await
    }

    #[expect(clippy::too_many_arguments)]
    pub async fn vector_search(
        &self,
        label: &str,
        property: &str,
        query: &[f32],
        k: usize,
        filter: Option<&str>,
        opts: VectorQueryOpts,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(Vid, f32)>> {
        use crate::backend::types::DistanceMetric as BackendMetric;

        // Look up vector index config to get the correct distance metric.
        let schema = self.schema_manager.schema();

        // A query vector that doesn't match the declared column dimensions can never
        // score a row — every candidate would be skipped by the length guards below
        // and in the backend, silently returning 0 rows. Error instead (issue #137).
        // Undeclared (schemaless) properties skip the check: no dim is authoritative.
        if let Some(meta) = schema
            .properties
            .get(label)
            .and_then(|props| props.get(property))
            && let uni_common::core::schema::DataType::Vector { dimensions } = &meta.r#type
            && query.len() != *dimensions
        {
            return Err(anyhow!(
                "vector dimension mismatch: query vector has {} dimensions but '{}.{}' is \
                 declared VECTOR({})",
                query.len(),
                label,
                property,
                dimensions
            ));
        }

        let index_cfg = schema.vector_index_for_property(label, property);
        let metric = index_cfg
            .map(|config| config.metric.clone())
            .unwrap_or(DistanceMetric::L2);

        // Quantized indexes rank on lossy codes; without a refine pass recall is
        // capped by quantization rather than by search breadth (0.56 vs 0.99 on a
        // 32x-compressed 1M corpus, for ~3% throughput). Prefer the value stored
        // at index creation, and compute an equivalent for indexes created before
        // that field existed so they benefit without a rebuild. An explicit
        // `refine_factor` — including `1`, the documented opt-out — is untouched.
        let mut opts = opts;
        let mut defaulted_refine = false;
        if opts.refine_factor.is_none()
            && let Some(cfg) = index_cfg
        {
            let dim = schema
                .properties
                .get(label)
                .and_then(|props| props.get(property))
                .and_then(|meta| crate::storage::muvera_index::resolve_vector_dim(&meta.r#type));
            opts.refine_factor = cfg.default_refine_factor.or_else(|| {
                uni_common::vector_index_opts::default_refine_factor(&cfg.index_type, dim)
            });
            defaulted_refine = opts.refine_factor.is_some();
        }

        let backend = self.backend.as_ref();
        let name = table_names::vertex_table_name(label);

        let mut results = Vec::new();

        // Only search if the table exists
        if backend.table_exists(&name).await? {
            let backend_metric = match &metric {
                DistanceMetric::L2 => BackendMetric::L2,
                DistanceMetric::Cosine => BackendMetric::Cosine,
                DistanceMetric::Dot => BackendMetric::Dot,
                // L1 has no ANN backend metric (its column is never ANN-indexed);
                // candidates are fetched by L2 over the FULL table (`fetch_k`
                // below) then re-scored with exact L1, so this L2 ordering does
                // not affect the final result.
                DistanceMetric::L1 => BackendMetric::L2,
                _ => BackendMetric::L2,
            };

            // Build combined filter: _deleted = false + optional user filter + HWM
            let mut filter_parts = vec![Self::build_active_filter(filter)];
            if ctx.is_some()
                && let Some(hwm) = self.version_high_water_mark()
            {
                filter_parts.push(FilterExpr::version_at_most(hwm));
            }
            let combined_filter = FilterExpr::all(filter_parts);

            // L1/Manhattan is served exact/brute-force: fetch every candidate
            // (limit = full row count) and rank by the exact L1 re-score below.
            // O(N) — the inherent cost of exact L1, which cannot use an ANN index.
            let fetch_k = if matches!(metric, DistanceMetric::L1) {
                backend.count_rows(&name, None).await.unwrap_or(k).max(k)
            } else {
                k
            };

            // Cap the *work* a defaulted refine may do, now that `fetch_k` is
            // known. Only `defaulted_refine` is clamped; a caller-supplied value
            // is left exactly as given.
            let mut opts = opts;
            if defaulted_refine && let Some(r) = opts.refine_factor {
                let budget = (REFINE_CANDIDATE_CAP / fetch_k.max(1)).max(1);
                opts.refine_factor = Some(r.min(budget as u32).max(1));
            }

            let batches = backend
                .vector_search(
                    &name,
                    property,
                    query,
                    fetch_k,
                    backend_metric,
                    combined_filter,
                    opts,
                )
                .await?;

            // Re-score ANN candidates with an EXACT distance rather than trusting
            // Lance's `_distance`. Lance's cosine ANN distance is on a different
            // scale (`2(1-cos)`) than the flat path (`1-cos`) that
            // `calculate_score` expects, so the raw value makes the final
            // similarity depend on whether an ANN index served the query
            // (issue #138). The candidate vectors come back in `batches`, so
            // re-scoring needs no extra fetch and puts these candidates on the
            // same `compute_distance` scale as the L0 candidates merged below.
            results = extract_vid_and_vector_pairs(&batches, "_vid", property)?
                .into_iter()
                .filter(|(_, emb)| emb.len() == query.len())
                .map(|(vid, emb)| (vid, metric.compute_distance(&emb, query)))
                .collect();
        }

        // Merge L0 buffer vertices into results for visibility of unflushed data.
        if let Some(qctx) = ctx {
            merge_l0_into_vector_results(&mut results, qctx, label, property, query, k, &metric);
        }

        // Exact top-k. The L1 over-fetch (and the no-L0-activity early return in
        // the merge) can leave more than `k` candidates; rank by distance
        // ascending and truncate. Idempotent for the ANN paths, which already
        // return `k` sorted.
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);

        Ok(results)
    }

    /// First-stage candidate generation for a MUVERA index: single-vector ANN over the
    /// derived FDE column with the **Dot** metric (the FDE inner product approximates
    /// MaxSim — the physical index was built with Dot, see
    /// `IndexManager::create_vector_index`).
    ///
    /// Flushed/indexed data ONLY — unlike [`Self::vector_search`] this deliberately does
    /// NOT merge L0, because the live L0 has no FDE column (it is materialised at flush by
    /// `Writer::materialize_fde_columns`). L0 visibility is provided one layer up by
    /// `multivector_rerank`, which unions live L0 vids and re-scores everything by exact
    /// MaxSim. Scores returned here are placeholder distances (the caller re-ranks).
    #[expect(clippy::too_many_arguments)]
    pub async fn muvera_fde_candidates(
        &self,
        label: &str,
        fde_column: &str,
        fde_query: &[f32],
        k: usize,
        filter: Option<&str>,
        opts: VectorQueryOpts,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(Vid, f32)>> {
        use crate::backend::types::{DistanceMetric as BackendMetric, FilterExpr};

        let backend = self.backend.as_ref();
        let name = table_names::vertex_table_name(label);
        // Distinguish "table genuinely absent" (Ok(false) → nothing flushed yet, L0-only
        // candidates merged upstream) from a transient backend fault (Err): the latter
        // must surface, not silently degrade to incomplete results (issue #96).
        if !backend.table_exists(&name).await? {
            return Ok(Vec::new());
        }

        let mut filter_parts = vec![Self::build_active_filter(filter)];
        if ctx.is_some()
            && let Some(hwm) = self.version_high_water_mark()
        {
            filter_parts.push(FilterExpr::version_at_most(hwm));
        }
        let combined_filter = FilterExpr::all(filter_parts);

        let batches = backend
            .vector_search(
                &name,
                fde_column,
                fde_query,
                k,
                BackendMetric::Dot,
                combined_filter,
                opts,
            )
            .await?;
        extract_vid_score_pairs(&batches, "_vid", "_distance")
    }

    /// Late-interaction (ColBERT / MaxSim) first-stage search over a multi-vector
    /// (`List<Vector>`) column.
    ///
    /// Issues a multi-token query — Lance scores each row's token set by MaxSim —
    /// and defaults to **Cosine** (the ColBERT convention) when the property has
    /// no index, vs `L2` for dense vectors. `opts` (`nprobes` / `refine_factor`)
    /// tune the underlying ANN index.
    ///
    /// This is a **candidate generator over flushed/indexed data only** — it does
    /// not merge unflushed L0 rows (unlike [`Self::vector_search`], which can,
    /// because the single-vector in-process distance is on the identical scale as
    /// Lance's `_distance`). Lance's multi-vector `_distance` is an opaque internal
    /// aggregate whose scale cannot be matched against an in-process MaxSim, so L0
    /// visibility is provided one layer up: the uni-query `multivector_rerank`
    /// helper unions these flushed candidates with live L0 vids and re-scores
    /// *every* candidate by exact MaxSim. Callers wanting recent-write visibility
    /// must go through that path (the `uni.vector.query` procedure and the inline
    /// `vector_similarity` predicate both do); calling this directly sees flushed
    /// data only. Multi-vector search on forks/branches remains unsupported
    /// (`backend::branched` bails).
    #[expect(clippy::too_many_arguments)]
    pub async fn multivector_search(
        &self,
        label: &str,
        property: &str,
        query: &[Vec<f32>],
        k: usize,
        filter: Option<&str>,
        opts: VectorQueryOpts,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(Vid, f32)>> {
        use crate::backend::types::{DistanceMetric as BackendMetric, FilterExpr};

        let schema = self.schema_manager.schema();
        let metric = schema
            .vector_index_for_property(label, property)
            .map(|config| config.metric.clone())
            .unwrap_or(DistanceMetric::Cosine);

        let backend = self.backend.as_ref();
        let name = table_names::vertex_table_name(label);

        // On a branched table, Lance has no per-branch multi-vector nearest
        // (and lancedb cannot open a `Table` on a non-main branch), so the
        // backend's `multivector_search` bails. Instead, enumerate the branch's
        // candidate vids via a branch-aware scan (`BranchedBackend::scan`
        // applies the branch, surfacing fork-local + parent-inherited rows via
        // `base_paths`) and let the uni-query layer re-score by exact MaxSim —
        // it fetches candidate properties branch-aware and merges fork L0. The
        // returned score is a placeholder (the only caller re-ranks). This is a
        // brute-force scan, O(branch rows incl. inherited): the inherent cost of
        // having no multi-vector index on branches.
        let branched = self
            .fork_scope
            .as_ref()
            .is_some_and(|s| s.branch_for(&name).is_some());
        if branched {
            // Ok(false) = nothing flushed on the branch (fork L0 rows merged upstream);
            // Err = a backend fault that must surface rather than degrade silently (#96).
            if !backend.table_exists(&name).await? {
                return Ok(Vec::new());
            }
            let mut filter_parts = vec![Self::build_active_filter(filter)];
            if ctx.is_some()
                && let Some(hwm) = self.version_high_water_mark()
            {
                filter_parts.push(FilterExpr::version_at_most(hwm));
            }
            let request = ScanRequest::all(&name)
                .with_filter(FilterExpr::all(filter_parts))
                .with_columns(vec!["_vid".to_string()]);
            let batches = backend.scan(request).await?;
            let mut results = Vec::new();
            for batch in batches {
                let vid_col = batch
                    .column_by_name("_vid")
                    .ok_or(anyhow!("Missing _vid"))?
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or(anyhow!("Invalid _vid"))?;
                for i in 0..batch.num_rows() {
                    results.push((Vid::from(vid_col.value(i)), 0.0_f32));
                }
            }
            return Ok(results);
        }

        let mut results = Vec::new();
        // Ok(false) = no flushed table yet (L0-only); Err must propagate, not fail open (#96).
        if backend.table_exists(&name).await? {
            let backend_metric = match &metric {
                DistanceMetric::L2 => BackendMetric::L2,
                DistanceMetric::Cosine => BackendMetric::Cosine,
                DistanceMetric::Dot => BackendMetric::Dot,
                _ => BackendMetric::Cosine,
            };

            let mut filter_parts = vec![Self::build_active_filter(filter)];
            if ctx.is_some()
                && let Some(hwm) = self.version_high_water_mark()
            {
                filter_parts.push(FilterExpr::version_at_most(hwm));
            }
            let combined_filter = FilterExpr::all(filter_parts);

            let batches = backend
                .multivector_search(
                    &name,
                    property,
                    query,
                    k,
                    backend_metric,
                    combined_filter,
                    opts,
                )
                .await?;
            results = extract_vid_score_pairs(&batches, "_vid", "_distance")?;
        }

        Ok(results)
    }

    /// Flushed-data candidate generation for scored sparse-vector retrieval.
    ///
    /// Loads the registered sparse index and returns its top-`k`
    /// `(vid, prelim_score)` by dot product over the flushed postings. Like
    /// [`Self::multivector_search`], this is **flushed-only** and the prelim
    /// score is advisory: the uni-query `sparse_rerank` helper unions live L0
    /// rows and re-scores *every* candidate exactly and MVCC-aware via
    /// `sparse_dot`, so callers wanting recent-write visibility must go through
    /// that path. Returns empty if no sparse index is registered for the
    /// property. On a fork/branch there is no per-branch sparse index, so this
    /// brute-force enumerates the branch's candidate vids (Approach A — see the
    /// branched arm) for the re-score path.
    pub async fn sparse_search(
        &self,
        label: &str,
        property: &str,
        query: &[(u32, f32)],
        k: usize,
    ) -> Result<Vec<(Vid, f32)>> {
        #[cfg(feature = "lance-backend")]
        {
            let name = table_names::vertex_table_name(label);
            let branched = self
                .fork_scope
                .as_ref()
                .is_some_and(|s| s.branch_for(&name).is_some());
            if branched {
                // Approach A (v1): the sparse index is a separate hand-rolled
                // Lance dataset, not a vertices-table index, so it cannot ride
                // Lance's `base_paths` branch fusion the way the dense/FTS
                // Lance-native indexes do — there is no per-branch sparse index to
                // query. Enumerate the branch's candidate vids via a branch-aware
                // scan (`base_paths` surfaces fork-local + parent-inherited rows;
                // the `_deleted = false` prefilter drops tombstoned inherited rows)
                // and let the uni-query `sparse_rerank` helper re-score every
                // candidate exactly via `sparse_dot` and union fork L0. The
                // returned score is a placeholder (the only caller re-ranks). This
                // is a brute-force scan, O(branch rows incl. inherited): the
                // proposal's brute-force-DAAT-first choice, mirroring
                // [`Self::multivector_search`]'s branched path.
                //
                // Approach B (deferred, benchmark-gated — issue #95 M5): build a
                // fork-local sparse postings dataset on a fork-scoped path
                // (`SparseVectorIndex::postings_path` made fork-aware), then query
                // parent ∪ fork-local postings minus the fork tombstone set,
                // resolving nested forks through ancestor postings datasets. Faster
                // on large fork corpora, but it re-implements by hand the fusion /
                // tombstone / nested-fork correctness this scan gets from Lance.
                let backend = self.backend.as_ref();
                // Ok(false) = nothing flushed on the branch (fork L0 rows merge upstream);
                // Err = a backend fault that must surface, not fail open silently (#95).
                if !backend.table_exists(&name).await? {
                    return Ok(Vec::new());
                }
                let request = ScanRequest::all(&name)
                    .with_filter(Self::build_active_filter(None))
                    .with_columns(vec!["_vid".to_string()]);
                let batches = backend.scan(request).await?;
                let mut results = Vec::new();
                for batch in batches {
                    let vid_col = batch
                        .column_by_name("_vid")
                        .ok_or_else(|| anyhow!("Missing _vid"))?
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| anyhow!("Invalid _vid"))?;
                    for i in 0..batch.num_rows() {
                        results.push((Vid::from(vid_col.value(i)), 0.0_f32));
                    }
                }
                return Ok(results);
            }
            match self
                .index_manager()
                .sparse_vector_index(label, property)
                .await
            {
                Ok(idx) => idx.query_topk(query, k).await,
                Err(_) => Ok(Vec::new()),
            }
        }
        #[cfg(not(feature = "lance-backend"))]
        {
            let _ = (label, property, query, k);
            Ok(Vec::new())
        }
    }

    /// Perform a full-text search with BM25 scoring.
    ///
    /// Returns vertices matching the search query along with their BM25 scores.
    /// Results are sorted by score descending (most relevant first).
    ///
    /// # Arguments
    /// * `label` - The label to search within
    /// * `property` - The property column to search (must have FTS index)
    /// * `query` - The search query text
    /// * `k` - Maximum number of results to return
    /// * `filter` - Optional Lance filter expression
    /// * `ctx` - Optional query context for visibility checks
    ///
    /// # Returns
    /// Vector of (Vid, score) tuples, where score is the BM25 relevance score.
    pub async fn fts_search(
        &self,
        label: &str,
        property: &str,
        query: &str,
        k: usize,
        filter: Option<&str>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<(Vid, f32)>> {
        let backend = self.backend.as_ref();
        let name = table_names::vertex_table_name(label);

        let mut results = if backend.table_exists(&name).await? {
            // Build combined filter: _deleted = false + optional user filter + HWM
            let mut filter_parts = vec![Self::build_active_filter(filter)];
            if ctx.is_some()
                && let Some(hwm) = self.version_high_water_mark()
            {
                filter_parts.push(FilterExpr::version_at_most(hwm));
            }
            let combined_filter = FilterExpr::all(filter_parts);

            let batches = backend
                .full_text_search(&name, property, query, k, combined_filter)
                .await?;

            let mut fts_results = extract_vid_score_pairs(&batches, "_vid", "_score")?;
            // Results should already be sorted by score from backend, but ensure descending order
            fts_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            fts_results
        } else {
            Vec::new()
        };

        // Merge L0 buffer vertices for visibility of unflushed data.
        if let Some(qctx) = ctx {
            merge_l0_into_fts_results(&mut results, qctx, label, property, query, k);
        }

        Ok(results)
    }

    #[cfg(feature = "lance-backend")]
    pub async fn get_vertex_by_uid(&self, uid: &UniId, label: &str) -> Result<Option<Vid>> {
        let index = self.uid_index(label)?;
        index.get_vid(uid).await
    }

    #[cfg(feature = "lance-backend")]
    pub async fn insert_vertex_with_uid(&self, label: &str, vid: Vid, uid: UniId) -> Result<()> {
        let index = self.uid_index(label)?;
        index.write_mapping(&[(uid, vid)]).await
    }

    pub async fn load_subgraph(
        &self,
        start_vids: &[Vid],
        edge_types: &[u32],
        max_hops: usize,
        direction: GraphDirection,
        l0: Option<&L0Buffer>,
    ) -> Result<WorkingGraph> {
        let mut graph = WorkingGraph::new();
        let schema = self.schema_manager.schema();

        // Build maps for ID lookups
        let label_map: HashMap<u16, String> = schema
            .labels
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.label_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        let edge_type_map: HashMap<u32, String> = schema
            .edge_types
            .values()
            .map(|meta| {
                (
                    meta.id,
                    schema.edge_type_name_by_id(meta.id).unwrap().to_owned(),
                )
            })
            .collect();

        let target_edge_types: HashSet<u32> = edge_types.iter().copied().collect();

        // Initialize frontier
        let mut frontier: Vec<Vid> = start_vids.to_vec();
        let mut visited: HashSet<Vid> = HashSet::new();

        // Add start vertices to graph
        for &vid in start_vids {
            graph.add_vertex(vid);
        }

        for _hop in 0..max_hops {
            let mut next_frontier = HashSet::new();

            for &vid in &frontier {
                if visited.contains(&vid) {
                    continue;
                }
                visited.insert(vid);
                graph.add_vertex(vid);

                // For each edge type we want to traverse
                for &etype_id in &target_edge_types {
                    let etype_name = edge_type_map
                        .get(&etype_id)
                        .ok_or_else(|| anyhow!("Unknown edge type ID: {}", etype_id))?;

                    // Determine directions
                    // Storage direction: "fwd" or "bwd".
                    // Query direction: Outgoing -> "fwd", Incoming -> "bwd".
                    let (dir_str, neighbor_is_dst) = match direction {
                        GraphDirection::Outgoing => ("fwd", true),
                        GraphDirection::Incoming => ("bwd", false),
                    };

                    let mut edges: HashMap<Eid, EdgeState> = HashMap::new();

                    // 1. L2: Adjacency (Base)
                    // In the new storage model, VIDs don't embed label info.
                    // We need to try all labels to find the adjacency data.
                    // Edge version from snapshot (reserved for future version filtering)
                    let _edge_ver = self
                        .pinned_snapshot
                        .as_ref()
                        .and_then(|s| s.edges.get(etype_name).map(|es| es.lance_version));

                    // Try each label until we find adjacency data
                    let backend = self.backend();
                    for current_src_label in label_map.values() {
                        let adj_ds =
                            match self.adjacency_dataset(etype_name, current_src_label, dir_str) {
                                Ok(ds) => ds,
                                Err(_) => continue,
                            };
                        if let Some((neighbors, eids)) =
                            adj_ds.read_adjacency_backend(backend, vid).await?
                        {
                            for (n, eid) in neighbors.into_iter().zip(eids) {
                                edges.insert(
                                    eid,
                                    EdgeState {
                                        neighbor: n,
                                        version: 0,
                                        deleted: false,
                                    },
                                );
                            }
                            break; // Found adjacency data for this vid, no need to try other labels
                        }
                    }

                    // 2. L1: Delta
                    let delta_ds = self.delta_dataset(etype_name, dir_str)?;
                    let delta_entries = delta_ds
                        .read_deltas(backend, vid, &schema, self.snapshot_version_hwm())
                        .await?;
                    Self::apply_delta_to_edges(&mut edges, delta_entries, neighbor_is_dst);

                    // 3. L0: Buffer
                    if let Some(l0) = l0 {
                        Self::apply_l0_to_edges(&mut edges, l0, vid, etype_id, direction);
                    }

                    // Add resulting edges to graph
                    Self::add_edges_to_graph(
                        &mut graph,
                        edges,
                        vid,
                        etype_id,
                        neighbor_is_dst,
                        &visited,
                        &mut next_frontier,
                    );
                }
            }
            frontier = next_frontier.into_iter().collect();

            // Early termination: if frontier is empty, no more vertices to explore
            if frontier.is_empty() {
                break;
            }
        }

        Ok(graph)
    }

    /// Apply delta entries to edge state map, handling version conflicts.
    fn apply_delta_to_edges(
        edges: &mut HashMap<Eid, EdgeState>,
        delta_entries: Vec<crate::storage::delta::L1Entry>,
        neighbor_is_dst: bool,
    ) {
        for entry in delta_entries {
            let neighbor = if neighbor_is_dst {
                entry.dst_vid
            } else {
                entry.src_vid
            };
            let current_ver = edges.get(&entry.eid).map(|s| s.version).unwrap_or(0);

            if entry.version > current_ver {
                edges.insert(
                    entry.eid,
                    EdgeState {
                        neighbor,
                        version: entry.version,
                        deleted: matches!(entry.op, Op::Delete),
                    },
                );
            }
        }
    }

    /// Apply L0 buffer edges and tombstones to edge state map.
    fn apply_l0_to_edges(
        edges: &mut HashMap<Eid, EdgeState>,
        l0: &L0Buffer,
        vid: Vid,
        etype_id: u32,
        direction: GraphDirection,
    ) {
        let l0_neighbors = l0.get_neighbors(vid, etype_id, direction);
        for (neighbor, eid, ver) in l0_neighbors {
            let current_ver = edges.get(&eid).map(|s| s.version).unwrap_or(0);
            if ver > current_ver {
                edges.insert(
                    eid,
                    EdgeState {
                        neighbor,
                        version: ver,
                        deleted: false,
                    },
                );
            }
        }

        // Check tombstones in L0
        for (eid, state) in edges.iter_mut() {
            if l0.is_tombstoned(*eid) {
                state.deleted = true;
            }
        }
    }

    /// Add non-deleted edges to graph and collect next frontier.
    fn add_edges_to_graph(
        graph: &mut WorkingGraph,
        edges: HashMap<Eid, EdgeState>,
        vid: Vid,
        etype_id: u32,
        neighbor_is_dst: bool,
        visited: &HashSet<Vid>,
        next_frontier: &mut HashSet<Vid>,
    ) {
        for (eid, state) in edges {
            if state.deleted {
                continue;
            }
            graph.add_vertex(state.neighbor);

            if !visited.contains(&state.neighbor) {
                next_frontier.insert(state.neighbor);
            }

            if neighbor_is_dst {
                graph.add_edge(vid, state.neighbor, eid, etype_id);
            } else {
                graph.add_edge(state.neighbor, vid, eid, etype_id);
            }
        }
    }
}

/// Extracts `(Vid, f32)` pairs from record batches using the given VID and score column names.
fn extract_vid_score_pairs(
    batches: &[arrow_array::RecordBatch],
    vid_column: &str,
    score_column: &str,
) -> Result<Vec<(Vid, f32)>> {
    let mut results = Vec::new();
    for batch in batches {
        let vid_col = batch
            .column_by_name(vid_column)
            .ok_or_else(|| anyhow!("Missing {} column", vid_column))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("Invalid {} column type", vid_column))?;

        let score_col = batch
            .column_by_name(score_column)
            .ok_or_else(|| anyhow!("Missing {} column", score_column))?
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow!("Invalid {} column type", score_column))?;

        for i in 0..batch.num_rows() {
            results.push((Vid::from(vid_col.value(i)), score_col.value(i)));
        }
    }
    Ok(results)
}

/// Extracts `(vid, embedding)` pairs from vector-search result batches, decoding
/// the dense `FixedSizeList<Float32>` vector column.
///
/// Used to re-score ANN candidates with an exact `compute_distance` (issue #138):
/// Lance's cosine `_distance` scale differs between the ANN-index path
/// (`2(1-cos)`) and the flat path (`1-cos`), so the raw value cannot be trusted
/// for the final similarity. Rows whose vector is null or the wrong length are
/// skipped by the caller.
fn extract_vid_and_vector_pairs(
    batches: &[arrow_array::RecordBatch],
    vid_column: &str,
    vector_column: &str,
) -> Result<Vec<(Vid, Vec<f32>)>> {
    use arrow_array::{Array, FixedSizeListArray};
    let mut out = Vec::new();
    for batch in batches {
        let vid_col = batch
            .column_by_name(vid_column)
            .ok_or_else(|| anyhow!("Missing {} column", vid_column))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("Invalid {} column type", vid_column))?;

        let vec_col = batch
            .column_by_name(vector_column)
            .ok_or_else(|| anyhow!("Missing vector column {}", vector_column))?
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .ok_or_else(|| anyhow!("Vector column {} is not FixedSizeList", vector_column))?;

        for i in 0..batch.num_rows() {
            if vec_col.is_null(i) {
                continue;
            }
            let element = vec_col.value(i);
            let floats = element
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    anyhow!("Vector column {} inner type is not Float32", vector_column)
                })?;
            let emb: Vec<f32> = (0..floats.len()).map(|j| floats.value(j)).collect();
            out.push((Vid::from(vid_col.value(i)), emb));
        }
    }
    Ok(out)
}

/// Extracts a dense `Vec<f32>` embedding from an L0 property value.
///
/// Accepts both representations of a dense vector: the typed
/// [`Value::Vector`] (what the Cypher write path stores) and a
/// [`Value::List`] of numbers (the JSON-ingest representation), via the
/// canonical `TryFrom<&Value>` converter. Returns `None` if the property is
/// missing or not coercible to a numeric vector.
///
/// # Why both variants
///
/// Real Cypher writes land dense embeddings in L0 as `Value::Vector`; scoring
/// L0 candidates off only `Value::List` silently dropped every such candidate,
/// so committed-but-unflushed inserts/updates were invisible to dense search.
fn extract_embedding_from_props(
    props: &uni_common::Properties,
    property: &str,
) -> Option<Vec<f32>> {
    Vec::<f32>::try_from(props.get(property)?).ok()
}

/// Collects the query's L0 buffers in precedence order: pending flush → main
/// → transaction. Earliest first, so a later buffer's write wins.
fn l0_buffers_in_precedence(ctx: &QueryContext) -> Vec<Arc<parking_lot::RwLock<L0Buffer>>> {
    let mut buffers: Vec<Arc<parking_lot::RwLock<L0Buffer>>> =
        ctx.pending_flush_l0s.iter().map(Arc::clone).collect();
    buffers.push(Arc::clone(&ctx.l0));
    if let Some(ref txn) = ctx.transaction_l0 {
        buffers.push(Arc::clone(txn));
    }
    buffers
}

/// Merges L0 buffer vertices into LanceDB vector search results.
///
/// Visits L0 buffers in precedence order (pending flush → main → transaction),
/// collects tombstoned VIDs and candidate embeddings, then merges them with the
/// existing LanceDB results so that:
/// - Tombstoned VIDs are removed (unless re-created in a later L0).
/// - VIDs present in both L0 and LanceDB use the L0 distance.
/// - New L0-only VIDs are appended.
/// - Results are re-sorted by distance ascending and truncated to `k`.
fn merge_l0_into_vector_results(
    results: &mut Vec<(Vid, f32)>,
    ctx: &QueryContext,
    label: &str,
    property: &str,
    query: &[f32],
    k: usize,
    metric: &DistanceMetric,
) {
    let buffers = l0_buffers_in_precedence(ctx);

    // Maps VID → distance for L0 candidates (last writer wins).
    let mut l0_candidates: HashMap<Vid, f32> = HashMap::new();
    // Tombstoned VIDs across all L0 buffers.
    let mut tombstoned: HashSet<Vid> = HashSet::new();

    for buf_arc in &buffers {
        let buf = buf_arc.read();

        // Accumulate tombstones.
        for &vid in &buf.vertex_tombstones {
            tombstoned.insert(vid);
        }

        // Scan vertices with the target label.
        for (&vid, labels) in &buf.vertex_labels {
            if !labels.iter().any(|l| l == label) {
                continue;
            }
            if let Some(props) = buf.vertex_properties.get(&vid)
                && let Some(emb) = extract_embedding_from_props(props, property)
            {
                if emb.len() != query.len() {
                    // Inner defense only: `vector_search` rejects wrong-dim queries
                    // against declared columns up front, and the write paths reject
                    // wrong-dim values (issue #137), so for post-#137 data this skip
                    // is unreachable. It still guards mixed-dim rows written by older
                    // versions and undeclared (schemaless) properties, where
                    // `compute_distance` would panic on a length mismatch.
                    continue; // dimension mismatch
                }
                let dist = metric.compute_distance(&emb, query);
                // Last writer wins: later buffer overwrites earlier.
                l0_candidates.insert(vid, dist);
                // If re-created in a later L0, remove from tombstones.
                tombstoned.remove(&vid);
            }
        }
    }

    // If no L0 activity affects this search, skip merge.
    if l0_candidates.is_empty() && tombstoned.is_empty() {
        return;
    }

    // Remove tombstoned VIDs from LanceDB results.
    results.retain(|(vid, _)| !tombstoned.contains(vid));

    // Overwrite or append L0 candidates.
    for (vid, dist) in &l0_candidates {
        // Skip a vid the precedence chain ultimately tombstoned: it can be a
        // live candidate from an earlier buffer while a later buffer deleted it
        // (that later buffer never revisits it in the label loop, so it stays
        // in `l0_candidates`). Appending it here would resurrect a deleted
        // vertex into vector-search results.
        if tombstoned.contains(vid) {
            continue;
        }
        if let Some(existing) = results.iter_mut().find(|(v, _)| v == vid) {
            existing.1 = *dist;
        } else {
            results.push((*vid, *dist));
        }
    }

    // Re-sort by distance ascending.
    results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(k);
}

/// Collects the live L0 vids carrying `label`, plus the set of vids tombstoned,
/// across the 3-tier L0 chain (pending flush → main → transaction).
///
/// Walks the buffers in precedence order (last writer wins): a vid created in a
/// later buffer clears an earlier tombstone, and a vid tombstoned in a later
/// buffer is removed from the live set. The returned `live` set therefore
/// excludes anything currently tombstoned.
///
/// This mirrors the L0 traversal in `merge_l0_into_vector_results` but returns
/// membership only — it does not score — so callers that re-score candidates
/// themselves (e.g. multi-vector MaxSim re-ranking, where Lance's `_distance`
/// scale is opaque) can build a candidate set without duplicating the
/// tombstone/precedence semantics.
pub fn collect_l0_label_candidates(ctx: &QueryContext, label: &str) -> (Vec<Vid>, HashSet<Vid>) {
    let buffers = l0_buffers_in_precedence(ctx);

    let mut live: HashSet<Vid> = HashSet::new();
    let mut tombstoned: HashSet<Vid> = HashSet::new();

    for buf_arc in &buffers {
        let buf = buf_arc.read();

        // A delete in this buffer wins over earlier creations.
        for &vid in &buf.vertex_tombstones {
            tombstoned.insert(vid);
            live.remove(&vid);
        }

        // A (re-)creation with the target label in this buffer wins over an
        // earlier tombstone.
        for (&vid, labels) in &buf.vertex_labels {
            if !labels.iter().any(|l| l == label) {
                continue;
            }
            if buf.vertex_properties.contains_key(&vid) {
                live.insert(vid);
                tombstoned.remove(&vid);
            }
        }
    }

    (live.into_iter().collect(), tombstoned)
}

/// Computes a simple token-overlap relevance score between a query and text.
///
/// Returns the fraction of query tokens found in the text (case-insensitive),
/// producing a score in [0.0, 1.0]. Sufficient for the small L0 buffer.
fn compute_text_relevance(query: &str, text: &str) -> f32 {
    let query_tokens: HashSet<String> =
        query.split_whitespace().map(|t| t.to_lowercase()).collect();
    if query_tokens.is_empty() {
        return 0.0;
    }
    let text_tokens: HashSet<String> = text.split_whitespace().map(|t| t.to_lowercase()).collect();
    let hits = query_tokens
        .iter()
        .filter(|t| text_tokens.contains(t.as_str()))
        .count();
    hits as f32 / query_tokens.len() as f32
}

/// Extracts a string slice from a property value.
fn extract_text_from_props<'a>(
    props: &'a uni_common::Properties,
    property: &str,
) -> Option<&'a str> {
    props.get(property)?.as_str()
}

/// Merges L0 buffer vertices into LanceDB full-text search results.
///
/// Follows the same pattern as [`merge_l0_into_vector_results`]: visits L0
/// buffers in precedence order, collects tombstoned VIDs and text-match
/// candidates, then merges them so that:
/// - Tombstoned VIDs are removed (unless re-created in a later L0).
/// - VIDs present in both L0 and LanceDB use the L0 score.
/// - New L0-only VIDs are appended.
/// - Results are re-sorted by score **descending** and truncated to `k`.
fn merge_l0_into_fts_results(
    results: &mut Vec<(Vid, f32)>,
    ctx: &QueryContext,
    label: &str,
    property: &str,
    query: &str,
    k: usize,
) {
    let buffers = l0_buffers_in_precedence(ctx);

    // Maps VID → relevance score for L0 candidates (last writer wins).
    let mut l0_candidates: HashMap<Vid, f32> = HashMap::new();
    // Tombstoned VIDs across all L0 buffers.
    let mut tombstoned: HashSet<Vid> = HashSet::new();

    for buf_arc in &buffers {
        let buf = buf_arc.read();

        // Accumulate tombstones.
        for &vid in &buf.vertex_tombstones {
            tombstoned.insert(vid);
        }

        // Scan vertices with the target label.
        for (&vid, labels) in &buf.vertex_labels {
            if !labels.iter().any(|l| l == label) {
                continue;
            }
            if let Some(props) = buf.vertex_properties.get(&vid)
                && let Some(text) = extract_text_from_props(props, property)
            {
                let score = compute_text_relevance(query, text);
                if score > 0.0 {
                    // Last writer wins: later buffer overwrites earlier.
                    l0_candidates.insert(vid, score);
                }
                // If re-created in a later L0, remove from tombstones.
                tombstoned.remove(&vid);
            }
        }
    }

    // If no L0 activity affects this search, skip merge.
    if l0_candidates.is_empty() && tombstoned.is_empty() {
        return;
    }

    // Remove tombstoned VIDs from LanceDB results.
    results.retain(|(vid, _)| !tombstoned.contains(vid));

    // Overwrite or append L0 candidates.
    for (vid, score) in &l0_candidates {
        // Skip a vid the precedence chain ultimately tombstoned (see the vector
        // helper above): a later buffer's delete must not be resurrected into
        // full-text-search results by an earlier buffer's live candidate.
        if tombstoned.contains(vid) {
            continue;
        }
        if let Some(existing) = results.iter_mut().find(|(v, _)| v == vid) {
            existing.1 = *score;
        } else {
            results.push((*vid, *score));
        }
    }

    // Re-sort by score descending (higher relevance first).
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(k);
}
