// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::backend::StorageBackend;
#[cfg(feature = "lance-backend")]
use crate::backend::lance::LanceDbBackend;
use crate::backend::table_names;
use crate::backend::types::{
    CmpOp, FilterExpr, OptimizeReport, Scalar, ScanRequest, VectorQueryOpts,
};
use crate::compaction::{CompactionStats, CompactionStatus, CompactionTask};
use crate::runtime::WorkingGraph;
use crate::runtime::context::QueryContext;
use crate::runtime::l0::L0Buffer;
use crate::storage::adjacency::AdjacencyDataset;
use crate::storage::compaction::{
    CompactionCancelled, CompactionDirLock, Compactor, SemanticCompactionReport,
    is_compaction_cancelled,
};
use crate::storage::declared_values::DeclaredEntity;
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

/// Requested endpoint vids per edge-type row at which one pass beats lookups.
///
/// Measured, not chosen: see `StorageManager::endpoint_scan_beats_lookup`. The
/// crossover sat near 25 000 requested vids against 1 611 869 rows, which is
/// about one requested vid per 64 rows.
///
/// Like every constant of this shape in this codebase it is fitted to one
/// dataset on one machine, and unlike most of them it now has a probe that can
/// be re-run to refit it.
const ENDPOINT_SCAN_CROSSOVER_RATIO: u64 = 64;

/// Requested endpoint vids below which the scan arm is never worth considering.
///
/// A small request cannot repay a whole-type pass whatever the table looks
/// like, and stopping here avoids paying for the per-type count that the
/// decision would otherwise need.
const MIN_ENDPOINT_VIDS_FOR_SCAN: usize = 4_096;

/// Maximum frontier VIDs per adjacency or delta scan in
/// [`StorageManager::load_subgraph`].
///
/// Mirrors `MAX_VIDS_PER_CHUNK` on the vertex read path and `MAX_EIDS_PER_CHUNK`
/// on the edge one: a BFS frontier is unbounded, and an unbounded `IN` list
/// inflates the scan request and stops the scalar index earning its keep, while
/// a bounded one preserves the indexed lookup at any frontier size.
const MAX_SUBGRAPH_VIDS_PER_SCAN: usize = 10_000;

/// Edge state during subgraph loading - tracks version and deletion status.
struct EdgeState {
    neighbor: Vid,
    version: u64,
    deleted: bool,
}

/// One semantic compaction merge in flight; see
/// [`StorageManager::begin_compaction_merge`]. Dropping it ends the merge.
#[derive(Debug)]
pub struct CompactionMerge {
    pins: Arc<crate::storage::adjacency_manager::PinnedVersions>,
    token: Option<crate::storage::adjacency_manager::MergeToken>,
    floor: Option<u64>,
    merged_through: u64,
}

impl CompactionMerge {
    /// The oldest pinned version when the merge began; the merge keeps every
    /// row and delta above it. `None` when nothing was pinned.
    #[must_use]
    pub fn floor(&self) -> Option<u64> {
        self.floor
    }

    /// Records, before the merge writes, that it loses the history below
    /// `merged_through`: persisted with `snapshots`, so a later process
    /// refuses a pin below it too, and applied in memory when the merge ends.
    ///
    /// # Errors
    ///
    /// When the record cannot be persisted; the merge must not write then.
    pub async fn record(&mut self, snapshots: &SnapshotManager, merged_through: u64) -> Result<()> {
        if merged_through == 0 {
            return Ok(());
        }
        let recorded = snapshots.load_compaction_horizon().await?;
        if merged_through > recorded {
            snapshots.save_compaction_horizon(merged_through).await?;
        }
        self.merged_through = self.merged_through.max(merged_through);
        Ok(())
    }
}

impl Drop for CompactionMerge {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            self.pins.end_merge(token, self.merged_through);
        }
    }
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
    /// Held shared by every background index build and exclusively by a
    /// snapshot restore or capture; see [`Self::pause_index_builds`]. Shared
    /// with pinned views of the same store.
    index_build_gate: Arc<tokio::sync::RwLock<()>>,
    /// Cancels compaction on this manager at its next safe point. Cancelled
    /// once, at shutdown; it is never reset. See [`Self::cancel_compaction`].
    compaction_cancel: tokio_util::sync::CancellationToken,
    /// Counter of in-flight `flush_to_l1` operations. Compaction skips
    /// delta-clear when this is non-zero to avoid wiping rows a flush is
    /// about to append. Counter (not bool) so multiple async flushes can
    /// be in flight concurrently.
    pub flush_in_progress: std::sync::atomic::AtomicUsize,
    /// Cached flushed row counts, readable without `await` (#260).
    ///
    /// Shared by `Arc` with every manager derived from this one, so a refresh
    /// on any view is visible to all of them. Sharing is safe because the
    /// derived views that must not use it — fork-scoped and pinned — decline in
    /// [`Self::cached_row_count`] rather than relying on holding a separate
    /// cache.
    cardinality: Arc<crate::storage::cardinality::CardinalityCache>,
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
    /// registry so shadow-CSR GC cannot reclaim entries it still resolves and
    /// semantic compaction keeps the rows it reads
    /// ([`Self::begin_compaction_merge`]).
    ///
    /// Set by `pinned_at_version` and by `pinned()`, which registers with the
    /// live manager's registry (its own `AdjacencyManager` is fresh). A view
    /// at a restore point clears it: its tables are tagged versions. Released
    /// when the view drops; only ever written, never read.
    pin_guard: Option<crate::storage::adjacency_manager::PinGuard>,
    /// Optional fork scope for branch-aware reads (Phase 1 read-only).
    ///
    /// Mutually exclusive with `pinned_snapshot`: a single
    /// `StorageManager` is either pinned to a snapshot or scoped to a
    /// fork, never both. Phase 4's `pin_to_version` on a forked session
    /// builds a separate combined manager out of band; Phase 1 forbids
    /// mixing.
    fork_scope: Option<Arc<crate::fork::ForkScope>>,
    /// The database's fork registry, when one exists.
    ///
    /// Set after construction — the registry is built from the metadata store
    /// only once recovery can run, which is well after the `StorageManager`
    /// is wrapped in an `Arc`. Shared (not copied) across every derived
    /// manager, so a fork-scoped or pinned view sees the same registry.
    ///
    /// Needed so a DDL path can enumerate live fork branches and widen them
    /// alongside primary (issue #249): a branch carries its own schema copy,
    /// and a read through one that lacks a declared column fails outright.
    fork_registry: Arc<std::sync::OnceLock<Arc<crate::fork::ForkRegistryHandle>>>,
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
    /// Hides the rows of bulk loads whose publish is in flight (see
    /// [`crate::storage::bulk_gate`]).
    ///
    /// Shared with the pinned and transaction views of this manager, which read
    /// the same version space. A fork has its own version space, and its own
    /// gate.
    bulk_gate: Arc<crate::storage::bulk_gate::BulkVisibilityGate>,
    /// Bulk-load versions this view excludes for good: loads that were staged
    /// or publishing when its version ceiling was captured, and became
    /// visible only later. Empty for a live manager.
    excluded_versions: Arc<[u64]>,
    /// Whether this view's version ceiling is above everything semantic
    /// compaction merged when the view was made; see
    /// [`Self::history_is_whole`]. Always `true` for a live manager.
    history_whole: bool,
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
    // Match the typed error, not its message. The message is Lance's to
    // reword, and matching it got this wrong twice in the same way: the
    // concurrent-creation case reaches us as `DatasetAlreadyExists` (Lance
    // maps `CommitError::CommitConflict` onto it, so the name is misleading
    // rather than the classification) and `TooMuchWriteContention` displays
    // as "Too many concurrent writers" -- neither contains "conflict" or
    // "Incompatible transaction", so both were treated as fatal.
    let mut saw_lance_error = false;
    for cause in err.chain() {
        if let Some(lance_err) = cause.downcast_ref::<lance::Error>() {
            saw_lance_error = true;
            if matches!(
                lance_err,
                lance::Error::CommitConflict { .. }
                    | lance::Error::RetryableCommitConflict { .. }
                    | lance::Error::IncompatibleTransaction { .. }
                    | lance::Error::TooMuchWriteContention { .. }
                    | lance::Error::DatasetAlreadyExists { .. }
            ) {
                return true;
            }
            // Keep walking rather than returning: `Error::Wrapped` and
            // `Error::External` can carry a retryable error underneath.
        }
    }
    if saw_lance_error {
        return false;
    }

    // Fallback for an error that crossed a boundary erasing the concrete type
    // (stringified across a task, or re-wrapped by a caller). Deliberately
    // second: it cannot distinguish a retryable conflict from a message that
    // merely mentions one.
    let msg = err.to_string();
    msg.contains("Incompatible transaction")
        || msg.contains("conflict")
        || msg.contains("already exists")
        || msg.contains("Too many concurrent writers")
}

/// Runs `op` with exponential-backoff retry on Lance commit conflicts.
/// Up to 10 attempts (~10s worst case); backoff is 1ms, 2ms, 4ms, ...,
/// 512ms. Non-conflict errors return immediately. `op` is re-invoked each
/// attempt so it can re-check table existence and adjust strategy.
///
/// Every Lance commit belongs inside this, not just the vertex and delta
/// writes it was introduced for. Lance reports a losing commit as a
/// *retryable* conflict — "Retryable commit conflict … preempted by concurrent
/// transaction" — and a writer that propagates it instead of retrying turns a
/// scheduling coincidence into a user-visible failure. The secondary-index
/// writers (`index`, `json_index`, `inverted_index`, `sparse_index`) each
/// committed unguarded, which is what made
/// `sparse_recovered_update_overrides_stale_posting_without_rebuild` fail under
/// parallel load and pass alone.
pub(crate) async fn retry_on_lance_conflict<T, F, Fut>(mut op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    for attempt in 0u32..10 {
        match op().await {
            Ok(v) => return Ok(v),
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

/// Write `batches` to the Lance dataset at `uri`, retrying a losing commit.
///
/// One concrete function rather than a closure per call site, for two reasons.
/// It is the whole of what the four secondary-index writers were doing by hand,
/// so a single definition keeps them from drifting apart again. And it keeps the
/// future's type flat: wrapping each site's write in its own async closure
/// nested another layer of anonymous future inside already-`#[instrument]`ed
/// flush paths, and the compiler gave up proving `Send` for the result
/// (`E0275`, overflow evaluating the requirement) in
/// `repro_16_compact_vertices_concurrent_flush`.
pub(crate) async fn write_dataset_with_lance_conflict_retry(
    uri: &str,
    schema: Arc<arrow_schema::Schema>,
    batches: Vec<arrow_array::RecordBatch>,
    mode: lance::dataset::WriteMode,
) -> anyhow::Result<lance::Dataset> {
    // The attempt future is boxed, which is load-bearing rather than stylistic.
    // Unboxed, each retry layer nests `lance::Dataset::write`'s anonymous future
    // inside this one, inside the `#[instrument]`ed flush future above it, and
    // the trait solver gives up proving `Send` while unrolling Lance's internal
    // moka cache types — `E0275`, reported against
    // `repro_16_compact_vertices_concurrent_flush`'s `tokio::spawn`, several
    // crates away from the cause. `Box::pin` erases the inner type and the
    // nesting stops.
    type Attempt<'a> = std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<lance::Dataset>> + Send + 'a>,
    >;
    retry_on_lance_conflict(|| -> Attempt<'_> {
        let schema = Arc::clone(&schema);
        let batches = batches.clone();
        Box::pin(async move {
            let reader = arrow_array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            let params = lance::dataset::WriteParams {
                mode,
                ..Default::default()
            };
            Ok(lance::Dataset::write(Box::new(reader), uri, Some(params)).await?)
        })
    })
    .await
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

/// A vertex's newest row version and, unless that row is a tombstone, its
/// labels.
type NewestLabels = (u64, Option<Vec<String>>);

/// Each vid's newest row in `batches` of the main vertex table (`_vid`,
/// `labels`, `_version`, `_deleted`). A tombstone wins a version tie.
fn newest_vertex_labels(
    batches: &[arrow_array::RecordBatch],
) -> Result<HashMap<Vid, NewestLabels>> {
    let mut newest: HashMap<Vid, NewestLabels> = HashMap::new();
    for batch in batches {
        let vid_col = batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("Missing _vid column"))?;
        let labels_col = batch
            .column_by_name("labels")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>())
            .ok_or_else(|| anyhow!("Missing labels column"))?;
        let ver_col = batch
            .column_by_name("_version")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("Missing _version column"))?;
        let deleted_col = batch
            .column_by_name("_deleted")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>())
            .ok_or_else(|| anyhow!("Missing _deleted column"))?;
        for row in 0..batch.num_rows() {
            let vid = Vid::from(vid_col.value(row));
            let version = ver_col.value(row);
            let deleted = deleted_col.value(row);
            if let Some((best, best_labels)) = newest.get(&vid)
                && (version < *best || (version == *best && (best_labels.is_none() || !deleted)))
            {
                continue;
            }
            let labels = if deleted {
                None
            } else {
                let list = labels_col.value(row);
                let strings = list
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| anyhow!("Invalid labels array element type"))?;
                Some(
                    (0..strings.len())
                        .map(|i| strings.value(i).to_string())
                        .collect(),
                )
            };
            newest.insert(vid, (version, labels));
        }
    }
    Ok(newest)
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
    //
    // The widening below takes that same lock, so it runs to completion
    // *before* the retry loop rather than nested inside it. `Partial`: a merge
    // source carries only the touched columns, so an absent column here says
    // nothing about whether a property was dropped.
    crate::storage::schema_evolution::ensure_table_accepts(
        backend,
        table_name,
        batch.schema_ref(),
        crate::storage::schema_evolution::BatchCoverage::Partial,
    )
    .await?;
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
    // Widen the stored schema first if this batch carries a column the table
    // does not have, or omits a NOT NULL one it does (issue #249). A no-op on
    // every write that is not the first after a property declaration, and it
    // must run *before* the retry loop rather than inside it: the widening
    // takes the same per-table write lock `write` does, and re-running it per
    // attempt would buy nothing. This batch comes from `get_arrow_schema`, so
    // it carries the full declared schema.
    crate::storage::schema_evolution::ensure_table_accepts(
        backend,
        table_name,
        batch.schema_ref(),
        crate::storage::schema_evolution::BatchCoverage::Full,
    )
    .await?;
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

/// Stage `batch` for `table_name` without making any of it readable.
///
/// The staging counterpart of [`write_batch_with_lance_conflict_retry`]: the
/// stored schema is widened the same way (issue #249), and a table that does
/// not exist yet is created *empty* from the batch's schema so the staged
/// files carry the field ids every later append will use. An empty table is
/// the only thing a reader can observe before [`StorageBackend::publish_staged`].
///
/// [`StorageBackend::publish_staged`]: crate::backend::StorageBackend::publish_staged
///
/// # Errors
///
/// Returns an error if widening, creating, or staging fails.
pub async fn stage_batch(
    backend: &dyn crate::backend::StorageBackend,
    table_name: &str,
    batch: arrow_array::RecordBatch,
) -> anyhow::Result<crate::backend::traits::StagedAppend> {
    crate::storage::schema_evolution::ensure_table_accepts(
        backend,
        table_name,
        batch.schema_ref(),
        crate::storage::schema_evolution::BatchCoverage::Full,
    )
    .await?;
    if !backend.table_exists(table_name).await? {
        // A concurrent writer may create it first; that is the state wanted.
        if let Err(e) = backend.create_empty_table(table_name, batch.schema()).await
            && !backend.table_exists(table_name).await?
        {
            return Err(e);
        }
    }
    backend.stage_append(table_name, vec![batch]).await
}

/// Holds compaction off while it lives; see [`StorageManager::pause_compaction`].
#[must_use = "compaction resumes as soon as this is dropped"]
pub struct CompactionPause {
    _guard: CompactionGuard,
    _dir_lock: Option<CompactionDirLock>,
}

impl std::fmt::Debug for CompactionPause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionPause").finish_non_exhaustive()
    }
}

/// Holds background index builds off while it lives; see
/// [`StorageManager::pause_index_builds`].
#[must_use = "index builds resume as soon as this is dropped"]
pub struct IndexBuildPause {
    _guard: tokio::sync::OwnedRwLockWriteGuard<()>,
}

impl std::fmt::Debug for IndexBuildPause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexBuildPause").finish_non_exhaustive()
    }
}

/// Lets one background index build run; see
/// [`StorageManager::index_build_permit`].
#[must_use = "the build is no longer covered once this is dropped"]
pub struct IndexBuildPermit {
    _guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl std::fmt::Debug for IndexBuildPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexBuildPermit").finish_non_exhaustive()
    }
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

/// Clamp a **defaulted** refine factor so a refine pass re-scores at most
/// [`REFINE_CANDIDATE_CAP`] candidates. A caller-supplied `refine_factor` never
/// reaches here — it is honoured verbatim.
///
/// Extracted from the search path so the arithmetic, and the counter below, are
/// reachable from a test: exercising this end-to-end needs `fetch_k > 833` and a
/// trained PQ index, which no unit test can afford to build.
///
/// Clamping trades recall for latency and does it silently — the caller asked
/// for no `refine_factor`, got a default, and now gets less than that default
/// with no signal at all. Only the case where the cap actually *binds* is
/// counted; an unclamped default is not an event (#233).
fn clamp_defaulted_refine(refine_factor: u32, fetch_k: usize) -> u32 {
    let budget = (REFINE_CANDIDATE_CAP / fetch_k.max(1)).max(1);
    let clamped = refine_factor.min(budget as u32).max(1);
    if clamped < refine_factor {
        metrics::counter!("uni_vector_refine_factor_clamped_total").increment(1);
    }
    clamped
}

/// Vids per `_vid IN (...)` probe of a label table during a label backfill.
const LABEL_BACKFILL_VID_CHUNK: usize = 10_000;

/// Directory of the markers [`StorageManager::mark_label_backfill_pending`]
/// writes.
const LABEL_BACKFILL_MARKER_DIR: &str = "catalog/label_backfill";

/// Marker path for `label`. Hex-encoded: a schemaless label is any string, and
/// a `/` in it would otherwise nest the path.
fn label_backfill_marker_path(label: &str) -> object_store::path::Path {
    use std::fmt::Write as _;
    let mut name = String::with_capacity(label.len() * 2);
    for b in label.as_bytes() {
        let _ = write!(name, "{b:02x}");
    }
    object_store::path::Path::from(format!("{LABEL_BACKFILL_MARKER_DIR}/{name}"))
}

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
            cardinality: Arc::new(crate::storage::cardinality::CardinalityCache::new()),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            index_build_gate: Arc::new(tokio::sync::RwLock::new(())),
            compaction_cancel: tokio_util::sync::CancellationToken::new(),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: None,
            pinned_version_hwm: None,
            pin_guard: None,
            fork_scope: None,
            fork_registry: Arc::new(std::sync::OnceLock::new()),
            backend,
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                crate::storage::vid_labels::VidLabelsIndex::new(),
            )),
            plugin_registry: None,
            bulk_gate: Arc::new(crate::storage::bulk_gate::BulkVisibilityGate::new()),
            history_whole: true,
            excluded_versions: Arc::from(Vec::new()),
        };

        // How far earlier processes' semantic compaction merged history, so a
        // session pinned below it is refused rather than served merged rows.
        let horizon = sm.snapshot_manager.load_compaction_horizon().await?;
        sm.adjacency_manager
            .pinned_versions()
            .raise_horizon(horizon);

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
        let cache_sizes = crate::backend::lance_directory::LanceCacheSizes {
            index_bytes: config.lance_index_cache_bytes,
            metadata_bytes: config.lance_metadata_cache_bytes,
        };
        let backend = Arc::new(
            LanceDbBackend::connect_with_cache_sizes(
                base_uri,
                lancedb_storage_options,
                cache_sizes,
            )
            .await?,
        );
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
        let snapshot_excluded = snapshot.unpublished_bulk_versions.clone();
        let (pin_guard, history_whole) = self
            .adjacency_manager
            .pinned_versions()
            .pin_checked(snapshot.version_high_water_mark);
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
            cardinality: self.cardinality.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            index_build_gate: Arc::clone(&self.index_build_gate),
            compaction_cancel: tokio_util::sync::CancellationToken::new(),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            // Registered with the live manager's pins, so semantic compaction
            // keeps the rows this view reads (`Self::begin_compaction_merge`).
            pin_guard: Some(pin_guard),
            pinned_snapshot: Some(snapshot),
            pinned_version_hwm: None,
            fork_scope: self.fork_scope.clone(),
            fork_registry: self.fork_registry.clone(),
            backend: self.backend.clone(),
            // Its own copy, not the shared lock: a fork/pin must get its OWN
            // label index so its flushes/relabels don't mutate the parent's, nor
            // the parent's reach it (review H1/L2). The clone is copy-on-write
            // by shard, so taking it costs no pass over the index. The snapshot
            // is taken after flush-before-branch so inherited labels (#99) are
            // preserved.
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                self.vid_labels_index.read().clone(),
            )),
            plugin_registry: self.plugin_registry.clone(),
            bulk_gate: Arc::clone(&self.bulk_gate),
            history_whole,
            excluded_versions: Arc::from(snapshot_excluded),
        }
    }

    /// [`Self::pinned`] for an explicit snapshot: reads each table at the
    /// version its restore point recorded (`tables`), when the backend can
    /// ([`StorageBackend::at_table_versions`]).
    ///
    /// Those versions are tagged against cleanup and hold exactly the
    /// snapshot's rows, so the view is unaffected by what happened to the
    /// live tables since: semantic compaction folding superseded rows and
    /// edge deltas away, or a table dropped and recreated. The version
    /// ceiling still applies on top.
    #[must_use]
    pub fn pinned_at_restore_point(
        &self,
        snapshot: SnapshotManifest,
        tables: &std::collections::BTreeMap<String, u64>,
    ) -> Self {
        let mut view = self.pinned(snapshot);
        if let Some(backend) = self.backend.at_table_versions(tables) {
            view.backend = backend;
            // Its tables are tagged versions compaction cannot change.
            view.pin_guard = None;
            view.history_whole = true;
        }
        view
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
    /// transaction. Its edges are version-pinned at READ time instead
    /// ([`Self::neighbors`] filters by [`Self::edge_visibility`]): every
    /// adjacency entry carries the version that created it, and the shadow CSR
    /// keeps edges deleted after the pin. The shared CSR is still warmed
    /// unfiltered ([`Self::snapshot_version_hwm`]), since a filtered warm would
    /// poison it for every other reader.
    pub fn pinned_at_version(&self, hwm: u64) -> Self {
        self.pinned_at_version_excluding(hwm, &[])
    }

    /// [`Self::pinned_at_version`], also excluding the bulk-load versions
    /// `excluded` for good: loads reserved at or below `hwm` that were not
    /// visible when the ceiling was captured
    /// ([`crate::runtime::l0_manager::SnapshotView::unpublished_bulk`]).
    /// Without it such a load, published while the transaction runs, would
    /// appear part way through it.
    #[must_use]
    pub fn pinned_at_version_excluding(&self, hwm: u64, excluded: &[u64]) -> Self {
        let excluded: Vec<u64> = excluded.to_vec();
        // Register the pin for as long as this view lives, so shadow GC cannot
        // reclaim entries this reader still resolves and compaction neither
        // merges a vertex row above `hwm` nor folds an edge delta this reader
        // must not see into unversioned L2. Dropped with the view.
        let edge_pin = excluded
            .iter()
            .min()
            .map_or(hwm, |&oldest| oldest.saturating_sub(1).min(hwm));
        let pin_guard = Some(
            self.adjacency_manager
                .pinned_versions()
                .pin_transaction(hwm, edge_pin),
        );
        Self {
            requires_adjacency_revalidation: self.requires_adjacency_revalidation,
            base_uri: self.base_uri.clone(),
            store: self.store.clone(),
            schema_manager: self.schema_manager.clone(),
            snapshot_manager: self.snapshot_manager.clone(),
            adjacency_manager: self.adjacency_manager.clone(),
            config: self.config.clone(),
            cardinality: self.cardinality.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            index_build_gate: Arc::clone(&self.index_build_gate),
            compaction_cancel: tokio_util::sync::CancellationToken::new(),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: None,
            pinned_version_hwm: Some(hwm),
            pin_guard,
            fork_scope: self.fork_scope.clone(),
            fork_registry: self.fork_registry.clone(),
            backend: self.backend.clone(),
            // Its own copy, not the shared lock: a fork/pin must get its OWN
            // label index so its flushes/relabels don't mutate the parent's, nor
            // the parent's reach it (review H1/L2). The clone is copy-on-write
            // by shard, so taking it costs no pass over the index. The snapshot
            // is taken after flush-before-branch so inherited labels (#99) are
            // preserved.
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                self.vid_labels_index.read().clone(),
            )),
            plugin_registry: self.plugin_registry.clone(),
            bulk_gate: Arc::clone(&self.bulk_gate),
            history_whole: true,
            excluded_versions: Arc::from(excluded),
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
            cardinality: self.cardinality.clone(),
            compaction_status: Arc::new(Mutex::new(CompactionStatus::default())),
            index_build_gate: Arc::new(tokio::sync::RwLock::new(())),
            compaction_cancel: tokio_util::sync::CancellationToken::new(),
            flush_in_progress: std::sync::atomic::AtomicUsize::new(0),
            pinned_snapshot: None,
            pinned_version_hwm: None,
            pin_guard: None,
            fork_scope: Some(scope),
            fork_registry: self.fork_registry.clone(),
            backend: branched_backend,
            // Its own copy, not the shared lock: a fork/pin must get its OWN
            // label index so its flushes/relabels don't mutate the parent's, nor
            // the parent's reach it (review H1/L2). The clone is copy-on-write
            // by shard, so taking it costs no pass over the index. The snapshot
            // is taken after flush-before-branch so inherited labels (#99) are
            // preserved.
            vid_labels_index: Arc::new(parking_lot::RwLock::new(
                self.vid_labels_index.read().clone(),
            )),
            plugin_registry: self.plugin_registry.clone(),
            // A fork stamps versions from its own counter, so the primary's
            // hidden versions mean nothing here, and the fork's loads must not
            // hide primary rows.
            bulk_gate: Arc::new(crate::storage::bulk_gate::BulkVisibilityGate::new()),
            history_whole: true,
            excluded_versions: Arc::from(Vec::new()),
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

    /// Attach the database's fork registry. Idempotent; a second call with a
    /// different handle is ignored rather than panicking, since the first
    /// writer wins and every caller passes the same database's registry.
    pub fn set_fork_registry(&self, registry: Arc<crate::fork::ForkRegistryHandle>) {
        let _ = self.fork_registry.set(registry);
    }

    /// The fork registry, if one has been attached.
    pub fn fork_registry(&self) -> Option<&Arc<crate::fork::ForkRegistryHandle>> {
        self.fork_registry.get()
    }

    /// Tables whose Arrow schema is derived from `entity`'s declared
    /// properties.
    ///
    /// A vertex label owns one table. An edge type owns *both* delta
    /// directions: `PropertyManager` only ever updates through `fwd`, but
    /// `bwd` carries the same property columns and its `get_arrow_schema`
    /// would disagree with the stored table if it were skipped.
    pub fn property_tables_for(&self, entity: &str) -> Vec<String> {
        use crate::backend::table_names::{delta_table_name, vertex_table_name};
        let schema = self.schema_manager().schema();
        if schema.labels.contains_key(entity) {
            vec![vertex_table_name(entity)]
        } else if schema.edge_types.contains_key(entity) {
            vec![
                delta_table_name(entity, "fwd"),
                delta_table_name(entity, "bwd"),
            ]
        } else {
            Vec::new()
        }
    }

    /// Rows that already exist for `entity`, flushed or not.
    ///
    /// Answers whether a `NOT NULL` a caller is about to declare could be
    /// violated by rows that have no value for it. Tables are created lazily,
    /// so "no table" counts as no flushed rows — but committed rows still in
    /// L0 exist just as much. Counting only flushed tables recorded a NOT NULL
    /// over an all-L0 label, and every flush after it then failed on the NULLs
    /// those rows carried. `l0` supplies the unflushed side; an upper bound is
    /// fine, since only zero versus non-zero matters.
    pub async fn existing_row_count(
        &self,
        entity: &str,
        l0: &crate::runtime::l0_visibility::L0Context,
    ) -> anyhow::Result<usize> {
        let backend = self.backend();
        let mut total = l0.entity_count(entity);
        for table in self.property_tables_for(entity) {
            if backend.table_exists(&table).await? {
                total += backend.count_rows(&table, None).await?;
            }
        }
        // A label's schemaless rows live only in the main table until the
        // label is declared and backfilled (#304), so a label populated
        // schemalessly and flushed has no label table to count yet.
        let schema = self.schema_manager.schema();
        if total == 0 && !schema.edge_types.contains_key(entity) {
            let main = table_names::main_vertex_table_name();
            if backend.table_exists(main).await? {
                let carries = FilterExpr::array_contains("labels", Scalar::Str(entity.to_string()));
                total += backend.count_rows(main, Some(&carries)).await?;
            }
        }
        Ok(total)
    }

    /// The nullability to record for a property being *added* to `entity`.
    ///
    /// A `NOT NULL` property cannot be satisfied by rows that already exist --
    /// they have no value for it. It is recorded as nullable instead, with a
    /// warning, so the catalog matches what the data can support; `NOT NULL`
    /// is still enforced forward, at write time. Only for a property the
    /// catalog does not hold yet: rewriting the declaration of one it already
    /// holds is what made unchanged re-declarations collide (#286).
    ///
    /// # Errors
    ///
    /// Propagates a row-count failure.
    pub async fn new_property_nullability(
        &self,
        entity: &str,
        property: &str,
        nullable: bool,
        l0: &crate::runtime::l0_visibility::L0Context,
    ) -> Result<bool> {
        if nullable {
            return Ok(true);
        }
        let rows = self.existing_row_count(entity, l0).await?;
        if rows == 0 {
            return Ok(false);
        }
        tracing::warn!(
            entity,
            property,
            rows,
            "declaring a NOT NULL property on a populated entity; \
             recording it as nullable because existing rows have no value"
        );
        Ok(true)
    }

    /// Copy `label`'s rows from the main `vertices` table into its label
    /// table, so a label declared over existing schemaless vertices keeps
    /// them (#304).
    ///
    /// A flush writes a vertex into `vertices_{label}` only for a label the
    /// schema it snapshotted declares; every other label's rows live only in
    /// the main table. Once the label is declared, a label-scoped read goes to
    /// the label table alone, so anything flushed before the declaration is
    /// invisible until it is copied over. This is that copy.
    ///
    /// The main table's latest row per vertex decides what the label table
    /// should hold, and the label table is brought to it — never past it:
    /// - latest row live and carrying `label`: written at its own version,
    ///   labels, properties and timestamps, unless the label table already
    ///   has that vertex at that version or later;
    /// - latest row a tombstone, or no longer carrying `label`: if the label
    ///   table's latest row for it is live and older, a tombstone is written
    ///   at the main row's version, as a flush writes for a delete or a
    ///   relabel away. This retires rows a backfill interrupted earlier, or a
    ///   label dropped and re-declared, left behind.
    ///
    /// That makes it idempotent and safe to re-run, and safe to race with a
    /// flush that already sees the declaration: reads keep the highest
    /// `_version` per vertex, and every row written here is one the main
    /// table already holds, at the version it holds it.
    ///
    /// The caller must ensure no flush that snapshotted the schema *before*
    /// the declaration is still writing; such a flush adds main-only rows
    /// this scan cannot see. `Writer::backfill_declared_label` provides that
    /// barrier.
    ///
    /// Appends only, so it is fork-safe: on a fork-scoped manager the writes
    /// land on the fork's branch, and the reads see the fork's fused view of
    /// the main table.
    ///
    /// Returns the number of live rows copied (tombstones written to retire
    /// stale rows are not counted), which is what a flush adds to the
    /// label's manifest row count for the same rows. A label the schema does
    /// not declare is skipped.
    ///
    /// # Errors
    ///
    /// Propagates scan, decode and write failures.
    pub async fn backfill_declared_label(&self, label: &str) -> Result<usize> {
        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Ok(0);
        }
        let backend = self.backend();

        // Every version, bulk-gated ones included: a row copied here keeps
        // its version, so whatever hides it in the main table hides it in the
        // label table too.
        let latest = MainVertexDataset::find_label_latest_rows(
            backend,
            label,
            crate::storage::bulk_gate::RowVisibility::at_most(None),
            None,
        )
        .await?;
        if latest.is_empty() {
            return Ok(0);
        }

        // The label table's latest (version, deleted) per candidate vertex.
        let table = table_names::vertex_table_name(label);
        let mut present: HashMap<Vid, (u64, bool)> = HashMap::new();
        if backend.table_exists(&table).await? {
            let mut vids: Vec<u64> = latest.keys().map(|v| v.as_u64()).collect();
            vids.sort_unstable();
            for chunk in vids.chunks(LABEL_BACKFILL_VID_CHUNK) {
                let batches = backend
                    .scan(
                        ScanRequest::all(&table)
                            .with_filter(FilterExpr::one_of(
                                "_vid",
                                chunk.iter().map(|v| Scalar::UInt(*v)),
                            ))
                            .with_columns(vec![
                                "_vid".to_string(),
                                "_version".to_string(),
                                "_deleted".to_string(),
                            ]),
                    )
                    .await?;
                for batch in &batches {
                    let (Some(vid_arr), Some(ver_arr)) = (
                        batch
                            .column_by_name("_vid")
                            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                        batch
                            .column_by_name("_version")
                            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                    ) else {
                        continue;
                    };
                    let del_arr = batch
                        .column_by_name("_deleted")
                        .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());
                    for i in 0..batch.num_rows() {
                        if vid_arr.is_null(i) {
                            continue;
                        }
                        let vid = Vid::new(vid_arr.value(i));
                        let version = if ver_arr.is_null(i) {
                            0
                        } else {
                            ver_arr.value(i)
                        };
                        let deleted = del_arr.is_some_and(|d| d.is_valid(i) && d.value(i));
                        if present.get(&vid).is_some_and(|(v, _)| version < *v) {
                            continue;
                        }
                        present.insert(vid, (version, deleted));
                    }
                }
            }
        }

        let mut vertices: Vec<(Vid, Vec<String>, uni_common::Properties)> = Vec::new();
        let mut deleted: Vec<bool> = Vec::new();
        let mut versions: Vec<u64> = Vec::new();
        let mut created_at: HashMap<Vid, i64> = HashMap::new();
        let mut updated_at: HashMap<Vid, i64> = HashMap::new();
        let mut live_rows: Vec<(Vid, u64)> = Vec::new();
        let mut ordered: Vec<_> = latest.into_iter().collect();
        ordered.sort_unstable_by_key(|(vid, _)| *vid);
        for (vid, row) in ordered {
            let existing = present.get(&vid).copied();
            if row.is_live_with(label) {
                if existing.is_some_and(|(v, _)| v >= row.version) {
                    continue;
                }
                // Declared properties in the form a write of them stores.
                // `check_declared_values` refused the declaration if any value
                // could not be; a failure here is a value that arrived since,
                // refused rather than stored as NULL.
                let mut row = row;
                if let Some(declared) = schema.properties.get(label) {
                    for (name, meta) in declared {
                        if let Some(value) = row.props.remove(name) {
                            let value =
                                crate::storage::declared_values::coerce(name, &meta.r#type, value)
                                    .map_err(|e| {
                                        anyhow!(
                                            "cannot backfill vid {} into label '{label}': {e}",
                                            vid.as_u64()
                                        )
                                    })?;
                            row.props.insert(name.clone(), value);
                        }
                    }
                }
                if let Some(ts) = row.created_at {
                    created_at.insert(vid, ts);
                }
                if let Some(ts) = row.updated_at {
                    updated_at.insert(vid, ts);
                }
                live_rows.push((vid, row.version));
                vertices.push((vid, row.labels, row.props));
                deleted.push(false);
                versions.push(row.version);
            } else if existing.is_some_and(|(v, gone)| !gone && v < row.version) {
                if let Some(ts) = row.updated_at {
                    updated_at.insert(vid, ts);
                }
                vertices.push((vid, vec![label.to_string()], uni_common::Properties::new()));
                deleted.push(true);
                versions.push(row.version);
            }
        }
        if vertices.is_empty() {
            return Ok(0);
        }

        let ds = self.vertex_dataset(label)?;
        let batch = ds.build_record_batch_with_timestamps(
            &vertices,
            &deleted,
            &versions,
            &schema,
            Some(&created_at),
            Some(&updated_at),
        )?;
        ds.write_batch(backend, batch, &schema).await?;
        ds.ensure_default_indexes(backend).await?;
        let copied = live_rows.len();

        // UID mappings for the live rows, stamped with each row's own
        // version, as the flush that would have written them stamps its own.
        #[cfg(feature = "lance-backend")]
        if !live_rows.is_empty()
            && let Ok(uid_index) = self.uid_index(label)
        {
            let props_by_vid: HashMap<Vid, &uni_common::Properties> = vertices
                .iter()
                .zip(&deleted)
                .filter(|(_, gone)| !**gone)
                .map(|((vid, _, props), _)| (*vid, props))
                .collect();
            let mappings: Vec<(UniId, Vid, u64)> = live_rows
                .iter()
                .filter_map(|(vid, version)| {
                    let props = props_by_vid.get(vid)?;
                    let ext_id = props.get("ext_id").and_then(|v| v.as_str());
                    let uid = VertexDataset::compute_vertex_uid(label, ext_id, props);
                    Some((uid, *vid, *version))
                })
                .collect();
            uid_index.write_mappings_at_versions(&mappings).await?;
        }
        #[cfg(not(feature = "lance-backend"))]
        let _ = live_rows;

        self.invalidate_table_cache(label);
        self.cardinality().invalidate_all();
        tracing::info!(
            label,
            copied,
            retired = vertices.len() - copied,
            "backfilled a newly declared label from the main vertex table"
        );
        Ok(copied)
    }

    /// Record that `label` is being declared and its backfill has not yet
    /// completed (#304).
    ///
    /// Written before the label enters the live catalog: any commit may save
    /// a dirty catalog, so the declaration can reach disk before the backfill
    /// runs, and a crash in between would leave the label's flushed rows
    /// hidden for good. [`Self::recover_label_backfills`] finishes the job on
    /// the next open; [`Self::clear_label_backfill_marker`] retires the marker
    /// once the backfill and the catalog save have both completed.
    ///
    /// A no-op on a fork-scoped manager. A fork persists its schema only
    /// through its overlay, which its declaration path writes after the
    /// backfill, so ordering alone covers it.
    ///
    /// # Errors
    ///
    /// Propagates the object-store write failure.
    pub async fn mark_label_backfill_pending(&self, label: &str) -> Result<()> {
        use object_store::ObjectStoreExt;
        if self.fork_scope.is_some() {
            return Ok(());
        }
        self.store
            .put(
                &label_backfill_marker_path(label),
                label.as_bytes().to_vec().into(),
            )
            .await?;
        Ok(())
    }

    /// Retire the marker [`Self::mark_label_backfill_pending`] wrote.
    ///
    /// # Errors
    ///
    /// Propagates an object-store delete failure other than "not found".
    pub async fn clear_label_backfill_marker(&self, label: &str) -> Result<()> {
        use object_store::ObjectStoreExt;
        if self.fork_scope.is_some() {
            return Ok(());
        }
        match self.store.delete(&label_backfill_marker_path(label)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Finish every label backfill a crash interrupted (#304).
    ///
    /// For each pending marker: a label the loaded catalog declares is
    /// backfilled (idempotent); a label it does not declare never reached
    /// disk, so its rows are still read from the main table and there is
    /// nothing to do. Either way the marker is then cleared. Returns each
    /// label that was backfilled with the live rows copied, for the caller to
    /// rebuild its indexes and record the rows in the manifest.
    ///
    /// A backfill that fails is logged at `error` and its marker kept, so the
    /// next open retries it; the open itself goes on. Failing the open instead
    /// would turn a deterministic failure (one the declaration itself would
    /// have hit too) into a database that cannot be opened, while going on
    /// confines the cost to the label's pre-declaration rows staying hidden.
    ///
    /// Run on a writable open, before any write can race it.
    ///
    /// # Errors
    ///
    /// Propagates marker listing, reading and deleting failures.
    pub async fn recover_label_backfills(&self) -> Result<Vec<(String, usize)>> {
        use futures::TryStreamExt;
        use object_store::ObjectStoreExt;
        if self.fork_scope.is_some() {
            return Ok(Vec::new());
        }
        let prefix = object_store::path::Path::from(LABEL_BACKFILL_MARKER_DIR);
        let markers: Vec<object_store::ObjectMeta> =
            self.store.list(Some(&prefix)).try_collect().await?;
        let mut recovered = Vec::new();
        for meta in markers {
            let bytes = self.store.get(&meta.location).await?.bytes().await?;
            let label = String::from_utf8(bytes.to_vec()).map_err(|e| {
                anyhow!("label backfill marker {} is not UTF-8: {e}", meta.location)
            })?;
            if self.schema_manager.schema().labels.contains_key(&label) {
                match self.backfill_declared_label(&label).await {
                    Ok(rows) => {
                        tracing::warn!(
                            label = %label,
                            rows,
                            "completed a label backfill interrupted by a crash"
                        );
                        recovered.push((label.clone(), rows));
                    }
                    Err(e) => {
                        tracing::error!(
                            label = %label,
                            error = %e,
                            "label backfill failed; vertices flushed before the label was \
                             declared stay hidden from label-scoped reads until it succeeds \
                             (retried on the next open)"
                        );
                        continue;
                    }
                }
            }
            self.clear_label_backfill_marker(&label).await?;
        }
        Ok(recovered)
    }

    /// Refuse to declare `properties` on `entity` if values it already holds
    /// cannot be stored in their declared types.
    ///
    /// The declaration moves those values into typed columns, and a column
    /// builder stores a value it cannot hold as NULL, so without this check
    /// the declaration itself would silently lose data. The values examined
    /// are the ones the declaration moves:
    /// - for a declared label or edge type (#249's promotion): every row's
    ///   `overflow_json` entry for the property in the entity's tables, all
    ///   versions, as the promotion rewrites all of them;
    /// - for a label not yet declared (#304's backfill): the property on the
    ///   latest live main-table row of every vertex carrying the label.
    ///
    /// Each value is judged by the write path's rule (see
    /// [`crate::storage::declared_values`]). Unflushed values are not read
    /// here; callers flush the entity's committed L0 first
    /// (`Writer::flush_before_declaration`), which moves them where this
    /// looks.
    ///
    /// Must run before the catalog or storage changes, so a refused
    /// declaration leaves both as they were.
    ///
    /// # Errors
    ///
    /// [`uni_common::UniError::DeclaredTypeConflict`] (wrapped in
    /// `anyhow::Error`) for the first conflicting property; scan and decode
    /// failures otherwise.
    pub async fn check_declared_values(
        &self,
        entity: &str,
        kind: DeclaredEntity,
        properties: &[(String, uni_common::DataType)],
    ) -> Result<()> {
        use crate::storage::declared_values::check_value;
        // A property the catalog already declares had its values checked
        // when it was declared, and they live in its typed column; re-running
        // the check for an unchanged re-declaration (the register-on-every-
        // open pattern) would rescan the entity for nothing.
        let properties: Vec<(String, uni_common::DataType)> = {
            let schema = self.schema_manager.schema();
            let declared = schema.properties.get(entity);
            properties
                .iter()
                .filter(|(name, _)| !declared.is_some_and(|p| p.contains_key(name)))
                .cloned()
                .collect()
        };
        let properties = properties.as_slice();
        if properties.is_empty() {
            return Ok(());
        }
        let backend = self.backend();
        // Per property: (count, first offender as (id, value)).
        let mut conflicts: Vec<(usize, Option<(String, uni_common::Value)>)> =
            vec![(0, None); properties.len()];
        let mut note = |i: usize, id: String, value: &uni_common::Value| {
            let (count, example) = &mut conflicts[i];
            *count += 1;
            if example.is_none() {
                *example = Some((id, value.clone()));
            }
        };

        // A relationship's rows sit in both delta directions; one is read.
        let tables = self.declaration_tables(entity, kind);
        if !tables.is_empty() {
            for table in tables.iter().take(1) {
                if !backend.table_exists(table).await? {
                    continue;
                }
                let is_vertex_table = table == &table_names::vertex_table_name(entity);
                let id_column = if is_vertex_table { "_vid" } else { "eid" };
                let batches = backend
                    .scan(
                        ScanRequest::all(table)
                            .with_columns(vec![id_column.to_string(), "overflow_json".to_string()]),
                    )
                    .await?;
                for batch in &batches {
                    let ids = batch
                        .column_by_name(id_column)
                        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
                    let overflow = batch
                        .column_by_name("overflow_json")
                        .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());
                    if overflow.is_none() {
                        continue;
                    }
                    for row in 0..batch.num_rows() {
                        for (i, (name, declared)) in properties.iter().enumerate() {
                            let Some(bytes) =
                                crate::runtime::columnar_scan::extract_from_overflow_blob(
                                    overflow, row, name,
                                )?
                            else {
                                continue;
                            };
                            let value = uni_common::cypher_value_codec::decode(&bytes)
                                .map_err(|e| anyhow!("corrupt overflow value: {e}"))?;
                            if !check_value(name, declared, &value) {
                                let id = ids.map_or(0, |a| a.value(row));
                                let kind = if is_vertex_table { "vid" } else { "eid" };
                                note(i, format!("{kind} {id}"), &value);
                            }
                        }
                    }
                }
            }
        } else if kind == DeclaredEntity::Label {
            let latest = MainVertexDataset::find_label_latest_rows(
                backend,
                entity,
                crate::storage::bulk_gate::RowVisibility::at_most(None),
                None,
            )
            .await?;
            let mut ordered: Vec<_> = latest.iter().collect();
            ordered.sort_unstable_by_key(|(vid, _)| **vid);
            for (vid, row) in ordered {
                if !row.is_live_with(entity) {
                    continue;
                }
                for (i, (name, declared)) in properties.iter().enumerate() {
                    if let Some(value) = row.props.get(name)
                        && !check_value(name, declared, value)
                    {
                        note(i, format!("vid {}", vid.as_u64()), value);
                    }
                }
            }
        }

        for ((name, declared), (count, example)) in properties.iter().zip(conflicts) {
            if count == 0 {
                continue;
            }
            let (example_id, example_value) =
                example.unwrap_or_else(|| (String::new(), uni_common::Value::Null));
            return Err(uni_common::UniError::DeclaredTypeConflict {
                detail: Box::new(uni_common::DeclaredTypeConflict {
                    entity: entity.to_string(),
                    property: name.clone(),
                    declared: format!("{declared:?}"),
                    count,
                    example_id,
                    example_value: example_value.to_string(),
                }),
            }
            .into());
        }
        Ok(())
    }

    /// Widen storage so newly declared properties on `entity` have a column
    /// everywhere they will be written **or read** (issue #249).
    ///
    /// Call this *before* the property enters the live catalog, not merely
    /// before this caller saves it. The asymmetry decides the order: a Lance
    /// column absent from the catalog is invisible and harmless, while a
    /// catalog column absent from Lance is a hard read failure. There is no WAL
    /// record for schema DDL, so ordering is the only recovery lever — and any
    /// commit saves a dirty catalog (`save_if_dirty`), so a property declared
    /// in memory first could reach disk ahead of its column, and a crash before
    /// the widening finished left the entity unreadable. Each property comes
    /// with its declared type for that reason: the catalog does not hold it
    /// yet. The widening is idempotent, so re-running a half-applied DDL
    /// converges.
    ///
    /// Primary is widened first, so any fork branch cut *after* this point
    /// inherits the column from primary's current version and needs nothing.
    /// Branches cut before it are then widened explicitly. That second step is
    /// not redundant with the write-path reconcile in
    /// [`crate::storage::schema_evolution`]: a *write* through a stale branch
    /// repairs itself there, but a *read* does not go through that path at all
    /// and fails with `No field named …` from Lance's projection.
    pub async fn widen_for_declared_properties(
        &self,
        entity: &str,
        properties: &[(String, uni_common::DataType)],
    ) -> anyhow::Result<()> {
        // Test-only seam (no-op without `fail/failpoints`): a DDL is about to
        // widen storage, so the property must not be in the live catalog yet.
        fail::fail_point!("storage::widen-declared-properties");
        if properties.is_empty() {
            return Ok(());
        }
        let kind = if self.schema_manager.schema().edge_types.contains_key(entity) {
            DeclaredEntity::EdgeType
        } else {
            DeclaredEntity::Label
        };
        // Before anything changes: a refused declaration leaves storage as it was.
        self.check_declared_values(entity, kind, properties).await?;
        self.widen_for_checked_properties(entity, kind, properties)
            .await
    }

    /// The tables a declaration of `entity`'s properties widens and promotes
    /// from: the entity's own tables once declared (see
    /// [`Self::property_tables_for`]); for an edge type not yet declared, the
    /// delta tables its schemaless relationships were flushed to, which keep
    /// serving it once declared; for a label not yet declared, none -- its
    /// rows are backfilled into a new table instead
    /// ([`Self::backfill_declared_label`]).
    fn declaration_tables(&self, entity: &str, kind: DeclaredEntity) -> Vec<String> {
        let tables = self.property_tables_for(entity);
        if !tables.is_empty() || kind == DeclaredEntity::Label {
            return tables;
        }
        vec![
            table_names::delta_table_name(entity, "fwd"),
            table_names::delta_table_name(entity, "bwd"),
        ]
    }

    /// Rewrite the main-table copies of `entity`'s values for newly declared
    /// `properties` into the form a write of the declared property stores.
    ///
    /// The main `vertices` / `main_edges` tables keep every property in a
    /// `props_json` blob, which reads that do not go through the entity's
    /// typed columns (an unlabeled `MATCH (n)`, `properties(n)`) return as
    /// stored. A value written after the declaration was converted on the way
    /// in (an `Int` stored as a `Float` in a float column, an ISO string as a
    /// datetime); one written before it would otherwise stay in its schemaless
    /// form there, so the two reads disagreed. Only the latest row of each
    /// vertex (carrying the label) or relationship (of the type) is rewritten,
    /// in place by `(id, _version)`, and only when a value changes; older
    /// versions keep the form they were written in.
    ///
    /// Primary only, as for the promotion out of `overflow_json`.
    ///
    /// # Errors
    ///
    /// A value the declared type cannot hold (refused earlier by
    /// [`Self::check_declared_values`]), and scan or write failures.
    pub async fn convert_main_table_values(
        &self,
        entity: &str,
        kind: DeclaredEntity,
        properties: &[(String, uni_common::DataType)],
    ) -> Result<usize> {
        use crate::storage::declared_values::coerce;
        if properties.is_empty() || self.fork_scope.is_some() {
            return Ok(0);
        }
        let backend = self.backend();
        let convert = |props: &uni_common::Properties| -> Result<Option<uni_common::Properties>> {
            let mut out = props.clone();
            let mut changed = false;
            for (name, declared) in properties {
                if let Some(value) = props.get(name) {
                    let new = coerce(name, declared, value.clone())?;
                    if &new != value {
                        out.insert(name.clone(), new);
                        changed = true;
                    }
                }
            }
            Ok(changed.then_some(out))
        };
        let encode = |props: uni_common::Properties| {
            uni_common::cypher_value_codec::encode(&uni_common::Value::Map(props))
        };

        // (id, _version, re-encoded props_json) of a row to rewrite.
        type Rewrite = (u64, u64, Vec<u8>);
        let (table, id_column, rows): (&str, &str, Vec<Rewrite>) = match kind {
            DeclaredEntity::Label => {
                let latest = MainVertexDataset::find_label_latest_rows(
                    backend,
                    entity,
                    crate::storage::bulk_gate::RowVisibility::at_most(None),
                    None,
                )
                .await?;
                let mut rows = Vec::new();
                for (vid, row) in latest {
                    if !row.is_live_with(entity) {
                        continue;
                    }
                    if let Some(props) = convert(&row.props)? {
                        rows.push((vid.as_u64(), row.version, encode(props)));
                    }
                }
                (table_names::main_vertex_table_name(), "_vid", rows)
            }
            DeclaredEntity::EdgeType => {
                let table = table_names::main_edge_table_name();
                let mut rows = Vec::new();
                if backend.table_exists(table).await? {
                    let batches = backend
                        .scan(
                            ScanRequest::all(table)
                                .with_filter(FilterExpr::equals(
                                    "type",
                                    Scalar::Str(entity.to_string()),
                                ))
                                .with_columns(vec![
                                    "_eid".to_string(),
                                    "_version".to_string(),
                                    "_deleted".to_string(),
                                    "props_json".to_string(),
                                ]),
                        )
                        .await?;
                    // Latest row per relationship: (version, deleted, blob).
                    let mut latest: HashMap<u64, (u64, bool, Option<Vec<u8>>)> = HashMap::new();
                    for batch in &batches {
                        let (Some(eids), Some(versions)) = (
                            batch
                                .column_by_name("_eid")
                                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                            batch
                                .column_by_name("_version")
                                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
                        ) else {
                            continue;
                        };
                        let deleted = batch
                            .column_by_name("_deleted")
                            .and_then(|c| c.as_any().downcast_ref::<arrow_array::BooleanArray>());
                        let blobs = batch.column_by_name("props_json").and_then(|c| {
                            c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>()
                        });
                        for i in 0..batch.num_rows() {
                            let (eid, version) = (eids.value(i), versions.value(i));
                            if latest.get(&eid).is_some_and(|(v, _, _)| version < *v) {
                                continue;
                            }
                            let gone = deleted.is_some_and(|d| d.is_valid(i) && d.value(i));
                            let blob = blobs
                                .filter(|b| b.is_valid(i) && !b.value(i).is_empty())
                                .map(|b| b.value(i).to_vec());
                            latest.insert(eid, (version, gone, blob));
                        }
                    }
                    for (eid, (version, gone, blob)) in latest {
                        let Some(blob) = blob.filter(|_| !gone) else {
                            continue;
                        };
                        let props = match uni_common::cypher_value_codec::decode(&blob)
                            .map_err(|e| anyhow!("corrupt props_json of edge {eid}: {e}"))?
                        {
                            uni_common::Value::Map(m) => m,
                            _ => continue,
                        };
                        if let Some(props) = convert(&props)? {
                            rows.push((eid, version, encode(props)));
                        }
                    }
                }
                (table, "_eid", rows)
            }
        };
        if rows.is_empty() {
            return Ok(0);
        }
        let count = rows.len();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new(id_column, arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new("_version", arrow_schema::DataType::UInt64, false),
            arrow_schema::Field::new("props_json", arrow_schema::DataType::LargeBinary, true),
        ]));
        let mut ids = Vec::with_capacity(count);
        let mut versions = Vec::with_capacity(count);
        let mut blobs = arrow_array::builder::LargeBinaryBuilder::new();
        for (id, version, blob) in rows {
            ids.push(id);
            versions.push(version);
            blobs.append_value(blob);
        }
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(UInt64Array::from(versions)),
                Arc::new(blobs.finish()),
            ],
        )?;
        merge_insert_batch_with_lance_conflict_retry(
            backend,
            table,
            batch,
            &[id_column, "_version"],
        )
        .await?;
        tracing::info!(
            entity,
            rows = count,
            "converted main-table values of newly declared properties"
        );
        Ok(count)
    }

    /// [`Self::widen_for_declared_properties`] without its value check, for a
    /// caller that has already run [`Self::check_declared_values`] for every
    /// entity in a batch, so that one refusal leaves the whole batch's storage
    /// untouched.
    ///
    /// # Errors
    ///
    /// Propagates widening and promotion failures.
    pub async fn widen_for_checked_properties(
        &self,
        entity: &str,
        kind: DeclaredEntity,
        properties: &[(String, uni_common::DataType)],
    ) -> anyhow::Result<()> {
        if properties.is_empty() {
            return Ok(());
        }
        // The main tables first: values there are only rewritten into the
        // declared form, which is a valid schemaless value too, so this is
        // safe ahead of the catalog.
        self.convert_main_table_values(entity, kind, properties)
            .await?;
        let mut tables = self.declaration_tables(entity, kind);
        if self.property_tables_for(entity).is_empty() {
            // An undeclared edge type's delta tables exist only if its
            // relationships were ever flushed.
            let mut existing = Vec::with_capacity(tables.len());
            for table in tables {
                if self.backend().table_exists(&table).await? {
                    existing.push(table);
                }
            }
            tables = existing;
        }
        if tables.is_empty() {
            return Ok(());
        }
        let fields: Vec<arrow_schema::Field> = properties
            .iter()
            .map(|(name, ty)| arrow_schema::Field::new(name, ty.to_arrow(), true))
            .collect();
        let fields = fields.as_slice();

        let backend = self.backend();
        for table in &tables {
            backend.evolve_table_schema(table, fields, &[]).await?;
        }

        // Promote any values the property already has as *schemaless* data.
        // Widening alone would leave the typed column all-NULL while the blob
        // still held the values, and the two read paths would disagree --
        // trading #249's loud failure for a silent wrong answer. See
        // `backfill_property_from_overflow`.
        //
        // Primary only. On a fork-scoped manager the backend is a
        // `BranchedBackend`, whose `replace_table_atomic` rewrites the branch
        // tip -- and because a branch scan *fuses* with its parent, that would
        // materialize every inherited row into the branch, destroying the
        // copy-on-write sharing and corrupting anything that computes a fork's
        // own delta.
        if self.fork_scope().is_none() {
            for (name, declared) in properties {
                for table in &tables {
                    crate::storage::schema_evolution::backfill_property_from_overflow(
                        backend, table, name, declared,
                    )
                    .await?;
                }
            }
        }

        // Propagate to live fork branches. `ForkInfo::datasets` is the
        // authoritative per-fork table -> branch map; a fork with no branch for
        // a table needs nothing, because its reads resolve through `base_paths`
        // to primary, which now has the column. Cutting a branch just to hold
        // the column would be actively wrong -- it would pin a
        // pre-declaration parent version.
        let (Some(registry), Some(branching)) = (self.fork_registry(), backend.branching()) else {
            return Ok(());
        };
        for fork in registry.snapshot().await.forks.values() {
            if fork.status == uni_common::core::fork::ForkStatus::Tombstoned {
                continue;
            }
            for table in &tables {
                let Some(branch) = fork.datasets.get(table) else {
                    continue;
                };
                if let Err(e) = branching
                    .evolve_branch_schema(table, branch, fields, &[])
                    .await
                {
                    // A fork dropped between the snapshot and here takes its
                    // branch with it, which is benign; and a branch that still
                    // exists would be repaired by the fork's own next write.
                    // Surface it rather than swallowing it.
                    tracing::warn!(
                        fork = %fork.name,
                        table = %table,
                        branch = %branch,
                        error = %e,
                        "could not widen a fork branch for a newly declared property"
                    );
                }
            }
        }
        Ok(())
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
    /// `_version <= version_high_water_mark` are visible. Adjacency reads are
    /// bounded by it too, through [`Self::edge_visibility`]; adjacency
    /// WARMING must use [`Self::snapshot_version_hwm`] instead.
    pub fn version_high_water_mark(&self) -> Option<u64> {
        self.pinned_snapshot
            .as_ref()
            .map(|s| s.version_high_water_mark)
            .or(self.pinned_version_hwm)
    }

    /// Starts one semantic compaction merge (a label's vertices, or with
    /// `edge_tier` an edge type's deltas in one direction).
    ///
    /// The guard's [`CompactionMerge::floor`] is the oldest version an
    /// in-flight pinned reader reads at (a transaction's pin, or a session
    /// pinned to a snapshot read from the live tables; for edge deltas a
    /// transaction's edge pin, which sits below any bulk load it excludes):
    /// the merge must merge no row and fold no delta above it, so each such
    /// reader still finds its rows. Before the merge writes, [`CompactionMerge::record`] persists the
    /// version below which it loses history; a session pinned below that
    /// afterwards is refused ([`Self::history_is_whole`]).
    pub fn begin_compaction_merge(&self, edge_tier: bool) -> CompactionMerge {
        let pins = Arc::clone(self.adjacency_manager.pinned_versions());
        let (floor, token) = pins.begin_merge(edge_tier);
        CompactionMerge {
            pins,
            token: Some(token),
            floor,
            merged_through: 0,
        }
    }

    /// Whether this view's version ceiling was at or above every version
    /// semantic compaction had merged history through when the view was
    /// made. `false` means the live tables no longer hold the rows the view
    /// should read: a session pinned to an old flush manifest after a
    /// compaction pass. Always `true` for a live manager, a transaction view
    /// and a view at a restore point.
    #[must_use]
    pub fn history_is_whole(&self) -> bool {
        self.history_whole
    }

    /// Version high-water mark from a manifest-pinned (time-travel) snapshot
    /// ONLY — never from a transaction-level version pin.
    ///
    /// The ceiling adjacency is WARMED at. A time-travel snapshot is flushed
    /// state with its own fresh `AdjacencyManager`, so warming it filtered is
    /// right; a transaction pin shares the LIVE adjacency manager, and warming
    /// that filtered would poison the cache for every other reader. A
    /// transaction's adjacency reads are filtered instead
    /// ([`Self::edge_visibility`]).
    pub fn snapshot_version_hwm(&self) -> Option<u64> {
        self.pinned_snapshot
            .as_ref()
            .map(|s| s.version_high_water_mark)
    }

    /// What this view's adjacency reads may return, or `None` for a live
    /// view, which sees every committed edge.
    ///
    /// A pinned view — a time-travel snapshot or a transaction — sees the
    /// edges created at or below its ceiling and not deleted by then, minus
    /// the bulk loads it excludes. The in-flight bulk gate is not consulted:
    /// a load reserved at or below the ceiling and unpublished when the view
    /// was made is already excluded, and any later one is above the ceiling.
    #[must_use]
    pub fn edge_visibility(&self) -> Option<crate::storage::bulk_gate::RowVisibility> {
        self.version_high_water_mark().map(|hwm| {
            crate::storage::bulk_gate::RowVisibility::new(
                Some(hwm),
                Arc::clone(&self.excluded_versions),
            )
        })
    }

    /// The L1 visibility a pinned view's reads must carry into the shared
    /// property manager ([`crate::runtime::context::QueryContext::l1_visibility`]),
    /// or `None` for a live view.
    #[must_use]
    pub fn pinned_row_visibility(&self) -> Option<crate::storage::bulk_gate::RowVisibility> {
        self.version_high_water_mark()
            .map(|_| self.row_visibility())
    }

    /// The committed neighbours of `vid` this view sees: the adjacency
    /// manager's, filtered by [`Self::edge_visibility`] when the view is
    /// pinned.
    ///
    /// Excludes a transaction's own uncommitted edges, which live in its
    /// private L0 buffer; callers holding one overlay it on top.
    pub fn neighbors(
        &self,
        vid: uni_common::core::id::Vid,
        edge_type: u32,
        direction: crate::storage::direction::Direction,
    ) -> Vec<(uni_common::core::id::Vid, uni_common::core::id::Eid)> {
        match self.edge_visibility() {
            Some(visibility) => {
                self.adjacency_manager
                    .get_neighbors_visible(vid, edge_type, direction, &visibility)
            }
            None => self
                .adjacency_manager
                .get_neighbors(vid, edge_type, direction),
        }
    }

    /// Apply version filtering to a base filter expression.
    ///
    /// If a snapshot is pinned, conjoins `_version <= hwm` onto `base_filter`.
    /// Otherwise returns `base_filter` unchanged.
    ///
    /// Also excludes the rows of bulk loads whose publish is in flight; see
    /// [`Self::row_visibility`].
    pub fn apply_version_filter(&self, base_filter: FilterExpr) -> FilterExpr {
        self.row_visibility().bound(base_filter)
    }

    /// The gate that hides in-flight bulk publishes from this manager's reads.
    #[must_use]
    pub fn bulk_gate(&self) -> &Arc<crate::storage::bulk_gate::BulkVisibilityGate> {
        &self.bulk_gate
    }

    /// The rows an L1 read at ceiling `hwm` may return: at most `hwm`, and none
    /// of a bulk load whose publish is in flight.
    #[must_use]
    pub fn visibility_at(&self, hwm: Option<u64>) -> crate::storage::bulk_gate::RowVisibility {
        crate::storage::bulk_gate::RowVisibility::new(
            hwm,
            self.with_excluded_versions(self.bulk_gate.hidden()),
        )
    }

    /// `hidden` (the bulk gate's hidden versions) together with the versions
    /// this view excludes for good.
    #[must_use]
    pub fn with_excluded_versions(&self, hidden: Arc<[u64]>) -> Arc<[u64]> {
        if self.excluded_versions.is_empty() {
            return hidden;
        }
        let mut all: Vec<u64> = hidden
            .iter()
            .chain(self.excluded_versions.iter())
            .copied()
            .collect();
        all.sort_unstable();
        all.dedup();
        Arc::from(all)
    }

    /// Row visibility for the scan tier: [`Self::version_high_water_mark`]
    /// plus the bulk visibility gate.
    #[must_use]
    pub fn row_visibility(&self) -> crate::storage::bulk_gate::RowVisibility {
        self.visibility_at(self.version_high_water_mark())
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

    /// Runs a full compaction pass now: semantic compaction (Tier 2) and a
    /// backend optimize of every table (Tier 3), exactly what a background
    /// pass does.
    ///
    /// Waits for a pass already running in this handle or in another handle on
    /// the same directory, so a manual and a background pass never overlap,
    /// then runs under the same guard and directory lock. Like a background
    /// pass it lowers the flush-generation count (`l1_runs`) that drives write
    /// throttling, by the runs that existed when it started.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionCancelled`] if the database shuts down while it
    /// waits or runs, a lock-file I/O error, or a Tier 3 failure.
    pub async fn compact(self: &Arc<Self>) -> Result<CompactionStats> {
        let _guard = loop {
            if let Some(guard) = CompactionGuard::new(self.compaction_status.clone()) {
                break guard;
            }
            if self.compaction_cancel.is_cancelled() {
                return Err(anyhow::Error::new(CompactionCancelled {
                    at: "compaction::waiting-for-guard",
                }));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let _dir_lock = self.lock_compaction_dir().await?;
        Self::run_full_pass(Arc::clone(self)).await
    }

    /// Wait until no compaction runs in this handle or another handle on the
    /// same directory, and keep any from starting until the returned value is
    /// dropped.
    ///
    /// A snapshot restore rewinds tables a compaction would otherwise rewrite
    /// concurrently.
    ///
    /// # Errors
    ///
    /// [`CompactionCancelled`] if the database shuts down while waiting, or a
    /// lock-file I/O error.
    pub async fn pause_compaction(&self) -> Result<CompactionPause> {
        let guard = loop {
            if let Some(guard) = CompactionGuard::new(self.compaction_status.clone()) {
                break guard;
            }
            if self.compaction_cancel.is_cancelled() {
                return Err(anyhow::Error::new(CompactionCancelled {
                    at: "pause-compaction::waiting-for-guard",
                }));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let dir_lock = self.lock_compaction_dir().await?;
        Ok(CompactionPause {
            _guard: guard,
            _dir_lock: dir_lock,
        })
    }

    /// Keep background index builds from starting, and wait for those under
    /// way to finish, until the returned value is dropped.
    ///
    /// A snapshot restore holds it while it rewinds storage and the catalog,
    /// and a snapshot capture while it records them. Without it, a build that
    /// read the store before the rewind could commit its artifact, or record
    /// its index `Online`, after it: an index over rows the restore removed,
    /// or an `Online` index whose artifact the rewind took away.
    pub async fn pause_index_builds(&self) -> IndexBuildPause {
        IndexBuildPause {
            _guard: Arc::clone(&self.index_build_gate).write_owned().await,
        }
    }

    /// Permission for one background index build, held from the moment it
    /// claims its work until it has recorded the outcome in the catalog.
    /// Waits while [`Self::pause_index_builds`] is held.
    pub async fn index_build_permit(&self) -> IndexBuildPermit {
        IndexBuildPermit {
            _guard: Arc::clone(&self.index_build_gate).read_owned().await,
        }
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

    /// Cancels compaction on this manager: a running pass stops at its next
    /// safe point and no new pass starts.
    ///
    /// Irreversible. Called when the owning database shuts down, so a pass
    /// does not keep rewriting tables after its owner is gone. A stopped pass
    /// leaves the store consistent; the next open redoes its work.
    pub fn cancel_compaction(&self) {
        self.compaction_cancel.cancel();
    }

    /// The token that [`Self::cancel_compaction`] cancels.
    #[must_use]
    pub fn compaction_cancel_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.compaction_cancel
    }

    /// Waits for this directory's compaction lock, then takes it.
    ///
    /// Returns `Ok(None)` for a remote store, which has no lock.
    ///
    /// # Errors
    ///
    /// Returns [`CompactionCancelled`] if cancelled while waiting, or the
    /// lock file's I/O error.
    pub(crate) async fn lock_compaction_dir(&self) -> Result<Option<CompactionDirLock>> {
        let Some(root) = self.local_fs_root() else {
            return Ok(None);
        };
        loop {
            if self.compaction_cancel.is_cancelled() {
                return Err(anyhow::Error::new(CompactionCancelled {
                    at: "compaction::waiting-for-dir-lock",
                }));
            }
            if let Some(lock) = CompactionDirLock::try_acquire(&root)? {
                return Ok(Some(lock));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until no other handle is compacting this directory.
    ///
    /// A writable open calls this before it writes anything: a pass still
    /// running from a dropped handle on the same path would otherwise
    /// overwrite a table this handle appends to and lose the append. Takes
    /// the lock and releases it at once; a no-op for remote stores.
    ///
    /// # Errors
    ///
    /// Fails if the lock is still held after `timeout`, or on the lock
    /// file's I/O error.
    pub async fn wait_for_foreign_compaction(&self, timeout: Duration) -> Result<()> {
        let Some(root) = self.local_fs_root() else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if CompactionDirLock::try_acquire(&root)?.is_some() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "another handle is still compacting {} after {:?}; refusing to \
                     open it for writing while a foreign compaction can overwrite \
                     this handle's writes",
                    root.display(),
                    timeout
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
            // A pass can outlast many ticks; replaying them as a burst would
            // only re-check status back to back.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                // `biased`, shutdown first: unbiased, a tick that became ready
                // during a long pass won half the races against a pending
                // shutdown and started another full pass after it.
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => {
                        log::info!("Background compaction shutting down");
                        let _ = self.wait_for_compaction().await;
                        break;
                    }
                    () = self.compaction_cancel.cancelled() => {
                        log::info!("Background compaction cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = self.update_compaction_status().await {
                            log::error!("Failed to update compaction status: {}", e);
                            continue;
                        }

                        if let Some(task) = self.pick_compaction_task() {
                            log::info!("Triggering background compaction: {:?}", task);
                            match Self::execute_compaction(Arc::clone(&self), task).await {
                                Ok(_) => {}
                                Err(e) if is_compaction_cancelled(&e) => {
                                    log::info!("{e}");
                                }
                                Err(e) => log::error!("Compaction failed: {}", e),
                            }
                        }
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
        // Note: l1_runs is managed by flush_to_l1 (increment) and a full
        // compaction pass (`run_full_pass`, which subtracts the runs it saw at
        // its start). It counts flush generations, not delta table count.
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

    pub(crate) async fn execute_compaction(
        this: Arc<Self>,
        _task: CompactionTask,
    ) -> Result<CompactionStats> {
        let _guard = CompactionGuard::new(this.compaction_status.clone())
            .ok_or_else(|| anyhow!("Compaction already in progress"))?;
        if this.compaction_cancel.is_cancelled() {
            return Err(anyhow::Error::new(CompactionCancelled {
                at: "compaction::before-start",
            }));
        }
        // Skip, rather than wait, when a bulk load is being published: its
        // rows are hidden from readers, and semantic compaction would expose
        // them through L2 adjacency. The next tick retries.
        if this.bulk_gate.is_closed() {
            log::debug!("Background compaction deferred: a bulk load is being published");
            return Ok(CompactionStats::default());
        }
        // Skip, rather than wait, when another handle on this directory is
        // compacting: the next tick retries. See `CompactionDirLock`.
        let _dir_lock = match this.local_fs_root() {
            Some(root) => Some(CompactionDirLock::try_acquire(&root)?.ok_or_else(|| {
                anyhow!("Compaction already in progress in another handle on this directory")
            })?),
            None => None,
        };
        Self::run_full_pass(this).await
    }

    /// One full pass (Tier 2 then Tier 3), for a caller holding the
    /// compaction guard and the directory lock.
    ///
    /// Lowers `l1_runs` by the runs counted when the pass started, not to
    /// zero: a flush that lands while the pass runs adds a run the pass did
    /// not fold, and resetting to zero erased it from the write throttle.
    async fn run_full_pass(this: Arc<Self>) -> Result<CompactionStats> {
        let start = std::time::Instant::now();
        let runs_at_start = acquire_mutex(&this.compaction_status, "compaction_status")?.l1_runs;

        let schema = this.schema_manager.schema();
        let mut stats = CompactionStats::default();

        // ── Tier 2: Semantic compaction ──
        // Dedup vertices, merge CRDTs, consolidate L1→L2 deltas, clean tombstones
        let compactor = Compactor::new(Arc::clone(&this));
        // On failure the default report leaves `semantic_passes` at zero, which
        // correctly reads as "no CRDT merge count was measured" rather than
        // "no CRDT merges happened".
        //
        // The report also drives the CSR re-warm below, and *that* cannot absorb
        // a failure the same way: a pass that failed part way has already
        // rewritten an unknown subset of the adjacency tables and names none of
        // them, so re-warming only what it reports leaves the rest stale and a
        // traversal reads the old edges (#233). Continue with backend optimize as
        // before -- a persistent semantic failure must not block fragment
        // compaction -- but drop every cached CSR, because what changed is
        // exactly what is not known.
        let semantic = match compactor.compact_all_locked().await {
            Ok(report) => report,
            Err(e) if is_compaction_cancelled(&e) => {
                // Stopped at a safe point. Some adjacency tables may already
                // be rewritten, and the report naming them was not returned.
                this.adjacency_manager().invalidate_all_csr();
                return Err(e);
            }
            Err(e) => {
                log::error!(
                    "Semantic compaction failed (continuing with backend optimize; \
                     invalidating every cached adjacency CSR): {}",
                    e
                );
                this.adjacency_manager().invalidate_all_csr();
                SemanticCompactionReport::default()
            }
        };
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
            // Each optimize is one atomic Lance commit, so stopping between
            // tables is safe.
            if this.compaction_cancel.is_cancelled() {
                return Err(anyhow::Error::new(CompactionCancelled {
                    at: "compaction::before-optimize",
                }));
            }
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
            status.l1_runs = status.l1_runs.saturating_sub(runs_at_start);
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
        let index = self.load_vid_labels_index().await?;
        self.vid_labels_index = Arc::new(parking_lot::RwLock::new(index));
        Ok(())
    }

    /// Replace the VID-to-labels index, in place, with one read from the main
    /// vertex table.
    ///
    /// For a snapshot restore, which rewinds that table under a live handle:
    /// in place, so every holder of the index sees the restored labels.
    ///
    /// # Errors
    ///
    /// When the main vertex table cannot be read; the index is then unchanged.
    pub async fn reload_vid_labels_index(&self) -> Result<()> {
        let index = self.load_vid_labels_index().await?;
        *self.vid_labels_index.write() = index;
        Ok(())
    }

    /// Bring this pinned view's VID-to-labels index back to its version.
    ///
    /// A pinned view starts from a copy of the live index, which holds the
    /// labels of the newest flushed rows. Only the vertices with a row above
    /// the pin can differ; `live` (the manager the view was derived from)
    /// names them, and they are read again through this view: a vertex
    /// relabelled since gets its old labels back, one deleted since is put
    /// back, and one created since is removed. When most of the table
    /// changed, the index is rebuilt through this view instead. A no-op on a
    /// view without a ceiling.
    ///
    /// # Errors
    ///
    /// When the main vertex table cannot be read; the index is then unchanged.
    pub async fn rewind_vid_labels_index_to_pin(&self, live: &StorageManager) -> Result<()> {
        /// Above this many changed vertices a rebuild is cheaper than the
        /// per-vertex re-read.
        const REREAD_LIMIT: usize = 100_000;
        let Some(hwm) = self.version_high_water_mark() else {
            return Ok(());
        };
        let backend = self.backend.as_ref();
        let vtable = table_names::main_vertex_table_name();
        if !backend.table_exists(vtable).await? {
            *self.vid_labels_index.write() = crate::storage::vid_labels::VidLabelsIndex::new();
            return Ok(());
        }
        if !live.backend.table_exists(vtable).await? {
            return Ok(());
        }
        // Read from the live table: a view at a restore point reads the
        // table as it was, which has no row above the pin.
        let changed_batches = live
            .backend
            .scan(
                ScanRequest::all(vtable)
                    .with_columns(vec!["_vid".to_string()])
                    .with_filter(FilterExpr::compare(
                        "_version",
                        crate::backend::types::CmpOp::Gt,
                        crate::backend::types::Scalar::UInt(hwm),
                    )),
            )
            .await
            .map_err(|e| anyhow!("Failed to query main vertex table: {}", e))?;
        let mut changed: HashSet<Vid> = HashSet::new();
        for batch in &changed_batches {
            let vids = batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow!("Missing _vid column"))?;
            changed.extend(vids.iter().flatten().map(Vid::from));
        }
        if changed.is_empty() {
            return Ok(());
        }
        if changed.len() > REREAD_LIMIT {
            return self.reload_vid_labels_index().await;
        }
        let changed: Vec<Vid> = changed.into_iter().collect();
        let mut at_pin: HashMap<Vid, Vec<String>> = HashMap::new();
        for chunk in changed.chunks(10_000) {
            at_pin.extend(
                MainVertexDataset::find_batch_labels_by_vids(backend, chunk, self.row_visibility())
                    .await?,
            );
        }
        let mut index = self.vid_labels_index.write();
        for vid in changed {
            match at_pin.remove(&vid) {
                Some(labels) => index.insert(vid, labels),
                None => index.remove_vid(vid),
            }
        }
        Ok(())
    }

    /// The VID-to-labels index as the main vertex table holds it.
    async fn load_vid_labels_index(&self) -> Result<crate::storage::vid_labels::VidLabelsIndex> {
        use crate::storage::vid_labels::VidLabelsIndex;

        let backend = self.backend.as_ref();
        let vtable = table_names::main_vertex_table_name();

        // Check if the table exists (fresh database)
        if !backend.table_exists(vtable).await? {
            return Ok(VidLabelsIndex::new());
        }

        // Scan every row and keep each vid's newest one, tombstones included:
        // a vertex's rows are appended per write (a relabel adds a row with
        // the new label set, a delete adds a `_deleted` row), so neither a
        // `_deleted = false` filter nor the physical row order says which
        // label set is current. Bounded by this manager's row visibility, so
        // a pinned view gets the labels as of its version.
        //
        // No limit, deliberately. This index is not a cache that may miss — a
        // traversal's target-label filter consults it and *keeps* the row when a
        // vid does not resolve (`df_graph/traverse.rs`, "trust storage-level
        // filtering"), so a truncated index silently disables label filtering
        // for every vertex it omits.
        //
        // It carried `.with_limit(100_000)`. At LDBC SF1 (~3.6M vertices) that
        // left everything past the first 100k unresolvable, and
        // `MATCH (p:Person)<-[:HAS_CREATOR]-(post:Post)` returned 3 055 774 rows
        // — Comments included — against the 1 003 605 the scan-anchored form
        // returns. No test caught it because every fixture in the repo is
        // smaller than the cap, so the truncation never fired. See issue #211.
        // Projected to the columns actually read below: `ScanRequest::all`
        // means `ColumnProjection::All`, and the backend materialises every
        // batch before returning, so without it the rebuild pulls each
        // vertex's whole row.
        let mut request = ScanRequest::all(vtable).with_columns(vec![
            "_vid".to_string(),
            "labels".to_string(),
            "_version".to_string(),
            "_deleted".to_string(),
        ]);
        if let Some(filter) = self.row_visibility().filter() {
            request = request.with_filter(filter);
        }
        let batches = backend
            .scan(request)
            .await
            .map_err(|e| anyhow!("Failed to query main vertex table: {}", e))?;

        let newest = newest_vertex_labels(&batches)?;
        let mut index = VidLabelsIndex::with_capacity(newest.len(), 0);
        for (vid, (_, labels)) in newest {
            if let Some(labels) = labels {
                index.insert(vid, labels);
            }
        }

        Ok(index)
    }

    /// Whether each VID carries at least one of `labels`, per the in-memory
    /// VID-to-labels index; `None` for a VID the index does not hold.
    ///
    /// One lock acquisition for the whole slice and no label copies, for a
    /// pass over millions of edge endpoints.
    pub fn vids_carry_any_label(&self, vids: &[Vid], labels: &[String]) -> Vec<Option<bool>> {
        let index = self.vid_labels_index.read();
        vids.iter()
            .map(|vid| {
                index
                    .get_labels(*vid)
                    .map(|have| have.iter().any(|l| labels.contains(l)))
            })
            .collect()
    }

    /// The `(src, dst)` of every edge of `type_name` live in storage (not L0)
    /// as of version `hwm`, bulk-hidden rows included, read without
    /// properties.
    ///
    /// # Errors
    ///
    /// Returns an error if the main edges table cannot be read.
    pub async fn find_edge_endpoints_by_type_at(
        &self,
        type_name: &str,
        hwm: u64,
    ) -> Result<Vec<(Vid, Vid)>> {
        MainEdgeDataset::find_visible_endpoints_by_type_name(
            self.backend(),
            type_name,
            &crate::storage::bulk_gate::RowVisibility::at_most(Some(hwm)),
        )
        .await
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
                    let edges = self.neighbors(vid, etype_id, dir);

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

    /// Rows stored for `label`, without reading any of them — or `None` when
    /// answering would mean reading them after all.
    ///
    /// Metadata-only on Lance: no predicate, so fragment counts answer it.
    /// Measured at 35-116 us against 10k-1M rows, flat in row count and
    /// growing only with fragments (`examples/count_rows_probe.rs`), which is
    /// what makes it affordable on the query path at all.
    ///
    /// Used to decide whether a scan is large enough to be worth walking in
    /// ranges (#214). The count is an upper bound on live rows, since MVCC
    /// keeps superseded versions, which is the right direction for that
    /// decision — it can only over-estimate, and over-estimating chunks a scan
    /// that did not need it rather than failing to bound one that did.
    ///
    /// # Why this can decline to answer
    ///
    /// `BranchedBackend::count_rows` has no branch-aware count in lancedb
    /// 0.27.1, so for a table the fork has written it *scans the branch and
    /// sums `num_rows`*. Sizing a forked scan that way would fully materialize
    /// the table in order to decide whether to avoid materializing it — the
    /// exact cost the caller is trying to bound, paid up front and
    /// unconditionally.
    ///
    /// So this returns `None` there rather than a number, and the caller falls
    /// back to the unchunked read. A fork keeps today's behaviour, which is a
    /// documented limit and not a regression; the bound arrives on forks when
    /// the backend can count a branch from metadata.
    ///
    /// # Errors
    ///
    /// Propagates backend failures. A table that does not exist reports zero.
    /// The shared cardinality cache, for invalidation hooks and tests (#260).
    #[must_use]
    pub fn cardinality(&self) -> &Arc<crate::storage::cardinality::CardinalityCache> {
        &self.cardinality
    }

    /// An upper bound on this table's live rows, readable without `await`
    /// (#260).
    ///
    /// `None` means "no answer", and callers must keep whatever behaviour they
    /// had without one — it is never to be read as zero. There are three ways to
    /// get it, and they are deliberately indistinguishable, because the caller's
    /// response to all three is the same:
    ///
    /// * nothing has counted this table since it was last invalidated;
    /// * this view is fork-scoped for the table, where `BranchedBackend::count_rows`
    ///   degrades to a real scan, so the cached primary count is not this
    ///   reader's answer;
    /// * this view is pinned, to a snapshot or a version, where the count must
    ///   match the version being read and the cache tracks the live tip.
    ///
    /// # It is an upper bound
    ///
    /// The flushed half comes from the cache and the L0 half is read live, so an
    /// unflushed write is never invisible. Their overlap is not knowable
    /// synchronously: a vertex updated in place is counted twice, and an L0
    /// tombstone over a flushed row is not subtracted. See the module docs for
    /// why that direction is the safe one for the decisions this feeds.
    ///
    /// Pass `l0` to include unflushed rows. Passing `None` asks for the flushed
    /// count alone, which is exact but can badly understate a young table.
    #[must_use]
    pub fn cached_row_count(
        &self,
        key: &crate::storage::cardinality::CardinalityKey,
        l0: Option<&crate::runtime::l0::L0Buffer>,
    ) -> Option<u64> {
        use crate::storage::cardinality::CardinalityKey;

        if self.pinned_snapshot.is_some() || self.pinned_version_hwm.is_some() {
            return None;
        }
        let table_name = match key {
            CardinalityKey::Vertex(label) => table_names::vertex_table_name(label),
            CardinalityKey::EdgeType(_) => table_names::main_edge_table_name().to_string(),
        };
        let flushed = if let Some(scope) = self.fork_scope() {
            if scope.branch_for(&table_name).is_some() {
                return None;
            }
            // Unmapped tables are absent in this fork. Its buffered rows
            // still contribute below, independently of primary's cache.
            0
        } else {
            self.cardinality.get(key)?
        };
        let Some(l0) = l0 else {
            return Some(flushed);
        };

        let pending = match key {
            CardinalityKey::Vertex(label) => {
                l0.label_to_vids.get(label).map_or(0, HashSet::len) as u64
            }
            // O(L0 edges) rather than an index lookup: L0 keys edges by eid and
            // carries the type beside it, with no per-type index to ask. The
            // walk is bounded by `auto_flush_threshold` (10 000 by default) and
            // is paid once per decision, against a scan this exists to avoid.
            CardinalityKey::EdgeType(etype) => {
                l0.edge_types.values().filter(|t| *t == etype).count() as u64
            }
        };
        Some(flushed.saturating_add(pending))
    }

    /// Measure a table's flushed row count and cache it (#260).
    ///
    /// The `async` half that [`Self::cached_row_count`] exists to avoid. Returns
    /// `None` without caching anything when this view cannot answer for the
    /// primary — the same fork and pin conditions the sync reader declines on,
    /// so a fork's count can never be written into the shared cache and read
    /// back later as primary's. An unmapped fork table returns its known zero
    /// count without storing it in that shared cache.
    ///
    /// # Errors
    ///
    /// Returns an error if the count cannot be read.
    pub async fn refresh_row_count(
        &self,
        key: &crate::storage::cardinality::CardinalityKey,
    ) -> Result<Option<u64>> {
        use crate::backend::types::{FilterExpr, Scalar};
        use crate::storage::cardinality::CardinalityKey;

        if self.pinned_snapshot.is_some() || self.pinned_version_hwm.is_some() {
            return Ok(None);
        }
        let backend = self.backend();
        let (table_name, filter) = match key {
            CardinalityKey::Vertex(label) => (table_names::vertex_table_name(label), None),
            CardinalityKey::EdgeType(etype) => (
                table_names::main_edge_table_name().to_string(),
                Some(FilterExpr::equals("type", Scalar::Str(etype.clone()))),
            ),
        };
        if let Some(scope) = self.fork_scope() {
            // Never publish a fork's count into the shared primary cache,
            // including the empty view of an unmapped table.
            return Ok(scope.branch_for(&table_name).is_none().then_some(0));
        }
        if !backend.table_exists(&table_name).await? {
            self.cardinality.put(key.clone(), 0);
            return Ok(Some(0));
        }

        let rows = backend.count_rows(&table_name, filter.as_ref()).await? as u64;
        self.cardinality.put(key.clone(), rows);
        Ok(Some(rows))
    }

    pub async fn vertex_row_count(&self, label: &str) -> Result<Option<usize>> {
        self.table_row_count(&table_names::vertex_table_name(label))
            .await
    }

    /// Rows in the shared `vertices` table.
    ///
    /// [`Self::vertex_row_count`] answers this for one label's own table, which
    /// is what a labelled `Scan` reads. A `ScanAll` reads `vertices` instead
    /// (`GraphScanExec::new_schemaless_all_scan`), so sizing *its* range walk
    /// needs this count and not a per-label one. Without it the schemaless arm
    /// had no cheap size and was excluded from chunking altogether, so a
    /// full-graph scan built every vertex as a single batch — 711.8 MB reserved
    /// and ~2.2 GB resident at LDBC SF1 for a `MATCH (n)`.
    pub async fn main_vertex_row_count(&self) -> Result<Option<usize>> {
        self.table_row_count(table_names::main_vertex_table_name())
            .await
    }

    /// Rows in `table_name`, or `None` when no cheap count is available.
    async fn table_row_count(&self, table_name: &str) -> Result<Option<usize>> {
        let backend = self.backend();

        // Mirrors `BranchedBackend::count_rows`'s own branch test, so this
        // declines exactly when that would have scanned.
        if self
            .fork_scope()
            .and_then(|scope| scope.branch_for(table_name))
            .is_some()
        {
            return Ok(None);
        }

        if !backend.table_exists(table_name).await? {
            return Ok(Some(0));
        }
        backend.count_rows(table_name, None).await.map(Some)
    }

    /// Whether any row of `label` still has `_vid >= lo`.
    ///
    /// The termination test for a scan walking a label in `_vid` ranges (#214).
    /// A range walk has no upper bound to stop at: the id allocator is on the
    /// `Writer`, which a read-only query path does not hold, and `ScanRequest`
    /// has no ordering, so "the largest vid" cannot be read directly.
    ///
    /// Asking this instead is exact and is paid only when a range comes back
    /// empty — normally once, just past the end of the label. A sparse label
    /// pays once per gap, which is the case where being certain is worth a
    /// count.
    ///
    /// Honours the same `_version` ceiling as the scans it terminates, so a
    /// pinned session does not keep walking toward rows it cannot see.
    ///
    /// # Errors
    ///
    /// Propagates backend failures. A table that does not exist is not an
    /// error and reports no rows.
    pub async fn vertex_rows_at_or_above(&self, label: &str, lo: u64) -> Result<bool> {
        self.table_rows_at_or_above(&table_names::vertex_table_name(label), lo)
            .await
    }

    /// [`Self::vertex_rows_at_or_above`] over the shared `vertices` table, for
    /// a `ScanAll`'s range walk.
    pub async fn main_vertex_rows_at_or_above(&self, lo: u64) -> Result<bool> {
        self.table_rows_at_or_above(table_names::main_vertex_table_name(), lo)
            .await
    }

    async fn table_rows_at_or_above(&self, table_name: &str, lo: u64) -> Result<bool> {
        let backend = self.backend();
        if !backend.table_exists(table_name).await? {
            return Ok(false);
        }
        let at_or_above = FilterExpr::compare("_vid", CmpOp::GtEq, Scalar::UInt(lo));
        let filter = self
            .visibility_at(self.version_high_water_mark())
            .combine(Some(&at_or_above));
        Ok(backend.count_rows(table_name, filter.as_ref()).await? > 0)
    }

    /// The smallest `_vid` at or above `lo` that this label has a row for.
    ///
    /// [`Self::vertex_rows_at_or_above`] answers *whether* the label continues
    /// above a point; this answers *where*. The range walk in `GraphScanExec`
    /// needs the second question because vids are allocated globally while each
    /// label gets its own table, so a label's rows begin at an arbitrary offset
    /// — LDBC SF1 puts `Person` at vid 0, `Post` at 100 384 and `Comment` at
    /// 1 103 989. A walk that starts at 0 and doubles its range to find the
    /// first row arrives with the range already grown to ~1M vids wide, and so
    /// reads ~985 000 rows to answer `LIMIT 1` (#239).
    ///
    /// # Cost
    ///
    /// A binary search over `count_rows` with a half-open `_vid` predicate,
    /// bounded by `log2(hi - lo)` — at most 64 counts, and in practice ~20 for
    /// a realistic vid space. Each is a filtered count, which is *not*
    /// metadata-only (unlike an unfiltered one), so this is worth paying to
    /// avoid a large read and not worth paying speculatively. Callers should
    /// reach for it when they know a small bound makes the saved read large.
    ///
    /// Honours the same `_version` ceiling as the scans it steers, so a pinned
    /// session does not seek toward rows it cannot see. Note this deliberately
    /// consults flushed storage only: an L0-resident row at a lower vid is not
    /// seen, so the result is an upper bound on the true minimum. That is safe
    /// for steering a walk whose L0 overlay is applied per range anyway, but it
    /// is the reason this is not a general "minimum vid" accessor.
    ///
    /// Returns `None` when the label has no row at or above `lo`.
    ///
    /// # Errors
    ///
    /// Propagates backend failures. A missing table is not an error.
    pub async fn vertex_min_vid_at_or_above(&self, label: &str, lo: u64) -> Result<Option<u64>> {
        self.table_min_vid_at_or_above(&table_names::vertex_table_name(label), lo)
            .await
    }

    /// [`Self::vertex_min_vid_at_or_above`] over the shared `vertices` table,
    /// for a `ScanAll`'s range walk.
    pub async fn main_vertex_min_vid_at_or_above(&self, lo: u64) -> Result<Option<u64>> {
        self.table_min_vid_at_or_above(table_names::main_vertex_table_name(), lo)
            .await
    }

    async fn table_min_vid_at_or_above(&self, table_name: &str, lo: u64) -> Result<Option<u64>> {
        let backend = self.backend();
        let table_name = table_name.to_string();
        if !backend.table_exists(&table_name).await? {
            return Ok(None);
        }
        let hwm = self.version_high_water_mark();

        // Does anything remain at all? Also establishes the invariant the
        // search below relies on: `hi` always has a row at or above `lo`.
        let at_or_above = FilterExpr::compare("_vid", CmpOp::GtEq, Scalar::UInt(lo));
        let filter = self.visibility_at(hwm).combine(Some(&at_or_above));
        if backend.count_rows(&table_name, filter.as_ref()).await? == 0 {
            return Ok(None);
        }

        // Invariant: `[lo, hi)` is empty, `[lo, ..]` is not. Narrow until `hi`
        // is the first populated vid. Starting `hi` at `lo` and doubling keeps
        // the search proportional to the *distance to the data*, not to the
        // width of the vid space — a label that starts right at `lo` costs two
        // counts rather than sixty-four.
        let mut hi = lo;
        let mut step = 1u64;
        loop {
            let probe = hi.saturating_add(step);
            if probe == hi {
                // `hi` saturated at u64::MAX while still empty, which
                // contradicts the check above; treat as "no row".
                return Ok(None);
            }
            if self
                .vertex_rows_in_range(&table_name, hwm, lo, probe)
                .await?
            {
                break;
            }
            hi = probe;
            step = step.saturating_mul(2);
        }
        // `[lo, hi)` empty, `[lo, hi + step)` non-empty. Bisect for the first
        // vid, searching `[hi, hi + step)`.
        let mut low = hi;
        let mut high = hi.saturating_add(step);
        while low < high {
            let mid = low + (high - low) / 2;
            // `mid + 1` because the predicate is half-open and we are asking
            // "is there a row at or below `mid`".
            if self
                .vertex_rows_in_range(&table_name, hwm, lo, mid.saturating_add(1))
                .await?
            {
                high = mid;
            } else {
                low = mid.saturating_add(1);
            }
        }
        Ok(Some(low))
    }

    /// Whether the label's table holds a row with `lo <= _vid < hi`.
    ///
    /// Helper for [`Self::vertex_min_vid_at_or_above`]; the half-open shape
    /// matches the range walk's own so an off-by-one cannot creep in between
    /// the two.
    async fn vertex_rows_in_range(
        &self,
        table_name: &str,
        hwm: Option<u64>,
        lo: u64,
        hi: u64,
    ) -> Result<bool> {
        let backend = self.backend();
        let in_range = FilterExpr::all([
            FilterExpr::compare("_vid", CmpOp::GtEq, Scalar::UInt(lo)),
            FilterExpr::compare("_vid", CmpOp::Lt, Scalar::UInt(hi)),
        ]);
        let filter = self.visibility_at(hwm).combine(Some(&in_range));
        Ok(backend.count_rows(table_name, filter.as_ref()).await? > 0)
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
        let filter = self.visibility_at(hwm).combine(additional_filter);

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
        self.scan_delta_table_counted(edge_type, direction, columns, additional_filter, None)
            .await
    }

    /// [`Self::scan_delta_table`], carrying a query's counters into the backend.
    ///
    /// Edge scans were invisible to `idx_scans` and, more importantly, to
    /// `scans_reported` — the denominator that lets a zero in `idx_scans` be
    /// told apart from silence. See `docs/perf/index-scan-counter-2026-08-27.md`
    /// for what the counter does and does not observe; this closes the part of
    /// the gap that was purely an omission.
    pub async fn scan_delta_table_counted(
        &self,
        edge_type: &str,
        direction: &str,
        columns: &[&str],
        additional_filter: Option<&FilterExpr>,
        counters: Option<&Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        // Bounded by this view's ceiling, transaction pins included. A
        // transaction's own edges are never in L1 before it commits — they are
        // in its private L0, which readers overlay on top — so the ceiling
        // hides only what other transactions committed after it began.
        let edge_hwm = self.version_high_water_mark();
        let backend = self.backend();
        let table_name = table_names::delta_table_name(edge_type, direction);

        if !backend.table_exists(&table_name).await? {
            return Ok(None);
        }

        // Filter columns to those that exist
        let Some(actual_columns) = existing_columns(backend, &table_name, columns).await? else {
            return Ok(None);
        };

        // Counted where the ceiling is applied, as for vertex tables. Edge and
        // unlabelled scans of a pinned session read the snapshot but were never
        // counted, so a pinned query that touched no labelled vertex reported
        // `snapshot_reads: 0`.
        if self.snapshot_version_hwm().is_some()
            && let Some(c) = counters
        {
            c.add_snapshot_read();
        }
        let filter = self.visibility_at(edge_hwm).combine(additional_filter);

        let mut request = ScanRequest::all(&table_name)
            .with_columns(actual_columns)
            .with_counters(counters.cloned());
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
        self.scan_main_vertex_table_counted(columns, filter, None)
            .await
    }

    /// [`Self::scan_main_vertex_table`], carrying a query's counters.
    ///
    /// Same reason as `scan_delta_table_counted`: a scan the counter never sees
    /// is missing from `scans_reported` as well as `idx_scans`, and it is the
    /// denominator that makes the zero readable.
    pub async fn scan_main_vertex_table_counted(
        &self,
        columns: &[&str],
        filter: Option<&FilterExpr>,
        counters: Option<&Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Option<arrow_array::RecordBatch>> {
        let backend = self.backend();
        let table_name = table_names::main_vertex_table_name();

        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        // Combine caller filter with version HWM for snapshot isolation
        if self.snapshot_version_hwm().is_some()
            && let Some(c) = counters
        {
            c.add_snapshot_read();
        }
        let full_filter = self
            .visibility_at(self.version_high_water_mark())
            .combine(filter);

        let request = ScanRequest::all(table_name)
            .with_columns(columns.iter().map(|s| s.to_string()).collect())
            .with_counters(counters.cloned());
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
        if let Some(f) = self.visibility_at(None).combine(filter) {
            request = request.with_filter(f);
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

        let mut request = ScanRequest::all(&table_name);
        if let Some(f) = self.visibility_at(None).filter() {
            request = request.with_filter(f);
        }
        let stream = backend.scan_stream(request).await?;
        Ok(Some(stream))
    }

    /// Find a vertex VID by external ID. Uses pinned snapshot HWM if present.
    pub async fn find_vertex_by_ext_id(&self, ext_id: &str) -> Result<Option<Vid>> {
        MainVertexDataset::find_by_ext_id(self.backend(), ext_id, self.row_visibility()).await
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
    /// Covers flushed (Lance) rows visible to this view.
    ///
    /// The main vertex table is append-only: a deleted vertex keeps its older
    /// live rows beside the tombstone appended after them, and a vertex's
    /// `ext_id` can be cleared or move to another vertex. So each VID is judged
    /// by its newest row alone (a tombstone wins a tie), as
    /// `MainVertexDataset::find_by_ext_id` does. Reading every row whose
    /// `_deleted` is false instead returned deleted vertices too, and inverting
    /// the map for a reused `ext_id` could then pick the dead VID.
    pub async fn get_vertex_ext_ids(&self) -> Result<std::collections::HashMap<Vid, String>> {
        use arrow_array::{BooleanArray, StringArray};
        let backend = self.backend.as_ref();
        let vtable = table_names::main_vertex_table_name();
        let mut out = std::collections::HashMap::new();
        if !backend.table_exists(vtable).await? {
            return Ok(out);
        }
        let mut request = ScanRequest::all(vtable).with_columns(vec![
            "_vid".to_string(),
            "ext_id".to_string(),
            "_version".to_string(),
            "_deleted".to_string(),
        ]);
        if let Some(filter) = self.row_visibility().filter() {
            request = request.with_filter(filter);
        }
        let batches = backend
            .scan(request)
            .await
            .map_err(|e| anyhow!("get_vertex_ext_ids: {}", e))?;
        // Newest row per VID: (version, deleted, ext_id).
        let mut newest: std::collections::HashMap<Vid, (u64, bool, Option<String>)> =
            std::collections::HashMap::new();
        for batch in batches {
            let vids = batch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow!("get_vertex_ext_ids: missing/invalid _vid column"))?;
            let exts = batch
                .column_by_name("ext_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| anyhow!("get_vertex_ext_ids: missing/invalid ext_id column"))?;
            let versions = batch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| anyhow!("get_vertex_ext_ids: missing/invalid _version column"))?;
            let deleted = batch
                .column_by_name("_deleted")
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                .ok_or_else(|| anyhow!("get_vertex_ext_ids: missing/invalid _deleted column"))?;
            for i in 0..batch.num_rows() {
                let row = (
                    if versions.is_null(i) {
                        0
                    } else {
                        versions.value(i)
                    },
                    !deleted.is_null(i) && deleted.value(i),
                    (!exts.is_null(i)).then(|| exts.value(i).to_string()),
                );
                newest
                    .entry(Vid::from(vids.value(i)))
                    .and_modify(|best| {
                        if (row.0, row.1) > (best.0, best.1) {
                            *best = row.clone();
                        }
                    })
                    .or_insert(row);
            }
        }
        for (vid, (_, deleted, ext)) in newest {
            if let Some(ext) = ext
                && !deleted
                && !ext.is_empty()
            {
                out.insert(vid, ext);
            }
        }
        Ok(out)
    }

    /// Find labels for a vertex by VID. Uses pinned snapshot HWM if present.
    pub async fn find_vertex_labels_by_vid(&self, vid: Vid) -> Result<Option<Vec<String>>> {
        self.find_vertex_labels_by_vid_counted(vid, None).await
    }

    /// [`Self::find_vertex_labels_by_vid`], carrying a query's counters.
    pub async fn find_vertex_labels_by_vid_counted(
        &self,
        vid: Vid,
        counters: Option<&Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Option<Vec<String>>> {
        MainVertexDataset::find_labels_by_vid_counted(
            self.backend(),
            vid,
            self.row_visibility(),
            counters,
        )
        .await
    }

    /// Find edges from the main edge table by type names, optionally pushing
    /// a bounded endpoint vid set into the scan (review perf #5).
    pub async fn find_edges_by_type_names(
        &self,
        type_names: &[&str],
        endpoint_filter: Option<(crate::storage::main_edge::EndpointSide, &[Vid])>,
    ) -> Result<Vec<(Eid, Vid, Vid, String, uni_common::Properties)>> {
        self.find_edges_by_type_names_counted(type_names, endpoint_filter, None)
            .await
    }

    /// [`Self::find_edges_by_type_names`], carrying a query's counters.
    ///
    /// Same reason as `scan_delta_table_counted`: a scan the counter never sees
    /// is missing from `scans_reported` as well as `idx_scans`, and it is the
    /// denominator that makes the zero readable.
    pub async fn find_edges_by_type_names_counted(
        &self,
        type_names: &[&str],
        endpoint_filter: Option<(crate::storage::main_edge::EndpointSide, &[Vid])>,
        counters: Option<&Arc<crate::runtime::counters::QueryCounters>>,
    ) -> Result<Vec<(Eid, Vid, Vid, String, uni_common::Properties)>> {
        // Choose the read strategy on measured selectivity rather than on which
        // `match` arm the caller lands in (#237, using #260's statistic).
        if let Some((side, vids)) = endpoint_filter
            && self.endpoint_scan_beats_lookup(type_names, vids).await?
        {
            let all = MainEdgeDataset::find_visible_edges_by_type_names_counted(
                self.backend(),
                type_names,
                None,
                &self.row_visibility(),
                counters,
            )
            .await?;
            let wanted: HashSet<Vid> = vids.iter().copied().collect();
            return Ok(all
                .into_iter()
                .filter(|(_, src, dst, ..)| match side {
                    crate::storage::main_edge::EndpointSide::Src => wanted.contains(src),
                    crate::storage::main_edge::EndpointSide::Dst => wanted.contains(dst),
                    crate::storage::main_edge::EndpointSide::Either => {
                        wanted.contains(src) || wanted.contains(dst)
                    }
                })
                .collect());
        }

        // The view's visibility: its ceiling (a snapshot's or a transaction's)
        // and the bulk gate. Without the ceiling a pinned read of a schemaless
        // type lost every edge deleted after the pin (the tombstone outranked
        // the live row) and gained every edge created after it — the same
        // ceiling the CSR path reads at (see `neighbors`).
        MainEdgeDataset::find_visible_edges_by_type_names_counted(
            self.backend(),
            type_names,
            endpoint_filter,
            &self.row_visibility(),
            counters,
        )
        .await
    }

    /// Resolves edges by EID: stored endpoints and type, as `ctx` sees them.
    ///
    /// The L0 chain answers first (transaction, current, pending flush), via
    /// [`crate::runtime::l0_visibility::lookup_edges`]: a tombstone in any
    /// layer makes the edge absent, and a resident edge is taken from the
    /// newest layer holding it. Whatever L0 does not mention is read from the
    /// main edges table by `_eid`
    /// ([`MainEdgeDataset::find_edges_by_eids_counted`]) through this
    /// manager's backend — a fork's branch when this storage is forked — and
    /// bounded by [`Self::version_high_water_mark`] when it is pinned.
    ///
    /// An EID that resolves nowhere is absent from the result. Properties are
    /// not read; see `PropertyManager::get_batch_edge_props`. Each L0-resident
    /// edge counts as one L0 row scanned and each main-table row as a storage
    /// row, on `ctx`'s counters. Under a read-write transaction every
    /// requested EID joins the SSI read set.
    ///
    /// # Errors
    ///
    /// Returns an error if the main edges table cannot be read.
    pub async fn lookup_edges_by_eids(
        &self,
        eids: &[Eid],
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Eid, crate::storage::main_edge::EdgeRecord>> {
        let l0 = ctx
            .map(|c| crate::runtime::l0_visibility::lookup_edges(eids, c))
            .unwrap_or_default();
        let counters = ctx.and_then(|c| c.counters.as_ref());
        if let Some(c) = counters {
            c.add_l0_rows(l0.found.len());
            c.add_rows_scanned(l0.found.len());
        }

        let schema = self.schema_manager.schema();
        let mut resolved = HashMap::with_capacity(eids.len());
        let mut unresolved = Vec::new();
        for &eid in eids {
            if l0.deleted.contains(&eid) {
                continue;
            }
            // A buffer that recorded no type name still carries the type id;
            // only an id the schema cannot name falls through to storage.
            let from_l0 = l0.found.get(&eid).and_then(|edge| {
                edge.type_name
                    .clone()
                    .or_else(|| schema.edge_type_name_by_id_unified(edge.type_id))
                    .map(|edge_type| crate::storage::main_edge::EdgeRecord {
                        src_vid: edge.src_vid,
                        dst_vid: edge.dst_vid,
                        edge_type,
                    })
            });
            match from_l0 {
                Some(record) => {
                    resolved.insert(eid, record);
                }
                None => unresolved.push(eid),
            }
        }
        if !unresolved.is_empty() {
            resolved.extend(
                MainEdgeDataset::find_edges_by_eids_counted(
                    self.backend(),
                    &unresolved,
                    self.row_visibility(),
                    counters,
                )
                .await?,
            );
        }
        Ok(resolved)
    }

    /// Whether one pass over the edge type beats chunked endpoint lookups.
    ///
    /// The endpoint-vid arm used to chunk `src_vid IN (...)` unconditionally,
    /// with `prefers_full_scan` — the selectivity helper #221 added for the eid
    /// path — sitting in the same file and never consulted (#237).
    ///
    /// # Measured, not fitted by eye
    ///
    /// `examples/endpoint_arm_probe.rs` at LDBC SF1 over `HAS_MEMBER`
    /// (1 611 869 edges, 79 470 distinct src vids), min-of-3, release: the
    /// lookup arm is linear in the requested set (21 ms at K=100, 389 ms at
    /// 10 000, 2 485 ms at 79 000) and the scan arm is flat at 840 ms. They
    /// cross between K = 20 000 and K = 30 000. At full breadth the lookup this
    /// used to take unconditionally is **3.0x slower** than the scan.
    ///
    /// [`ENDPOINT_SCAN_CROSSOVER_RATIO`] is that crossover expressed against the
    /// type's row count, which is the denominator actually available here.
    ///
    /// # Why it declines rather than guesses
    ///
    /// `false` — keep chunked lookups — whenever the type's size is unknown:
    /// nothing cached and no cheap way to find out, a fork, or a pinned view.
    /// Lookup costs the request and scan costs the table, so the unknown case
    /// errs toward the arm whose cost the caller already controls.
    ///
    /// The count is only *refreshed* once the request is large enough for the
    /// answer to change the decision. A per-type count needs a predicate and a
    /// filtered count reads every row, so asking is itself a scan — worth it
    /// once, cached thereafter, and not worth it at all for a small request
    /// that was always going to take the lookup.
    async fn endpoint_scan_beats_lookup(&self, type_names: &[&str], vids: &[Vid]) -> Result<bool> {
        use crate::storage::cardinality::CardinalityKey;

        // A single-type request is the only shape with a meaningful denominator;
        // a multi-type one would need them summed, and no caller does that here.
        let [type_name] = type_names else {
            return Ok(false);
        };
        if vids.len() < MIN_ENDPOINT_VIDS_FOR_SCAN {
            return Ok(false);
        }

        let key = CardinalityKey::EdgeType((*type_name).to_string());
        let rows = match self.cached_row_count(&key, None) {
            Some(rows) => rows,
            None => match self.refresh_row_count(&key).await? {
                Some(rows) => rows,
                None => return Ok(false),
            },
        };

        Ok(rows > 0 && (vids.len() as u64).saturating_mul(ENDPOINT_SCAN_CROSSOVER_RATIO) >= rows)
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
            .with_filter(self.visibility_at(None).bound(full_filter))
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

        if self.is_schemaless_property(label, property) {
            reject_schemaless_filter(label, property, filter)?;
            let mut deadline = ctx.map(DeadlineTicker::new);
            for (vid, props) in self.flushed_label_properties(label, ctx).await? {
                if let Some(ticker) = deadline.as_mut() {
                    ticker.tick()?;
                }
                if let Some(emb) = extract_embedding_from_props(&props, property)
                    && emb.len() == query.len()
                {
                    results.push((vid, metric.compute_distance(&emb, query)));
                }
            }
        } else if backend.table_exists(&name).await? {
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
            let hwm = ctx.and(self.version_high_water_mark());
            if let Some(visible) = self.visibility_at(hwm).filter() {
                filter_parts.push(visible);
            }
            let combined_filter = FilterExpr::all(filter_parts);

            // L1/Manhattan is served exact/brute-force: fetch every candidate
            // (limit = full row count) and rank by the exact L1 re-score below.
            // O(N) — the inherent cost of exact L1, which cannot use an ANN index.
            let fetch_k = if matches!(metric, DistanceMetric::L1) {
                backend.count_rows(&name, None).await.unwrap_or(k).max(k)
            } else {
                // Each buffered VID can shadow at most one latest disk row.
                // Fetch enough unaffected neighbors even when every shadowed
                // row precedes them. This is a data-dependent bound, not a
                // fixed over-fetch factor.
                let mut shadowed = HashSet::new();
                if let Some(ctx) = ctx {
                    for buffer in l0_buffers_in_precedence(ctx) {
                        let buffer = buffer.read();
                        shadowed.extend(buffer.vertex_properties.iter().filter_map(
                            |(vid, props)| {
                                let relevant = buffer.vertex_labels.get(vid).is_none_or(|labels| {
                                    labels.is_empty() || labels.iter().any(|l| l == label)
                                });
                                (relevant && (props.contains_key(property) || filter.is_some()))
                                    .then_some(*vid)
                            },
                        ));
                        shadowed.extend(buffer.vertex_tombstones.iter().copied());
                        shadowed.extend(buffer.vertex_label_overwrites.iter().copied());
                    }
                }
                k.saturating_add(shadowed.len())
            };

            // Cap the *work* a defaulted refine may do, now that `fetch_k` is
            // known. Only `defaulted_refine` is clamped; a caller-supplied value
            // is left exactly as given.
            let mut opts = opts;
            if defaulted_refine && let Some(r) = opts.refine_factor {
                opts.refine_factor = Some(clamp_defaulted_refine(r, fetch_k));
            }

            // Hits on superseded row versions are dropped; only when that
            // happened and left the answer short is the search widened.
            let mut fetch = fetch_k;
            let batches = loop {
                let raw = backend
                    .vector_search(
                        &name,
                        property,
                        query,
                        fetch,
                        backend_metric,
                        combined_filter.clone(),
                        opts,
                        ctx.and_then(|c| c.counters.clone()),
                    )
                    .await?;
                let returned: usize = raw.iter().map(|b| b.num_rows()).sum();
                let (kept, dropped) = self.keep_latest_row_versions(&name, raw, hwm).await?;
                let kept_rows: usize = kept.iter().map(|b| b.num_rows()).sum();
                if dropped == 0 || kept_rows >= fetch_k || returned < fetch || fetch == usize::MAX {
                    break kept;
                }
                if let Some(ctx) = ctx {
                    ctx.check_timeout()?;
                }
                fetch = fetch.saturating_mul(2);
            };

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
            let excluded = self.l0_rows_failing_filter(label, filter, qctx).await?;
            merge_l0_into_vector_results(
                &mut results,
                qctx,
                label,
                property,
                query,
                k,
                &metric,
                &excluded,
            )?;
        }

        // Exact top-k. The L1 over-fetch (and the no-L0-activity early return in
        // the merge) can leave more than `k` candidates; rank by distance
        // ascending and truncate. Idempotent for the ANN paths, which already
        // return `k` sorted.
        results.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
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
        if let Some(visible) = self
            .visibility_at(ctx.and(self.version_high_water_mark()))
            .filter()
        {
            filter_parts.push(visible);
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
                ctx.and_then(|c| c.counters.clone()),
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
            if let Some(visible) = self
                .visibility_at(ctx.and(self.version_high_water_mark()))
                .filter()
            {
                filter_parts.push(visible);
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
            if let Some(visible) = self
                .visibility_at(ctx.and(self.version_high_water_mark()))
                .filter()
            {
                filter_parts.push(visible);
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
                    ctx.and_then(|c| c.counters.clone()),
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
    /// Rescale a sparse query's weights by inverse document frequency, when the
    /// index asks for it (#120).
    ///
    /// Returns the query unchanged when the modifier is off, when the index is
    /// absent, or when the corpus size cannot be read cheaply — an unknown `N`
    /// makes `idf` meaningless, and silently scoring by a guessed one would be
    /// worse than not scaling at all.
    ///
    /// Scaling happens **query-side**, once, before retrieval. That is what
    /// keeps the index's candidate generation and the caller's exact
    /// `sparse_dot` re-score consistent: both consume the same reweighted
    /// query, so neither can undo the other. Applying it inside the index alone
    /// is the trap the issue calls out.
    ///
    /// The form is BM25's smoothed idf, `ln(1 + (N - df + 0.5) / (df + 0.5))`,
    /// rather than a raw `ln(N / df)`. Raw idf goes **negative** for a term in
    /// more than half the corpus, which does not merely discount that term — it
    /// flips the sign of its contribution, so a document matching a common term
    /// scores *worse* than one matching nothing. The smoothed form stays
    /// positive over the whole range.
    ///
    /// # Errors
    ///
    /// Returns an error if the index exists but its posting lists cannot be
    /// read.
    pub async fn sparse_idf_scaled_query(
        &self,
        label: &str,
        property: &str,
        query: &[(u32, f32)],
    ) -> Result<Vec<(u32, f32)>> {
        let enabled = self
            .schema_manager()
            .schema()
            .sparse_index_for_property(label, property)
            .is_some_and(|cfg| cfg.idf_modifier);
        if !enabled || query.is_empty() {
            return Ok(query.to_vec());
        }

        let Some(total_docs) = self.vertex_row_count(label).await?.filter(|n| *n > 0) else {
            return Ok(query.to_vec());
        };
        let idx = match self
            .index_manager()
            .sparse_vector_index(label, property)
            .await
        {
            Ok(idx) => idx,
            Err(e) if crate::store_utils::is_dataset_not_found(&e) => return Ok(query.to_vec()),
            Err(e) => return Err(e),
        };

        let terms: Vec<u32> = query.iter().map(|(t, _)| *t).collect();
        let dfs = idx.document_frequencies(&terms).await?;
        let n = total_docs as f64;

        Ok(query
            .iter()
            .map(|(term, weight)| {
                // A term the index has never seen keeps its weight rather than
                // being scaled by a fabricated df. It contributes nothing to the
                // score anyway — no posting list means no document matches it.
                let Some(&df) = dfs.get(term) else {
                    return (*term, *weight);
                };
                let df = df as f64;
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                (*term, (f64::from(*weight) * idf) as f32)
            })
            .collect())
    }

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
                // No index for this label/property is an empty result; an index
                // that exists and cannot be opened is a failure, and returning
                // zero hits for it made a broken index look like a miss (#233).
                Err(e) if crate::store_utils::is_dataset_not_found(&e) => Ok(Vec::new()),
                Err(e) => Err(e),
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
        let schemaless = self.is_schemaless_property(label, property);
        if schemaless {
            reject_schemaless_filter(label, property, filter)?;
        }

        // The unflushed rows, with those failing `filter` set aside. They are
        // scored together with the flushed rows below, so that the ranking
        // does not move when they are flushed.
        let unflushed = match ctx {
            Some(qctx) => {
                let excluded = self.l0_rows_failing_filter(label, filter, qctx).await?;
                let l0 = collect_l0_texts(qctx, label, property)?;
                Some((excluded, l0))
            }
            None => None,
        };
        let docs: Vec<(u64, String)> = unflushed
            .as_ref()
            .map(|(excluded, l0)| {
                l0.texts
                    .iter()
                    .filter(|(vid, _)| !excluded.contains(vid))
                    .filter_map(|(vid, text)| text.as_ref().map(|t| (vid.as_u64(), t.clone())))
                    .collect()
            })
            .unwrap_or_default();

        // Schemaless rows have no text column in Lance. Assemble their visible
        // corpus before scoring so superseded/deleted rows do not affect BM25.
        if schemaless {
            let scoring_ctx = ctx.cloned();
            let check = move || {
                scoring_ctx
                    .as_ref()
                    .map_or(Ok(()), QueryContext::check_timeout)
            };
            check()?;
            let mut corpus: HashMap<Vid, String> = HashMap::new();
            for (vid, props) in self.flushed_label_properties(label, ctx).await? {
                check()?;
                if let Some(text) = props.get(property).and_then(|value| value.as_str()) {
                    corpus.insert(vid, text.to_string());
                }
            }
            if let Some((excluded, l0)) = &unflushed {
                for vid in l0.tombstoned.iter().chain(excluded) {
                    corpus.remove(vid);
                }
                for (vid, text) in &l0.texts {
                    check()?;
                    if l0.tombstoned.contains(vid) || excluded.contains(vid) {
                        continue;
                    }
                    if let Some(text) = text {
                        corpus.insert(*vid, text.clone());
                    } else {
                        corpus.remove(vid);
                    }
                }
            }
            let mut documents: Vec<_> = corpus
                .into_iter()
                .map(|(vid, text)| (vid.as_u64(), text))
                .collect();
            documents.sort_unstable_by_key(|(vid, _)| *vid);
            let scored = crate::backend::score_documents(property, query, documents, check).await?;
            let mut hits: Vec<_> = scored
                .into_iter()
                .map(|(vid, score)| (Vid::new(vid), score))
                .collect();
            hits.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            hits.truncate(k);
            return Ok(hits);
        }

        let mut scores: HashMap<Vid, f32> = HashMap::new();
        let mut results = if docs.is_empty() && !backend.table_exists(&name).await? {
            Vec::new()
        } else {
            // Build combined filter: _deleted = false + optional user filter + HWM
            let mut filter_parts = vec![Self::build_active_filter(filter)];
            let hwm = ctx.and(self.version_high_water_mark());
            if let Some(visible) = self.visibility_at(hwm).filter() {
                filter_parts.push(visible);
            }
            let combined_filter = FilterExpr::all(filter_parts);

            // Hits on superseded row versions are dropped; only when that
            // happened and left the answer short is the search widened.
            let mut fetch = k;
            let batches = loop {
                let counters = ctx.and_then(|c| c.counters.clone());
                let raw = if docs.is_empty() {
                    backend
                        .full_text_search(
                            &name,
                            property,
                            query,
                            fetch,
                            combined_filter.clone(),
                            counters,
                        )
                        .await?
                } else {
                    // Scored by the backend, on the scale, tokenizer and
                    // statistics its search will use once these rows are
                    // flushed, together with the flushed rows.
                    let searched = backend
                        .full_text_search_with_unstored(
                            &name,
                            property,
                            query,
                            fetch,
                            combined_filter.clone(),
                            docs.clone(),
                            counters,
                        )
                        .await?;
                    scores = searched
                        .unstored
                        .into_iter()
                        .map(|(vid, score)| (Vid::new(vid), score))
                        .collect();
                    searched.stored
                };
                let returned: usize = raw.iter().map(|b| b.num_rows()).sum();
                if returned == 0 {
                    break raw;
                }
                let (kept, dropped) = self.keep_latest_row_versions(&name, raw, hwm).await?;
                if dropped == 0 || returned < fetch || fetch >= k.saturating_mul(8) {
                    break kept;
                }
                fetch = fetch.saturating_mul(2);
            };

            let mut fts_results = extract_vid_score_pairs(&batches, "_vid", "_score")?;
            fts_results.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.0.cmp(&b.0))
            });
            fts_results
        };

        if let Some((excluded, l0)) = unflushed {
            merge_l0_text_scores(&mut results, l0, &scores, k, &excluded);
        }
        results.truncate(k);

        Ok(results)
    }

    /// Vids of `label`'s unflushed rows whose current values fail `filter`.
    ///
    /// A search's `filter` reaches only the backend scan of flushed rows. The
    /// unflushed rows merged in afterwards were never filtered: a search with
    /// `filter: 'id = 2'` returned every unflushed row, and a row whose
    /// flushed version matched kept matching after an unflushed update moved
    /// it out. The unflushed rows are laid out as the label's table and judged
    /// by the backend's own rules (`StorageBackend::filter_batch`), so a row
    /// passes before a flush exactly when it would after one.
    ///
    /// # Errors
    /// Propagates a filter the backend cannot evaluate, and read failures.
    async fn l0_rows_failing_filter(
        &self,
        label: &str,
        filter: Option<&str>,
        ctx: &QueryContext,
    ) -> Result<HashSet<Vid>> {
        let Some(filter) = filter else {
            return Ok(HashSet::new());
        };
        let schema = self.schema_manager.schema();
        let table = self.vertex_dataset(label)?.get_arrow_schema(&schema)?;
        // Every column nullable: an L0 row need not carry a column that the
        // table's writer fills in at flush. `_uid` is left out: it is derived
        // at flush and L0 has no value for it, and a filter on it fails to
        // plan here rather than being skipped.
        let table: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(
            table
                .fields()
                .iter()
                .filter(|f| f.name() != "_uid")
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        let l0_ctx = crate::runtime::l0_visibility::L0Context::from_query_context(ctx);
        let label_only = crate::runtime::columnar_scan::label_only_l0_base_props(
            self,
            &l0_ctx,
            label,
            |_| true,
            ctx.counters.as_ref(),
        )
        .await?;
        let batch = crate::runtime::columnar_scan::build_l0_vertex_batch(
            &l0_ctx,
            label,
            &table,
            schema.properties.get(label),
            crate::runtime::columnar_scan::L0VertexTargets::All,
            &label_only,
        )?;
        if batch.num_rows() == 0 {
            return Ok(HashSet::new());
        }
        let passes = self
            .backend
            .filter_batch(&batch, &FilterExpr::Raw(filter.to_string()))?;
        let vids = batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("unflushed rows have no _vid column"))?;
        Ok(passes
            .iter()
            .enumerate()
            .filter(|(_, pass)| !**pass)
            .map(|(row, _)| Vid::new(vids.value(row)))
            .collect())
    }

    /// Whether flushed values of `label.property` have no column of their own.
    ///
    /// Only a property the schema declares on `label` is flushed to a column
    /// named after it. An undeclared label is flushed to the main vertex table
    /// only, and an undeclared property of a declared label to the label's
    /// overflow column; either way the main table's `props_json` is the one
    /// flushed copy a search can read.
    fn is_schemaless_property(&self, label: &str, property: &str) -> bool {
        !self
            .schema_manager
            .schema()
            .properties
            .get(label)
            .is_some_and(|props| props.contains_key(property))
    }

    /// Latest flushed properties of every live `label` vertex.
    ///
    /// The flushed side of a search over a schemaless property, which has no
    /// column for a backend search to use, so every flushed row of the label
    /// is read and scored by the caller. Bounded by the same snapshot ceiling
    /// as the column searches. Unflushed rows are merged in afterwards by the
    /// L0 merges, exactly as for a declared property.
    ///
    /// # Errors
    /// Propagates backend scan and decode failures.
    async fn flushed_label_properties(
        &self,
        label: &str,
        ctx: Option<&QueryContext>,
    ) -> Result<HashMap<Vid, uni_common::Properties>> {
        MainVertexDataset::find_label_props_counted(
            self.backend.as_ref(),
            label,
            self.visibility_at(ctx.and(self.version_high_water_mark())),
            ctx.and_then(|c| c.counters.as_ref()),
        )
        .await
    }

    /// `batches` narrowed to rows that are their vertex's latest version.
    ///
    /// Vertex tables are append-only: an update writes a new row and leaves
    /// the old one, which a search index still matches. A search over them
    /// returned a node through text or a vector it no longer has. Each hit is
    /// kept only if its `_version` is the highest the vertex has, within the
    /// pinned snapshot's ceiling when `hwm` is set. Batches without `_version`
    /// are returned unchanged. Also returns how many hits were dropped.
    ///
    /// # Errors
    /// Propagates backend failures from the version lookup.
    async fn keep_latest_row_versions(
        &self,
        table: &str,
        batches: Vec<arrow_array::RecordBatch>,
        hwm: Option<u64>,
    ) -> Result<(Vec<arrow_array::RecordBatch>, usize)> {
        let before: usize = batches.iter().map(|b| b.num_rows()).sum();
        let columns = |b: &arrow_array::RecordBatch| {
            let vid = b
                .column_by_name("_vid")?
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .clone();
            let version = b
                .column_by_name("_version")?
                .as_any()
                .downcast_ref::<UInt64Array>()?
                .clone();
            Some((vid, version))
        };
        let hit_vids: HashSet<u64> = batches
            .iter()
            .filter_map(columns)
            .flat_map(|(vid, _)| vid.iter().flatten().collect::<Vec<_>>())
            .collect();
        if hit_vids.is_empty() {
            return Ok((batches, 0));
        }
        let mut parts = vec![FilterExpr::one_of(
            "_vid",
            hit_vids.iter().map(|v| Scalar::UInt(*v)),
        )];
        if let Some(visible) = self.visibility_at(hwm).filter() {
            parts.push(visible);
        }
        let rows = self
            .backend
            .scan(
                crate::backend::types::ScanRequest::all(table)
                    .with_columns(vec!["_vid".to_string(), "_version".to_string()])
                    .with_filter(FilterExpr::all(parts)),
            )
            .await?;
        let mut latest: HashMap<u64, u64> = HashMap::new();
        for (vid, version) in rows.iter().filter_map(columns) {
            for (vid, version) in vid.iter().zip(version.iter()) {
                if let (Some(vid), Some(version)) = (vid, version) {
                    let slot = latest.entry(vid).or_insert(version);
                    *slot = (*slot).max(version);
                }
            }
        }
        batches
            .into_iter()
            .map(|batch| {
                let Some((vid, version)) = columns(&batch) else {
                    return Ok(batch);
                };
                let keep: arrow_array::BooleanArray = vid
                    .iter()
                    .zip(version.iter())
                    .map(|(vid, version)| {
                        Some(match (vid, version) {
                            (Some(vid), Some(version)) => latest.get(&vid) == Some(&version),
                            _ => true,
                        })
                    })
                    .collect();
                Ok(arrow::compute::filter_record_batch(&batch, &keep)?)
            })
            .collect::<Result<Vec<_>>>()
            .map(|kept| {
                let after: usize = kept.iter().map(|b| b.num_rows()).sum();
                (kept, before - after)
            })
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

            // The whole hop's vertices, resolved before any storage read, so the
            // reads below can be issued once per (edge type, label) rather than
            // once per vertex per hop (#220). Marking them visited up front is
            // what makes that possible, and does not change the resulting graph:
            // `visited` gates only `next_frontier` insertion, never an edge, and
            // a neighbour suppressed by it is one this hop already covers — the
            // old code would have queued it and skipped it on arrival.
            let hop_vids: Vec<Vid> = {
                let mut seen = Vec::new();
                for &vid in &frontier {
                    if visited.insert(vid) {
                        graph.add_vertex(vid);
                        seen.push(vid);
                    }
                }
                seen
            };
            if hop_vids.is_empty() {
                break;
            }

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

                let mut per_vid: HashMap<Vid, HashMap<Eid, EdgeState>> = HashMap::new();

                // 1. L2: Adjacency (Base)
                // In the new storage model, VIDs don't embed label info.
                // We need to try all labels to find the adjacency data.
                // Edge version from snapshot (reserved for future version filtering)
                let _edge_ver = self
                    .pinned_snapshot
                    .as_ref()
                    .and_then(|s| s.edges.get(etype_name).map(|es| es.lance_version));

                // Try each label, carrying forward only the vertices no earlier
                // label answered for. This preserves the per-vertex `break` the
                // unbatched form used — the first label with data for a vertex
                // wins — while costing one scan per label instead of one per
                // (vertex, label).
                let backend = self.backend();
                let mut unresolved: Vec<Vid> = hop_vids.clone();
                for current_src_label in label_map.values() {
                    if unresolved.is_empty() {
                        break;
                    }
                    let adj_ds =
                        match self.adjacency_dataset(etype_name, current_src_label, dir_str) {
                            Ok(ds) => ds,
                            Err(_) => continue,
                        };
                    let mut found: HashMap<Vid, (Vec<Vid>, Vec<Eid>)> = HashMap::new();
                    for chunk in unresolved.chunks(MAX_SUBGRAPH_VIDS_PER_SCAN) {
                        found.extend(adj_ds.read_adjacency_backend_batch(backend, chunk).await?);
                    }
                    if found.is_empty() {
                        continue;
                    }
                    for (vid, (neighbors, eids)) in found {
                        let edges = per_vid.entry(vid).or_default();
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
                    }
                    unresolved.retain(|vid| !per_vid.contains_key(vid));
                }

                // 2. L1: Delta
                let delta_ds = self.delta_dataset(etype_name, dir_str)?;
                let mut delta_by_vid: HashMap<Vid, Vec<crate::storage::delta::L1Entry>> =
                    HashMap::new();
                for chunk in hop_vids.chunks(MAX_SUBGRAPH_VIDS_PER_SCAN) {
                    let part = delta_ds
                        .read_deltas_batch(backend, chunk, &schema, self.row_visibility())
                        .await?;
                    for (vid, entries) in part {
                        delta_by_vid.entry(vid).or_default().extend(entries);
                    }
                }

                for &vid in &hop_vids {
                    let mut edges = per_vid.remove(&vid).unwrap_or_default();
                    if let Some(entries) = delta_by_vid.remove(&vid) {
                        Self::apply_delta_to_edges(&mut edges, entries, neighbor_is_dst);
                    }

                    // 3. L0: Buffer
                    if let Some(l0) = l0 {
                        Self::apply_l0_to_edges(&mut edges, l0, vid, etype_id, direction);
                    }

                    if edges.is_empty() {
                        continue;
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
#[expect(clippy::too_many_arguments)]
fn merge_l0_into_vector_results(
    results: &mut Vec<(Vid, f32)>,
    ctx: &QueryContext,
    label: &str,
    property: &str,
    query: &[f32],
    k: usize,
    metric: &DistanceMetric,
    excluded: &HashSet<Vid>,
) -> Result<()> {
    let buffers = l0_buffers_in_precedence(ctx);
    let mut deadline = DeadlineTicker::new(ctx);

    // Maps VID → distance for L0 candidates (last writer wins).
    let mut l0_candidates: HashMap<Vid, f32> = HashMap::new();
    // Tombstoned VIDs across all L0 buffers.
    let mut tombstoned: HashSet<Vid> = HashSet::new();
    let mut label_membership = HashMap::new();

    for buf_arc in &buffers {
        let buf = buf_arc.read();

        // Accumulate tombstones.
        for &vid in &buf.vertex_tombstones {
            tombstoned.insert(vid);
        }

        for vid in &buf.vertex_label_overwrites {
            label_membership.insert(
                *vid,
                buf.vertex_labels
                    .get(vid)
                    .is_some_and(|labels| labels.iter().any(|l| l == label)),
            );
        }

        // Scan vertices with the target label.
        for (&vid, labels) in &buf.vertex_labels {
            deadline.tick()?;
            if buf.vertex_tombstones.contains(&vid) || !labels.iter().any(|l| l == label) {
                continue;
            }
            if let Some(props) = buf.vertex_properties.get(&vid)
                && props.contains_key(property)
                && extract_embedding_from_props(props, property).is_none()
            {
                l0_candidates.remove(&vid);
                tombstoned.insert(vid);
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

    // A vertex whose current (unflushed) row fails the search's filter is out,
    // however its flushed row scored: drop it exactly like a tombstone.
    tombstoned.extend(excluded.iter().copied());
    tombstoned.extend(
        label_membership
            .into_iter()
            .filter_map(|(vid, member)| (!member).then_some(vid)),
    );

    // If no L0 activity affects this search, skip merge.
    if l0_candidates.is_empty() && tombstoned.is_empty() {
        return Ok(());
    }

    // Remove tombstoned VIDs from LanceDB results.
    results.retain(|(vid, _)| !tombstoned.contains(vid));

    apply_l0_candidates(results, &l0_candidates, &tombstoned);

    // Re-sort by distance ascending.
    results.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    results.truncate(k);
    Ok(())
}

/// Overwrites or appends L0 candidates in `results`.
///
/// An L0 score replaces the flushed one for the same vid, and a vid only L0
/// knows is appended. A vid the precedence chain ultimately tombstoned is
/// skipped: it can be a live candidate from an earlier buffer while a later
/// buffer deleted it (that later buffer never revisits it in the label loop, so
/// it stays in the candidates), and appending it would resurrect a deleted
/// vertex into the results.
///
/// Positions are indexed once. Looking each candidate up by a linear scan of
/// `results`, which grows as candidates are appended, made a search over N
/// unflushed rows cost N^2/2 comparisons: 2.2 s for 40,000 rows.
fn apply_l0_candidates(
    results: &mut Vec<(Vid, f32)>,
    candidates: &HashMap<Vid, f32>,
    tombstoned: &HashSet<Vid>,
) {
    let mut position: HashMap<Vid, usize> = HashMap::with_capacity(results.len());
    for (i, (vid, _)) in results.iter().enumerate() {
        // The first occurrence, which is the one a linear search would find.
        position.entry(*vid).or_insert(i);
    }
    for (vid, score) in candidates {
        if tombstoned.contains(vid) {
            continue;
        }
        match position.get(vid) {
            Some(&i) => results[i].1 = *score,
            None => {
                position.insert(*vid, results.len());
                results.push((*vid, *score));
            }
        }
    }
}

/// Checks a query's deadline and cancellation every [`Self::EVERY`] rows.
///
/// The L0 merges score every unflushed row of a label in one synchronous span,
/// with no await for the caller's `tokio::time::timeout` to fire at, so a check
/// of their own is the only thing that can stop them.
struct DeadlineTicker<'a> {
    ctx: &'a QueryContext,
    rows: usize,
}

impl<'a> DeadlineTicker<'a> {
    const EVERY: usize = 1024;

    fn new(ctx: &'a QueryContext) -> Self {
        Self { ctx, rows: 0 }
    }

    fn tick(&mut self) -> Result<()> {
        self.rows += 1;
        if self.rows.is_multiple_of(Self::EVERY) {
            self.ctx.check_timeout()?;
        }
        Ok(())
    }
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

/// Refuses a backend filter on a search over a schemaless property.
///
/// The filter is an expression over the label table's columns, and the rows
/// such a search reads come from the main vertex table, which has none of
/// them. Ignoring it would return rows the caller excluded.
///
/// # Errors
/// Returns an error when `filter` is set.
fn reject_schemaless_filter(label: &str, property: &str, filter: Option<&str>) -> Result<()> {
    match filter {
        Some(expr) => Err(anyhow!(
            "a filter ('{expr}') is not supported when searching '{label}.{property}', \
             which the schema does not declare; declare the property to filter its search"
        )),
        None => Ok(()),
    }
}

/// Computes a simple token-overlap relevance score between a query and text.
///
/// Returns the fraction of query tokens found in the text (case-insensitive),
/// producing a score in [0.0, 1.0]. Sufficient for the small L0 buffer.
pub(crate) fn compute_text_relevance(query: &str, text: &str) -> f32 {
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

/// The unflushed side of a full-text search: each L0 vertex of the label
/// whose text L0 now determines, and the vids L0 tombstoned.
///
/// `texts` maps a vid to its current text, or to `None` when the property is
/// now NULL or not a string. Every such vertex is recorded, because its score
/// replaces the flushed hit's: recording only matching ones left a flushed hit
/// standing after an update removed the match. A partial update that does not
/// touch the property says nothing about it and is skipped.
#[derive(Debug, Default)]
struct L0Texts {
    texts: HashMap<Vid, Option<String>>,
    tombstoned: HashSet<Vid>,
}

/// Collects [`L0Texts`] across the L0 chain, later buffers winning.
///
/// # Errors
/// Returns the query's timeout or cancellation, checked every
/// [`DeadlineTicker::EVERY`] rows.
fn collect_l0_texts(ctx: &QueryContext, label: &str, property: &str) -> Result<L0Texts> {
    let mut deadline = DeadlineTicker::new(ctx);
    let mut out = L0Texts::default();
    for buf_arc in &l0_buffers_in_precedence(ctx) {
        let buf = buf_arc.read();
        out.tombstoned.extend(buf.vertex_tombstones.iter().copied());
        for (&vid, labels) in &buf.vertex_labels {
            deadline.tick()?;
            if !labels.iter().any(|l| l == label) {
                continue;
            }
            let Some(props) = buf.vertex_properties.get(&vid) else {
                continue;
            };
            let text = match props.get(property) {
                Some(value) => value.as_str().map(str::to_string),
                None => continue,
            };
            out.texts.insert(vid, text);
            // A re-creation in a later buffer clears an earlier tombstone.
            out.tombstoned.remove(&vid);
        }
    }
    Ok(out)
}

/// Merges scored L0 vertices into the flushed full-text hits.
///
/// Follows [`merge_l0_into_vector_results`]: a tombstoned or `excluded` vid is
/// removed, an L0 score replaces the flushed one, an L0-only vid is appended,
/// a vertex whose current text does not match (absent from `scores`) is
/// dropped, and the result is re-sorted descending and cut to `k`.
fn merge_l0_text_scores(
    results: &mut Vec<(Vid, f32)>,
    l0: L0Texts,
    scores: &HashMap<Vid, f32>,
    k: usize,
    excluded: &HashSet<Vid>,
) {
    let L0Texts {
        texts,
        mut tombstoned,
    } = l0;
    tombstoned.extend(excluded.iter().copied());
    if texts.is_empty() && tombstoned.is_empty() {
        return;
    }
    let candidates: HashMap<Vid, f32> = texts
        .keys()
        .map(|vid| (*vid, scores.get(vid).copied().unwrap_or(0.0)))
        .collect();
    results.retain(|(vid, _)| !tombstoned.contains(vid));
    apply_l0_candidates(results, &candidates, &tombstoned);
    results.retain(|(_, score)| *score > 0.0);
    results.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });
    results.truncate(k);
}

#[cfg(test)]
mod conflict_classification_tests {
    use super::is_lance_conflict;
    use super::{REFINE_CANDIDATE_CAP, clamp_defaulted_refine};

    /// A defaulted refine factor is left alone while the work stays under the cap.
    ///
    /// The bound is on *candidates re-scored* (`fetch_k * refine_factor`), not on
    /// the factor, so a small `k` may keep even the maximum default (32) intact.
    #[test]
    fn a_defaulted_refine_under_the_cap_is_not_clamped() {
        // 10 * 32 = 320 candidates, far under the 10_000 cap.
        assert_eq!(clamp_defaulted_refine(32, 10), 32);
        // Exactly at the cap: 100 * 100 == 10_000 is affordable, so it stands.
        assert_eq!(clamp_defaulted_refine(100, 100), 100);
    }

    /// Past the cap the factor is reduced to the affordable budget.
    #[test]
    fn a_defaulted_refine_over_the_cap_is_clamped_to_the_budget() {
        // fetch_k = 1000 affords 10_000/1000 = 10, below the minimum default 12.
        assert_eq!(clamp_defaulted_refine(12, 1000), 10);
        // The reduction tracks fetch_k rather than being a fixed ceiling.
        assert_eq!(clamp_defaulted_refine(32, 2000), 5);
    }

    /// The clamp never disables refine entirely, however large `fetch_k` grows.
    ///
    /// `refine_factor = 0` would mean "no refine pass"; the floor of 1 keeps the
    /// exact re-score that the ANN distance is not trusted without.
    #[test]
    fn the_clamp_floors_at_one_rather_than_disabling_refine() {
        assert_eq!(clamp_defaulted_refine(32, REFINE_CANDIDATE_CAP * 10), 1);
        assert_eq!(clamp_defaulted_refine(32, usize::MAX), 1);
    }

    /// `fetch_k = 0` must not divide by zero.
    #[test]
    fn a_zero_fetch_k_does_not_panic() {
        assert_eq!(clamp_defaulted_refine(12, 0), 12);
    }

    /// Clamping is counted, and only when the cap actually binds.
    ///
    /// The clamp silently trades recall for latency, so the counter is the only
    /// signal that it happened (#233). A metering bug is invisible by
    /// construction — nothing downstream reads the counter — so it is asserted
    /// here rather than assumed. Installs a `DebuggingRecorder`, which is
    /// process-global and one-shot, hence the delta-based read.
    #[test]
    fn clamping_is_counted_but_only_when_the_cap_binds() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Another test in this binary may already hold the global slot; in that
        // case this recorder observes nothing and the assertions below would
        // read 0, so run against a local recorder instead of the global one.
        metrics::with_local_recorder(&recorder, || {
            // Under the cap: no clamp, so nothing to report.
            assert_eq!(clamp_defaulted_refine(32, 10), 32);
            // Over the cap: one clamp.
            assert_eq!(clamp_defaulted_refine(12, 1000), 10);
            assert_eq!(clamp_defaulted_refine(32, 2000), 5);
        });

        let total: u64 = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(ckey, _, _, _)| ckey.key().name() == "uni_vector_refine_factor_clamped_total")
            .map(|(_, _, _, value)| match value {
                DebugValue::Counter(v) => v,
                _ => 0,
            })
            .sum();

        assert_eq!(
            total, 2,
            "expected exactly the two clamped calls to be counted, not the unclamped one"
        );
    }

    /// Concurrent table creation is retryable.
    ///
    /// `DatasetAlreadyExists` is what Lance raises when two writers race to
    /// create the same table -- it maps `CommitError::CommitConflict` onto it,
    /// so the name understates what it is -- and its message contains neither
    /// "conflict" nor "Incompatible transaction", so the message matcher
    /// classified it fatal and stranded a losing async flush's L0
    /// (`async_flush_repro` R2/R3).
    #[test]
    fn concurrent_creation_is_retryable() {
        let already_exists: anyhow::Error =
            lance::Error::dataset_already_exists("/tmp/x/index.lance").into();
        assert!(
            !already_exists.to_string().contains("conflict"),
            "precondition: if Lance ever adds 'conflict' to this message the \
             test stops covering the case it was written for"
        );
        assert!(is_lance_conflict(&already_exists));
    }

    // `TooMuchWriteContention` is the other variant the message matcher missed
    // ("Too many concurrent writers"), but its Snafu selector lives in
    // `lance_core::error`, which `lance` does not re-export -- constructing one
    // would mean a dev-dependency on `lance-core` for a single `matches!` arm.
    // It is covered by `stringified_conflicts_still_match_via_the_fallback`
    // instead, which pins the message; the typed arm is unexercised.

    /// A non-conflict Lance error must not be retried, and must not reach the
    /// string fallback -- which would match any message mentioning a conflict.
    #[test]
    fn unrelated_lance_errors_are_not_retryable() {
        let corrupt: anyhow::Error =
            lance::Error::io_source("disk quota exceeded".to_string().into()).into();
        assert!(!is_lance_conflict(&corrupt));

        let misleading: anyhow::Error =
            lance::Error::io_source("failed while resolving a conflict".to_string().into()).into();
        assert!(
            !is_lance_conflict(&misleading),
            "a typed non-conflict error must not fall through to message matching"
        );
    }

    /// Retained for errors that crossed a boundary erasing the concrete type.
    #[test]
    fn stringified_conflicts_still_match_via_the_fallback() {
        for msg in [
            "Incompatible transaction: ...",
            "Retryable commit conflict for version 3",
            "Dataset already exists: /tmp/x",
            "Too many concurrent writers. retry limit reached",
        ] {
            assert!(
                is_lance_conflict(&anyhow::anyhow!("{msg}")),
                "fallback missed: {msg}"
            );
        }
        assert!(!is_lance_conflict(&anyhow::anyhow!("schema mismatch")));
    }
}

#[cfg(test)]
mod l0_merge_tests {
    use std::time::{Duration, Instant};

    use super::*;
    use uni_common::Value;

    /// A context over one L0 buffer holding `n` `Doc` vertices.
    fn ctx_with_docs(n: u64) -> QueryContext {
        let mut buf = L0Buffer::new(0, None);
        let labels = vec!["Doc".to_string()];
        for i in 0..n {
            let mut props = uni_common::Properties::new();
            props.insert(
                "emb".to_string(),
                Value::List(vec![Value::Float(i as f64), Value::Float(1.0)]),
            );
            props.insert("body".to_string(), Value::String(format!("alpha {i}")));
            buf.insert_vertex_with_labels(Vid::new(i), props, &labels);
        }
        QueryContext::new(Arc::new(parking_lot::RwLock::new(buf)))
    }

    fn expired(mut ctx: QueryContext) -> QueryContext {
        let past = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("the clock is more than a second past its origin");
        ctx.set_deadline(past);
        ctx
    }

    /// Both merges score every unflushed row in one synchronous span, so
    /// nothing above them can stop it; they must check the deadline themselves.
    #[test]
    fn the_l0_merges_stop_at_an_expired_deadline() {
        let ctx = expired(ctx_with_docs(4096));
        let mut results = Vec::new();
        let err = merge_l0_into_vector_results(
            &mut results,
            &ctx,
            "Doc",
            "emb",
            &[0.0, 1.0],
            10,
            &DistanceMetric::L2,
            &HashSet::new(),
        )
        .expect_err("the deadline has passed");
        assert!(err.to_string().contains("timed out"), "{err}");

        let err = collect_l0_texts(&ctx, "Doc", "body").expect_err("the deadline has passed");
        assert!(err.to_string().contains("timed out"), "{err}");
    }

    #[test]
    fn the_l0_merges_stop_when_cancelled() {
        let mut ctx = ctx_with_docs(4096);
        let token = tokio_util::sync::CancellationToken::new();
        token.cancel();
        ctx.set_cancellation_token(token);
        let err = collect_l0_texts(&ctx, "Doc", "body").expect_err("the query was cancelled");
        assert!(err.to_string().contains("cancelled"), "{err}");
    }

    /// Without a deadline the merge still answers, nearest first.
    #[test]
    fn the_l0_vector_merge_ranks_every_unflushed_row() {
        let ctx = ctx_with_docs(4096);
        let mut results = Vec::new();
        merge_l0_into_vector_results(
            &mut results,
            &ctx,
            "Doc",
            "emb",
            &[0.0, 1.0],
            3,
            &DistanceMetric::L2,
            &HashSet::new(),
        )
        .unwrap();
        let vids: Vec<u64> = results.iter().map(|(v, _)| v.as_u64()).collect();
        assert_eq!(vids, vec![0, 1, 2]);
    }

    /// The indexed merge keeps the linear one's answers: an L0 score replaces a
    /// flushed one (the first occurrence), a new vid is appended, and a
    /// tombstoned vid is not resurrected.
    #[test]
    fn apply_l0_candidates_overwrites_appends_and_skips_tombstones() {
        let mut results = vec![(Vid::new(1), 0.1), (Vid::new(2), 0.2), (Vid::new(1), 0.9)];
        let candidates =
            HashMap::from([(Vid::new(1), 0.5), (Vid::new(3), 0.3), (Vid::new(4), 0.4)]);
        let tombstoned = HashSet::from([Vid::new(4)]);
        apply_l0_candidates(&mut results, &candidates, &tombstoned);
        assert_eq!(
            results,
            vec![
                (Vid::new(1), 0.5),
                (Vid::new(2), 0.2),
                (Vid::new(1), 0.9),
                (Vid::new(3), 0.3),
            ]
        );
    }
}

#[cfg(test)]
mod label_backfill_tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use uni_common::Value;

    fn props(uid: &str) -> uni_common::Properties {
        let mut p = uni_common::Properties::new();
        p.insert("uid".to_string(), Value::String(uid.to_string()));
        p
    }

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    async fn manager(dir: &tempfile::TempDir) -> (Arc<SchemaManager>, StorageManager) {
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let schema_manager = Arc::new(
            SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json"))
                .await
                .unwrap(),
        );
        let sm = StorageManager::new(dir.path().to_str().unwrap(), schema_manager.clone())
            .await
            .unwrap();
        (schema_manager, sm)
    }

    fn declare_l(schema_manager: &SchemaManager) {
        schema_manager.add_label("L").unwrap();
        schema_manager
            .add_property("L", "uid", uni_common::DataType::String, true)
            .unwrap();
    }

    async fn write_main(
        sm: &StorageManager,
        rows: &[(Vid, Vec<String>, uni_common::Properties, bool, u64)],
    ) {
        MainVertexDataset::write_batch(
            sm.backend(),
            MainVertexDataset::build_record_batch(rows, None, None).unwrap(),
        )
        .await
        .unwrap();
    }

    /// `vid`'s rows in the label table `L`, as sorted (version, deleted, uid).
    async fn label_rows(sm: &StorageManager, vid: u64) -> Vec<(u64, bool, Option<String>)> {
        let batches = sm
            .backend()
            .scan(
                ScanRequest::all(table_names::vertex_table_name("L"))
                    .with_filter(FilterExpr::one_of("_vid", [Scalar::UInt(vid)]))
                    .with_columns(vec![
                        "_version".to_string(),
                        "_deleted".to_string(),
                        "uid".to_string(),
                    ]),
            )
            .await
            .unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let ver = b
                .column_by_name("_version")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let del = b
                .column_by_name("_deleted")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();
            let uid = arrow::compute::cast(
                b.column_by_name("uid").unwrap(),
                &arrow_schema::DataType::Utf8,
            )
            .unwrap();
            let uid = uid
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                // A tombstone's property cells are placeholders.
                let live = !del.value(i);
                rows.push((
                    ver.value(i),
                    del.value(i),
                    (live && uid.is_valid(i)).then(|| uid.value(i).to_string()),
                ));
            }
        }
        rows.sort();
        rows
    }

    /// The backfill brings the label table to the main table's latest row per
    /// vertex: it copies what is missing, retires what the main table deleted
    /// or relabeled away, leaves what is current alone, and converges.
    #[tokio::test]
    async fn backfill_reconciles_label_table_with_main_table() {
        let dir = tempfile::tempdir().unwrap();
        let (schema_manager, sm) = manager(&dir).await;
        declare_l(&schema_manager);
        let l = labels(&["L"]);

        // 1 deleted at v2, 2 relabeled away at v2, 3 never copied, 4 already
        // copied at its latest version.
        write_main(
            &sm,
            &[
                (Vid::new(1), l.clone(), props("one"), false, 1),
                (
                    Vid::new(1),
                    l.clone(),
                    uni_common::Properties::new(),
                    true,
                    2,
                ),
                (Vid::new(2), l.clone(), props("two"), false, 1),
                (Vid::new(2), labels(&["Other"]), props("two"), false, 2),
                (
                    Vid::new(3),
                    labels(&["L", "Other"]),
                    props("three"),
                    false,
                    3,
                ),
                (Vid::new(4), l.clone(), props("four"), false, 1),
            ],
        )
        .await;

        // What an interrupted earlier backfill left: 1, 2 and 4 live at v1.
        let ds = sm.vertex_dataset("L").unwrap();
        let schema = schema_manager.schema();
        let residue = vec![
            (Vid::new(1), l.clone(), props("one")),
            (Vid::new(2), l.clone(), props("two")),
            (Vid::new(4), l.clone(), props("four")),
        ];
        let batch = ds
            .build_record_batch(&residue, &[false; 3], &[1; 3], &schema)
            .unwrap();
        ds.write_batch(sm.backend(), batch, &schema).await.unwrap();

        assert_eq!(sm.backfill_declared_label("L").await.unwrap(), 1);

        assert_eq!(
            label_rows(&sm, 1).await,
            vec![(1, false, Some("one".into())), (2, true, None)]
        );
        assert_eq!(
            label_rows(&sm, 2).await,
            vec![(1, false, Some("two".into())), (2, true, None)]
        );
        assert_eq!(
            label_rows(&sm, 3).await,
            vec![(3, false, Some("three".into()))]
        );
        assert_eq!(
            label_rows(&sm, 4).await,
            vec![(1, false, Some("four".into()))]
        );

        assert_eq!(
            sm.backfill_declared_label("L").await.unwrap(),
            0,
            "converged"
        );
        assert_eq!(
            sm.backfill_declared_label("Other").await.unwrap(),
            0,
            "an undeclared label is skipped"
        );
    }

    /// Recovery backfills a marked label the catalog declares and clears its
    /// marker; a marker for a label the catalog lacks is only cleared.
    #[tokio::test]
    async fn recovery_backfills_marked_declared_labels_and_clears_markers() {
        let dir = tempfile::tempdir().unwrap();
        let (schema_manager, sm) = manager(&dir).await;
        write_main(
            &sm,
            &[(Vid::new(7), labels(&["L"]), props("seven"), false, 1)],
        )
        .await;

        sm.mark_label_backfill_pending("L").await.unwrap();
        sm.mark_label_backfill_pending("never/saved").await.unwrap();
        declare_l(&schema_manager);

        assert_eq!(
            sm.recover_label_backfills().await.unwrap(),
            vec![("L".to_string(), 1)]
        );
        assert_eq!(
            label_rows(&sm, 7).await,
            vec![(1, false, Some("seven".into()))]
        );
        assert!(
            sm.recover_label_backfills().await.unwrap().is_empty(),
            "markers cleared"
        );
    }

    /// The row count a NOT NULL declaration consults includes vertices that
    /// carry the label only in the main table.
    #[tokio::test]
    async fn existing_row_count_counts_main_table_rows_of_an_undeclared_label() {
        let dir = tempfile::tempdir().unwrap();
        let (_schema_manager, sm) = manager(&dir).await;
        let l0 = crate::runtime::l0_visibility::L0Context::default();
        assert_eq!(sm.existing_row_count("L", &l0).await.unwrap(), 0);
        write_main(
            &sm,
            &[(Vid::new(7), labels(&["L"]), props("seven"), false, 1)],
        )
        .await;
        assert_eq!(sm.existing_row_count("L", &l0).await.unwrap(), 1);
        assert!(
            sm.new_property_nullability("L", "p", false, &l0)
                .await
                .unwrap()
        );
    }
}
