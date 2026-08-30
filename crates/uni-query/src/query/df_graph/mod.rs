// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Custom graph operators for DataFusion execution.
//!
//! This module provides DataFusion `ExecutionPlan` implementations for graph-specific
//! operations that cannot be expressed in standard relational algebra:
//!
//! - [`GraphScanExec`]: Scans vertices/edges with property materialization
//! - [`GraphExtIdLookupExec`]: Looks up a vertex by external ID
//! - [`GraphTraverseExec`]: Single-hop edge traversal using CSR adjacency
//! - `GraphVariableLengthTraverseExec`: Multi-hop BFS traversal
//! - [`GraphShortestPathExec`]: Shortest path computation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │      DataFusion ExecutionPlan Tree      │
//! ├─────────────────────────────────────────┤
//! │  ProjectionExec (DataFusion)            │
//! │       │                                 │
//! │  FilterExec (DataFusion)                │
//! │       │                                 │
//! │  GraphTraverseExec (CUSTOM)             │
//! │       │                                 │
//! │  GraphScanExec (CUSTOM)                 │
//! │       │                                 │
//! │  UniTableProvider + UniMergeExec        │
//! └─────────────────────────────────────────┘
//! ```
//!
//! Graph operators use [`GraphExecutionContext`] to access:
//! - AdjacencyManager for O(1) neighbor lookups
//! - L0 buffers for uncommitted edge visibility
//! - Property manager for lazy property loading

pub mod apply;
pub mod bind_fixed_path;
pub mod bind_zero_length_path;
pub mod bitmap;
pub mod catalog_scan;
pub mod common;
pub mod comprehension;
pub mod endpoint_hydrate;
pub mod expr_compiler;
pub mod ext_id_lookup;
pub mod iteration_driver;
pub mod locy_abduce;
pub mod locy_assume;
pub mod locy_ast_builder;
pub(crate) mod locy_bdd;
pub mod locy_best_by;
pub mod locy_calibrate;
pub mod locy_complement;
pub mod locy_delta;
pub mod locy_derive;
pub mod locy_errors;
pub mod locy_eval;
pub mod locy_explain;
pub mod locy_fixpoint;
pub mod locy_fold;
pub mod locy_model_invoke;
pub mod locy_priority;
pub mod locy_profile;
pub mod locy_program;
pub mod locy_query;
pub mod locy_slg;
pub mod locy_traits;
pub mod locy_validate;
pub mod mutation_common;
pub mod mutation_delete;
pub mod mutation_foreach;
pub mod mutation_remove;
pub mod mutation_set;
pub mod nfa;
pub mod optional_filter;
pub mod pattern_comprehension;
pub mod pattern_exists;
pub mod pred_dag;
pub mod procedure_call;
pub mod quantifier;
pub mod raw_bytes_marker;
mod read_set_exec;
pub mod recursive_cte;
pub mod reduce;
pub mod scan;
pub mod shortest_path;
pub(crate) mod similar_to_expr;
pub mod traverse;
pub mod unwind;
pub mod vector_knn;
pub mod vid_lookup_join;

use crate::query::executor::procedure::ProcedureRegistry;
use parking_lot::RwLock;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uni_algo::algo::AlgorithmRegistry;
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::adjacency_manager::AdjacencyManager;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

pub mod search_procedures;
use uni_xervo::runtime::ModelRuntime;

use crate::types::QueryWarning;
use uni_common::cypher_value_codec::handle::HandleScope;

pub use apply::GraphApplyExec;
pub use ext_id_lookup::GraphExtIdLookupExec;
// CREATE and MERGE both execute through `MutationExec`; these aliases and
// the `new_*_exec` builders are re-exported here so the planner can refer to
// them by their clause-specific names without a dedicated module each.
pub use mutation_common::{
    MutationContext, MutationExec, MutationExec as MutationCreateExec,
    MutationExec as MutationMergeExec, new_create_exec, new_merge_exec,
};
pub use mutation_delete::MutationDeleteExec;
pub use mutation_foreach::ForeachExec;
pub use mutation_remove::MutationRemoveExec;
pub use mutation_set::MutationSetExec;
pub use optional_filter::OptionalFilterExec;
pub use procedure_call::GraphProcedureCallExec;
pub use read_set_exec::ReadSetRecordingExec;
pub use scan::GraphScanExec;
pub use shortest_path::GraphShortestPathExec;
pub use traverse::{GraphTraverseExec, GraphTraverseMainExec};
pub use unwind::GraphUnwindExec;
pub use vector_knn::GraphVectorKnnExec;

pub use locy_best_by::BestByExec;
pub use locy_explain::{ProofTerm, ProvenanceAnnotation, ProvenanceStore};
pub use locy_fixpoint::{
    DerivedScanEntry, DerivedScanExec, DerivedScanRegistry, FixpointClausePlan, FixpointExec,
    FixpointRulePlan, FixpointState, IsRefBinding, MonotonicFoldBinding,
};
pub use locy_fold::FoldExec;
pub use locy_priority::PriorityExec;
pub use locy_program::{DerivedStore, LocyProgramExec};
pub use locy_traits::{DerivedFactSource, LocyExecutionContext};

/// Shared context for graph operators.
///
/// Provides access to graph-specific resources needed during query execution:
/// - CSR adjacency cache for fast neighbor lookups
/// - L0 buffers for MVCC visibility of uncommitted changes
/// - Property manager for lazy-loading vertex/edge properties
/// - Storage manager for schema and dataset access
///
/// # Example
///
/// ```ignore
/// let ctx = GraphExecutionContext::new(
///     storage_manager,
///     l0_buffer,
///     property_manager,
/// );
///
/// // Get neighbors with L0 overlay
/// let neighbors = ctx.get_neighbors(vid, edge_type_id, Direction::Outgoing);
/// ```
pub struct GraphExecutionContext {
    /// Storage manager for schema and dataset access.
    storage: Arc<StorageManager>,

    /// L0 visibility context for MVCC.
    l0_context: L0Context,

    /// Property manager for lazy property loading.
    property_manager: Arc<PropertyManager>,

    /// Query timeout deadline.
    deadline: Option<Instant>,

    /// Algorithm registry for `uni.algo.*` procedure dispatch.
    algo_registry: Option<Arc<AlgorithmRegistry>>,

    /// External procedure registry for test/user-defined procedures.
    procedure_registry: Option<Arc<ProcedureRegistry>>,
    /// Plugin registry — used by the native-label scan dispatcher
    /// (M5h.2) to route a label's reads through plugin `Storage` when
    /// one is registered via `PluginRegistry::register_label_storage`.
    plugin_registry: Option<Arc<uni_plugin::PluginRegistry>>,
    /// Uni-Xervo runtime used by vector auto-embedding paths.
    xervo_runtime: Option<Arc<ModelRuntime>>,

    /// Runtime warnings collected during query execution.
    warnings: Arc<Mutex<Vec<QueryWarning>>>,

    /// Per-query execution counters, shared with the executor.
    ///
    /// Unlike `warnings`, this needs no harvest step: it is an `Arc` of atomics
    /// shared with the `Executor` that seeded it, so the executor reads its own
    /// handle once execution finishes.
    counters: Option<Arc<uni_store::QueryCounters>>,

    /// Cooperative cancellation token, threaded from `QueryContext`.
    cancellation_token: Option<tokio_util::sync::CancellationToken>,

    /// Outer transaction's writer handle (FU-1 / M11 #6). Threaded
    /// from the [`crate::Executor`] when the query is running inside a
    /// write-mode transaction; consumed by
    /// `QueryProcedureHost::with_writer` at procedure invocation time
    /// so a declared `WRITE`-mode procedure's Cypher body can mutate
    /// the outer transaction's L0. `Arc<Writer>` (interior-mutable,
    /// no outer lock) matches the executor's writer handle type.
    writer: Option<Arc<uni_store::Writer>>,

    /// Owns the values interned during this query.
    ///
    /// A `collect()` large enough to be worth interning registers its list here
    /// and travels as a 9-byte handle, so the operators above copy the handle
    /// per row instead of the whole list. The scope is dropped with the
    /// context, which is what releases the ids — see
    /// `uni_common::cypher_value_codec::handle`.
    handle_scope: Arc<HandleScope>,
}

impl std::fmt::Debug for GraphExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphExecutionContext")
            .field("l0_context", &self.l0_context)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// L0 buffer visibility context for MVCC reads.
///
/// Defined in `uni-store` beside the rest of the L0 visibility machinery — it
/// is made entirely of `uni-store` types and the storage layer needs it too.
/// Re-exported here so the ~30 references across the workspace, including the
/// struct-literal constructions in `crates/uni/src/api/`, are unaffected.
pub use uni_store::runtime::l0_visibility::L0Context;

/// An edge's stored orientation, plus the type the probe identified on the way.
///
/// See [`GraphExecutionContext::resolve_stored_edge`] for when `type_id` is
/// populated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedEdge {
    /// Stored start vertex.
    pub src: u64,
    /// Stored end vertex.
    pub dst: u64,
    /// The edge's type, when the adjacency probe identified it.
    pub type_id: Option<u32>,
}

impl GraphExecutionContext {
    /// Shared constructor for the public entry points. The three public
    /// constructors differ only in the L0 visibility context, deadline,
    /// and cancellation token; every other field starts at its default.
    fn with_parts(
        storage: Arc<StorageManager>,
        l0_context: L0Context,
        property_manager: Arc<PropertyManager>,
        deadline: Option<Instant>,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self {
            storage,
            l0_context,
            property_manager,
            deadline,
            algo_registry: None,
            procedure_registry: None,
            plugin_registry: None,
            xervo_runtime: None,
            warnings: Arc::new(Mutex::new(Vec::new())),
            counters: None,
            cancellation_token,
            writer: None,
            handle_scope: Arc::new(HandleScope::new()),
        }
    }

    /// The interning scope owning values registered during this query.
    pub fn handle_scope(&self) -> &Arc<HandleScope> {
        &self.handle_scope
    }

    /// Create a new graph execution context.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage manager for schema and dataset access
    /// * `l0` - Current L0 buffer for MVCC visibility
    /// * `property_manager` - Manager for lazy property loading
    pub fn new(
        storage: Arc<StorageManager>,
        l0: Arc<RwLock<L0Buffer>>,
        property_manager: Arc<PropertyManager>,
    ) -> Self {
        Self::with_parts(
            storage,
            L0Context::with_current(l0),
            property_manager,
            None,
            None,
        )
    }

    /// Create context with full L0 visibility.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage manager for schema and dataset access
    /// * `l0_context` - L0 visibility context with all buffers
    /// * `property_manager` - Manager for lazy property loading
    pub fn with_l0_context(
        storage: Arc<StorageManager>,
        l0_context: L0Context,
        property_manager: Arc<PropertyManager>,
    ) -> Self {
        Self::with_parts(storage, l0_context, property_manager, None, None)
    }

    /// Create context from a query context.
    pub fn from_query_context(
        storage: Arc<StorageManager>,
        query_ctx: &QueryContext,
        property_manager: Arc<PropertyManager>,
    ) -> Self {
        let mut ctx = Self::with_parts(
            storage,
            L0Context::from_query_context(query_ctx),
            property_manager,
            query_ctx.deadline,
            query_ctx.cancellation_token.clone(),
        );
        ctx.counters = query_ctx.counters.clone();
        ctx
    }

    /// Attach the per-query counter set.
    #[must_use]
    pub fn with_counters(mut self, counters: Option<Arc<uni_store::QueryCounters>>) -> Self {
        self.counters = counters;
        self
    }

    /// The per-query counter set, if this context belongs to a counted query.
    pub fn counters(&self) -> Option<&Arc<uni_store::QueryCounters>> {
        self.counters.as_ref()
    }

    /// Records `n` rows served from an L0 buffer, if counting is active.
    pub fn count_l0_rows(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_l0_rows(n);
        }
    }

    /// Records `n` rows served from L1 / Lance storage, if counting is active.
    pub fn count_storage_rows(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_storage_rows(n);
        }
    }

    /// Records `n` rows examined by a scan, if counting is active.
    pub fn count_rows_scanned(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_rows_scanned(n);
        }
    }

    /// Set query timeout deadline.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Attach the outer transaction's writer handle so declared
    /// `WRITE`-mode procedures invoked through this context can run
    /// their Cypher bodies via the write-enabled inner-query host.
    #[must_use]
    pub fn with_writer(mut self, writer: Arc<uni_store::Writer>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Borrow the outer transaction's writer handle, if any.
    #[must_use]
    pub fn writer(&self) -> Option<&Arc<uni_store::Writer>> {
        self.writer.as_ref()
    }

    /// Set the algorithm registry for `uni.algo.*` procedure dispatch.
    pub fn with_algo_registry(mut self, registry: Arc<AlgorithmRegistry>) -> Self {
        self.algo_registry = Some(registry);
        self
    }

    /// Get a reference to the algorithm registry, if set.
    pub fn algo_registry(&self) -> Option<&Arc<AlgorithmRegistry>> {
        self.algo_registry.as_ref()
    }

    /// Set the external procedure registry for test/user-defined procedures.
    pub fn with_procedure_registry(mut self, registry: Arc<ProcedureRegistry>) -> Self {
        self.procedure_registry = Some(registry);
        self
    }

    /// Set Uni-Xervo runtime for query-time auto-embedding.
    pub fn with_xervo_runtime(mut self, runtime: Arc<ModelRuntime>) -> Self {
        self.xervo_runtime = Some(runtime);
        self
    }

    /// Get a reference to the procedure registry, if set.
    pub fn procedure_registry(&self) -> Option<&Arc<ProcedureRegistry>> {
        self.procedure_registry.as_ref()
    }

    /// Attach the plugin registry. Required by the M5h.2 native-label
    /// plugin-storage routing in `columnar_scan_vertex_batch_static`.
    pub fn with_plugin_registry(mut self, registry: Arc<uni_plugin::PluginRegistry>) -> Self {
        self.plugin_registry = Some(registry);
        self
    }

    /// Reference to the plugin registry (if set).
    pub fn plugin_registry(&self) -> Option<&Arc<uni_plugin::PluginRegistry>> {
        self.plugin_registry.as_ref()
    }

    pub fn xervo_runtime(&self) -> Option<&Arc<ModelRuntime>> {
        self.xervo_runtime.as_ref()
    }

    /// Record a runtime warning.
    pub fn push_warning(&self, warning: QueryWarning) {
        if let Ok(mut w) = self.warnings.lock() {
            w.push(warning);
        }
    }

    /// Take all collected warnings, leaving the collector empty.
    pub fn take_warnings(&self) -> Vec<QueryWarning> {
        self.warnings
            .lock()
            .map(|mut w| std::mem::take(&mut *w))
            .unwrap_or_default()
    }

    /// Check if the query has timed out.
    ///
    /// # Errors
    ///
    /// Returns an error if the deadline has passed.
    pub fn check_timeout(&self) -> anyhow::Result<()> {
        common::check_deadline(self.cancellation_token.as_ref(), self.deadline)
    }

    /// Get a reference to the storage manager.
    pub fn storage(&self) -> &Arc<StorageManager> {
        &self.storage
    }

    /// Get a reference to the adjacency manager.
    pub fn adjacency_manager(&self) -> Arc<AdjacencyManager> {
        self.storage.adjacency_manager()
    }

    /// Get a reference to the property manager.
    pub fn property_manager(&self) -> &Arc<PropertyManager> {
        &self.property_manager
    }

    /// Get a reference to the L0 context.
    pub fn l0_context(&self) -> &L0Context {
        &self.l0_context
    }

    /// Resolve a vertex's labels for a traversal-time label predicate.
    ///
    /// A vertex deleted in a live (unflushed) L0 layer resolves to an empty
    /// label set: its tombstone is recorded but its persisted `VidLabelsIndex`
    /// entry survives until the next flush, so consulting the index directly
    /// would resurrect stale labels for a deleted vertex (#140). Returning
    /// `Some(empty)` — rather than `None` — makes every label predicate reject
    /// the deleted vertex instead of admitting it through a fail-open path.
    ///
    /// Otherwise checks the L0 chain first (which also records the read for
    /// SSI), then the persisted `VidLabelsIndex`. The L0 chain covers fork-local
    /// and committed-but-unflushed writes; the index covers vertices that live
    /// only in Lance storage — notably every vertex on a fork, whose data is
    /// flushed to Lance before branching. Returns `None` only when the vertex
    /// is absent from both, in which case callers fall back to the label that
    /// scoped the traversal.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// if let Some(labels) = ctx.resolve_vertex_labels(target_vid, &query_ctx) {
    ///     keep = labels.iter().any(|l| l == "B");
    /// }
    /// ```
    pub fn resolve_vertex_labels(&self, vid: Vid, query_ctx: &QueryContext) -> Option<Vec<String>> {
        if uni_store::runtime::l0_visibility::is_vertex_deleted(vid, Some(query_ctx)) {
            return Some(Vec::new());
        }
        uni_store::runtime::l0_visibility::get_vertex_labels_optional(vid, query_ctx)
            .or_else(|| self.storage.get_labels_from_index(vid))
    }

    /// Wall-clock deadline for the surrounding query, if any.
    ///
    /// Internal accessor used by [`crate::query::executor::procedure_host::QueryProcedureHost`]
    /// to snapshot the deadline so procedure plugins can implement
    /// `check_timeout` without holding a borrow on this context.
    #[must_use]
    pub fn deadline_for_host(&self) -> Option<Instant> {
        self.deadline
    }

    /// Cancellation token clone for the surrounding query, if any.
    ///
    /// Internal accessor for [`crate::query::executor::procedure_host::QueryProcedureHost`];
    /// see [`Self::deadline_for_host`].
    #[must_use]
    pub fn cancellation_token_for_host(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.cancellation_token.clone()
    }

    /// Create a query context for property manager calls.
    ///
    /// If there is no current L0 buffer (e.g., for snapshot queries), creates an empty one.
    pub fn query_context(&self) -> QueryContext {
        let l0 = self
            .l0_context
            .current_l0
            .clone()
            .unwrap_or_else(|| Arc::new(RwLock::new(L0Buffer::new(0, None))));

        let mut ctx = QueryContext::new_with_pending(
            l0,
            self.l0_context.transaction_l0.clone(),
            self.l0_context.pending_flush_l0s.clone(),
        );
        if let Some(deadline) = self.deadline {
            ctx.set_deadline(deadline);
        }
        // Carry the counters through. The scan path does not need this — it
        // builds a `ScanRequest` and puts them there directly — but vector and
        // FTS search have no `ScanRequest`, so this `QueryContext` is the only
        // channel by which their index-consultation counts can be attributed to
        // the query that caused them.
        if let Some(counters) = self.counters.clone() {
            ctx.set_counters(counters);
        }
        ctx
    }

    /// Ensure adjacency CSRs are warmed for the given edge types and direction.
    ///
    /// This loads any missing CSR data from storage into the adjacency manager
    /// so that subsequent `get_neighbors` calls return complete results.
    /// Skips warming if the adjacency manager already has data (Main CSR or
    /// active overlay) for the edge type, avoiding duplicate entries.
    pub async fn ensure_adjacency_warmed(
        &self,
        edge_type_ids: &[u32],
        direction: Direction,
    ) -> anyhow::Result<()> {
        let am = self.adjacency_manager();
        // Manifest pin only: tx version pins must NOT filter adjacency
        // warming/reads (see StorageManager::snapshot_version_hwm).
        let version = self.storage.snapshot_version_hwm();
        for &etype_id in edge_type_ids {
            for &dir in direction.expand() {
                // Warm each (edge_type, direction) CSR exactly once, gated on the
                // CSR itself — NOT on the dual-write overlay. The previous
                // `is_active_for` gate skipped warming whenever the overlay held
                // ANY edge for the direction; on a fork a single relationship
                // `SET` dual-writes one edge into that direction's overlay, which
                // then suppressed loading the inherited L1/L2 edges — silently
                // dropping every inherited edge from reads of that direction
                // (reverse traversal in #110). `get_neighbors` always merges the
                // CSR with the overlay and dedups by `eid`, so warming when the
                // overlay also holds edges never double-counts; committed-
                // unflushed edges remain in the overlay until flush re-warms the
                // CSR (executor `apply_compaction` → `AdjacencyManager::warm`).
                // `warm_adjacency_coalesced` fast-paths on `has_csr`, keeping
                // this idempotent (warm-once) and stampede-safe (Issue #13).
                // Revalidate before trusting the cache. Presence alone was the
                // gate, and the CSR is never invalidated by a flush — sound for
                // the handle that owns the writer, whose commits go straight
                // into the overlay, but a second handle (a read-only reader
                // beside a writer) gets no such updates and was pinned to the
                // edge set it first traversed, permanently. Issue #168.
                if self.storage.requires_adjacency_revalidation()
                    && !self.storage.adjacency_csr_is_fresh(etype_id, dir).await
                {
                    self.storage.invalidate_adjacency_csr(etype_id, dir);
                }
                if !am.has_csr(etype_id, dir) {
                    self.storage
                        .warm_adjacency_coalesced(etype_id, dir, version)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Create a boxed warming future for use in DataFusion stream state machines.
    ///
    /// Wraps `ensure_adjacency_warmed` into a `Pin<Box<dyn Future<Output = DFResult<()>> + Send>>`
    /// suitable for polling in stream `poll_next` implementations.
    pub fn warming_future(
        self: &Arc<Self>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = datafusion::common::Result<()>> + Send>>
    {
        let ctx = self.clone();
        Box::pin(async move {
            ctx.ensure_adjacency_warmed(&edge_type_ids, direction)
                .await
                .map_err(common::exec_err)
        })
    }

    /// Get neighbors for a vertex, merging CSR and all L0 buffers.
    ///
    /// This implements the MVCC visibility rules:
    /// 1. Load from CSR (L2 + L1 merged, auto-warms on cache miss)
    /// 2. Overlay pending flush L0s (oldest to newest)
    /// 3. Overlay current L0
    /// 4. Overlay transaction L0 (if present)
    /// 5. Filter tombstones (handled by overlay)
    ///
    /// # Arguments
    ///
    /// * `vid` - Source vertex ID
    /// * `edge_type` - Edge type ID to traverse
    /// * `direction` - Traversal direction (Outgoing, Incoming, or Both)
    ///
    /// # Returns
    ///
    /// Vector of (neighbor VID, edge ID) pairs.
    pub fn get_neighbors(&self, vid: Vid, edge_type: u32, direction: Direction) -> Vec<(Vid, Eid)> {
        // Manifest pin only (time-travel); tx pins read live edges + L0 overlays.
        let version_hwm = self.storage.snapshot_version_hwm();
        // Single-vid case: acquire the transaction-L0 guard once for this
        // vertex (the batch path amortizes it across many vertices).
        let tx_guard = self.l0_context.transaction_l0.as_ref().map(|l0| l0.read());
        self.neighbors_for_vid(
            vid,
            edge_type,
            direction,
            version_hwm,
            tx_guard.as_deref(),
            true,
        )
    }

    /// Get neighbors for multiple vertices in batch.
    ///
    /// More efficient than calling `get_neighbors` repeatedly as it amortizes
    /// lock acquisition for L0 buffers.
    ///
    /// # Arguments
    ///
    /// * `vids` - Source vertex IDs
    /// * `edge_type` - Edge type ID to traverse
    /// * `direction` - Traversal direction
    ///
    /// # Returns
    ///
    /// Vector of (source VID, neighbor VID, edge ID) triples.
    pub fn get_neighbors_batch(
        &self,
        vids: &[Vid],
        edge_type: u32,
        direction: Direction,
    ) -> Vec<(Vid, Vid, Eid)> {
        // Manifest pin only (time-travel); tx pins read live edges + L0 overlays.
        let version_hwm = self.storage.snapshot_version_hwm();
        let tx_guard = self.l0_context.transaction_l0.as_ref().map(|l0| l0.read());

        let mut results = Vec::new();
        for &vid in vids {
            // record_reads=false: the whole batch is recorded in one read-set
            // lock below instead of two lock acquisitions per source vertex.
            let neighbors = self.neighbors_for_vid(
                vid,
                edge_type,
                direction,
                version_hwm,
                tx_guard.as_deref(),
                false,
            );
            results.extend(
                neighbors
                    .into_iter()
                    .map(|(neighbor, eid)| (vid, neighbor, eid)),
            );
        }
        drop(tx_guard);
        self.record_neighbor_reads_batch(vids, &results);
        results
    }

    /// Resolve an edge's STORED `(src, dst)` orientation given a traversed hop.
    ///
    /// A relationship in a path must report its stored (start -> end) direction
    /// even when the path traversed it backward (undirected `-[r]-` or incoming
    /// `<-[r]-`). This first consults the L0 visibility chain (exact for
    /// in-memory edges); if the edge has been flushed to durable storage and is
    /// no longer L0-resident, it recovers the orientation with a bounded
    /// directed-outgoing adjacency probe: the edge is stored
    /// `(traversal_src -> traversal_dst)` iff `eid` appears among
    /// `traversal_src`'s outgoing neighbours for one of `edge_type_ids`,
    /// otherwise it is the reverse. Falls back to the traversal order only when
    /// the probe is inconclusive (e.g. the type carries no CSR adjacency).
    ///
    /// The probe costs at most the out-degree of one vertex per candidate edge
    /// type — never a full edge scan — and reads the adjacency manager directly,
    /// so it does not perturb the SSI read-set.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let (src, dst) =
    ///     ctx.resolve_stored_edge_endpoints(eid, node_path[i], node_path[i + 1], &edge_type_ids);
    /// ```
    #[must_use]
    pub fn resolve_stored_edge_endpoints(
        &self,
        eid: Eid,
        traversal_src: Vid,
        traversal_dst: Vid,
        edge_type_ids: &[u32],
    ) -> (u64, u64) {
        let resolved = self.resolve_stored_edge(eid, traversal_src, traversal_dst, edge_type_ids);
        (resolved.src, resolved.dst)
    }

    /// [`Self::resolve_stored_edge_endpoints`], additionally reporting the edge
    /// type the probe identified.
    ///
    /// `type_id` is `Some` only when the adjacency probe ran and succeeded —
    /// that is, for a flushed edge. An L0-resident edge resolves from the
    /// visibility chain without ever asking which type it is, and an
    /// inconclusive probe identifies nothing. Callers that need a type name
    /// should therefore consult the L0 chain *and* this, in either order:
    /// exactly one of the two has the answer for any given edge.
    ///
    /// This exists because the probe already determines the type in order to
    /// find the edge, and discarding it left flushed relationships reporting a
    /// placeholder type name.
    #[must_use]
    pub fn resolve_stored_edge(
        &self,
        eid: Eid,
        traversal_src: Vid,
        traversal_dst: Vid,
        edge_type_ids: &[u32],
    ) -> ResolvedEdge {
        // 1. L0 visibility chain — exact stored endpoints for in-memory edges.
        let query_ctx = self.query_context();
        if let Some((src, dst)) =
            uni_store::runtime::l0_visibility::get_edge_endpoints(eid, &query_ctx)
        {
            return ResolvedEdge {
                src: src.as_u64(),
                dst: dst.as_u64(),
                type_id: None,
            };
        }

        // 2. Flushed (L1-resident) edge: recover orientation via a directed
        //    outgoing adjacency probe. Read the adjacency manager / versioned
        //    snapshot directly so the probe stays out of the SSI read-set.
        //
        //    When the caller could not supply the edge's type ids (e.g. an
        //    anonymous `-[]-` relationship reaching BindFixedPath without a
        //    `_type` column), fall back to the adjacency manager's warmed types
        //    — exactly the set traversed by this query, so still a bounded probe.
        let adjacency_manager = self.adjacency_manager();
        let warmed_fallback: Vec<u32>;
        let probe_types: &[u32] = if edge_type_ids.is_empty() {
            warmed_fallback = adjacency_manager.known_edge_type_ids();
            &warmed_fallback
        } else {
            edge_type_ids
        };
        let version_hwm = self.storage.snapshot_version_hwm();
        // Returns the edge type that places `eid` on `vid`'s outgoing side.
        // The type is a by-product of the search, not extra work.
        let outgoing_type = |vid: Vid| -> Option<u32> {
            probe_types.iter().copied().find(|&etype| {
                let neighbors = match version_hwm {
                    Some(hwm) => {
                        self.storage
                            .get_neighbors_at_version(vid, etype, Direction::Outgoing, hwm)
                    }
                    None => adjacency_manager.get_neighbors(vid, etype, Direction::Outgoing),
                };
                neighbors.iter().any(|&(_, e)| e == eid)
            })
        };

        if let Some(type_id) = outgoing_type(traversal_src) {
            ResolvedEdge {
                src: traversal_src.as_u64(),
                dst: traversal_dst.as_u64(),
                type_id: Some(type_id),
            }
        } else if let Some(type_id) = outgoing_type(traversal_dst) {
            ResolvedEdge {
                src: traversal_dst.as_u64(),
                dst: traversal_src.as_u64(),
                type_id: Some(type_id),
            }
        } else {
            // 3. Inconclusive (no CSR adjacency for this type): preserve the
            //    long-standing traversal-order behaviour.
            ResolvedEdge {
                src: traversal_src.as_u64(),
                dst: traversal_dst.as_u64(),
                type_id: None,
            }
        }
    }

    /// Resolve a single vertex's neighbours, overlaying the transaction L0
    /// (if visible) and recording the traversal into the SSI read-set.
    ///
    /// `tx_guard` is the already-acquired read guard over the transaction
    /// L0 buffer (if any), so batch callers acquire the lock once and pass
    /// the borrow in for every vertex.
    /// When `record_reads` is false the caller takes responsibility for
    /// recording the traversal into the SSI read-set (the batch path records
    /// once per batch via [`record_neighbor_reads_batch`]).
    ///
    /// [`record_neighbor_reads_batch`]: Self::record_neighbor_reads_batch
    fn neighbors_for_vid(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
        version_hwm: Option<u64>,
        tx_guard: Option<&L0Buffer>,
        record_reads: bool,
    ) -> Vec<(Vid, Eid)> {
        // Use AdjacencyManager which reads Main CSR + overlay (dual-write).
        // For snapshot queries, filter by version via StorageManager delegate.
        let mut neighbors = if let Some(hwm) = version_hwm {
            self.storage
                .get_neighbors_at_version(vid, edge_type, direction, hwm)
        } else {
            self.adjacency_manager()
                .get_neighbors(vid, edge_type, direction)
        };

        // Overlay transaction L0 if present (transaction edges bypass Writer/AM).
        if version_hwm.is_none()
            && let Some(tx_guard) = tx_guard
        {
            overlay_l0_neighbors(
                vid,
                edge_type,
                direction,
                tx_guard,
                &mut neighbors,
                version_hwm,
            );
        }

        if record_reads {
            self.record_neighbor_reads(vid, &neighbors);
        }

        neighbors
    }

    /// Records traversed edges and discovered neighbours into the SSI read-set.
    ///
    /// No-op unless this is a read-write transaction (`occ_read_set` is `Some`
    /// only then), so read-only and analytical traversals pay nothing. Recording
    /// the source plus each neighbour vid and edge id gives item-level
    /// antidependency coverage for traversals, matching the keyed read paths.
    fn record_neighbor_reads(&self, src: Vid, neighbors: &[(Vid, Eid)]) {
        let Some(tx_l0) = &self.l0_context.transaction_l0 else {
            return;
        };
        let guard = tx_l0.read();
        let Some(read_set) = &guard.occ_read_set else {
            return;
        };
        let mut rs = read_set.lock();
        rs.vertices.insert(src);
        for (nbr, eid) in neighbors {
            rs.vertices.insert(*nbr);
            rs.edges.insert(*eid);
        }
    }

    /// Batch variant of [`record_neighbor_reads`](Self::record_neighbor_reads):
    /// records an entire expansion batch under ONE read-set lock instead of
    /// two lock acquisitions per source vertex.
    ///
    /// `srcs` is recorded in full — a source with zero neighbours is still a
    /// read ("no edges here") that a concurrent edge insert must conflict
    /// with, exactly as the per-vertex recorder behaves.
    fn record_neighbor_reads_batch(&self, srcs: &[Vid], triples: &[(Vid, Vid, Eid)]) {
        if srcs.is_empty() && triples.is_empty() {
            return;
        }
        let Some(tx_l0) = &self.l0_context.transaction_l0 else {
            return;
        };
        let guard = tx_l0.read();
        let Some(read_set) = &guard.occ_read_set else {
            return;
        };
        let mut rs = read_set.lock();
        for src in srcs {
            rs.vertices.insert(*src);
        }
        for (_, nbr, eid) in triples {
            rs.vertices.insert(*nbr);
            rs.edges.insert(*eid);
        }
    }

    /// Records the vertex/edge ids in the given batch columns into the read-set.
    ///
    /// Used by [`ReadSetRecordingExec`] to capture the identities of rows that
    /// survived a scan's filters. No-op when there is no transaction read-set
    /// (read-only / analytical contexts).
    ///
    /// [`ReadSetRecordingExec`]: crate::query::df_graph::ReadSetRecordingExec
    pub(crate) fn record_batch_ids(
        &self,
        batch: &arrow_array::RecordBatch,
        vertex_cols: &[usize],
        edge_cols: &[usize],
    ) {
        use arrow_array::{Array, UInt64Array};

        if vertex_cols.is_empty() && edge_cols.is_empty() {
            return;
        }
        let Some(tx_l0) = &self.l0_context.transaction_l0 else {
            return;
        };
        let guard = tx_l0.read();
        let Some(read_set) = &guard.occ_read_set else {
            return;
        };
        let mut rs = read_set.lock();
        for &col in vertex_cols {
            if let Some(arr) = batch.column(col).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        rs.vertices.insert(Vid::from(arr.value(i)));
                    }
                }
            }
        }
        for &col in edge_cols {
            if let Some(arr) = batch.column(col).as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        rs.edges.insert(Eid::from(arr.value(i)));
                    }
                }
            }
        }
    }
}

/// Overlay L0 buffer neighbors onto existing neighbor list.
///
/// Adds new edges from L0 and removes tombstoned edges.
/// Filters by version if a snapshot boundary is provided.
fn overlay_l0_neighbors(
    vid: Vid,
    edge_type: u32,
    direction: Direction,
    l0: &L0Buffer,
    neighbors: &mut Vec<(Vid, Eid)>,
    version_hwm: Option<u64>,
) {
    use std::collections::HashMap;

    // Convert to map for efficient updates
    let mut neighbor_map: HashMap<Eid, Vid> = neighbors.drain(..).map(|(v, e)| (e, v)).collect();

    // Query L0 for each direction
    for &simple_dir in direction.to_simple_directions() {
        for (neighbor, eid, version) in l0.get_neighbors(vid, edge_type, simple_dir) {
            // Skip edges beyond snapshot boundary
            if version_hwm.is_some_and(|hwm| version > hwm) {
                continue;
            }

            // Apply insert or remove tombstone
            if l0.is_tombstoned(eid) {
                neighbor_map.remove(&eid);
            } else {
                neighbor_map.insert(eid, neighbor);
            }
        }
    }

    // Remove edges tombstoned in this L0 but originating from other layers
    for eid in l0.tombstones.keys() {
        neighbor_map.remove(eid);
    }

    // Convert back to vec
    *neighbors = neighbor_map.into_iter().map(|(e, v)| (v, e)).collect();
}

/// Extension trait to convert storage Direction to SimpleGraph directions.
trait DirectionExt {
    fn to_simple_directions(&self) -> &'static [uni_common::graph::simple_graph::Direction];
}

impl DirectionExt for Direction {
    fn to_simple_directions(&self) -> &'static [uni_common::graph::simple_graph::Direction] {
        use uni_common::graph::simple_graph::Direction as SimpleDirection;
        match self {
            Direction::Outgoing => &[SimpleDirection::Outgoing],
            Direction::Incoming => &[SimpleDirection::Incoming],
            Direction::Both => &[SimpleDirection::Outgoing, SimpleDirection::Incoming],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l0_context_empty() {
        let ctx = L0Context::empty();
        assert!(ctx.current_l0.is_none());
        assert!(ctx.transaction_l0.is_none());
        assert!(ctx.pending_flush_l0s.is_empty());
    }
}
