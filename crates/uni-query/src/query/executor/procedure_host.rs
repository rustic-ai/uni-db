// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Concrete [`ProcedureHost`] implementation backed by a snapshot of
//! [`GraphExecutionContext`].
//!
//! `ProcedurePlugin` impls living in `crates/uni-query/src/procedures_plugin/`
//! (host-coupled plugins for `uni.schema.*`, `uni.vector.*`, `uni.fts.*`,
//! `uni.search`, `uni.algo.*`) downcast a `&dyn ProcedureHost` to
//! [`QueryProcedureHost`] to reach the storage, schema, algorithm
//! registry, L0 visibility, query context, deadline, and other host
//! facilities that the `uni-plugin` ABI cannot expose without a cyclic
//! dependency. This is the interim bridge while the proposal-spec
//! `session` / `tx` plumbing waits on the M6 ABI freeze.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_schema::SchemaRef;
use tokio_util::sync::CancellationToken;
use uni_algo::algo::AlgorithmRegistry;
use uni_plugin::traits::procedure::{ProcedureHost, ProcedureMode};
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;
use uni_xervo::runtime::ModelRuntime;

use crate::query::df_graph::{GraphExecutionContext, L0Context};
use crate::query::executor::procedure::ProcedureRegistry;

// Rust guideline compliant

/// Host facade exposing a snapshot of [`GraphExecutionContext`] to
/// in-tree procedure plugins.
///
/// Built-in host-coupled procedures invoked via `CALL uni.X` receive a
/// `ProcedureContext` whose `host` field points at a `QueryProcedureHost`
/// constructed by the dispatch sites
/// (`procedure_call::execute_plugin_procedure` and
/// `executor::procedure`). Plugins downcast to recover the concrete
/// type, then call the typed accessors below.
///
/// All fields are owned (Arc-shared) rather than borrowed, so the host
/// is `'static`-friendly — which is the constraint
/// [`std::any::Any`] imposes for downcasting. Construction is a small
/// number of Arc-clones; the per-call cost is negligible.
#[derive(Clone)]
pub struct QueryProcedureHost {
    storage: Arc<StorageManager>,
    algo_registry: Option<Arc<AlgorithmRegistry>>,
    procedure_registry: Option<Arc<ProcedureRegistry>>,
    xervo_runtime: Option<Arc<ModelRuntime>>,
    property_manager: Option<Arc<PropertyManager>>,
    l0_context: L0Context,
    deadline: Option<Instant>,
    /// Per-query counters, inherited from the `GraphExecutionContext`.
    ///
    /// Procedures reach storage through [`Self::query_context`] rather than a
    /// `ScanRequest`, so without this a `CALL uni.vector.query(...)` executes
    /// with no way to attribute what it consulted back to the query (#175).
    counters: Option<Arc<uni_store::QueryCounters>>,
    cancellation_token: Option<CancellationToken>,
    /// Per-request projection map: output variable name → requested
    /// property names. Populated from the surrounding query's plan in
    /// `procedure_call.rs::execute_plugin_procedure`. Empty if the
    /// procedure is invoked without surrounding projection context
    /// (simple-executor path).
    target_properties: HashMap<String, Vec<String>>,
    /// Per-request YIELD list: `(yield_name, alias)`. Search procedures
    /// (`uni.vector.query` / `.fts.query` / `.search`) need this to
    /// expand `node` yields into the planner-expected
    /// `{alias}._vid` / `{alias}` / `{alias}._labels` / `{alias}.X`
    /// column shape; other plugins ignore it.
    yield_items: Vec<(String, Option<String>)>,
    /// Per-request planner-expected output schema. When the plugin
    /// produces a batch whose schema matches this, the dispatcher
    /// passes it through without reprojection.
    expected_schema: Option<SchemaRef>,
    /// Monotonic per-query counter feeding `allocate_transient_id`
    /// (M5g). Shared across `Clone`s of the host so all dispatches
    /// within the same procedure invocation draw from one stream.
    /// Bottom 63 bits become a `Vid`/`Eid` after OR-ing with
    /// `Vid::EPHEMERAL_BIT`.
    transient_counter: Arc<AtomicU64>,
    /// Outer transaction's writer handle, threaded through when the
    /// host is constructed inside a write transaction. Required for
    /// `Write`/`Schema`/`Dbms`-mode invocations of
    /// [`Self::execute_inner_query`]; when `None`, write-mode inner
    /// queries fail with a clear "no writer available" error.
    writer: Option<Arc<Writer>>,
    /// Re-entrancy depth for declared-trigger action bodies (WS-A R1).
    ///
    /// A host built for a normal query / procedure invocation carries
    /// `0`. When a declared trigger fires, the host it runs its action
    /// body against carries `1`; if that action writes rows that
    /// re-fire a declared trigger, the next host carries `2`, and so on.
    /// The synthetic-trigger plugin refuses to fire once this exceeds
    /// [`Self::MAX_TRIGGER_DEPTH`], collapsing an otherwise-unbounded
    /// self-referential write storm to a bounded chain. Propagated
    /// through [`Self::execute_inner_query`] so the commit the action
    /// produces stamps the incremented depth onto the host it builds for
    /// its own triggers.
    trigger_depth: u32,
}

impl QueryProcedureHost {
    /// Maximum declared-trigger action re-entrancy depth (WS-A R1).
    ///
    /// A depth of `1` is the trigger's own action body; deeper values
    /// mean the action re-fired another declared trigger. Firing is
    /// refused past this cap so a self-referential trigger terminates
    /// instead of driving an unbounded async write storm.
    pub const MAX_TRIGGER_DEPTH: u32 = 4;
    /// Snapshot the host-shaped components of `graph_ctx`. The
    /// per-request fields start empty; use
    /// [`Self::from_graph_ctx_with_request`] when the surrounding query
    /// has projection / yield context.
    #[must_use]
    pub fn from_graph_ctx(graph_ctx: &GraphExecutionContext) -> Self {
        Self::from_graph_ctx_with_request(graph_ctx, HashMap::new(), Vec::new(), None)
    }

    /// Snapshot the host-shaped components of `graph_ctx` along with
    /// the per-request projection map, YIELD list, and planner-expected
    /// output schema. Used by the DataFusion procedure dispatcher
    /// (`procedure_call.rs::execute_plugin_procedure`) to feed search
    /// procedures (`uni.vector.query` etc.) everything they need to
    /// expand `node` yields into the planner-expected column shape.
    #[must_use]
    pub fn from_graph_ctx_with_request(
        graph_ctx: &GraphExecutionContext,
        target_properties: HashMap<String, Vec<String>>,
        yield_items: Vec<(String, Option<String>)>,
        expected_schema: Option<SchemaRef>,
    ) -> Self {
        Self {
            storage: Arc::clone(graph_ctx.storage()),
            algo_registry: graph_ctx.algo_registry().cloned(),
            procedure_registry: graph_ctx.procedure_registry().cloned(),
            xervo_runtime: graph_ctx.xervo_runtime().cloned(),
            property_manager: Some(Arc::clone(graph_ctx.property_manager())),
            l0_context: graph_ctx.l0_context().clone(),
            counters: graph_ctx.counters().cloned(),
            deadline: graph_ctx.deadline_for_host(),
            cancellation_token: graph_ctx.cancellation_token_for_host(),
            target_properties,
            yield_items,
            expected_schema,
            transient_counter: Arc::new(AtomicU64::new(0)),
            writer: None,
            trigger_depth: 0,
        }
    }

    /// Construct a host from raw components (used by the simple
    /// executor, which holds these directly rather than via a
    /// `GraphExecutionContext`).
    #[must_use]
    pub fn from_components(
        storage: Arc<StorageManager>,
        algo_registry: Option<Arc<AlgorithmRegistry>>,
        procedure_registry: Option<Arc<ProcedureRegistry>>,
    ) -> Self {
        Self {
            storage,
            algo_registry,
            procedure_registry,
            xervo_runtime: None,
            property_manager: None,
            l0_context: L0Context::empty(),
            deadline: None,
            counters: None,
            cancellation_token: None,
            target_properties: HashMap::new(),
            yield_items: Vec::new(),
            expected_schema: None,
            transient_counter: Arc::new(AtomicU64::new(0)),
            writer: None,
            trigger_depth: 0,
        }
    }

    /// Construct a write-enabled host from the transaction commit path
    /// (WS-A), so a declared trigger's after-commit Cypher action can run
    /// against the same storage / property-manager / L0 snapshot the
    /// commit just produced.
    ///
    /// `l0_context` should carry `current_l0 =
    /// writer.l0_manager.get_current()` (the just-committed main L0),
    /// the pending-flush L0s, and — for read-your-writes fidelity — the
    /// committing transaction's private `tx_l0` as `transaction_l0`.
    /// Attach the writer via [`Self::with_writer`] to enable write-mode
    /// action bodies. `trigger_depth` is the re-entrancy depth this host
    /// runs at (see [`Self::MAX_TRIGGER_DEPTH`]); the commit path passes
    /// the depth carried by the host that produced this commit, or `0`
    /// for a top-level commit.
    #[must_use]
    pub fn from_commit_parts(
        storage: Arc<StorageManager>,
        property_manager: Arc<PropertyManager>,
        procedure_registry: Option<Arc<ProcedureRegistry>>,
        xervo_runtime: Option<Arc<ModelRuntime>>,
        l0_context: L0Context,
        trigger_depth: u32,
    ) -> Self {
        Self {
            storage,
            algo_registry: None,
            procedure_registry,
            xervo_runtime,
            property_manager: Some(property_manager),
            l0_context,
            deadline: None,
            counters: None,
            cancellation_token: None,
            target_properties: HashMap::new(),
            yield_items: Vec::new(),
            expected_schema: None,
            transient_counter: Arc::new(AtomicU64::new(0)),
            writer: None,
            trigger_depth,
        }
    }

    /// Re-entrancy depth this host runs its action body at (WS-A R1).
    /// `0` for a normal query / procedure host; `>= 1` inside a declared
    /// trigger's action.
    #[must_use]
    pub fn trigger_depth(&self) -> u32 {
        self.trigger_depth
    }

    /// Attach the outer transaction's writer handle to this host.
    ///
    /// Required for `Write`/`Schema`/`Dbms`-mode invocations of
    /// [`Self::execute_inner_query`]. Call sites that construct a host
    /// inside a write transaction should thread the writer through; the
    /// inner-query path otherwise has no path to mutate the graph.
    #[must_use]
    pub fn with_writer(mut self, writer: Arc<Writer>) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Attach the outer query's L0 visibility snapshot to this host.
    ///
    /// [`Self::from_components`] starts from [`L0Context::empty`] because the
    /// simple-executor path holds no `GraphExecutionContext`. Any call site that
    /// has a `QueryContext` in hand must thread it through here, or this host's
    /// inner queries ([`Self::execute_inner_query`]) read flushed storage only
    /// and silently miss committed-but-unflushed rows — while the *outer* query
    /// that built them sees the full state. That asymmetry is a wrong answer
    /// that depends on which projection mode the caller happened to use.
    #[must_use]
    pub fn with_l0_context(mut self, l0_context: L0Context) -> Self {
        self.l0_context = l0_context;
        self
    }

    /// Allocate a fresh transient id, unique within this host's
    /// lifetime. Wraps the bottom 63 bits and OR-s in the ephemeral
    /// bit before returning. Use `Vid::ephemeral` / `Eid::ephemeral`
    /// when you want the typed `Vid` / `Eid` form.
    ///
    /// Always available — no capability is required. Per proposal
    /// §4.13.1, IDs are stable only within a single query execution.
    #[must_use]
    pub fn allocate_transient_id(&self) -> u64 {
        // Bottom 63 bits only (mask in case of wraparound on a long
        // run); the high bit is OR'd by `Vid::ephemeral` / `Eid::ephemeral`.
        self.transient_counter.fetch_add(1, Ordering::Relaxed) & !(1u64 << 63)
    }

    /// Storage manager — schema, datasets, vector / fts search.
    #[must_use]
    pub fn storage(&self) -> &Arc<StorageManager> {
        &self.storage
    }

    /// Algorithm registry, if the host wired one in.
    #[must_use]
    pub fn algo_registry(&self) -> Option<&Arc<AlgorithmRegistry>> {
        self.algo_registry.as_ref()
    }

    /// Procedure registry, if the host wired one in.
    #[must_use]
    pub fn procedure_registry(&self) -> Option<&Arc<ProcedureRegistry>> {
        self.procedure_registry.as_ref()
    }

    /// Uni-Xervo runtime for query-time auto-embedding, if wired.
    #[must_use]
    pub fn xervo_runtime(&self) -> Option<&Arc<ModelRuntime>> {
        self.xervo_runtime.as_ref()
    }

    /// Property manager for lazy property loading, if the host wired
    /// one in. Returns `None` on the simple-executor path
    /// (`from_components` does not have access to it).
    #[must_use]
    pub fn property_manager(&self) -> Option<&Arc<PropertyManager>> {
        self.property_manager.as_ref()
    }

    /// Per-request projection map (output variable name → requested
    /// property names). Empty unless the host was constructed via
    /// [`Self::from_graph_ctx_with_request`] with non-empty data.
    #[must_use]
    pub fn target_properties(&self) -> &HashMap<String, Vec<String>> {
        &self.target_properties
    }

    /// Per-request YIELD list as `(yield_name, alias)` pairs.
    #[must_use]
    pub fn yield_items(&self) -> &[(String, Option<String>)] {
        &self.yield_items
    }

    /// Planner-expected output schema. Used by search procedures to
    /// emit columns matching the schema the surrounding query plan
    /// expects, avoiding a name-mismatch reprojection step in the
    /// dispatcher.
    #[must_use]
    pub fn expected_schema(&self) -> Option<&SchemaRef> {
        self.expected_schema.as_ref()
    }

    /// L0 visibility context (current / pending / transaction buffers).
    #[must_use]
    pub fn l0_context(&self) -> &L0Context {
        &self.l0_context
    }

    /// Build a `QueryContext` for property-manager calls. Mirrors
    /// [`GraphExecutionContext::query_context`].
    #[must_use]
    pub fn query_context(&self) -> QueryContext {
        use parking_lot::RwLock;
        use uni_store::runtime::l0::L0Buffer;

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
        if let Some(counters) = self.counters.clone() {
            ctx.set_counters(counters);
        }
        ctx
    }

    /// Run an inner Cypher query against the same storage / L0
    /// snapshot the outer procedure sees, returning the materialised
    /// row vector.
    ///
    /// Used by:
    ///
    /// - the V2 algorithm adapter (M5c.3) to materialise
    ///   `ProjectionInput::Cypher { node_query, edge_query, ... }`;
    /// - the meta-plugin persistence backend (M9 cutover) to issue
    ///   `MERGE (:_DeclaredPlugin {...})` through Cypher;
    /// - the synthetic-procedure plugin (M9 cutover) to evaluate the
    ///   stored body of a `CALL uni.plugin.declareProcedure(...)`.
    ///
    /// `mode` controls which Cypher operations are accepted:
    ///
    /// - [`ProcedureMode::Read`] constructs the inner executor without
    ///   a writer; mutation clauses (`CREATE`, `SET`, `MERGE`,
    ///   `DELETE`, `REMOVE`) fail with "Database is in read-only mode".
    /// - [`ProcedureMode::Write`] / [`ProcedureMode::Schema`] /
    ///   [`ProcedureMode::Dbms`] construct the inner executor with the
    ///   outer transaction's writer handle (set via
    ///   [`Self::with_writer`]); mutations land in the outer
    ///   transaction's L0 buffer. If no writer was threaded through,
    ///   write-mode invocations error with `"inner write requires a
    ///   writer-enabled procedure host"`.
    ///
    /// L0 visibility mirrors the outer query's snapshot
    /// (`l0_context.current_l0` / `transaction_l0` /
    /// `pending_flush_l0s`) so recently-written rows are visible. The
    /// `PropertyManager` is reused from the outer host when present;
    /// otherwise a fresh per-call one is constructed.
    ///
    /// `params` are bound into the inner executor by name, exactly as
    /// `session.query(cypher, params)` would for a top-level Cypher
    /// query.
    ///
    /// # Errors
    ///
    /// Returns any parse / plan / execution error from the inner
    /// query. Write-attempt errors in `Read` mode propagate as the
    /// host's "Database is in read-only mode" string. Write-mode
    /// invocations without a writer attached return a clear error
    /// rather than silently downgrading to read-only.
    pub async fn execute_inner_query(
        &self,
        cypher: &str,
        params: &HashMap<String, uni_common::Value>,
        mode: ProcedureMode,
    ) -> anyhow::Result<Vec<HashMap<String, uni_common::Value>>> {
        use uni_store::runtime::l0_manager::L0Manager;
        use uni_store::runtime::property_manager::PropertyManager as PM;

        use crate::query::executor::Executor;
        use crate::query::planner::QueryPlanner;

        let needs_writer = !matches!(mode, ProcedureMode::Read);
        let mut executor = if needs_writer {
            let writer = self.writer.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "inner write requires a writer-enabled procedure host \
                     (mode = {mode:?}); call QueryProcedureHost::with_writer \
                     at construction time"
                )
            })?;
            Executor::new_with_writer(Arc::clone(&self.storage), Arc::clone(writer))
        } else {
            Executor::new(Arc::clone(&self.storage))
        };

        // Mirror outer L0 visibility into the inner executor.
        if let Some(current) = self.l0_context.current_l0.as_ref() {
            let mut pending = self.l0_context.pending_flush_l0s.clone();
            if let Some(tx_l0) = &self.l0_context.transaction_l0 {
                pending.push(tx_l0.clone());
            }
            executor.l0_manager =
                Some(Arc::new(L0Manager::from_snapshot(current.clone(), pending)));
        }

        let schema_manager_arc = self.storage.schema_manager_arc();
        let schema = self.storage.schema_manager().schema();
        let planner = QueryPlanner::new(schema);
        let ast = uni_cypher::parse(cypher)?;
        let plan = planner.plan(ast)?;

        let prop_manager = if let Some(pm) = &self.property_manager {
            Arc::clone(pm)
        } else {
            Arc::new(PM::new(Arc::clone(&self.storage), schema_manager_arc, 100))
        };

        executor.execute(plan, &prop_manager, params).await
    }

    /// Check whether the query has timed out or been cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if the deadline has passed or the cancellation
    /// token has been triggered.
    pub fn check_timeout(&self) -> anyhow::Result<()> {
        crate::query::df_graph::common::check_deadline(
            self.cancellation_token.as_ref(),
            self.deadline,
        )
    }
}

impl ProcedureHost for QueryProcedureHost {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
