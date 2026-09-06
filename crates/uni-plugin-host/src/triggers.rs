// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host-side dispatch for `TriggerPlugin` registrations (M5f).
//!
//! Bridges `PluginRegistry::triggers()` into the transaction commit
//! path. The dispatcher builds a per-phase routing table once per
//! commit, drains mutation events from the transaction's private L0
//! buffer into a stable Arrow `RecordBatch`, applies subscription
//! selectors (event-kind mask + label / edge-type / property filter),
//! and invokes each matching trigger at the appropriate phase.
//!
//! Phase ordering inside a single commit:
//! 1. `BeforeMutation` then `BeforeCommit` — fired before the writer
//!    lock is taken. `Synchronous` reject aborts the transaction.
//! 2. WAL flush + L1 merge run.
//! 3. `AfterMutation` then `AfterCommit` — fired after publish. `Async`
//!    fire-mode triggers are spawned onto the tokio runtime so the
//!    writer's hot path stays untouched.
//!
//! Behavior contract:
//! - `predicate_source` is compiled at router build (per-commit) via
//!   `uni_cypher::parse_expression` → AST property-ref rewrite →
//!   `cypher_expr_to_df` → DataFusion `PhysicalExpr`, and evaluated
//!   against the per-row event batch in `filter_for`. Predicates may
//!   reference the event-row columns (`event_kind`, `vid_or_eid`,
//!   `label`, `property`, `old_value`, `new_value`) as well as
//!   per-entity properties: `n.foo` reads the new (post-mutation)
//!   property value, `old.foo` reads the pre-image. Referenced
//!   property keys are tracked in `RouteEntry::properties_referenced`
//!   so [`MutationEvents::from_l0_with_probe`] materializes exactly
//!   those keys into the per-row property bags — predicate-gated
//!   cost, no work for property-free predicates.
//! - `TriggerOutcome::Defer` enqueues the trigger fire into the
//!   per-`Uni` [`DeferralQueue`], ticked at 50ms by the background
//!   task spawned in `Uni::build`. Items re-fire on the next tick;
//!   re-deferring is capped at `DEFER_MAX_ATTEMPTS`. When built with
//!   [`DeferralQueue::with_persistence`] the queue mirrors to a JSON
//!   sidecar (FU-5) and reloads on restart via `load_from_sidecar`.
//! - `NODE_CREATE` / `NODE_UPDATE` / `NODE_DELETE` (and the edge
//!   analogs) are distinguished via a committed-state probe
//!   ([`PreExistingProbe`]) passed to
//!   [`MutationEvents::from_l0_with_probe`]. The probe covers (a) the
//!   current L0 buffer + pending-flush L0s via
//!   [`PreExistingProbe::from_l0_chain`] (sync, no I/O) and (b) the
//!   L1 storage layer via [`PreExistingProbe::extend_with_l1`] (async,
//!   batched `_vid IN (…)` scan per label, chunked at 1024 VIDs).
//!   Callers that construct [`MutationEvents`] without a probe
//!   ([`MutationEvents::from_l0`]) fall back to emitting `NODE_UPDATE`
//!   / `EDGE_UPDATE` for every non-tombstoned write.
//! - `old_value` is populated from the L0-chain probe for vertices
//!   and edges visible there, and from the L1 probe (which now
//!   projects every property column on the candidate label) for
//!   vertices that were drained out of L0 by a previous flush. Edge
//!   pre-images are captured in the L0 chain via `edge_properties`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use arrow_array::{BooleanArray, Int64Array, LargeBinaryArray, RecordBatch, UInt8Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::PhysicalExpr;
use tokio::runtime::Handle;
use tracing::warn;
use uni_common::cypher_value_codec;
use uni_common::{Properties, UniError, Value};
use uni_plugin::PluginRegistry;
use uni_plugin::traits::procedure::ProcedureHost;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_store::runtime::L0Manager;
use uni_store::runtime::l0::L0Buffer;

/// Number of distinct `TriggerPhase` variants (`BeforeMutation`,
/// `AfterMutation`, `BeforeCommit`, `AfterCommit`).
const PHASE_COUNT: usize = 4;

/// Canonical Arrow schema for the per-row event batch handed to each
/// `TriggerPlugin::fire` call. Kept in one place so `filter_for` and
/// the `predicate_source` compiler agree on column names + types.
///
/// Also used by the CDC delivery path (M11 FU-4) so subscribers
/// receive events in the same shape triggers do.
pub fn event_row_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_kind", DataType::UInt8, false),
        Field::new("vid_or_eid", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("property", DataType::Utf8, false),
        Field::new("old_value", DataType::LargeBinary, true),
        Field::new("new_value", DataType::LargeBinary, true),
        // Per-row property bags carrying a CypherValue-encoded
        // `Value::Map` of the (selected) post-mutation and pre-image
        // property values. The Cypher predicate compiler rewrites
        // `n.foo` / `old.foo` references against these columns, which
        // the existing `index(map, key)` UDF handles via the
        // CypherValue codec — no bespoke map-access path required.
        Field::new("properties_new", DataType::LargeBinary, true),
        Field::new("properties_old", DataType::LargeBinary, true),
    ]))
}

/// Compile a Cypher boolean expression (`predicate_source`) into a
/// DataFusion `PhysicalExpr` that evaluates against [`event_row_schema`],
/// together with the set of node/edge property keys the predicate
/// references (used downstream to predicate-gate property-bag
/// materialization).
///
/// Pipeline: `uni_cypher::parse_expression` → in-place AST rewrite of
/// `n.foo` / `old.foo` into `properties_new.foo` / `properties_old.foo`
/// → `cypher_expr_to_df` (whose property-access translator emits
/// `index(col, "foo")` for non-graph-entity bases — the existing
/// `index` UDF then performs map lookup on the CypherValue-encoded
/// `LargeBinary` bag) → DataFusion `TypeCoercion` →
/// `create_physical_expr`. Same pattern as `apply_having_filter` in
/// `crates/uni-query/src/query/df_graph/locy_fixpoint.rs:2734-2810`,
/// just narrowed to a single expression against a fixed schema.
///
/// # Errors
///
/// Returns an error string if the predicate fails to parse, references
/// columns not present in the event-row schema (event-row columns or
/// `n.<prop>` / `old.<prop>` property references), or fails type
/// coercion.
fn compile_predicate(source: &str) -> Result<(Arc<dyn PhysicalExpr>, HashSet<String>), String> {
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::prelude::SessionContext;

    let mut cypher_expr =
        uni_cypher::parse_expression(source).map_err(|e| format!("parse: {e}"))?;
    let mut props_referenced: HashSet<String> = HashSet::new();
    rewrite_property_refs(&mut cypher_expr, &mut props_referenced);
    let df_expr_raw = uni_query::query::df_expr::cypher_expr_to_df(&cypher_expr, None)
        .map_err(|e| format!("translate: {e}"))?;

    let schema = event_row_schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone())
        .map_err(|e| format!("schema-conversion: {e}"))?;

    let ctx = SessionContext::new();
    // Register Cypher UDFs (`index`, `_cypher_gt`, ...) so (a) UDF
    // resolution below can swap placeholder `DummyUdf` nodes (which
    // declare `return_type = Null`) for the real impls (which declare
    // `LargeBinary` etc.), and (b) the resulting physical-expr can
    // invoke them at evaluation time.
    uni_query::query::df_udfs::register_cypher_udfs(&ctx)
        .map_err(|e| format!("udf-register: {e}"))?;
    let state = ctx.state();
    let config = state.config_options().clone();
    let props = state.execution_props();

    // Resolve UDFs first so the type-system sees the *real* return
    // types (e.g. `index` → LargeBinary) when the Cypher coercion pass
    // below decides whether `LargeBinary > Int64` needs to be rewritten
    // to `_cypher_gt`. Without this, `apply_type_coercion` sees Null and
    // routes through the bogus cast-to-Int64 path.
    let df_expr_resolved = resolve_dummy_udfs(df_expr_raw, &state)
        .map_err(|e| format!("resolve-udfs (pre-coerce): {e}"))?;

    // Apply Cypher-aware type coercion: rewrites `LargeBinary <op>
    // <native>` (e.g. `index(properties_new, "balance") > 100`) into
    // `_cypher_gt(left, right)` so the property-bag access path works
    // for native operands.
    let df_expr = uni_query::query::df_expr::apply_type_coercion(&df_expr_resolved, &df_schema)
        .map_err(|e| format!("cypher-coercion: {e}"))?;

    // Wrap in a Filter plan so TypeCoercion can align literals against
    // the event-row column types (e.g. `event_kind = 1` coerces `1`
    // from Int64 literal to UInt8 to match the column).
    let empty = datafusion::logical_expr::LogicalPlan::EmptyRelation(
        datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema.clone()),
        },
    );
    let filter_plan = LogicalPlanBuilder::from(empty)
        .filter(df_expr.clone())
        .map_err(|e| format!("filter-plan: {e}"))?
        .build()
        .map_err(|e| format!("plan-build: {e}"))?;
    let coerced_expr = match TypeCoercion::new().analyze(filter_plan, &config) {
        Ok(datafusion::logical_expr::LogicalPlan::Filter(f)) => f.predicate,
        _ => df_expr,
    };

    // Resolve placeholder `DummyUdf` scalar-function nodes (produced by
    // `cypher_expr_to_df` / `apply_type_coercion`) into the real UDF
    // impls registered on the SessionContext. Mirrors
    // `QueryExecutor::resolve_udfs` (`df_planner.rs:5168`) — without
    // this pass, `index` and `_cypher_gt` evaluation fails at runtime
    // with "UDF '<name>' is not registered".
    let resolved_expr =
        resolve_dummy_udfs(coerced_expr, &state).map_err(|e| format!("resolve-udfs: {e}"))?;

    let physical = create_physical_expr(&resolved_expr, &df_schema, props)
        .map_err(|e| format!("physical-expr: {e}"))?;
    Ok((physical, props_referenced))
}

/// Walk `expr` and replace every `ScalarFunction` whose name matches a
/// UDF registered on `state.scalar_functions()` with the registered
/// implementation. The Cypher translator (`cypher_expr_to_df`) emits
/// placeholder `DummyUdf` wrappers carrying only the name; the real
/// `IndexUdf` / `_cypher_gt` / ... impls live on the SessionContext.
fn resolve_dummy_udfs(
    expr: datafusion::logical_expr::Expr,
    state: &datafusion::execution::SessionState,
) -> Result<datafusion::logical_expr::Expr, String> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::Expr as DfExpr;

    let result = expr
        .transform_up(|node| {
            if let DfExpr::ScalarFunction(ref func) = node {
                let udf_name = func.func.name();
                if let Some(registered_udf) = state.scalar_functions().get(udf_name) {
                    return Ok(Transformed::yes(DfExpr::ScalarFunction(
                        datafusion::logical_expr::expr::ScalarFunction {
                            func: registered_udf.clone(),
                            args: func.args.clone(),
                        },
                    )));
                }
            }
            Ok(Transformed::no(node))
        })
        .map_err(|e| format!("udf-resolve walk: {e}"))?;
    Ok(result.data)
}

/// Walk a parsed Cypher expression and rewrite property references on
/// the canonical entity aliases (`n` for the post-mutation row,
/// `old` for the pre-image) so they resolve against the per-row
/// `properties_new` / `properties_old` columns of [`event_row_schema`].
///
/// `n.foo` → `properties_new.foo` (translates downstream to
/// `index(col("properties_new"), "foo")` via the standard
/// non-graph-entity property-access path in `cypher_expr_to_df`).
/// `old.foo` → `properties_old.foo`. All referenced property names
/// are collected into `referenced` for predicate-gated materialization
/// in [`MutationEvents::from_l0_with_probe`].
///
/// Other Cypher expressions are walked recursively so a predicate like
/// `n.balance > 100 AND old.status <> n.status` is fully rewritten.
fn rewrite_property_refs(expr: &mut uni_cypher::ast::Expr, referenced: &mut HashSet<String>) {
    use uni_cypher::ast::Expr;
    match expr {
        Expr::Property(base, prop) => {
            // First recurse into the base — supports chained access like
            // `n.address.city` (the inner `n.address` is rewritten to
            // `properties_new.address`, then `index(...)` chains).
            rewrite_property_refs(base, referenced);
            if let Expr::Variable(name) = base.as_ref() {
                match name.as_str() {
                    "n" => {
                        referenced.insert(prop.clone());
                        **base = Expr::Variable("properties_new".to_owned());
                    }
                    "old" => {
                        referenced.insert(prop.clone());
                        **base = Expr::Variable("properties_old".to_owned());
                    }
                    _ => {}
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_property_refs(left, referenced);
            rewrite_property_refs(right, referenced);
        }
        Expr::UnaryOp { expr: inner, .. } => rewrite_property_refs(inner, referenced),
        Expr::FunctionCall { args, .. } => {
            for a in args {
                rewrite_property_refs(a, referenced);
            }
        }
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = case_expr.as_deref_mut() {
                rewrite_property_refs(e, referenced);
            }
            for (w, t) in when_then {
                rewrite_property_refs(w, referenced);
                rewrite_property_refs(t, referenced);
            }
            if let Some(e) = else_expr.as_deref_mut() {
                rewrite_property_refs(e, referenced);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::IsUnique(inner) => {
            rewrite_property_refs(inner, referenced);
        }
        Expr::In { expr: e, list } => {
            rewrite_property_refs(e, referenced);
            rewrite_property_refs(list, referenced);
        }
        Expr::List(items) => {
            for i in items {
                rewrite_property_refs(i, referenced);
            }
        }
        Expr::Map(pairs) => {
            for (_, v) in pairs {
                rewrite_property_refs(v, referenced);
            }
        }
        Expr::ArrayIndex { array, index } => {
            rewrite_property_refs(array, referenced);
            rewrite_property_refs(index, referenced);
        }
        Expr::ArraySlice { array, start, end } => {
            rewrite_property_refs(array, referenced);
            if let Some(s) = start.as_deref_mut() {
                rewrite_property_refs(s, referenced);
            }
            if let Some(e) = end.as_deref_mut() {
                rewrite_property_refs(e, referenced);
            }
        }
        // Literal / Parameter / Variable / Wildcard / subquery variants
        // do not carry rewritable property refs at the surface level.
        _ => {}
    }
}

fn phase_index(p: TriggerPhase) -> usize {
    // `TriggerPhase` is `#[non_exhaustive]` — fall back to BeforeMutation
    // bucket so a future variant can't silently slot into an existing
    // route's phase by accident.
    match p {
        TriggerPhase::BeforeMutation => 0,
        TriggerPhase::AfterMutation => 1,
        TriggerPhase::BeforeCommit => 2,
        TriggerPhase::AfterCommit => 3,
        _ => 0,
    }
}

/// A single route in the per-phase dispatch table.
struct RouteEntry {
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    /// Index of this trigger among all registered triggers that share the same
    /// `name` (in registry order). Two triggers with identical docs derive the
    /// same `subscription_name`, so the name alone cannot re-bind a persisted
    /// deferral to the right plugin on restart; the (name, ordinal) pair does,
    /// as long as registration order is deterministic (it is — same plugin code).
    name_ordinal: usize,
    event_mask: u32,
    label_filter: Option<Vec<String>>,
    edge_type_filter: Option<Vec<String>>,
    property_filter: Option<Vec<String>>,
    fire_mode: FireMode,
    /// Compiled `predicate_source` expression, evaluated per-row in
    /// `filter_for` to drop rows where the predicate is false. `None`
    /// when the subscription has no predicate. The compile is done
    /// once per [`TriggerRouter::from_registry`] call.
    compiled_predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Property names that the compiled predicate references via
    /// `n.<prop>` or `old.<prop>`. Used to predicate-gate the
    /// property-bag materialization in
    /// [`MutationEvents::from_l0_with_probe`] — when this set is
    /// empty the event-row pipeline does no per-property work for
    /// this route.
    properties_referenced: HashSet<String>,
}

impl RouteEntry {
    fn matches(&self, kind: TriggerEventMask, label_or_type: &str) -> bool {
        if (self.event_mask & kind.0) == 0 {
            return false;
        }
        if let Some(ref labels) = self.label_filter
            && kind_is_node(kind)
            && !labels.iter().any(|l| l.as_str() == label_or_type)
        {
            return false;
        }
        if let Some(ref ets) = self.edge_type_filter
            && kind_is_edge(kind)
            && !ets.iter().any(|e| e.as_str() == label_or_type)
        {
            return false;
        }
        true
    }
}

fn kind_is_node(kind: TriggerEventMask) -> bool {
    let mask = TriggerEventMask::NODE_CREATE
        .union(TriggerEventMask::NODE_UPDATE)
        .union(TriggerEventMask::NODE_DELETE)
        .union(TriggerEventMask::LABEL_ADDED)
        .union(TriggerEventMask::LABEL_REMOVED);
    (kind.0 & mask.0) != 0
}

fn kind_is_edge(kind: TriggerEventMask) -> bool {
    let mask = TriggerEventMask::EDGE_CREATE
        .union(TriggerEventMask::EDGE_UPDATE)
        .union(TriggerEventMask::EDGE_DELETE);
    (kind.0 & mask.0) != 0
}

/// Per-commit trigger dispatcher.
pub struct TriggerRouter {
    by_phase: [Vec<RouteEntry>; PHASE_COUNT],
    /// Per-`Uni` deferral queue. `None` for read-only / test setups
    /// without a queue — `TriggerOutcome::Defer` then falls back to
    /// the legacy warn-and-collapse behavior.
    defer_queue: Option<Arc<DeferralQueue>>,
    /// Per-`Uni` EventualConsistency coalescing queue (WS-E). Shared by
    /// `Arc` from `UniInner` (buckets must survive across per-commit
    /// router rebuilds). `EventualConsistency` after-phase fires buffer
    /// here instead of spawning per-commit like `Async`.
    ec_queue: Arc<EcQueue>,
}

impl TriggerRouter {
    /// Snapshot the registered triggers into a routing table.
    ///
    /// Cheap for predicate-less subscriptions (one `Arc` clone for the
    /// trigger vector, then one pass to bucket by phase). For
    /// subscriptions carrying `predicate_source`, compiles the Cypher
    /// predicate into a DataFusion `PhysicalExpr` once and stashes it
    /// on the route — sub-millisecond per predicate, amortized against
    /// commit overhead.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::TriggerRejected`] (with a descriptive
    /// `reason`) if any subscription's `predicate_source` fails to
    /// parse, references unknown columns, or fails type coercion. The
    /// error surfaces at commit time, not at registration — this is a
    /// deliberate trade-off to keep `uni-plugin` free of a `uni-cypher`
    /// dependency.
    pub fn from_registry(reg: &PluginRegistry) -> Result<Self, UniError> {
        // No wired queues: deferrals warn-and-drop and EventualConsistency
        // buffers into an unwired `EcQueue` (a no-op on flush). Matches the
        // legacy read-only / test fallback for the deferral queue.
        Self::from_registry_with_queue(
            reg,
            None,
            EcQueue::new(None, Duration::from_secs(1), 10_000),
        )
    }

    /// Variant that wires in a per-`Uni` deferral queue so
    /// `TriggerOutcome::Defer` enqueues for re-firing instead of
    /// being warned and dropped.
    ///
    /// # Errors
    ///
    /// Same as [`Self::from_registry`].
    pub fn from_registry_with_queue(
        reg: &PluginRegistry,
        defer_queue: Option<Arc<DeferralQueue>>,
        ec_queue: Arc<EcQueue>,
    ) -> Result<Self, UniError> {
        let triggers = reg.triggers();
        let mut by_phase: [Vec<RouteEntry>; PHASE_COUNT] =
            [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        // Running per-name counter (registry order) → each trigger's ordinal among
        // its same-named siblings. Matched at reload (load_from_sidecar walks the
        // same registry order), so a persisted deferral re-binds to the exact
        // trigger that created it even when names collide.
        let mut name_counts: HashMap<String, usize> = HashMap::new();
        for plugin in triggers.iter() {
            let sub: &TriggerSubscription = plugin.subscription();
            let name = subscription_name(sub);
            let name_ordinal = {
                let c = name_counts.entry(name.clone()).or_insert(0);
                let ordinal = *c;
                *c += 1;
                ordinal
            };
            let (compiled_predicate, properties_referenced) = match sub.predicate_source.as_deref()
            {
                Some(src) => {
                    let (expr, refs) =
                        compile_predicate(src).map_err(|e| UniError::TriggerRejected {
                            trigger: name.clone(),
                            reason: format!(
                                "predicate_source compile failed: {e}. \
                                 Supported references: event-row columns \
                                 (event_kind, vid_or_eid, label, property, \
                                 old_value, new_value) and entity property \
                                 references `n.<prop>` (post-mutation) / \
                                 `old.<prop>` (pre-image)."
                            ),
                        })?;
                    (Some(expr), refs)
                }
                None => (None, HashSet::new()),
            };
            let entry = RouteEntry {
                plugin: Arc::clone(plugin),
                name,
                name_ordinal,
                event_mask: sub.events.0,
                label_filter: sub
                    .labels
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.to_string()).collect()),
                edge_type_filter: sub
                    .edge_types
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.to_string()).collect()),
                property_filter: sub
                    .properties
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.to_string()).collect()),
                fire_mode: sub.fire_mode,
                compiled_predicate,
                properties_referenced,
            };
            by_phase[phase_index(sub.phase)].push(entry);
        }
        Ok(Self {
            by_phase,
            defer_queue,
            ec_queue,
        })
    }

    /// True if no triggers are registered at any phase.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_phase.iter().all(|v| v.is_empty())
    }

    /// Union of node/edge property names that any compiled trigger
    /// predicate references (across all phases). Empty when no
    /// trigger has a `predicate_source` mentioning `n.<prop>` /
    /// `old.<prop>`. Drives predicate-gated property-bag
    /// materialization in [`MutationEvents::from_l0_with_probe`].
    #[must_use]
    pub fn properties_referenced(&self) -> HashSet<String> {
        self.by_phase
            .iter()
            .flatten()
            .flat_map(|entry| entry.properties_referenced.iter().cloned())
            .collect()
    }

    /// Fire `BeforeMutation` then `BeforeCommit` phases in order.
    ///
    /// Returns `Err(UniError::TriggerRejected)` if a `Synchronous`
    /// trigger returns `TriggerOutcome::Reject` or `Err`. `Async` /
    /// `EventualConsistency` triggers are ignored at before-phases
    /// (they ride on after-phases only — firing async work pre-commit
    /// would let it observe a transaction that subsequently aborts).
    ///
    /// # Errors
    ///
    /// `UniError::TriggerRejected` on reject or fire error.
    pub fn dispatch_before(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationEvents,
    ) -> Result<(), UniError> {
        // Owned host handle (WS-A), cloned into each per-fire context so
        // a declared trigger's action body can reach the write-enabled
        // host. `None` for native-only setups. (Declared triggers are
        // AfterCommit + Async and never actually fire in the before
        // phase, but thread the host through for uniformity.)
        let host = ctx.host().cloned();
        // Deferrals are BUFFERED here and only committed to the queue once ALL
        // synchronous before-triggers have run without a reject/error. If a later
        // trigger rejects (returning `Err`), the transaction aborts and these
        // buffered deferrals are dropped — otherwise a deferred item would later
        // fire with the mutations of a transaction that never committed.
        #[allow(clippy::type_complexity)]
        let mut pending_deferrals: Vec<(
            Arc<dyn TriggerPlugin>,
            String,
            usize,
            MutationBatch,
            String,
            u64,
            uni_plugin::traits::trigger::TriggerDeferral,
        )> = Vec::new();
        for &phase in &[TriggerPhase::BeforeMutation, TriggerPhase::BeforeCommit] {
            let routes = &self.by_phase[phase_index(phase)];
            for entry in routes {
                if !matches!(entry.fire_mode, FireMode::Synchronous) {
                    continue;
                }
                // A failure here must abort the transaction rather than skip
                // the trigger: a synchronous before-trigger that could not be
                // evaluated has not approved the mutation (#233).
                let filtered = events
                    .filter_for(entry)
                    .map_err(|e| UniError::TriggerRejected {
                        trigger: entry.name.to_string(),
                        reason: format!("could not evaluate trigger selection: {e}"),
                    })?;
                let Some(batch) = filtered else {
                    continue;
                };
                let mb = MutationBatch {
                    events: Arc::new(batch),
                };
                let mut ctx_ref = TriggerContext::new(ctx.session_id, ctx.tx_id);
                if let Some(h) = host.as_ref() {
                    ctx_ref = ctx_ref.with_host(Arc::clone(h));
                }
                match entry.plugin.fire(ctx_ref, &mb) {
                    Ok(TriggerOutcome::Continue) => {}
                    Ok(TriggerOutcome::Reject { reason }) => {
                        return Err(UniError::TriggerRejected {
                            trigger: entry.name.to_string(),
                            reason,
                        });
                    }
                    Ok(TriggerOutcome::Defer { until }) => {
                        // Buffer the deferral (see the note above); it is committed
                        // to the queue only after the whole before-dispatch
                        // succeeds. FU-5 adds an optional `delay` to
                        // `TriggerDeferral`; `None` re-fires on the next queue tick,
                        // `Some(d)` schedules at `now + d`.
                        pending_deferrals.push((
                            Arc::clone(&entry.plugin),
                            entry.name.clone(),
                            entry.name_ordinal,
                            mb.clone(),
                            ctx.session_id.to_owned(),
                            ctx.tx_id,
                            until,
                        ));
                    }
                    Ok(_) => {
                        // `TriggerOutcome` is `#[non_exhaustive]`; an
                        // unrecognised future variant is conservatively
                        // treated as Continue.
                    }
                    Err(e) => {
                        return Err(UniError::TriggerRejected {
                            trigger: entry.name.to_string(),
                            reason: e.to_string(),
                        });
                    }
                }
            }
        }
        // All before-triggers passed (no reject) — the transaction is cleared to
        // commit, so commit the buffered deferrals now.
        for (plugin, name, name_ordinal, mb, session_id, tx_id, until) in pending_deferrals {
            enqueue_deferral(
                &self.defer_queue,
                plugin,
                name,
                name_ordinal,
                mb,
                session_id,
                tx_id,
                until,
            );
        }
        Ok(())
    }

    /// Fire `AfterMutation` then `AfterCommit` phases. Cannot abort.
    ///
    /// `Synchronous` after-phase triggers run inline (panics caught and
    /// logged). `Async` triggers are spawned on `runtime`.
    /// `EventualConsistency` triggers buffer into the per-`Uni`
    /// [`EcQueue`], which coalesces batches across commits and flushes a
    /// single `DeferredItem` per bucket on the deferral tick (WS-E).
    pub fn dispatch_after(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationEvents,
        runtime: &Handle,
    ) {
        // Owned host handle (WS-A). Cloned into the async spawn closure so
        // a declared trigger's after-commit action body reaches the
        // write-enabled host after the commit stack frame is gone.
        let host = ctx.host().cloned();
        for &phase in &[TriggerPhase::AfterMutation, TriggerPhase::AfterCommit] {
            let routes = &self.by_phase[phase_index(phase)];
            for entry in routes {
                // The commit is already durable, so there is nothing to
                // abort. Record at error level rather than dropping silently:
                // this trigger did not observe mutations it was registered
                // for (#233).
                let filtered = match events.filter_for(entry) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::error!(
                            trigger = %entry.name,
                            error = %e,
                            "after-trigger selection failed; trigger did not fire for this commit",
                        );
                        continue;
                    }
                };
                let Some(batch) = filtered else {
                    continue;
                };
                let mb = MutationBatch {
                    events: Arc::new(batch),
                };
                match entry.fire_mode {
                    FireMode::Synchronous => {
                        fire_caught(
                            entry,
                            ctx.session_id,
                            ctx.tx_id,
                            &mb,
                            &self.defer_queue,
                            host.as_ref(),
                        );
                    }
                    // WS-E: EventualConsistency buffers into the per-`Uni`
                    // coalescing queue instead of spawning per-commit. Due
                    // buckets flush a single coalesced fire on the deferral
                    // tick, riding the existing defer-queue durability + fire
                    // ladder. (No longer aliases `Async`.)
                    FireMode::EventualConsistency => {
                        self.ec_queue.enqueue(
                            entry,
                            ctx.session_id,
                            ctx.tx_id,
                            Arc::clone(&mb.events),
                        );
                    }
                    // `FireMode::Async` and any future variant land on the
                    // spawn path: one `spawn_blocking`-style task per matching
                    // commit, fired after the transaction lands.
                    _ => {
                        let plugin = Arc::clone(&entry.plugin);
                        let name = entry.name.clone();
                        let name_ordinal = entry.name_ordinal;
                        let session_id = ctx.session_id.to_owned();
                        let tx_id = ctx.tx_id;
                        let queue = self.defer_queue.clone();
                        let host = host.clone();
                        runtime.spawn(async move {
                            let mb_inner = mb;
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    let mut c = TriggerContext::new(&session_id, tx_id);
                                    if let Some(h) = host.as_ref() {
                                        c = c.with_host(Arc::clone(h));
                                    }
                                    plugin.fire(c, &mb_inner)
                                }));
                            handle_fire_outcome(result, &name, "async trigger", |until| {
                                enqueue_deferral(
                                    &queue,
                                    Arc::clone(&plugin),
                                    name.clone(),
                                    name_ordinal,
                                    mb_inner,
                                    session_id.clone(),
                                    tx_id,
                                    until,
                                );
                            });
                        });
                    }
                }
            }
        }
    }
}

/// Enqueue a [`TriggerOutcome::Defer`] result into the host's
/// in-memory [`DeferralQueue`]. When no queue is wired (read-only or
/// test setups) the item is dropped with a warn — matches the legacy
/// fallback behavior.
///
/// The fire instant honors `until.delay` (FU-5); `None` collapses to
/// "now" so the item fires on the next tick.
#[allow(clippy::too_many_arguments)]
fn enqueue_deferral(
    queue: &Option<Arc<DeferralQueue>>,
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    name_ordinal: usize,
    mb: MutationBatch,
    session_id: String,
    tx_id: u64,
    until: uni_plugin::traits::trigger::TriggerDeferral,
) {
    let Some(queue) = queue else {
        warn!(trigger = %name, "Defer with no queue wired; dropping");
        return;
    };
    let fire_at = StdInstant::now() + until.delay.unwrap_or(Duration::ZERO);
    queue.push(
        DeferredItem {
            plugin,
            name,
            name_ordinal,
            batch: mb,
            session_id,
            tx_id,
            attempts: 0,
            payload: until.payload,
        },
        fire_at,
    );
}

/// Dispatch the result of a `catch_unwind`-wrapped trigger fire.
///
/// All three fire paths (`dispatch_after`'s spawned task, [`fire_caught`], and
/// [`DeferralQueue::tick`]) share the same four-way ladder:
/// `Ok(Ok(Defer))` / `Ok(Ok(_))` (Continue/Reject/future) / `Ok(Err)` (the
/// plugin errored) / `Err` (the plugin panicked). They differ only in the log
/// `label` and what to do on a `Defer` — captured by `on_defer`.
fn handle_fire_outcome<E: std::fmt::Display>(
    outcome: Result<Result<TriggerOutcome, E>, Box<dyn std::any::Any + Send>>,
    name: &str,
    label: &str,
    on_defer: impl FnOnce(uni_plugin::traits::trigger::TriggerDeferral),
) {
    match outcome {
        Ok(Ok(TriggerOutcome::Defer { until })) => on_defer(until),
        Ok(Ok(_)) => {}
        Ok(Err(e)) => warn!(trigger = %name, error = %e, "{label} errored"),
        Err(_) => warn!(trigger = %name, "{label} panicked"),
    }
}

fn fire_caught(
    entry: &RouteEntry,
    session_id: &str,
    tx_id: u64,
    mb: &MutationBatch,
    defer_queue: &Option<Arc<DeferralQueue>>,
    host: Option<&Arc<dyn ProcedureHost>>,
) {
    let plugin = Arc::clone(&entry.plugin);
    let name = entry.name.clone();
    let name_ordinal = entry.name_ordinal;
    let mb_clone = mb.clone();
    let session_id_owned = session_id.to_owned();
    let host = host.cloned();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut c = TriggerContext::new(&session_id_owned, tx_id);
        if let Some(h) = host.as_ref() {
            c = c.with_host(Arc::clone(h));
        }
        plugin.fire(c, &mb_clone)
    }));
    handle_fire_outcome(result, &name, "after-phase trigger", |until| {
        enqueue_deferral(
            defer_queue,
            plugin,
            name.clone(),
            name_ordinal,
            mb_clone,
            session_id_owned,
            tx_id,
            until,
        );
    });
}

fn subscription_name(sub: &TriggerSubscription) -> String {
    // `TriggerSubscription` carries no explicit name field; use the
    // first line of the docs as a stable identifier, falling back to
    // a generic label. Keeps `UniError::TriggerRejected` human-readable
    // without an ABI bump for a name field on the subscription struct.
    sub.docs
        .lines()
        .next()
        .map(str::to_owned)
        .unwrap_or_else(|| "<unnamed trigger>".to_owned())
}

// ── Mutation event extraction ──────────────────────────────────────

/// In-memory, untyped event log drained from `tx_l0`. Held by value
/// across the commit boundary and filtered per-route on dispatch.
pub struct MutationEvents {
    rows: Vec<MutationRow>,
}

struct MutationRow {
    event_kind: TriggerEventMask,
    vid_or_eid: i64,
    /// For NODE_* events: the affected label (one row per label).
    /// For EDGE_* events: the edge type.
    label_or_type: String,
    /// Pre-image properties when known (probe was supplied and the
    /// vertex/edge existed before this tx); `None` otherwise. The
    /// `BeforeCommit` dispatch path serializes this into the
    /// `old_value` Arrow column.
    old_value: Option<Vec<u8>>,
    /// Post-mutation property map filtered to the predicate-referenced
    /// keys; serialized into the `properties_new` LargeBinary column.
    /// `None` when no trigger references any property.
    new_properties: Option<Properties>,
    /// Pre-image property map filtered to the predicate-referenced
    /// keys; serialized into the `properties_old` LargeBinary column.
    /// `None` when no trigger references any property or the entity
    /// did not pre-exist.
    old_properties: Option<Properties>,
}

/// Snapshot of the committed graph state used to (a) distinguish
/// CREATE from UPDATE in [`MutationEvents::from_l0_with_probe`] and
/// (b) populate `old_value` for vertex and edge mutation events.
///
/// Built once per commit. The cheap [`Self::from_l0_chain`] scans the
/// writer's `L0Manager` (current L0 + pending-flush L0s) — no I/O,
/// runs before the writer write lock is acquired. The richer
/// [`Self::extend_with_l1`] adds an async L1 storage probe for VIDs
/// not found in the L0 chain — closes the gap where a vertex flushed
/// to L1 in a previous commit would otherwise be misclassified as
/// `NODE_CREATE` on its next mutation. The L1 probe also projects
/// every property column on the target label so the resulting
/// `old_value` carries the same pre-image fidelity as the L0-chain
/// path. Edge pre-images are captured via the L0 chain's
/// `edge_properties` snapshot.
#[derive(Default)]
pub struct PreExistingProbe {
    /// VIDs known to exist in committed state (with their pre-image
    /// properties, when captured — populated by L0 probe; empty
    /// `Properties` map when added by L1 existence probe).
    vertices: HashMap<uni_common::Vid, Properties>,
    /// EIDs known to exist in committed state (with their pre-image
    /// properties, when captured — populated by L0 probe). The map
    /// uses `Properties::default()` for entries added through an
    /// existence-only path.
    edges: HashMap<uni_common::Eid, Properties>,
}

impl PreExistingProbe {
    /// Build a probe by scanning the current L0 + pending-flush L0s
    /// for vertices/edges referenced in `tx_l0`. Properties are cloned
    /// from the committed L0 chain.
    ///
    /// Only mutations actually present in `tx_l0` are probed — keeping
    /// the work proportional to the commit's mutation count rather
    /// than to the total graph size.
    #[must_use]
    pub fn from_l0_chain(l0_manager: &L0Manager, tx_l0: &L0Buffer) -> Self {
        let mut vertices: HashMap<uni_common::Vid, Properties> = HashMap::new();
        let mut edges: HashMap<uni_common::Eid, Properties> = HashMap::new();

        let candidate_vids: Vec<uni_common::Vid> = tx_l0
            .vertex_properties
            .keys()
            .copied()
            .chain(tx_l0.vertex_tombstones.iter().copied())
            .collect();
        let candidate_eids: Vec<uni_common::Eid> = tx_l0
            .edge_endpoints
            .keys()
            .copied()
            .chain(tx_l0.tombstones.keys().copied())
            .collect();

        // Buffers are probed newest→oldest (current, then pending-flush). A
        // tombstone in a newer buffer means the entity is DEAD as of that
        // buffer — not merely "no info here". We record it in `dead_*` so an
        // OLDER buffer's stale CREATE props can never resurrect it as
        // pre-existing (which would mis-emit a later recreate as an UPDATE with
        // stale `old_value` instead of a CREATE).
        let mut dead_vids: HashSet<uni_common::Vid> = HashSet::new();
        let mut dead_eids: HashSet<uni_common::Eid> = HashSet::new();
        let mut probe_buffer = |buf: &L0Buffer| {
            for vid in &candidate_vids {
                if vertices.contains_key(vid) || dead_vids.contains(vid) {
                    continue;
                }
                if buf.vertex_tombstones.contains(vid) {
                    dead_vids.insert(*vid);
                    continue;
                }
                if let Some(props) = buf.vertex_properties.get(vid) {
                    vertices.insert(*vid, props.clone());
                }
            }
            for eid in &candidate_eids {
                if edges.contains_key(eid) || dead_eids.contains(eid) {
                    continue;
                }
                if buf.tombstones.contains_key(eid) {
                    dead_eids.insert(*eid);
                    continue;
                }
                if buf.edge_endpoints.contains_key(eid) {
                    let props = buf.edge_properties.get(eid).cloned().unwrap_or_default();
                    edges.insert(*eid, props);
                }
            }
        };

        // Probe newest → oldest so the first definitive state per entity wins:
        // `current` is the newest buffer, then the pending-flush buffers from
        // newest to oldest (`get_pending_flush` returns them oldest-first). This
        // ordering is what makes the `dead_*` short-circuit correct — a newer
        // tombstone is seen before an older buffer's stale CREATE props — and it
        // also records the newest (not oldest) pre-image props for a vertex
        // updated across flush windows.
        {
            let current = l0_manager.get_current();
            let g = current.read();
            probe_buffer(&g);
        }
        for pending in l0_manager.get_pending_flush().iter().rev() {
            let g = pending.read();
            probe_buffer(&g);
        }

        Self { vertices, edges }
    }

    /// Snapshot the (vid, label) pairs that should be probed against
    /// L1 storage — VIDs in `tx_l0` not already marked pre-existing
    /// by the L0 chain. Sync — must run under the `tx_l0` read lock.
    /// Returned vector is sized by chunked-IN-list quota, ready to
    /// hand to [`Self::extend_with_l1`] outside the lock.
    #[must_use]
    pub fn pending_l1_candidates(&self, tx_l0: &L0Buffer) -> Vec<(uni_common::Vid, String)> {
        let mut out: Vec<(uni_common::Vid, String)> = Vec::new();
        for vid in tx_l0
            .vertex_properties
            .keys()
            .chain(tx_l0.vertex_tombstones.iter())
        {
            if self.vertices.contains_key(vid) {
                continue;
            }
            let label = tx_l0
                .vertex_labels
                .get(vid)
                .and_then(|labels| labels.first())
                .cloned();
            match label {
                Some(label) => out.push((*vid, label)),
                None => {
                    // #233 Tier 1, PARTIAL. Skipping the candidate means the
                    // L1 probe never runs for this vid, so a vertex that DOES
                    // pre-exist in L1 is reported as NODE_CREATE with no
                    // pre-image instead of NODE_UPDATE. Resolving it needs a
                    // label source this layer does not have: the per-label L1
                    // table cannot be scanned without a label, and the main
                    // vertices table's `labels` column lives behind
                    // `uni-store`'s writer. Made visible rather than left
                    // silent; the fix needs a label lookup threaded in.
                    warn!(
                        vid = ?vid,
                        "no label for a pending vertex; skipping its L1 pre-existence probe, \
                         so it may be reported as CREATE rather than UPDATE",
                    );
                }
            }
        }
        out
    }

    /// Extend an existing probe with an L1 storage scan for the
    /// supplied `(vid, label)` candidates (typically the output of
    /// [`Self::pending_l1_candidates`]). Async — runs outside the
    /// tx_l0 read lock.
    ///
    /// Groups candidates by label, chunks each group into 1024-VID
    /// batches, and issues one `scan_vertex_table` per chunk with a
    /// `_vid IN (…)` filter — bounded I/O proportional to the
    /// commit's mutation count, not the graph size. For every
    /// returned VID, every non-vid column is converted via
    /// [`uni_store::storage::arrow_convert::arrow_to_value`] and
    /// stashed as the pre-image `Properties` map; this populates the
    /// `old_value` column on `NODE_UPDATE` / `NODE_DELETE` events
    /// emitted by [`MutationEvents::from_l0_with_probe`] for vertices
    /// that were only visible after the last L0 flush.
    ///
    /// # Errors
    ///
    /// Per-chunk scan errors are logged and ignored — the L0 probe
    /// already captured the high-fidelity subset, so a failed L1
    /// probe degrades to "L1 vertices are misclassified as CREATE"
    /// rather than failing the commit.
    pub async fn extend_with_l1(
        &mut self,
        candidates: Vec<(uni_common::Vid, String)>,
        storage: &uni_store::storage::manager::StorageManager,
    ) {
        use arrow_array::Array;
        use std::collections::HashMap as StdHashMap;
        use uni_store::storage::arrow_convert::arrow_to_value;
        const CHUNK_SIZE: usize = 1024;

        let mut by_label: StdHashMap<String, Vec<uni_common::Vid>> = StdHashMap::new();
        for (vid, label) in candidates {
            by_label.entry(label).or_default().push(vid);
        }

        for (label, vids) in by_label {
            // Discover the table's full column set once per label so
            // we can request every property (not just `_vid`).
            let table_name = uni_store::backend::table_names::vertex_table_name(&label);
            let column_names: Vec<String> =
                match storage.backend().get_table_schema(&table_name).await {
                    Ok(Some(schema)) => schema.fields().iter().map(|f| f.name().clone()).collect(),
                    Ok(None) => {
                        // Table absent: nothing to probe.
                        continue;
                    }
                    Err(e) => {
                        warn!(label = %label, error = %e, "L1 pre-image probe: \
                          schema lookup failed; vids fall back to CREATE");
                        continue;
                    }
                };
            // Always include `_vid`; the column-filter inside
            // `scan_vertex_table` is permissive about missing columns,
            // so passing every name from the schema is safe.
            let col_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();

            for chunk in vids.chunks(CHUNK_SIZE) {
                let filter = uni_store::backend::types::FilterExpr::one_of(
                    "_vid",
                    chunk
                        .iter()
                        .map(|v| uni_store::backend::types::Scalar::UInt(v.as_u64())),
                );
                let batch = match storage
                    .scan_vertex_table(&label, &col_refs, Some(&filter))
                    .await
                {
                    Ok(Some(b)) => b,
                    Ok(None) => continue,
                    Err(e) => {
                        warn!(label = %label, error = %e, "L1 pre-image probe failed; \
                              affected vids fall back to NODE_CREATE classification");
                        continue;
                    }
                };
                let Some(vid_col) = batch
                    .column_by_name("_vid")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>())
                else {
                    warn!(label = %label, "L1 probe returned batch without _vid column");
                    continue;
                };
                // Cache (column_index, column_name) pairs for the
                // per-row property assembly. Skip storage-internal
                // columns (`_vid`, `_version`, `_label`) — user
                // properties are everything else.
                let schema = batch.schema();
                let property_cols: Vec<(usize, String)> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, f)| {
                        let name = f.name();
                        if name == "_vid"
                            || name == "_version"
                            || name == "_label"
                            || name == "_labels"
                        {
                            None
                        } else {
                            Some((idx, name.clone()))
                        }
                    })
                    .collect();

                for row in 0..vid_col.len() {
                    if vid_col.is_null(row) {
                        continue;
                    }
                    let raw = vid_col.value(row);
                    let vid = uni_common::Vid::from(raw);
                    let mut props = Properties::new();
                    for (col_idx, col_name) in &property_cols {
                        let col = batch.column(*col_idx);
                        let value = arrow_to_value(col.as_ref(), row, None);
                        if !matches!(value, uni_common::Value::Null) {
                            props.insert(col_name.clone(), value);
                        }
                    }
                    // First insert wins: L0-chain entries always come
                    // first and may already hold richer pre-image data.
                    self.vertices.entry(vid).or_insert(props);
                }
            }
        }
    }

    /// True if `vid` was visible in committed state before this tx.
    #[must_use]
    pub fn vertex_pre_existed(&self, vid: uni_common::Vid) -> bool {
        self.vertices.contains_key(&vid)
    }

    /// True if `eid` was visible in committed state before this tx.
    #[must_use]
    pub fn edge_pre_existed(&self, eid: uni_common::Eid) -> bool {
        self.edges.contains_key(&eid)
    }

    fn edge_old_bytes(&self, eid: uni_common::Eid) -> Option<Vec<u8>> {
        self.edges.get(&eid).and_then(serialize_properties)
    }

    fn vertex_old_bytes(&self, vid: uni_common::Vid) -> Option<Vec<u8>> {
        self.vertices.get(&vid).and_then(serialize_properties)
    }

    /// Borrow the captured pre-image properties for `vid`, when the
    /// vertex pre-existed in committed state. Used by
    /// [`MutationEvents::from_l0_with_probe`] to populate the
    /// `properties_old` event-row column with the subset of keys any
    /// trigger predicate references.
    #[must_use]
    pub fn vertex_properties(&self, vid: uni_common::Vid) -> Option<&Properties> {
        self.vertices.get(&vid)
    }

    /// Borrow the captured pre-image properties for `eid`, when the
    /// edge pre-existed in committed state.
    #[must_use]
    pub fn edge_properties(&self, eid: uni_common::Eid) -> Option<&Properties> {
        self.edges.get(&eid)
    }
}

/// Serialize a `Properties` map into a stable byte representation for
/// the trigger event row's `old_value` column. Uses JSON for now —
/// matches the codec other plugin surfaces use for `CypherValue`
/// payloads and keeps the bytes inspectable in trigger plugins
/// without pulling a bespoke decoder.
fn serialize_properties(props: &Properties) -> Option<Vec<u8>> {
    // #233 Tier 1: `unwrap_or_default()` produced EMPTY bytes on a
    // serialization failure. `old_value` is an `Option<Vec<u8>>` whose `None`
    // means "no pre-image"; empty bytes instead mean "the pre-image was an
    // empty property map", so a trigger comparing old against new saw a
    // spurious change. `None` is the honest answer for "could not serialize".
    match serde_json::to_vec(props) {
        Ok(bytes) => Some(bytes),
        Err(e) => {
            warn!(error = %e, "trigger pre-image serialization failed; emitting no old_value");
            None
        }
    }
}

impl MutationEvents {
    /// Drain the tx-private L0 buffer into a typed event log without a
    /// committed-state probe. Every non-tombstoned write emits an
    /// `UPDATE` event; `old_value` is `None`. Equivalent to
    /// [`Self::from_l0_with_probe`] with `probe = None`.
    #[must_use]
    pub fn from_l0(l0: &L0Buffer) -> Self {
        Self::from_l0_with_probe(l0, None, &HashSet::new(), &|_| None)
    }

    /// Drain the tx-private L0 buffer into a typed event log.
    ///
    /// When `probe` is supplied, the probe distinguishes CREATE from
    /// UPDATE per-VID/EID and supplies the pre-image bytes used to
    /// populate `old_value` for `BeforeCommit` triggers. When `probe`
    /// is `None`, every write emits `UPDATE` and `old_value` stays
    /// `None` (legacy behavior — kept for callers that don't yet
    /// build a probe).
    ///
    /// Multi-label vertices emit one row per label so a label-filtered
    /// trigger fires exactly once per (vid, matching-label) pair.
    /// Vertices with no labels emit a single row with an empty label
    /// so unfiltered triggers still observe them.
    #[must_use]
    pub fn from_l0_with_probe(
        l0: &L0Buffer,
        probe: Option<&PreExistingProbe>,
        properties_referenced: &HashSet<String>,
        resolve_edge_type: &dyn Fn(u32) -> Option<String>,
    ) -> Self {
        let mut rows: Vec<MutationRow> = Vec::with_capacity(l0.mutation_count);
        let track_props = !properties_referenced.is_empty();

        // Extract the subset of `props` whose keys appear in
        // `properties_referenced`. Returns `None` when nothing is
        // tracked or no referenced key is present, keeping the column
        // null for property-free triggers.
        let filtered = |props: &Properties| -> Option<Properties> {
            if !track_props {
                return None;
            }
            let mut out: Properties = Properties::new();
            for k in properties_referenced {
                if let Some(v) = props.get(k) {
                    out.insert(k.clone(), v.clone());
                }
            }
            // Always emit the (possibly empty) bag when properties are
            // tracked so the predicate sees a Map rather than NULL
            // (which would short-circuit `index` to NULL and risk
            // type-coercion surprises in `>` / `<>` comparisons).
            Some(out)
        };

        // Vertex writes — CREATE if the probe says the vid didn't
        // pre-exist, UPDATE otherwise. Legacy callers with no probe
        // get UPDATE for every write.
        for (vid, props) in &l0.vertex_properties {
            if l0.vertex_tombstones.contains(vid) {
                continue;
            }
            let id = vid_to_i64(*vid);
            let labels = l0.vertex_labels.get(vid);
            let (kind, old, old_props_map) = match probe {
                Some(p) if p.vertex_pre_existed(*vid) => (
                    TriggerEventMask::NODE_UPDATE,
                    p.vertex_old_bytes(*vid),
                    p.vertex_properties(*vid).and_then(&filtered),
                ),
                Some(_) => (TriggerEventMask::NODE_CREATE, None, None),
                None => (TriggerEventMask::NODE_UPDATE, None, None),
            };
            let new_props_map = filtered(props);
            match labels {
                Some(ls) if !ls.is_empty() => {
                    for l in ls {
                        rows.push(MutationRow {
                            event_kind: kind,
                            vid_or_eid: id,
                            label_or_type: l.clone(),
                            old_value: old.clone(),
                            new_properties: new_props_map.clone(),
                            old_properties: old_props_map.clone(),
                        });
                    }
                }
                _ => {
                    rows.push(MutationRow {
                        event_kind: kind,
                        vid_or_eid: id,
                        label_or_type: String::new(),
                        old_value: old,
                        new_properties: new_props_map,
                        old_properties: old_props_map,
                    });
                }
            }
        }

        // Vertex deletes. `old_value` is the pre-tx property image when
        // the probe captured it (the row is about to disappear).
        for vid in &l0.vertex_tombstones {
            let id = vid_to_i64(*vid);
            let labels = l0.vertex_labels.get(vid);
            let old = probe.and_then(|p| p.vertex_old_bytes(*vid));
            let old_props_map = probe
                .and_then(|p| p.vertex_properties(*vid))
                .and_then(&filtered);
            match labels {
                Some(ls) if !ls.is_empty() => {
                    for l in ls {
                        rows.push(MutationRow {
                            event_kind: TriggerEventMask::NODE_DELETE,
                            vid_or_eid: id,
                            label_or_type: l.clone(),
                            old_value: old.clone(),
                            new_properties: None,
                            old_properties: old_props_map.clone(),
                        });
                    }
                }
                _ => {
                    rows.push(MutationRow {
                        event_kind: TriggerEventMask::NODE_DELETE,
                        vid_or_eid: id,
                        label_or_type: String::new(),
                        old_value: old,
                        new_properties: None,
                        old_properties: old_props_map,
                    });
                }
            }
        }

        // Edge writes — CREATE if not pre-existing, else UPDATE.
        // `old_value` carries the pre-image edge properties for UPDATE
        // and is `None` for CREATE.
        for (eid, (_, _, type_id)) in &l0.edge_endpoints {
            if l0.tombstones.contains_key(eid) {
                continue;
            }
            // #233 Tier 1: `unwrap_or_default()` produced an EMPTY type name,
            // which the router compares against a trigger's declared type — so
            // `ON -[:KNOWS]` silently never fired for an edge whose name was
            // missing from `edge_types` (an edge inserted with `etype_name:
            // None` has endpoints but no name). The type *id* is carried in
            // `edge_endpoints` all along, so the name is recoverable through
            // the schema rather than guessed.
            let etype = l0.edge_types.get(eid).cloned().or_else(|| {
                let resolved = resolve_edge_type(*type_id);
                if resolved.is_none() {
                    warn!(
                        eid = ?eid,
                        type_id = *type_id,
                        "edge type name unresolvable; trigger routing will not match this edge",
                    );
                }
                resolved
            });
            let Some(etype) = etype else {
                continue;
            };
            let (kind, old, old_props_map) = match probe {
                Some(p) if p.edge_pre_existed(*eid) => (
                    TriggerEventMask::EDGE_UPDATE,
                    p.edge_old_bytes(*eid),
                    p.edge_properties(*eid).and_then(&filtered),
                ),
                Some(_) => (TriggerEventMask::EDGE_CREATE, None, None),
                None => (TriggerEventMask::EDGE_UPDATE, None, None),
            };
            let new_props_map = l0.edge_properties.get(eid).and_then(&filtered);
            rows.push(MutationRow {
                event_kind: kind,
                vid_or_eid: eid_to_i64(*eid),
                label_or_type: etype,
                old_value: old,
                new_properties: new_props_map,
                old_properties: old_props_map,
            });
        }

        // Edge deletes. `old_value` is the pre-tx property image when
        // the probe captured it.
        for (eid, ts) in &l0.tombstones {
            let etype = l0
                .edge_types
                .get(eid)
                .cloned()
                .unwrap_or_else(|| format!("type:{}", ts.edge_type));
            let old = probe.and_then(|p| p.edge_old_bytes(*eid));
            let old_props_map = probe
                .and_then(|p| p.edge_properties(*eid))
                .and_then(&filtered);
            rows.push(MutationRow {
                event_kind: TriggerEventMask::EDGE_DELETE,
                vid_or_eid: eid_to_i64(*eid),
                label_or_type: etype,
                old_value: old,
                new_properties: None,
                old_properties: old_props_map,
            });
        }

        Self { rows }
    }

    /// Project every captured row into the canonical [`event_row_schema`]
    /// `RecordBatch`, with no per-route filtering and no predicate.
    ///
    /// Used by the CDC delivery path to hand subscribers the full
    /// stream of mutations for a committed transaction (M11 FU-4). The
    /// per-trigger filtered shape is built by `Self::filter_for`.
    ///
    /// Returns `Ok(None)` when there are zero rows (lets callers skip
    /// constructing an empty `CdcBatch`).
    ///
    /// # Errors
    ///
    /// Returns an error if the event rows cannot be materialized. The caller
    /// must NOT treat that as "no mutations" — see [`EventRowColumns::into_batch`].
    pub fn materialize_all(&self) -> anyhow::Result<Option<RecordBatch>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let mut cols = EventRowColumns::with_capacity(self.rows.len());
        for row in &self.rows {
            cols.push_row(row);
        }
        cols.into_batch()
    }

    /// Filter rows matching `entry`'s subscription selectors and
    /// project them into the §4.18 RecordBatch shape.
    ///
    /// Returns `Ok(None)` if no rows match (caller skips the `fire` call).
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be built or the compiled
    /// predicate cannot be evaluated. #233 Tier 1: both used to collapse into
    /// `None`, i.e. "no rows match", so a trigger silently failed to fire on
    /// rows that did match.
    fn filter_for(&self, entry: &RouteEntry) -> anyhow::Result<Option<RecordBatch>> {
        // property_filter is satisfied vacuously here — per-property
        // event-row population (one row per (vid, property) write) is
        // not the chosen surface; predicate authors instead reference
        // `n.<prop>` directly and the property-bag column resolves it
        // through `index`.
        let _ = &entry.property_filter;
        let mut cols = EventRowColumns::default();
        for row in &self.rows {
            if entry.matches(row.event_kind, &row.label_or_type) {
                cols.push_row(row);
            }
        }
        let Some(batch) = cols.into_batch()? else {
            return Ok(None);
        };

        // Apply the compiled `predicate_source` boolean mask if any. An
        // evaluation failure is an error, not "no rows match": the latter
        // silently suppresses a trigger that should have fired (#233).
        let batch = match &entry.compiled_predicate {
            Some(predicate) => match apply_predicate(predicate, batch)? {
                Some(b) => b,
                None => return Ok(None),
            },
            None => batch,
        };

        if batch.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(batch))
    }
}

/// Column-oriented builder for the canonical event-row [`RecordBatch`]
/// produced by [`MutationEvents::materialize_all`] and
/// [`MutationEvents::filter_for`]. Keeps the per-column allocation +
/// per-row push logic in one place so the two callers stay in lockstep.
#[derive(Default)]
struct EventRowColumns {
    kinds: Vec<u8>,
    ids: Vec<i64>,
    labels: Vec<String>,
    properties: Vec<String>,
    olds: Vec<Option<Vec<u8>>>,
    news: Vec<Option<Vec<u8>>>,
    props_new: Vec<Option<Vec<u8>>>,
    props_old: Vec<Option<Vec<u8>>>,
}

impl EventRowColumns {
    fn with_capacity(cap: usize) -> Self {
        Self {
            kinds: Vec::with_capacity(cap),
            ids: Vec::with_capacity(cap),
            labels: Vec::with_capacity(cap),
            properties: Vec::with_capacity(cap),
            olds: Vec::with_capacity(cap),
            news: Vec::with_capacity(cap),
            props_new: Vec::with_capacity(cap),
            props_old: Vec::with_capacity(cap),
        }
    }

    fn push_row(&mut self, row: &MutationRow) {
        self.kinds.push(mask_to_discriminant(row.event_kind));
        self.ids.push(row.vid_or_eid);
        self.labels.push(row.label_or_type.clone());
        self.properties.push(String::new());
        self.olds.push(row.old_value.clone());
        self.news.push(None);
        self.props_new.push(
            row.new_properties
                .as_ref()
                .map(|m| cypher_value_codec::encode(&Value::Map(m.clone()))),
        );
        self.props_old.push(
            row.old_properties
                .as_ref()
                .map(|m| cypher_value_codec::encode(&Value::Map(m.clone()))),
        );
    }

    /// Materialize the columns into a `RecordBatch`.
    ///
    /// Returns `Ok(None)` when zero rows were collected (callers skip the
    /// empty case).
    ///
    /// # Errors
    ///
    /// Returns an error if the collected columns do not form a valid batch
    /// against [`event_row_schema`]. #233 Tier 1: this used to be `.ok()`,
    /// which made that failure indistinguishable from "no rows" — and a whole
    /// commit's mutations then reached `CdcRuntime` as `None`, which delivered
    /// an empty batch carrying the real LSN range and checkpointed past it.
    fn into_batch(self) -> anyhow::Result<Option<RecordBatch>> {
        if self.kinds.is_empty() {
            return Ok(None);
        }
        // Build a nullable `LargeBinary` column from a `Vec<Option<Vec<u8>>>`.
        let large_binary = |col: &[Option<Vec<u8>>]| -> Arc<dyn arrow_array::Array> {
            let refs: Vec<Option<&[u8]>> = col.iter().map(|o| o.as_deref()).collect();
            Arc::new(LargeBinaryArray::from(refs))
        };

        let columns: Vec<Arc<dyn arrow_array::Array>> = vec![
            Arc::new(UInt8Array::from(self.kinds)),
            Arc::new(Int64Array::from(self.ids)),
            Arc::new(arrow_array::StringArray::from(self.labels)),
            Arc::new(arrow_array::StringArray::from(self.properties)),
            large_binary(&self.olds),
            large_binary(&self.news),
            large_binary(&self.props_new),
            large_binary(&self.props_old),
        ];

        Ok(Some(RecordBatch::try_new(event_row_schema(), columns)?))
    }
}

/// Run a compiled trigger predicate against the candidate batch.
///
/// Returns `Ok(None)` when the predicate eliminates every row.
///
/// # Errors
///
/// Returns an error if the predicate cannot be evaluated or does not yield a
/// Boolean column. #233 Tier 1: these were warn-and-return-`None`, which is
/// indistinguishable from "the predicate rejected every row" and silently
/// suppressed triggers that should have fired.
fn apply_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    batch: RecordBatch,
) -> anyhow::Result<Option<RecordBatch>> {
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::logical_expr::ColumnarValue;

    let value = predicate.evaluate(&batch)?;
    let array = match value {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(batch.num_rows())?,
    };
    let Some(bool_arr) = array.as_any().downcast_ref::<BooleanArray>() else {
        anyhow::bail!("trigger predicate must yield a Boolean column");
    };
    let filtered = filter_record_batch(&batch, bool_arr)?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }
    Ok(Some(filtered))
}

fn mask_to_discriminant(m: TriggerEventMask) -> u8 {
    // 1-based bit position of the lowest set bit (e.g. `0b001 → 1`,
    // `0b100 → 3`); falls back to 0 when no bit is set. Emitted rows
    // always carry exactly one bit, so the lowest set bit is *the* bit.
    if m.0 == 0 {
        return 0;
    }
    m.0.trailing_zeros() as u8 + 1
}

fn vid_to_i64(vid: uni_common::Vid) -> i64 {
    // Vid is a newtype around a u64; reinterpret-cast preserves bits.
    vid.as_u64() as i64
}

fn eid_to_i64(eid: uni_common::Eid) -> i64 {
    eid.as_u64() as i64
}

// ── M11 deferral queue (memory-backed v1) ──────────────────────────

/// Maximum number of times a `TriggerOutcome::Defer` will be re-queued
/// before the queue gives up and drops the item with a warning. Caps
/// the worst case for a pathological plugin that always returns
/// `Defer` from cascading.
const DEFER_MAX_ATTEMPTS: u32 = 10;

struct DeferredItem {
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    /// Index among same-named triggers (see [`RouteEntry::name_ordinal`]) —
    /// persisted so a reload re-binds this item to the exact trigger.
    name_ordinal: usize,
    batch: MutationBatch,
    session_id: String,
    tx_id: u64,
    attempts: u32,
    /// `TriggerDeferral::payload` passed back to
    /// [`TriggerPlugin::on_deferred`] when this item fires (FU-5).
    payload: String,
}

// ── WS-E: EventualConsistency coalescing queue ─────────────────────

/// One per-trigger coalescing bucket inside [`EcQueue`].
///
/// Accumulates the projected event `RecordBatch`es for a single
/// `EventualConsistency` trigger across commits, then flushes them as
/// ONE concatenated `DeferredItem` into the shared [`DeferralQueue`]
/// once the bucket is due (age >= interval or rows >= threshold).
struct EcBucket {
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    /// Index among same-named triggers — see [`RouteEntry::name_ordinal`].
    /// Combined with `name` to re-bind the coalesced fire to the exact
    /// trigger (same contract the deferral sidecar uses on reload).
    name_ordinal: usize,
    /// Session that opened this coalescing window (the first enqueue).
    /// A coalesced fire spans multiple commits, so the per-commit session
    /// identity is inherently lossy; we keep the window opener's.
    session_id: String,
    /// Tx that opened this coalescing window (the first enqueue).
    tx_id: u64,
    /// Per-commit projected batches, in enqueue (FIFO) order. Concatenated
    /// in this order on flush so the coalesced batch preserves arrival order.
    pending: Vec<Arc<RecordBatch>>,
    /// Running total of `pending` row counts — the size-flush signal.
    rows: usize,
    /// When the first batch of the current window was enqueued — the
    /// age-flush signal. Reset each window because a flushed bucket is
    /// removed from the map and re-created fresh on the next enqueue.
    first_enqueued: StdInstant,
}

/// Per-`Uni` batched/coalescing queue for [`FireMode::EventualConsistency`]
/// triggers.
///
/// Where [`FireMode::Async`] spawns one task per matching commit,
/// EventualConsistency triggers buffer their projected event batches into
/// per-trigger `EcBucket`s and flush a SINGLE coalesced fire once a
/// bucket is due. Coalesced work rides the existing [`DeferralQueue`]
/// durability + fire ladder — there is no separate sidecar or fire path:
/// a due bucket concatenates its pending batches into one
/// [`MutationBatch`] and pushes one `DeferredItem`.
///
/// **Lifetime:** unlike [`TriggerRouter`] (rebuilt every commit), the
/// `EcQueue` is a per-`Uni` `Arc` living in `UniInner`, `Arc`-cloned into
/// each commit's router exactly like the deferral queue — so buckets
/// survive across commits and actually coalesce.
///
/// **Flush triggers** (per bucket):
/// - **age** — oldest pending batch is older than `flush_interval`;
/// - **size** — accumulated `rows` reach `flush_threshold`;
/// - **back-pressure** — a single enqueue pushes `rows` past
///   `4 × flush_threshold`, forcing an inline drain of that bucket so the
///   commit path never blocks and no data is dropped.
pub struct EcQueue {
    buckets: parking_lot::Mutex<HashMap<(String, usize), EcBucket>>,
    /// Shared with `UniInner::defer_queue`: coalesced fires and
    /// back-pressure drains push here. `None` for read-only / test setups
    /// without a queue (buffering then no-ops on flush, like the deferral
    /// queue's warn-and-drop fallback).
    defer_queue: Option<Arc<DeferralQueue>>,
    /// Max age of a bucket's oldest pending batch before it flushes.
    flush_interval: Duration,
    /// Row count at which a bucket flushes early (before `flush_interval`).
    flush_threshold: usize,
}

impl std::fmt::Debug for EcQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let buckets = self.buckets.lock();
        f.debug_struct("EcQueue")
            .field("buckets", &buckets.len())
            .field("flush_interval", &self.flush_interval)
            .field("flush_threshold", &self.flush_threshold)
            .finish()
    }
}

impl EcQueue {
    /// Build a queue wired to the shared deferral queue and configured
    /// from `UniConfig::ec_flush_interval` / `ec_flush_threshold`.
    #[must_use]
    pub fn new(
        defer_queue: Option<Arc<DeferralQueue>>,
        flush_interval: Duration,
        flush_threshold: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            buckets: parking_lot::Mutex::new(HashMap::new()),
            defer_queue,
            // A zero threshold would make every enqueue immediately
            // over-cap (back-pressure); clamp to at least 1 row.
            flush_interval,
            flush_threshold: flush_threshold.max(1),
        })
    }

    /// Number of live (non-empty) coalescing buckets — diagnostics / tests.
    #[must_use]
    pub fn bucket_count(&self) -> usize {
        self.buckets.lock().len()
    }

    /// Buffer one commit's projected batch for `entry`'s coalescing bucket.
    ///
    /// Appends in FIFO order, bumps the running row count, and opens a
    /// fresh window (`first_enqueued`, `session_id`, `tx_id`) when the
    /// bucket is new. If this enqueue pushes the bucket past
    /// `4 × flush_threshold`, the bucket is force-drained inline to the
    /// deferral queue (back-pressure) so the commit path never blocks and
    /// no data is dropped. Called from the after-phase dispatch on the
    /// commit thread — never blocks on I/O beyond the deferral push.
    ///
    /// Module-private because it takes the module-private [`RouteEntry`];
    /// the only caller is [`TriggerRouter::dispatch_after`] (same module).
    fn enqueue(&self, entry: &RouteEntry, session_id: &str, tx_id: u64, batch: Arc<RecordBatch>) {
        let key = (entry.name.clone(), entry.name_ordinal);
        // Back-pressure cap: 4× the early-flush threshold.
        let cap = self.flush_threshold.saturating_mul(4);
        let mut buckets = self.buckets.lock();
        let bucket = buckets.entry(key.clone()).or_insert_with(|| EcBucket {
            plugin: Arc::clone(&entry.plugin),
            name: entry.name.clone(),
            name_ordinal: entry.name_ordinal,
            session_id: session_id.to_owned(),
            tx_id,
            pending: Vec::new(),
            rows: 0,
            first_enqueued: StdInstant::now(),
        });
        let n = batch.num_rows();
        bucket.pending.push(batch);
        bucket.rows += n;
        let over_cap = bucket.rows > cap;
        // Drop the `&mut bucket` borrow (NLL) before mutating the map again.
        if over_cap
            && let Some(defer_queue) = self.defer_queue.as_ref()
            && let Some(drained) = buckets.remove(&key)
        {
            Self::drain_bucket(drained, defer_queue, StdInstant::now());
        }
    }

    /// Flush every bucket that is due as of `now`: age (oldest pending
    /// batch older than `flush_interval`) OR size (`rows >= flush_threshold`).
    /// Each due bucket becomes ONE coalesced `DeferredItem`. Called from
    /// the per-`Uni` 50ms deferral tick alongside `DeferralQueue::tick`.
    pub fn flush_due(&self, now: StdInstant) {
        let Some(defer_queue) = self.defer_queue.as_ref() else {
            return;
        };
        let mut buckets = self.buckets.lock();
        let due_keys: Vec<(String, usize)> = buckets
            .iter()
            .filter(|(_, b)| {
                now.saturating_duration_since(b.first_enqueued) >= self.flush_interval
                    || b.rows >= self.flush_threshold
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in due_keys {
            if let Some(bucket) = buckets.remove(&key) {
                Self::drain_bucket(bucket, defer_queue, now);
            }
        }
    }

    /// Concatenate a bucket's pending batches (in FIFO order) into one
    /// coalesced `DeferredItem` and push it into `defer_queue` with
    /// `fire_at = now`. On concat failure (schema drift) each pending batch
    /// is pushed individually so data is never lost.
    fn drain_bucket(bucket: EcBucket, defer_queue: &DeferralQueue, now: StdInstant) {
        let EcBucket {
            plugin,
            name,
            name_ordinal,
            session_id,
            tx_id,
            pending,
            ..
        } = bucket;
        if pending.is_empty() {
            return;
        }
        let schema = pending[0].schema();
        match arrow_select::concat::concat_batches(&schema, pending.iter().map(Arc::as_ref)) {
            Ok(coalesced) => {
                defer_queue.push(
                    DeferredItem {
                        plugin,
                        name,
                        name_ordinal,
                        batch: MutationBatch {
                            events: Arc::new(coalesced),
                        },
                        session_id,
                        tx_id,
                        attempts: 0,
                        payload: String::new(),
                    },
                    now,
                );
            }
            Err(e) => {
                warn!(
                    trigger = %name,
                    error = %e,
                    "EcQueue: batch concat failed (schema drift?); flushing pending batches individually"
                );
                for events in pending {
                    defer_queue.push(
                        DeferredItem {
                            plugin: Arc::clone(&plugin),
                            name: name.clone(),
                            name_ordinal,
                            batch: MutationBatch { events },
                            session_id: session_id.clone(),
                            tx_id,
                            attempts: 0,
                            payload: String::new(),
                        },
                        now,
                    );
                }
            }
        }
    }
}

/// In-memory deferral queue for `TriggerOutcome::Defer`.
///
/// Items are keyed by their scheduled fire instant in a `BTreeMap`,
/// so `drain_due` pops the next-due slot in O(log n). The queue is
/// drained by a per-`Uni` background tick task spawned at DB build
/// time; firing happens on the tokio runtime.
///
/// **Durability:**
/// - By default the queue is in-memory and restart drops queued items.
///   Built via [`DeferralQueue::with_persistence`] it mirrors every
///   `push`/`drain_due` to a JSON sidecar (FU-5) and reloads on restart
///   via [`DeferralQueue::load_from_sidecar`].
/// - Even with persistence there is no transactional guarantee tying a
///   deferred fire to the originating commit; a crash between commit and
///   the next sidecar write can still lose an item.
/// - Per-item retry is capped at `DEFER_MAX_ATTEMPTS` to prevent
///   runaway re-deferral loops.
#[derive(Default)]
pub struct DeferralQueue {
    inner: parking_lot::Mutex<BTreeMap<StdInstant, Vec<DeferredItem>>>,
    /// Optional JSON-sidecar persistence (FU-5). When set, every
    /// `push` mirrors the queue state to disk and every `drain_due`
    /// rewrites the sidecar so a crash-restart can re-load the queue
    /// state. The persistence sink resolves [`TriggerPlugin`]s by qname
    /// from the host's [`uni_plugin::PluginRegistry`] at load time.
    sidecar: parking_lot::Mutex<Option<DeferralSidecar>>,
}

impl std::fmt::Debug for DeferralQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len: usize = self.inner.lock().values().map(|v| v.len()).sum();
        f.debug_struct("DeferralQueue").field("size", &len).finish()
    }
}

impl DeferralQueue {
    /// Build a fresh empty queue.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Build a queue with JSON-sidecar persistence rooted at
    /// `<data_path>/_system/deferred_triggers.json`.
    ///
    /// On startup the queue's `load` method walks the sidecar and
    /// re-binds each row to its `TriggerPlugin` by qname via the
    /// supplied [`uni_plugin::PluginRegistry`]. Items whose plugin
    /// can no longer be resolved are dropped with a warn.
    ///
    /// Persists on every `push` and after every `drain_due` (FU-5).
    /// I/O failures degrade to debug logs — in-memory queue state
    /// remains authoritative for the running process.
    #[must_use]
    pub fn with_persistence(data_path: std::path::PathBuf) -> Arc<Self> {
        let queue = Arc::new(Self::default());
        *queue.sidecar.lock() = Some(DeferralSidecar::new(data_path));
        queue
    }

    /// Borrow the sidecar path, if persistence is enabled.
    #[must_use]
    pub fn sidecar_path(&self) -> Option<std::path::PathBuf> {
        self.sidecar.lock().as_ref().map(|s| s.path().to_path_buf())
    }

    /// Replay persisted items from the sidecar, re-binding each row's
    /// trigger qname against the registry. Should be called once
    /// after `Uni::build` finishes wiring triggers but before the
    /// queue tick task starts. Idempotent.
    ///
    /// Returns the number of items reloaded.
    pub fn load_from_sidecar(
        self: &Arc<Self>,
        registry: &Arc<uni_plugin::PluginRegistry>,
    ) -> usize {
        let Some(sidecar) = self.sidecar.lock().clone() else {
            return 0;
        };
        let now_wall = std::time::SystemTime::now();
        let now_mono = StdInstant::now();
        let rows = match sidecar.read_all() {
            Ok(rows) => rows,
            Err(e) => {
                tracing::debug!(error = %e, "DeferralQueue: sidecar read failed");
                return 0;
            }
        };
        let mut restored = 0usize;
        for row in rows {
            // Re-bind by (name, ordinal): the Nth trigger with this name in
            // registry order, matching the ordinal assigned in `from_registry`.
            // `find` (first match) would misroute a deferral from a later trigger
            // to an earlier same-named one.
            let Some(entry) = registry
                .triggers()
                .iter()
                .filter(|t| subscription_name(t.subscription()) == row.name)
                .nth(row.name_ordinal)
                .cloned()
            else {
                tracing::warn!(
                    trigger = %row.name,
                    ordinal = row.name_ordinal,
                    "DeferralQueue: dropping persisted item; trigger no longer registered"
                );
                continue;
            };
            // Re-decode the persisted MutationBatch from Arrow IPC.
            let batch = match arrow_ipc_decode(&row.batch_ipc) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "DeferralQueue: drop persisted item; IPC decode failed");
                    continue;
                }
            };
            // Translate the persisted wall-clock fire_at to a monotonic
            // Instant relative to current time. Past-due fire-ats
            // collapse to "now" so they fire on the next tick.
            let fire_at_wall = std::time::UNIX_EPOCH + Duration::from_millis(row.fire_at_epoch_ms);
            let mono_delta = fire_at_wall
                .duration_since(now_wall)
                .unwrap_or(Duration::ZERO);
            let fire_at_mono = now_mono + mono_delta;
            let item = DeferredItem {
                plugin: entry,
                name: row.name,
                name_ordinal: row.name_ordinal,
                batch: MutationBatch {
                    events: Arc::new(batch),
                },
                session_id: row.session_id,
                tx_id: row.tx_id,
                attempts: row.attempts,
                payload: row.payload,
            };
            self.inner
                .lock()
                .entry(fire_at_mono)
                .or_default()
                .push(item);
            restored += 1;
        }
        restored
    }

    /// Persist the current queue state to the sidecar (no-op when
    /// persistence is disabled). I/O errors degrade to debug log.
    fn persist_locked(
        &self,
        guard: &parking_lot::MutexGuard<'_, BTreeMap<StdInstant, Vec<DeferredItem>>>,
    ) {
        let Some(sidecar) = self.sidecar.lock().clone() else {
            return;
        };
        let now_wall = std::time::SystemTime::now();
        let now_mono = StdInstant::now();
        let mut rows: Vec<PersistedDeferral> = Vec::new();
        for (fire_at_mono, items) in guard.iter() {
            for item in items {
                // Convert the monotonic Instant back to wall-clock by
                // measuring the delta against `now` and offsetting
                // `now_wall`. Past-due items get a fire_at slightly
                // before `now_wall` so they fire immediately on
                // restart.
                let fire_at_wall = if *fire_at_mono <= now_mono {
                    now_wall
                } else {
                    now_wall + fire_at_mono.duration_since(now_mono)
                };
                let fire_at_epoch_ms = fire_at_wall
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let batch_ipc = match arrow_ipc_encode(&item.batch.events) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "DeferralQueue: IPC encode failed; skipping row");
                        continue;
                    }
                };
                rows.push(PersistedDeferral {
                    name: item.name.clone(),
                    name_ordinal: item.name_ordinal,
                    session_id: item.session_id.clone(),
                    tx_id: item.tx_id,
                    attempts: item.attempts,
                    payload: item.payload.clone(),
                    batch_ipc,
                    fire_at_epoch_ms,
                });
            }
        }
        if let Err(e) = sidecar.write_all(&rows) {
            tracing::debug!(error = %e, "DeferralQueue: sidecar write failed");
        }
    }

    fn push(&self, item: DeferredItem, fire_at: StdInstant) {
        let mut guard = self.inner.lock();
        guard.entry(fire_at).or_default().push(item);
        self.persist_locked(&guard);
    }

    /// Pop every item whose scheduled fire instant is `<= now`.
    fn drain_due(&self, now: StdInstant) -> Vec<DeferredItem> {
        let mut guard = self.inner.lock();
        let mut due = Vec::new();
        // BTreeMap::split_off gives us [now+ε..) so we keep that half
        // and the front half is everything ≤ now.
        let mut to_keep = guard.split_off(&(now + Duration::from_nanos(1)));
        std::mem::swap(&mut *guard, &mut to_keep);
        for (_, mut items) in to_keep {
            due.append(&mut items);
        }
        // FU-5: persist the remaining queue state after each drain so
        // a restart sees only the still-pending items.
        self.persist_locked(&guard);
        due
    }

    /// Approximate pending count — for diagnostics / tests.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.lock().values().map(|v| v.len()).sum()
    }

    /// Tick the queue once: drain due items, fire each. Items that
    /// re-defer are re-enqueued until `DEFER_MAX_ATTEMPTS`. Async
    /// because plugin `fire` may block the runtime; we re-enter the
    /// tokio executor between items via `spawn_blocking` -- but since
    /// most triggers are CPU-light, the inline call here is fine for
    /// v1.
    pub fn tick(self: &Arc<Self>) {
        let due = self.drain_due(StdInstant::now());
        for mut item in due {
            // FU-5: invoke the dedicated `on_deferred` callback so
            // trigger plugins can receive the original `payload`.
            // The default impl on the trait delegates back to `fire`,
            // so existing trigger plugins keep working unchanged.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                item.plugin.on_deferred(
                    TriggerContext::new(&item.session_id, item.tx_id),
                    &item.batch,
                    &item.payload,
                )
            }));
            let name = item.name.clone();
            handle_fire_outcome(outcome, &name, "deferred trigger", |until| {
                item.attempts += 1;
                if item.attempts >= DEFER_MAX_ATTEMPTS {
                    warn!(
                        trigger = %item.name,
                        attempts = item.attempts,
                        "deferred trigger exceeded DEFER_MAX_ATTEMPTS; dropping"
                    );
                    return;
                }
                // FU-5: honor the new `delay` field when re-deferring.
                // `None` falls back to "next tick" — matches the legacy
                // semantics. The trigger may have updated the payload on
                // re-defer; propagate the new one.
                let fire_at = StdInstant::now() + until.delay.unwrap_or(Duration::ZERO);
                item.payload = until.payload;
                self.push(item, fire_at);
            });
        }
    }
}

// ── Helpers used by `Transaction::commit` ──────────────────────────

/// Convenience: stable-hash a `&str` tx id (commit path stores tx_id
/// as `String`) down to the `u64` the `TriggerContext` carries.
#[must_use]
pub fn tx_id_to_u64(tx_id: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    tx_id.hash(&mut hasher);
    hasher.finish()
}

// ── FU-5: persisted deferral sidecar ──────────────────────────────

/// On-disk row in `<data_path>/_system/deferred_triggers.json`.
///
/// `batch_ipc` is the trigger's [`MutationBatch`] encoded as Arrow
/// IPC stream bytes — preserves schema + values across restarts. The
/// `name` is the trigger's `subscription_name`, which the host's
/// re-resolution path uses to find the registered `TriggerPlugin`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedDeferral {
    name: String,
    /// Index among same-named triggers, used with `name` to re-bind to the exact
    /// trigger on restart. `#[serde(default)]` keeps rows written before this
    /// field readable (they resolve to the first same-named trigger, the prior
    /// behavior).
    #[serde(default)]
    name_ordinal: usize,
    session_id: String,
    tx_id: u64,
    attempts: u32,
    payload: String,
    /// Arrow IPC stream bytes for the [`MutationBatch::events`]
    /// `RecordBatch`.
    #[serde(with = "serde_bytes")]
    batch_ipc: Vec<u8>,
    /// Wall-clock fire instant, milliseconds since UNIX epoch.
    fire_at_epoch_ms: u64,
}

/// Atomic JSON-sidecar persistence handle for the deferral queue.
#[derive(Clone, Debug)]
struct DeferralSidecar {
    sidecar: uni_sidecar::VecSidecar<PersistedDeferral>,
}

impl DeferralSidecar {
    /// Construct rooted at `<data_path>/_system/deferred_triggers.json`.
    fn new(data_path: std::path::PathBuf) -> Self {
        Self {
            sidecar: uni_sidecar::VecSidecar::new(data_path, "deferred_triggers.json"),
        }
    }

    /// Borrow the resolved sidecar path (for diagnostics).
    fn path(&self) -> &std::path::Path {
        self.sidecar.path()
    }

    fn read_all(&self) -> Result<Vec<PersistedDeferral>, String> {
        self.sidecar.load().map_err(|e| e.to_string())
    }

    fn write_all(&self, rows: &[PersistedDeferral]) -> Result<(), String> {
        self.sidecar.store(rows).map_err(|e| e.to_string())
    }
}

/// Encode a `RecordBatch` as Arrow IPC stream bytes (FU-5).
fn arrow_ipc_encode(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, String> {
    let schema = batch.schema();
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| format!("ipc writer: {e}"))?;
        w.write(batch).map_err(|e| format!("ipc write: {e}"))?;
        w.finish().map_err(|e| format!("ipc finish: {e}"))?;
    }
    Ok(buf)
}

/// Decode Arrow IPC stream bytes into a single `RecordBatch` (FU-5).
fn arrow_ipc_decode(bytes: &[u8]) -> Result<arrow_array::RecordBatch, String> {
    let reader = arrow_ipc::reader::StreamReader::try_new(bytes, None)
        .map_err(|e| format!("ipc reader: {e}"))?;
    let batches: Vec<arrow_array::RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("ipc collect: {e}"))?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| "ipc decode: empty stream".to_owned())
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use uni_plugin::traits::trigger::TriggerEventMask;

    #[test]
    fn mask_discriminants_are_stable() {
        assert_eq!(mask_to_discriminant(TriggerEventMask::NODE_CREATE), 1);
        assert_eq!(mask_to_discriminant(TriggerEventMask::NODE_UPDATE), 2);
        assert_eq!(mask_to_discriminant(TriggerEventMask::NODE_DELETE), 3);
        assert_eq!(mask_to_discriminant(TriggerEventMask::EDGE_CREATE), 4);
        assert_eq!(mask_to_discriminant(TriggerEventMask::EDGE_UPDATE), 5);
        assert_eq!(mask_to_discriminant(TriggerEventMask::EDGE_DELETE), 6);
    }

    #[test]
    fn empty_router_is_empty() {
        let by_phase = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        let router = TriggerRouter {
            by_phase,
            defer_queue: None,
            ec_queue: EcQueue::new(None, Duration::from_secs(1), 10_000),
        };
        assert!(router.is_empty());
    }

    #[test]
    fn tx_id_to_u64_is_deterministic() {
        let a = tx_id_to_u64("tx-1");
        let b = tx_id_to_u64("tx-1");
        let c = tx_id_to_u64("tx-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── WS-E: EcQueue coalescing tests ─────────────────────────────

    use arrow_array::Array;
    use uni_plugin::FnError;
    use uni_plugin::traits::trigger::TriggerDeferral;

    /// Minimal trigger double for building `RouteEntry`s in tests.
    struct DummyTrigger {
        sub: TriggerSubscription,
    }

    impl TriggerPlugin for DummyTrigger {
        fn subscription(&self) -> &TriggerSubscription {
            &self.sub
        }
        fn fire(
            &self,
            _ctx: TriggerContext<'_>,
            _events: &MutationBatch,
        ) -> Result<TriggerOutcome, FnError> {
            Ok(TriggerOutcome::Continue)
        }
    }

    /// Build an EventualConsistency `RouteEntry` for `(name, ordinal)`.
    fn ec_route(name: &str, ordinal: usize) -> RouteEntry {
        let sub = TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::NODE_CREATE,
            labels: None,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::EventualConsistency,
            docs: name.to_owned(),
        };
        RouteEntry {
            plugin: Arc::new(DummyTrigger { sub: sub.clone() }),
            name: name.to_owned(),
            name_ordinal: ordinal,
            event_mask: TriggerEventMask::NODE_CREATE.0,
            label_filter: None,
            edge_type_filter: None,
            property_filter: None,
            fire_mode: FireMode::EventualConsistency,
            compiled_predicate: None,
            properties_referenced: HashSet::new(),
        }
    }

    /// One-column Int64 `RecordBatch` carrying `vals` — a stand-in for the
    /// projected event batch. EcQueue is schema-agnostic; a stable single
    /// column keeps the coalescing/order assertions readable.
    fn int_batch(vals: &[i64]) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let col = Arc::new(Int64Array::from(vals.to_vec()));
        Arc::new(RecordBatch::try_new(schema, vec![col]).expect("valid record batch"))
    }

    /// Read the (single) Int64 column of a `DeferredItem`'s coalesced batch.
    fn item_values(item: &DeferredItem) -> Vec<i64> {
        let col = item
            .batch
            .events
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        (0..col.len()).map(|i| col.value(i)).collect()
    }

    #[test]
    fn ec_queue_coalesces_under_threshold_into_one_item() {
        let defer = DeferralQueue::new();
        let ec = EcQueue::new(Some(Arc::clone(&defer)), Duration::from_millis(10), 10_000);
        let entry = ec_route("t", 0);

        // Enqueue several small batches (well under the size threshold).
        for i in 0..5i64 {
            ec.enqueue(&entry, "s1", 1, int_batch(&[i]));
        }
        // Nothing has flushed yet (age < interval, rows < threshold).
        assert_eq!(defer.pending(), 0);
        assert_eq!(ec.bucket_count(), 1);

        // After the interval elapses, flush_due drains the bucket as ONE item.
        std::thread::sleep(Duration::from_millis(20));
        ec.flush_due(StdInstant::now());
        assert_eq!(defer.pending(), 1, "expected a single coalesced item");
        assert_eq!(ec.bucket_count(), 0, "bucket removed after flush");

        // The one item carries all 5 rows, in FIFO order.
        let due = defer.drain_due(StdInstant::now() + Duration::from_secs(1));
        assert_eq!(due.len(), 1);
        assert_eq!(item_values(&due[0]), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn ec_queue_size_threshold_flushes_before_interval() {
        let defer = DeferralQueue::new();
        // Long interval so only the size threshold can trigger the flush.
        let ec = EcQueue::new(Some(Arc::clone(&defer)), Duration::from_secs(3600), 3);
        let entry = ec_route("t", 0);

        // Enqueue 3 rows == threshold, immediately (elapsed ≈ 0 << interval).
        ec.enqueue(&entry, "s1", 1, int_batch(&[1, 2]));
        ec.enqueue(&entry, "s1", 1, int_batch(&[3]));

        // flush_due with elapsed < interval still flushes: size-triggered.
        ec.flush_due(StdInstant::now());
        assert_eq!(defer.pending(), 1);
        let due = defer.drain_due(StdInstant::now() + Duration::from_secs(1));
        assert_eq!(item_values(&due[0]), vec![1, 2, 3]);
    }

    #[test]
    fn ec_queue_isolates_buckets_by_ordinal_and_preserves_fifo() {
        let defer = DeferralQueue::new();
        let ec = EcQueue::new(Some(Arc::clone(&defer)), Duration::from_millis(5), 10_000);
        // Two distinct triggers (same name, different ordinal → distinct bucket).
        let a = ec_route("t", 0);
        let b = ec_route("t", 1);

        // Interleave enqueues; per-bucket FIFO must be preserved.
        ec.enqueue(&a, "s", 1, int_batch(&[10]));
        ec.enqueue(&b, "s", 1, int_batch(&[20]));
        ec.enqueue(&a, "s", 1, int_batch(&[11]));
        ec.enqueue(&b, "s", 1, int_batch(&[21]));
        assert_eq!(ec.bucket_count(), 2);

        std::thread::sleep(Duration::from_millis(10));
        ec.flush_due(StdInstant::now());
        assert_eq!(defer.pending(), 2, "each bucket flushes independently");

        let mut due = defer.drain_due(StdInstant::now() + Duration::from_secs(1));
        due.sort_by_key(|item| item_values(item)[0]);
        assert_eq!(item_values(&due[0]), vec![10, 11]);
        assert_eq!(item_values(&due[1]), vec![20, 21]);
    }

    #[test]
    fn ec_queue_backpressure_force_drains_without_data_loss() {
        let defer = DeferralQueue::new();
        // threshold 2 → back-pressure cap = 8 rows.
        let ec = EcQueue::new(Some(Arc::clone(&defer)), Duration::from_secs(3600), 2);
        let entry = ec_route("t", 0);

        // Flood past the 4× cap in a single burst of 1-row batches. The
        // interval is an hour, so age never triggers: only inline
        // back-pressure drains (rows > 8) can move data to the defer queue.
        for i in 0..20i64 {
            ec.enqueue(&entry, "s", 1, int_batch(&[i]));
        }
        // The force-drains fired inline; a sub-cap tail may still be
        // buffered in the bucket. Collect both to prove zero data loss.
        let mut all: Vec<i64> = defer
            .drain_due(StdInstant::now() + Duration::from_secs(1))
            .iter()
            .flat_map(item_values)
            .collect();
        assert!(!all.is_empty(), "back-pressure must have force-drained");
        // Flush whatever remained below the cap (size threshold is 2).
        ec.flush_due(StdInstant::now());
        all.extend(
            defer
                .drain_due(StdInstant::now() + Duration::from_secs(1))
                .iter()
                .flat_map(item_values),
        );
        // Every enqueued row survived exactly once — nothing dropped, nothing
        // duplicated, across the inline drains + the tail flush.
        all.sort_unstable();
        assert_eq!(
            all,
            (0..20).collect::<Vec<_>>(),
            "no rows dropped or duplicated under back-pressure"
        );
        assert_eq!(ec.bucket_count(), 0, "bucket fully drained");
    }

    #[test]
    fn ec_queue_concat_failure_falls_back_to_individual_batches() {
        // Two incompatible schemas in one bucket force concat to fail; the
        // fallback must push each pending batch individually (no data loss).
        let defer = DeferralQueue::new();
        let ec = EcQueue::new(Some(Arc::clone(&defer)), Duration::from_millis(1), 10_000);
        let entry = ec_route("t", 0);

        let good = int_batch(&[1, 2]);
        let drifted_schema = Arc::new(Schema::new(vec![Field::new("m", DataType::Utf8, false)]));
        let drifted = Arc::new(
            RecordBatch::try_new(
                drifted_schema,
                vec![Arc::new(arrow_array::StringArray::from(vec!["x"]))],
            )
            .expect("valid batch"),
        );
        ec.enqueue(&entry, "s", 1, good);
        ec.enqueue(&entry, "s", 1, drifted);

        std::thread::sleep(Duration::from_millis(5));
        ec.flush_due(StdInstant::now());
        // Fallback path: 2 separate items rather than 1 coalesced one.
        assert_eq!(defer.pending(), 2, "concat failure → individual pushes");
    }

    #[test]
    fn ec_queue_without_wired_defer_queue_is_noop_on_flush() {
        // No wired defer queue: buffering is allowed but flush cannot land
        // items anywhere — must not panic, must not lose the bucket silently
        // to a phantom queue.
        let ec = EcQueue::new(None, Duration::from_millis(1), 10_000);
        let entry = ec_route("t", 0);
        ec.enqueue(&entry, "s", 1, int_batch(&[1]));
        std::thread::sleep(Duration::from_millis(5));
        ec.flush_due(StdInstant::now()); // no-op, no panic
        assert_eq!(ec.bucket_count(), 1, "bucket retained when no queue wired");

        // Silence unused-import warnings for the deferral helper type.
        let _ = TriggerDeferral::from_payload("");
    }
}
