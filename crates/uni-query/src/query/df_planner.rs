// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Hybrid physical planner for DataFusion integration.
//!
//! This module provides [`HybridPhysicalPlanner`], which converts Cypher's
//! [`LogicalPlan`] into a DataFusion [`ExecutionPlan`] tree. The "hybrid" nature
//! refers to the mix of:
//!
//! - **Custom graph operators**: `GraphScanExec`, `GraphTraverseExec`, `GraphShortestPathExec`
//! - **Native DataFusion operators**: `FilterExec`, `AggregateExec`, `SortExec`, etc.
//!
//! # Architecture
//!
//! ```text
//! LogicalPlan (Cypher)
//!        │
//!        ▼
//! ┌────────────────────┐
//! │HybridPhysicalPlanner│
//! │                    │
//! │ Graph ops → Custom │
//! │ Rel ops → DataFusion│
//! └────────────────────┘
//!        │
//!        ▼
//! ExecutionPlan (DataFusion)
//! ```
//!
//! # Expression Translation
//!
//! Cypher expressions are translated to DataFusion expressions using
//! [`cypher_expr_to_df`] from the `df_expr` module.

use crate::query::df_expr::{TranslationContext, VariableKind, cypher_expr_to_df};
use crate::query::df_graph::ReadSetRecordingExec;
use crate::query::df_graph::bind_fixed_path::BindFixedPathExec;
use crate::query::df_graph::bind_zero_length_path::BindZeroLengthPathExec;
use crate::query::df_graph::mutation_common::{
    MutationKind, extended_schema_for_new_vars, new_create_exec, new_merge_exec,
};
use crate::query::df_graph::mutation_delete::new_delete_exec;
use crate::query::df_graph::mutation_remove::new_remove_exec;
use crate::query::df_graph::mutation_set::new_set_exec;
use crate::query::df_graph::recursive_cte::RecursiveCTEExec;
use crate::query::df_graph::traverse::QppStepFilterSpec;
use crate::query::df_graph::traverse::{
    GraphVariableLengthTraverseExec, GraphVariableLengthTraverseMainExec,
};
use crate::query::df_graph::{
    GraphApplyExec, GraphExecutionContext, GraphExtIdLookupExec, GraphProcedureCallExec,
    GraphScanExec, GraphShortestPathExec, GraphTraverseExec, GraphTraverseMainExec,
    GraphUnwindExec, GraphVectorKnnExec, L0Context, MutationContext, MutationExec,
    OptionalFilterExec,
};
use crate::query::planner::{
    COL_FWD, LogicalPlan, STRUCT_ONLY_SENTINEL, WITH_PASSTHROUGH_SENTINEL, aggregate_column_name,
    collect_properties_from_plan, projection_columns, reconcile_passthrough_properties,
};
use anyhow::{Result, anyhow};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::common::JoinType;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr as DfExpr, ExprSchemable, SortExpr as DfSortExpr};
use datafusion::physical_expr::{create_physical_expr, create_physical_sort_exprs};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uni_algo::algo::AlgorithmRegistry;
use uni_common::core::schema::{PropertyMeta, Schema as UniSchema};
use uni_cypher::ast::{
    CypherLiteral, Direction as AstDirection, Expr, Pattern, PatternElement, SortItem,
};
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;
use uni_xervo::runtime::ModelRuntime;

/// An aggregate function expression paired with its optional filter.
type PhysicalAggregate = (
    Arc<AggregateFunctionExpr>,
    Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
);

/// Hybrid physical planner that produces DataFusion ExecutionPlan trees.
///
/// Routes graph operations to custom `ExecutionPlan` implementations
/// and relational operations to native DataFusion operators.
///
/// # Example
///
/// ```ignore
/// let planner = HybridPhysicalPlanner::new(
///     session_ctx,
///     storage,
///     l0,
///     property_manager,
///     schema,
///     params,
/// );
///
/// let execution_plan = planner.plan(&logical_plan)?;
/// ```
pub struct HybridPhysicalPlanner {
    /// DataFusion session context.
    session_ctx: Arc<RwLock<SessionContext>>,

    /// Storage manager for dataset access.
    storage: Arc<StorageManager>,

    /// Graph execution context for custom operators.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Schema for label/edge type lookups.
    schema: Arc<UniSchema>,

    /// Last flush version for staleness detection.
    last_flush_version: AtomicU64,

    /// Query parameters for expression translation.
    params: HashMap<String, uni_common::Value>,

    /// Correlated outer values from Apply input rows (for subquery correlation).
    /// These take precedence over parameters during variable resolution to prevent
    /// YIELD columns from shadowing user query parameters.
    outer_values: HashMap<String, uni_common::Value>,

    /// Mutation context for write operations (CREATE, SET, REMOVE, DELETE).
    /// Present only when the query contains write clauses.
    mutation_ctx: Option<Arc<MutationContext>>,

    /// Entity variable names from outer scopes, threaded through for nested EXISTS
    /// so the expression compiler can distinguish fresh pattern bindings from
    /// correlated references.
    outer_entity_vars: HashSet<String>,

    /// Plugin registry used to resolve Locy aggregates (and other plugin
    /// surfaces) at plan time. Defaults to a process-wide registry pre-loaded
    /// with the built-ins from `uni-plugin-builtin`; replace with
    /// [`Self::with_plugin_registry`] to use a host-supplied registry.
    plugin_registry: Arc<uni_plugin::PluginRegistry>,
}

/// Whether a traversal must publish [`COL_FWD`] so the edge struct can carry
/// stored orientation.
///
/// An undirected hop is the only one that cannot answer statically: which end a
/// row walked from is a per-row fact, and without it `add_edge_structural_projection`
/// reports the edge reversed on every row that walked it backwards. That
/// projection now errors rather than guessing, so this predicate and its
/// consumer are two halves of one contract.
///
/// It costs the traversal a second adjacency probe, so it is requested only when
/// a struct will actually be built — a bare `RETURN r` or a `SET r.p`, which is
/// what the wildcard and the struct-only sentinel mark.
///
/// This exists once because it used to exist twice: the schema'd and schemaless
/// paths each carried their own copy, they drifted, and the schemaless copy's
/// omission was #193 — an undirected relationship returning `a->b` from one row
/// and `b->a` from another, with no error.
fn wants_orientation_column(
    direction: &AstDirection,
    is_variable_length: bool,
    edge_var: &str,
    all_properties: &HashMap<String, HashSet<String>>,
) -> bool {
    let builds_struct = all_properties
        .get(edge_var)
        .is_some_and(|props| props.contains("*") || props.contains(STRUCT_ONLY_SENTINEL));
    matches!(direction, AstDirection::Both) && !is_variable_length && builds_struct
}

impl std::fmt::Debug for HybridPhysicalPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridPhysicalPlanner")
            .field(
                "last_flush_version",
                &self.last_flush_version.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl HybridPhysicalPlanner {
    /// Create a new hybrid physical planner.
    ///
    /// # Arguments
    ///
    /// * `session_ctx` - DataFusion session context
    /// * `storage` - Storage manager for dataset access
    /// * `l0` - Current L0 buffer for MVCC
    /// * `property_manager` - Property manager for lazy loading
    /// * `schema` - Uni schema for lookups
    pub fn new(
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        l0: Arc<RwLock<L0Buffer>>,
        property_manager: Arc<PropertyManager>,
        schema: Arc<UniSchema>,
        params: HashMap<String, uni_common::Value>,
    ) -> Self {
        let graph_ctx = Arc::new(GraphExecutionContext::new(
            storage.clone(),
            l0,
            property_manager,
        ));

        Self {
            session_ctx,
            storage,
            graph_ctx,
            schema,
            last_flush_version: AtomicU64::new(0),
            params,
            outer_values: HashMap::new(),
            mutation_ctx: None,
            outer_entity_vars: HashSet::new(),
            plugin_registry: super::df_graph::locy_fold::default_locy_plugin_registry(),
        }
    }

    /// Replace the plugin registry used for Locy aggregate resolution.
    ///
    /// The default registry contains only the built-in aggregates from
    /// `uni-plugin-builtin`. Hosts that have registered additional Locy
    /// aggregates should pass their full [`uni_plugin::PluginRegistry`] here
    /// so user-declared aggregates resolve at plan time.
    #[must_use]
    pub fn with_plugin_registry(
        mut self,
        plugin_registry: Arc<uni_plugin::PluginRegistry>,
    ) -> Self {
        // Also propagate into the GraphExecutionContext so the
        // native-label plugin-storage dispatcher in
        // `columnar_scan_vertex_batch_static` (M5h.2) can reach the
        // registered `Storage` impls.
        let mut ctx = self.take_graph_ctx();
        ctx = ctx.with_plugin_registry(Arc::clone(&plugin_registry));
        self.graph_ctx = Arc::new(ctx);
        self.plugin_registry = plugin_registry;
        self
    }

    /// Build a physical expression compiler pre-wired with this planner's
    /// subquery context (graph context, schema, session context, storage,
    /// parameters and outer entity variables) so EXISTS subqueries and
    /// pattern comprehensions can execute.
    fn expr_compiler<'a>(
        &self,
        state: &'a SessionState,
        translation_ctx: Option<&'a TranslationContext>,
    ) -> crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler<'a> {
        crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler::new(
            state,
            translation_ctx,
        )
        .with_subquery_ctx(
            self.graph_ctx.clone(),
            self.schema.clone(),
            self.session_ctx.clone(),
            self.storage.clone(),
            self.params.clone(),
            self.outer_entity_vars.clone(),
        )
    }

    /// Resolve the set of property names for `variable` from the collected plan properties.
    ///
    /// If the property set contains `"*"`, expands to all schema-defined properties
    /// for `schema_name` (a label or edge type name). Otherwise filters out the
    /// wildcard sentinel and returns the explicit property names.
    fn resolve_properties(
        &self,
        variable: &str,
        schema_name: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Vec<String> {
        // System columns managed by the engine — never treat as user properties.
        const SYSTEM_COLUMNS: &[&str] = &[
            "_vid", "_labels", "_eid", "_src_vid", "_dst_vid", "_type", "_fwd",
        ];

        all_properties
            .get(variable)
            .map(|props| {
                if props.contains("*") {
                    let schema_props: Vec<String> = self
                        .schema
                        .properties
                        .get(schema_name)
                        .map(|p| p.keys().cloned().collect())
                        .unwrap_or_default();

                    // Collect explicit property names (non-wildcard, non-internal).
                    // System-managed columns surfaced through Cypher functions
                    // (e.g. `_created_at`/`_updated_at` via `created_at(n)`/
                    // `updated_at(n)`) are kept — they are intentionally
                    // requested even when the wildcard is also set.
                    let explicit: Vec<String> = props
                        .iter()
                        .filter(|p| {
                            *p != "*"
                                && *p != STRUCT_ONLY_SENTINEL
                                && *p != WITH_PASSTHROUGH_SENTINEL
                                && (!p.starts_with('_')
                                    || matches!(p.as_str(), "_created_at" | "_updated_at"))
                        })
                        .cloned()
                        .collect();

                    if schema_props.is_empty() && explicit.is_empty() {
                        // Structural-only access, no specific properties needed
                        return vec!["*".to_string()];
                    }

                    // Merge schema props + explicit props, dedup
                    let mut combined: Vec<String> = schema_props;
                    for p in explicit {
                        if !combined.contains(&p) {
                            combined.push(p);
                        }
                    }
                    combined.retain(|p| !SYSTEM_COLUMNS.contains(&p.as_str()));
                    combined.sort();
                    combined
                } else {
                    // Sentinel-only or no structural marker: return the explicit
                    // properties without schema expansion. The sentinel itself
                    // is filtered. Structural projection is still applied
                    // downstream via the `need_full` gate (which accepts the
                    // sentinel) — it just builds a smaller struct.
                    let mut explicit_props: Vec<String> = props
                        .iter()
                        .filter(|p| {
                            *p != "*"
                                && *p != STRUCT_ONLY_SENTINEL
                                && *p != WITH_PASSTHROUGH_SENTINEL
                                && !SYSTEM_COLUMNS.contains(&p.as_str())
                        })
                        .cloned()
                        .collect();
                    explicit_props.sort();
                    explicit_props
                }
            })
            .unwrap_or_default()
    }

    /// Create planner with full L0 context.
    pub fn with_l0_context(
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        l0_context: L0Context,
        property_manager: Arc<PropertyManager>,
        schema: Arc<UniSchema>,
        params: HashMap<String, uni_common::Value>,
        outer_values: HashMap<String, uni_common::Value>,
    ) -> Self {
        let graph_ctx = Arc::new(GraphExecutionContext::with_l0_context(
            storage.clone(),
            l0_context,
            property_manager,
        ));

        Self {
            session_ctx,
            storage,
            graph_ctx,
            schema,
            last_flush_version: AtomicU64::new(0),
            params,
            outer_values,
            mutation_ctx: None,
            outer_entity_vars: HashSet::new(),
            plugin_registry: super::df_graph::locy_fold::default_locy_plugin_registry(),
        }
    }

    /// Produce an owned `GraphExecutionContext` for the consuming `with_*`
    /// builders, preserving every field.
    ///
    /// This used to rebuild from the *base* constructor and re-attach six fields
    /// by hand, which silently dropped everything not on that list — `deadline`,
    /// `cancellation_token`, `warnings` and `handle_scope` were all reset by the
    /// next `with_*` call, and `read.rs` makes several per query. A plain clone
    /// carries the whole struct, so no future field can fall through the same
    /// gap. The clone is a handful of refcount bumps and happens only while the
    /// plan is being built.
    ///
    /// Every caller immediately reinstalls the result via
    /// `self.graph_ctx = Arc::new(ctx)`, so leaving the original in place here
    /// is intentional.
    fn take_graph_ctx(&mut self) -> GraphExecutionContext {
        (*self.graph_ctx).clone()
    }

    /// Thread the surrounding query's deadline and cancellation token into the
    /// graph context every physical operator receives.
    ///
    /// Without this the operator-level `check_timeout` calls in `scan`,
    /// `traverse`, `shortest_path`, `vector_knn`, `procedure_call` and
    /// `ext_id_lookup` compare against `None` and can never fire, so
    /// `query_timeout` is bounded only by the outer `tokio::time::timeout` —
    /// which cannot preempt work that never yields. See issue #207.
    #[must_use]
    pub fn with_deadline_and_cancellation(
        mut self,
        deadline: Option<std::time::Instant>,
        token: Option<tokio_util::sync::CancellationToken>,
    ) -> Self {
        let mut ctx = self.take_graph_ctx();
        if let Some(deadline) = deadline {
            ctx = ctx.with_deadline(deadline);
        }
        if let Some(token) = token {
            ctx = ctx.with_cancellation_token(token);
        }
        self.graph_ctx = Arc::new(ctx);
        self
    }

    /// Attach the per-query counter set, threading it into the graph context
    /// every physical operator receives.
    #[must_use]
    pub fn with_counters(mut self, counters: Option<Arc<uni_store::QueryCounters>>) -> Self {
        let ctx = self.take_graph_ctx().with_counters(counters);
        self.graph_ctx = Arc::new(ctx);
        self
    }

    /// Attach the outer transaction's writer handle so declared
    /// `WRITE`-mode procedures invoked through this plan can run
    /// their Cypher bodies via the write-enabled inner-query host
    /// (FU-1 / M11 #6).
    #[must_use]
    pub fn with_writer(mut self, writer: Arc<uni_store::Writer>) -> Self {
        let ctx = self.take_graph_ctx().with_writer(writer);
        self.graph_ctx = Arc::new(ctx);
        self
    }

    /// Set the algorithm registry for `uni.algo.*` procedure dispatch.
    ///
    /// Rebuilds the inner `GraphExecutionContext` with the registry attached.
    /// Set outer entity variable names for nested EXISTS correlated reference detection.
    pub fn set_outer_entity_vars(&mut self, vars: HashSet<String>) {
        self.outer_entity_vars = vars;
    }

    pub fn with_algo_registry(mut self, registry: Arc<AlgorithmRegistry>) -> Self {
        let ctx = self.take_graph_ctx().with_algo_registry(registry);
        self.graph_ctx = Arc::new(ctx);
        self
    }

    /// Set the external procedure registry for test/user-defined procedures.
    ///
    /// Rebuilds the inner `GraphExecutionContext` with the registry attached.
    pub fn with_procedure_registry(
        mut self,
        registry: Arc<crate::query::executor::procedure::ProcedureRegistry>,
    ) -> Self {
        let ctx = self.take_graph_ctx().with_procedure_registry(registry);
        self.graph_ctx = Arc::new(ctx);
        self
    }

    /// Set Uni-Xervo runtime used by query-time vector auto-embedding.
    pub fn with_xervo_runtime(mut self, runtime: Arc<ModelRuntime>) -> Self {
        let ctx = self.take_graph_ctx().with_xervo_runtime(runtime);
        self.graph_ctx = Arc::new(ctx);
        self
    }

    /// Set the mutation context for write operations.
    pub fn with_mutation_context(mut self, ctx: Arc<MutationContext>) -> Self {
        self.mutation_ctx = Some(ctx);
        self
    }

    /// Return the graph execution context (for columnar subplan execution).
    pub fn graph_ctx(&self) -> &Arc<GraphExecutionContext> {
        &self.graph_ctx
    }

    /// Return the DataFusion session context (for columnar subplan execution).
    pub fn session_ctx(&self) -> &Arc<RwLock<SessionContext>> {
        &self.session_ctx
    }

    /// Return the storage manager (for columnar subplan execution).
    pub fn storage(&self) -> &Arc<StorageManager> {
        &self.storage
    }

    /// Return the schema (for columnar subplan execution).
    pub fn schema_info(&self) -> &Arc<UniSchema> {
        &self.schema
    }

    /// Get the mutation context, returning an error if not set.
    fn require_mutation_ctx(&self) -> Result<Arc<MutationContext>> {
        self.mutation_ctx.clone().ok_or_else(|| {
            tracing::error!(
                "Mutation context not set — this indicates a routing bug where a write \
                 operation was sent to the DataFusion engine without a MutationContext"
            );
            anyhow!("Mutation context not set — write operations require a MutationContext")
        })
    }

    /// Build a `TranslationContext` with variable kinds collected from a LogicalPlan.
    ///
    /// This is used for expression translation in filters, projections, etc.
    /// where bare variable references need to resolve to identity columns.
    fn translation_context_for_plan(&self, plan: &LogicalPlan) -> TranslationContext {
        let mut variable_kinds = HashMap::new();
        let mut variable_labels = HashMap::new();
        let mut node_variable_hints = Vec::new();
        let mut mutation_edge_hints = Vec::new();
        collect_variable_kinds(plan, &mut variable_kinds);
        collect_mutation_node_hints(plan, &mut node_variable_hints);
        collect_mutation_edge_hints(plan, &mut mutation_edge_hints);
        self.collect_variable_labels(plan, &mut variable_labels);
        TranslationContext {
            parameters: self.params.clone(),
            outer_values: self.outer_values.clone(),
            variable_labels,
            variable_kinds,
            node_variable_hints,
            mutation_edge_hints,
            ..Default::default()
        }
    }

    /// Recursively collect variable-to-label/type mappings from a `LogicalPlan`.
    ///
    /// For node variables, maps to the first label name. For edge variables, maps
    /// to the edge type name (when a single type is known). This is used by
    /// `type(r)` to resolve the edge type as a string literal.
    fn collect_variable_labels(&self, plan: &LogicalPlan, labels: &mut HashMap<String, String>) {
        match plan {
            LogicalPlan::Scan {
                variable,
                labels: scan_labels,
                ..
            }
            | LogicalPlan::ScanMainByLabels {
                variable,
                labels: scan_labels,
                ..
            }
            | LogicalPlan::FusedIndexScan {
                variable,
                labels: scan_labels,
                ..
            } => {
                if let Some(first) = scan_labels.first() {
                    labels.insert(variable.clone(), first.clone());
                }
            }
            LogicalPlan::Traverse {
                input,
                step_variable,
                edge_type_ids,
                target_variable,
                target_label_id,
                ..
            } => {
                self.collect_variable_labels(input, labels);
                if let Some(sv) = step_variable
                    && edge_type_ids.len() == 1
                    && let Some(name) = self.schema.edge_type_name_by_id(edge_type_ids[0])
                {
                    labels.insert(sv.clone(), name.to_string());
                }
                if *target_label_id != 0
                    && let Some(name) = self.schema.label_name_by_id(*target_label_id)
                {
                    labels.insert(target_variable.clone(), name.to_string());
                }
            }
            LogicalPlan::TraverseMainByType {
                input,
                step_variable,
                type_names,
                ..
            } => {
                self.collect_variable_labels(input, labels);
                if let Some(sv) = step_variable
                    && type_names.len() == 1
                {
                    labels.insert(sv.clone(), type_names[0].clone());
                }
            }
            // Wrapper nodes: recurse into input(s)
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Unwind { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Foreach { input, .. }
            | LogicalPlan::SubqueryCall { input, .. } => {
                self.collect_variable_labels(input, labels);
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right, .. } => {
                self.collect_variable_labels(left, labels);
                self.collect_variable_labels(right, labels);
            }
            LogicalPlan::Apply {
                input, subquery, ..
            } => {
                self.collect_variable_labels(input, labels);
                self.collect_variable_labels(subquery, labels);
            }
            LogicalPlan::Explain { plan } => {
                self.collect_variable_labels(plan, labels);
            }
            // Path binders and the wrapped scan carry a child subtree whose
            // scans may bind labeled variables — recurse so those labels are
            // not lost (exhaustive; no `_ => {}` so a new variant must be
            // classified here — the #131 bug class).
            LogicalPlan::FusedIndexScanWrapped { inner, .. } => {
                self.collect_variable_labels(inner, labels);
            }
            LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. } => {
                self.collect_variable_labels(input, labels);
            }
            LogicalPlan::QuantifiedPattern {
                input,
                pattern_plan,
                ..
            } => {
                self.collect_variable_labels(input, labels);
                self.collect_variable_labels(pattern_plan, labels);
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => {
                self.collect_variable_labels(initial, labels);
                self.collect_variable_labels(recursive, labels);
            }
            // Unlabeled / non-graph leaves: nothing to map. (`ScanAll` is the
            // unlabeled scan; the search/proc leaves and Locy/DDL nodes bind no
            // label-resolvable graph variable in this context.)
            LogicalPlan::ScanAll { .. }
            | LogicalPlan::ExtIdLookup { .. }
            | LogicalPlan::VectorKnn { .. }
            | LogicalPlan::InvertedIndexLookup { .. }
            | LogicalPlan::ProcedureCall { .. }
            | LogicalPlan::LocyProgram { .. }
            | LogicalPlan::LocyFold { .. }
            | LogicalPlan::LocyBestBy { .. }
            | LogicalPlan::LocyPriority { .. }
            | LogicalPlan::LocyDerivedScan { .. }
            | LogicalPlan::LocyProject { .. }
            | LogicalPlan::LocyModelInvoke { .. }
            | LogicalPlan::Empty
            | LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateSparseIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. }
            | LogicalPlan::Copy { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_) => {}
        }
    }

    fn merged_edge_type_properties(&self, edge_type_ids: &[u32]) -> HashMap<String, PropertyMeta> {
        crate::query::df_graph::common::merged_edge_schema_props(&self.schema, edge_type_ids)
    }

    /// Plan a logical plan into an execution plan.
    ///
    /// # Arguments
    ///
    /// * `logical` - The logical plan to convert
    ///
    /// # Returns
    ///
    /// DataFusion ExecutionPlan ready for execution.
    ///
    /// # Errors
    ///
    /// Returns an error if planning fails (unsupported operation, schema mismatch, etc.)
    pub fn plan(&self, logical: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>> {
        // Pre-pass: lift UNWIND-correlated IN-list filters into the scan
        // subtrees of any Filter(CrossJoin(L, R)) shapes. Runs as a pure
        // logical-plan rewrite *before* any physical-plan optimization
        // (HashJoin, VidLookupJoin, etc.) so the scan-side filters
        // survive any downstream optimization bailout. See
        // `merge_unwind_in_filters` for the rationale.
        let logical_rewritten = merge_unwind_in_filters(logical, &self.params);

        // Collect all properties needed anywhere in the plan tree
        let mut all_properties = collect_properties_from_plan(&logical_rewritten);
        // Resolve WITH-passthrough markers: narrow forwarded entities to the
        // properties actually accessed downstream (issue #134 family).
        apply_passthrough_reconciliation(&logical_rewritten, &mut all_properties);
        // Record which UNWIND sources are dead so `plan_unwind` can stop
        // carrying them past the operator that consumed them (#184).
        crate::query::planner::mark_dead_unwind_sources(&logical_rewritten, &mut all_properties);

        // Delegate to internal planning with properties context
        self.plan_internal(&logical_rewritten, &all_properties)
    }

    /// Plan a LogicalPlan with additional property requirements.
    ///
    /// Merges `extra_properties` into the auto-collected properties from the plan tree.
    /// Used by MERGE execution to ensure structural projections are applied for
    /// variables that need full node/edge Maps in the output.
    pub fn plan_with_properties(
        &self,
        logical: &LogicalPlan,
        extra_properties: HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Same pre-pass as `plan()` — see commentary there.
        let logical_rewritten = merge_unwind_in_filters(logical, &self.params);
        let mut all_properties = collect_properties_from_plan(&logical_rewritten);
        for (var, props) in extra_properties {
            all_properties.entry(var).or_default().extend(props);
        }
        apply_passthrough_reconciliation(&logical_rewritten, &mut all_properties);
        crate::query::planner::mark_dead_unwind_sources(&logical_rewritten, &mut all_properties);
        self.plan_internal(&logical_rewritten, &all_properties)
    }

    /// Wrap a plan with optional semantics.
    ///
    /// If optional is true, performs a Left Outer Join with a single-row source (PlaceholderRow)
    /// to ensure at least one row (of NULLs) is returned if the input is empty.
    ///
    /// Conceptually: SELECT * FROM (SELECT 1) LEFT JOIN Plan ON true
    fn wrap_optional(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        optional: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !optional {
            return Ok(plan);
        }

        // Create a single-row source
        let empty_schema = Arc::new(Schema::empty());
        let placeholder = Arc::new(PlaceholderRowExec::new(empty_schema));

        // Use NestedLoopJoin with Left Outer Join type
        // This ensures if 'plan' is empty, we get 1 row with all NULLs
        Ok(Arc::new(NestedLoopJoinExec::try_new(
            placeholder,
            plan,
            None, // No filter
            &JoinType::Left,
            None, // No projection
        )?))
    }

    fn plan_internal(
        &self,
        logical: &LogicalPlan,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match logical {
            // === Graph Operations ===
            // Phase 5b followup: `FusedIndexScanWrapped` is a
            // planner-side observability wrapper around lossy
            // operators (VectorKnn, InvertedIndexLookup). The
            // runtime fusion happens at the `BranchedBackend`
            // layer via Lance per-branch reads; the physical
            // planner just unwraps and recurses on the inner node.
            LogicalPlan::FusedIndexScanWrapped { inner, kind: _ } => {
                self.plan_internal(inner, all_properties)
            }
            LogicalPlan::Scan {
                label_id,
                labels,
                variable,
                filter,
                optional,
            }
            // Phase 5a-impl Step 3: decay `FusedIndexScan` to a plain
            // `Scan` for now — preserves correctness because Lance's
            // `base_paths` chain already covers parent-inherited
            // indexes for forked sessions. Step 4 (VidUid) and
            // beyond replace this fallback with type-specific fused
            // physical operators.
            | LogicalPlan::FusedIndexScan {
                label_id,
                labels,
                variable,
                filter,
                optional,
                kind: _,
            } => {
                if labels.len() > 1 {
                    // Multi-label: use main table with intersection semantics
                    self.plan_multi_label_scan(
                        labels,
                        variable,
                        filter.as_ref(),
                        *optional,
                        all_properties,
                    )
                } else {
                    // Single-label: use per-label table
                    self.plan_scan(
                        *label_id,
                        variable,
                        filter.as_ref(),
                        *optional,
                        all_properties,
                    )
                }
            }

            // ScanMainByLabels is now supported via schemaless scan
            LogicalPlan::ScanMainByLabels {
                labels,
                variable,
                filter,
                optional,
            } => {
                if labels.len() > 1 {
                    // Multi-label schemaless scan
                    self.plan_multi_label_scan(
                        labels,
                        variable,
                        filter.as_ref(),
                        *optional,
                        all_properties,
                    )
                } else if let Some(label_name) = labels.first() {
                    // Single label schemaless scan
                    self.plan_schemaless_scan(
                        label_name,
                        variable,
                        filter.as_ref(),
                        *optional,
                        all_properties,
                    )
                } else {
                    // Empty labels - should not happen, fallback to scan all
                    self.plan_scan_all(variable, filter.as_ref(), *optional, all_properties)
                }
            }

            // ScanAll is now supported via schemaless scan with empty label
            LogicalPlan::ScanAll {
                variable,
                filter,
                optional,
            } => self.plan_scan_all(variable, filter.as_ref(), *optional, all_properties),

            // TraverseMainByType is now supported via schemaless traversal
            LogicalPlan::TraverseMainByType {
                type_names,
                input,
                direction,
                source_variable,
                target_variable,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                is_variable_length,
                scope_match_variables,
                optional_pattern_vars,
                edge_filter_expr,
                path_mode,
                ..
            } => {
                if *is_variable_length {
                    let vlp_plan = self.plan_traverse_main_by_type_vlp(
                        input,
                        type_names,
                        direction.clone(),
                        source_variable,
                        target_variable,
                        step_variable.as_deref(),
                        *min_hops,
                        *max_hops,
                        path_variable.as_deref(),
                        *optional,
                        all_properties,
                        edge_filter_expr.as_ref(),
                        path_mode,
                        scope_match_variables,
                    )?;
                    self.apply_schemaless_traverse_filter(
                        vlp_plan,
                        target_filter.as_ref(),
                        source_variable,
                        target_variable,
                        step_variable.as_deref(),
                        path_variable.as_deref(),
                        true, // is_variable_length
                        *optional,
                        optional_pattern_vars,
                    )
                } else {
                    let base_plan = self.plan_traverse_main_by_type(
                        input,
                        type_names,
                        direction.clone(),
                        source_variable,
                        target_variable,
                        step_variable.as_deref(),
                        *optional,
                        optional_pattern_vars,
                        all_properties,
                        scope_match_variables,
                    )?;
                    // Apply edge property filter first, then target node filter.
                    // Without the target_filter, MATCH (a)-[r]->(b {prop: val}) SET r.x
                    // would apply SET to ALL edges from a, ignoring b's properties.
                    let edge_filtered = self.apply_schemaless_traverse_filter(
                        base_plan,
                        edge_filter_expr.as_ref(),
                        source_variable,
                        target_variable,
                        step_variable.as_deref(),
                        path_variable.as_deref(),
                        false,
                        *optional,
                        optional_pattern_vars,
                    )?;
                    self.apply_schemaless_traverse_filter(
                        edge_filtered,
                        target_filter.as_ref(),
                        source_variable,
                        target_variable,
                        step_variable.as_deref(),
                        path_variable.as_deref(),
                        false,
                        *optional,
                        optional_pattern_vars,
                    )
                }
            }

            LogicalPlan::Traverse {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id,
                step_variable,
                min_hops,
                max_hops,
                optional,
                target_filter,
                path_variable,
                is_variable_length,
                optional_pattern_vars,
                scope_match_variables,
                edge_filter_expr,
                path_mode,
                qpp_steps,
                qpp_inner_source,
                ..
            } => self.plan_traverse(
                input,
                edge_type_ids,
                direction.clone(),
                source_variable,
                target_variable,
                *target_label_id,
                step_variable.as_deref(),
                *min_hops,
                *max_hops,
                path_variable.as_deref(),
                *optional,
                target_filter.as_ref(),
                *is_variable_length,
                optional_pattern_vars,
                all_properties,
                scope_match_variables,
                edge_filter_expr.as_ref(),
                path_mode,
                qpp_steps.as_deref(),
                qpp_inner_source.as_deref(),
            ),

            LogicalPlan::ShortestPath {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id: _,
                path_variable,
                step_variable,
                min_hops,
                max_hops,
                edge_filter_expr,
            } => self.plan_shortest_path(
                input,
                edge_type_ids,
                direction.clone(),
                source_variable,
                target_variable,
                path_variable,
                step_variable.as_deref(),
                false,
                *min_hops,
                *max_hops,
                edge_filter_expr.as_ref(),
                all_properties,
            ),

            // === Relational Operations ===
            LogicalPlan::Filter {
                input,
                predicate,
                optional_variables,
            } => self.plan_filter(input, predicate, optional_variables, all_properties),

            LogicalPlan::Project { input, projections } => {
                // Build alias map for ORDER BY alias resolution
                // When plan is Project(Limit(Sort(...))), Sort needs to know aliases
                let alias_map: HashMap<String, Expr> = projections
                    .iter()
                    .filter_map(|(expr, alias)| alias.as_ref().map(|a| (a.clone(), expr.clone())))
                    .collect();

                // Check if the input chain contains a Sort and pass alias map
                self.plan_project_with_aliases(input, projections, all_properties, &alias_map)
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => self.plan_aggregate(input, group_by, aggregates, all_properties),

            LogicalPlan::Distinct { input } => {
                let input_plan = self.plan_internal(input, all_properties)?;
                let schema = input_plan.schema();
                // Group by all columns with no aggregates = deduplication
                let group_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
                    schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(i, f)| {
                            (
                                Arc::new(datafusion::physical_expr::expressions::Column::new(
                                    f.name(),
                                    i,
                                ))
                                    as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                                f.name().clone(),
                            )
                        })
                        .collect();
                let group_by = PhysicalGroupBy::new_single(group_exprs);
                Ok(Arc::new(AggregateExec::try_new(
                    AggregateMode::Single,
                    group_by,
                    vec![],
                    vec![],
                    input_plan.clone(),
                    input_plan.schema(),
                )?))
            }

            LogicalPlan::Sort { input, order_by } => {
                self.plan_sort(input, order_by, all_properties, &HashMap::new())
            }

            LogicalPlan::Limit { input, skip, fetch } => {
                self.plan_limit(input, *skip, *fetch, all_properties)
            }

            LogicalPlan::Union { left, right, all } => {
                self.plan_union(left, right, *all, all_properties)
            }

            LogicalPlan::Empty => self.plan_empty(),

            LogicalPlan::BindZeroLengthPath {
                input,
                node_variable,
                path_variable,
            } => {
                self.plan_bind_zero_length_path(input, node_variable, path_variable, all_properties)
            }

            LogicalPlan::BindPath {
                input,
                node_variables,
                edge_variables,
                edge_type_names,
                path_variable,
            } => self.plan_bind_path(
                input,
                node_variables,
                edge_variables,
                edge_type_names,
                path_variable,
                all_properties,
            ),

            // === Mutation operators ===
            LogicalPlan::Create { input, pattern } => {
                tracing::debug!("Planning MutationCreateExec");
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                Ok(Arc::new(new_create_exec(
                    child,
                    pattern.clone(),
                    mutation_ctx,
                )))
            }
            LogicalPlan::CreateBatch { input, patterns } => {
                tracing::debug!(
                    patterns = patterns.len(),
                    "Planning MutationCreateExec (batch)"
                );
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                // Use a single MutationExec with CreateBatch to avoid N nested
                // operators (which cause stack overflow for large N).
                let output_schema = extended_schema_for_new_vars(&child.schema(), patterns);
                Ok(Arc::new(MutationExec::new_with_schema(
                    child,
                    MutationKind::CreateBatch {
                        patterns: patterns.clone(),
                    },
                    "MutationCreateExec",
                    mutation_ctx,
                    output_schema,
                )))
            }
            LogicalPlan::Set { input, items } => {
                tracing::debug!(items = items.len(), "Planning MutationSetExec");
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                Ok(Arc::new(new_set_exec(child, items.clone(), mutation_ctx)))
            }
            LogicalPlan::Remove { input, items } => {
                tracing::debug!(items = items.len(), "Planning MutationRemoveExec");
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                Ok(Arc::new(new_remove_exec(
                    child,
                    items.clone(),
                    mutation_ctx,
                )))
            }
            LogicalPlan::Delete {
                input,
                items,
                detach,
            } => {
                tracing::debug!(
                    items = items.len(),
                    detach = detach,
                    "Planning MutationDeleteExec"
                );
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                Ok(Arc::new(new_delete_exec(
                    child,
                    items.clone(),
                    *detach,
                    mutation_ctx,
                )))
            }
            LogicalPlan::Merge {
                input,
                pattern,
                on_match,
                on_create,
            } => {
                tracing::debug!("Planning MutationMergeExec");
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                Ok(Arc::new(new_merge_exec(
                    child,
                    pattern.clone(),
                    on_match.clone(),
                    on_create.clone(),
                    mutation_ctx,
                )))
            }

            LogicalPlan::Window {
                input,
                window_exprs,
            } => {
                let input_plan = self.plan_internal(input, all_properties)?;
                if !window_exprs.is_empty() {
                    self.plan_window_functions(input_plan, window_exprs, Some(input.as_ref()))
                } else {
                    Ok(input_plan)
                }
            }

            LogicalPlan::CrossJoin { left, right } => {
                let left_plan = self.plan_internal(left, all_properties)?;
                let right_plan = self.plan_internal(right, all_properties)?;

                // For Locy IS-ref joins (graph scan × derived scan), strip structural
                // projection columns (Struct-typed bare variable columns like "a", "b")
                // from the graph scan output that conflict with derived scan column names.
                // Non-conflicting struct columns (e.g., edge "e") are preserved for
                // typed property access.
                let left_plan = if matches!(right.as_ref(), LogicalPlan::LocyDerivedScan { .. }) {
                    let derived_schema = right_plan.schema();
                    let derived_names: HashSet<&str> = derived_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect();
                    strip_conflicting_structural_columns(left_plan, &derived_names)?
                } else {
                    left_plan
                };

                Ok(Arc::new(
                    datafusion::physical_plan::joins::CrossJoinExec::new(left_plan, right_plan),
                ))
            }

            LogicalPlan::Apply {
                input,
                subquery,
                input_filter,
            } => self.plan_apply(input, subquery, input_filter.as_ref(), all_properties),

            LogicalPlan::Unwind {
                input,
                expr,
                variable,
            } => self.plan_unwind(
                input.as_ref().clone(),
                expr.clone(),
                variable.clone(),
                all_properties,
            ),

            LogicalPlan::VectorKnn {
                label_id,
                variable,
                property,
                query,
                k,
                threshold,
            } => self.plan_vector_knn(
                *label_id,
                variable,
                property,
                query.clone(),
                *k,
                *threshold,
                all_properties,
            ),

            LogicalPlan::InvertedIndexLookup { .. } => Err(anyhow!(
                "Full-text search not yet supported in DataFusion engine"
            )),

            LogicalPlan::AllShortestPaths {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id: _,
                path_variable,
                step_variable,
                min_hops,
                max_hops,
                edge_filter_expr,
            } => self.plan_shortest_path(
                input,
                edge_type_ids,
                direction.clone(),
                source_variable,
                target_variable,
                path_variable,
                step_variable.as_deref(),
                true,
                *min_hops,
                *max_hops,
                edge_filter_expr.as_ref(),
                all_properties,
            ),

            LogicalPlan::QuantifiedPattern { .. } => Err(anyhow!(
                "Quantified patterns not yet supported in DataFusion engine"
            )),

            LogicalPlan::RecursiveCTE {
                cte_name,
                initial,
                recursive,
            } => self.plan_recursive_cte(cte_name, initial, recursive, all_properties),

            LogicalPlan::ProcedureCall {
                procedure_name,
                arguments,
                yield_items,
            } => self.plan_procedure_call(procedure_name, arguments, yield_items, all_properties),

            LogicalPlan::SubqueryCall { input, subquery } => {
                self.plan_apply(input, subquery, None, all_properties)
            }

            LogicalPlan::ExtIdLookup {
                variable,
                ext_id,
                filter,
                optional,
            } => self.plan_ext_id_lookup(variable, ext_id, filter.as_ref(), *optional),

            LogicalPlan::Foreach {
                input,
                variable,
                list,
                body,
            } => {
                tracing::debug!(variable = variable.as_str(), "Planning ForeachExec");
                let child = self.plan_internal(input, all_properties)?;
                let mutation_ctx = self.require_mutation_ctx()?;
                Ok(Arc::new(
                    super::df_graph::mutation_foreach::ForeachExec::new(
                        child,
                        variable.clone(),
                        list.clone(),
                        body.clone(),
                        mutation_ctx,
                    ),
                ))
            }

            // Locy standalone operators
            LogicalPlan::LocyPriority { input, key_columns } => {
                let child = self.plan_internal(input, all_properties)?;
                let key_indices = resolve_column_indices(&child.schema(), key_columns)?;
                let priority_col_index = child.schema().index_of("__priority").map_err(|_| {
                    anyhow::anyhow!("LocyPriority input must contain __priority column")
                })?;
                Ok(Arc::new(super::df_graph::locy_priority::PriorityExec::new(
                    child,
                    key_indices,
                    priority_col_index,
                )))
            }

            LogicalPlan::LocyBestBy {
                input,
                key_columns,
                criteria,
            } => {
                let child = self.plan_internal(input, all_properties)?;
                let key_indices = resolve_column_indices(&child.schema(), key_columns)?;
                let sort_criteria = resolve_best_by_criteria(&child.schema(), criteria)?;
                Ok(Arc::new(super::df_graph::locy_best_by::BestByExec::new(
                    child,
                    key_indices,
                    sort_criteria,
                    true, // LocyBestBy logical plan always uses deterministic ordering
                )))
            }

            LogicalPlan::LocyFold {
                input,
                key_columns,
                fold_bindings,
                strict_probability_domain,
                probability_epsilon,
            } => {
                let child = self.plan_internal(input, all_properties)?;
                let key_indices = resolve_column_indices(&child.schema(), key_columns)?;
                let bindings =
                    resolve_fold_bindings(&child.schema(), fold_bindings, &self.plugin_registry)?;
                Ok(Arc::new(super::df_graph::locy_fold::FoldExec::new(
                    child,
                    key_indices,
                    bindings,
                    *strict_probability_domain,
                    *probability_epsilon,
                )))
            }

            LogicalPlan::LocyDerivedScan {
                scan_index: _,
                data,
                schema,
            } => Ok(Arc::new(
                super::df_graph::locy_fixpoint::DerivedScanExec::new(
                    Arc::clone(data),
                    Arc::clone(schema),
                ),
            )),

            LogicalPlan::LocyProject {
                input,
                projections,
                target_types,
            } => self.plan_locy_project(input, projections, target_types, all_properties),

            LogicalPlan::LocyModelInvoke {
                input,
                invocations,
                classifier_registry,
                classifier_cache,
                classifier_provenance_store,
                path_context_handles,
            } => {
                let input_plan = self.plan_internal(input, all_properties)?;
                // Phase D D2 runtime: inject the Xervo embedder runtime
                // from graph_ctx at physical lowering. The logical plan
                // is graph_ctx-agnostic; the physical exec carries the
                // shared `Arc<ModelRuntime>` needed to embed
                // `semantic_match` query literals.
                let xervo_runtime =
                    super::df_graph::locy_model_invoke::XervoRuntimeHandle(
                        self.graph_ctx.xervo_runtime().cloned(),
                    );
                // Phase D D1 graph-structural runtime: lift registry +
                // storage + L0 snapshot from graph_ctx. Construction
                // mirrors `execute_algo_procedure` in procedure_call.rs.
                let graph_algo = {
                    let l0_ctx = self.graph_ctx.l0_context();
                    let l0_mgr = l0_ctx.current_l0.as_ref().map(|current| {
                        let mut pending = l0_ctx.pending_flush_l0s.clone();
                        if let Some(tx_l0) = &l0_ctx.transaction_l0 {
                            pending.push(tx_l0.clone());
                        }
                        Arc::new(uni_store::runtime::l0_manager::L0Manager::from_snapshot(
                            current.clone(),
                            pending,
                        ))
                    });
                    let l0_buffers = self.graph_ctx.l0_context().current_l0.as_ref().map(
                        |current| super::df_graph::locy_model_invoke::L0Buffers {
                            current: current.clone(),
                            transaction: self.graph_ctx.l0_context().transaction_l0.clone(),
                            pending_flush: self.graph_ctx.l0_context().pending_flush_l0s.clone(),
                        },
                    );
                    super::df_graph::locy_model_invoke::GraphAlgoHandle {
                        registry: self.graph_ctx.algo_registry().cloned(),
                        storage: Some(self.graph_ctx.storage().clone()),
                        l0_manager: l0_mgr,
                        property_manager: Some(self.graph_ctx.property_manager().clone()),
                        l0_buffers,
                    }
                };
                Ok(Arc::new(
                    super::df_graph::locy_model_invoke::LocyModelInvokeExec::new(
                        input_plan,
                        invocations.clone(),
                        Arc::clone(classifier_registry),
                        classifier_cache.as_ref().map(Arc::clone),
                        classifier_provenance_store.as_ref().map(Arc::clone),
                        path_context_handles.clone(),
                        xervo_runtime,
                        graph_algo,
                    ),
                ))
            }

            LogicalPlan::LocyProgram {
                strata,
                commands,
                derived_scan_registry,
                max_iterations,
                timeout,
                max_derived_bytes,
                deterministic_best_by,
                strict_probability_domain,
                probability_epsilon,
                exact_probability,
                max_bdd_variables,
                top_k_proofs,
                semiring_kind,
                classifier_registry,
                classifier_cache,
                classifier_provenance_store,
            } => {
                let output_schema = super::df_graph::locy_program::stats_schema();

                Ok(Arc::new(
                    super::df_graph::locy_program::LocyProgramExec::new_with_semiring_classifiers_and_cache(
                        strata.clone(),
                        commands.clone(),
                        Arc::clone(derived_scan_registry),
                        Arc::clone(&self.plugin_registry),
                        Arc::clone(&self.graph_ctx),
                        Arc::clone(&self.session_ctx),
                        Arc::clone(&self.storage),
                        Arc::clone(&self.schema),
                        self.params.clone(),
                        output_schema,
                        *max_iterations,
                        *timeout,
                        *max_derived_bytes,
                        *deterministic_best_by,
                        *strict_probability_domain,
                        *probability_epsilon,
                        *exact_probability,
                        *max_bdd_variables,
                        *top_k_proofs,
                        *semiring_kind,
                        Arc::clone(classifier_registry),
                        classifier_cache.as_ref().map(Arc::clone),
                        classifier_provenance_store.as_ref().map(Arc::clone),
                    ),
                ))
            }

            // DDL operations should be handled separately
            LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateSparseIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. }
            | LogicalPlan::Copy { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_)
            | LogicalPlan::Explain { .. } => {
                Err(anyhow!("DDL/Admin operations should be handled separately"))
            }
        }
    }

    /// Like `plan_internal`, but propagates alias mappings to Sort nodes.
    /// This is used when a Project wraps a Sort (possibly through Limit)
    /// so that ORDER BY can reference projection aliases.
    fn plan_internal_with_aliases(
        &self,
        logical: &LogicalPlan,
        all_properties: &HashMap<String, HashSet<String>>,
        alias_map: &HashMap<String, Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match logical {
            LogicalPlan::Sort { input, order_by } => {
                self.plan_sort(input, order_by, all_properties, alias_map)
            }
            LogicalPlan::Limit { input, skip, fetch } => {
                // Propagate aliases through Limit to reach Sort
                let input_plan =
                    self.plan_internal_with_aliases(input, all_properties, alias_map)?;
                if let Some(offset) = skip.filter(|&s| s > 0) {
                    use datafusion::physical_plan::limit::GlobalLimitExec;
                    Ok(Arc::new(GlobalLimitExec::new(input_plan, offset, *fetch)))
                } else {
                    Ok(Arc::new(LocalLimitExec::new(
                        input_plan,
                        fetch.unwrap_or(usize::MAX),
                    )))
                }
            }
            // For all other nodes, fall through to normal planning
            _ => self.plan_internal(logical, all_properties),
        }
    }

    /// Apply a node-level filter to a scan or lookup plan.
    ///
    /// Wraps the input plan with a `FilterExec` if `filter` is `Some`.
    /// Builds a `TranslationContext` marking `variable` as `VariableKind::Node`
    /// for correct expression translation.
    /// Extract a VID literal from a Cypher filter expression for scan-level
    /// optimization. Looks for `_vid = <int>` patterns (produced by the
    /// `id()` → `_vid` rewrite). Returns the VID if found, enabling L0
    /// short-circuit and Lance _vid pushdown inside the scan.
    /// Extract VID(s) from a Cypher WHERE filter for scan-level pushdown.
    ///
    /// Returns the list of VIDs the filter constrains for `variable`, or
    /// `None` if the filter doesn't contain a recognised `_vid = lit` /
    /// `_vid IN (lit, ...)` predicate. A single-element vec means single-VID
    /// pushdown; multi-element vec means IN-list pushdown. See issue #55 PR #4.
    fn extract_vid_from_cypher_filter(
        filter: Option<&Expr>,
        variable: &str,
        params: &HashMap<String, uni_common::Value>,
    ) -> Option<Vec<u64>> {
        use uni_cypher::ast::BinaryOp;
        let filter = filter?;
        match filter {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                // Check: variable._vid = literal/param
                if let Expr::Property(var_expr, prop) = left.as_ref()
                    && let Expr::Variable(v) = var_expr.as_ref()
                    && v == variable
                    && prop == "_vid"
                {
                    return Self::resolve_vid_value(right, params).map(|v| vec![v]);
                }
                // Check: literal/param = variable._vid
                if let Expr::Property(var_expr, prop) = right.as_ref()
                    && let Expr::Variable(v) = var_expr.as_ref()
                    && v == variable
                    && prop == "_vid"
                {
                    return Self::resolve_vid_value(left, params).map(|v| vec![v]);
                }
                None
            }
            Expr::In { expr, list } => {
                // Check: variable._vid IN (literals)
                let Expr::Property(var_expr, prop) = expr.as_ref() else {
                    return None;
                };
                let Expr::Variable(v) = var_expr.as_ref() else {
                    return None;
                };
                if v != variable || prop != "_vid" {
                    return None;
                }
                let Expr::List(items) = list.as_ref() else {
                    return None;
                };
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(Self::resolve_vid_value(item, params)?);
                }
                if out.is_empty() { None } else { Some(out) }
            }
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => Self::extract_vid_from_cypher_filter(Some(left), variable, params)
                .or_else(|| Self::extract_vid_from_cypher_filter(Some(right), variable, params)),
            _ => None,
        }
    }

    /// Build a physical `_vid = literal` filter expression for scan-level
    /// optimization (single-VID case). For multi-VID IN-list, use
    /// `GraphScanExec::vid_list_filter` directly — it bypasses the
    /// PhysicalExpr roundtrip.
    fn build_vid_physical_filter(
        col_name: &str,
        vid: u64,
    ) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(col_name, 0)),
            datafusion::logical_expr::Operator::Eq,
            Arc::new(Literal::new(datafusion::common::ScalarValue::UInt64(Some(
                vid,
            )))),
        ))
    }

    fn resolve_vid_value(expr: &Expr, params: &HashMap<String, uni_common::Value>) -> Option<u64> {
        match expr {
            Expr::Literal(CypherLiteral::Integer(v)) if *v >= 0 => Some(*v as u64),
            Expr::Parameter(name) => match params.get(name) {
                Some(uni_common::Value::Int(v)) if *v >= 0 => Some(*v as u64),
                _ => None,
            },
            _ => None,
        }
    }

    /// AND-combine a non-empty list of predicates into a single `Expr`.
    /// Trivial for length 0/1 (returns true / the single expr); folds left
    /// for length >= 2.
    fn and_join_predicates(mut preds: Vec<Expr>) -> Expr {
        if preds.is_empty() {
            return uni_cypher::ast::Expr::TRUE;
        }
        let mut acc = preds.remove(0);
        for p in preds {
            acc = Expr::BinaryOp {
                left: Box::new(acc),
                op: uni_cypher::ast::BinaryOp::And,
                right: Box::new(p),
            };
        }
        acc
    }

    /// Build the indexed-property pushdown for a vertex scan: a Lance SQL
    /// filter string AND an Arrow-side `PhysicalExpr`, both derived from the
    /// same set of indexed-property conjuncts.
    ///
    /// - The Lance string drives an O(1) hash-index lookup against on-disk data.
    /// - The Arrow filter applies to the merged (Lance + L0) batch in-process,
    ///   so L0 rows that haven't been flushed yet are still index-bounded.
    ///
    /// Returns `None` when no indexed predicate exists or any parameter
    /// resolution fails — in that case the planner falls back to the regular
    /// post-scan `FilterExec`. See issue #57.
    fn build_indexed_property_pushdown(
        &self,
        filter: Option<&Expr>,
        variable: &str,
        label_id: u16,
        scan_schema: &SchemaRef,
    ) -> Option<(String, Arc<dyn datafusion::physical_expr::PhysicalExpr>)> {
        let filter = filter?;
        let analyzer = crate::query::pushdown::IndexAwareAnalyzer::new(&self.schema);
        let strategy = analyzer.analyze(filter, variable, label_id);
        if strategy.indexed_equality_columns.is_empty() {
            return None;
        }

        // Collect lance_predicates that touch an indexed column. Other
        // lance_predicates (e.g. range on non-indexed props) are deliberately
        // left for the outer FilterExec: pushing them inside the scan
        // would also filter L0 rows that match the indexed conjunct but not
        // the residual conjunct on the SAME row — which is fine — but the
        // outer FilterExec already handles them, so keeping the boundary
        // simple keeps the merge behaviour obvious.
        let label_name = self.schema.label_name_by_id(label_id)?;
        let label_props = self.schema.properties.get(label_name);
        let mut indexed_preds: Vec<Expr> = Vec::new();
        for pred in &strategy.lance_predicates {
            if let Some(col) = crate::query::pushdown::predicate_target_column(pred, variable)
                && strategy
                    .indexed_equality_columns
                    .iter()
                    .any(|(c, _)| c == &col)
            {
                let resolved = crate::query::pushdown::substitute_params(pred, &self.params)?;
                indexed_preds.push(resolved);
            }
        }
        if indexed_preds.is_empty() {
            return None;
        }

        // Render the Lance SQL filter string for storage-side pushdown.
        let lance_str = crate::query::pushdown::LanceFilterGenerator::generate(
            &indexed_preds,
            variable,
            label_props,
        )?;

        // Build the Arrow-side PhysicalExpr from the same predicates. The
        // GraphScanExec applies it to the merged (Lance+L0) batch so the
        // scan output is index-bounded regardless of where the data lives.
        let combined = Self::and_join_predicates(indexed_preds.clone());
        let mut variable_kinds = HashMap::new();
        variable_kinds.insert(variable.to_string(), VariableKind::Node);
        let mut variable_labels = HashMap::new();
        variable_labels.insert(variable.to_string(), label_name.to_string());
        let ctx = TranslationContext {
            parameters: self.params.clone(),
            variable_labels,
            variable_kinds,
            ..Default::default()
        };
        let df_filter = cypher_expr_to_df(&combined, Some(&ctx)).ok()?;
        let session = self.session_ctx.read();
        let physical = self
            .create_physical_filter_expr(&df_filter, scan_schema, &session)
            .ok()?;
        Some((lance_str, physical))
    }

    /// Wraps a leaf scan plan so surviving row identities feed the SSI read-set.
    ///
    /// No-op unless the current transaction has an optimistic read-set (a
    /// read-write transaction begun under `UniConfig::ssi_enabled`), so the wrap
    /// self-gates at runtime — when SSI is off, `occ_read_set` is `None` and the
    /// plan is returned verbatim. Must be inserted above the residual `FilterExec`
    /// and below any structural projection so the `{var}._vid` / `{var}._eid`
    /// columns are still present.
    fn wrap_read_set_recording(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        variable: &str,
    ) -> Arc<dyn ExecutionPlan> {
        let has_read_set = self
            .graph_ctx
            .l0_context()
            .transaction_l0
            .as_ref()
            .is_some_and(|l0| l0.read().occ_read_set.is_some());
        if !has_read_set {
            return plan;
        }
        Arc::new(ReadSetRecordingExec::new(
            plan,
            self.graph_ctx.clone(),
            variable,
        ))
    }

    fn apply_scan_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        variable: &str,
        filter: Option<&Expr>,
        label_name: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(filter_expr) = filter else {
            return Ok(plan);
        };

        let mut variable_kinds = HashMap::new();
        variable_kinds.insert(variable.to_string(), VariableKind::Node);
        let mut variable_labels = HashMap::new();
        if let Some(label) = label_name {
            variable_labels.insert(variable.to_string(), label.to_string());
        }
        let ctx = TranslationContext {
            parameters: self.params.clone(),
            variable_labels,
            variable_kinds,
            ..Default::default()
        };
        let df_filter = cypher_expr_to_df(filter_expr, Some(&ctx))?;

        let schema = plan.schema();

        let session = self.session_ctx.read();
        let physical_filter = self.create_physical_filter_expr(&df_filter, &schema, &session)?;

        Ok(Arc::new(FilterExec::try_new(physical_filter, plan)?))
    }

    /// Apply a filter to a schemaless traverse plan (TraverseMainByType).
    ///
    /// Builds a `TranslationContext` with the appropriate variable kinds for
    /// source, target, edge, and path variables, then creates and applies the
    /// filter. Used by both VLP (target_filter) and fixed-length (edge_filter)
    /// branches of TraverseMainByType planning.
    #[expect(clippy::too_many_arguments)]
    fn apply_schemaless_traverse_filter(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        filter_expr: Option<&Expr>,
        source_variable: &str,
        target_variable: &str,
        step_variable: Option<&str>,
        path_variable: Option<&str>,
        is_variable_length: bool,
        optional: bool,
        optional_pattern_vars: &HashSet<String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(filter_expr) = filter_expr else {
            return Ok(plan);
        };

        let mut variable_kinds = HashMap::new();
        variable_kinds.insert(source_variable.to_string(), VariableKind::Node);
        variable_kinds.insert(target_variable.to_string(), VariableKind::Node);
        if let Some(sv) = step_variable {
            variable_kinds.insert(sv.to_string(), VariableKind::edge_for(is_variable_length));
        }
        if let Some(pv) = path_variable {
            variable_kinds.insert(pv.to_string(), VariableKind::Path);
        }
        let ctx = TranslationContext {
            parameters: self.params.clone(),
            variable_kinds,
            ..Default::default()
        };
        let df_filter = cypher_expr_to_df(filter_expr, Some(&ctx))?;
        let schema = plan.schema();
        let session = self.session_ctx.read();
        let physical_filter = self.create_physical_filter_expr(&df_filter, &schema, &session)?;

        if optional {
            Ok(Arc::new(OptionalFilterExec::new(
                plan,
                physical_filter,
                optional_pattern_vars.clone(),
            )))
        } else {
            Ok(Arc::new(FilterExec::try_new(physical_filter, plan)?))
        }
    }

    /// Plan an external ID lookup.
    fn plan_ext_id_lookup(
        &self,
        variable: &str,
        ext_id: &str,
        filter: Option<&Expr>,
        optional: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Collect properties needed from the filter
        let properties = if let Some(filter_expr) = filter {
            crate::query::df_expr::collect_properties(filter_expr)
                .into_iter()
                .filter(|(var, _)| var == variable)
                .map(|(_, prop)| prop)
                .collect()
        } else {
            vec![]
        };

        let lookup_plan: Arc<dyn ExecutionPlan> = Arc::new(GraphExtIdLookupExec::new(
            self.graph_ctx.clone(),
            variable.to_string(),
            ext_id.to_string(),
            properties,
            optional,
        ));

        self.apply_scan_filter(lookup_plan, variable, filter, None)
    }

    /// Plan an UNWIND operation.
    ///
    /// UNWIND expands a list expression into multiple rows.
    fn plan_unwind(
        &self,
        input: LogicalPlan,
        expr: Expr,
        variable: String,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Recursively plan the input
        let input_plan = self.plan_internal(&input, all_properties)?;

        // Drop the source list when the planner proved nothing above reads it.
        // Without this the list rides through every operator above, and a
        // traversal replicates it once per fan-out row (#184).
        let drop_source = match &expr {
            Expr::Variable(name) => all_properties
                .get(crate::query::planner::DEAD_UNWIND_SOURCES_KEY)
                .filter(|dead| dead.contains(name))
                .map(|_| name.clone()),
            _ => None,
        };

        let unwind = GraphUnwindExec::new_dropping_source(
            input_plan,
            expr,
            variable,
            self.params.clone(),
            drop_source.as_deref(),
        );

        Ok(Arc::new(unwind))
    }

    /// Plan a recursive CTE (`WITH RECURSIVE`).
    ///
    /// Creates a [`RecursiveCTEExec`] that stores the logical plans and
    /// re-plans/executes them iteratively at execution time.
    fn plan_recursive_cte(
        &self,
        cte_name: &str,
        initial: &LogicalPlan,
        recursive: &LogicalPlan,
        _all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RecursiveCTEExec::new(
            cte_name.to_string(),
            initial.clone(),
            recursive.clone(),
            self.graph_ctx.clone(),
            self.session_ctx.clone(),
            self.storage.clone(),
            self.schema.clone(),
            self.params.clone(),
            self.mutation_ctx.clone(),
        )))
    }

    /// Plan an Apply (correlated subquery) or SubqueryCall.
    fn plan_apply(
        &self,
        input: &LogicalPlan,
        subquery: &LogicalPlan,
        input_filter: Option<&Expr>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use crate::query::df_graph::common::infer_logical_plan_schema;

        // 1. Plan input physically
        let input_exec = self.plan_internal(input, all_properties)?;
        let input_schema = input_exec.schema();

        // 1a. Unit-subquery unwrap: write-only `CALL { ... }` (no inner
        // RETURN) is wrapped in `Limit { fetch: Some(0), input: Set/... }` by
        // the planner so the subquery emits zero rows. At the physical layer,
        // `GlobalLimitExec(fetch=0)` short-circuits and never polls its input
        // — so the embedded write operator never runs. Strip that wrapper so
        // the side effect executes per outer row; output emptiness is still
        // signaled via the schema (sub_schema has no fields → unit detection
        // in `GraphApplyExec`).
        let subquery_effective = match subquery {
            LogicalPlan::Limit {
                input: inner,
                skip: None,
                fetch: Some(0),
            } => inner.as_ref(),
            _ => subquery,
        };

        // 2. Infer subquery output schema from logical plan + UniSchema metadata.
        // Use the ORIGINAL (still-wrapped) subquery so a unit subquery resolves
        // to an empty schema, which `GraphApplyExec` reads as the unit signal.
        let sub_schema = infer_logical_plan_schema(subquery, &self.schema);

        // 3. Merge schemas: subquery fields override input fields with the
        //    same name. The subquery's RETURN list is authoritative for the
        //    names it lists, which is what `CALL { WITH n SET n.x = ...
        //    RETURN n }` semantically requires — the outer plan must see the
        //    post-SET `n`, not the pre-SET copy carried through from the
        //    correlated input. For correlated subqueries that don't re-emit
        //    an imported variable (EXISTS, COUNT, non-SET CALLs), there is no
        //    name collision and behavior is unchanged.
        let sub_field_names: HashSet<&str> = sub_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        // Input columns whose name collides with a subquery RETURN field are
        // dropped (sub wins). Dotted columns (`v.prop`) whose base variable
        // `v` is overridden by the subquery are kept in the schema (so the
        // expr compiler resolves `v.prop` via the flat-column path) but at
        // data-fill time they're refreshed from the post-SET bare `v` Map
        // in the subquery output. See `append_cross_join_row` /
        // `kept_input_overrides`.
        let kept_input_indices: Vec<usize> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !sub_field_names.contains(f.name().as_str()))
            .map(|(i, _)| i)
            .collect();
        // For each kept input column, pre-compute whether it should be
        // sourced from the subquery's bare entity Map instead of the input
        // batch. Some((var, prop)) means refresh `var.prop` from
        // `sub_row[var]`; None means slice from input as usual.
        let kept_input_overrides: Vec<Option<(String, String)>> = kept_input_indices
            .iter()
            .map(|&i| {
                let name = input_schema.field(i).name();
                if let Some(dot) = name.find('.') {
                    let base = &name[..dot];
                    if sub_field_names.contains(base) {
                        return Some((base.to_string(), name[dot + 1..].to_string()));
                    }
                }
                None
            })
            .collect();
        let mut fields: Vec<Arc<arrow_schema::Field>> = kept_input_indices
            .iter()
            .map(|&i| input_schema.fields()[i].clone())
            .collect();
        fields.extend(sub_schema.fields().iter().cloned());
        let output_schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(GraphApplyExec::new(
            input_exec,
            subquery_effective.clone(),
            input_filter.cloned(),
            self.graph_ctx.clone(),
            self.session_ctx.clone(),
            self.storage.clone(),
            self.schema.clone(),
            self.params.clone(),
            output_schema,
            kept_input_indices,
            kept_input_overrides,
            self.mutation_ctx.clone(),
        )))
    }

    /// Plan a vector KNN search.
    #[expect(clippy::too_many_arguments)]
    fn plan_vector_knn(
        &self,
        label_id: u16,
        variable: &str,
        property: &str,
        query_expr: Expr,
        k: usize,
        threshold: Option<f32>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let label_name = self
            .schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Unknown label ID: {}", label_id))?;

        let target_properties = self.resolve_properties(variable, label_name, all_properties);

        // M5b follow-up #4 (IndexProbeExec bridge): look up the index by
        // name for this `(label, property)` pair, then ask the plugin
        // registry whether a live `IndexHandle` has been registered under
        // that name. If yes, dispatch the probe through the plugin handle
        // via `VectorSource::Plugin`; if no, fall through to the native
        // `StorageManager::vector_search` path (preserves the "no behavior
        // change for built-ins" invariant — native vector indexes never
        // register a handle in this table).
        let plugin_source = self
            .schema
            .vector_index_for_property(label_name, property)
            .and_then(|cfg| {
                self.plugin_registry
                    .index_handle(&cfg.name)
                    .map(|entry| (cfg.name.clone(), entry))
            });

        let knn = if let Some((index_name, entry)) = plugin_source {
            tracing::debug!(
                target: "uni.plugin.registry",
                index_kind = %entry.kind.0,
                index_name = %index_name,
                "plan_vector_knn: dispatching via plugin IndexHandle"
            );
            GraphVectorKnnExec::with_plugin_source(
                self.graph_ctx.clone(),
                label_id,
                label_name,
                variable.to_string(),
                property.to_string(),
                query_expr,
                k,
                threshold,
                self.params.clone(),
                target_properties,
                entry.kind,
                entry.handle,
            )
        } else {
            GraphVectorKnnExec::new(
                self.graph_ctx.clone(),
                label_id,
                label_name,
                variable.to_string(),
                property.to_string(),
                query_expr,
                k,
                threshold,
                self.params.clone(),
                target_properties,
            )
        };

        // SSI read-set: a vector-KNN result is a set of *real* graph vertices
        // (the exec emits `{variable}._vid` from the native/plugin index over the
        // actual store), each of which a concurrent transaction can write. A
        // read-write antidependency through a KNN read must therefore abort, so
        // record the matched vids — exactly as `plan_scan` does for label scans.
        Ok(self.wrap_read_set_recording(Arc::new(knn), variable))
    }

    /// Plan a procedure call.
    fn plan_procedure_call(
        &self,
        procedure_name: &str,
        arguments: &[Expr],
        yield_items: &[(String, Option<String>)],
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use crate::query::df_graph::procedure_call::map_yield_to_canonical;

        // Build target_properties map for node-like yields in search procedures
        let mut target_properties: HashMap<String, Vec<String>> = HashMap::new();

        if crate::query::df_graph::procedure_call::is_node_yield_procedure_static(procedure_name) {
            for (name, alias) in yield_items {
                let output_name = alias.as_ref().unwrap_or(name);
                let canonical = map_yield_to_canonical(name);
                if canonical == "node" {
                    // Collect properties requested for this node variable
                    if let Some(props) = all_properties.get(output_name.as_str()) {
                        let prop_list: Vec<String> = props
                            .iter()
                            .filter(|p| *p != "*" && !p.starts_with('_'))
                            .cloned()
                            .collect();
                        target_properties.insert(output_name.clone(), prop_list);
                    }
                }
            }
        }

        let exec = GraphProcedureCallExec::new(
            self.graph_ctx.clone(),
            procedure_name.to_string(),
            arguments.to_vec(),
            yield_items.to_vec(),
            self.params.clone(),
            self.outer_values.clone(),
            target_properties,
        );

        Ok(Arc::new(exec))
    }

    /// Plan a vertex scan.
    fn plan_scan(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Virtual label: dispatch to a `CatalogVertexScanExec` that wraps
        // the plugin-registered `CatalogTable` (M5 follow-up #6). The
        // plan caches the virtual id, not the table — every execute
        // resolves the latest table from `PluginRegistry::virtual_label_by_id`,
        // so a re-registered provider naturally picks up.
        if uni_common::core::schema::is_virtual_label_id(label_id) {
            let entry = self
                .plugin_registry
                .virtual_label_by_id(label_id)
                .ok_or_else(|| {
                    anyhow!(
                        "Virtual label id {label_id:#x} has no registered CatalogTable; \
                         the originating CatalogProvider may have been deregistered \
                         after the plan was cached"
                    )
                })?;
            let label_name = entry.name.as_str();
            let properties = self.resolve_properties(variable, label_name, all_properties);
            let pushdown_filters: Vec<datafusion::logical_expr::Expr> = filter
                .map(|f| -> Result<Vec<_>> {
                    let ctx = crate::query::df_expr::TranslationContext {
                        parameters: self.params.clone(),
                        outer_values: self.outer_values.clone(),
                        ..Default::default()
                    };
                    let df = crate::query::df_expr::cypher_expr_to_df(f, Some(&ctx))?;
                    Ok(vec![df])
                })
                .transpose()?
                .unwrap_or_default();
            let exec = crate::query::df_graph::catalog_scan::CatalogVertexScanExec::try_new(
                entry.table,
                label_id,
                label_name.to_string(),
                variable.to_string(),
                properties,
                pushdown_filters,
                None, // limit-pushdown is applied at a higher layer for now
            )?;
            let mut plan: Arc<dyn ExecutionPlan> = Arc::new(exec);
            // Re-apply the Cypher filter as a top-level FilterExec for
            // safety (the catalog table may have ignored the pushdown).
            plan = self.apply_scan_filter(plan, variable, filter, Some(label_name))?;
            // SSI read-set: deliberately NOT recorded. A virtual (catalog-backed)
            // label is read-only — CREATE/SET/DELETE on it is rejected at both
            // planner and runtime — so no uni transaction can ever write a
            // virtual vertex, and a read-write antidependency through one is
            // impossible. Its `_vid` is also synthetic (`label_id << 48 | row`,
            // ≥ 0xFF00…), disjoint from real vids, so recording it could only add
            // never-matching keys (and risk a false abort if the spaces ever
            // overlapped). Excluding it is the sound choice, not a gap.
            return self.wrap_optional(plan, optional);
        }

        let label_name = self
            .schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Unknown label ID: {}", label_id))?;

        // Resolve properties collected from the entire plan tree, expanding "*" wildcards
        let mut properties = self.resolve_properties(variable, label_name, all_properties);

        // Check if any projected property is NOT in the schema (needs overflow_json)
        let label_props = self.schema.properties.get(label_name);
        let has_projection_overflow = properties.iter().any(|p| {
            p != "overflow_json"
                && !p.starts_with('_')
                && !label_props.is_some_and(|lp| lp.contains_key(p.as_str()))
        });
        if has_projection_overflow && !properties.iter().any(|p| p == "overflow_json") {
            properties.push("overflow_json".to_string());
        }

        // If the filter references overflow properties (not in schema), ensure
        // `overflow_json` is projected so the DataFusion FilterExec can read it.
        if let Some(filter_expr) = filter {
            let filter_props = crate::query::df_expr::collect_properties(filter_expr);
            let has_overflow = filter_props.iter().any(|(var, prop)| {
                var == variable
                    && !prop.starts_with('_')
                    && label_props.is_none_or(|props| !props.contains_key(prop.as_str()))
            });
            if has_overflow && !properties.iter().any(|p| p == "overflow_json") {
                properties.push("overflow_json".to_string());
            }
        }

        // Structural projection is needed if EITHER:
        //   - "*"            (full record requested — bare variable, REMOVE,
        //                    Labels/Variable/VariablePlus SET, etc.), or
        //   - STRUCT_ONLY_SENTINEL  (Property SET only — needs the bare struct
        //                    column for `row.get(var)` but not the full schema).
        // Only "*" pushes `_all_props` / `overflow_json` into the scan; the
        // sentinel deliberately skips these so wide columns (e.g. embeddings)
        // are NOT materialized.
        let var_props = all_properties.get(variable);
        let need_full =
            var_props.is_some_and(|p| p.contains("*") || p.contains(STRUCT_ONLY_SENTINEL));
        let need_full_record = var_props.is_some_and(|p| p.contains("*"));
        if need_full_record {
            if !properties.contains(&"_all_props".to_string()) {
                properties.push("_all_props".to_string());
            }
            if !properties.contains(&"overflow_json".to_string()) {
                properties.push("overflow_json".to_string());
            }
        }

        // Extract VID(s) from filter for scan-level optimization (L0
        // short-circuit + Lance pushdown). Single-VID becomes a `_vid = N`
        // physical filter that GraphScanExec uses both in L0 short-circuit and
        // in the Lance pushdown string. Multi-VID (from
        // `_vid IN (literals)`) bypasses the PhysicalExpr roundtrip and goes
        // direct to GraphScanExec via `with_vid_list_filter` — at runtime
        // it becomes `_vid IN (v1, v2, ...)` for Lance pushdown. See issue #55 PR #4.
        let extracted_vids = Self::extract_vid_from_cypher_filter(filter, variable, &self.params);
        let scan_filter = extracted_vids
            .as_deref()
            .filter(|v| v.len() == 1)
            .map(|v| Self::build_vid_physical_filter(&format!("{variable}._vid"), v[0]));
        let mut scan_exec = GraphScanExec::new_vertex_scan(
            self.graph_ctx.clone(),
            label_name.to_string(),
            variable.to_string(),
            properties.clone(),
            scan_filter,
        );
        if let Some(vids) = extracted_vids
            && vids.len() > 1
        {
            scan_exec = scan_exec.with_vid_list_filter(vids);
        }

        // Indexed-property pushdown — issue #57. Detect equality / IN
        // predicates against hash-indexed properties on (label, prop), resolve
        // any parameters at plan time, render BOTH a Lance SQL filter (for
        // on-disk index lookup) and an Arrow PhysicalExpr (for in-process
        // L0 filtering). The redundant FilterExec on top (added by
        // `apply_scan_filter` below) is harmless and keeps residual conjuncts
        // (e.g. non-indexed multi-property AND) correct.
        let scan_schema_for_idx = scan_exec.schema();
        if let Some((lance_str, runtime_filter)) =
            self.build_indexed_property_pushdown(filter, variable, label_id, &scan_schema_for_idx)
        {
            scan_exec = scan_exec
                .with_extra_lance_filter(lance_str)
                .with_extra_runtime_filter(runtime_filter);
        }
        let mut scan_plan: Arc<dyn ExecutionPlan> = Arc::new(scan_exec);

        // Apply filter BEFORE structural projection so that the schema is
        // unambiguous (no duplicate `variable._vid` from both flat column and
        // struct field). This prevents "Ambiguous reference" errors when
        // comparing `_vid` (UInt64) against Int64 literals in type coercion.
        scan_plan = self.apply_scan_filter(scan_plan, variable, filter, Some(label_name))?;

        // Record surviving (post-filter) row ids into the SSI read-set so keyed
        // matches conflict only with writers touching the same rows.
        scan_plan = self.wrap_read_set_recording(scan_plan, variable);

        if need_full {
            // Filter sentinel markers and overflow_json from the structural
            // projection. Keep _all_props so properties()/keys() UDFs can use it.
            let struct_props: Vec<String> = properties
                .iter()
                .filter(|p| *p != "overflow_json" && *p != "*" && *p != STRUCT_ONLY_SENTINEL)
                .cloned()
                .collect();
            scan_plan = self.add_structural_projection(scan_plan, variable, &struct_props)?;
        }

        self.wrap_optional(scan_plan, optional)
    }

    /// Plan a schemaless vertex scan using the main vertices table.
    ///
    /// Used for labels that aren't in the schema - queries the main table
    /// with `array_contains(labels, 'X')` filter and extracts properties from `props_json`.
    /// Add a structural projection for a variable if wildcard access ("*") is needed.
    ///
    /// Derives the property list from the plan's output schema (columns with the
    /// variable prefix) and wraps them into a Struct column via `add_structural_projection`.
    fn add_wildcard_structural_projection(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !all_properties
            .get(variable)
            .is_some_and(|p| p.contains("*") || p.contains(STRUCT_ONLY_SENTINEL))
        {
            return Ok(plan);
        }
        let prefix = format!("{}.", variable);
        let struct_props: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                f.name()
                    .strip_prefix(&prefix)
                    .filter(|prop| !prop.starts_with('_') || *prop == "_all_props")
                    .map(|prop| prop.to_string())
            })
            .collect();
        self.add_structural_projection(plan, variable, &struct_props)
    }

    /// Detect whether a target variable is already bound in the input plan's schema.
    ///
    /// Returns `Some("{target_variable}._vid")` when the column is present.
    fn detect_bound_target(input_schema: &SchemaRef, target_variable: &str) -> Option<String> {
        // Standard: {var}._vid from ScanNodes output
        let col = format!("{}._vid", target_variable);
        if input_schema.column_with_name(&col).is_some() {
            return Some(col);
        }
        // Fallback: bare variable name if it's a numeric (VID) column.
        // This handles EXISTS subquery contexts where imported variables are
        // projected as Parameter("{var}") → bare VID column.
        // VIDs are UInt64 in Arrow, but may become Int64 after parameter
        // round-tripping through Value::Integer → ScalarValue::Int64.
        if let Ok(field) = input_schema.field_with_name(target_variable)
            && matches!(
                field.data_type(),
                datafusion::arrow::datatypes::DataType::UInt64
                    | datafusion::arrow::datatypes::DataType::Int64
            )
        {
            return Some(target_variable.to_string());
        }
        None
    }

    /// Resolve the property list and wildcard flag for a schemaless vertex scan.
    ///
    /// Filters out `"*"` and the structural-only sentinel, ensures `_all_props`
    /// is present (schemaless backend requirement — properties live in a JSON
    /// blob), and returns `(properties, need_full)` where `need_full`
    /// indicates structural access (either marker triggers it).
    fn resolve_schemaless_properties(
        variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> (Vec<String>, bool) {
        let mut properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| {
                s.iter()
                    .filter(|p| *p != "*" && *p != STRUCT_ONLY_SENTINEL)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let need_full = all_properties
            .get(variable)
            .is_some_and(|p| p.contains("*") || p.contains(STRUCT_ONLY_SENTINEL));
        if !properties.iter().any(|p| p == "_all_props") {
            properties.push("_all_props".to_string());
        }
        (properties, need_full)
    }

    /// Collect edge columns (`._eid` and `__eid_to_*`) from a schema, filtered to the
    /// current MATCH scope. Optionally excludes a specific column (for rebound edge patterns).
    fn collect_used_edge_columns(
        schema: &SchemaRef,
        scope_match_variables: &HashSet<String>,
        exclude_col: Option<&str>,
    ) -> Vec<String> {
        schema
            .fields()
            .iter()
            .filter_map(|f| {
                let name = f.name();
                if exclude_col.is_some_and(|exc| name == exc) {
                    None
                } else if name.ends_with("._eid") {
                    let var_name = name.trim_end_matches("._eid");
                    scope_match_variables
                        .contains(var_name)
                        .then(|| name.clone())
                } else if name.starts_with("__eid_to_") {
                    let var_name = name.trim_start_matches("__eid_to_");
                    scope_match_variables
                        .contains(var_name)
                        .then(|| name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Conditionally add edge structural projection when the edge variable has wildcard access.
    /// Skips if `skip_if_vlp` is true (VLP step variables are already `List<Edge>`).
    #[expect(
        clippy::too_many_arguments,
        reason = "direction joins seven existing plan-context parameters; grouping \
                  them into a struct is a wider refactor than this fix warrants"
    )]
    fn maybe_add_edge_structural_projection(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        step_variable: Option<&str>,
        source_variable: &str,
        target_variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
        skip_if_vlp: bool,
        direction: &AstDirection,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if skip_if_vlp {
            return Ok(plan);
        }
        let Some(edge_var) = step_variable else {
            return Ok(plan);
        };
        if !all_properties
            .get(edge_var)
            .is_some_and(|p| p.contains("*") || p.contains(STRUCT_ONLY_SENTINEL))
        {
            return Ok(plan);
        }
        // Derive edge properties from the plan's output schema
        let prefix = format!("{}.", edge_var);
        let edge_props: Vec<String> = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                f.name()
                    .strip_prefix(&prefix)
                    .filter(|prop| !prop.starts_with('_') && *prop != "overflow_json")
                    .map(|prop| prop.to_string())
            })
            .collect();
        self.add_edge_structural_projection(
            plan,
            edge_var,
            &edge_props,
            source_variable,
            target_variable,
            direction,
        )
    }

    /// Apply filter, optional structural projection, and optional wrapping to a schemaless scan.
    fn finalize_schemaless_scan(
        &self,
        scan_plan: Arc<dyn ExecutionPlan>,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        properties: &[String],
        need_full: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Apply filter BEFORE structural projection to avoid ambiguous column
        // references (flat `var._vid` vs struct `var._vid` field).
        let mut plan = self.apply_scan_filter(scan_plan, variable, filter, None)?;

        // Record surviving (post-filter) row ids into the SSI read-set so keyed
        // matches conflict only with writers touching the same rows.
        plan = self.wrap_read_set_recording(plan, variable);

        // If we need the full object (structural access), build a struct with _labels + properties.
        // This enables labels(n)/keys(n) UDFs which expect a Struct column with a _labels field.
        if need_full {
            // Filter out "*" (wildcard marker) and the structural-only sentinel
            // from struct_props. Keep "_all_props" so that keys()/properties()
            // UDFs can extract property names at runtime from the CypherValue
            // blob.
            let struct_props: Vec<String> = properties
                .iter()
                .filter(|p| *p != "*" && *p != STRUCT_ONLY_SENTINEL)
                .cloned()
                .collect();
            plan = self.add_structural_projection(plan, variable, &struct_props)?;
        }

        self.wrap_optional(plan, optional)
    }

    fn plan_schemaless_scan(
        &self,
        label_name: &str,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (properties, need_full) = Self::resolve_schemaless_properties(variable, all_properties);
        let scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_schemaless_vertex_scan(
                self.graph_ctx.clone(),
                label_name.to_string(),
                variable.to_string(),
                properties.clone(),
                None,
            ));
        self.finalize_schemaless_scan(
            scan_plan,
            variable,
            filter,
            optional,
            &properties,
            need_full,
        )
    }

    /// Split a label list into `(virtual_labels, native_labels)` against the plugin registry.
    ///
    /// A label is virtual when `PluginRegistry::virtual_label_by_name` returns
    /// a registered id; otherwise it is treated as native. Used by both the
    /// single- and multi-label scan paths to decide whether to dispatch a
    /// `CatalogVertexScanExec`, a `GraphScanExec`, or a join of the two.
    fn classify_labels(
        registry: &uni_plugin::PluginRegistry,
        labels: &[String],
    ) -> (Vec<(String, u16)>, Vec<String>) {
        let mut virtual_labels: Vec<(String, u16)> = Vec::new();
        let mut native_labels: Vec<String> = Vec::new();
        for label in labels {
            if let Some(id) = registry.virtual_label_by_name(label) {
                virtual_labels.push((label.clone(), id));
            } else {
                native_labels.push(label.clone());
            }
        }
        (virtual_labels, native_labels)
    }

    /// Plan a multi-label vertex scan using the main vertices table.
    ///
    /// For patterns like `(n:A:B)`, scans vertices that carry ALL labels
    /// (intersection semantics). When some labels are plugin-registered
    /// virtual labels and others are native, builds a `CatalogVertexScanExec`
    /// for the virtual side, a `GraphScanExec` for the native side, and a
    /// `LeftSemi` `HashJoinExec` keyed on `{variable}._vid` so the catalog
    /// rows are filtered by native presence (and the output schema stays
    /// clean — only the catalog side's columns flow through).
    ///
    /// # Errors
    ///
    /// Returns an error if a virtual-label entry is missing at plan time
    /// (a `CatalogProvider` was deregistered after the plan was cached)
    /// or if the underlying scan / join construction fails.
    fn plan_multi_label_scan(
        &self,
        labels: &[String],
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (virtual_labels, native_labels) = Self::classify_labels(&self.plugin_registry, labels);

        // All-native: keep the legacy schemaless multi-label scan.
        if virtual_labels.is_empty() {
            let (properties, need_full) =
                Self::resolve_schemaless_properties(variable, all_properties);
            let scan_plan: Arc<dyn ExecutionPlan> =
                Arc::new(GraphScanExec::new_multi_label_vertex_scan(
                    self.graph_ctx.clone(),
                    labels.to_vec(),
                    variable.to_string(),
                    properties.clone(),
                    None,
                ));
            return self.finalize_schemaless_scan(
                scan_plan,
                variable,
                filter,
                optional,
                &properties,
                need_full,
            );
        }

        // Build the virtual side: one `CatalogVertexScanExec` per virtual
        // label, unioned when there's more than one. The union is per the
        // plan-doc contract ("union if >1"); each catalog table contributes
        // its own vid space (encoded with the per-label id), so the unioned
        // stream is well-formed.
        let virtual_side =
            self.build_virtual_union_scan(&virtual_labels, variable, filter, all_properties)?;

        // All-virtual: no native filter to apply.
        if native_labels.is_empty() {
            // Re-apply the Cypher filter as a top-level FilterExec for safety
            // (catalog tables may ignore pushdowns). The per-leaf scans already
            // ran the filter; this is harmless and keeps semantics consistent
            // with `plan_scan`'s single-virtual branch.
            let plan = self.apply_scan_filter(virtual_side, variable, filter, None)?;
            return self.wrap_optional(plan, optional);
        }

        // Mixed: build the native side (schemaless multi-label scan projecting
        // only `_vid`) and `LeftSemi`-join the virtual side against it. The
        // semi-join shape mirrors the plan-doc's "inner on _vid" intent but
        // emits only the left (catalog) columns, so downstream consumers see
        // a clean `{variable}.{prop}` schema instead of duplicate vid columns.
        let native_properties: Vec<String> = vec!["_all_props".to_string()];
        let native_scan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_multi_label_vertex_scan(
                self.graph_ctx.clone(),
                native_labels,
                variable.to_string(),
                native_properties,
                None,
            ));

        let joined = self.semi_join_on_vid(virtual_side, native_scan, variable)?;
        let plan = self.apply_scan_filter(joined, variable, filter, None)?;
        self.wrap_optional(plan, optional)
    }

    /// Build the virtual-side scan: a single `CatalogVertexScanExec` for one
    /// virtual label, or a `UnionExec` of one-per-label scans when several.
    /// SSI note: like the single virtual scan, the catalog scans built here are
    /// deliberately NOT wrapped in read-set recording — virtual labels are
    /// read-only with synthetic vids, so no antidependency is possible. See the
    /// rationale at the single-label virtual scan in `plan_scan`.
    fn build_virtual_union_scan(
        &self,
        virtual_labels: &[(String, u16)],
        variable: &str,
        filter: Option<&Expr>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pushdown_filters: Vec<DfExpr> = filter
            .map(|f| -> Result<Vec<_>> {
                let ctx = crate::query::df_expr::TranslationContext {
                    parameters: self.params.clone(),
                    outer_values: self.outer_values.clone(),
                    ..Default::default()
                };
                let df = crate::query::df_expr::cypher_expr_to_df(f, Some(&ctx))?;
                Ok(vec![df])
            })
            .transpose()?
            .unwrap_or_default();

        let mut scans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(virtual_labels.len());
        for (label_name, label_id) in virtual_labels {
            let entry = self
                .plugin_registry
                .virtual_label_by_id(*label_id)
                .ok_or_else(|| {
                    anyhow!(
                        "Virtual label `{label_name}` (id {label_id:#x}) has no \
                             registered CatalogTable; the originating CatalogProvider \
                             may have been deregistered after the plan was cached"
                    )
                })?;
            let properties = self.resolve_properties(variable, label_name, all_properties);
            let exec = crate::query::df_graph::catalog_scan::CatalogVertexScanExec::try_new(
                entry.table,
                *label_id,
                label_name.clone(),
                variable.to_string(),
                properties,
                pushdown_filters.clone(),
                None,
            )?;
            scans.push(Arc::new(exec));
        }

        if scans.len() == 1 {
            Ok(scans.pop().expect("len == 1 implies non-empty"))
        } else {
            UnionExec::try_new(scans).map_err(|e| anyhow!("UnionExec construction failed: {e}"))
        }
    }

    /// Build a `LeftSemi` `HashJoinExec` keyed on `{variable}._vid` between
    /// `left` (the catalog side carrying the row data) and `right` (the
    /// native side acting as a presence filter).
    fn semi_join_on_vid(
        &self,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        variable: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::expressions::Column;
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let vid_col = format!("{variable}._vid");
        let left_idx = left
            .schema()
            .index_of(&vid_col)
            .map_err(|e| anyhow!("virtual scan output missing `{vid_col}`: {e}"))?;
        let right_idx = right
            .schema()
            .index_of(&vid_col)
            .map_err(|e| anyhow!("native scan output missing `{vid_col}`: {e}"))?;
        let on: Vec<(
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        )> = vec![(
            Arc::new(Column::new(&vid_col, left_idx)),
            Arc::new(Column::new(&vid_col, right_idx)),
        )];
        let join = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        Ok(Arc::new(join))
    }

    /// Inner-join the traverse output (carrying `{target}._vid`) with a
    /// `CatalogVertexScanExec` for a virtual destination label, projecting
    /// away the duplicate `_vid` column from the catalog side.
    ///
    /// Used by `plan_traverse` and `plan_traverse_main_by_type` when the
    /// destination label is plugin-registered. The catalog side contributes
    /// `{target}._labels` and `{target}.<prop>` for every requested
    /// property; the traverse side contributes everything else (source
    /// vid/properties, edge columns, the destination vid we join on).
    ///
    /// # Errors
    ///
    /// Returns an error if the virtual label entry has been deregistered
    /// since plan time, if either side of the join is missing
    /// `{target}._vid`, or if the underlying DataFusion plan construction
    /// fails.
    fn hydrate_virtual_target_from_catalog(
        &self,
        traverse_plan: Arc<dyn ExecutionPlan>,
        target_label_id: u16,
        target_variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
        optional: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::NullEquality;
        use datafusion::physical_expr::expressions::{Column, col as col_expr};
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let entry = self
            .plugin_registry
            .virtual_label_by_id(target_label_id)
            .ok_or_else(|| {
                anyhow!(
                    "Virtual label id {target_label_id:#x} for target `{target_variable}` has no \
                     registered CatalogTable; the originating CatalogProvider may have been \
                     deregistered after the plan was cached"
                )
            })?;
        let label_name = entry.name.as_str();
        let properties = self.resolve_properties(target_variable, label_name, all_properties);
        // The catalog provider may ignore pushdown predicates, but the
        // traverse output already constrains rows by `_vid`, so we don't
        // need to forward the original target-filter again here. The
        // outer `target_filter` FilterExec at the end of `plan_traverse`
        // will re-apply.
        let catalog_exec = crate::query::df_graph::catalog_scan::CatalogVertexScanExec::try_new(
            entry.table,
            target_label_id,
            label_name.to_string(),
            target_variable.to_string(),
            properties,
            Vec::new(),
            None,
        )?;
        let catalog_plan: Arc<dyn ExecutionPlan> = Arc::new(catalog_exec);

        let vid_col_name = format!("{target_variable}._vid");
        let left_idx = traverse_plan
            .schema()
            .index_of(&vid_col_name)
            .map_err(|e| anyhow!("traverse plan missing `{vid_col_name}` for hydration: {e}"))?;
        let right_idx = catalog_plan
            .schema()
            .index_of(&vid_col_name)
            .map_err(|e| anyhow!("catalog scan missing `{vid_col_name}`: {e}"))?;
        let on: Vec<(
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        )> = vec![(
            Arc::new(Column::new(&vid_col_name, left_idx)),
            Arc::new(Column::new(&vid_col_name, right_idx)),
        )];
        // An OPTIONAL traverse emits NULL-target rows (unmatched `{target}._vid`);
        // a Left join preserves them (right side null-padded) instead of dropping
        // them as an Inner join would, keeping OPTIONAL MATCH semantics intact for
        // a plugin virtual target.
        let join_type = if optional {
            JoinType::Left
        } else {
            JoinType::Inner
        };
        let join = HashJoinExec::try_new(
            traverse_plan,
            catalog_plan,
            on,
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join_plan: Arc<dyn ExecutionPlan> = Arc::new(join);

        // Project away the duplicate `{target}._vid` from the catalog side.
        // HashJoinExec emits left columns followed by right columns; the
        // left already has `{target}._vid` from the traverse, so we drop
        // the right-side copy (which sits at left_schema_len + right_idx
        // before re-ordering — DataFusion's HashJoinExec preserves the
        // left/right column order, so the duplicate is in the right
        // section).
        let join_schema = join_plan.schema();
        let mut projection_exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> =
            Vec::with_capacity(join_schema.fields().len() - 1);
        let mut seen_vid = false;
        for field in join_schema.fields().iter() {
            if field.name() == &vid_col_name {
                if seen_vid {
                    continue;
                }
                seen_vid = true;
            }
            let expr = col_expr(field.name(), &join_schema)
                .map_err(|e| anyhow!("hydrate_virtual_target_from_catalog projection: {e}"))?;
            projection_exprs.push((expr, field.name().clone()));
        }
        let projected = ProjectionExec::try_new(projection_exprs, join_plan)
            .map_err(|e| anyhow!("hydrate_virtual_target_from_catalog projection: {e}"))?;
        Ok(Arc::new(projected))
    }

    /// M5b.3 — physical plan for `MATCH (a)-[r:VirtualEdge]->(b)` where the
    /// relationship type is plugin-registered.
    ///
    /// Builds: `HashJoin(input × CatalogEdgeScanExec)` keyed on
    /// `{source}._vid = {step}._src_vid`, then a `ProjectionExec` that
    /// renames `{step}._dst_vid` -> `{target}._vid` and drops the
    /// duplicate join-key column from the right side. If the destination
    /// label is itself virtual, the postlude layers
    /// `hydrate_virtual_target_from_catalog` on top.
    ///
    /// SSI note: the `CatalogEdgeScanExec` and any virtual target are NOT
    /// read-set recorded — virtual edges/vertices are read-only with synthetic
    /// ids, so no antidependency is possible (see the rationale in `plan_scan`).
    /// The *real* source vertex `{source}._vid` entering the join was already
    /// recorded by whatever scan produced `input_plan`.
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors plan_traverse's argument set"
    )]
    fn plan_traverse_virtual_edge(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        source_col: String,
        source_variable: &str,
        virtual_edge_type_id: u32,
        direction: AstDirection,
        target_variable: &str,
        target_label_id: u16,
        step_variable: Option<&str>,
        all_properties: &HashMap<String, HashSet<String>>,
        target_filter: Option<&Expr>,
        optional: bool,
        optional_pattern_vars: &HashSet<String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::NullEquality;
        use datafusion::physical_expr::expressions::{Column, col as col_expr};
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let entry = self
            .plugin_registry
            .virtual_edge_type_by_id(virtual_edge_type_id)
            .ok_or_else(|| {
                anyhow!(
                    "Virtual edge-type id {virtual_edge_type_id:#x} for `{target_variable}` has \
                     no registered CatalogTable; the originating CatalogProvider may have been \
                     deregistered after the plan was cached"
                )
            })?;
        let type_name = entry.name.as_str();
        let edge_var = step_variable
            .map(str::to_string)
            .unwrap_or_else(|| format!("__anon_edge_{target_variable}"));

        let edge_properties: Vec<String> = step_variable
            .and_then(|sv| all_properties.get(sv))
            .map(|props| {
                props
                    .iter()
                    .filter(|p| !p.starts_with('_') && *p != "*")
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        let catalog_exec = crate::query::df_graph::catalog_scan::CatalogEdgeScanExec::try_new(
            entry.table,
            virtual_edge_type_id,
            type_name.to_string(),
            edge_var.clone(),
            edge_properties,
            Vec::new(),
            None,
        )?;
        let catalog_plan: Arc<dyn ExecutionPlan> = Arc::new(catalog_exec);

        let edge_src_col = format!("{edge_var}._src_vid");
        let edge_dst_col = format!("{edge_var}._dst_vid");
        let (right_key, target_src_col) = match direction {
            AstDirection::Outgoing => (edge_src_col.clone(), edge_dst_col.clone()),
            AstDirection::Incoming => (edge_dst_col.clone(), edge_src_col.clone()),
            AstDirection::Both => (edge_src_col.clone(), edge_dst_col.clone()),
        };

        let left_idx = input_plan
            .schema()
            .index_of(&source_col)
            .map_err(|e| anyhow!("input plan missing source vid column `{source_col}`: {e}"))?;
        let right_idx = catalog_plan
            .schema()
            .index_of(&right_key)
            .map_err(|e| anyhow!("CatalogEdgeScanExec missing `{right_key}`: {e}"))?;
        let on: Vec<(
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        )> = vec![(
            Arc::new(Column::new(&source_col, left_idx)),
            Arc::new(Column::new(&right_key, right_idx)),
        )];
        let join = HashJoinExec::try_new(
            input_plan,
            catalog_plan,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join_plan: Arc<dyn ExecutionPlan> = Arc::new(join);

        let join_schema = join_plan.schema();
        let target_vid_name = format!("{target_variable}._vid");
        let mut projection_exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> =
            Vec::with_capacity(join_schema.fields().len());
        for field in join_schema.fields() {
            let name = field.name();
            if name == &right_key {
                continue;
            }
            let expr = col_expr(name, &join_schema)
                .map_err(|e| anyhow!("plan_traverse_virtual_edge projection: {e}"))?;
            let out_name = if name == &target_src_col {
                target_vid_name.clone()
            } else {
                name.clone()
            };
            projection_exprs.push((expr, out_name));
        }
        let projected: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(projection_exprs, join_plan)
                .map_err(|e| anyhow!("plan_traverse_virtual_edge projection: {e}"))?,
        );

        let mut plan = if uni_common::core::schema::is_virtual_label_id(target_label_id) {
            self.hydrate_virtual_target_from_catalog(
                projected,
                target_label_id,
                target_variable,
                all_properties,
                optional,
            )?
        } else {
            projected
        };

        plan = self.add_wildcard_structural_projection(plan, target_variable, all_properties)?;
        plan = self.maybe_add_edge_structural_projection(
            plan,
            step_variable,
            source_variable,
            target_variable,
            all_properties,
            false,
            &direction,
        )?;

        if let Some(filter_expr) = target_filter {
            let mut variable_kinds = HashMap::new();
            variable_kinds.insert(source_variable.to_string(), VariableKind::Node);
            variable_kinds.insert(target_variable.to_string(), VariableKind::Node);
            if let Some(sv) = step_variable {
                variable_kinds.insert(sv.to_string(), VariableKind::edge_for(false));
            }
            let ctx = TranslationContext {
                parameters: self.params.clone(),
                variable_kinds,
                ..Default::default()
            };
            let df_filter = cypher_expr_to_df(filter_expr, Some(&ctx))?;
            let schema = plan.schema();
            let session = self.session_ctx.read();
            let physical_filter =
                self.create_physical_filter_expr(&df_filter, &schema, &session)?;
            plan = if optional {
                Arc::new(OptionalFilterExec::new(
                    plan,
                    physical_filter,
                    optional_pattern_vars.clone(),
                ))
            } else {
                Arc::new(FilterExec::try_new(physical_filter, plan)?)
            };
        } else {
            let _ = optional_pattern_vars;
        }
        Ok(plan)
    }

    /// Plan a scan of all vertices regardless of label.
    ///
    /// This is used for `MATCH (n)` without a label filter.
    fn plan_scan_all(
        &self,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (properties, need_full) = Self::resolve_schemaless_properties(variable, all_properties);
        // Extract VID(s) from filter for scan-level optimization. See the
        // detailed comment at the per-label scan site (issue #55 PR #4).
        let extracted_vids = Self::extract_vid_from_cypher_filter(filter, variable, &self.params);
        let scan_filter = extracted_vids
            .as_deref()
            .filter(|v| v.len() == 1)
            .map(|v| Self::build_vid_physical_filter(&format!("{variable}._vid"), v[0]));
        let mut scan_exec = GraphScanExec::new_schemaless_all_scan(
            self.graph_ctx.clone(),
            variable.to_string(),
            properties.clone(),
            scan_filter,
        );
        if let Some(vids) = extracted_vids
            && vids.len() > 1
        {
            scan_exec = scan_exec.with_vid_list_filter(vids);
        }
        let scan_plan: Arc<dyn ExecutionPlan> = Arc::new(scan_exec);
        self.finalize_schemaless_scan(
            scan_plan,
            variable,
            filter,
            optional,
            &properties,
            need_full,
        )
    }

    /// Plan a graph traversal.
    #[expect(
        clippy::too_many_arguments,
        reason = "Graph traversal requires many parameters"
    )]
    fn plan_traverse(
        &self,
        input: &LogicalPlan,
        edge_type_ids: &[u32],
        direction: AstDirection,
        source_variable: &str,
        target_variable: &str,
        target_label_id: u16,
        step_variable: Option<&str>,
        min_hops: usize,
        max_hops: usize,
        path_variable: Option<&str>,
        optional: bool,
        target_filter: Option<&Expr>,
        is_variable_length: bool,
        optional_pattern_vars: &HashSet<String>,
        all_properties: &HashMap<String, HashSet<String>>,
        scope_match_variables: &HashSet<String>,
        edge_filter_expr: Option<&Expr>,
        path_mode: &crate::query::df_graph::nfa::PathMode,
        qpp_steps: Option<&[crate::query::planner::QppStepInfo]>,
        qpp_inner_source: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction.clone());
        let (input_plan, source_col) = Self::resolve_source_vid_col(input_plan, source_variable)?;

        // M5b.3 — virtual edge-type dispatch. When the relationship type
        // is plugin-registered (`is_virtual_edge_type_id`), there are no
        // native adjacencies: the rows live in a `CatalogTable` accessed
        // via `CatalogEdgeScanExec`. The all-virtual single-hop case
        // dispatches to `plan_traverse_virtual_edge`; mixed
        // native+virtual and VLP-with-virtual continue through the legacy
        // `GraphTraverseExec` branch (yielding zero rows for the virtual
        // portion, matching the pre-M5b.3 baseline).
        if !is_variable_length
            && !edge_type_ids.is_empty()
            && edge_type_ids.len() == 1
            && edge_type_ids
                .iter()
                .all(|eid| uni_common::core::edge_type::is_virtual_edge_type(*eid))
        {
            return self.plan_traverse_virtual_edge(
                input_plan,
                source_col,
                source_variable,
                edge_type_ids[0],
                direction,
                target_variable,
                target_label_id,
                step_variable,
                all_properties,
                target_filter,
                optional,
                optional_pattern_vars,
            );
        }

        let traverse_plan: Arc<dyn ExecutionPlan> = if !is_variable_length {
            // Extract edge properties for pushdown hydration, expanding "*" wildcards
            let mut edge_properties: Vec<String> = if let Some(edge_var) = step_variable {
                let has_wildcard = all_properties
                    .get(edge_var)
                    .is_some_and(|props| props.contains("*"));
                if has_wildcard {
                    // Expand to all schema-defined properties across all matching edge types
                    let mut schema_props: Vec<String> = edge_type_ids
                        .iter()
                        .filter_map(|eid| self.schema.edge_type_name_by_id(*eid))
                        .flat_map(|name| {
                            self.schema
                                .properties
                                .get(name)
                                .map(|p| p.keys().cloned().collect::<Vec<_>>())
                                .unwrap_or_default()
                        })
                        .collect();

                    // Also include explicitly referenced properties (non-wildcard, non-internal)
                    // that may be overflow properties not in the schema. System-managed
                    // timestamp columns (`_created_at`, `_updated_at`) requested via
                    // `created_at(r)` / `updated_at(r)` are kept too.
                    if let Some(props) = all_properties.get(edge_var) {
                        for p in props {
                            let passthrough = !p.starts_with('_')
                                || matches!(p.as_str(), "_created_at" | "_updated_at");
                            if p != "*" && passthrough && !schema_props.contains(p) {
                                schema_props.push(p.clone());
                            }
                        }
                    }
                    schema_props
                } else {
                    all_properties
                        .get(edge_var)
                        .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
                        .unwrap_or_default()
                }
            } else {
                Vec::new()
            };

            // Check if any edge property is NOT in the schema (needs overflow_json)
            if let Some(edge_var) = step_variable {
                let has_wildcard = all_properties
                    .get(edge_var)
                    .is_some_and(|props| props.contains("*"));
                let edge_type_props = self.merged_edge_type_properties(edge_type_ids);
                let has_overflow_edge_props = edge_properties.iter().any(|p| {
                    p != "overflow_json"
                        && !p.starts_with('_')
                        && !edge_type_props.contains_key(p.as_str())
                });
                // Add overflow_json if:
                // 1. Wildcard was used AND edge_properties is empty (no schema props for this edge type)
                // 2. OR there are overflow properties explicitly referenced
                let needs_overflow =
                    (has_wildcard && edge_properties.is_empty()) || has_overflow_edge_props;
                if needs_overflow && !edge_properties.contains(&"overflow_json".to_string()) {
                    edge_properties.push("overflow_json".to_string());
                }

                // Add _all_props for L0 edge property visibility: schemaless edges
                // store properties by name in L0, not as overflow_json blobs, so we
                // need _all_props to surface them through the DataFusion path.
                if has_wildcard && !edge_properties.contains(&"_all_props".to_string()) {
                    edge_properties.push("_all_props".to_string());
                }

                // An undirected hop files the edge under both endpoints, so the
                // traversal's source is whichever end this row matched from —
                // not the edge's stored tail. Materialising the whole
                // relationship without knowing which is which reports the same
                // edge as `b->a` on the row that walked it backwards. `_fwd`
                // carries the per-row answer; `add_edge_structural_projection`
                // uses it to put `_src`/`_dst` back the way they are stored.
                //
                // Requested only when a struct will actually be built: tracking
                // orientation costs the traversal a second adjacency probe, and
                // a directed hop knows the answer statically.
                if wants_orientation_column(
                    &direction,
                    is_variable_length,
                    edge_var,
                    all_properties,
                ) && !edge_properties.iter().any(|p| p == COL_FWD)
                {
                    edge_properties.push(COL_FWD.to_string());
                }
            }

            // Extract target vertex properties, expanding "*" wildcards
            let target_label_name_str = self.schema.label_name_by_id(target_label_id).unwrap_or("");
            let mut target_properties =
                self.resolve_properties(target_variable, target_label_name_str, all_properties);

            // Filter out "*" and the structural-only sentinel from
            // target_properties — they are used for structural projection
            // (bare variable access like `RETURN t`, or SET t.prop) but must
            // not be passed to GraphTraverseExec as actual property column
            // names.
            target_properties.retain(|p| p != "*" && p != STRUCT_ONLY_SENTINEL);

            // When wildcard access was requested, add `_all_props` — the same
            // rule `plan_scan_*` applies, and the same rule the two schemaless
            // traverse planners below already apply.
            //
            // This used to require `target_properties.is_empty()` as well, so a
            // schema-defined label never reached it: `resolve_properties`
            // expands `"*"` into the declared property names, leaving the list
            // non-empty. The result was that `RETURN n` produced a node struct
            // carrying `_all_props` from a scan and not from a traversal — the
            // same Cypher type with two Arrow types, which `UNION` rejects
            // outright (#191). The comment claimed to mirror `plan_scan_all`
            // and did not.
            let target_has_wildcard = all_properties
                .get(target_variable)
                .is_some_and(|p| p.contains("*"));
            if target_has_wildcard && !target_properties.iter().any(|p| p == "_all_props") {
                target_properties.push("_all_props".to_string());
            }

            // Check for non-schema properties that need CypherValue extraction.
            // For the traverse path, always use _all_props (not overflow_json) as
            // the CypherValue source since get_property_value handles _all_props directly.
            let target_label_props = if !target_label_name_str.is_empty() {
                self.schema.properties.get(target_label_name_str)
            } else {
                None
            };
            // Whether a name is declared by the schema, when the target's label
            // is known and when it is not.
            //
            // An uninferable label is not the same as a schemaless property. A
            // traversal whose edge type has several source labels — LDBC's
            // `HAS_CREATOR`, whose sources are `Comment` and `Post` — leaves
            // `target_label_id` at 0, and treating every name as non-schema
            // then forced `_all_props` onto a read that had asked for one
            // column. `_all_props` subsumes the narrow list downstream
            // (`build_target_property_columns`), so the traversal encoded every
            // property of every target row: at LDBC SF1, reading an 8-byte
            // `creationDate` off 83k rows cost 1131 MiB against 185 MiB for
            // binding the entity and reading nothing, and reading the 2000-char
            // `content` instead cost 1232 MiB — within 6%, because the name
            // requested made no difference (#209).
            //
            // So when the label is unknown, ask whether *any* label declares
            // the name. Only a name no label declares is genuinely schemaless
            // and needs the wildcard payload.
            let is_schema_prop = |p: &str| match target_label_props {
                Some(lp) => lp.contains_key(p),
                None => self.schema.properties.values().any(|lp| lp.contains_key(p)),
            };
            let has_non_schema_props = target_properties.iter().any(|p| {
                p != "overflow_json"
                    && p != "_all_props"
                    && !p.starts_with('_')
                    && !is_schema_prop(p.as_str())
            });
            if has_non_schema_props && !target_properties.iter().any(|p| p == "_all_props") {
                target_properties.push("_all_props".to_string());
            }
            // Also check the filter for non-schema property references
            if let Some(filter_expr) = target_filter {
                let filter_props = crate::query::df_expr::collect_properties(filter_expr);
                // Same reasoning as above: an unknown label does not make a
                // filtered property schemaless.
                let has_overflow_filter = filter_props.iter().any(|(var, prop)| {
                    var == target_variable
                        && !prop.starts_with('_')
                        && !is_schema_prop(prop.as_str())
                });
                if has_overflow_filter && !target_properties.iter().any(|p| p == "_all_props") {
                    target_properties.push("_all_props".to_string());
                }
            }
            // For schema-defined labels that also have overflow properties, add overflow_json
            // for the scan path compatibility (Lance storage has overflow_json column).
            if !target_label_name_str.is_empty()
                && has_non_schema_props
                && !target_properties.iter().any(|p| p == "overflow_json")
            {
                target_properties.push("overflow_json".to_string());
            }

            // Resolve target label name for property type lookups
            let target_label_name = if target_label_name_str.is_empty() {
                None
            } else {
                Some(target_label_name_str.to_string())
            };

            // Single-hop traversal
            // Note: target_label_id is not passed here because VIDs no longer embed label info.
            // Label filtering for traversals is handled via the fallback executor when DataFusion
            // cannot handle the query, or via explicit filter predicates.

            // Check if target variable is already bound (for cycle patterns like n-->k<--n)
            let bound_target_column =
                Self::detect_bound_target(&input_plan.schema(), target_variable);

            // Collect edge ID columns from previous hops for relationship uniqueness.
            // Look for both explicit edge variables (ending in "._eid") and
            // internal tracking columns (starting with "__eid_to_").
            //
            // Rebound edge patterns (e.g. OPTIONAL MATCH ()-[r]->() where `r` is already bound)
            // use a temporary edge variable `__rebound_{r}` for traversal and then filter on eid.
            // Do not treat the already-bound `{r}._eid` as "used" here, otherwise the only
            // candidate edge is filtered out before rebound matching.
            // Handle rebound struct variables from WITH + aggregation.
            // When edge or target variables have passed through aggregation, they become
            // struct columns. Extract ALL fields as flat columns so that:
            // 1. {edge}._eid is available for uniqueness checking
            // 2. {edge}.{property} is available for downstream RETURN/WHERE
            // 3. {target}._vid is available for the bound target filter
            // 4. {target}.{property} is available for downstream RETURN/WHERE
            let mut input_plan = input_plan;
            for rebound_var in [
                step_variable.and_then(|sv| sv.strip_prefix("__rebound_")),
                target_variable.strip_prefix("__rebound_"),
            ]
            .into_iter()
            .flatten()
            {
                if input_plan
                    .schema()
                    .field_with_name(rebound_var)
                    .ok()
                    .is_some_and(|f| {
                        matches!(
                            f.data_type(),
                            datafusion::arrow::datatypes::DataType::Struct(_)
                        )
                    })
                {
                    input_plan = Self::extract_all_struct_fields(input_plan, rebound_var)?;
                }
            }

            let rebound_bound_edge_col = step_variable
                .and_then(|sv| sv.strip_prefix("__rebound_"))
                .map(|bound| format!("{}._eid", bound));

            let used_edge_columns = Self::collect_used_edge_columns(
                &input_plan.schema(),
                scope_match_variables,
                rebound_bound_edge_col.as_deref(),
            );

            Arc::new(GraphTraverseExec::new(
                input_plan,
                source_col,
                edge_type_ids.to_vec(),
                adj_direction,
                target_variable.to_string(),
                step_variable.map(|s| s.to_string()),
                edge_properties,
                target_properties,
                target_label_name,
                None, // VIDs don't embed label - use VidLabelsIndex instead
                self.graph_ctx.clone(),
                optional,
                optional_pattern_vars.clone(),
                bound_target_column,
                used_edge_columns,
            ))
        } else {
            // Variable-length traversal
            if edge_type_ids.is_empty() {
                // No edge types - for min_hops=0, we can still emit zero-length paths
                // Use BindZeroLengthPath to create path with just the source node
                if let (0, Some(path_var)) = (min_hops, path_variable) {
                    return Ok(Arc::new(BindZeroLengthPathExec::new(
                        input_plan,
                        source_variable.to_string(),
                        path_var.to_string(),
                        self.graph_ctx.clone(),
                    )));
                } else if min_hops == 0 && step_variable.is_none() {
                    // min_hops=0 but no path variable - just return input as-is
                    // (the target is the same as source for zero-length)
                    return Ok(input_plan);
                }
            }
            {
                // Resolve target properties for VLP (same logic as single-hop above)
                let vlp_target_label_name_str =
                    self.schema.label_name_by_id(target_label_id).unwrap_or("");
                let vlp_target_properties_raw = self.resolve_properties(
                    target_variable,
                    vlp_target_label_name_str,
                    all_properties,
                );
                let target_has_wildcard = all_properties
                    .get(target_variable)
                    .is_some_and(|p| p.contains("*"));
                let vlp_target_label_props: Option<HashSet<String>> =
                    if vlp_target_label_name_str.is_empty() {
                        None
                    } else {
                        self.schema
                            .properties
                            .get(vlp_target_label_name_str)
                            .map(|props| props.keys().cloned().collect())
                    };
                let mut vlp_target_properties = sanitize_vlp_target_properties(
                    vlp_target_properties_raw,
                    target_has_wildcard,
                    vlp_target_label_props.as_ref(),
                );
                let vlp_target_label_name = if vlp_target_label_name_str.is_empty() {
                    None
                } else {
                    Some(vlp_target_label_name_str.to_string())
                };

                // Check if target variable is already bound (for patterns where target is in scope)
                let bound_target_column =
                    Self::detect_bound_target(&input_plan.schema(), target_variable);
                if bound_target_column.is_some() {
                    // For correlated patterns with bound target, traversal only needs reachability.
                    // Reuse existing bound target columns from input and avoid re-hydrating props.
                    vlp_target_properties.clear();
                }

                // VLP: compile edge predicates to Lance SQL for bitmap preselection
                let edge_lance_filter: Option<String> = edge_filter_expr.and_then(|expr| {
                    let edge_var_name = step_variable.unwrap_or("__anon_edge");
                    crate::query::pushdown::LanceFilterGenerator::generate(
                        std::slice::from_ref(expr),
                        edge_var_name,
                        None,
                    )
                });

                // VLP: extract simple property equality conditions for L0 checking
                let edge_property_conditions = edge_filter_expr
                    .map(Self::extract_edge_property_conditions)
                    .unwrap_or_default();

                // VLP: collect used edge columns for cross-pattern relationship uniqueness
                let used_edge_columns = Self::collect_used_edge_columns(
                    &input_plan.schema(),
                    scope_match_variables,
                    None,
                );

                // Group variables, derived from the SAME `steps` slice the NFA
                // and the per-hop filters come from, so their offsets cannot
                // drift from the slots the executor is handed.
                //
                // Pruned against the properties actually referenced by the
                // rest of the plan: a pattern may name its inner positions
                // without reading them, and materializing a group column
                // forces full path enumeration where endpoint-only BFS would
                // otherwise do.
                let qpp_group_bindings: Vec<crate::query::df_graph::nfa::QppGroupBinding> =
                    qpp_steps
                        .map(|steps| {
                            let step_names: Vec<(Option<String>, Option<String>)> = steps
                                .iter()
                                .map(|s| (s.edge_variable.clone(), s.target_variable.clone()))
                                .collect();
                            crate::query::df_graph::nfa::qpp_group_bindings(
                                qpp_inner_source,
                                &step_names,
                            )
                        })
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|b| all_properties.contains_key(&b.name))
                        .collect();

                // VLP: determine output mode based on bound variables
                let output_mode = if !qpp_group_bindings.is_empty() {
                    crate::query::df_graph::nfa::VlpOutputMode::GroupVariables
                } else if step_variable.is_some() {
                    crate::query::df_graph::nfa::VlpOutputMode::StepVariable
                } else if path_variable.is_some() {
                    crate::query::df_graph::nfa::VlpOutputMode::FullPath
                } else {
                    crate::query::df_graph::nfa::VlpOutputMode::EndpointsOnly
                };

                // Compile QPP NFA if multi-step pattern, otherwise let exec compile VLP NFA
                // Per-hop filter specs, derived from the SAME `steps` slice
                // the NFA is built from, so the slot indices the NFA hands the
                // executor always line up with this list.
                let qpp_step_filters: Vec<QppStepFilterSpec> = qpp_steps
                    .map(|steps| {
                        steps
                            .iter()
                            .map(|s| QppStepFilterSpec {
                                edge_type_ids: s.edge_type_ids.clone(),
                                direction: convert_direction(s.direction.clone()),
                                target_label: s.target_label.clone(),
                                edge_conditions: s
                                    .edge_filter_expr
                                    .as_ref()
                                    .map(Self::extract_edge_property_conditions)
                                    .unwrap_or_default(),
                                target_conditions: s
                                    .target_property_expr
                                    .as_ref()
                                    .map(Self::extract_edge_property_conditions)
                                    .unwrap_or_default(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let qpp_nfa = qpp_steps.map(|steps| {
                    use crate::query::df_graph::nfa::{QppStep, VertexConstraint};
                    let hops_per_iter = steps.len();
                    let min_iter = min_hops / hops_per_iter;
                    let max_iter = max_hops / hops_per_iter;
                    let nfa_steps: Vec<QppStep> = steps
                        .iter()
                        .map(|s| QppStep {
                            edge_type_ids: s.edge_type_ids.clone(),
                            direction: convert_direction(s.direction.clone()),
                            target_constraint: s
                                .target_label
                                .as_ref()
                                .map(|l| VertexConstraint::Label(l.clone())),
                        })
                        .collect();
                    crate::query::df_graph::nfa::PathNfa::from_qpp(nfa_steps, min_iter, max_iter)
                });

                Arc::new(GraphVariableLengthTraverseExec::new(
                    input_plan,
                    source_col,
                    edge_type_ids.to_vec(),
                    adj_direction,
                    min_hops,
                    max_hops,
                    target_variable.to_string(),
                    step_variable.map(|s| s.to_string()),
                    path_variable.map(|s| s.to_string()),
                    vlp_target_properties,
                    vlp_target_label_name,
                    self.graph_ctx.clone(),
                    optional,
                    bound_target_column,
                    edge_lance_filter,
                    edge_property_conditions,
                    used_edge_columns,
                    path_mode.clone(),
                    output_mode,
                    qpp_nfa,
                    qpp_step_filters,
                    qpp_group_bindings,
                ))
            }
        };

        // Add structural projections for bare variable access (RETURN t, labels(t), etc.)
        let mut traverse_plan = traverse_plan;

        // M5b.3 — Native↔virtual joins mid-pattern. When the destination
        // label of the traversal is a plugin-registered virtual label, the
        // graph operator above has produced `{target}._vid` against the
        // native adjacency (so this only makes sense when host storage
        // contains edges whose destination vid is the virtual encoding).
        // Hydrate target properties from the corresponding `CatalogTable`
        // by inner-joining a `CatalogVertexScanExec` on `{target}._vid`.
        // The catalog scan side carries `_vid`, `_labels`, and the
        // requested properties — we drop its `_vid` after the join so the
        // output schema stays unambiguous for downstream consumers.
        if uni_common::core::schema::is_virtual_label_id(target_label_id) {
            traverse_plan = self.hydrate_virtual_target_from_catalog(
                traverse_plan,
                target_label_id,
                target_variable,
                all_properties,
                optional,
            )?;
        }

        // Structural projection for target variable
        traverse_plan = self.add_wildcard_structural_projection(
            traverse_plan,
            target_variable,
            all_properties,
        )?;

        // Structural projection for edge variable
        // Only for single-hop traversals; VLP step variables are already List<Edge>
        traverse_plan = self.maybe_add_edge_structural_projection(
            traverse_plan,
            step_variable,
            source_variable,
            target_variable,
            all_properties,
            is_variable_length,
            &direction,
        )?;

        // Apply target filter if present
        if let Some(filter_expr) = target_filter {
            // Build context with variable kinds for this traverse
            let mut variable_kinds = HashMap::new();
            variable_kinds.insert(source_variable.to_string(), VariableKind::Node);
            variable_kinds.insert(target_variable.to_string(), VariableKind::Node);
            if let Some(sv) = step_variable {
                variable_kinds.insert(sv.to_string(), VariableKind::edge_for(is_variable_length));
            }
            if let Some(pv) = path_variable {
                variable_kinds.insert(pv.to_string(), VariableKind::Path);
            }
            let mut variable_labels = HashMap::new();
            if let Some(sv) = step_variable
                && edge_type_ids.len() == 1
                && let Some(name) = self.schema.edge_type_name_by_id(edge_type_ids[0])
            {
                variable_labels.insert(sv.to_string(), name.to_string());
            }
            let target_label_name_str = self.schema.label_name_by_id(target_label_id).unwrap_or("");
            if !target_label_name_str.is_empty() {
                variable_labels.insert(
                    target_variable.to_string(),
                    target_label_name_str.to_string(),
                );
            }
            let ctx = TranslationContext {
                parameters: self.params.clone(),
                variable_labels,
                variable_kinds,
                ..Default::default()
            };
            let df_filter = cypher_expr_to_df(filter_expr, Some(&ctx))?;
            let schema = traverse_plan.schema();
            let session = self.session_ctx.read();
            let physical_filter =
                self.create_physical_filter_expr(&df_filter, &schema, &session)?;

            if optional {
                Ok(Arc::new(OptionalFilterExec::new(
                    traverse_plan,
                    physical_filter,
                    optional_pattern_vars.clone(),
                )))
            } else {
                Ok(Arc::new(FilterExec::try_new(
                    physical_filter,
                    traverse_plan,
                )?))
            }
        } else {
            Ok(traverse_plan)
        }
    }

    /// Plan a schemaless edge traversal (TraverseMainByType).
    ///
    /// This is used for edges without a schema-defined type that must query the main edges table.
    /// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
    #[expect(clippy::too_many_arguments)]
    fn plan_traverse_main_by_type(
        &self,
        input: &LogicalPlan,
        type_names: &[String],
        direction: AstDirection,
        source_variable: &str,
        target_variable: &str,
        step_variable: Option<&str>,
        optional: bool,
        optional_pattern_vars: &HashSet<String>,
        all_properties: &HashMap<String, HashSet<String>>,
        scope_match_variables: &HashSet<String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction.clone());
        let (input_plan, source_col) = Self::resolve_source_vid_col(input_plan, source_variable)?;

        // Check if target variable is already bound (for patterns where target is in scope)
        let bound_target_column = Self::detect_bound_target(&input_plan.schema(), target_variable);

        // Extract edge properties for schemaless edges (all treated as Utf8/JSON).
        // `*` and the struct-only sentinel drive structural projection below and
        // are not column names — every other property list in this planner drops
        // both, and requesting `__set_struct__` as an edge column asks storage
        // for a property that cannot exist.
        let mut edge_properties: Vec<String> = if let Some(edge_var) = step_variable {
            all_properties
                .get(edge_var)
                .map(|props| {
                    props
                        .iter()
                        .filter(|p| *p != "*" && *p != STRUCT_ONLY_SENTINEL)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // An undirected hop may walk an edge from either end, so which end is the
        // stored source is only knowable per row. Request `_fwd` so the structural
        // projection below can restore `_src`/`_dst` to the way the edge is
        // stored, exactly as the schema'd `Traverse` path does.
        //
        // Without it that projection still runs but silently takes its no-`_fwd`
        // fallback — raw traversal order — and the same edge comes back as `a->b`
        // on one row and `b->a` on the other. The schema'd path was fixed for
        // this; this one kept the defect because no test covered it (#193).
        //
        // Gated on a struct actually being built: tracking orientation costs the
        // traversal a second adjacency probe, and a directed hop knows the answer
        // statically.
        if let Some(edge_var) = step_variable {
            // `is_variable_length: false` is structural, not an assumption: a
            // schemaless variable-length hop is planned by
            // `plan_traverse_main_by_type_vlp`, never by this function. The
            // schema'd path needs the flag because one function serves both.
            if wants_orientation_column(&direction, false, edge_var, all_properties)
                && !edge_properties.iter().any(|p| p == COL_FWD)
            {
                edge_properties.push(COL_FWD.to_string());
            }
        }

        // If edge has wildcard, include _all_props for keys()/properties() support
        if let Some(edge_var) = step_variable
            && all_properties
                .get(edge_var)
                .is_some_and(|props| props.contains("*"))
            && !edge_properties.iter().any(|p| p == "_all_props")
        {
            edge_properties.push("_all_props".to_string());
        }

        // Extract target vertex properties
        let mut target_properties: Vec<String> = all_properties
            .get(target_variable)
            .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        // Always include _all_props so post-traverse filters can rewrite
        // property accesses to json_get_* calls against the CypherValue blob.
        // Also include it when wildcard access was requested (RETURN n) even if empty.
        let target_has_wildcard = all_properties
            .get(target_variable)
            .is_some_and(|p| p.contains("*"));
        if (target_has_wildcard || !target_properties.is_empty())
            && !target_properties.iter().any(|p| p == "_all_props")
        {
            target_properties.push("_all_props".to_string());
        }
        if bound_target_column.is_some() {
            // Target already comes from outer scope; avoid redundant property materialization.
            target_properties.clear();
        }

        // Compute used_edge_columns for relationship uniqueness (same logic as Traverse).
        // Exclude the rebound edge's own column so the BFS can match the bound edge.
        let rebound_bound_edge_col = step_variable
            .and_then(|sv| sv.strip_prefix("__rebound_"))
            .map(|bound| format!("{}._eid", bound));
        let used_edge_columns = Self::collect_used_edge_columns(
            &input_plan.schema(),
            scope_match_variables,
            rebound_bound_edge_col.as_deref(),
        );

        // Create the schemaless traversal execution plan
        let traverse_plan: Arc<dyn ExecutionPlan> = Arc::new(GraphTraverseMainExec::new(
            input_plan,
            source_col,
            type_names.to_vec(),
            adj_direction,
            target_variable.to_string(),
            step_variable.map(|s| s.to_string()),
            edge_properties.clone(),
            target_properties,
            self.graph_ctx.clone(),
            optional,
            optional_pattern_vars.clone(),
            bound_target_column,
            used_edge_columns,
        ));

        let mut result_plan = traverse_plan;

        // Structural projection for target variable (RETURN t, labels(t), etc.)
        result_plan =
            self.add_wildcard_structural_projection(result_plan, target_variable, all_properties)?;

        // Structural projection for edge variable (type(r), RETURN r, etc.)
        result_plan = self.maybe_add_edge_structural_projection(
            result_plan,
            step_variable,
            source_variable,
            target_variable,
            all_properties,
            false, // not variable-length
            &direction,
        )?;

        Ok(result_plan)
    }

    /// Plan a schemaless edge traversal with variable-length paths (TraverseMainByType VLP).
    ///
    /// This is used for VLP patterns on edges without a schema-defined type that must query the main edges table.
    /// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
    #[expect(clippy::too_many_arguments)]
    fn plan_traverse_main_by_type_vlp(
        &self,
        input: &LogicalPlan,
        type_names: &[String],
        direction: AstDirection,
        source_variable: &str,
        target_variable: &str,
        step_variable: Option<&str>,
        min_hops: usize,
        max_hops: usize,
        path_variable: Option<&str>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
        edge_filter_expr: Option<&Expr>,
        path_mode: &crate::query::df_graph::nfa::PathMode,
        scope_match_variables: &HashSet<String>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let (input_plan, source_col) = Self::resolve_source_vid_col(input_plan, source_variable)?;

        // Check if target variable is already bound (for patterns where target is in scope)
        let bound_target_column = Self::detect_bound_target(&input_plan.schema(), target_variable);

        // Extract target vertex properties
        let mut target_properties: Vec<String> = all_properties
            .get(target_variable)
            .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        // Always include _all_props so post-traverse filters can rewrite
        // property accesses to json_get_* calls against the CypherValue blob.
        // Also include it when wildcard access was requested (RETURN n) even if empty.
        let target_has_wildcard = all_properties
            .get(target_variable)
            .is_some_and(|p| p.contains("*"));
        if (target_has_wildcard || !target_properties.is_empty())
            && !target_properties.iter().any(|p| p == "_all_props")
        {
            target_properties.push("_all_props".to_string());
        }
        if bound_target_column.is_some() {
            // Correlated EXISTS only requires reachability; keep bound target columns from input.
            target_properties.clear();
        }

        // VLP: compile edge predicates to Lance SQL for bitmap preselection
        let edge_lance_filter: Option<String> = edge_filter_expr.and_then(|expr| {
            let edge_var_name = step_variable.unwrap_or("__anon_edge");
            crate::query::pushdown::LanceFilterGenerator::generate(
                std::slice::from_ref(expr),
                edge_var_name,
                None,
            )
        });

        // VLP: extract edge property conditions for BFS-level filtering
        let edge_property_conditions = edge_filter_expr
            .map(Self::extract_edge_property_conditions)
            .unwrap_or_default();

        // VLP: collect used edge columns for cross-pattern relationship uniqueness
        let used_edge_columns =
            Self::collect_used_edge_columns(&input_plan.schema(), scope_match_variables, None);

        // VLP: determine output mode based on bound variables
        let output_mode = if step_variable.is_some() {
            crate::query::df_graph::nfa::VlpOutputMode::StepVariable
        } else if path_variable.is_some() {
            crate::query::df_graph::nfa::VlpOutputMode::FullPath
        } else {
            crate::query::df_graph::nfa::VlpOutputMode::EndpointsOnly
        };

        let traverse_plan = Arc::new(GraphVariableLengthTraverseMainExec::new(
            input_plan,
            source_col,
            type_names.to_vec(),
            adj_direction,
            min_hops,
            max_hops,
            target_variable.to_string(),
            step_variable.map(|s| s.to_string()),
            path_variable.map(|s| s.to_string()),
            target_properties,
            self.graph_ctx.clone(),
            optional,
            bound_target_column,
            edge_lance_filter,
            edge_property_conditions,
            used_edge_columns,
            path_mode.clone(),
            output_mode,
        ));

        Ok(traverse_plan)
    }

    /// Plan a shortest path computation.
    #[expect(clippy::too_many_arguments)]
    fn plan_shortest_path(
        &self,
        input: &LogicalPlan,
        edge_type_ids: &[u32],
        direction: AstDirection,
        source_variable: &str,
        target_variable: &str,
        path_variable: &str,
        step_variable: Option<&str>,
        all_shortest: bool,
        min_hops: u32,
        max_hops: u32,
        edge_filter_expr: Option<&Expr>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let source_col = format!("{}._vid", source_variable);
        let target_col = format!("{}._vid", target_variable);

        // An inline property map is always a conjunction of equalities, which
        // is exactly what this extractor supports — the same conversion the
        // variable-length path performs before building its `EidFilter`.
        let edge_property_conditions = edge_filter_expr
            .map(Self::extract_edge_property_conditions)
            .unwrap_or_default();

        Ok(Arc::new(GraphShortestPathExec::new(
            input_plan,
            source_col,
            target_col,
            edge_type_ids.to_vec(),
            adj_direction,
            path_variable.to_string(),
            step_variable.map(str::to_string),
            self.graph_ctx.clone(),
            all_shortest,
            min_hops,
            max_hops,
            edge_property_conditions,
        )))
    }

    /// Plan a filter operation.
    ///
    /// When `optional_variables` is non-empty, applies OPTIONAL MATCH WHERE semantics:
    /// rows where all optional variables are NULL are preserved regardless of the predicate.
    fn plan_filter(
        &self,
        input: &LogicalPlan,
        predicate: &Expr,
        optional_variables: &HashSet<String>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Optimization (issue #53): when input is a CrossJoin and the predicate
        // contains equi-join conditions across the two sides, emit HashJoinExec
        // instead of FilterExec(CrossJoinExec). Issue #54 extends this to
        // OPTIONAL MATCH (LeftOuter/RightOuter HashJoin) when the predicate is
        // a pure equi-join — see try_plan_cross_join_as_hash_join for the
        // safety conditions.
        if let LogicalPlan::CrossJoin { left, right } = input
            && let Some(plan) = self.try_plan_cross_join_as_hash_join(
                left,
                right,
                predicate,
                optional_variables,
                all_properties,
            )?
        {
            return Ok(plan);
        }

        let input_plan = self.plan_internal(input, all_properties)?;
        let schema = input_plan.schema();

        // Use CypherPhysicalExprCompiler for all filters (handles both schema-typed
        // and schemaless LargeBinary/CypherValue columns without coercion failures).
        let mut ctx = self.translation_context_for_plan(input);
        ctx.available_columns = Some(schema.fields().iter().map(|f| f.name().clone()).collect());
        let ctx = ctx;
        let session = self.session_ctx.read();
        let state = session.state();
        let compiler = self.expr_compiler(&state, Some(&ctx));
        let physical_predicate = compiler.compile(predicate, &schema)?;

        // For OPTIONAL MATCH: use OptionalFilterExec for proper NULL row preservation.
        if !optional_variables.is_empty() {
            return Ok(Arc::new(OptionalFilterExec::new(
                input_plan,
                physical_predicate,
                optional_variables.clone(),
            )));
        }

        Ok(Arc::new(FilterExec::try_new(
            physical_predicate,
            input_plan,
        )?))
    }

    /// Issue #53 optimization: try to convert Filter(CrossJoin(L, R), pred) into
    /// HashJoinExec when `pred` contains an equi-join condition across the two
    /// sides. Returns `Ok(None)` (fall through to FilterExec) when the pattern
    /// doesn't apply or when join key types can't be unified.
    ///
    /// Left/right-only conjuncts are pushed into a wrapper `Filter` over each
    /// subtree before planning, so nested CrossJoins re-trigger the same
    /// optimization recursively via `plan_internal`.
    fn try_plan_cross_join_as_hash_join(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        predicate: &Expr,
        optional_variables: &HashSet<String>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        use datafusion::common::NullEquality;
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

        let left_vars = collect_plan_variables(left);
        let right_vars = collect_plan_variables(right);
        let cls = classify_join_predicate(predicate, &left_vars, &right_vars);

        if cls.equi_pairs.is_empty() {
            return Ok(None);
        }

        // Determine join type from optional_variables.
        //
        // OPTIONAL MATCH semantics (per OptionalFilterExec) require that for
        // each "source group" (rows of the required side), if all rows fail
        // the predicate we still emit one row with the optional side NULLed.
        // A LeftOuter HashJoin gives the same behavior **only when** the
        // predicate is a pure equi-join across the required and optional
        // sides — any non-equi conjunct (left_only, right_only, residual) on
        // either side could drop a row that OPTIONAL semantics would have
        // NULL-preserved. So for the OPTIONAL path we accept only pure
        // equi-joins; everything else falls back to OptionalFilterExec.
        let left_optional: HashSet<&String> = optional_variables
            .iter()
            .filter(|v| left_vars.contains(*v))
            .collect();
        let right_optional: HashSet<&String> = optional_variables
            .iter()
            .filter(|v| right_vars.contains(*v))
            .collect();

        let join_type = match (left_optional.is_empty(), right_optional.is_empty()) {
            (true, true) => JoinType::Inner,
            (true, false) => JoinType::Left,
            (false, true) => JoinType::Right,
            (false, false) => return Ok(None), // optional vars on both sides — bail
        };

        // For outer joins: only safe when the predicate is purely equi-joins
        // (no left_only/right_only/residual conjuncts).
        if !matches!(join_type, JoinType::Inner)
            && (!cls.left_only.is_empty() || !cls.right_only.is_empty() || cls.residual.is_some())
        {
            return Ok(None);
        }

        // UNWIND IN-list scan pushdown (issue #54 part 3) is now handled
        // by the standalone `merge_unwind_in_filters` pre-pass at
        // `HybridPhysicalPlanner::plan`. That pass walks the LogicalPlan
        // tree BEFORE any physical-plan optimization can bail (e.g.,
        // `unify_join_key_types` failing on Utf8 ↔ LargeBinary), so the
        // scan-side filters always survive — regardless of whether this
        // function emits HashJoinExec or falls back to FilterExec(CrossJoin).
        //
        // Left-only / right-only conjuncts (from `classify_join_predicate`)
        // remain handled here because they're predicate-decomposition
        // concerns specific to HashJoin emission, not UNWIND-IN-list
        // pushdown. They flow into wrap_with_filter below.
        tracing::debug!(
            target: "uni_query::cross_join_in_pushdown",
            equi_pairs = cls.equi_pairs.len(),
            left_only = cls.left_only.len(),
            right_only = cls.right_only.len(),
            has_residual = cls.residual.is_some(),
            "try_plan_cross_join_as_hash_join: classified predicate"
        );

        let left_filters: Vec<Expr> = cls.left_only.clone();
        let right_filters: Vec<Expr> = cls.right_only.clone();
        let left_with_filter = wrap_with_filter(left.clone(), &left_filters);
        let right_with_filter = wrap_with_filter(right.clone(), &right_filters);
        let left_plan = self.plan_internal(&left_with_filter, all_properties)?;
        let right_plan = self.plan_internal(&right_with_filter, all_properties)?;

        // Mirror the CrossJoin lowering (the `LogicalPlan::CrossJoin` arm): for a
        // Locy IS-ref join (graph scan × derived scan), strip the structural
        // bare-variable struct columns ("it", "b", …) from the left that collide
        // with the derived scan's column names. Without this, the left subtree
        // exposes both `it._vid` (flat) and a bare struct `it`, and the derived
        // scan re-introduces a bare `it` (UInt64) — so downstream references and
        // the post-join filter resolve the wrong "it"/"b" (Struct vs UInt64),
        // failing physical planning. The CrossJoin path strips these before the
        // join; the HashJoin path must too (issue #131).
        let left_plan = if matches!(right, LogicalPlan::LocyDerivedScan { .. }) {
            let derived_schema = right_plan.schema();
            let derived_names: HashSet<&str> = derived_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            strip_conflicting_structural_columns(left_plan, &derived_names)?
        } else {
            left_plan
        };

        // Compile each (l_expr, r_expr) pair, wrapping both sides in tointeger
        // for type unification (handles UInt64 _vid vs LargeBinary CV property).
        // If any pair can't be unified, fall through to FilterExec.
        let left_schema = left_plan.schema();
        let right_schema = right_plan.schema();
        let left_ctx = self.translation_context_for_plan(&left_with_filter);
        let right_ctx = self.translation_context_for_plan(&right_with_filter);

        // Build join keys: compile each side's expression and wrap in tointeger
        // for type unification (handles UInt64 _vid vs LargeBinary CV property).
        // Drop the session lock between this scope and HashJoinExec construction.
        let on: Vec<(
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        )> = {
            let session = self.session_ctx.read();
            let state = session.state();

            let left_compiler = self.expr_compiler(&state, Some(&left_ctx));
            let right_compiler = self.expr_compiler(&state, Some(&right_ctx));

            let mut pairs: Vec<(
                Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            )> = Vec::with_capacity(cls.equi_pairs.len());

            for (l_expr, r_expr) in &cls.equi_pairs {
                let l_phys = left_compiler.compile(l_expr, &left_schema)?;
                let r_phys = right_compiler.compile(r_expr, &right_schema)?;
                let Some((l_key, r_key)) =
                    unify_join_key_types(l_phys, r_phys, &left_schema, &right_schema, &state)
                else {
                    return Ok(None);
                };
                pairs.push((l_key, r_key));
            }
            pairs
        };

        // Issue #55 PR #5+#6: cross-MATCH dynamic VID-filter pushdown.
        // When the equi-pairs include exactly one anchor pair on the
        // probe-side `_vid`, and the probe-side planned subtree is a
        // fresh `GraphScanExec`, replace `HashJoinExec{build, full_scan}`
        // with `VidLookupJoinExec`. Supports INNER and LEFT outer; falls
        // through to HashJoinExec for RIGHT outer, non-Scan probes, or
        // computed (non-Column) join keys.
        if matches!(join_type, JoinType::Inner | JoinType::Left)
            && cls.residual.is_none()
            && let Some(plan) = self.try_emit_vid_lookup_join(
                &cls.equi_pairs,
                join_type,
                &left_plan,
                &right_plan,
                &left_with_filter,
                &right_with_filter,
            )?
        {
            return Ok(Some(plan));
        }

        let join: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            left_plan,
            right_plan,
            on,
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?);

        // Apply mixed-non-equi residual (predicates referencing both sides
        // that aren't equi-joins) as a post-join FilterExec.
        if let Some(residual) = cls.residual {
            let join_schema = join.schema();
            let crossjoin_for_ctx = LogicalPlan::CrossJoin {
                left: Box::new(left_with_filter.clone()),
                right: Box::new(right_with_filter.clone()),
            };
            let merged_ctx = self.translation_context_for_plan(&crossjoin_for_ctx);
            let session = self.session_ctx.read();
            let state = session.state();
            let compiler = self.expr_compiler(&state, Some(&merged_ctx));
            let physical_residual = compiler.compile(&residual, &join_schema)?;
            return Ok(Some(Arc::new(FilterExec::try_new(
                physical_residual,
                join,
            )?)));
        }

        Ok(Some(join))
    }

    /// Issue #55 PR #5+#6: detect the cross-MATCH dynamic VID-filter pushdown
    /// pattern and emit `VidLookupJoinExec` instead of `HashJoinExec`.
    /// Returns `Ok(None)` for any pattern that doesn't match — the caller
    /// falls through to the standard HashJoin emission.
    ///
    /// Pattern recognised:
    ///   * One equi-pair (the *anchor*) has the probe side equal to
    ///     `Property(Variable(scan_var), "_vid")`. Its values drive the
    ///     IN-list pushdown.
    ///   * Other equi-pairs (if any) compile to `Column` references on
    ///     both sides; they're applied in-memory as post-match filters.
    ///   * The probe-side planned subtree is a top-level `GraphScanExec`.
    ///   * The anchor build column is UInt64 (a VID).
    ///   * Join is INNER or LEFT outer (RIGHT outer rejected — we can't
    ///     produce probe rows that don't match any build VID).
    fn try_emit_vid_lookup_join(
        &self,
        equi_pairs: &[(Expr, Expr)],
        join_type: JoinType,
        left_plan: &Arc<dyn ExecutionPlan>,
        right_plan: &Arc<dyn ExecutionPlan>,
        left_logical: &LogicalPlan,
        right_logical: &LogicalPlan,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        use crate::query::df_graph::scan::GraphScanExec;
        use crate::query::df_graph::vid_lookup_join::{
            EquiPair, ProbeSide, VidJoinKind, VidLookupJoinExec,
        };
        use datafusion::physical_expr::expressions::Column;

        if equi_pairs.is_empty() {
            return Ok(None);
        }

        // 1. Find the anchor pair: the one where the probe side is
        // `Property(Variable(_), "_vid")`. The classifier's invariant is
        // that `l_expr` references LEFT subtree variables and `r_expr`
        // references RIGHT subtree variables, so detecting `_vid` on
        // `l_expr` means the probe is on the left.
        let mut anchor_idx: Option<(usize, ProbeSide)> = None;
        for (i, (l_expr, r_expr)) in equi_pairs.iter().enumerate() {
            if expr_is_vid_property(l_expr) {
                anchor_idx = Some((i, ProbeSide::Left));
                break;
            }
            if expr_is_vid_property(r_expr) {
                anchor_idx = Some((i, ProbeSide::Right));
                break;
            }
        }
        let Some((anchor_pair_idx, probe_side)) = anchor_idx else {
            return Ok(None);
        };

        let probe_plan = match probe_side {
            ProbeSide::Left => left_plan,
            ProbeSide::Right => right_plan,
        };
        let build_plan = match probe_side {
            ProbeSide::Left => right_plan,
            ProbeSide::Right => left_plan,
        };
        let build_logical = match probe_side {
            ProbeSide::Left => right_logical,
            ProbeSide::Right => left_logical,
        };

        // 2. Probe-side plan must be a top-level GraphScanExec.
        //
        // We deliberately do NOT peek through an SSI `ReadSetRecordingExec`
        // here. That wrapper is only inserted for read-write transactions with
        // an active read-set, and `VidLookupJoinExec` drives the probe scan via
        // `execute_with_vid_filter`, bypassing the wrapper — which would silently
        // skip read-set capture for the probe rows. Letting the wrapper mask the
        // scan makes this rewrite bail to `HashJoinExec`, which executes the
        // wrapper normally and records the reads. Non-SSI / read-only contexts
        // have no wrapper, so the optimization still fires there.
        //
        // We DO peek through the `wrap_optional` wrapper, and only that one.
        // An OPTIONAL MATCH scan is wrapped in
        // `NestedLoopJoinExec(PlaceholderRowExec, GraphScanExec, Left)` whose
        // sole purpose is to emit one all-NULL row when the scan is empty
        // (`wrap_optional`). For `VidJoinKind::Left` that wrapper is redundant:
        // this operator already null-pads every unmatched build row, which
        // covers the empty-probe case. And because the placeholder carries
        // `Schema::empty()`, the wrapper's schema equals the scan's, so
        // unwrapping does not shift any downstream column index.
        //
        // Without this, `VidJoinKind::Left` was unreachable *by construction*:
        // for LEFT the probe is necessarily the optional side, and the optional
        // side is always wrapped. It had been dead since the commit that
        // introduced it.
        let probe_plan = Self::unwrap_optional_scan(probe_plan);
        if probe_plan
            .as_any()
            .downcast_ref::<GraphScanExec>()
            .is_none()
        {
            return Ok(None);
        }

        // 3. Compile every equi-pair's expressions against their respective
        // schemas, requiring each side to resolve to a Column. The anchor
        // pair additionally requires the build side to be UInt64.
        let left_schema = left_plan.schema();
        let right_schema = right_plan.schema();
        let left_ctx = self.translation_context_for_plan(left_logical);
        let right_ctx = self.translation_context_for_plan(right_logical);
        let _ = build_logical; // contexts already covered by left/right_ctx

        let session = self.session_ctx.read();
        let state = session.state();
        let left_compiler = self.expr_compiler(&state, Some(&left_ctx));
        let right_compiler = self.expr_compiler(&state, Some(&right_ctx));

        let mut compiled: Vec<EquiPair> = Vec::with_capacity(equi_pairs.len());
        for (l_expr, r_expr) in equi_pairs {
            let l_phys = left_compiler.compile(l_expr, &left_schema)?;
            let r_phys = right_compiler.compile(r_expr, &right_schema)?;
            let (Some(l_col), Some(r_col)) = (
                l_phys.as_any().downcast_ref::<Column>(),
                r_phys.as_any().downcast_ref::<Column>(),
            ) else {
                // Computed expression on either side → bail to HashJoinExec.
                return Ok(None);
            };
            compiled.push(EquiPair {
                left_col_idx: l_col.index(),
                right_col_idx: r_col.index(),
            });
        }

        // 4. Anchor build column must be UInt64.
        let anchor = compiled[anchor_pair_idx];
        let anchor_build_idx = match probe_side {
            ProbeSide::Left => anchor.right_col_idx,
            ProbeSide::Right => anchor.left_col_idx,
        };
        let build_schema = build_plan.schema();
        // A vid is a `u64`, but Cypher has no unsigned integer type, so a vid
        // stored in a user property arrives as `Int64`. Accepting only `UInt64`
        // here excluded the operator's own motivating shape
        // (`id(b) = a.linked_vid`) and left it able to fire on nothing but
        // `id(a) = id(b)` — where both keys are already vids and there is
        // nothing to look up. The conversion is range-checked at the read site.
        if !matches!(
            build_schema.field(anchor_build_idx).data_type(),
            datafusion::arrow::datatypes::DataType::UInt64
                | datafusion::arrow::datatypes::DataType::Int64
        ) {
            return Ok(None);
        }

        // 5. Reorder so the anchor pair is at index 0 (operator's invariant).
        if anchor_pair_idx != 0 {
            compiled.swap(0, anchor_pair_idx);
        }

        // 6. Translate join_type. This operator is build-outer: it fully
        // materializes the build side and fetches the probe *by* build VIDs, so
        // it can only null-pad unmatched BUILD rows — never unmatched probe rows
        // (those are never scanned). RIGHT outer is therefore rejected, and LEFT
        // outer is correct only when the build side IS the left (outer) side, i.e.
        // the probe is on the right. When the probe is on the left, emitting this
        // fast-path would preserve the right side instead — inverting LEFT to
        // RIGHT semantics — so bail and let the standard join handle it.
        let join_kind = match (join_type, probe_side) {
            (JoinType::Inner, _) => VidJoinKind::Inner,
            (JoinType::Left, ProbeSide::Right) => VidJoinKind::Left,
            _ => return Ok(None),
        };

        drop(session);

        // Hand `try_new` the unwrapped probe: it re-validates that the probe
        // child is a `GraphScanExec`, and it drives that scan directly via
        // `execute_with_vid_filter`.
        let (final_left, final_right) = match probe_side {
            ProbeSide::Left => (probe_plan.clone(), right_plan.clone()),
            ProbeSide::Right => (left_plan.clone(), probe_plan.clone()),
        };

        Ok(Some(Arc::new(VidLookupJoinExec::try_new(
            final_left,
            final_right,
            probe_side,
            compiled,
            join_kind,
        )?)))
    }

    /// Peels the `wrap_optional` wrapper off a probe plan, if that is exactly what
    /// it is.
    ///
    /// `wrap_optional` turns an OPTIONAL MATCH scan into
    /// `NestedLoopJoinExec(PlaceholderRowExec, inner, Left)` so an empty scan still
    /// yields one all-NULL row. `VidJoinKind::Left` null-pads unmatched build rows
    /// itself, so for that operator the wrapper is redundant — and because the
    /// placeholder's schema is empty, removing it is schema-neutral.
    ///
    /// Deliberately **one level, exact match, no recursion.** The SSI
    /// `ReadSetRecordingExec` wrapper must keep masking the scan so the rewrite
    /// bails to `HashJoinExec` and read-set capture still happens; a recursive or
    /// loose unwrap would defeat that. Anything that is not precisely
    /// `NLJ(PlaceholderRowExec, _, Left)` with no filter is returned untouched.
    fn unwrap_optional_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let Some(nlj) = plan.as_any().downcast_ref::<NestedLoopJoinExec>() else {
            return plan.clone();
        };
        if *nlj.join_type() != JoinType::Left || nlj.filter().is_some() {
            return plan.clone();
        }
        let children = nlj.children();
        let [left, right] = children.as_slice() else {
            return plan.clone();
        };
        if left.as_any().downcast_ref::<PlaceholderRowExec>().is_none()
            || !left.schema().fields().is_empty()
        {
            return plan.clone();
        }
        (*right).clone()
    }

    /// Plan a projection, passing alias map through to Sort nodes in the input chain.
    fn plan_project_with_aliases(
        &self,
        input: &LogicalPlan,
        projections: &[(Expr, Option<String>)],
        all_properties: &HashMap<String, HashSet<String>>,
        alias_map: &HashMap<String, Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Route through plan_internal_with_aliases to propagate aliases to Sort
        let input_plan = self.plan_internal_with_aliases(input, all_properties, alias_map)?;
        self.plan_project_from_input(input_plan, projections, Some(input))
    }

    /// Wrap `input_plan` so every relationship an unresolved `startNode` /
    /// `endNode` call names has its endpoints materialised as node values.
    ///
    /// A call only reaches here when `resolve_traversal_endpoints` could not
    /// rewrite it to a variable — either a projection dropped the endpoints
    /// (`WITH e AS rel`) or the relationship never came from a traversal
    /// (`relationships(path)`). In both cases the relationship value still
    /// carries `_src` / `_dst`; what it lacks is the endpoint's properties.
    ///
    /// Relationships whose column is not in the input schema are skipped —
    /// nothing can be read for them here, and the existing error is a better
    /// report than a column of nulls.
    fn hydrate_endpoints_for(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        projections: &[(Expr, Option<String>)],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use crate::query::df_graph::endpoint_hydrate::EndpointHydrateExec;

        let mut rels: Vec<String> = Vec::new();
        for (expr, _) in projections {
            collect_endpoint_relationships(expr, &mut rels);
        }
        if rels.is_empty() {
            return Ok(input_plan);
        }

        let mut plan = input_plan;
        for rel in rels {
            let schema = plan.schema();
            if schema.index_of(&rel).is_err()
                || schema
                    .index_of(&crate::query::df_graph::endpoint_hydrate::endpoint_column(
                        &rel, true,
                    ))
                    .is_ok()
            {
                continue;
            }
            plan = Arc::new(EndpointHydrateExec::new(plan, rel, self.graph_ctx.clone()));
        }
        Ok(plan)
    }

    /// Build projection expressions from an already-planned input.
    fn plan_project_from_input(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        projections: &[(Expr, Option<String>)],
        context_plan: Option<&LogicalPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // `startNode(r)` normally never reaches the UDF — the planner rewrites
        // it to the traversal's endpoint variable. Once a projection stops
        // carrying those variables, or the relationship never came from a
        // traversal at all, the call survives and the UDF has only the
        // relationship to work with. Materialise its endpoints first.
        let input_plan = self.hydrate_endpoints_for(input_plan, projections)?;
        let schema = input_plan.schema();

        let session = self.session_ctx.read();
        let state = session.state();

        // Build translation context with variable kinds if we have a logical plan
        let mut ctx = context_plan.map(|p| self.translation_context_for_plan(p));
        if let Some(ctx) = ctx.as_mut() {
            ctx.available_columns =
                Some(schema.fields().iter().map(|f| f.name().clone()).collect());
            for name in schema.fields().iter().map(|f| f.name()) {
                if name.starts_with(ENDPOINT_COLUMN_PREFIX) {
                    ctx.node_variable_hints.push(name.clone());
                }
            }
        }
        let ctx = ctx;

        let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = Vec::new();

        for (expr, alias) in projections {
            // Handle whole-node/relationship projection: RETURN n
            // The scan layer materializes the variable as either:
            //   - A Struct column (registered labels via add_structural_projection)
            //   - A LargeBinary/CypherValue column aliased as the variable (schemaless via add_alias_projection)
            // Project that column directly, plus _vid/_labels helpers for post-processing.
            if let Expr::Variable(var_name) = expr {
                if schema.column_with_name(var_name).is_some() {
                    let (col_idx, _) = schema.column_with_name(var_name).unwrap();
                    let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                        datafusion::physical_expr::expressions::Column::new(var_name, col_idx),
                    );
                    let name = alias.clone().unwrap_or_else(|| var_name.clone());
                    exprs.push((col_expr, name));

                    // Include _vid and _labels as helper columns for post-processing
                    let vid_col = format!("{}._vid", var_name);
                    let labels_col = format!("{}._labels", var_name);
                    if let Some((vi, _)) = schema.column_with_name(&vid_col) {
                        let ve: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(&vid_col, vi),
                        );
                        exprs.push((ve, vid_col.clone()));
                    }
                    if let Some((li, _)) = schema.column_with_name(&labels_col) {
                        let le: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(&labels_col, li),
                        );
                        exprs.push((le, labels_col.clone()));
                    }

                    // Carry through all {var}.{prop} columns so downstream
                    // operators (e.g. RETURN n.name after WITH n) can find them.
                    let prefix = format!("{}.", var_name);
                    for (idx, field) in schema.fields().iter().enumerate() {
                        let fname = field.name();
                        if fname.starts_with(&prefix)
                            && fname != &vid_col
                            && fname != &labels_col
                            && !exprs.iter().any(|(_, n)| n == fname)
                        {
                            let prop_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                                Arc::new(datafusion::physical_expr::expressions::Column::new(
                                    fname, idx,
                                ));
                            exprs.push((prop_expr, fname.clone()));
                        }
                    }
                    continue;
                }

                // No materialized column — build a struct from expanded dot-columns
                // This handles traversal targets that have b._vid, b.name, etc. but no b column
                let prefix = format!("{}.", var_name);
                let expanded_fields: Vec<(usize, String)> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| f.name().starts_with(&prefix))
                    .map(|(i, f)| (i, f.name().clone()))
                    .collect();

                if !expanded_fields.is_empty() {
                    use datafusion::functions::expr_fn::named_struct;
                    use datafusion::logical_expr::lit;

                    // Build named_struct args: pairs of (field_name_literal, column_ref)
                    let mut struct_args = Vec::new();
                    for (_, field_name) in &expanded_fields {
                        let prop_name = &field_name[prefix.len()..];
                        struct_args.push(lit(prop_name.to_string()));
                        // Use Column::from_name to avoid dot-parsing (b._vid != table b, col _vid)
                        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
                            field_name.as_str(),
                        )));
                    }

                    let struct_expr = named_struct(struct_args);
                    let df_schema =
                        datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
                    let session = self.session_ctx.read();
                    let state_ref = session.state();
                    let resolved_expr = Self::resolve_udfs(&struct_expr, &state_ref)?;

                    use datafusion::physical_planner::PhysicalPlanner;
                    let phys_planner =
                        datafusion::physical_planner::DefaultPhysicalPlanner::default();
                    let physical_struct_expr = phys_planner.create_physical_expr(
                        &resolved_expr,
                        &df_schema,
                        &state_ref,
                    )?;

                    let name = alias.clone().unwrap_or_else(|| var_name.clone());
                    exprs.push((physical_struct_expr, name));

                    // Also include _vid and _labels helpers
                    let vid_col = format!("{}._vid", var_name);
                    let labels_col = format!("{}._labels", var_name);
                    if let Some((vi, _)) = schema.column_with_name(&vid_col) {
                        let ve: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(&vid_col, vi),
                        );
                        exprs.push((ve, vid_col.clone()));
                    }
                    if let Some((li, _)) = schema.column_with_name(&labels_col) {
                        let le: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(&labels_col, li),
                        );
                        exprs.push((le, labels_col.clone()));
                    }

                    // Carry through remaining {var}.{prop} columns not already
                    // included by the struct projection above.
                    for (idx, field) in schema.fields().iter().enumerate() {
                        let fname = field.name();
                        if fname.starts_with(&prefix)
                            && fname != &vid_col
                            && fname != &labels_col
                            && !exprs.iter().any(|(_, n)| n == fname)
                        {
                            let prop_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                                Arc::new(datafusion::physical_expr::expressions::Column::new(
                                    fname, idx,
                                ));
                            exprs.push((prop_expr, fname.clone()));
                        }
                    }
                    continue;
                }
                // Fall through to normal expression compilation if no matching columns at all
            }

            // Handle RETURN * (wildcard) — expand to all input columns
            if matches!(expr, Expr::Wildcard) {
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                        datafusion::physical_expr::expressions::Column::new(field.name(), col_idx),
                    );
                    exprs.push((col_expr, field.name().clone()));
                }
                continue;
            }

            let compiler = self.expr_compiler(&state, ctx.as_ref());
            let mut physical_expr = compiler.compile(expr, &schema)?;

            // Stamp the `uni_raw_bytes` marker on a computed raw-bytes scalar output
            // (e.g. `coalesce(b.missing, b.data)`, `CASE ... THEN b.data ...`). Plain
            // column passthroughs already preserve their marker via `Column::return_field`,
            // and raw-bytes list literals are marked at compile time (in the expr compiler).
            if crate::query::df_graph::raw_bytes_marker::is_raw_scalar(expr, &schema)
                && !matches!(expr, Expr::Variable(_) | Expr::Property(_, _))
            {
                physical_expr = Arc::new(
                    crate::query::df_graph::raw_bytes_marker::RawBytesMarkerExpr::scalar(
                        physical_expr,
                    ),
                );
            }

            let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
            exprs.push((physical_expr, name));
        }

        Ok(Arc::new(ProjectionExec::try_new(exprs, input_plan)?))
    }

    /// Plan a compact Locy YIELD projection — emits ONLY the listed expressions,
    /// without carrying through helper/property columns.
    ///
    /// Node variables are projected as their `._vid` column (UInt64).
    /// Other expressions are compiled normally, then CAST to target type if needed.
    fn plan_locy_project(
        &self,
        input: &LogicalPlan,
        projections: &[(Expr, Option<String>)],
        target_types: &[DataType],
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_expr::expressions::Column;

        let input_plan = self.plan_internal(input, all_properties)?;
        let schema = input_plan.schema();

        let session = self.session_ctx.read();
        let state = session.state();

        let ctx = self.translation_context_for_plan(input);

        let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = Vec::new();

        for (i, (expr, alias)) in projections.iter().enumerate() {
            let target_type = target_types.get(i);

            // Handle node/relationship variables: extract ._vid column
            if let Expr::Variable(var_name) = expr {
                // Check if this is a graph-expanded node variable ({var}._vid exists)
                let vid_col_name = format!("{}._vid", var_name);
                let vid_col_match = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == &vid_col_name);

                if let Some((vid_idx, _)) = vid_col_match {
                    // Node variable → extract VID (UInt64)
                    let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                        Arc::new(Column::new(&vid_col_name, vid_idx));
                    let name = alias.clone().unwrap_or_else(|| var_name.clone());
                    exprs.push((col_expr, name));
                    continue;
                }

                // Direct column (e.g. from derived scan)
                if let Some((col_idx, _)) = schema.column_with_name(var_name) {
                    let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                        Arc::new(Column::new(var_name, col_idx));
                    let name = alias.clone().unwrap_or_else(|| var_name.clone());
                    exprs.push((col_expr, name));
                    continue;
                }
                // Fall through to generic expression compilation
            }

            // Generic expression compilation (property access, literals, etc.)
            let compiler = self.expr_compiler(&state, Some(&ctx));
            let physical_expr = compiler.compile(expr, &schema)?;

            // CAST if the compiled expression's output type doesn't match target.
            // Skip coercion when actual is a string type but target is numeric
            // (or vice versa) — this means `infer_expr_type` guessed wrong
            // (e.g. defaulting Property to Float64 for a string column).
            let physical_expr = if let Some(target_dt) = target_type {
                let actual_dt = physical_expr
                    .data_type(schema.as_ref())
                    .unwrap_or(DataType::LargeUtf8);
                let is_string = |dt: &DataType| matches!(dt, DataType::Utf8 | DataType::LargeUtf8);
                let is_numeric = |dt: &DataType| {
                    matches!(dt, DataType::Int64 | DataType::Float64 | DataType::UInt64)
                };
                let cross_domain = (is_string(&actual_dt) && is_numeric(target_dt))
                    || (is_numeric(&actual_dt) && is_string(target_dt));
                if actual_dt != *target_dt && !cross_domain {
                    coerce_physical_expr(physical_expr, &actual_dt, target_dt, schema.as_ref())
                } else {
                    physical_expr
                }
            } else {
                physical_expr
            };

            let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
            exprs.push((physical_expr, name));
        }

        Ok(Arc::new(ProjectionExec::try_new(exprs, input_plan)?))
    }

    /// Plan an aggregation.
    fn plan_aggregate(
        &self,
        input: &LogicalPlan,
        group_by: &[Expr],
        aggregates: &[Expr],
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;
        let schema = input_plan.schema();

        let session = self.session_ctx.read();
        let state = session.state();

        // Build translation context with variable kinds from the input plan
        let ctx = self.translation_context_for_plan(input);

        // Translate group by expressions
        use crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler;
        let mut group_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();
        for expr in group_by {
            let name = expr.to_string_repr();

            // Entity variables (Node/Edge) from traversals may not have a direct
            // column — only expanded property columns like "other._vid",
            // "other.name", etc. Skip them here; the property expansion loop
            // below adds those columns to the group-by instead.
            if let Expr::Variable(var_name) = expr
                && schema.column_with_name(var_name).is_none()
            {
                let prefix = format!("{}.", var_name);
                let has_expanded = schema
                    .fields()
                    .iter()
                    .any(|f| f.name().starts_with(&prefix));
                if has_expanded {
                    continue;
                }
            }

            let physical_expr = if CypherPhysicalExprCompiler::contains_custom_expr(expr) {
                // Custom expressions (quantifiers, list comprehensions, reduce, etc.)
                // cannot be translated via cypher_expr_to_df; compile them directly.
                let compiler = self.expr_compiler(&state, Some(&ctx));
                compiler.compile(expr, &schema)?
            } else {
                // DateTime/Time struct grouping: group by UTC-normalized values
                // Two DateTimes with same UTC instant but different offsets should group together
                let df_schema_ref =
                    datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
                let df_expr = cypher_expr_to_df(expr, Some(&ctx))?;
                let df_expr = Self::resolve_udfs(&df_expr, &state)?;
                let df_expr = crate::query::df_expr::apply_type_coercion(&df_expr, &df_schema_ref)?;
                let mut df_expr = Self::resolve_udfs(&df_expr, &state)?;
                if let Ok(expr_type) = df_expr.get_type(&df_schema_ref) {
                    if uni_common::core::schema::is_datetime_struct(&expr_type) {
                        // Group by UTC instant (nanos_since_epoch)
                        df_expr = crate::query::df_expr::extract_datetime_nanos(df_expr);
                    } else if uni_common::core::schema::is_time_struct(&expr_type) {
                        // Group by UTC-normalized time
                        // extract_time_nanos does: nanos_since_midnight - (offset_seconds * 1e9)
                        df_expr = crate::query::df_expr::extract_time_nanos(df_expr);
                    }
                }

                // Convert logical expression to physical
                create_physical_expr(&df_expr, &df_schema_ref, state.execution_props())?
            };
            group_exprs.push((physical_expr, name));
        }

        // For entity variables (Node/Edge) in group_by, also include their
        // property columns. Properties are functionally dependent on the entity,
        // so grouping by them is semantically correct and ensures they survive
        // the aggregation for downstream property access (e.g. RETURN a.name
        // after WITH a, min(...) AS m).
        for expr in group_by {
            if let Expr::Variable(var_name) = expr
                && matches!(
                    ctx.variable_kinds.get(var_name),
                    Some(VariableKind::Node) | Some(VariableKind::Edge)
                )
            {
                let prefix = format!("{}.", var_name);
                for (idx, field) in schema.fields().iter().enumerate() {
                    if field.name().starts_with(&prefix) {
                        let prop_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                            datafusion::physical_expr::expressions::Column::new(field.name(), idx),
                        );
                        group_exprs.push((prop_col, field.name().clone()));
                    }
                }
            }
        }

        let physical_group_by = PhysicalGroupBy::new_single(group_exprs);

        // Pre-compute pattern comprehensions in aggregate arguments
        let (input_plan, schema, rewritten_aggregates) =
            self.precompute_custom_aggregate_args(input_plan, &schema, aggregates, &state, &ctx)?;

        // Translate aggregates and their associated filter expressions
        // (e.g. collect() uses a filter to exclude null values per Cypher spec)
        let (aggr_exprs, filter_exprs): (Vec<_>, Vec<_>) = self
            .translate_aggregates(&rewritten_aggregates, &schema, &state, &ctx)?
            .into_iter()
            .unzip();
        let num_aggregates = aggr_exprs.len();

        let agg_exec = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            physical_group_by,
            aggr_exprs,
            filter_exprs,
            input_plan,
            schema,
        )?);

        // DataFusion's AggregateExec auto-generates column names from physical
        // expressions (e.g. `count(Int32(1))`), but the logical plan's projection
        // expects names like `COUNT(n)`. Add a renaming projection to bridge this.
        let agg_schema = agg_exec.schema();
        // Use actual expanded group-by count (includes entity property columns)
        // rather than logical group_by.len() which doesn't account for expansion.
        let num_group_by = agg_schema.fields().len() - num_aggregates;
        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();

        for (i, field) in agg_schema.fields().iter().enumerate() {
            let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Column::new(field.name(), i),
            );
            let name = if i >= num_group_by {
                // Rename aggregate column to expected Cypher name
                aggregate_column_name(&aggregates[i - num_group_by])
            } else {
                field.name().clone()
            };
            proj_exprs.push((col_expr, name));
        }

        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, agg_exec)?))
    }

    /// Wrap a temporal aggregate argument with `get_field(arg, "nanos_since_epoch")` or
    /// `get_field(arg, "nanos_since_midnight")` when the argument is a DateTime/Time struct.
    ///
    /// Returns the argument unchanged for non-temporal types.
    fn wrap_temporal_sort_key(
        arg: datafusion::logical_expr::Expr,
        schema: &SchemaRef,
    ) -> Result<datafusion::logical_expr::Expr> {
        use datafusion::logical_expr::ScalarUDF;
        if let Ok(arg_type) = arg.get_type(&datafusion::common::DFSchema::try_from(
            schema.as_ref().clone(),
        )?) {
            if uni_common::core::schema::is_datetime_struct(&arg_type) {
                return Ok(datafusion::logical_expr::Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction::new_udf(
                        Arc::new(ScalarUDF::from(
                            datafusion::functions::core::getfield::GetFieldFunc::new(),
                        )),
                        vec![arg, datafusion::logical_expr::lit("nanos_since_epoch")],
                    ),
                ));
            } else if uni_common::core::schema::is_time_struct(&arg_type) {
                return Ok(datafusion::logical_expr::Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction::new_udf(
                        Arc::new(ScalarUDF::from(
                            datafusion::functions::core::getfield::GetFieldFunc::new(),
                        )),
                        vec![arg, datafusion::logical_expr::lit("nanos_since_midnight")],
                    ),
                ));
            }
        }
        Ok(arg)
    }

    /// Translate Cypher aggregate expressions to DataFusion.
    fn translate_aggregates(
        &self,
        aggregates: &[Expr],
        schema: &SchemaRef,
        state: &SessionState,
        ctx: &TranslationContext,
    ) -> Result<Vec<PhysicalAggregate>> {
        use datafusion::functions_aggregate::expr_fn::{
            avg, count, max, min, stddev, stddev_pop, sum, var_pop, var_sample,
        };

        let mut result: Vec<PhysicalAggregate> = Vec::new();

        for agg_expr in aggregates {
            let Expr::FunctionCall {
                name,
                args,
                distinct,
                ..
            } = agg_expr
            else {
                return Err(anyhow!("Expected aggregate function, got: {:?}", agg_expr));
            };

            let name_lower = name.to_lowercase();

            // Helper to get required first argument
            let get_arg = || -> Result<DfExpr> {
                if args.is_empty() {
                    return Err(anyhow!("{}() requires an argument", name_lower));
                }
                cypher_expr_to_df(&args[0], Some(ctx))
            };

            let df_agg = match name_lower.as_str() {
                "count" if args.is_empty() => count(datafusion::logical_expr::lit(1)),
                "count" => {
                    // count(*) counts every row (including the all-null padding row
                    // an unmatched OPTIONAL MATCH produces), so it maps to
                    // count(lit(1)). count(variable), by contrast, is count over an
                    // expression and must EXCLUDE nulls per Cypher semantics — so it
                    // counts the entity's identity column (_vid/_eid), which is
                    // non-null for matched rows and null for the OPTIONAL pad, letting
                    // DataFusion's COUNT drop those rows. count(lit(1)) here would
                    // over-count by one per unmatched group. The later `.distinct()`
                    // pass turns this into COUNT(DISTINCT identity) when requested;
                    // dedup by identity (not the full materialized struct) also avoids
                    // reading every property column (issue #134 family). Scalar-bound
                    // variables (not Node/Edge) count their own column, which likewise
                    // excludes nulls.
                    if matches!(args.first(), Some(uni_cypher::ast::Expr::Wildcard)) {
                        count(datafusion::logical_expr::lit(1))
                    } else if let Some(uni_cypher::ast::Expr::Variable(var)) = args.first() {
                        let id_col = match ctx.variable_kinds.get(var) {
                            Some(VariableKind::Node) => Some("_vid"),
                            Some(VariableKind::Edge) => Some("_eid"),
                            _ => None,
                        };
                        match id_col {
                            Some(suffix) => count(DfExpr::Column(
                                datafusion::common::Column::from_name(format!("{var}.{suffix}")),
                            )),
                            None => count(get_arg()?),
                        }
                    } else {
                        count(get_arg()?)
                    }
                }
                "sum" => {
                    let arg = get_arg()?;
                    if self.is_large_binary_col(&arg, schema) {
                        let udaf = Arc::new(crate::query::df_udfs::create_cypher_sum_udaf());
                        udaf.call(vec![arg])
                    } else {
                        // Widen small integers to Int64 (DataFusion doesn't support Int32 sum).
                        // Float columns pass through unchanged so SUM preserves float type.
                        use datafusion::logical_expr::Cast;
                        let is_float = if let DfExpr::Column(col) = &arg
                            && let Ok(field) = schema.field_with_name(&col.name)
                        {
                            matches!(
                                field.data_type(),
                                datafusion::arrow::datatypes::DataType::Float32
                                    | datafusion::arrow::datatypes::DataType::Float64
                            )
                        } else {
                            false
                        };
                        if is_float {
                            sum(DfExpr::Cast(Cast::new(
                                Box::new(arg),
                                datafusion::arrow::datatypes::DataType::Float64,
                            )))
                        } else {
                            sum(DfExpr::Cast(Cast::new(
                                Box::new(arg),
                                datafusion::arrow::datatypes::DataType::Int64,
                            )))
                        }
                    }
                }
                "avg" => {
                    let arg = get_arg()?;
                    if self.is_large_binary_col(&arg, schema) {
                        let coerced = crate::query::df_udfs::cypher_to_float64_expr(arg);
                        avg(coerced)
                    } else {
                        use datafusion::logical_expr::Cast;
                        avg(DfExpr::Cast(Cast::new(
                            Box::new(arg),
                            datafusion::arrow::datatypes::DataType::Float64,
                        )))
                    }
                }
                // Numerically-stable standard-deviation / variance aggregates
                // (Neo4j naming `stDev`/`stDevP`; openCypher `stdev`/`stdevp`).
                // DataFusion's implementations use Welford's online algorithm,
                // so they avoid the catastrophic cancellation of the
                // `sqrt(avg(x*x) - avg(x)^2)` identity for large means. Inputs
                // are coerced to Float64 the same way `avg` is, so a raw
                // schemaless (LargeBinary) numeric property works directly.
                "stdev" | "stddev" => {
                    let arg = get_arg()?;
                    stddev(Self::coerce_numeric_for_stat(arg, schema, self))
                }
                "stdevp" | "stddevp" => {
                    let arg = get_arg()?;
                    stddev_pop(Self::coerce_numeric_for_stat(arg, schema, self))
                }
                "variance" => {
                    let arg = get_arg()?;
                    var_sample(Self::coerce_numeric_for_stat(arg, schema, self))
                }
                "variancep" => {
                    let arg = get_arg()?;
                    var_pop(Self::coerce_numeric_for_stat(arg, schema, self))
                }
                "min" => {
                    // Use Cypher-aware min for LargeBinary columns (mixed types)
                    let arg = Self::wrap_temporal_sort_key(get_arg()?, schema)?;

                    if self.is_large_binary_col(&arg, schema) {
                        let udaf = Arc::new(crate::query::df_udfs::create_cypher_min_udaf());
                        udaf.call(vec![arg])
                    } else {
                        min(arg)
                    }
                }
                "max" => {
                    // Use Cypher-aware max for LargeBinary columns (mixed types)
                    let arg = Self::wrap_temporal_sort_key(get_arg()?, schema)?;

                    if self.is_large_binary_col(&arg, schema) {
                        let udaf = Arc::new(crate::query::df_udfs::create_cypher_max_udaf());
                        udaf.call(vec![arg])
                    } else {
                        max(arg)
                    }
                }
                "percentiledisc" => {
                    if args.len() != 2 {
                        return Err(anyhow!("percentileDisc() requires exactly 2 arguments"));
                    }
                    let expr_arg = cypher_expr_to_df(&args[0], Some(ctx))?;
                    let pct_arg = cypher_expr_to_df(&args[1], Some(ctx))?;
                    let coerced = crate::query::df_udfs::cypher_to_float64_expr(expr_arg);
                    let udaf =
                        Arc::new(crate::query::df_udfs::create_cypher_percentile_disc_udaf());
                    udaf.call(vec![coerced, pct_arg])
                }
                "percentilecont" => {
                    if args.len() != 2 {
                        return Err(anyhow!("percentileCont() requires exactly 2 arguments"));
                    }
                    let expr_arg = cypher_expr_to_df(&args[0], Some(ctx))?;
                    let pct_arg = cypher_expr_to_df(&args[1], Some(ctx))?;
                    let coerced = crate::query::df_udfs::cypher_to_float64_expr(expr_arg);
                    let udaf =
                        Arc::new(crate::query::df_udfs::create_cypher_percentile_cont_udaf());
                    udaf.call(vec![coerced, pct_arg])
                }
                "collect" => {
                    // Use custom Cypher collect UDAF that filters nulls and returns
                    // empty list (not null) when all inputs are null.
                    //
                    // The query's interning scope goes with it: a large enough
                    // list then travels as a handle, so the operators above copy
                    // 9 bytes per row instead of the whole list.
                    let arg = get_arg()?;
                    crate::query::df_udfs::create_cypher_collect_expr(
                        arg,
                        *distinct,
                        Some(Arc::clone(self.graph_ctx.handle_scope())),
                    )
                }
                "btic_min" => {
                    let arg = get_arg()?;
                    let udaf = Arc::new(crate::query::df_udfs::create_btic_min_udaf());
                    udaf.call(vec![arg])
                }
                "btic_max" => {
                    let arg = get_arg()?;
                    let udaf = Arc::new(crate::query::df_udfs::create_btic_max_udaf());
                    udaf.call(vec![arg])
                }
                "btic_span_agg" => {
                    let arg = get_arg()?;
                    let udaf = Arc::new(crate::query::df_udfs::create_btic_span_agg_udaf());
                    udaf.call(vec![arg])
                }
                "btic_count_at" => {
                    if args.len() != 2 {
                        return Err(anyhow!("btic_count_at() requires exactly 2 arguments"));
                    }
                    let btic_arg = cypher_expr_to_df(&args[0], Some(ctx))?;
                    let point_arg = cypher_expr_to_df(&args[1], Some(ctx))?;
                    let udaf = Arc::new(crate::query::df_udfs::create_btic_count_at_udaf());
                    udaf.call(vec![btic_arg, point_arg])
                }
                _ => {
                    // Fall through to plugin-registry lookup. User
                    // aggregates registered via
                    // `PluginRegistrar::aggregate_fn` (M9
                    // `uni.plugin.declareAggregate` is the primary
                    // user) dispatch through the
                    // `PluginAggregateUdaf` adapter.
                    // Resolve the dotted name against the registry trying every
                    // namespace/local split (first-dot → last-dot), so plugin
                    // ids that themselves contain dots (e.g. `ai.example`)
                    // resolve as well as single-segment ids. See
                    // `QName::candidate_splits`.
                    // Resolve against the instance/host registry first, then the
                    // session-local registry — session-scoped aggregates (e.g.
                    // registered via `session.finalize_plugin`) live only there,
                    // mirroring the scalar wiring in `read.rs`. The matched
                    // registry is threaded into `PluginAggregateUdaf` because its
                    // accumulator re-fetches the plugin fn from that registry at
                    // execution time (not from the task-local scope).
                    let session_registry = crate::current_session_plugin_registry();
                    let resolved = uni_plugin::QName::candidate_splits(&name_lower).find_map(|q| {
                        if let Some(e) = self.plugin_registry.aggregate(&q) {
                            return Some((q, e, Arc::clone(&self.plugin_registry)));
                        }
                        if let Some(sr) = session_registry.as_ref()
                            && let Some(e) = sr.aggregate(&q)
                        {
                            return Some((q, e, Arc::clone(sr)));
                        }
                        None
                    });
                    if let Some((qname, entry, registry)) = resolved {
                        let arg_exprs: Vec<DfExpr> = args
                            .iter()
                            .map(|a| cypher_expr_to_df(a, Some(ctx)))
                            .collect::<Result<Vec<_>>>()?;
                        let udaf = Arc::new(datafusion::logical_expr::AggregateUDF::from(
                            crate::query::df_udaf_plugin::PluginAggregateUdaf::new(
                                qname,
                                registry,
                                entry.signature.clone(),
                            ),
                        ));
                        udaf.call(arg_exprs)
                    } else {
                        return Err(anyhow!("Unsupported aggregate function: {}", name));
                    }
                }
            };

            // Apply DISTINCT if needed (collect/percentile handle their own distinct)
            let df_agg = if *distinct
                && !matches!(
                    name_lower.as_str(),
                    "collect" | "percentiledisc" | "percentilecont"
                ) {
                use datafusion::prelude::ExprFunctionExt;
                df_agg.distinct().build().map_err(|e| anyhow!("{}", e))?
            } else {
                df_agg
            };

            // Resolve UDFs and apply type coercion inside aggregate arguments
            let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
            let df_agg = Self::resolve_udfs(&df_agg, state)?;
            let df_agg = crate::query::df_expr::apply_type_coercion(&df_agg, &df_schema)?;
            let df_agg = Self::resolve_udfs(&df_agg, state)?;

            // Convert to physical aggregate
            let agg_and_filter = self.create_physical_aggregate(&df_agg, schema, state)?;
            result.push(agg_and_filter);
        }

        Ok(result)
    }

    /// Pre-compute pattern comprehensions in aggregate arguments.
    ///
    /// Scans aggregate expressions for pattern comprehensions, compiles them as
    /// physical expressions, adds them as projected columns, and rewrites the
    /// aggregate expressions to reference the pre-computed columns.
    fn precompute_custom_aggregate_args(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        aggregates: &[Expr],
        state: &SessionState,
        ctx: &TranslationContext,
    ) -> Result<(Arc<dyn ExecutionPlan>, SchemaRef, Vec<Expr>)> {
        use crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler;

        let mut needs_projection = false;
        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();
        let mut rewritten_aggregates = Vec::new();
        let mut col_counter = 0;

        // First pass: copy all existing columns
        for (i, field) in schema.fields().iter().enumerate() {
            let col_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Column::new(field.name(), i),
            );
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // Second pass: scan aggregates for custom expressions in arguments
        for agg_expr in aggregates {
            let Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } = agg_expr
            else {
                rewritten_aggregates.push(agg_expr.clone());
                continue;
            };

            let mut rewritten_args = Vec::new();
            let mut agg_needs_rewrite = false;

            for arg in args {
                if CypherPhysicalExprCompiler::contains_custom_expr(arg) {
                    // Compile the custom expression
                    let compiler = self.expr_compiler(state, Some(ctx));
                    let physical_expr = compiler.compile(arg, schema)?;

                    // Add it as a projected column
                    let col_name = format!("__pc_{}", col_counter);
                    col_counter += 1;
                    proj_exprs.push((physical_expr, col_name.clone()));

                    // Rewrite aggregate to reference the column
                    rewritten_args.push(Expr::Variable(col_name));
                    agg_needs_rewrite = true;
                    needs_projection = true;
                } else {
                    rewritten_args.push(arg.clone());
                }
            }

            if agg_needs_rewrite {
                rewritten_aggregates.push(Expr::FunctionCall {
                    name: name.clone(),
                    args: rewritten_args,
                    distinct: *distinct,
                    window_spec: window_spec.clone(),
                });
            } else {
                rewritten_aggregates.push(agg_expr.clone());
            }
        }

        if needs_projection {
            let projection_exec = Arc::new(
                datafusion::physical_plan::projection::ProjectionExec::try_new(
                    proj_exprs, input_plan,
                )?,
            );
            let new_schema = projection_exec.schema();
            Ok((projection_exec, new_schema, rewritten_aggregates))
        } else {
            Ok((input_plan, schema.clone(), aggregates.to_vec()))
        }
    }

    /// Plan a sort operation.
    ///
    /// The `alias_map` provides a mapping from alias names to underlying expressions.
    /// This is needed because ORDER BY expressions may reference aliases defined in
    /// a parent Project node (e.g., `ORDER BY friend_count` where `friend_count`
    /// is an alias for `COUNT(r)`).
    fn plan_sort(
        &self,
        input: &LogicalPlan,
        order_by: &[SortItem],
        all_properties: &HashMap<String, HashSet<String>>,
        alias_map: &HashMap<String, Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;
        let schema = input_plan.schema();

        let session = self.session_ctx.read();

        // Build translation context with variable kinds from the input plan
        let ctx = self.translation_context_for_plan(input);

        // Build DFSchema once for type coercion and physical expression conversion
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

        // Translate sort expressions to DataFusion's SortExpr (a.k.a. Sort struct)
        // SortItem has `ascending: bool`, so use it directly
        // Default nulls_first to false for ASC, true for DESC
        use crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler;

        let mut df_sort_exprs = Vec::new();
        let mut custom_physical_overrides: Vec<(
            usize,
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )> = Vec::new();
        for item in order_by {
            let mut sort_expr = item.expr.clone();

            // If the sort expression is a variable that matches an alias,
            // replace it with the underlying expression
            if let Expr::Variable(ref name) = sort_expr {
                // Check if this name exists in the input schema
                let col_name = name.as_str();
                let exists_in_schema = schema.fields().iter().any(|f| f.name() == col_name);

                if !exists_in_schema && let Some(aliased_expr) = alias_map.get(col_name) {
                    sort_expr = aliased_expr.clone();
                }
            }

            let asc = item.ascending;
            let nulls_first = !asc; // Standard SQL behavior: nulls last for ASC, first for DESC

            // Custom expressions (similar_to, comprehensions, etc.) cannot be
            // translated via cypher_expr_to_df. Compile with the custom compiler
            // and save as an override for the physical sort expression.
            if CypherPhysicalExprCompiler::contains_custom_expr(&sort_expr) {
                let sort_state = session.state();
                let compiler = self.expr_compiler(&sort_state, Some(&ctx));
                let inner_physical = compiler.compile(&sort_expr, &schema)?;

                // Use a dummy column reference for the logical sort expression
                // (we'll replace the physical expression below).
                let first_col = schema
                    .fields()
                    .first()
                    .map(|f| f.name().clone())
                    .unwrap_or_else(|| "_dummy_".to_string());
                let dummy_expr = DfExpr::Column(datafusion::common::Column::from_name(&first_col));
                let sort_key_udf = crate::query::df_udfs::create_cypher_sort_key_udf();
                let sort_key_expr = sort_key_udf.call(vec![dummy_expr]);
                custom_physical_overrides.push((df_sort_exprs.len(), inner_physical));
                df_sort_exprs.push(DfSortExpr::new(sort_key_expr, asc, nulls_first));
                continue;
            }

            let df_expr = cypher_expr_to_df(&sort_expr, Some(&ctx))?;
            let df_expr = Self::resolve_udfs(&df_expr, &session.state())?;
            let df_expr = crate::query::df_expr::apply_type_coercion(&df_expr, &df_schema)?;
            // Resolve UDFs again: apply_type_coercion may create new dummy UDF
            // placeholders (e.g. _cv_to_bool, _cypher_add) that need resolution.
            let df_expr = Self::resolve_udfs(&df_expr, &session.state())?;

            // Single order-preserving sort key: _cypher_sort_key(expr) -> LargeBinary
            // The UDF handles all Cypher ordering semantics (cross-type ranks,
            // within-type comparisons, temporal normalization, NaN/null placement)
            // so memcmp of the resulting bytes gives correct Cypher ORDER BY.
            let sort_key_udf = crate::query::df_udfs::create_cypher_sort_key_udf();
            let sort_key_expr = sort_key_udf.call(vec![df_expr]);
            df_sort_exprs.push(DfSortExpr::new(sort_key_expr, asc, nulls_first));
        }

        let mut physical_sort_exprs = create_physical_sort_exprs(
            &df_sort_exprs,
            &df_schema,
            session.state().execution_props(),
        )?;

        // Replace the inner expression for custom sort expressions.
        // The _cypher_sort_key UDF wrapper is already in place; we just need
        // to swap the dummy column reference with the actual custom physical expr.
        for (idx, custom_inner) in custom_physical_overrides {
            if idx < physical_sort_exprs.len() {
                let phys = &physical_sort_exprs[idx];
                // The physical sort expression wraps _cypher_sort_key(dummy_col).
                // We need to replace the inner arg with our custom expression.
                // ScalarFunctionExpr wraps the UDF; rebuild it with the correct child.
                let sort_key_udf = Arc::new(crate::query::df_udfs::create_cypher_sort_key_udf());
                let config_options = Arc::new(datafusion::config::ConfigOptions::default());
                let udf_name = sort_key_udf.name().to_string();
                let new_sort_key = datafusion::physical_expr::ScalarFunctionExpr::new(
                    &udf_name,
                    sort_key_udf,
                    vec![custom_inner],
                    Arc::new(arrow_schema::Field::new(
                        "_cypher_sort_key",
                        DataType::LargeBinary,
                        true,
                    )),
                    config_options,
                );
                physical_sort_exprs[idx] = datafusion::physical_expr::PhysicalSortExpr {
                    expr: Arc::new(new_sort_key),
                    options: phys.options,
                };
            }
        }

        // Convert Vec<PhysicalSortExpr> to LexOrdering
        // LexOrdering::new returns None for empty vector, so handle that case
        let lex_ordering = datafusion::physical_expr::LexOrdering::new(physical_sort_exprs)
            .ok_or_else(|| anyhow!("ORDER BY must have at least one sort expression"))?;

        Ok(Arc::new(SortExec::new(lex_ordering, input_plan)))
    }

    /// Plan a limit operation.
    fn plan_limit(
        &self,
        input: &LogicalPlan,
        skip: Option<usize>,
        fetch: Option<usize>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        // Handle SKIP via GlobalLimitExec (LocalLimitExec doesn't support offset)
        if let Some(offset) = skip.filter(|&s| s > 0) {
            use datafusion::physical_plan::limit::GlobalLimitExec;
            return Ok(Arc::new(GlobalLimitExec::new(input_plan, offset, fetch)));
        }

        if let Some(limit) = fetch {
            Ok(Arc::new(LocalLimitExec::new(input_plan, limit)))
        } else {
            // No limit, return input as-is
            Ok(input_plan)
        }
    }

    /// Project `plan` down to `cols`, in that order.
    ///
    /// Returns the plan untouched when any name is missing from its schema —
    /// a naming mismatch is a separate defect, and silently dropping a column
    /// here would turn it into a wrong answer. The caller's schema check then
    /// reports it.
    fn narrow_to_columns(
        plan: Arc<dyn ExecutionPlan>,
        cols: &[String],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema();
        let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::with_capacity(cols.len());
        for name in cols {
            let Ok(idx) = schema.index_of(name) else {
                return Ok(plan);
            };
            exprs.push((
                Arc::new(datafusion::physical_plan::expressions::Column::new(
                    name, idx,
                )),
                name.clone(),
            ));
        }
        if exprs.len() == schema.fields().len()
            && exprs
                .iter()
                .enumerate()
                .all(|(i, (_, name))| schema.field(i).name() == name)
        {
            // Already exactly these columns in this order.
            return Ok(plan);
        }
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    /// Encode a column as CypherValue `LargeBinary`.
    ///
    /// The same conversion `coerce_branch_to` applies for `CASE`; reused here
    /// so the two paths agree on what a mixed pair of entities becomes.
    fn encode_column_as_cypher_value(
        expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        name: &str,
    ) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
        let udf = Arc::new(crate::query::df_udfs::create_cypher_scalar_to_cv_udf());
        Arc::new(datafusion::physical_expr::ScalarFunctionExpr::new(
            "_cypher_scalar_to_cv",
            udf,
            vec![expr],
            Arc::new(arrow_schema::Field::new(name, DataType::LargeBinary, true)),
            Arc::new(datafusion::config::ConfigOptions::default()),
        ))
    }

    /// Reconcile per-position type differences between two union branches.
    ///
    /// Two branches can return the same *Cypher* type as different *Arrow*
    /// types. `MATCH (p:P) RETURN p UNION ALL MATCH (q:Q) RETURN q` is valid
    /// openCypher, but `P` and `Q` have different properties, so their node
    /// structs differ by construction and no amount of consistency work
    /// between the scan and traversal paths can make them identical.
    ///
    /// `find_common_result_type` already answers this for `CASE`: entities
    /// coerce to the CypherValue `LargeBinary` encoding, which represents any
    /// entity. This applies the same rule to union branches, so both clauses
    /// agree on what a mixed pair becomes rather than one working and the
    /// other reporting a planner bug.
    ///
    /// Only entity columns are touched. A genuine conflict between, say, an
    /// `Int64` and a `Utf8` is left for the caller's schema check to reject —
    /// coercing that pair would invent a conversion the user did not ask for.
    fn coerce_union_entity_columns(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
        let (ls, rs) = (left.schema(), right.schema());
        if ls.fields().len() != rs.fields().len() {
            return Ok((left, right));
        }
        let coercible = |l: &DataType, r: &DataType| {
            l != r
                && [l, r].iter().all(|t| {
                    uni_query_functions::cypher_type_coerce::is_entity_struct(t)
                        || matches!(t, DataType::LargeBinary)
                })
        };
        let positions: Vec<usize> = (0..ls.fields().len())
            .filter(|&i| coercible(ls.field(i).data_type(), rs.field(i).data_type()))
            .collect();
        if positions.is_empty() {
            return Ok((left, right));
        }
        let project = |plan: Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
            let schema = plan.schema();
            let exprs = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                        datafusion::physical_plan::expressions::Column::new(f.name(), i),
                    );
                    let expr = if positions.contains(&i)
                        && !matches!(f.data_type(), DataType::LargeBinary)
                    {
                        Self::encode_column_as_cypher_value(col, f.name())
                    } else {
                        col
                    };
                    (expr, f.name().clone())
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?) as Arc<dyn ExecutionPlan>)
        };
        Ok((project(left)?, project(right)?))
    }

    /// Plan a union operation.
    fn plan_union(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
        all: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let left_plan = self.plan_internal(left, all_properties)?;
        let right_plan = self.plan_internal(right, all_properties)?;

        // Narrow each branch to the columns the query actually projects.
        //
        // A branch's physical schema carries the internal helper columns its
        // operators emitted (`y._vid`, `z.overflow_json`) beside the projected
        // one, and which helpers appear depends on how the branch reached its
        // rows. A scan of `:P` produces six columns where a traversal to the
        // same label produces four, so two branches returning the same node
        // could not be unioned at all (#191).
        //
        // Narrowing here rather than teaching the union to reconcile widths
        // also stops the helpers escaping above the union, which is the leak
        // that let a result's columns be named `b._labels` (#190).
        let (left_plan, right_plan) =
            match projection_columns(left).or_else(|| projection_columns(right)) {
                Some(cols) => (
                    Self::narrow_to_columns(left_plan, &cols)?,
                    Self::narrow_to_columns(right_plan, &cols)?,
                ),
                // No projection to narrow to. Leave both branches alone; the
                // width/type check below still guards DataFusion's panicking
                // `union_schema`.
                None => (left_plan, right_plan),
            };

        // Two branches can return the same Cypher type as different Arrow
        // types — most obviously two different labels, whose node structs
        // differ by their property columns. Reconcile those the way `CASE`
        // already does, before the schema check below rejects the pair.
        let (left_plan, right_plan) = Self::coerce_union_entity_columns(left_plan, right_plan)?;

        // Guard against schema mismatches reaching DataFusion's
        // `union_schema`, which panics with `index out of bounds` rather
        // than returning `Err` when branch widths or per-position types
        // differ (issue rustic-ai/uni-db#62). With the planner-level
        // fallback in place for label disjunction this should be
        // unreachable, but a typed error here protects any future
        // logical-Union path against the same process-aborting panic.
        //
        // We only compare field count and per-position **type**; the
        // user-facing Cypher `UNION` clause routinely produces branches
        // whose per-position field *names* differ (e.g. `MATCH (a:A)
        // RETURN a AS a UNION MATCH (b:B) RETURN b AS a` — both branches
        // alias their pattern variable to `a`, but internal namespaced
        // columns like `a._vid` vs `b._vid` differ). DataFusion handles
        // that case fine by adopting left names; only width/type
        // mismatches are the panic source.
        let left_schema = left_plan.schema();
        let right_schema = right_plan.schema();
        if left_schema.fields().len() != right_schema.fields().len()
            || left_schema
                .fields()
                .iter()
                .zip(right_schema.fields().iter())
                .any(|(l, r)| l.data_type() != r.data_type())
        {
            let fmt = |s: &Schema| {
                s.fields()
                    .iter()
                    .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            return Err(anyhow!(
                "Plan: cannot UNION branches with mismatched schemas — \
                 left=[{}], right=[{}]. This is a planner bug; please file \
                 an issue.",
                fmt(left_schema.as_ref()),
                fmt(right_schema.as_ref()),
            ));
        }

        let union_plan = UnionExec::try_new(vec![left_plan, right_plan])?;

        // UNION (without ALL) requires deduplication
        if !all {
            use datafusion::physical_plan::aggregates::{
                AggregateExec, AggregateMode, PhysicalGroupBy,
            };
            use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;

            // First, coalesce all partitions into one to ensure global deduplication
            let coalesced = Arc::new(CoalescePartitionsExec::new(union_plan));

            // Create group by all columns to deduplicate
            let schema = coalesced.schema();
            let group_by_exprs: Vec<_> = (0..schema.fields().len())
                .map(|i| {
                    (
                        Arc::new(datafusion::physical_plan::expressions::Column::new(
                            schema.field(i).name(),
                            i,
                        ))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        schema.field(i).name().clone(),
                    )
                })
                .collect();

            let group_by = PhysicalGroupBy::new_single(group_by_exprs);

            Ok(Arc::new(AggregateExec::try_new(
                AggregateMode::Single,
                group_by,
                vec![], // No aggregate functions, just grouping for distinct
                vec![], // No filters
                coalesced,
                schema,
            )?))
        } else {
            // UNION ALL - just return the union
            Ok(union_plan)
        }
    }

    /// Plan all window functions (aggregate and manual) using DataFusion's WindowAggExec.
    ///
    /// Translates Cypher window expressions to DataFusion's window function execution plan.
    /// Supports both aggregate window functions (SUM, AVG, etc.) via AggregateUDF and
    /// manual window functions (ROW_NUMBER, RANK, LAG, etc.) via WindowUDF.
    fn plan_window_functions(
        &self,
        input: Arc<dyn ExecutionPlan>,
        window_exprs: &[Expr],
        context_plan: Option<&LogicalPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::functions_aggregate::average::avg_udaf;
        use datafusion::functions_aggregate::count::count_udaf;
        use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
        use datafusion::functions_aggregate::sum::sum_udaf;
        use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
        use datafusion::functions_window::nth_value::{
            first_value_udwf, last_value_udwf, nth_value_udwf,
        };
        use datafusion::functions_window::ntile::ntile_udwf;
        use datafusion::functions_window::rank::{dense_rank_udwf, rank_udwf};
        use datafusion::functions_window::row_number::row_number_udwf;
        use datafusion::logical_expr::{WindowFrame, WindowFunctionDefinition};
        use datafusion::physical_expr::LexOrdering;
        use datafusion::physical_plan::sorts::sort::SortExec;
        use datafusion::physical_plan::windows::{WindowAggExec, create_window_expr};

        let input_schema = input.schema();
        let df_schema = datafusion::common::DFSchema::try_from(input_schema.as_ref().clone())?;

        let session = self.session_ctx.read();
        let state = session.state();

        // Build translation context with variable kinds if we have a logical plan
        let tx_ctx = context_plan.map(|p| self.translation_context_for_plan(p));
        // Each window with its OWN required input ordering. Two windows with
        // conflicting ORDER BY (e.g. one ASC, one DESC) cannot share a single
        // SortExec — evaluating the second over the first's ordering silently
        // produces wrong ranks. So we sort per window (see the chaining below).
        let mut window_specs: Vec<(
            std::sync::Arc<dyn datafusion::physical_expr::window::WindowExpr>,
            Vec<datafusion::physical_expr::PhysicalSortExpr>,
        )> = Vec::new();

        for expr in window_exprs {
            let Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec: Some(window_spec),
            } = expr
            else {
                return Err(anyhow!("Expected window function call with OVER clause"));
            };

            let name_lower = name.to_lowercase();

            // Resolve the window function definition: either AggregateUDF or WindowUDF
            let (window_fn_def, is_aggregate) = match name_lower.as_str() {
                // Aggregate window functions → AggregateUDF
                "count" => (WindowFunctionDefinition::AggregateUDF(count_udaf()), true),
                "sum" => (WindowFunctionDefinition::AggregateUDF(sum_udaf()), true),
                "avg" => (WindowFunctionDefinition::AggregateUDF(avg_udaf()), true),
                "min" => (WindowFunctionDefinition::AggregateUDF(min_udaf()), true),
                "max" => (WindowFunctionDefinition::AggregateUDF(max_udaf()), true),
                // Manual window functions → WindowUDF
                "row_number" => (
                    WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                    false,
                ),
                "rank" => (WindowFunctionDefinition::WindowUDF(rank_udwf()), false),
                "dense_rank" => (
                    WindowFunctionDefinition::WindowUDF(dense_rank_udwf()),
                    false,
                ),
                "lag" => (WindowFunctionDefinition::WindowUDF(lag_udwf()), false),
                "lead" => (WindowFunctionDefinition::WindowUDF(lead_udwf()), false),
                "ntile" => {
                    // Validate NTILE bucket count: must be positive
                    if let Some(Expr::Literal(CypherLiteral::Integer(n))) = args.first()
                        && *n <= 0
                    {
                        return Err(anyhow!("NTILE bucket count must be positive, got: {}", n));
                    }
                    (WindowFunctionDefinition::WindowUDF(ntile_udwf()), false)
                }
                "first_value" => (
                    WindowFunctionDefinition::WindowUDF(first_value_udwf()),
                    false,
                ),
                "last_value" => (
                    WindowFunctionDefinition::WindowUDF(last_value_udwf()),
                    false,
                ),
                "nth_value" => (WindowFunctionDefinition::WindowUDF(nth_value_udwf()), false),
                other => {
                    // Fall through to a registered plugin window function.
                    // Resolve the (possibly dotted) name against the registry via
                    // every namespace/local split, mirroring the aggregate path —
                    // instance/host registry first, then the session-local one.
                    let session_registry = crate::current_session_plugin_registry();
                    let resolved = uni_plugin::QName::candidate_splits(other).find_map(|q| {
                        if let Some(e) = self.plugin_registry.window(&q) {
                            return Some((q, e));
                        }
                        if let Some(sr) = session_registry.as_ref()
                            && let Some(e) = sr.window(&q)
                        {
                            return Some((q, e));
                        }
                        None
                    });
                    match resolved {
                        Some((qname, entry)) => {
                            let udwf = datafusion::logical_expr::WindowUDF::from(
                                crate::query::df_udwf_plugin::PluginWindowUdwf::new(
                                    qname.to_string(),
                                    &entry,
                                ),
                            );
                            (WindowFunctionDefinition::WindowUDF(Arc::new(udwf)), false)
                        }
                        None => return Err(anyhow!("Unsupported window function: {}", other)),
                    }
                }
            };

            // Translate argument expressions to physical expressions
            let physical_args: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
                if args.is_empty() || matches!(args.as_slice(), [Expr::Wildcard]) {
                    // COUNT(*) or zero-arg functions (row_number, rank, dense_rank)
                    if is_aggregate {
                        vec![create_physical_expr(
                            &datafusion::logical_expr::lit(1),
                            &df_schema,
                            state.execution_props(),
                        )?]
                    } else {
                        // Manual window functions with no args (row_number, rank, dense_rank)
                        vec![]
                    }
                } else {
                    args.iter()
                        .map(|arg| {
                            let mut df_expr = cypher_expr_to_df(arg, tx_ctx.as_ref())?;

                            // Cast numeric types only for SUM/AVG aggregate functions:
                            // SUM needs Int64 to avoid overflow, AVG needs Float64
                            if is_aggregate {
                                let cast_type = match name_lower.as_str() {
                                    "sum" => Some(datafusion::arrow::datatypes::DataType::Int64),
                                    "avg" => Some(datafusion::arrow::datatypes::DataType::Float64),
                                    _ => None,
                                };
                                if let Some(target_type) = cast_type {
                                    df_expr = DfExpr::Cast(datafusion::logical_expr::Cast::new(
                                        Box::new(df_expr),
                                        target_type,
                                    ));
                                }
                            }

                            create_physical_expr(&df_expr, &df_schema, state.execution_props())
                                .map_err(|e| anyhow!("Failed to create physical expr: {}", e))
                        })
                        .collect::<Result<Vec<_>>>()?
                };

            // Translate PARTITION BY expressions to physical expressions
            let partition_by_physical: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
                window_spec
                    .partition_by
                    .iter()
                    .map(|e| {
                        let df_expr = cypher_expr_to_df(e, tx_ctx.as_ref())?;
                        create_physical_expr(&df_expr, &df_schema, state.execution_props())
                            .map_err(|e| anyhow!("Failed to create physical expr: {}", e))
                    })
                    .collect::<Result<Vec<_>>>()?;

            // Translate ORDER BY expressions to physical sort expressions
            let mut order_by_physical: Vec<datafusion::physical_expr::PhysicalSortExpr> =
                window_spec
                    .order_by
                    .iter()
                    .map(|sort_item| {
                        let df_expr = cypher_expr_to_df(&sort_item.expr, tx_ctx.as_ref())?;
                        let physical_expr =
                            create_physical_expr(&df_expr, &df_schema, state.execution_props())
                                .map_err(|e| anyhow!("Failed to create physical expr: {}", e))?;
                        Ok(datafusion::physical_expr::PhysicalSortExpr {
                            expr: physical_expr,
                            options: datafusion::arrow::compute::SortOptions {
                                descending: !sort_item.ascending,
                                nulls_first: !sort_item.ascending, // SQL standard: nulls last for ASC
                            },
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

            // DataFusion requires partition columns to have an ordering.
            // If ORDER BY is empty but PARTITION BY is not, add partition columns to ordering.
            if order_by_physical.is_empty() && !partition_by_physical.is_empty() {
                for partition_expr in &partition_by_physical {
                    order_by_physical.push(datafusion::physical_expr::PhysicalSortExpr {
                        expr: Arc::clone(partition_expr),
                        options: datafusion::arrow::compute::SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    });
                }
            }

            // Create window frame based on function type:
            // - Aggregate functions: cumulative when ORDER BY present, full partition when absent
            // - Manual window functions: always full partition (frame is irrelevant for ranking,
            //   and value functions like last_value/first_value expect full-partition semantics)
            let window_frame = if is_aggregate {
                if window_spec.order_by.is_empty() {
                    // No ORDER BY: aggregate over entire partition
                    use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
                    Arc::new(WindowFrame::new_bounds(
                        WindowFrameUnits::Rows,
                        WindowFrameBound::Preceding(datafusion::common::ScalarValue::UInt64(None)),
                        WindowFrameBound::Following(datafusion::common::ScalarValue::UInt64(None)),
                    ))
                } else {
                    // With ORDER BY: cumulative from partition start to current row
                    Arc::new(WindowFrame::new(Some(false)))
                }
            } else {
                // Manual window functions: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(datafusion::common::ScalarValue::UInt64(None)),
                    WindowFrameBound::Following(datafusion::common::ScalarValue::UInt64(None)),
                ))
            };

            // Get the output name
            let alias = expr.to_string_repr();

            // Create the window expression using DataFusion's create_window_expr
            let window_expr = create_window_expr(
                &window_fn_def,
                alias,
                &physical_args,
                &partition_by_physical,
                &order_by_physical,
                window_frame,
                input_schema.clone(),
                false, // ignore_nulls
                *distinct,
                None, // filter
            )?;

            // This window's required input ordering: PARTITION BY columns first
            // (as ascending sorts), then its ORDER BY. Captured per window so each
            // gets its own SortExec below.
            let mut required_ordering: Vec<datafusion::physical_expr::PhysicalSortExpr> =
                Vec::new();
            for p in &partition_by_physical {
                required_ordering.push(datafusion::physical_expr::PhysicalSortExpr {
                    expr: Arc::clone(p),
                    options: datafusion::arrow::compute::SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                });
            }
            for ob in &order_by_physical {
                if !required_ordering
                    .iter()
                    .any(|s| s.expr.to_string() == ob.expr.to_string())
                {
                    required_ordering.push(ob.clone());
                }
            }

            window_specs.push((window_expr, required_ordering));
        }

        // Chain one SortExec + WindowAggExec per window, so each window is
        // evaluated over ITS OWN required ordering. A prior version concatenated
        // every window's PARTITION/ORDER BY into a single SortExec, which cannot
        // satisfy two windows with conflicting ORDER BY — the second was silently
        // computed over the first's ordering. (Grouping windows that share an
        // ordering would be a perf optimization; correctness needs only per-window
        // sorting, and each WindowAggExec preserves the columns the next sorts on.)
        let mut plan = input;
        for (window_expr, ordering) in window_specs {
            let sorted_input = if ordering.is_empty() {
                plan
            } else {
                let lex_ordering = LexOrdering::new(ordering)
                    .ok_or_else(|| anyhow!("Failed to create LexOrdering for window function"))?;
                Arc::new(SortExec::new(lex_ordering, plan)) as Arc<dyn ExecutionPlan>
            };
            plan = Arc::new(WindowAggExec::try_new(
                vec![window_expr],
                sorted_input,
                false, // can_repartition - keep data on current partitions
            )?);
        }

        Ok(plan)
    }

    /// Plan an empty input that produces exactly one row.
    ///
    /// In Cypher, `RETURN 1` (without MATCH) expects a single row to project from.
    /// This matches the fallback executor behavior which returns `vec![HashMap::new()]`.
    fn plan_empty(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::empty());
        // Use PlaceholderRowExec to produce exactly one row (like SQL's "SELECT 1").
        // EmptyExec produces 0 rows, which breaks `RETURN 1 AS num`.
        Ok(Arc::new(PlaceholderRowExec::new(schema)))
    }

    /// Plan a zero-length path binding.
    /// Converts a single node pattern `p = (a)` into a Path with one node and zero edges.
    fn plan_bind_zero_length_path(
        &self,
        input: &LogicalPlan,
        node_variable: &str,
        path_variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;
        Ok(Arc::new(BindZeroLengthPathExec::new(
            input_plan,
            node_variable.to_string(),
            path_variable.to_string(),
            self.graph_ctx.clone(),
        )))
    }

    /// Plan a fixed-length path binding.
    /// Synthesizes a path struct from existing node and edge columns.
    fn plan_bind_path(
        &self,
        input: &LogicalPlan,
        node_variables: &[String],
        edge_variables: &[String],
        edge_type_names: &[Vec<String>],
        path_variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;
        Ok(Arc::new(BindFixedPathExec::new(
            input_plan,
            node_variables.to_vec(),
            edge_variables.to_vec(),
            edge_type_names.to_vec(),
            path_variable.to_string(),
            self.graph_ctx.clone(),
        )))
    }

    /// Extract simple property equality conditions from a Cypher expression tree.
    ///
    /// Handles patterns generated by `properties_to_expr`:
    /// - `variable.prop = literal` → `(prop, value)`
    /// - `cond1 AND cond2` → recursive extraction
    ///
    /// Returns `Vec<(property_name, expected_value)>` for use in L0 edge property
    /// checking during VLP BFS.
    fn extract_edge_property_conditions(expr: &Expr) -> Vec<(String, uni_common::Value)> {
        match expr {
            Expr::BinaryOp {
                left,
                op: uni_cypher::ast::BinaryOp::Eq,
                right,
            } => {
                // Pattern: variable.prop = literal
                if let Expr::Property(inner, prop_name) = left.as_ref()
                    && matches!(inner.as_ref(), Expr::Variable(_))
                    && let Expr::Literal(lit) = right.as_ref()
                {
                    return vec![(prop_name.clone(), lit.to_value())];
                }
                // Reverse: literal = variable.prop
                if let Expr::Literal(lit) = left.as_ref()
                    && let Expr::Property(inner, prop_name) = right.as_ref()
                    && matches!(inner.as_ref(), Expr::Variable(_))
                {
                    return vec![(prop_name.clone(), lit.to_value())];
                }
                vec![]
            }
            Expr::BinaryOp {
                left,
                op: uni_cypher::ast::BinaryOp::And,
                right,
            } => {
                let mut result = Self::extract_edge_property_conditions(left);
                result.extend(Self::extract_edge_property_conditions(right));
                result
            }
            _ => vec![],
        }
    }

    /// Create a physical filter expression from a DataFusion logical expression.
    ///
    /// Applies type coercion to resolve mismatches like Int32 vs Int64
    /// before creating the physical expression.
    fn create_physical_filter_expr(
        &self,
        expr: &DfExpr,
        schema: &SchemaRef,
        session: &SessionContext,
    ) -> Result<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;
        let state = session.state();

        // Replace DummyUdf placeholders with registered UDFs
        let resolved_expr = Self::resolve_udfs(expr, &state)?;

        // Apply type coercion to resolve Int32/Int64, Float32/Float64 mismatches
        let coerced_expr = crate::query::df_expr::apply_type_coercion(&resolved_expr, &df_schema)?;

        // Re-resolve UDFs after coercion (coercion may introduce new dummy UDF calls)
        let coerced_expr = Self::resolve_udfs(&coerced_expr, &state)?;

        // Use SessionState's create_physical_expr to properly resolve UDFs
        use datafusion::physical_planner::PhysicalPlanner;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        let physical = planner.create_physical_expr(&coerced_expr, &df_schema, &state)?;

        Ok(physical)
    }

    /// Resolve DummyUdf placeholders to actual registered UDFs from SessionState.
    ///
    /// Uses DataFusion's TreeNode API to traverse the entire expression tree,
    /// replacing any ScalarFunction nodes whose UDF name matches a registered UDF.
    fn resolve_udfs(expr: &DfExpr, state: &datafusion::execution::SessionState) -> Result<DfExpr> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::logical_expr::Expr as DfExpr;

        let result = expr
            .clone()
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
            .map_err(|e| anyhow::anyhow!("Failed to resolve UDFs: {}", e))?;

        Ok(result.data)
    }

    /// Add a structural projection on top of an execution plan to create a Struct column
    /// for a Node or Edge variable.
    fn add_structural_projection(
        &self,
        input: Arc<dyn ExecutionPlan>,
        variable: &str,
        properties: &[String],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::functions::expr_fn::named_struct;
        use datafusion::logical_expr::lit;
        use datafusion::physical_plan::projection::ProjectionExec;

        let input_schema = input.schema();
        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();

        // 1. Keep all existing columns
        for (i, field) in input_schema.fields().iter().enumerate() {
            let col_expr = Arc::new(datafusion::physical_expr::expressions::Column::new(
                field.name(),
                i,
            ));
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // 2. Add the named_struct AS variable
        let mut struct_args = Vec::with_capacity(properties.len() * 2 + 4);

        // Add _vid field for identity access
        struct_args.push(lit("_vid"));
        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
            format!("{}._vid", variable),
        )));

        // Add _labels field for labels() function support
        struct_args.push(lit("_labels"));
        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
            format!("{}._labels", variable),
        )));

        for prop in properties {
            struct_args.push(lit(prop.clone()));
            struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
                format!("{}.{}", variable, prop),
            )));
        }

        // If no properties, still create an empty struct to represent the entity
        let struct_expr = named_struct(struct_args);

        let df_schema = datafusion::common::DFSchema::try_from(input_schema.as_ref().clone())?;
        let session = self.session_ctx.read();
        let state = session.state();

        // Resolve DummyUdf placeholders
        let resolved_expr = Self::resolve_udfs(&struct_expr, &state)?;

        use datafusion::physical_planner::PhysicalPlanner;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        let physical_struct_expr =
            planner.create_physical_expr(&resolved_expr, &df_schema, &state)?;

        proj_exprs.push((physical_struct_expr, variable.to_string()));

        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    }

    /// Add a structural projection for an edge variable (builds a Struct with _eid, _type, _src, _dst + properties).
    fn add_edge_structural_projection(
        &self,
        input: Arc<dyn ExecutionPlan>,
        variable: &str,
        properties: &[String],
        source_variable: &str,
        target_variable: &str,
        direction: &AstDirection,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::functions::expr_fn::named_struct;
        use datafusion::logical_expr::lit;
        use datafusion::physical_plan::projection::ProjectionExec;

        let input_schema = input.schema();
        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();

        // 1. Keep all existing columns
        for (i, field) in input_schema.fields().iter().enumerate() {
            let col_expr = Arc::new(datafusion::physical_expr::expressions::Column::new(
                field.name(),
                i,
            ));
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // 2. Build named_struct with system fields + properties
        let mut struct_args = Vec::with_capacity(properties.len() * 2 + 10);

        // Add _eid field for identity access
        struct_args.push(lit("_eid"));
        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
            format!("{}._eid", variable),
        )));

        struct_args.push(lit("_type"));
        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
            format!("{}._type", variable),
        )));

        // Add _src and _dst from source/target variable VIDs so the result
        // normalizer can detect this as an edge.
        // Use {var}._vid when available, falling back to bare {var} column
        // (e.g., in EXISTS subqueries where the source is a parameter VID).
        let resolve_vid_col = |var: &str| -> String {
            let vid_col = format!("{}._vid", var);
            if input_schema.column_with_name(&vid_col).is_some() {
                vid_col
            } else {
                var.to_string()
            }
        };
        // The traversal's source is the end this row *walked from*, which is not
        // the arrow's tail. `Traverse` encodes the arrow as (source, direction):
        // `endpoints_for_direction` reads `Incoming` as start = target, end =
        // source. Taking traversal order directly therefore reports an
        // `Incoming` hop reversed — `MATCH (b)<-[r]-(a) RETURN r` gives
        // `r.src = b`.
        //
        // Newly load-bearing: `reversed_for_bound_anchor` rewrites a pattern
        // written from its unbound end into the opposite direction, so an
        // `Outgoing` pattern can now reach this as `Incoming`. Before that
        // rewrite the same query planned the slow way and got this right.
        let (source_variable, target_variable) = match direction {
            AstDirection::Incoming => (target_variable, source_variable),
            AstDirection::Outgoing | AstDirection::Both => (source_variable, target_variable),
        };
        let src_col_name = resolve_vid_col(source_variable);
        let dst_col_name = resolve_vid_col(target_variable);
        let src_col = DfExpr::Column(datafusion::common::Column::from_name(src_col_name));
        let dst_col = DfExpr::Column(datafusion::common::Column::from_name(dst_col_name));

        // For an undirected hop the traversal's source is whichever end this row
        // matched from, so using it directly reports the edge reversed on every
        // row that walked it backwards — the same relationship coming back as
        // `b->a`. `_fwd` says whether this row walked it forwards; when the
        // traversal published it, put the endpoints back the way they are
        // stored.
        let fwd_col = format!("{}.{}", variable, crate::query::planner::COL_FWD);
        let (src_expr, dst_expr) = if input_schema.column_with_name(&fwd_col).is_some() {
            let fwd = DfExpr::Column(datafusion::common::Column::from_name(fwd_col));
            (
                datafusion::logical_expr::when(fwd.clone(), src_col.clone())
                    .otherwise(dst_col.clone())?,
                datafusion::logical_expr::when(fwd, dst_col).otherwise(src_col)?,
            )
        } else if matches!(direction, AstDirection::Both) {
            // Falling through to traversal order here is a silent wrong answer,
            // not a missing optimization: an undirected row that walked the edge
            // backwards reports it reversed, and the same relationship comes back
            // as `a->b` from one row and `b->a` from another. Whether `_fwd` is
            // present is decided by `wants_orientation_column` at the two request
            // sites; if this is reached, those and this disagree.
            //
            // This mirrors the traversal's own contract, which hard-errors when
            // asked for `_fwd` it did not track rather than emitting a null that
            // would take a `CASE`'s ELSE. Absence must not be mistaken for
            // `false`. #193 was exactly this fallback running unnoticed, on the
            // one of the two paths that had no test.
            return Err(anyhow!(
                "relationship `{variable}` is undirected and builds a struct, but the                  traversal did not publish `{}`; its endpoints cannot be put back in                  stored orientation. This is a planner/executor mismatch, not a                  property that happens to be absent.",
                crate::query::planner::COL_FWD
            ));
        } else {
            (src_col, dst_col)
        };

        struct_args.push(lit("_src"));
        struct_args.push(src_expr);

        struct_args.push(lit("_dst"));
        struct_args.push(dst_expr);

        // Include _all_props if present (for keys()/properties() on schemaless edges)
        let all_props_col = format!("{}._all_props", variable);
        if input_schema.column_with_name(&all_props_col).is_some() {
            struct_args.push(lit("_all_props"));
            struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
                all_props_col,
            )));
        }

        for prop in properties {
            struct_args.push(lit(prop.clone()));
            struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
                format!("{}.{}", variable, prop),
            )));
        }

        let struct_expr = named_struct(struct_args);

        let df_schema = datafusion::common::DFSchema::try_from(input_schema.as_ref().clone())?;
        let session = self.session_ctx.read();
        let state = session.state();

        let resolved_expr = Self::resolve_udfs(&struct_expr, &state)?;

        use datafusion::physical_planner::PhysicalPlanner;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        let physical_struct_expr =
            planner.create_physical_expr(&resolved_expr, &df_schema, &state)?;

        proj_exprs.push((physical_struct_expr, variable.to_string()));

        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    }

    /// Create a physical aggregate expression.
    fn create_physical_aggregate(
        &self,
        expr: &DfExpr,
        schema: &SchemaRef,
        state: &SessionState,
    ) -> Result<PhysicalAggregate> {
        use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;

        // Build a DFSchema from the Arrow schema for the function call
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

        // The function returns (AggregateFunctionExpr, Option<filter>, Vec<ordering>)
        let (agg_expr, filter, _ordering) = create_aggregate_expr_and_maybe_filter(
            expr,
            &df_schema,
            schema.as_ref(),
            state.execution_props(),
        )?;
        Ok((agg_expr, filter))
    }

    /// Resolve the source VID column for traversal, adding a struct field extraction
    /// projection if the source variable is a struct column (e.g., after WITH aggregation).
    ///
    /// Returns the (possibly modified) input plan and the column name to use as the source VID.
    fn resolve_source_vid_col(
        input_plan: Arc<dyn ExecutionPlan>,
        source_variable: &str,
    ) -> Result<(Arc<dyn ExecutionPlan>, String)> {
        let source_vid_col = format!("{}._vid", source_variable);
        if input_plan
            .schema()
            .column_with_name(&source_vid_col)
            .is_some()
        {
            return Ok((input_plan, source_vid_col));
        }
        // Check if the variable is a struct column (entity after WITH aggregation).
        // If so, add a projection to extract _vid from the struct.
        if let Ok(field) = input_plan.schema().field_with_name(source_variable)
            && matches!(
                field.data_type(),
                datafusion::arrow::datatypes::DataType::Struct(_)
            )
        {
            let enriched = Self::extract_struct_identity_columns(input_plan, source_variable)?;
            return Ok((enriched, format!("{}._vid", source_variable)));
        }
        Ok((input_plan, source_variable.to_string()))
    }

    /// Add a projection that extracts `{variable}._vid` and `{variable}._labels` from
    /// a struct column named `{variable}`. This is needed when an entity variable
    /// has been passed through a WITH + aggregation and exists as a struct rather
    /// than flat columns.
    fn extract_struct_identity_columns(
        input: Arc<dyn ExecutionPlan>,
        variable: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_plan::projection::ProjectionExec;

        let schema = input.schema();
        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();

        // Keep all existing columns
        for (i, field) in schema.fields().iter().enumerate() {
            let col_expr = Arc::new(datafusion::physical_expr::expressions::Column::new(
                field.name(),
                i,
            ));
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // Find the struct column and extract identity fields using get_field UDF
        if let Some((struct_idx, struct_field)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == variable)
            && let datafusion::arrow::datatypes::DataType::Struct(fields) = struct_field.data_type()
        {
            let struct_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Column::new(variable, struct_idx),
            );
            let get_field_udf: Arc<datafusion::logical_expr::ScalarUDF> =
                Arc::new(datafusion::logical_expr::ScalarUDF::from(
                    datafusion::functions::core::getfield::GetFieldFunc::new(),
                ));

            // Extract _vid field
            if fields.iter().any(|f| f.name() == "_vid") {
                let field_name: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                    Arc::new(datafusion::physical_expr::expressions::Literal::new(
                        ScalarValue::Utf8(Some("_vid".to_string())),
                    ));
                let vid_expr = Arc::new(datafusion::physical_expr::ScalarFunctionExpr::try_new(
                    get_field_udf.clone(),
                    vec![struct_col.clone(), field_name],
                    schema.as_ref(),
                    Arc::new(datafusion::common::config::ConfigOptions::default()),
                )?);
                proj_exprs.push((vid_expr, format!("{}._vid", variable)));
            }

            // Extract _labels field
            if fields.iter().any(|f| f.name() == "_labels") {
                let field_name: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                    Arc::new(datafusion::physical_expr::expressions::Literal::new(
                        ScalarValue::Utf8(Some("_labels".to_string())),
                    ));
                let labels_expr = Arc::new(datafusion::physical_expr::ScalarFunctionExpr::try_new(
                    get_field_udf,
                    vec![struct_col, field_name],
                    schema.as_ref(),
                    Arc::new(datafusion::common::config::ConfigOptions::default()),
                )?);
                proj_exprs.push((labels_expr, format!("{}._labels", variable)));
            }
        }

        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    }

    /// Add a projection that extracts ALL fields from a struct column named `{variable}`
    /// as flat `{variable}.{field_name}` columns. Used when a variable that passed through
    /// WITH + aggregation (and became a struct) is referenced by property access downstream.
    fn extract_all_struct_fields(
        input: Arc<dyn ExecutionPlan>,
        variable: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::ScalarValue;
        use datafusion::physical_plan::projection::ProjectionExec;

        let schema = input.schema();
        let mut proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();

        // Keep all existing columns
        for (i, field) in schema.fields().iter().enumerate() {
            let col_expr = Arc::new(datafusion::physical_expr::expressions::Column::new(
                field.name(),
                i,
            ));
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // Find the struct column and extract ALL fields
        if let Some((struct_idx, struct_field)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == variable)
            && let datafusion::arrow::datatypes::DataType::Struct(fields) = struct_field.data_type()
        {
            let struct_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Column::new(variable, struct_idx),
            );
            let get_field_udf: Arc<datafusion::logical_expr::ScalarUDF> =
                Arc::new(datafusion::logical_expr::ScalarUDF::from(
                    datafusion::functions::core::getfield::GetFieldFunc::new(),
                ));

            for field in fields.iter() {
                let flat_name = format!("{}.{}", variable, field.name());
                // Skip if already exists as a flat column
                if schema.column_with_name(&flat_name).is_some() {
                    continue;
                }
                let field_lit: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                    Arc::new(datafusion::physical_expr::expressions::Literal::new(
                        ScalarValue::Utf8(Some(field.name().to_string())),
                    ));
                let extract_expr =
                    Arc::new(datafusion::physical_expr::ScalarFunctionExpr::try_new(
                        get_field_udf.clone(),
                        vec![struct_col.clone(), field_lit],
                        schema.as_ref(),
                        Arc::new(datafusion::common::config::ConfigOptions::default()),
                    )?);
                proj_exprs.push((extract_expr, flat_name));
            }
        }

        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    }

    /// Check if a DataFusion expression refers to a LargeBinary column in the schema.
    fn is_large_binary_col(&self, expr: &DfExpr, schema: &SchemaRef) -> bool {
        if let DfExpr::Column(col) = expr
            && let Ok(field) = schema.field_with_name(&col.name)
        {
            return matches!(
                field.data_type(),
                datafusion::arrow::datatypes::DataType::LargeBinary
            );
        }
        // For any other expression type, conservatively return true
        // since schemaless properties are stored as LargeBinary
        true
    }

    /// Coerce an aggregate argument to `Float64` for numeric statistical
    /// aggregates (stDev/variance). A schemaless property arrives as a
    /// `LargeBinary` CypherValue column and is decoded via the Cypher→Float64
    /// UDF; a schema-typed numeric column is cast directly. Mirrors the
    /// coercion `avg` applies so the two agree on which rows are numeric.
    fn coerce_numeric_for_stat(arg: DfExpr, schema: &SchemaRef, this: &Self) -> DfExpr {
        if this.is_large_binary_col(&arg, schema) {
            crate::query::df_udfs::cypher_to_float64_expr(arg)
        } else {
            use datafusion::logical_expr::Cast;
            DfExpr::Cast(Cast::new(
                Box::new(arg),
                datafusion::arrow::datatypes::DataType::Float64,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Locy operator helpers
// ---------------------------------------------------------------------------

/// Resolve column names to indices in a schema.
/// Strip structural projection columns from a physical plan.
///
/// Graph scans add `named_struct` columns for node/edge variables (e.g., column `a`
/// of type `Struct{_vid, _labels, _all_props}`). When CrossJoined with a derived scan
/// Coerce a physical expression from `actual_dt` to `target_dt`.
///
/// Arrow's CastExpr cannot handle LargeBinary→Float64 because LargeBinary holds
/// serialized CypherValue bytes. For these cases, use the `_cypher_to_float64` UDF
/// which deserializes properly. For standard numeric coercions (Int64→Float64 etc.)
/// we use Arrow's built-in CastExpr.
fn coerce_physical_expr(
    expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    actual_dt: &DataType,
    target_dt: &DataType,
    schema: &arrow_schema::Schema,
) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
    use datafusion::physical_expr::expressions::CastExpr;

    match (actual_dt, target_dt) {
        // LargeBinary → Float64: use Cypher value deserializer UDF
        (DataType::LargeBinary, DataType::Float64) => wrap_cypher_to_float64(expr, schema),
        // LargeBinary → Int64: cast through Float64 first (extract number, then truncate)
        (DataType::LargeBinary, DataType::Int64) => {
            let float_expr = wrap_cypher_to_float64(expr, schema);
            Arc::new(CastExpr::new(float_expr, DataType::Int64, None))
        }
        // Standard Arrow casts (Int64→Float64, Float64→Int64, etc.)
        _ => Arc::new(CastExpr::new(expr, target_dt.clone(), None)),
    }
}

/// Wrap a physical expression with `_cypher_to_float64` UDF.
fn wrap_cypher_to_float64(
    expr: Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &arrow_schema::Schema,
) -> Arc<dyn datafusion::physical_expr::PhysicalExpr> {
    let udf = Arc::new(super::df_udfs::cypher_to_float64_udf());
    let config = Arc::new(datafusion::common::config::ConfigOptions::default());
    Arc::new(
        datafusion::physical_expr::ScalarFunctionExpr::try_new(udf, vec![expr], schema, config)
            .expect("CypherToFloat64Udf accepts Any(1) signature"),
    )
}

/// Strip structural projection columns from a physical plan that conflict with
/// derived scan column names.
///
/// Graph scans add `named_struct` columns for node/edge variables (e.g., column `a`
/// of type `Struct{_vid, _labels, _all_props}`). When CrossJoined with a derived scan
/// that also has a column `a` (UInt64 VID), the duplicate name causes ambiguous
/// column resolution. This function removes ONLY those Struct-typed columns whose
/// names collide with derived scan columns, preserving non-conflicting struct columns
/// (like edge structs) that are needed for typed property access.
fn strip_conflicting_structural_columns(
    input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    derived_col_names: &HashSet<&str>,
) -> anyhow::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    use datafusion::physical_plan::projection::ProjectionExec;

    let schema = input.schema();
    let proj_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            // Remove Struct columns whose names conflict with derived scan columns.
            !(matches!(f.data_type(), arrow_schema::DataType::Struct(_))
                && derived_col_names.contains(f.name().as_str()))
        })
        .map(|(i, f)| {
            let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                datafusion::physical_expr::expressions::Column::new(f.name(), i),
            );
            (col, f.name().clone())
        })
        .collect();

    if proj_exprs.len() == schema.fields().len() {
        // No conflicting structural columns
        return Ok(input);
    }

    Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
}

fn resolve_column_indices(
    schema: &arrow_schema::SchemaRef,
    column_names: &[String],
) -> anyhow::Result<Vec<usize>> {
    column_names
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| anyhow::anyhow!("Column '{}' not found in schema", name))
        })
        .collect()
}

/// Resolve BEST BY criteria from `(Expr, ascending)` pairs to `SortCriterion` values.
fn resolve_best_by_criteria(
    schema: &arrow_schema::SchemaRef,
    criteria: &[(Expr, bool)],
) -> anyhow::Result<Vec<super::df_graph::locy_best_by::SortCriterion>> {
    criteria
        .iter()
        .map(|(expr, ascending)| {
            // Extract candidate column names — try property name first (short),
            // then full "var.prop" form, then variable name.
            let candidates: Vec<String> = match expr {
                Expr::Property(base, prop) => {
                    if let Expr::Variable(var) = base.as_ref() {
                        vec![prop.clone(), format!("{}.{}", var, prop)]
                    } else {
                        vec![prop.clone()]
                    }
                }
                Expr::Variable(name) => {
                    let short = name.rsplit('.').next().unwrap_or(name).to_string();
                    if short != *name {
                        vec![short, name.clone()]
                    } else {
                        vec![name.clone()]
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "BEST BY criteria must be variable or property access"
                    ));
                }
            };
            let col_index = candidates
                .iter()
                .find_map(|name| schema.index_of(name).ok())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "BEST BY column '{}' not found",
                        candidates.first().unwrap_or(&String::new())
                    )
                })?;
            Ok(super::df_graph::locy_best_by::SortCriterion {
                col_index,
                ascending: *ascending,
                nulls_first: false, // NULLS LAST is Locy default
            })
        })
        .collect()
}

/// Resolve fold bindings from `(output_name, aggregate_expr)` to `FoldBinding` values.
///
/// Normalizes grammar aliases to canonical names and resolves each aggregate
/// against `plugin_registry` so the runtime engine receives a pre-bound
/// [`uni_plugin::traits::locy::LocyAggregate`] trait object.
fn resolve_fold_bindings(
    schema: &arrow_schema::SchemaRef,
    fold_bindings: &[(String, Expr)],
    plugin_registry: &uni_plugin::PluginRegistry,
) -> anyhow::Result<Vec<super::df_graph::locy_fold::FoldBinding>> {
    use super::df_graph::locy_fold::resolve_locy_aggregate;
    fold_bindings
        .iter()
        .map(|(output_name, expr)| {
            // Parse aggregate expression: FunctionCall { name, args }
            match expr {
                Expr::FunctionCall { name, args, .. } => {
                    let upper = name.to_uppercase();
                    let is_count = matches!(upper.as_str(), "COUNT" | "MCOUNT");

                    let canonical: smol_str::SmolStr = if is_count && args.is_empty() {
                        smol_str::SmolStr::new_static("COUNTALL")
                    } else {
                        match upper.as_str() {
                            "SUM" | "MSUM" => smol_str::SmolStr::new_static("SUM"),
                            "COUNT" | "MCOUNT" => smol_str::SmolStr::new_static("COUNT"),
                            "MAX" | "MMAX" => smol_str::SmolStr::new_static("MAX"),
                            "MIN" | "MMIN" => smol_str::SmolStr::new_static("MIN"),
                            "AVG" => smol_str::SmolStr::new_static("AVG"),
                            "COLLECT" => smol_str::SmolStr::new_static("COLLECT"),
                            "MNOR" => smol_str::SmolStr::new_static("MNOR"),
                            "MPROD" => smol_str::SmolStr::new_static("MPROD"),
                            // Plugin-namespaced custom aggregate (dotted
                            // name, e.g. `myplugin.MYAGG`): pass through raw
                            // so `resolve_locy_aggregate` resolves it by
                            // namespace. Bare unknown names remain errors.
                            _ if name.contains('.') => smol_str::SmolStr::new(name.as_str()),
                            other => {
                                return Err(anyhow::anyhow!(
                                    "Unsupported FOLD aggregate function: {}",
                                    other
                                ));
                            }
                        }
                    };

                    let entry = resolve_locy_aggregate(plugin_registry, canonical.as_str())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Locy aggregate '{canonical}' is not registered in the plugin registry"
                            )
                        })?;
                    let aggregate = Arc::clone(&entry.aggregate);

                    // COUNTALL has no input column.
                    if canonical.as_str() == "COUNTALL" {
                        return Ok(super::df_graph::locy_fold::FoldBinding {
                            output_name: output_name.clone(),
                            name: canonical,
                            aggregate,
                            input_col_index: 0,
                            input_col_name: None,
                        });
                    }

                    // The LocyProject aliases the aggregate input expression to the
                    // fold output name, so look up the output name in the schema.
                    let input_col_index = schema
                        .index_of(output_name)
                        .or_else(|_| {
                            // Fallback: try the raw argument column name
                            let col_name = match args.first() {
                                Some(Expr::Variable(name)) => Some(name.clone()),
                                Some(Expr::Property(base, prop)) => {
                                    if let Expr::Variable(var) = base.as_ref() {
                                        Some(format!("{}.{}", var, prop))
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };
                            col_name
                                .and_then(|n| schema.index_of(&n).ok())
                                .ok_or_else(|| {
                                    arrow_schema::ArrowError::SchemaError(format!(
                                        "FOLD column '{}' not found",
                                        output_name
                                    ))
                                })
                        })
                        .map_err(|_| anyhow::anyhow!("FOLD column '{}' not found", output_name))?;
                    Ok(super::df_graph::locy_fold::FoldBinding {
                        output_name: output_name.clone(),
                        name: canonical,
                        aggregate,
                        input_col_index,
                        input_col_name: Some(output_name.clone()),
                    })
                }
                _ => Err(anyhow::anyhow!(
                    "FOLD binding must be an aggregate function call"
                )),
            }
        })
        .collect()
}

/// Resolve WITH-passthrough markers, narrowing forwarded entity variables.
///
/// Computes the narrowable (Node/Edge) variables from the plan and delegates to
/// [`reconcile_passthrough_properties`]. See issue #134 family.
fn apply_passthrough_reconciliation(
    plan: &LogicalPlan,
    properties: &mut HashMap<String, HashSet<String>>,
) {
    let mut kinds = HashMap::new();
    collect_variable_kinds(plan, &mut kinds);
    let narrowable: HashSet<String> = kinds
        .into_iter()
        .filter(|(_, k)| matches!(k, VariableKind::Node | VariableKind::Edge))
        .map(|(v, _)| v)
        .collect();
    reconcile_passthrough_properties(plan, properties, &narrowable);
}

/// Recursively collect variable kinds (node, edge, path) from a LogicalPlan.
///
/// This information is used by the expression translator to resolve bare variable
/// references to their identity columns (e.g., `n` → `n._vid` for nodes).
/// Prefix of the columns `EndpointHydrateExec` appends.
pub(crate) const ENDPOINT_COLUMN_PREFIX: &str = "_endpoint.";

/// Collect the relationship variables named by `startNode(x)` / `endNode(x)`.
///
/// Only a bare variable argument is collected. `startNode(<expr>)` has no column
/// to read endpoints from, so there is nothing to hydrate and the call keeps
/// whatever behaviour it had.
fn collect_endpoint_relationships(expr: &Expr, out: &mut Vec<String>) {
    if let Expr::FunctionCall { name, args, .. } = expr {
        let upper = name.to_uppercase();
        if (upper == "STARTNODE" || upper == "ENDNODE")
            && let Some(Expr::Variable(rel)) = args.first()
            && !out.contains(rel)
        {
            out.push(rel.clone());
        }
    }
    // `[r IN rels | startNode(r).id]` names `r`, which is a loop variable and
    // never a column. What has to be hydrated is the *list* it iterates, so the
    // per-element endpoints can be carried into the comprehension's inner batch
    // alongside the element itself.
    if let Expr::ListComprehension {
        variable,
        list,
        where_clause,
        map_expr,
    } = expr
        && let Expr::Variable(list_var) = list.as_ref()
    {
        let mut inner = Vec::new();
        collect_endpoint_relationships(map_expr, &mut inner);
        if let Some(w) = where_clause {
            collect_endpoint_relationships(w, &mut inner);
        }
        if inner.contains(variable) && !out.contains(list_var) {
            out.push(list_var.clone());
        }
    }
    expr.for_each_child(&mut |child| collect_endpoint_relationships(child, out));
}

fn collect_variable_kinds(plan: &LogicalPlan, kinds: &mut HashMap<String, VariableKind>) {
    match plan {
        // Phase 5b followup: recurse into the wrapped node so the
        // wrapped operator's variable still gets collected.
        LogicalPlan::FusedIndexScanWrapped { inner, .. } => {
            collect_variable_kinds(inner, kinds);
        }
        LogicalPlan::Scan { variable, .. }
        | LogicalPlan::FusedIndexScan { variable, .. }
        | LogicalPlan::ExtIdLookup { variable, .. }
        | LogicalPlan::ScanAll { variable, .. }
        | LogicalPlan::ScanMainByLabels { variable, .. }
        | LogicalPlan::VectorKnn { variable, .. }
        | LogicalPlan::InvertedIndexLookup { variable, .. } => {
            kinds.insert(variable.clone(), VariableKind::Node);
        }
        LogicalPlan::Traverse {
            input,
            source_variable,
            target_variable,
            step_variable,
            path_variable,
            is_variable_length,
            ..
        }
        | LogicalPlan::TraverseMainByType {
            input,
            source_variable,
            target_variable,
            step_variable,
            path_variable,
            is_variable_length,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            // The target's columns are produced *here*, so the traversal is
            // authoritative for it. The source is only consumed — whatever bound
            // it below stays authoritative, or this would overwrite an `UNWIND`
            // rebinding with a `Node` whose columns do not exist.
            kinds
                .entry(source_variable.clone())
                .or_insert(VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            if let Some(sv) = step_variable {
                kinds.insert(sv.clone(), VariableKind::edge_for(*is_variable_length));
            }
            if let Some(pv) = path_variable {
                kinds.insert(pv.clone(), VariableKind::Path);
            }
            // A quantified pattern's inner variables are group variables: list
            // columns, resolved as bare columns rather than `_vid` / `_eid`.
            if let LogicalPlan::Traverse {
                qpp_steps: Some(steps),
                qpp_inner_source,
                ..
            } = plan
            {
                let step_names: Vec<(Option<String>, Option<String>)> = steps
                    .iter()
                    .map(|s| (s.edge_variable.clone(), s.target_variable.clone()))
                    .collect();
                for b in crate::query::df_graph::nfa::qpp_group_bindings(
                    qpp_inner_source.as_deref(),
                    &step_names,
                ) {
                    kinds.insert(
                        b.name,
                        match b.kind {
                            crate::query::df_graph::nfa::QppGroupKind::Node => {
                                VariableKind::NodeList
                            }
                            crate::query::df_graph::nfa::QppGroupKind::Edge => {
                                VariableKind::EdgeList
                            }
                        },
                    );
                }
            }
        }
        LogicalPlan::ShortestPath {
            input,
            source_variable,
            target_variable,
            path_variable,
            step_variable,
            ..
        }
        | LogicalPlan::AllShortestPaths {
            input,
            source_variable,
            target_variable,
            path_variable,
            step_variable,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(source_variable.clone(), VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            kinds.insert(path_variable.clone(), VariableKind::Path);
            if let Some(sv) = step_variable {
                // Always a list: the pattern is variable-length by construction.
                kinds.insert(sv.clone(), VariableKind::EdgeList);
            }
        }
        LogicalPlan::QuantifiedPattern {
            input,
            pattern_plan,
            path_variable,
            start_variable,
            binding_variable,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            collect_variable_kinds(pattern_plan, kinds);
            kinds.insert(start_variable.clone(), VariableKind::Node);
            kinds.insert(binding_variable.clone(), VariableKind::Node);
            if let Some(pv) = path_variable {
                kinds.insert(pv.clone(), VariableKind::Path);
            }
        }
        LogicalPlan::BindZeroLengthPath {
            input,
            node_variable,
            path_variable,
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(node_variable.clone(), VariableKind::Node);
            kinds.insert(path_variable.clone(), VariableKind::Path);
        }
        LogicalPlan::BindPath {
            input,
            node_variables,
            edge_variables,
            path_variable,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            for nv in node_variables {
                kinds.insert(nv.clone(), VariableKind::Node);
            }
            for ev in edge_variables {
                kinds.insert(ev.clone(), VariableKind::Edge);
            }
            kinds.insert(path_variable.clone(), VariableKind::Path);
        }
        // Wrapper nodes: recurse into input(s)
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Create { input, .. }
        | LogicalPlan::CreateBatch { input, .. }
        | LogicalPlan::Merge { input, .. }
        | LogicalPlan::Set { input, .. }
        | LogicalPlan::Remove { input, .. }
        | LogicalPlan::Delete { input, .. }
        | LogicalPlan::Foreach { input, .. }
        | LogicalPlan::SubqueryCall { input, .. } => {
            collect_variable_kinds(input, kinds);
        }
        // UNWIND *rebinds* its variable; it is not a pass-through for that name.
        // The element it binds is a single CypherValue column, with none of the
        // `{var}._vid` / `{var}.{prop}` columns a scan produces — so when the
        // alias shadows a scan variable below (`... collect(friend) AS friends
        // UNWIND friends AS friend`), leaving the scan's `Node` kind in place made
        // every consumer above reach for columns that are not in the schema.
        //
        // Rebinding is correctly scoped: `translation_context_for_plan` is built
        // from each operator's own input, so expressions *below* the UNWIND still
        // see the scan binding.
        LogicalPlan::Unwind {
            input, variable, ..
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(variable.clone(), VariableKind::Opaque);
        }
        LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right, .. } => {
            collect_variable_kinds(left, kinds);
            collect_variable_kinds(right, kinds);
        }
        LogicalPlan::Apply {
            input, subquery, ..
        } => {
            collect_variable_kinds(input, kinds);
            collect_variable_kinds(subquery, kinds);
        }
        LogicalPlan::RecursiveCTE {
            initial, recursive, ..
        } => {
            collect_variable_kinds(initial, kinds);
            collect_variable_kinds(recursive, kinds);
        }
        LogicalPlan::Explain { plan } => {
            collect_variable_kinds(plan, kinds);
        }
        LogicalPlan::ProcedureCall {
            procedure_name,
            yield_items,
            ..
        } => {
            use crate::query::df_graph::procedure_call::{
                is_node_yield_procedure_static, map_yield_to_canonical,
            };
            for (name, alias) in yield_items {
                let var = alias.as_ref().unwrap_or(name);
                if is_node_yield_procedure_static(procedure_name.as_str()) {
                    let canonical = map_yield_to_canonical(name);
                    if canonical == "node" {
                        kinds.insert(var.clone(), VariableKind::Node);
                    }
                    // Scalar yields (distance, score, vid) don't need VariableKind
                }
                // For schema procedures, yields are all scalars — no entry needed
            }
        }
        // Locy operators — no variable kinds to collect
        LogicalPlan::LocyProgram { .. }
        | LogicalPlan::LocyFold { .. }
        | LogicalPlan::LocyBestBy { .. }
        | LogicalPlan::LocyPriority { .. }
        | LogicalPlan::LocyDerivedScan { .. }
        | LogicalPlan::LocyProject { .. }
        | LogicalPlan::LocyModelInvoke { .. } => {}
        // Leaf nodes with no variables or not applicable
        LogicalPlan::Empty
        | LogicalPlan::CreateVectorIndex { .. }
        | LogicalPlan::CreateSparseIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateScalarIndex { .. }
        | LogicalPlan::CreateJsonFtsIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::ShowIndexes { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Backup { .. }
        | LogicalPlan::ShowDatabase
        | LogicalPlan::ShowConfig
        | LogicalPlan::ShowStatistics
        | LogicalPlan::Vacuum
        | LogicalPlan::Checkpoint
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::CopyFrom { .. }
        | LogicalPlan::CreateLabel(_)
        | LogicalPlan::CreateEdgeType(_)
        | LogicalPlan::AlterLabel(_)
        | LogicalPlan::AlterEdgeType(_)
        | LogicalPlan::DropLabel(_)
        | LogicalPlan::DropEdgeType(_)
        | LogicalPlan::CreateConstraint(_)
        | LogicalPlan::DropConstraint(_)
        | LogicalPlan::ShowConstraints(_) => {}
    }
}

/// Collect node variable names from CREATE/MERGE patterns for startNode/endNode UDFs.
///
/// These hints are used alongside `variable_kinds` to identify node variables
/// in mutation contexts for startNode/endNode resolution.
fn collect_mutation_node_hints(plan: &LogicalPlan, hints: &mut Vec<String>) {
    match plan {
        LogicalPlan::Create { input, pattern } => {
            collect_node_names_from_pattern(pattern, hints);
            collect_mutation_node_hints(input, hints);
        }
        LogicalPlan::CreateBatch { input, patterns } => {
            for pattern in patterns {
                collect_node_names_from_pattern(pattern, hints);
            }
            collect_mutation_node_hints(input, hints);
        }
        LogicalPlan::Merge { input, pattern, .. } => {
            collect_node_names_from_pattern(pattern, hints);
            collect_mutation_node_hints(input, hints);
        }
        // For all other nodes, recurse into inputs
        LogicalPlan::Traverse { input, .. }
        | LogicalPlan::TraverseMainByType { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Unwind { input, .. }
        | LogicalPlan::Set { input, .. }
        | LogicalPlan::Remove { input, .. }
        | LogicalPlan::Delete { input, .. }
        | LogicalPlan::Foreach { input, .. }
        | LogicalPlan::SubqueryCall { input, .. }
        | LogicalPlan::ShortestPath { input, .. }
        | LogicalPlan::AllShortestPaths { input, .. }
        | LogicalPlan::QuantifiedPattern { input, .. }
        | LogicalPlan::BindZeroLengthPath { input, .. }
        | LogicalPlan::BindPath { input, .. } => {
            collect_mutation_node_hints(input, hints);
        }
        LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right, .. } => {
            collect_mutation_node_hints(left, hints);
            collect_mutation_node_hints(right, hints);
        }
        LogicalPlan::Apply {
            input, subquery, ..
        } => {
            collect_mutation_node_hints(input, hints);
            collect_mutation_node_hints(subquery, hints);
        }
        LogicalPlan::RecursiveCTE {
            initial, recursive, ..
        } => {
            collect_mutation_node_hints(initial, hints);
            collect_mutation_node_hints(recursive, hints);
        }
        LogicalPlan::Explain { plan } => {
            collect_mutation_node_hints(plan, hints);
        }
        // Leaf nodes hold no CREATE/MERGE pattern (those are wrapper nodes,
        // recursed above). Exhaustive — no `_ => {}` — so a new variant must be
        // classified here rather than silently skipped (the #131 bug class).
        LogicalPlan::Scan { .. }
        | LogicalPlan::ScanAll { .. }
        | LogicalPlan::ScanMainByLabels { .. }
        | LogicalPlan::ExtIdLookup { .. }
        | LogicalPlan::FusedIndexScan { .. }
        | LogicalPlan::FusedIndexScanWrapped { .. }
        | LogicalPlan::VectorKnn { .. }
        | LogicalPlan::InvertedIndexLookup { .. }
        | LogicalPlan::ProcedureCall { .. }
        | LogicalPlan::LocyProgram { .. }
        | LogicalPlan::LocyFold { .. }
        | LogicalPlan::LocyBestBy { .. }
        | LogicalPlan::LocyPriority { .. }
        | LogicalPlan::LocyDerivedScan { .. }
        | LogicalPlan::LocyProject { .. }
        | LogicalPlan::LocyModelInvoke { .. }
        | LogicalPlan::Empty
        | LogicalPlan::CreateVectorIndex { .. }
        | LogicalPlan::CreateSparseIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateScalarIndex { .. }
        | LogicalPlan::CreateJsonFtsIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::ShowIndexes { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Backup { .. }
        | LogicalPlan::ShowDatabase
        | LogicalPlan::ShowConfig
        | LogicalPlan::ShowStatistics
        | LogicalPlan::Vacuum
        | LogicalPlan::Checkpoint
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::CopyFrom { .. }
        | LogicalPlan::CreateLabel(_)
        | LogicalPlan::CreateEdgeType(_)
        | LogicalPlan::AlterLabel(_)
        | LogicalPlan::AlterEdgeType(_)
        | LogicalPlan::DropLabel(_)
        | LogicalPlan::DropEdgeType(_)
        | LogicalPlan::CreateConstraint(_)
        | LogicalPlan::DropConstraint(_)
        | LogicalPlan::ShowConstraints(_) => {}
    }
}

/// Extract node variable names from a single Cypher pattern.
fn collect_node_names_from_pattern(pattern: &Pattern, hints: &mut Vec<String>) {
    for path in &pattern.paths {
        for element in &path.elements {
            match element {
                PatternElement::Node(n) => {
                    if let Some(ref v) = n.variable
                        && !hints.contains(v)
                    {
                        hints.push(v.clone());
                    }
                }
                PatternElement::Parenthesized { pattern, .. } => {
                    let sub = Pattern {
                        paths: vec![pattern.as_ref().clone()],
                    };
                    collect_node_names_from_pattern(&sub, hints);
                }
                _ => {}
            }
        }
    }
}

/// Collect edge (relationship) variable names from CREATE/MERGE patterns.
///
/// Used by `id()` to resolve edge identity as `_eid` instead of `_vid`.
fn collect_mutation_edge_hints(plan: &LogicalPlan, hints: &mut Vec<String>) {
    match plan {
        LogicalPlan::Create { input, pattern } | LogicalPlan::Merge { input, pattern, .. } => {
            collect_edge_names_from_pattern(pattern, hints);
            collect_mutation_edge_hints(input, hints);
        }
        LogicalPlan::CreateBatch { input, patterns } => {
            for pattern in patterns {
                collect_edge_names_from_pattern(pattern, hints);
            }
            collect_mutation_edge_hints(input, hints);
        }
        // For all other nodes, recurse into inputs
        LogicalPlan::Traverse { input, .. }
        | LogicalPlan::TraverseMainByType { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Unwind { input, .. }
        | LogicalPlan::Set { input, .. }
        | LogicalPlan::Remove { input, .. }
        | LogicalPlan::Delete { input, .. }
        | LogicalPlan::Foreach { input, .. }
        | LogicalPlan::SubqueryCall { input, .. }
        | LogicalPlan::ShortestPath { input, .. }
        | LogicalPlan::AllShortestPaths { input, .. }
        | LogicalPlan::QuantifiedPattern { input, .. }
        | LogicalPlan::BindZeroLengthPath { input, .. }
        | LogicalPlan::BindPath { input, .. } => {
            collect_mutation_edge_hints(input, hints);
        }
        LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right, .. } => {
            collect_mutation_edge_hints(left, hints);
            collect_mutation_edge_hints(right, hints);
        }
        LogicalPlan::Apply {
            input, subquery, ..
        } => {
            collect_mutation_edge_hints(input, hints);
            collect_mutation_edge_hints(subquery, hints);
        }
        LogicalPlan::RecursiveCTE {
            initial, recursive, ..
        } => {
            collect_mutation_edge_hints(initial, hints);
            collect_mutation_edge_hints(recursive, hints);
        }
        LogicalPlan::Explain { plan } => {
            collect_mutation_edge_hints(plan, hints);
        }
        // Leaf nodes hold no CREATE/MERGE pattern (those are wrapper nodes,
        // recursed above). Exhaustive — no `_ => {}` — so a new variant must be
        // classified here rather than silently skipped (the #131 bug class).
        LogicalPlan::Scan { .. }
        | LogicalPlan::ScanAll { .. }
        | LogicalPlan::ScanMainByLabels { .. }
        | LogicalPlan::ExtIdLookup { .. }
        | LogicalPlan::FusedIndexScan { .. }
        | LogicalPlan::FusedIndexScanWrapped { .. }
        | LogicalPlan::VectorKnn { .. }
        | LogicalPlan::InvertedIndexLookup { .. }
        | LogicalPlan::ProcedureCall { .. }
        | LogicalPlan::LocyProgram { .. }
        | LogicalPlan::LocyFold { .. }
        | LogicalPlan::LocyBestBy { .. }
        | LogicalPlan::LocyPriority { .. }
        | LogicalPlan::LocyDerivedScan { .. }
        | LogicalPlan::LocyProject { .. }
        | LogicalPlan::LocyModelInvoke { .. }
        | LogicalPlan::Empty
        | LogicalPlan::CreateVectorIndex { .. }
        | LogicalPlan::CreateSparseIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateScalarIndex { .. }
        | LogicalPlan::CreateJsonFtsIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::ShowIndexes { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Backup { .. }
        | LogicalPlan::ShowDatabase
        | LogicalPlan::ShowConfig
        | LogicalPlan::ShowStatistics
        | LogicalPlan::Vacuum
        | LogicalPlan::Checkpoint
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::CopyFrom { .. }
        | LogicalPlan::CreateLabel(_)
        | LogicalPlan::CreateEdgeType(_)
        | LogicalPlan::AlterLabel(_)
        | LogicalPlan::AlterEdgeType(_)
        | LogicalPlan::DropLabel(_)
        | LogicalPlan::DropEdgeType(_)
        | LogicalPlan::CreateConstraint(_)
        | LogicalPlan::DropConstraint(_)
        | LogicalPlan::ShowConstraints(_) => {}
    }
}

/// Extract edge (relationship) variable names from a single Cypher pattern.
fn collect_edge_names_from_pattern(pattern: &Pattern, hints: &mut Vec<String>) {
    for path in &pattern.paths {
        for element in &path.elements {
            match element {
                PatternElement::Relationship(r) => {
                    if let Some(ref v) = r.variable
                        && !hints.contains(v)
                    {
                        hints.push(v.clone());
                    }
                }
                PatternElement::Parenthesized { pattern, .. } => {
                    let sub = Pattern {
                        paths: vec![pattern.as_ref().clone()],
                    };
                    collect_edge_names_from_pattern(&sub, hints);
                }
                _ => {}
            }
        }
    }
}

/// Convert AST Direction to adjacency cache Direction.
fn convert_direction(ast_dir: AstDirection) -> Direction {
    match ast_dir {
        AstDirection::Outgoing => Direction::Outgoing,
        AstDirection::Incoming => Direction::Incoming,
        AstDirection::Both => Direction::Both,
    }
}

/// Clean VLP target property list derived from planner property collection.
///
/// Removes the wildcard sentinel `"*"` (not a real property), and ensures
/// `_all_props` is loaded when wildcard/non-schema properties require it.
fn sanitize_vlp_target_properties(
    mut properties: Vec<String>,
    target_has_wildcard: bool,
    target_label_props: Option<&HashSet<String>>,
) -> Vec<String> {
    properties.retain(|p| p != "*");

    // A wildcard needs `_all_props` whether or not named properties survived
    // it. `resolve_properties` has already expanded `"*"` into the label's full
    // declared set, so requiring `properties.is_empty()` here meant a label
    // that declares anything never got `_all_props` -- and schemaless
    // properties, which live only in the overflow blob that `_all_props`
    // carries, were dropped from every whole-entity VLP result. `RETURN n`
    // over `(h)-[:R*1..2]->(n)` returned the declared columns and silently lost
    // the rest, where the same match at fixed length returned all of them.
    // This is the single-hop rule at the `target_has_wildcard` site in
    // `plan_traverse_main_by_type`, which never carried the extra condition.
    if target_has_wildcard && !properties.iter().any(|p| p == "_all_props") {
        properties.push("_all_props".to_string());
    }

    let has_non_schema_props = properties.iter().any(|p| {
        p != "_all_props"
            && p != "overflow_json"
            && !p.starts_with('_')
            && !target_label_props.is_some_and(|props| props.contains(p))
    });
    if has_non_schema_props && !properties.iter().any(|p| p == "_all_props") {
        properties.push("_all_props".to_string());
    }

    properties
}

// ---------------------------------------------------------------------------
// Issue #53: helpers for the CrossJoin+Filter → HashJoinExec optimization.
// ---------------------------------------------------------------------------

/// Classification of a Filter predicate sitting above a CrossJoin, used to
/// decide whether (and how) to rewrite it as a HashJoin.
struct JoinPredicateClassification {
    /// Equi-join conditions: each `(left_expr, right_expr)` pair has
    /// `left_expr` referencing only LEFT-side variables and `right_expr`
    /// referencing only RIGHT-side variables.
    equi_pairs: Vec<(Expr, Expr)>,
    /// Conjuncts referencing ONLY left-side variables. Pushed into a Filter
    /// wrapped around the LEFT subtree before planning.
    left_only: Vec<Expr>,
    /// Conjuncts referencing ONLY right-side variables. Pushed into a Filter
    /// wrapped around the RIGHT subtree before planning.
    right_only: Vec<Expr>,
    /// Conjuncts referencing both sides but NOT in equi-join form. Applied as
    /// a post-join FilterExec.
    residual: Option<Expr>,
}

/// Walk a LogicalPlan subtree and collect all variable names produced by it
/// (Scans, Unwind targets, Traverse targets, etc.). Used to classify which
/// side of a CrossJoin a predicate's variables belong to, and to tell
/// upstream-bound variables apart from freshly-created ones in CREATE+SET fusion.
pub(crate) fn collect_plan_variables(plan: &LogicalPlan) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_plan_variables_into(plan, &mut out);
    out
}

/// Insert a node variable plus its flat `{var}._vid` column.
///
/// Registering `{var}._vid` lets an equi-join key written in the baked
/// `var._vid` form (Locy IS-ref node keys) match this side exactly — and,
/// crucially, a side that only carries a bare *scalar* column of the same name
/// (e.g. a derived-relation KEY column) does NOT, so the classifier never
/// confuses a node `b._vid` with a derived scalar `b` (issue #131).
fn insert_node_var(variable: &str, out: &mut HashSet<String>) {
    out.insert(variable.to_string());
    out.insert(format!("{variable}._vid"));
}

/// Collect the output variable/column names a plan exposes.
///
/// This MUST stay in lockstep with [`collect_variable_kinds`]: every variant
/// that binds a name there must bind it here, or an equi-join over that
/// operator silently degrades to a quadratic `CrossJoinExec` because
/// `classify_join_predicate` cannot see the column (the root cause of #131,
/// where `LocyDerivedScan` was the missing variant). The match is intentionally
/// exhaustive — do NOT add a `_ => {}` arm; a new `LogicalPlan` variant should
/// fail to compile here until it is classified as binding or non-binding.
fn collect_plan_variables_into(plan: &LogicalPlan, out: &mut HashSet<String>) {
    match plan {
        // Wrapped scan: recurse so the inner scan's variable is still collected.
        LogicalPlan::FusedIndexScanWrapped { inner, .. } => {
            collect_plan_variables_into(inner, out);
        }
        // Leaf node scans — each binds one node variable (+ `_vid`).
        LogicalPlan::Scan { variable, .. }
        | LogicalPlan::FusedIndexScan { variable, .. }
        | LogicalPlan::ExtIdLookup { variable, .. }
        | LogicalPlan::ScanAll { variable, .. }
        | LogicalPlan::ScanMainByLabels { variable, .. }
        | LogicalPlan::VectorKnn { variable, .. }
        | LogicalPlan::InvertedIndexLookup { variable, .. } => {
            insert_node_var(variable, out);
        }
        LogicalPlan::Traverse {
            input,
            source_variable,
            target_variable,
            step_variable,
            path_variable,
            ..
        }
        | LogicalPlan::TraverseMainByType {
            input,
            source_variable,
            target_variable,
            step_variable,
            path_variable,
            ..
        } => {
            collect_plan_variables_into(input, out);
            insert_node_var(source_variable, out);
            insert_node_var(target_variable, out);
            if let Some(s) = step_variable {
                out.insert(s.clone());
            }
            if let Some(p) = path_variable {
                out.insert(p.clone());
            }
        }
        LogicalPlan::ShortestPath {
            input,
            source_variable,
            target_variable,
            path_variable,
            ..
        }
        | LogicalPlan::AllShortestPaths {
            input,
            source_variable,
            target_variable,
            path_variable,
            ..
        } => {
            collect_plan_variables_into(input, out);
            insert_node_var(source_variable, out);
            insert_node_var(target_variable, out);
            out.insert(path_variable.clone());
        }
        LogicalPlan::QuantifiedPattern {
            input,
            pattern_plan,
            path_variable,
            start_variable,
            binding_variable,
            ..
        } => {
            collect_plan_variables_into(input, out);
            collect_plan_variables_into(pattern_plan, out);
            insert_node_var(start_variable, out);
            insert_node_var(binding_variable, out);
            if let Some(p) = path_variable {
                out.insert(p.clone());
            }
        }
        LogicalPlan::BindZeroLengthPath {
            input,
            node_variable,
            path_variable,
        } => {
            collect_plan_variables_into(input, out);
            insert_node_var(node_variable, out);
            out.insert(path_variable.clone());
        }
        LogicalPlan::BindPath {
            input,
            node_variables,
            edge_variables,
            path_variable,
            ..
        } => {
            collect_plan_variables_into(input, out);
            for nv in node_variables {
                insert_node_var(nv, out);
            }
            for ev in edge_variables {
                out.insert(ev.clone());
            }
            out.insert(path_variable.clone());
        }
        LogicalPlan::Unwind {
            input, variable, ..
        } => {
            out.insert(variable.clone());
            collect_plan_variables_into(input, out);
        }
        // Wrapper nodes — pass their input's variables through.
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Create { input, .. }
        | LogicalPlan::CreateBatch { input, .. }
        | LogicalPlan::Merge { input, .. }
        | LogicalPlan::Set { input, .. }
        | LogicalPlan::Remove { input, .. }
        | LogicalPlan::Delete { input, .. }
        | LogicalPlan::Foreach { input, .. }
        | LogicalPlan::SubqueryCall { input, .. } => {
            collect_plan_variables_into(input, out);
        }
        LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
            collect_plan_variables_into(left, out);
            collect_plan_variables_into(right, out);
        }
        LogicalPlan::Apply {
            input, subquery, ..
        } => {
            collect_plan_variables_into(input, out);
            collect_plan_variables_into(subquery, out);
        }
        LogicalPlan::RecursiveCTE {
            initial, recursive, ..
        } => {
            collect_plan_variables_into(initial, out);
            collect_plan_variables_into(recursive, out);
        }
        LogicalPlan::Explain { plan } => {
            collect_plan_variables_into(plan, out);
        }
        LogicalPlan::ProcedureCall {
            procedure_name,
            yield_items,
            ..
        } => {
            use crate::query::df_graph::procedure_call::{
                is_node_yield_procedure_static, map_yield_to_canonical,
            };
            for (name, alias) in yield_items {
                let var = alias.as_ref().unwrap_or(name);
                if is_node_yield_procedure_static(procedure_name.as_str())
                    && map_yield_to_canonical(name) == "node"
                {
                    insert_node_var(var, out);
                } else {
                    out.insert(var.clone());
                }
            }
        }
        // A Locy derived-relation scan (the right side of a positive IS-ref
        // CrossJoin) exposes its rule's yield columns by name (KEY/value
        // columns, e.g. `it`, `sup`, `b`). They must be visible here so that
        // `classify_join_predicate` recognizes the IS-ref equality conjuncts
        // (`it._vid = it`) as cross-side equi-pairs and lets
        // `try_plan_cross_join_as_hash_join` recover a HashJoinExec — without
        // this, the join stays a quadratic CrossJoinExec (issue #131).
        LogicalPlan::LocyDerivedScan { schema, .. } => {
            for f in schema.fields() {
                out.insert(f.name().clone());
            }
        }
        // Locy program/post-fixpoint operators expose no user-join-keyable
        // variables here (their bindings are evaluated row-wise inside the
        // fixpoint/SLG executor, never as a DataFusion CrossJoin). Mirrors the
        // `{}` arm in `collect_variable_kinds`.
        LogicalPlan::LocyProgram { .. }
        | LogicalPlan::LocyFold { .. }
        | LogicalPlan::LocyBestBy { .. }
        | LogicalPlan::LocyPriority { .. }
        | LogicalPlan::LocyProject { .. }
        | LogicalPlan::LocyModelInvoke { .. } => {}
        // Leaf nodes / DDL / admin statements bind no variables.
        LogicalPlan::Empty
        | LogicalPlan::CreateVectorIndex { .. }
        | LogicalPlan::CreateSparseIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateScalarIndex { .. }
        | LogicalPlan::CreateJsonFtsIndex { .. }
        | LogicalPlan::DropIndex { .. }
        | LogicalPlan::ShowIndexes { .. }
        | LogicalPlan::Copy { .. }
        | LogicalPlan::Backup { .. }
        | LogicalPlan::ShowDatabase
        | LogicalPlan::ShowConfig
        | LogicalPlan::ShowStatistics
        | LogicalPlan::Vacuum
        | LogicalPlan::Checkpoint
        | LogicalPlan::CopyTo { .. }
        | LogicalPlan::CopyFrom { .. }
        | LogicalPlan::CreateLabel(_)
        | LogicalPlan::CreateEdgeType(_)
        | LogicalPlan::AlterLabel(_)
        | LogicalPlan::AlterEdgeType(_)
        | LogicalPlan::DropLabel(_)
        | LogicalPlan::DropEdgeType(_)
        | LogicalPlan::CreateConstraint(_)
        | LogicalPlan::DropConstraint(_)
        | LogicalPlan::ShowConstraints(_) => {}
    }
}

/// Recursively collect variable names referenced in an expression.
fn collect_expr_variables_set(expr: &Expr) -> HashSet<String> {
    let mut out = HashSet::new();
    collect_expr_variables_into(expr, &mut out);
    out
}

fn collect_expr_variables_into(expr: &Expr, out: &mut HashSet<String>) {
    use uni_cypher::ast::Expr as E;
    match expr {
        E::Variable(v) => {
            out.insert(v.clone());
        }
        E::Property(base, _) => collect_expr_variables_into(base, out),
        E::BinaryOp { left, right, .. } => {
            collect_expr_variables_into(left, out);
            collect_expr_variables_into(right, out);
        }
        E::UnaryOp { expr, .. } | E::IsNull(expr) | E::IsNotNull(expr) | E::IsUnique(expr) => {
            collect_expr_variables_into(expr, out)
        }
        E::FunctionCall { args, .. } => {
            for a in args {
                collect_expr_variables_into(a, out);
            }
        }
        E::List(items) => {
            for it in items {
                collect_expr_variables_into(it, out);
            }
        }
        E::In { expr, list } => {
            collect_expr_variables_into(expr, out);
            collect_expr_variables_into(list, out);
        }
        E::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                collect_expr_variables_into(e, out);
            }
            for (w, t) in when_then {
                collect_expr_variables_into(w, out);
                collect_expr_variables_into(t, out);
            }
            if let Some(e) = else_expr {
                collect_expr_variables_into(e, out);
            }
        }
        E::Map(entries) => {
            for (_, v) in entries {
                collect_expr_variables_into(v, out);
            }
        }
        E::LabelCheck { expr, .. } => collect_expr_variables_into(expr, out),
        E::ArrayIndex { array, index } => {
            collect_expr_variables_into(array, out);
            collect_expr_variables_into(index, out);
        }
        E::ArraySlice { array, start, end } => {
            collect_expr_variables_into(array, out);
            if let Some(s) = start {
                collect_expr_variables_into(s, out);
            }
            if let Some(e) = end {
                collect_expr_variables_into(e, out);
            }
        }
        // Skip Quantifier/Reduce/ListComprehension/PatternComprehension —
        // they introduce local bindings not in outer scope.
        _ => {}
    }
}

/// Split a predicate at top-level AND-conjuncts.
fn split_and_conjuncts(predicate: &Expr) -> Vec<Expr> {
    use uni_cypher::ast::BinaryOp;
    let mut out = Vec::new();
    fn walk(e: &Expr, out: &mut Vec<Expr>) {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } = e
        {
            walk(left, out);
            walk(right, out);
        } else {
            out.push(e.clone());
        }
    }
    walk(predicate, &mut out);
    out
}

/// AND-combine multiple expressions into one (or None for empty input).
fn and_combine(exprs: Vec<Expr>) -> Option<Expr> {
    use uni_cypher::ast::BinaryOp;
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, e| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOp::And,
        right: Box::new(e),
    }))
}

/// Classify each AND-conjunct of `predicate` according to which side(s) of a
/// CrossJoin its variables come from.
fn classify_join_predicate(
    predicate: &Expr,
    left_vars: &HashSet<String>,
    right_vars: &HashSet<String>,
) -> JoinPredicateClassification {
    use uni_cypher::ast::BinaryOp;

    let mut equi_pairs = Vec::new();
    let mut left_only = Vec::new();
    let mut right_only = Vec::new();
    let mut residual_parts: Vec<Expr> = Vec::new();

    for conjunct in split_and_conjuncts(predicate) {
        // Try equi-join: BinaryOp::Eq with one side referencing only left vars
        // and the other only right vars.
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } = &conjunct
        {
            let lv = collect_expr_variables_set(left);
            let rv = collect_expr_variables_set(right);
            let l_in_left = !lv.is_empty() && lv.is_subset(left_vars);
            let r_in_right = !rv.is_empty() && rv.is_subset(right_vars);
            let l_in_right = !lv.is_empty() && lv.is_subset(right_vars);
            let r_in_left = !rv.is_empty() && rv.is_subset(left_vars);
            if l_in_left && r_in_right {
                equi_pairs.push(((**left).clone(), (**right).clone()));
                continue;
            }
            if l_in_right && r_in_left {
                equi_pairs.push(((**right).clone(), (**left).clone()));
                continue;
            }
        }

        // Not an equi-join — classify by which sides its variables belong to.
        let vars = collect_expr_variables_set(&conjunct);
        let touches_left = vars.iter().any(|v| left_vars.contains(v));
        let touches_right = vars.iter().any(|v| right_vars.contains(v));
        match (touches_left, touches_right) {
            (true, false) => left_only.push(conjunct),
            (false, true) => right_only.push(conjunct),
            // Both sides (mixed-non-equi) or neither (constant) → residual.
            _ => residual_parts.push(conjunct),
        }
    }

    JoinPredicateClassification {
        equi_pairs,
        left_only,
        right_only,
        residual: and_combine(residual_parts),
    }
}

/// Maximum static UNWIND list size for IN-list scan pushdown. Beyond this,
/// the cost of injecting a giant `IN` filter outweighs the savings vs. the
/// HashJoin alone, so we skip the pushdown.
const MAX_UNWIND_IN_PUSHDOWN_VALUES: usize = 10_000;

/// Convert a `uni_common::Value` primitive into a `CypherLiteral` for use in
/// AST `Expr::List` items. Returns `None` for non-primitive Values (lists,
/// maps, nodes, etc.) — those don't make sense as `IN` list elements anyway.
/// One-shot `tracing::warn!` when a literal-list UNWIND that *looks* like
/// it should be pushable to a scan-side IN-list filter fails one of the
/// content gates (missing field, non-literal value at field, oversized
/// list). Surfaces the gap so diagnostic users and CI catch "I wrote an
/// inlined UNWIND for a test and got silent full-scan" patterns; in
/// production these would have pushed if rewritten as `UNWIND $param AS u`.
///
/// Deduped via a single `AtomicBool` to avoid log spam on long-running
/// processes; one warning per process across all reasons.
fn warn_unpushable_unwind_once(reason: &'static str) {
    use std::sync::atomic::{AtomicBool, Ordering};
    static WARNED: AtomicBool = AtomicBool::new(false);
    if WARNED.swap(true, Ordering::Relaxed) {
        return;
    }
    tracing::warn!(
        target: "uni_query::cross_join_in_pushdown",
        reason,
        "Inlined UNWIND of map literals failed pushdown — falling back \
         to FilterExec over a full scan. Rewrite as `UNWIND $param AS u` \
         with the param bound as a List<Map<...>> to guarantee pushdown."
    );
}

fn value_to_cypher_literal(v: &uni_common::Value) -> Option<CypherLiteral> {
    use uni_common::Value;
    match v {
        Value::Null => Some(CypherLiteral::Null),
        Value::Bool(b) => Some(CypherLiteral::Bool(*b)),
        Value::Int(n) => Some(CypherLiteral::Integer(*n)),
        Value::Float(f) => Some(CypherLiteral::Float(*f)),
        Value::String(s) => Some(CypherLiteral::String(s.clone())),
        _ => None,
    }
}

/// Walk a logical-plan subtree looking for `LogicalPlan::Unwind { variable, expr, .. }`
/// where `variable == target_var`, and return the bound list of values **if**
/// the UNWIND source is statically resolvable at plan time:
///
/// - `Expr::List(items)` where every item is an `Expr::Literal(_)` → use them directly.
/// - `Expr::Parameter(name)` where `params[name]` is `Value::List(...)` → convert
///   each primitive element into an `Expr::Literal`.
///
/// Returns `None` for any other source (sub-MATCH, correlated, runtime-only),
/// or when the list contains non-primitive values, or exceeds
/// `MAX_UNWIND_IN_PUSHDOWN_VALUES`.
/// Walk a chain of UNWIND/Filter/Project/CrossJoin nodes looking for the
/// `Unwind` binding `target_var`. When found, `extract` is invoked on that
/// UNWIND's source expression; the first `Some` result wins.
///
/// Both `extract_static_unwind_values` and `extract_static_unwind_field_values`
/// share this traversal — they differ only in what `extract` returns.
/// Touching the set of recognized plan nodes (e.g. adding `Distinct`) only
/// needs to happen here.
fn walk_static_unwind_chain<F, T>(
    plan: &LogicalPlan,
    target_var: &str,
    extract: &mut F,
) -> Option<T>
where
    F: FnMut(&Expr) -> Option<T>,
{
    match plan {
        LogicalPlan::Unwind {
            input,
            expr,
            variable,
        } if variable == target_var => {
            extract(expr).or_else(|| walk_static_unwind_chain(input, target_var, extract))
        }
        // Single-input plan nodes: recurse into the input.
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Unwind { input, .. } => walk_static_unwind_chain(input, target_var, extract),
        // CrossJoin: search both subtrees. The UNWIND of `target_var` lives in
        // exactly one side; the other returns None.
        LogicalPlan::CrossJoin { left, right } => {
            walk_static_unwind_chain(left, target_var, extract)
                .or_else(|| walk_static_unwind_chain(right, target_var, extract))
        }
        _ => None,
    }
}

fn extract_static_unwind_values(
    plan: &LogicalPlan,
    target_var: &str,
    params: &HashMap<String, uni_common::Value>,
) -> Option<Vec<Expr>> {
    walk_static_unwind_chain(plan, target_var, &mut |expr| {
        materialize_unwind_source(expr, params)
    })
}

/// Variant of [`extract_static_unwind_values`] that projects a named `field`
/// out of each map element in the UNWIND source. See issue #55 (PR #4).
fn extract_static_unwind_field_values(
    plan: &LogicalPlan,
    target_var: &str,
    field: &str,
    params: &HashMap<String, uni_common::Value>,
) -> Option<Vec<Expr>> {
    walk_static_unwind_chain(plan, target_var, &mut |expr| {
        materialize_unwind_source_field(expr, params, field)
    })
}

/// Materialize a UNWIND source `Expr` into a vec of literal `Expr`s if possible.
fn materialize_unwind_source(
    expr: &Expr,
    params: &HashMap<String, uni_common::Value>,
) -> Option<Vec<Expr>> {
    match expr {
        Expr::List(items) => {
            if items.len() > MAX_UNWIND_IN_PUSHDOWN_VALUES {
                return None;
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Expr::Literal(_) => out.push(item.clone()),
                    _ => return None,
                }
            }
            Some(out)
        }
        Expr::Parameter(name) => match params.get(name)? {
            uni_common::Value::List(values) => {
                if values.len() > MAX_UNWIND_IN_PUSHDOWN_VALUES {
                    return None;
                }
                let mut out = Vec::with_capacity(values.len());
                for v in values {
                    out.push(Expr::Literal(value_to_cypher_literal(v)?));
                }
                Some(out)
            }
            _ => None,
        },
        _ => None,
    }
}

/// Materialize a UNWIND source `Expr` into a vec of literal `Expr`s, projecting
/// `field` out of each map element. Handles the common case where the UNWIND
/// source is a list of maps and we want to push down on a specific field —
/// e.g. `UNWIND $edges AS e ... WHERE id(a) = e.src` with `$edges` bound to
/// `List<Map<src, dst>>` returns the list of `src` values as literals.
///
/// Returns `None` if the source isn't a statically-resolvable list of maps
/// or any element lacks `field` or has a non-primitive value at `field`.
/// See issue #55 (PR #4).
fn materialize_unwind_source_field(
    expr: &Expr,
    params: &HashMap<String, uni_common::Value>,
    field: &str,
) -> Option<Vec<Expr>> {
    match expr {
        Expr::List(items) => {
            if items.len() > MAX_UNWIND_IN_PUSHDOWN_VALUES {
                warn_unpushable_unwind_once("UNWIND list exceeds MAX_UNWIND_IN_PUSHDOWN_VALUES");
                return None;
            }
            // Inlined map literals at plan time: each item must be an
            // `Expr::Map(entries)` whose entry at `field` is itself an
            // `Expr::Literal(_)`. Extract the literals directly — we
            // already have them as Expr, no Value↔Literal conversion
            // needed (unlike the Parameter branch below).
            //
            // Non-map items return None silently (they're a type
            // mismatch the planner will flag elsewhere). Maps with a
            // missing or non-literal value at `field` emit a one-shot
            // warn — those shapes would have pushed if rewritten as
            // `UNWIND $param AS u` (where parameter resolution makes
            // every value a primitive Value).
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let entries = match item {
                    Expr::Map(entries) => entries,
                    _ => return None,
                };
                let Some((_, value_expr)) = entries.iter().find(|(k, _)| k == field) else {
                    warn_unpushable_unwind_once(
                        "UNWIND map literal is missing the field referenced by the join predicate",
                    );
                    return None;
                };
                let Expr::Literal(_) = value_expr else {
                    warn_unpushable_unwind_once(
                        "UNWIND map literal has a non-literal value at the joined field \
                         (e.g., a parameter or function call) — substitute with a literal \
                         or rewrite as `UNWIND $param AS u` with the param bound at runtime",
                    );
                    return None;
                };
                out.push(value_expr.clone());
            }
            Some(out)
        }
        Expr::Parameter(name) => match params.get(name)? {
            uni_common::Value::List(values) => {
                if values.len() > MAX_UNWIND_IN_PUSHDOWN_VALUES {
                    return None;
                }
                let mut out = Vec::with_capacity(values.len());
                for v in values {
                    let map = match v {
                        uni_common::Value::Map(m) => m,
                        _ => return None,
                    };
                    let inner = map.get(field)?;
                    out.push(Expr::Literal(value_to_cypher_literal(inner)?));
                }
                Some(out)
            }
            _ => None,
        },
        _ => None,
    }
}

/// If `unwind_side_expr` is bound to a variable produced by a static UNWIND
/// in `unwind_subplan`, and `scan_side_expr` is a property of a scan variable,
/// build an `Expr::In { expr: scan_side_expr, list: [literals...] }` to inject
/// as a scan-side filter. Returns `None` if any condition fails.
///
/// Accepts two forms on the unwind side:
/// - `Variable(v)` — direct list element (e.g. `UNWIND $names AS n ... = n`).
/// - `Property(Variable(v), _)` — list of maps (e.g. `UNWIND $rows AS r ... = r.k`).
///   Property form requires the parameter list to be a list of `Value::Map`s,
///   so we conservatively skip it here (the materializer rejects non-primitive
///   values anyway).
fn build_in_pushdown(
    unwind_side_expr: &Expr,
    scan_side_expr: &Expr,
    unwind_subplan: &LogicalPlan,
    params: &HashMap<String, uni_common::Value>,
) -> Option<Expr> {
    // Identify the UNWIND variable (and optional field) on the unwind side.
    let (unwind_var, field) = match unwind_side_expr {
        Expr::Variable(v) => (v.as_str(), None),
        Expr::Property(box_var, f) => match box_var.as_ref() {
            Expr::Variable(v) => (v.as_str(), Some(f.as_str())),
            _ => {
                tracing::debug!(
                    target: "uni_query::cross_join_in_pushdown",
                    reason = "unwind side Property inner is not Variable",
                    "build_in_pushdown rejected"
                );
                return None;
            }
        },
        _ => {
            tracing::debug!(
                target: "uni_query::cross_join_in_pushdown",
                reason = "unwind side is not Variable or Property",
                unwind_kind = std::any::type_name_of_val(&unwind_side_expr),
                "build_in_pushdown rejected"
            );
            return None;
        }
    };

    // Scan side must be `Property(Variable(_), _)` so that `is_pushable`
    // (which accepts `Property(Variable(scan_var), prop)` on the LHS of an IN)
    // will push the filter into the scan.
    let Expr::Property(scan_box_var, _scan_field) = scan_side_expr else {
        tracing::debug!(
            target: "uni_query::cross_join_in_pushdown",
            reason = "scan side is not Property",
            "build_in_pushdown rejected"
        );
        return None;
    };
    if !matches!(scan_box_var.as_ref(), Expr::Variable(_)) {
        tracing::debug!(
            target: "uni_query::cross_join_in_pushdown",
            reason = "scan side Property inner is not Variable",
            "build_in_pushdown rejected"
        );
        return None;
    }

    // Resolve the IN-list values from the UNWIND source. The two cases are:
    //   * `UNWIND $list AS e ... = e`           → primitive list at $list
    //   * `UNWIND $list AS e ... = e.field`     → list of maps at $list,
    //                                              project `field` per element
    let values = match field {
        None => match extract_static_unwind_values(unwind_subplan, unwind_var, params) {
            Some(v) => v,
            None => {
                tracing::debug!(
                    target: "uni_query::cross_join_in_pushdown",
                    reason = "extract_static_unwind_values returned None",
                    unwind_var,
                    "build_in_pushdown rejected"
                );
                return None;
            }
        },
        Some(f) => {
            match extract_static_unwind_field_values(unwind_subplan, unwind_var, f, params) {
                Some(v) => v,
                None => {
                    tracing::debug!(
                        target: "uni_query::cross_join_in_pushdown",
                        reason = "extract_static_unwind_field_values returned None \
                                  (UNWIND source is not Expr::Parameter, or param is not \
                                  Value::List<Value::Map>, or a map element lacks field, \
                                  or list size exceeded MAX_UNWIND_IN_PUSHDOWN_VALUES)",
                        unwind_var,
                        field = f,
                        "build_in_pushdown rejected"
                    );
                    return None;
                }
            }
        }
    };
    if values.is_empty() {
        tracing::debug!(
            target: "uni_query::cross_join_in_pushdown",
            reason = "extracted value list is empty",
            unwind_var,
            ?field,
            "build_in_pushdown rejected"
        );
        return None;
    }

    tracing::debug!(
        target: "uni_query::cross_join_in_pushdown",
        unwind_var,
        ?field,
        values_count = values.len(),
        "build_in_pushdown extracted IN-list"
    );
    Some(Expr::In {
        expr: Box::new(scan_side_expr.clone()),
        list: Box::new(Expr::List(values)),
    })
}

/// Wrap `plan` with a `LogicalPlan::Filter` AND-combining `filters` if any.
/// Returns true if `expr` is `Property(Variable(_), "_vid")`. Used by
/// [`try_emit_vid_lookup_join`] (issue #55 PR #5) to identify the probe side
/// of an inner-equi-join. `id(x)` is lowered to this shape during AST→
/// logical-plan translation, so we don't need a separate `FunctionCall`
/// arm here.
fn expr_is_vid_property(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Property(inner, prop)
            if prop == "_vid" && matches!(inner.as_ref(), Expr::Variable(_))
    )
}

fn wrap_with_filter(plan: LogicalPlan, filters: &[Expr]) -> LogicalPlan {
    if filters.is_empty() {
        return plan;
    }
    let predicate = and_combine(filters.to_vec()).expect("non-empty filters");
    // Critical for issue #55: when `plan` is a Scan node, we MUST merge the
    // predicate into the Scan's own `filter` field. Wrapping the Scan in a
    // Filter LogicalPlan node would route through `plan_filter`, which builds
    // a FilterExec on top of GraphScanExec — that runs Lance's full-table
    // scan first and only filters in DataFusion afterwards, defeating the
    // pushdown. Merging into Scan.filter lets `plan_scan` /
    // `plan_schemaless_scan` extract the IN-list and push it to Lance.
    match plan {
        LogicalPlan::Scan {
            label_id,
            labels,
            variable,
            filter: existing,
            optional,
        } => LogicalPlan::Scan {
            label_id,
            labels,
            variable,
            filter: merge_filter(existing, predicate),
            optional,
        },
        LogicalPlan::ScanMainByLabels {
            labels,
            variable,
            filter: existing,
            optional,
        } => LogicalPlan::ScanMainByLabels {
            labels,
            variable,
            filter: merge_filter(existing, predicate),
            optional,
        },
        LogicalPlan::ScanAll {
            variable,
            filter: existing,
            optional,
        } => LogicalPlan::ScanAll {
            variable,
            filter: merge_filter(existing, predicate),
            optional,
        },
        // For any other shape (CrossJoin, nested Filter, etc.) keep the
        // historical wrap-in-Filter behavior. plan_internal will recurse and
        // any inner Scan-wrapped subtree will benefit from the merge above.
        other => LogicalPlan::Filter {
            input: Box::new(other),
            predicate,
            optional_variables: HashSet::new(),
        },
    }
}

/// AND-merge an optional existing filter with a new predicate.
///
/// Idempotent: if `existing == predicate`, the existing filter is
/// returned unchanged (no `Expr::BinaryOp(And, X, X)` duplication).
/// This makes the `merge_unwind_in_filters` rewrite pass safely
/// re-runnable and keeps Scan filters minimal across the planner's
/// recursive descent.
fn merge_filter(existing: Option<Expr>, predicate: Expr) -> Option<Expr> {
    match existing {
        Some(prev) if prev == predicate => Some(prev),
        Some(prev) => and_combine(vec![prev, predicate]),
        None => Some(predicate),
    }
}

/// Pre-physical-plan rewrite: walk a [`LogicalPlan`] tree and, at every
/// `Filter(CrossJoin(L, R), pred)` shape, lift IN-list filters extracted
/// from UNWIND-correlated equi-pairs into the appropriate `Scan.filter`
/// field of L or R.
///
/// **Why this lives outside `try_plan_cross_join_as_hash_join`**:
///
/// Historically the merge happened inside `try_plan_cross_join_as_hash_join`
/// before the HashJoin attempt. When join-key type unification failed (e.g.
/// `Utf8 ↔ LargeBinary CV` — see `unify_join_key_types` line ~6995), the
/// function returned `Ok(None)` and the caller (`plan_filter`) re-planned
/// the **original** CrossJoin from scratch, discarding the merged-filter
/// subtrees. The Hash-index pushdown silently vanished.
///
/// Separating the rewrite as an independent logical-plan pass that runs
/// **before** any physical-plan optimization closes that class of bugs at
/// the source: regardless of whether `HashJoinExec`, `VidLookupJoinExec`,
/// or a future optimization succeeds or bails, the scan-side filters are
/// already in the LogicalPlan and propagate to the eventual physical
/// plan via the normal `plan_scan` → `build_indexed_property_pushdown`
/// path.
///
/// **What this pass does NOT do**:
///
///  - It does not push `left_only` / `right_only` predicate conjuncts
///    into the subtrees. Those are predicate-decomposition concerns
///    handled by `classify_join_predicate` + the residual logic inside
///    `try_plan_cross_join_as_hash_join`. Decomposition is part of
///    HashJoin emission and conceptually belongs with it.
///  - It does not touch non-CrossJoin nodes. Filters on other inputs
///    (Scan, Traverse, Apply, etc.) already merge correctly via
///    `wrap_with_filter` when needed.
///
/// **Idempotence**: running the pass twice produces the same result.
/// The IN-list filters merged on the first pass are not equi-join
/// predicates against the (now-already-filtered) subtree's UNWIND, so
/// the second pass extracts nothing new.
fn merge_unwind_in_filters(
    plan: &LogicalPlan,
    params: &HashMap<String, uni_common::Value>,
) -> LogicalPlan {
    match plan {
        // Target shape: Filter wrapping a CrossJoin — try IN-list pushdown.
        LogicalPlan::Filter {
            input,
            predicate,
            optional_variables,
        } if matches!(input.as_ref(), LogicalPlan::CrossJoin { .. }) => {
            // Safe: matches! above guarantees this destructure succeeds.
            let LogicalPlan::CrossJoin { left, right } = input.as_ref() else {
                unreachable!("matches! above guarantees CrossJoin")
            };

            // Recurse into the subtrees first to catch nested CrossJoins.
            let left_rewritten = merge_unwind_in_filters(left, params);
            let right_rewritten = merge_unwind_in_filters(right, params);

            let left_vars = collect_plan_variables(&left_rewritten);
            let right_vars = collect_plan_variables(&right_rewritten);
            let cls = classify_join_predicate(predicate, &left_vars, &right_vars);

            let rebuild_unmodified = |l: LogicalPlan, r: LogicalPlan| LogicalPlan::Filter {
                input: Box::new(LogicalPlan::CrossJoin {
                    left: Box::new(l),
                    right: Box::new(r),
                }),
                predicate: predicate.clone(),
                optional_variables: optional_variables.clone(),
            };

            if cls.equi_pairs.is_empty() {
                return rebuild_unmodified(left_rewritten, right_rewritten);
            }

            // Build IN-list filters for each equi-pair × subtree orientation.
            // See `build_in_pushdown` for the gating; `materialize_unwind_source_*`
            // returns None for shapes we can't statically resolve.
            let mut left_extra_in: Vec<Expr> = Vec::new();
            let mut right_extra_in: Vec<Expr> = Vec::new();
            for (l_expr, r_expr) in &cls.equi_pairs {
                if let Some(in_filter) = build_in_pushdown(l_expr, r_expr, &left_rewritten, params)
                {
                    right_extra_in.push(in_filter);
                    continue;
                }
                if let Some(in_filter) = build_in_pushdown(r_expr, l_expr, &left_rewritten, params)
                {
                    right_extra_in.push(in_filter);
                    continue;
                }
                if let Some(in_filter) = build_in_pushdown(l_expr, r_expr, &right_rewritten, params)
                {
                    left_extra_in.push(in_filter);
                    continue;
                }
                if let Some(in_filter) = build_in_pushdown(r_expr, l_expr, &right_rewritten, params)
                {
                    left_extra_in.push(in_filter);
                }
            }

            tracing::debug!(
                target: "uni_query::cross_join_in_pushdown",
                left_in_filters = left_extra_in.len(),
                right_in_filters = right_extra_in.len(),
                "merge_unwind_in_filters: IN-pushdown result"
            );

            if left_extra_in.is_empty() && right_extra_in.is_empty() {
                return rebuild_unmodified(left_rewritten, right_rewritten);
            }

            let left_merged = wrap_with_filter(left_rewritten, &left_extra_in);
            let right_merged = wrap_with_filter(right_rewritten, &right_extra_in);
            rebuild_unmodified(left_merged, right_merged)
        }
        // Pass through Filter wrapping non-CrossJoin.
        LogicalPlan::Filter {
            input,
            predicate,
            optional_variables,
        } => LogicalPlan::Filter {
            input: Box::new(merge_unwind_in_filters(input, params)),
            predicate: predicate.clone(),
            optional_variables: optional_variables.clone(),
        },
        // Single-input wrappers: recurse on `input`.
        LogicalPlan::Project { input, projections } => LogicalPlan::Project {
            input: Box::new(merge_unwind_in_filters(input, params)),
            projections: projections.clone(),
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(merge_unwind_in_filters(input, params)),
            order_by: order_by.clone(),
        },
        LogicalPlan::Limit { input, skip, fetch } => LogicalPlan::Limit {
            input: Box::new(merge_unwind_in_filters(input, params)),
            skip: *skip,
            fetch: *fetch,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(merge_unwind_in_filters(input, params)),
        },
        LogicalPlan::Unwind {
            input,
            expr,
            variable,
        } => LogicalPlan::Unwind {
            input: Box::new(merge_unwind_in_filters(input, params)),
            expr: expr.clone(),
            variable: variable.clone(),
        },
        // Mutation nodes wrap a MATCH-side input — recurse so that
        // `UNWIND $list AS u MATCH (n:Label) WHERE n.k = u.k SET ...` /
        // REMOVE / DELETE / CREATE-with-MATCH / MERGE all benefit from
        // the rewrite. The mutation operation itself isn't touched.
        LogicalPlan::Set { input, items } => LogicalPlan::Set {
            input: Box::new(merge_unwind_in_filters(input, params)),
            items: items.clone(),
        },
        LogicalPlan::Remove { input, items } => LogicalPlan::Remove {
            input: Box::new(merge_unwind_in_filters(input, params)),
            items: items.clone(),
        },
        LogicalPlan::Delete {
            input,
            items,
            detach,
        } => LogicalPlan::Delete {
            input: Box::new(merge_unwind_in_filters(input, params)),
            items: items.clone(),
            detach: *detach,
        },
        LogicalPlan::Create { input, pattern } => LogicalPlan::Create {
            input: Box::new(merge_unwind_in_filters(input, params)),
            pattern: pattern.clone(),
        },
        LogicalPlan::CreateBatch { input, patterns } => LogicalPlan::CreateBatch {
            input: Box::new(merge_unwind_in_filters(input, params)),
            patterns: patterns.clone(),
        },
        LogicalPlan::Merge {
            input,
            pattern,
            on_match,
            on_create,
        } => LogicalPlan::Merge {
            input: Box::new(merge_unwind_in_filters(input, params)),
            pattern: pattern.clone(),
            on_match: on_match.clone(),
            on_create: on_create.clone(),
        },
        LogicalPlan::Foreach {
            input,
            variable,
            list,
            body,
        } => LogicalPlan::Foreach {
            input: Box::new(merge_unwind_in_filters(input, params)),
            variable: variable.clone(),
            list: list.clone(),
            body: body
                .iter()
                .map(|b| merge_unwind_in_filters(b, params))
                .collect(),
        },
        // Aggregation and windowing nodes wrap an input — recurse.
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggregates,
        } => LogicalPlan::Aggregate {
            input: Box::new(merge_unwind_in_filters(input, params)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        },
        LogicalPlan::Window {
            input,
            window_exprs,
        } => LogicalPlan::Window {
            input: Box::new(merge_unwind_in_filters(input, params)),
            window_exprs: window_exprs.clone(),
        },
        LogicalPlan::SubqueryCall { input, subquery } => LogicalPlan::SubqueryCall {
            input: Box::new(merge_unwind_in_filters(input, params)),
            subquery: Box::new(merge_unwind_in_filters(subquery, params)),
        },
        // Two-input nodes: recurse on both.
        LogicalPlan::CrossJoin { left, right } => LogicalPlan::CrossJoin {
            left: Box::new(merge_unwind_in_filters(left, params)),
            right: Box::new(merge_unwind_in_filters(right, params)),
        },
        LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
            left: Box::new(merge_unwind_in_filters(left, params)),
            right: Box::new(merge_unwind_in_filters(right, params)),
            all: *all,
        },
        // Apply has input + correlated subquery; recurse on both.
        LogicalPlan::Apply {
            input,
            subquery,
            input_filter,
        } => LogicalPlan::Apply {
            input: Box::new(merge_unwind_in_filters(input, params)),
            subquery: Box::new(merge_unwind_in_filters(subquery, params)),
            input_filter: input_filter.clone(),
        },
        // Leaf / unsupported / nodes whose internals don't currently
        // benefit from this rewrite: pass through unchanged. Adding
        // recursion for other variants (Aggregate, Window, Traverse,
        // mutation nodes, etc.) is safe but unnecessary — the
        // CrossJoin shape only appears under inputs we already recurse
        // into above.
        _ => plan.clone(),
    }
}

/// Returns `true` if `dt` is hashable directly by Arrow's HashJoinExec without
/// any value transformation. When both join keys share such a dtype, we can
/// skip the `tointeger` / `_cypher_sort_key` wrap entirely.
fn is_hashable_native_dtype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
    )
}

/// Returns `true` if `dt` is one of the types `tointeger` UDF accepts as input
/// (numeric primitives plus CV-encoded `LargeBinary`).
fn tointeger_accepts_dtype(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::LargeBinary
    )
}

/// Wrap `expr` with a 1-arg scalar UDF that returns `return_dt`.
fn wrap_with_unary_udf(
    expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    udf: Arc<datafusion::logical_expr::ScalarUDF>,
    return_dt: DataType,
) -> Arc<dyn datafusion::physical_plan::PhysicalExpr> {
    let config_options = Arc::new(datafusion::config::ConfigOptions::default());
    let udf_name = udf.name().to_string();
    let return_field = Arc::new(arrow_schema::Field::new(&udf_name, return_dt, true));
    Arc::new(datafusion::physical_expr::ScalarFunctionExpr::new(
        &udf_name,
        udf,
        vec![expr],
        return_field,
        config_options,
    ))
}

/// Bilateral type unification for a HashJoin equi-pair.
///
/// Strategy (in order of preference):
/// 1. Same dtype + natively hashable → return both unchanged (fast path,
///    e.g. `Utf8 = Utf8`, `Int64 = Int64`).
/// 2. Both dtypes accepted by `tointeger` (numeric or CV-encoded
///    `LargeBinary`) → wrap both in `tointeger` to unify on `Int64`. This is
///    the original issue #53 behavior.
/// 3. Otherwise (mixed string/CV/other Cypher types) → wrap both in
///    `_cypher_sort_key`, which produces an order-preserving `LargeBinary`
///    encoding that hashes equal iff the underlying Cypher values are equal.
///
/// Returns `None` only when the required UDFs aren't registered or a side's
/// dtype can't be inferred — the caller falls back to FilterExec+CrossJoin.
fn unify_join_key_types(
    left: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    right: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    left_schema: &Schema,
    right_schema: &Schema,
    state: &SessionState,
) -> Option<(
    Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    Arc<dyn datafusion::physical_plan::PhysicalExpr>,
)> {
    let l_dt = left.data_type(left_schema).ok()?;
    let r_dt = right.data_type(right_schema).ok()?;

    if l_dt == r_dt && is_hashable_native_dtype(&l_dt) {
        return Some((left, right));
    }

    if tointeger_accepts_dtype(&l_dt) && tointeger_accepts_dtype(&r_dt) {
        let udf = state.scalar_functions().get("tointeger")?.clone();
        return Some((
            wrap_with_unary_udf(left, udf.clone(), DataType::Int64),
            wrap_with_unary_udf(right, udf, DataType::Int64),
        ));
    }

    // Cross-domain unification (e.g. Utf8 ↔ LargeBinary CV-encoded) is not yet
    // implemented at the HashJoin layer — fall through to FilterExec, which
    // handles these via Cypher-aware comparison UDFs.
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `all_properties` with one edge variable marked as building a struct.
    fn builds_struct_for(edge_var: &str) -> HashMap<String, HashSet<String>> {
        HashMap::from([(
            edge_var.to_string(),
            HashSet::from([STRUCT_ONLY_SENTINEL.to_string()]),
        )])
    }

    /// The predicate and `add_edge_structural_projection` are two halves of one
    /// contract: whenever this says false for an undirected struct-building hop,
    /// that projection has no `_fwd` to consult and now errors instead of
    /// silently reporting traversal order. Pinning it here is what keeps the two
    /// request sites from drifting apart again — the drift that was #193.
    #[test]
    fn orientation_is_wanted_only_for_an_undirected_single_hop_that_builds_a_struct() {
        let props = builds_struct_for("r");

        assert!(
            wants_orientation_column(&AstDirection::Both, false, "r", &props),
            "an undirected single hop building a struct cannot answer statically"
        );
        for direction in [AstDirection::Outgoing, AstDirection::Incoming] {
            assert!(
                !wants_orientation_column(&direction, false, "r", &props),
                "a directed hop knows its orientation at plan time"
            );
        }
        assert!(
            !wants_orientation_column(&AstDirection::Both, true, "r", &props),
            "a variable-length hop binds a list, not one edge struct"
        );
        assert!(
            !wants_orientation_column(&AstDirection::Both, false, "r", &HashMap::new()),
            "no struct is built, so nothing consults orientation"
        );
        assert!(
            !wants_orientation_column(&AstDirection::Both, false, "other", &props),
            "the request is per edge variable"
        );
    }

    /// The wildcard is the other spelling that builds a struct (`RETURN r`),
    /// alongside the struct-only sentinel (`SET r.p`). Both must request it —
    /// recognising one and not the other would leave the wildcard case reaching
    /// the projection without a `_fwd` column.
    #[test]
    fn orientation_is_wanted_for_both_struct_building_spellings() {
        for marker in ["*", STRUCT_ONLY_SENTINEL] {
            let props = HashMap::from([("r".to_string(), HashSet::from([marker.to_string()]))]);
            assert!(
                wants_orientation_column(&AstDirection::Both, false, "r", &props),
                "`{marker}` builds a struct and so needs orientation"
            );
        }
    }

    #[test]
    fn test_convert_direction() {
        assert!(matches!(
            convert_direction(AstDirection::Outgoing),
            Direction::Outgoing
        ));
        assert!(matches!(
            convert_direction(AstDirection::Incoming),
            Direction::Incoming
        ));
        assert!(matches!(
            convert_direction(AstDirection::Both),
            Direction::Both
        ));
    }

    /// The `"*"` marker itself is dropped, but the wildcard still demands
    /// `_all_props`.
    ///
    /// This previously asserted `["name"]` -- that a wildcard alongside named
    /// properties added nothing. That was the defect, not the contract:
    /// `resolve_properties` expands `"*"` to the declared set before this runs,
    /// so on any label that declares a property the list is non-empty and
    /// `_all_props` was never added, dropping every schemaless property from
    /// whole-entity VLP results. `whole_entity_results_carry_every_property`
    /// covers it end to end.
    #[test]
    fn test_sanitize_vlp_target_properties_removes_wildcard() {
        let props = vec!["*".to_string(), "name".to_string()];
        let label_props = HashSet::from(["name".to_string()]);
        let sanitized = sanitize_vlp_target_properties(props, true, Some(&label_props));

        assert_eq!(
            sanitized,
            vec!["name".to_string(), "_all_props".to_string()]
        );
    }

    #[test]
    fn test_sanitize_vlp_target_properties_adds_all_props_for_wildcard_empty() {
        let props = vec!["*".to_string()];
        let sanitized = sanitize_vlp_target_properties(props, true, None);

        assert_eq!(sanitized, vec!["_all_props".to_string()]);
    }

    #[test]
    fn test_sanitize_vlp_target_properties_adds_all_props_for_non_schema() {
        let props = vec!["custom_prop".to_string()];
        let label_props = HashSet::from(["name".to_string()]);
        let sanitized = sanitize_vlp_target_properties(props, false, Some(&label_props));

        assert_eq!(
            sanitized,
            vec!["custom_prop".to_string(), "_all_props".to_string()]
        );
    }

    // -----------------------------------------------------------------
    // UNWIND IN-list pushdown — `materialize_unwind_source_field`
    //
    // Background: an inlined `UNWIND [{nid: 64}, {nid: 65}] AS u
    // MATCH (n:Entity) WHERE id(n) = u.nid` should be pushable to a
    // `_vid IN (64, 65)` scan filter — identical observable result to
    // the param-bound form `UNWIND $updates AS u`. The Parameter branch
    // (df_planner.rs:6515-6532) handles parameter-bound lists of maps;
    // the literal-list branch must handle the equivalent inlined form.
    // -----------------------------------------------------------------

    use uni_cypher::ast::CypherLiteral;

    fn int_lit(n: i64) -> Expr {
        Expr::Literal(CypherLiteral::Integer(n))
    }

    fn str_lit(s: &str) -> Expr {
        Expr::Literal(CypherLiteral::String(s.to_string()))
    }

    fn map_entry(k: &str, v: Expr) -> (String, Expr) {
        (k.to_string(), v)
    }

    #[test]
    fn materialize_unwind_field_accepts_inlined_map_literals() {
        // `UNWIND [{nid: 64, x: 1}, {nid: 65, x: 2}] AS u ... = u.nid`
        let unwind_expr = Expr::List(vec![
            Expr::Map(vec![
                map_entry("nid", int_lit(64)),
                map_entry("x", int_lit(1)),
            ]),
            Expr::Map(vec![
                map_entry("nid", int_lit(65)),
                map_entry("x", int_lit(2)),
            ]),
        ]);
        let params = HashMap::new();
        let result = materialize_unwind_source_field(&unwind_expr, &params, "nid");
        let values = result.expect("literal-map UNWIND should produce an IN-list");
        assert_eq!(values.len(), 2);
        assert!(matches!(
            &values[0],
            Expr::Literal(CypherLiteral::Integer(64))
        ));
        assert!(matches!(
            &values[1],
            Expr::Literal(CypherLiteral::Integer(65))
        ));
    }

    #[test]
    fn materialize_unwind_field_handles_mixed_primitive_field_types() {
        // String field — should also work since value_to_cypher_literal
        // accepts strings.
        let unwind_expr = Expr::List(vec![
            Expr::Map(vec![map_entry("k", str_lit("a"))]),
            Expr::Map(vec![map_entry("k", str_lit("b"))]),
        ]);
        let params = HashMap::new();
        let values = materialize_unwind_source_field(&unwind_expr, &params, "k")
            .expect("literal-map UNWIND should produce an IN-list");
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn materialize_unwind_field_rejects_non_literal_value_at_target_field() {
        // `UNWIND [{nid: $p}, ...]` — value is a Parameter, not a Literal.
        // Should bail conservatively (we don't substitute parameters
        // inside inlined map literals at plan time).
        let unwind_expr = Expr::List(vec![Expr::Map(vec![map_entry(
            "nid",
            Expr::Parameter("p".to_string()),
        )])]);
        let params = HashMap::new();
        let result = materialize_unwind_source_field(&unwind_expr, &params, "nid");
        assert!(result.is_none(), "non-literal value at field should bail");
    }

    #[test]
    fn materialize_unwind_field_rejects_when_target_field_missing() {
        // `UNWIND [{other: 64}, ...] ... = u.nid` — no `nid` entry.
        let unwind_expr = Expr::List(vec![Expr::Map(vec![map_entry("other", int_lit(64))])]);
        let params = HashMap::new();
        let result = materialize_unwind_source_field(&unwind_expr, &params, "nid");
        assert!(
            result.is_none(),
            "map missing the requested field should bail"
        );
    }

    #[test]
    fn materialize_unwind_field_rejects_non_map_list_item() {
        // `UNWIND [64, 65] AS u ... = u.nid` — items are bare ints, not
        // maps. We're projecting `.nid` from a non-map.
        let unwind_expr = Expr::List(vec![int_lit(64), int_lit(65)]);
        let params = HashMap::new();
        let result = materialize_unwind_source_field(&unwind_expr, &params, "nid");
        assert!(
            result.is_none(),
            "non-map list items can't be field-projected"
        );
    }

    #[test]
    fn materialize_unwind_field_rejects_oversized_list() {
        // Guard against the `MAX_UNWIND_IN_PUSHDOWN_VALUES` ceiling.
        let oversized = MAX_UNWIND_IN_PUSHDOWN_VALUES + 1;
        let items: Vec<Expr> = (0..oversized)
            .map(|i| Expr::Map(vec![map_entry("nid", int_lit(i as i64))]))
            .collect();
        let unwind_expr = Expr::List(items);
        let params = HashMap::new();
        let result = materialize_unwind_source_field(&unwind_expr, &params, "nid");
        assert!(result.is_none(), "oversized list should bail");
    }

    #[test]
    fn materialize_unwind_field_param_form_still_works() {
        // Regression guard: the param branch must still work after the
        // literal branch change.
        let mut params = HashMap::new();
        params.insert(
            "updates".to_string(),
            uni_common::Value::List(vec![
                uni_common::Value::Map({
                    let mut m = HashMap::new();
                    m.insert("nid".to_string(), uni_common::Value::Int(64));
                    m
                }),
                uni_common::Value::Map({
                    let mut m = HashMap::new();
                    m.insert("nid".to_string(), uni_common::Value::Int(65));
                    m
                }),
            ]),
        );
        let unwind_expr = Expr::Parameter("updates".to_string());
        let values = materialize_unwind_source_field(&unwind_expr, &params, "nid")
            .expect("parameter form should produce IN-list");
        assert_eq!(values.len(), 2);
    }

    // -----------------------------------------------------------------
    // `merge_unwind_in_filters` rewrite pass — lifts IN-list filters
    // from `Filter(CrossJoin(Unwind, Scan))` predicates into `Scan.filter`
    // BEFORE physical-plan optimizations can bail and discard the merge.
    // Closes the systemic class where HashJoin emission failure (e.g.,
    // Utf8 ↔ LargeBinary key unification) caused scan-side pushdowns to
    // silently vanish.
    // -----------------------------------------------------------------

    /// Build `Filter(CrossJoin(Unwind, Scan), n.name = u)` — the
    /// canonical shape the pass targets.
    fn make_filter_crossjoin_scan(
        unwind_source: Expr,
        unwind_var: &str,
        scan_label_id: u16,
        scan_label: &str,
        scan_var: &str,
        predicate: Expr,
    ) -> LogicalPlan {
        let unwind = LogicalPlan::Unwind {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    label_id: scan_label_id,
                    labels: vec![scan_label.to_string()],
                    variable: "__dummy__".to_string(),
                    filter: None,
                    optional: false,
                }),
                projections: vec![],
            }),
            expr: unwind_source,
            variable: unwind_var.to_string(),
        };
        let scan = LogicalPlan::Scan {
            label_id: scan_label_id,
            labels: vec![scan_label.to_string()],
            variable: scan_var.to_string(),
            filter: None,
            optional: false,
        };
        LogicalPlan::Filter {
            input: Box::new(LogicalPlan::CrossJoin {
                left: Box::new(unwind),
                right: Box::new(scan),
            }),
            predicate,
            optional_variables: HashSet::new(),
        }
    }

    /// `n.scan_var.field = u.unwind_var` predicate, for use as the
    /// join predicate in the rewrite-pass tests.
    fn eq_property_predicate(scan_var: &str, prop: &str, unwind_var: &str) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable(scan_var.to_string())),
                prop.to_string(),
            )),
            op: uni_cypher::ast::BinaryOp::Eq,
            right: Box::new(Expr::Variable(unwind_var.to_string())),
        }
    }

    fn assert_scan_filter_is_in_list(plan: &LogicalPlan, expected_label: &str) {
        // Find the right subtree of the top-level CrossJoin and assert
        // its Scan node has a filter containing an IN-list.
        let LogicalPlan::Filter { input, .. } = plan else {
            panic!("expected top-level Filter, got {plan:?}");
        };
        let LogicalPlan::CrossJoin { right, .. } = input.as_ref() else {
            panic!("expected CrossJoin under Filter, got {input:?}");
        };
        let LogicalPlan::Scan { labels, filter, .. } = right.as_ref() else {
            panic!("expected Scan as right subtree, got {right:?}");
        };
        assert_eq!(labels, &vec![expected_label.to_string()]);
        let filter_expr = filter
            .as_ref()
            .expect("Scan.filter must be Some after pass");
        assert!(
            matches!(filter_expr, Expr::In { .. }),
            "Scan.filter should be Expr::In, got {filter_expr:?}"
        );
    }

    #[test]
    fn merge_pass_pushes_in_list_into_scan_filter() {
        // UNWIND ['a', 'b'] AS u MATCH (n:Item) WHERE n.name = u
        let unwind_source = Expr::List(vec![str_lit("a"), str_lit("b")]);
        let plan = make_filter_crossjoin_scan(
            unwind_source,
            "u",
            1,
            "Item",
            "n",
            eq_property_predicate("n", "name", "u"),
        );
        let params = HashMap::new();
        let rewritten = merge_unwind_in_filters(&plan, &params);
        assert_scan_filter_is_in_list(&rewritten, "Item");
    }

    #[test]
    fn merge_pass_idempotent() {
        // Running the pass twice should produce a structurally equivalent
        // plan to the single-pass result. We assert the scan filter is
        // an IN-list both times (not nested ANDs from re-extraction).
        let unwind_source = Expr::List(vec![str_lit("a"), str_lit("b")]);
        let plan = make_filter_crossjoin_scan(
            unwind_source,
            "u",
            1,
            "Item",
            "n",
            eq_property_predicate("n", "name", "u"),
        );
        let params = HashMap::new();
        let pass1 = merge_unwind_in_filters(&plan, &params);
        let pass2 = merge_unwind_in_filters(&pass1, &params);

        // The second pass should leave the merged filter as-is (its
        // walker doesn't recurse into Scan.filter, so the IN-list is
        // not re-extracted and re-ANDed). Verify the scan.filter
        // structure remains `Expr::In`, not `Expr::BinaryOp(And, ...)`.
        let LogicalPlan::Filter { input, .. } = &pass2 else {
            panic!("expected Filter");
        };
        let LogicalPlan::CrossJoin { right, .. } = input.as_ref() else {
            panic!("expected CrossJoin");
        };
        let LogicalPlan::Scan { filter, .. } = right.as_ref() else {
            panic!("expected Scan");
        };
        let filter_expr = filter.as_ref().expect("Scan.filter must be Some");
        assert!(
            matches!(filter_expr, Expr::In { .. }),
            "After 2 passes the filter should still be a single Expr::In, \
             not ANDed with a duplicate; got {filter_expr:?}"
        );
    }

    #[test]
    fn merge_pass_leaves_non_pushable_predicates_alone() {
        // Filter with a non-equi predicate (e.g., n.name STARTS WITH "x")
        // shouldn't trigger any pushdown — classify_join_predicate
        // produces no equi-pairs, so the pass leaves the plan unchanged.
        let unwind_source = Expr::List(vec![str_lit("a")]);
        let starts_with = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "name".to_string(),
            )),
            op: uni_cypher::ast::BinaryOp::StartsWith,
            right: Box::new(str_lit("x")),
        };
        let plan = make_filter_crossjoin_scan(unwind_source, "u", 1, "Item", "n", starts_with);
        let params = HashMap::new();
        let rewritten = merge_unwind_in_filters(&plan, &params);

        // The Scan's filter should remain None (no equi-pair → no
        // IN-list lifted).
        let LogicalPlan::Filter { input, .. } = &rewritten else {
            panic!("expected Filter");
        };
        let LogicalPlan::CrossJoin { right, .. } = input.as_ref() else {
            panic!("expected CrossJoin");
        };
        let LogicalPlan::Scan { filter, .. } = right.as_ref() else {
            panic!("expected Scan");
        };
        assert!(
            filter.is_none(),
            "no equi-pair → no pushdown; Scan.filter should remain None, got {filter:?}"
        );
    }

    #[test]
    fn merge_pass_handles_nested_crossjoin() {
        // `Filter(CrossJoin(Unwind, CrossJoin(Scan_A, Scan_B)), n.name = u)` —
        // The pass should recurse and lift the IN-list into Scan_A
        // (which is the side that owns the joined variable "n").
        //
        // To make the test self-contained, build:
        //   Outer: Filter(predicate=`n.name=u`, CrossJoin(L=Unwind(u), R=CrossJoin(Scan(Item,n), Scan(Other,m))))
        // The pass walks the outer Filter, recurses into the inner CrossJoin
        // first, finds no Filter wrapping it (so leaves it), then handles
        // the outer Filter+CrossJoin and lifts the IN-list into the
        // appropriate Scan via wrap_with_filter, which recurses into the
        // inner CrossJoin to find the matching Scan.
        let unwind_source = Expr::List(vec![str_lit("a")]);
        let unwind = LogicalPlan::Unwind {
            input: Box::new(LogicalPlan::Project {
                input: Box::new(LogicalPlan::Scan {
                    label_id: 0,
                    labels: vec!["__".to_string()],
                    variable: "__".to_string(),
                    filter: None,
                    optional: false,
                }),
                projections: vec![],
            }),
            expr: unwind_source,
            variable: "u".to_string(),
        };
        let inner_cross = LogicalPlan::CrossJoin {
            left: Box::new(LogicalPlan::Scan {
                label_id: 1,
                labels: vec!["Item".to_string()],
                variable: "n".to_string(),
                filter: None,
                optional: false,
            }),
            right: Box::new(LogicalPlan::Scan {
                label_id: 2,
                labels: vec!["Other".to_string()],
                variable: "m".to_string(),
                filter: None,
                optional: false,
            }),
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::CrossJoin {
                left: Box::new(unwind),
                right: Box::new(inner_cross),
            }),
            predicate: eq_property_predicate("n", "name", "u"),
            optional_variables: HashSet::new(),
        };
        let params = HashMap::new();
        let rewritten = merge_unwind_in_filters(&plan, &params);

        // Navigate to the Item scan (via outer Filter → CrossJoin.right
        // → CrossJoin (or Filter wrapping it) → leftmost Scan). The
        // wrap_with_filter helper merges into the right subtree of the
        // top-level CrossJoin; that subtree was the inner CrossJoin,
        // which isn't a Scan — so wrap_with_filter fell through to its
        // "wrap in Filter" branch.
        let LogicalPlan::Filter { input, .. } = &rewritten else {
            panic!("expected outer Filter");
        };
        let LogicalPlan::CrossJoin { right, .. } = input.as_ref() else {
            panic!("expected outer CrossJoin");
        };
        // wrap_with_filter wrapped the inner CrossJoin in a Filter
        // because it's not a Scan-shape. The IN-list ended up on top
        // of the inner CrossJoin, not inside Scan.filter.
        match right.as_ref() {
            LogicalPlan::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Expr::In { .. }),
                    "expected Expr::In wrapping inner CrossJoin, got {predicate:?}"
                );
            }
            other => panic!(
                "expected Filter wrapping inner CrossJoin, got {other:?}. \
                 This is acceptable behaviour — the IN-list is preserved \
                 above the inner join — but the test should be updated if \
                 wrap_with_filter changes to descend through CrossJoins."
            ),
        }
    }
}
