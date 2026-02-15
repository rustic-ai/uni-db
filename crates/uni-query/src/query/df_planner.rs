// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

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
use crate::query::df_graph::bind_fixed_path::BindFixedPathExec;
use crate::query::df_graph::bind_zero_length_path::BindZeroLengthPathExec;
use crate::query::df_graph::recursive_cte::RecursiveCTEExec;
use crate::query::df_graph::traverse::{
    GraphVariableLengthTraverseExec, GraphVariableLengthTraverseMainExec,
};
use crate::query::df_graph::{
    GraphApplyExec, GraphExecutionContext, GraphExtIdLookupExec, GraphProcedureCallExec,
    GraphScanExec, GraphShortestPathExec, GraphTraverseExec, GraphTraverseMainExec,
    GraphUnwindExec, GraphVectorKnnExec, L0Context, OptionalFilterExec,
};
use crate::query::planner::{
    LogicalPlan, aggregate_column_name, classify_window_expressions, collect_properties_from_plan,
};
use anyhow::{Result, anyhow};
use arrow_schema::{Schema, SchemaRef};
use datafusion::common::JoinType;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr as DfExpr, SortExpr as DfSortExpr};
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
use uni_common::core::schema::{PropertyMeta, Schema as UniSchema};
use uni_cypher::ast::{Direction as AstDirection, Expr, SortItem};
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

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
        }
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

                    // Collect explicit property names (non-wildcard, non-internal)
                    let explicit: Vec<String> = props
                        .iter()
                        .filter(|p| *p != "*" && !p.starts_with('_'))
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
                    combined.sort();
                    combined
                } else {
                    let mut explicit_props: Vec<String> =
                        props.iter().filter(|p| *p != "*").cloned().collect();
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
        }
    }

    /// Build a `TranslationContext` with variable kinds collected from a LogicalPlan.
    ///
    /// This is used for expression translation in filters, projections, etc.
    /// where bare variable references need to resolve to identity columns.
    fn translation_context_for_plan(&self, plan: &LogicalPlan) -> TranslationContext {
        let mut variable_kinds = HashMap::new();
        let mut variable_labels = HashMap::new();
        collect_variable_kinds(plan, &mut variable_kinds);
        self.collect_variable_labels(plan, &mut variable_labels);
        TranslationContext {
            parameters: self.params.clone(),
            variable_labels,
            variable_kinds,
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
            } => {
                if let Some(first) = scan_labels.first() {
                    labels.insert(variable.clone(), first.clone());
                }
            }
            LogicalPlan::ScanMainByLabels {
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
            _ => {}
        }
    }

    fn merged_edge_type_properties(&self, edge_type_ids: &[u32]) -> HashMap<String, PropertyMeta> {
        let mut merged = HashMap::new();
        let mut sorted_ids = edge_type_ids.to_vec();
        sorted_ids.sort_unstable();

        for edge_type_id in sorted_ids {
            if let Some(type_name) = self.schema.edge_type_name_by_id_unified(edge_type_id)
                && let Some(props) = self.schema.properties.get(&type_name)
            {
                for (name, meta) in props {
                    merged.entry(name.clone()).or_insert_with(|| meta.clone());
                }
            }
        }

        merged
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
        // Collect all properties needed anywhere in the plan tree
        let all_properties = collect_properties_from_plan(logical);

        // Delegate to internal planning with properties context
        self.plan_internal(logical, &all_properties)
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
            LogicalPlan::Scan {
                label_id,
                labels,
                variable,
                filter,
                optional,
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
                target_filter: _, // Applied as FilterExec later
                path_variable,
                is_variable_length,
                ..
            } => {
                if *is_variable_length {
                    self.plan_traverse_main_by_type_vlp(
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
                    )
                } else {
                    self.plan_traverse_main_by_type(
                        input,
                        type_names,
                        direction.clone(),
                        source_variable,
                        target_variable,
                        step_variable.as_deref(),
                        *optional,
                        all_properties,
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
            ),

            LogicalPlan::ShortestPath {
                input,
                edge_type_ids,
                direction,
                source_variable,
                target_variable,
                target_label_id: _,
                path_variable,
                min_hops: _,
                max_hops: _,
            } => self.plan_shortest_path(
                input,
                edge_type_ids,
                direction.clone(),
                source_variable,
                target_variable,
                path_variable,
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
                path_variable,
            } => self.plan_bind_path(
                input,
                node_variables,
                edge_variables,
                path_variable,
                all_properties,
            ),

            // === Unsupported (for now) ===
            LogicalPlan::Create { .. }
            | LogicalPlan::CreateBatch { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::Set { .. }
            | LogicalPlan::Remove { .. }
            | LogicalPlan::Delete { .. } => Err(anyhow!(
                "Write operations not yet supported in DataFusion engine"
            )),

            LogicalPlan::Window {
                input,
                window_exprs,
            } => {
                // Classify window expressions into manual and DataFusion-backed
                let (manual_exprs, df_exprs) = classify_window_expressions(window_exprs);

                // Only DataFusion aggregate window functions are supported here
                // Manual window functions (ROW_NUMBER, RANK, etc.) stay in fallback executor
                if !manual_exprs.is_empty() {
                    return Err(anyhow!(
                        "Manual window functions (ROW_NUMBER, RANK, etc.) must be executed in fallback executor, not DataFusion"
                    ));
                }

                // Plan input first
                let input_plan = self.plan_internal(input, all_properties)?;

                // Plan DataFusion window aggregates if present
                if !df_exprs.is_empty() {
                    self.plan_window_aggregate(input_plan, &df_exprs, Some(input.as_ref()))
                } else {
                    Ok(input_plan)
                }
            }

            LogicalPlan::CrossJoin { left, right } => {
                let left_plan = self.plan_internal(left, all_properties)?;
                let right_plan = self.plan_internal(right, all_properties)?;
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

            LogicalPlan::AllShortestPaths { .. } => Err(anyhow!(
                "allShortestPaths not yet supported in DataFusion engine"
            )),

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

            LogicalPlan::LoadCsv { .. } => {
                Err(anyhow!("LOAD CSV not yet supported in DataFusion engine"))
            }

            LogicalPlan::ExtIdLookup {
                variable,
                ext_id,
                filter,
                optional,
            } => self.plan_ext_id_lookup(variable, ext_id, filter.as_ref(), *optional),

            LogicalPlan::Foreach { .. } => {
                Err(anyhow!("FOREACH not yet supported in DataFusion engine"))
            }

            // DDL operations should be handled separately
            LogicalPlan::CreateVectorIndex { .. }
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
            | LogicalPlan::Begin
            | LogicalPlan::Commit
            | LogicalPlan::Rollback
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
        };
        let df_filter = cypher_expr_to_df(filter_expr, Some(&ctx))?;

        let schema = plan.schema();

        let session = self.session_ctx.read();
        let physical_filter = self.create_physical_filter_expr(&df_filter, &schema, &session)?;

        Ok(Arc::new(FilterExec::try_new(physical_filter, plan)?))
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

        let unwind = GraphUnwindExec::new(input_plan, expr, variable, self.params.clone());

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

        // 2. Infer subquery output schema from logical plan + UniSchema metadata
        let sub_schema = infer_logical_plan_schema(subquery, &self.schema);

        // 3. Merge schemas: input fields + subquery fields (skip duplicates by name)
        let mut fields: Vec<Arc<arrow_schema::Field>> =
            input_schema.fields().iter().cloned().collect();
        let input_field_names: HashSet<&str> = input_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        for field in sub_schema.fields() {
            if !input_field_names.contains(field.name().as_str()) {
                fields.push(field.clone());
            }
        }
        let output_schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(GraphApplyExec::new(
            input_exec,
            subquery.clone(),
            input_filter.cloned(),
            self.graph_ctx.clone(),
            self.session_ctx.clone(),
            self.storage.clone(),
            self.schema.clone(),
            self.params.clone(),
            output_schema,
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

        let knn = GraphVectorKnnExec::new(
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
        );

        Ok(Arc::new(knn))
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

        if matches!(
            procedure_name,
            "uni.vector.query" | "uni.fts.query" | "uni.search"
        ) {
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

        let mut scan_plan: Arc<dyn ExecutionPlan> = Arc::new(GraphScanExec::new_vertex_scan(
            self.graph_ctx.clone(),
            label_name.to_string(),
            variable.to_string(),
            properties.clone(),
            None, // Filter will be applied as FilterExec on top
        ));

        // If we need the full object (structural access), add a Struct projection
        if all_properties
            .get(variable)
            .is_some_and(|p| p.contains("*"))
        {
            // Filter overflow_json from the structural projection — it's an internal
            // column, not a user-visible property for the struct.
            let struct_props: Vec<String> = properties
                .iter()
                .filter(|p| *p != "overflow_json")
                .cloned()
                .collect();
            scan_plan = self.add_structural_projection(scan_plan, variable, &struct_props)?;
        }

        let filtered_plan =
            self.apply_scan_filter(scan_plan, variable, filter, Some(label_name))?;
        self.wrap_optional(filtered_plan, optional)
    }

    /// Plan a schemaless vertex scan using the main vertices table.
    ///
    /// Used for labels that aren't in the schema - queries the main table
    /// with `array_contains(labels, 'X')` filter and extracts properties from `props_json`.
    fn plan_schemaless_scan(
        &self,
        label_name: &str,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| s.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        let need_full = all_properties
            .get(variable)
            .is_some_and(|p| p.contains("*"));

        // Always include _all_props for schemaless scans so that downstream
        // plan_filter can rewrite property accesses to json_get_* calls.
        if !properties.iter().any(|p| p == "_all_props") {
            properties.push("_all_props".to_string());
        }

        let mut scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_schemaless_vertex_scan(
                self.graph_ctx.clone(),
                label_name.to_string(),
                variable.to_string(),
                properties.clone(),
                None, // Filter will be applied as FilterExec on top
            ));

        // If we need the full object (structural access), build a struct with _labels + properties.
        // This enables labels(n)/keys(n) UDFs which expect a Struct column with a _labels field.
        if need_full {
            // Filter out "*" (wildcard marker) from struct_props.
            // Keep "_all_props" so that keys()/properties() UDFs can extract
            // property names at runtime from the CypherValue blob.
            let struct_props: Vec<String> =
                properties.iter().filter(|p| *p != "*").cloned().collect();
            scan_plan = self.add_structural_projection(scan_plan, variable, &struct_props)?;
        }

        let filtered_plan = self.apply_scan_filter(scan_plan, variable, filter, None)?;
        self.wrap_optional(filtered_plan, optional)
    }

    /// Plan a multi-label vertex scan using the main vertices table.
    ///
    /// For patterns like `(n:A:B)`, scans vertices with ALL labels (intersection).
    fn plan_multi_label_scan(
        &self,
        labels: &[String],
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| s.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        let need_full = all_properties
            .get(variable)
            .is_some_and(|p| p.contains("*"));

        // Always include _all_props for schemaless scans so that downstream
        // plan_filter can rewrite property accesses to json_get_* calls.
        if !properties.iter().any(|p| p == "_all_props") {
            properties.push("_all_props".to_string());
        }

        let mut scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_multi_label_vertex_scan(
                self.graph_ctx.clone(),
                labels.to_vec(),
                variable.to_string(),
                properties.clone(),
                None,
            ));

        // If we need the full object (structural access), build a struct with _labels + properties.
        // This enables labels(n)/keys(n) UDFs which expect a Struct column with a _labels field.
        if need_full {
            // Filter out "*" (wildcard marker) from struct_props.
            // Keep "_all_props" so that keys()/properties() UDFs can extract
            // property names at runtime from the CypherValue blob.
            let struct_props: Vec<String> =
                properties.iter().filter(|p| *p != "*").cloned().collect();
            scan_plan = self.add_structural_projection(scan_plan, variable, &struct_props)?;
        }

        let filtered_plan = self.apply_scan_filter(scan_plan, variable, filter, None)?;
        self.wrap_optional(filtered_plan, optional)
    }

    /// Plan a scan of all vertices regardless of label.
    ///
    /// This is used for `MATCH (n)` without a label filter.
    /// Uses the schemaless scan with an empty label to signal "scan all".
    fn plan_scan_all(
        &self,
        variable: &str,
        filter: Option<&Expr>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| s.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        let need_full = all_properties
            .get(variable)
            .is_some_and(|p| p.contains("*"));

        // Always include _all_props for schemaless scans so that downstream
        // plan_filter can rewrite property accesses to json_get_* calls.
        if !properties.iter().any(|p| p == "_all_props") {
            properties.push("_all_props".to_string());
        }

        let mut scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_schemaless_all_scan(
                self.graph_ctx.clone(),
                variable.to_string(),
                properties.clone(),
                None, // Filter will be applied as FilterExec on top
            ));

        // If we need the full object (structural access), build a struct with _labels + properties.
        // This enables labels(n)/keys(n) UDFs which expect a Struct column with a _labels field.
        if need_full {
            // Filter out "*" (wildcard marker) from struct_props.
            // Keep "_all_props" so that keys()/properties() UDFs can extract
            // property names at runtime from the CypherValue blob.
            let struct_props: Vec<String> =
                properties.iter().filter(|p| *p != "*").cloned().collect();
            scan_plan = self.add_structural_projection(scan_plan, variable, &struct_props)?;
        }

        let filtered_plan = self.apply_scan_filter(scan_plan, variable, filter, None)?;
        self.wrap_optional(filtered_plan, optional)
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let (input_plan, source_col) = Self::resolve_source_vid_col(input_plan, source_variable)?;

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
                    // that may be overflow properties not in the schema
                    if let Some(props) = all_properties.get(edge_var) {
                        for p in props {
                            if p != "*" && !p.starts_with('_') && !schema_props.contains(p) {
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
            }

            // Extract target vertex properties, expanding "*" wildcards
            let target_label_name_str = self.schema.label_name_by_id(target_label_id).unwrap_or("");
            let mut target_properties =
                self.resolve_properties(target_variable, target_label_name_str, all_properties);

            // Filter out "*" from target_properties — it is used for structural
            // projection (bare variable access like `RETURN t`) but must not be
            // passed to GraphTraverseExec as an actual property column name.
            target_properties.retain(|p| p != "*");

            // When wildcard access was requested but no specific properties resolved,
            // add _all_props to ensure properties are loaded (mirrors plan_scan_all behavior).
            let target_has_wildcard = all_properties
                .get(target_variable)
                .is_some_and(|p| p.contains("*"));
            if target_has_wildcard && target_properties.is_empty() {
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
            let has_non_schema_props = target_properties.iter().any(|p| {
                p != "overflow_json"
                    && p != "_all_props"
                    && !p.starts_with('_')
                    && !target_label_props.is_some_and(|lp| lp.contains_key(p.as_str()))
            });
            if has_non_schema_props && !target_properties.iter().any(|p| p == "_all_props") {
                target_properties.push("_all_props".to_string());
            }
            // Also check the filter for non-schema property references
            if let Some(filter_expr) = target_filter {
                let filter_props = crate::query::df_expr::collect_properties(filter_expr);
                let has_overflow_filter = filter_props.iter().any(|(var, prop)| {
                    var == target_variable
                        && !prop.starts_with('_')
                        && !target_label_props
                            .is_some_and(|props| props.contains_key(prop.as_str()))
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
            let target_vid_col = format!("{}._vid", target_variable);
            let bound_target_column = if input_plan
                .schema()
                .column_with_name(&target_vid_col)
                .is_some()
            {
                Some(target_vid_col)
            } else {
                None
            };

            // Collect edge ID columns from previous hops for relationship uniqueness.
            // Look for both explicit edge variables (ending in "._eid") and
            // internal tracking columns (starting with "__eid_to_").
            //
            // Rebound edge patterns (e.g. OPTIONAL MATCH ()-[r]->() where `r` is already bound)
            // use a temporary edge variable `__rebound_{r}` for traversal and then filter on eid.
            // Do not treat the already-bound `{r}._eid` as "used" here, otherwise the only
            // candidate edge is filtered out before rebound matching.
            let rebound_bound_edge_col = step_variable
                .and_then(|sv| sv.strip_prefix("__rebound_"))
                .map(|bound| format!("{}._eid", bound));

            let used_edge_columns: Vec<String> = input_plan
                .schema()
                .fields()
                .iter()
                .filter_map(|f| {
                    let name = f.name();
                    if rebound_bound_edge_col
                        .as_ref()
                        .is_some_and(|bound_col| name == bound_col)
                    {
                        None
                    } else if name.ends_with("._eid") {
                        // Only include if the edge variable is in current MATCH scope
                        let var_name = name.trim_end_matches("._eid");
                        if scope_match_variables.contains(var_name) {
                            Some(name.clone())
                        } else {
                            None
                        }
                    } else if name.starts_with("__eid_to_") {
                        // Internal tracking columns - include only if associated variable is in scope
                        let var_name = name.trim_start_matches("__eid_to_");
                        if scope_match_variables.contains(var_name) {
                            Some(name.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

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
                let target_vid_col = format!("{}._vid", target_variable);
                let bound_target_column = if input_plan
                    .schema()
                    .column_with_name(&target_vid_col)
                    .is_some()
                {
                    Some(target_vid_col)
                } else {
                    None
                };
                if bound_target_column.is_some() {
                    // For correlated patterns with bound target, traversal only needs reachability.
                    // Reuse existing bound target columns from input and avoid re-hydrating props.
                    vlp_target_properties.clear();
                }

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
                ))
            }
        };

        // Add structural projections for bare variable access (RETURN t, labels(t), etc.)
        let mut traverse_plan = traverse_plan;

        // Structural projection for target variable
        if all_properties
            .get(target_variable)
            .is_some_and(|p| p.contains("*"))
        {
            // Derive target properties from the traverse plan's output schema.
            // The traverse exec materializes columns like `b.name` from resolved
            // properties, but all_properties may only contain {"*"}.
            let prefix = format!("{}.", target_variable);
            let struct_props: Vec<String> = traverse_plan
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
            traverse_plan =
                self.add_structural_projection(traverse_plan, target_variable, &struct_props)?;
        }

        // Structural projection for edge variable
        // Only for single-hop traversals; VLP step variables are already List<Edge>
        if let Some(edge_var) = step_variable
            && !is_variable_length
            && all_properties
                .get(edge_var)
                .is_some_and(|p| p.contains("*"))
        {
            // Derive edge properties from the traverse plan's output schema
            // instead of from all_properties (which may only contain {"*"}).
            // The traverse exec already materialized columns like `r.since`.
            let prefix = format!("{}.", edge_var);
            let edge_props: Vec<String> = traverse_plan
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
            traverse_plan = self.add_edge_structural_projection(
                traverse_plan,
                edge_var,
                &edge_props,
                source_variable,
                target_variable,
            )?;
        }

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
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let (input_plan, source_col) = Self::resolve_source_vid_col(input_plan, source_variable)?;

        // Check if target variable is already bound (for patterns where target is in scope)
        let target_vid_col = format!("{}._vid", target_variable);
        let bound_target_column = if input_plan
            .schema()
            .column_with_name(&target_vid_col)
            .is_some()
        {
            Some(target_vid_col)
        } else {
            None
        };

        // Extract edge properties for schemaless edges (all treated as Utf8/JSON)
        let mut edge_properties: Vec<String> = if let Some(edge_var) = step_variable {
            all_properties
                .get(edge_var)
                .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

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
            bound_target_column,
        ));

        let mut result_plan = traverse_plan;

        // Structural projection for target variable (RETURN t, labels(t), etc.)
        if all_properties
            .get(target_variable)
            .is_some_and(|p| p.contains("*"))
        {
            // Derive target properties from the plan's output schema.
            let prefix = format!("{}.", target_variable);
            let struct_props: Vec<String> = result_plan
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
            result_plan =
                self.add_structural_projection(result_plan, target_variable, &struct_props)?;
        }

        // Structural projection for edge variable (type(r), RETURN r, etc.)
        if let Some(edge_var) = step_variable
            && all_properties
                .get(edge_var)
                .is_some_and(|p| p.contains("*"))
        {
            // Derive edge properties from the plan's output schema
            let prefix = format!("{}.", edge_var);
            let actual_edge_props: Vec<String> = result_plan
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
            result_plan = self.add_edge_structural_projection(
                result_plan,
                edge_var,
                &actual_edge_props,
                source_variable,
                target_variable,
            )?;
        }

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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let (input_plan, source_col) = Self::resolve_source_vid_col(input_plan, source_variable)?;

        // Check if target variable is already bound (for patterns where target is in scope)
        let target_vid_col = format!("{}._vid", target_variable);
        let bound_target_column = if input_plan
            .schema()
            .column_with_name(&target_vid_col)
            .is_some()
        {
            Some(target_vid_col)
        } else {
            None
        };

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
        ));

        Ok(traverse_plan)
    }

    /// Plan a shortest path computation.
    #[allow(clippy::too_many_arguments)]
    fn plan_shortest_path(
        &self,
        input: &LogicalPlan,
        edge_type_ids: &[u32],
        direction: AstDirection,
        source_variable: &str,
        target_variable: &str,
        path_variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let source_col = format!("{}._vid", source_variable);
        let target_col = format!("{}._vid", target_variable);

        Ok(Arc::new(GraphShortestPathExec::new(
            input_plan,
            source_col,
            target_col,
            edge_type_ids.to_vec(),
            adj_direction,
            path_variable.to_string(),
            self.graph_ctx.clone(),
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
        let input_plan = self.plan_internal(input, all_properties)?;
        let schema = input_plan.schema();

        // Check if any referenced columns are LargeBinary (schemaless CypherValue).
        // If so, rewrite the filter to use json_get_* UDFs against _all_props.
        let has_schemaless_cols = has_schemaless_property_columns(&schema);

        if has_schemaless_cols {
            // For schemaless CypherValue columns, always use physical compilation.
            // This avoids logical planning coercion failures (e.g. LargeBinary vs Int64).
            let ctx = self.translation_context_for_plan(input);
            let session = self.session_ctx.read();
            let state = session.state();
            let compiler = crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler::new(
                &state,
                Some(&ctx),
            )
            .with_subquery_ctx(
                self.graph_ctx.clone(),
                self.schema.clone(),
                self.session_ctx.clone(),
                self.storage.clone(),
                self.params.clone(),
            );
            let physical_predicate = compiler.compile(predicate, &schema)?;

            if !optional_variables.is_empty() {
                return Ok(Arc::new(OptionalFilterExec::new(
                    input_plan,
                    physical_predicate,
                    optional_variables.clone(),
                )));
            }

            return Ok(Arc::new(FilterExec::try_new(
                physical_predicate,
                input_plan,
            )?));
        }

        let ctx = self.translation_context_for_plan(input);

        let session = self.session_ctx.read();
        let state = session.state();
        let compiler = crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler::new(
            &state,
            Some(&ctx),
        )
        .with_subquery_ctx(
            self.graph_ctx.clone(),
            self.schema.clone(),
            self.session_ctx.clone(),
            self.storage.clone(),
            self.params.clone(),
        );
        let physical_predicate = compiler.compile(predicate, &schema)?;

        // For OPTIONAL MATCH: use OptionalFilterExec for proper NULL row preservation.
        // Reuse the already-compiled physical_predicate (from CypherPhysicalExprCompiler)
        // instead of re-creating via cypher_expr_to_df, which cannot handle quantifiers
        // and other custom expressions.
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

    /// Build projection expressions from an already-planned input.
    fn plan_project_from_input(
        &self,
        input_plan: Arc<dyn ExecutionPlan>,
        projections: &[(Expr, Option<String>)],
        context_plan: Option<&LogicalPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input_plan.schema();

        let session = self.session_ctx.read();
        let state = session.state();

        // Build translation context with variable kinds if we have a logical plan
        let ctx = context_plan.map(|p| self.translation_context_for_plan(p));

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

            let compiler = crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler::new(
                &state,
                ctx.as_ref(),
            )
            .with_subquery_ctx(
                self.graph_ctx.clone(),
                self.schema.clone(),
                self.session_ctx.clone(),
                self.storage.clone(),
                self.params.clone(),
            );
            let physical_expr = compiler.compile(expr, &schema)?;

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
        let mut group_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();
        let compiler = crate::query::df_graph::expr_compiler::CypherPhysicalExprCompiler::new(
            &state,
            Some(&ctx),
        )
        .with_subquery_ctx(
            self.graph_ctx.clone(),
            self.schema.clone(),
            self.session_ctx.clone(),
            self.storage.clone(),
            self.params.clone(),
        );
        for expr in group_by {
            let physical_expr = compiler.compile(expr, &schema)?;
            let name = expr.to_string_repr();
            group_exprs.push((physical_expr, name));
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
        let num_group_by = group_by.len();
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

    /// Translate Cypher aggregate expressions to DataFusion.
    fn translate_aggregates(
        &self,
        aggregates: &[Expr],
        schema: &SchemaRef,
        state: &SessionState,
        ctx: &TranslationContext,
    ) -> Result<Vec<PhysicalAggregate>> {
        use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};

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
                    // For count(*) or count(variable) where variable is a node/edge
                    // (not a property), translate to count(lit(1)) since the variable
                    // itself has no column in the scan schema.
                    // Exception: COUNT(DISTINCT variable) needs the actual column
                    // reference so that null rows (from OPTIONAL MATCH) are excluded.
                    if matches!(args.first(), Some(uni_cypher::ast::Expr::Wildcard)) {
                        count(datafusion::logical_expr::lit(1))
                    } else if matches!(args.first(), Some(uni_cypher::ast::Expr::Variable(_))) {
                        if *distinct {
                            count(get_arg()?)
                        } else {
                            count(datafusion::logical_expr::lit(1))
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
                            matches!(field.data_type(), datafusion::arrow::datatypes::DataType::Float32 | datafusion::arrow::datatypes::DataType::Float64)
                        } else {
                            false
                        };
                        if is_float {
                            sum(DfExpr::Cast(Cast::new(Box::new(arg), datafusion::arrow::datatypes::DataType::Float64)))
                        } else {
                            sum(DfExpr::Cast(Cast::new(Box::new(arg), datafusion::arrow::datatypes::DataType::Int64)))
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
                        avg(DfExpr::Cast(Cast::new(Box::new(arg), datafusion::arrow::datatypes::DataType::Float64)))
                    }
                }
                "min" => {
                    // Use Cypher-aware min for LargeBinary columns (mixed types)
                    let arg = get_arg()?;
                    if self.is_large_binary_col(&arg, schema) {
                        let udaf = Arc::new(crate::query::df_udfs::create_cypher_min_udaf());
                        udaf.call(vec![arg])
                    } else {
                        min(arg)
                    }
                }
                "max" => {
                    // Use Cypher-aware max for LargeBinary columns (mixed types)
                    let arg = get_arg()?;
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
                    let udaf = Arc::new(crate::query::df_udfs::create_cypher_percentile_disc_udaf());
                    udaf.call(vec![coerced, pct_arg])
                }
                "percentilecont" => {
                    if args.len() != 2 {
                        return Err(anyhow!("percentileCont() requires exactly 2 arguments"));
                    }
                    let expr_arg = cypher_expr_to_df(&args[0], Some(ctx))?;
                    let pct_arg = cypher_expr_to_df(&args[1], Some(ctx))?;
                    let coerced = crate::query::df_udfs::cypher_to_float64_expr(expr_arg);
                    let udaf = Arc::new(crate::query::df_udfs::create_cypher_percentile_cont_udaf());
                    udaf.call(vec![coerced, pct_arg])
                }
                "collect" => {
                    // Use custom Cypher collect UDAF that filters nulls and returns
                    // empty list (not null) when all inputs are null.
                    let arg = get_arg()?;
                    crate::query::df_udfs::create_cypher_collect_expr(arg, *distinct)
                }
                _ => return Err(anyhow!("Unsupported aggregate function: {}", name)),
            };

            // Apply DISTINCT if needed (collect/percentile handle their own distinct)
            let df_agg = if *distinct
                && !matches!(name_lower.as_str(), "collect" | "percentiledisc" | "percentilecont")
            {
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
                    let compiler = CypherPhysicalExprCompiler::new(state, Some(ctx))
                        .with_subquery_ctx(
                            self.graph_ctx.clone(),
                            self.schema.clone(),
                            self.session_ctx.clone(),
                            self.storage.clone(),
                            self.params.clone(),
                        );
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

        // Translate sort expressions to DataFusion's SortExpr (a.k.a. Sort struct)
        // SortItem has `ascending: bool`, so use it directly
        // Default nulls_first to false for ASC, true for DESC
        let mut df_sort_exprs = Vec::new();
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

            let df_expr = cypher_expr_to_df(&sort_expr, Some(&ctx))?;
            let df_expr = Self::resolve_udfs(&df_expr, &session.state())?;
            let asc = item.ascending;
            let nulls_first = !asc; // Standard SQL behavior: nulls last for ASC, first for DESC

            // Cypher sort order: Map > Node > Rel > Path > List > String > Bool > Number
            // We prepend a type rank sort key to ensure correct ordering across mixed types
            let rank_udf = crate::query::df_udfs::create_type_rank_udf();
            let rank_expr = rank_udf.call(vec![df_expr.clone()]);

            // For ranks that share the same underlying DataFusion type (e.g. String vs Number vs Bool all coerced to String),
            // we need secondary keys to sort correctly within the rank.
            // Rank 1 (Number): sort by toFloat(x)
            let float_udf = crate::query::df_udfs::create_to_float_udf();
            let float_expr = float_udf.call(vec![df_expr.clone()]);

            // Rank 2 (Bool): sort by toBoolean(x)
            let bool_udf = crate::query::df_udfs::create_to_boolean_udf();
            let bool_expr = bool_udf.call(vec![df_expr.clone()]);

            df_sort_exprs.push(DfSortExpr::new(rank_expr, asc, nulls_first));
            df_sort_exprs.push(DfSortExpr::new(float_expr, asc, nulls_first));
            df_sort_exprs.push(DfSortExpr::new(bool_expr, asc, nulls_first));
            df_sort_exprs.push(DfSortExpr::new(df_expr, asc, nulls_first));
        }

        // Build DFSchema for conversion
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

        let physical_sort_exprs = create_physical_sort_exprs(
            &df_sort_exprs,
            &df_schema,
            session.state().execution_props(),
        )?;

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

        let union_plan = Arc::new(UnionExec::new(vec![left_plan, right_plan]));

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

    /// Plan aggregate window functions using DataFusion's WindowAggExec.
    ///
    /// Translates Cypher window aggregate expressions (SUM, AVG, MIN, MAX, COUNT with OVER)
    /// to DataFusion's window function execution plan.
    fn plan_window_aggregate(
        &self,
        input: Arc<dyn ExecutionPlan>,
        window_exprs: &[Expr],
        context_plan: Option<&LogicalPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::functions_aggregate::average::avg_udaf;
        use datafusion::functions_aggregate::count::count_udaf;
        use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
        use datafusion::functions_aggregate::sum::sum_udaf;
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
        let mut window_expr_list = Vec::new();

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

            // Get the appropriate aggregate UDF
            let aggregate_udf = match name_lower.as_str() {
                "count" => count_udaf(),
                "sum" => sum_udaf(),
                "avg" => avg_udaf(),
                "min" => min_udaf(),
                "max" => max_udaf(),
                other => return Err(anyhow!("Unsupported aggregate window function: {}", other)),
            };

            // Translate argument expressions to physical expressions
            let physical_args: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
                if args.is_empty() || matches!(args.as_slice(), [Expr::Wildcard]) {
                    // COUNT(*) case - args contain a single Wildcard or are empty
                    vec![create_physical_expr(
                        &datafusion::logical_expr::lit(1),
                        &df_schema,
                        state.execution_props(),
                    )?]
                } else {
                    args.iter()
                        .map(|arg| {
                            let mut df_expr = cypher_expr_to_df(arg, tx_ctx.as_ref())?;

                            // Cast numeric types for aggregate functions:
                            // SUM needs Int64 to avoid overflow, AVG needs Float64
                            use datafusion::logical_expr::Cast;
                            let cast_type = match name_lower.as_str() {
                                "sum" => Some(datafusion::arrow::datatypes::DataType::Int64),
                                "avg" => Some(datafusion::arrow::datatypes::DataType::Float64),
                                _ => None,
                            };
                            if let Some(target_type) = cast_type {
                                df_expr = DfExpr::Cast(Cast::new(Box::new(df_expr), target_type));
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

            // Create window frame based on whether there's an ORDER BY
            // - No ORDER BY: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING (full partition)
            // - With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (cumulative)
            let window_frame = if window_spec.order_by.is_empty() {
                // No ORDER BY: aggregate over entire partition
                use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(datafusion::common::ScalarValue::UInt64(None)), // UNBOUNDED PRECEDING
                    WindowFrameBound::Following(datafusion::common::ScalarValue::UInt64(None)), // UNBOUNDED FOLLOWING
                ))
            } else {
                // With ORDER BY: cumulative from partition start to current row
                Arc::new(WindowFrame::new(Some(false)))
            };

            // Create the window function definition
            let window_fn_def = WindowFunctionDefinition::AggregateUDF(aggregate_udf);

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
                input_schema.as_ref(),
                false, // ignore_nulls
                *distinct,
                None, // filter
            )?;

            window_expr_list.push(window_expr);
        }

        // WindowAggExec requires input to be sorted by partition columns + order by columns.
        // Create a SortExec to ensure proper ordering.
        let mut sort_exprs = Vec::new();

        // Add partition columns to sort (must be sorted by partition first)
        for expr in window_exprs {
            if let Expr::FunctionCall {
                window_spec: Some(window_spec),
                ..
            } = expr
            {
                for partition_expr in &window_spec.partition_by {
                    let df_expr = cypher_expr_to_df(partition_expr, tx_ctx.as_ref())?;
                    let physical_expr =
                        create_physical_expr(&df_expr, &df_schema, state.execution_props())?;

                    // Only add if not already in sort list
                    // Use display comparison as proxy for equality since PhysicalExpr doesn't implement Eq
                    if !sort_exprs
                        .iter()
                        .any(|s: &datafusion::physical_expr::PhysicalSortExpr| {
                            s.expr.to_string() == physical_expr.to_string()
                        })
                    {
                        sort_exprs.push(datafusion::physical_expr::PhysicalSortExpr {
                            expr: physical_expr,
                            options: datafusion::arrow::compute::SortOptions {
                                descending: false,
                                nulls_first: false,
                            },
                        });
                    }
                }

                // Then add order by columns
                for sort_item in &window_spec.order_by {
                    let df_expr = cypher_expr_to_df(&sort_item.expr, tx_ctx.as_ref())?;
                    let physical_expr =
                        create_physical_expr(&df_expr, &df_schema, state.execution_props())?;

                    sort_exprs.push(datafusion::physical_expr::PhysicalSortExpr {
                        expr: physical_expr,
                        options: datafusion::arrow::compute::SortOptions {
                            descending: !sort_item.ascending,
                            nulls_first: !sort_item.ascending,
                        },
                    });
                }
            }
        }

        // Add SortExec before WindowAggExec if we have partition or order by columns
        let sorted_input = if !sort_exprs.is_empty() {
            let lex_ordering = LexOrdering::new(sort_exprs)
                .ok_or_else(|| anyhow!("Failed to create LexOrdering for window function"))?;
            Arc::new(SortExec::new(lex_ordering, input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };

        // Create WindowAggExec
        let window_agg_exec = WindowAggExec::try_new(
            window_expr_list,
            sorted_input,
            false, // can_repartition - keep data on current partitions
        )?;

        Ok(Arc::new(window_agg_exec))
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
        path_variable: &str,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;
        Ok(Arc::new(BindFixedPathExec::new(
            input_plan,
            node_variables.to_vec(),
            edge_variables.to_vec(),
            path_variable.to_string(),
            self.graph_ctx.clone(),
        )))
    }

    /// Create a physical filter expression.
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
        struct_args.push(lit("_src"));
        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
            format!("{}._vid", source_variable),
        )));

        struct_args.push(lit("_dst"));
        struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(
            format!("{}._vid", target_variable),
        )));

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
        if input_plan.schema().column_with_name(&source_vid_col).is_some() {
            return Ok((input_plan, source_vid_col));
        }
        // Check if the variable is a struct column (entity after WITH aggregation).
        // If so, add a projection to extract _vid from the struct.
        if let Ok(field) = input_plan.schema().field_with_name(source_variable)
            && matches!(field.data_type(), datafusion::arrow::datatypes::DataType::Struct(_))
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
            && let datafusion::arrow::datatypes::DataType::Struct(fields) =
                struct_field.data_type()
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
                let field_name: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                    datafusion::physical_expr::expressions::Literal::new(
                        ScalarValue::Utf8(Some("_vid".to_string())),
                    ),
                );
                let vid_expr = Arc::new(
                    datafusion::physical_expr::ScalarFunctionExpr::try_new(
                        get_field_udf.clone(),
                        vec![struct_col.clone(), field_name],
                        schema.as_ref(),
                        Arc::new(datafusion::common::config::ConfigOptions::default()),
                    )?,
                );
                proj_exprs.push((vid_expr, format!("{}._vid", variable)));
            }

            // Extract _labels field
            if fields.iter().any(|f| f.name() == "_labels") {
                let field_name: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(
                    datafusion::physical_expr::expressions::Literal::new(
                        ScalarValue::Utf8(Some("_labels".to_string())),
                    ),
                );
                let labels_expr = Arc::new(
                    datafusion::physical_expr::ScalarFunctionExpr::try_new(
                        get_field_udf,
                        vec![struct_col, field_name],
                        schema.as_ref(),
                        Arc::new(datafusion::common::config::ConfigOptions::default()),
                    )?,
                );
                proj_exprs.push((labels_expr, format!("{}._labels", variable)));
            }
        }

        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    }

    /// Check if a DataFusion expression refers to a LargeBinary column in the schema.
    fn is_large_binary_col(&self, expr: &DfExpr, schema: &SchemaRef) -> bool {
        if let DfExpr::Column(col) = expr
            && let Ok(field) = schema.field_with_name(&col.name)
        {
            return matches!(field.data_type(), datafusion::arrow::datatypes::DataType::LargeBinary);
        }
        // For any other expression type, conservatively return true
        // since schemaless properties are stored as LargeBinary
        true
    }
}

/// Recursively collect variable kinds (node, edge, path) from a LogicalPlan.
///
/// This information is used by the expression translator to resolve bare variable
/// references to their identity columns (e.g., `n` → `n._vid` for nodes).
fn collect_variable_kinds(plan: &LogicalPlan, kinds: &mut HashMap<String, VariableKind>) {
    match plan {
        LogicalPlan::Scan { variable, .. } => {
            kinds.insert(variable.clone(), VariableKind::Node);
        }
        LogicalPlan::ExtIdLookup { variable, .. } => {
            kinds.insert(variable.clone(), VariableKind::Node);
        }
        LogicalPlan::ScanAll { variable, .. } => {
            kinds.insert(variable.clone(), VariableKind::Node);
        }
        LogicalPlan::ScanMainByLabels { variable, .. } => {
            kinds.insert(variable.clone(), VariableKind::Node);
        }
        LogicalPlan::VectorKnn { variable, .. } => {
            kinds.insert(variable.clone(), VariableKind::Node);
        }
        LogicalPlan::InvertedIndexLookup { variable, .. } => {
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
            kinds.insert(source_variable.clone(), VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            if let Some(sv) = step_variable {
                kinds.insert(sv.clone(), VariableKind::edge_for(*is_variable_length));
            }
            if let Some(pv) = path_variable {
                kinds.insert(pv.clone(), VariableKind::Path);
            }
        }
        LogicalPlan::ShortestPath {
            input,
            source_variable,
            target_variable,
            path_variable,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(source_variable.clone(), VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            kinds.insert(path_variable.clone(), VariableKind::Path);
        }
        LogicalPlan::AllShortestPaths {
            input,
            source_variable,
            target_variable,
            path_variable,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(source_variable.clone(), VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            kinds.insert(path_variable.clone(), VariableKind::Path);
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
        | LogicalPlan::Unwind { input, .. }
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
            use crate::query::df_graph::procedure_call::map_yield_to_canonical;
            for (name, alias) in yield_items {
                let var = alias.as_ref().unwrap_or(name);
                if matches!(
                    procedure_name.as_str(),
                    "uni.vector.query" | "uni.fts.query" | "uni.search"
                ) {
                    let canonical = map_yield_to_canonical(name);
                    if canonical == "node" {
                        kinds.insert(var.clone(), VariableKind::Node);
                    }
                    // Scalar yields (distance, score, vid) don't need VariableKind
                }
                // For schema procedures, yields are all scalars — no entry needed
            }
        }
        // Leaf nodes with no variables or not applicable
        LogicalPlan::Empty
        | LogicalPlan::LoadCsv { .. }
        | LogicalPlan::CreateVectorIndex { .. }
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
        | LogicalPlan::Begin
        | LogicalPlan::Commit
        | LogicalPlan::Rollback => {}
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

/// Return true when the schema contains CypherValue-encoded property columns.
fn has_schemaless_property_columns(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(|f| {
        f.data_type() == &arrow::datatypes::DataType::LargeBinary
            && f.name().contains('.')
            && !f.name().ends_with("._all_props")
    })
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

    if target_has_wildcard && properties.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

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

    #[test]
    fn test_has_schemaless_property_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n._vid", DataType::UInt64, false),
            Field::new("n.name", DataType::LargeBinary, true),
            Field::new("n._all_props", DataType::LargeBinary, true),
        ]));
        assert!(has_schemaless_property_columns(&schema));

        let schema_no_props = Arc::new(Schema::new(vec![
            Field::new("n._vid", DataType::UInt64, false),
            Field::new("n._all_props", DataType::LargeBinary, true),
            Field::new("blob", DataType::LargeBinary, true),
        ]));
        assert!(!has_schemaless_property_columns(&schema_no_props));
    }

    #[test]
    fn test_sanitize_vlp_target_properties_removes_wildcard() {
        let props = vec!["*".to_string(), "name".to_string()];
        let label_props = HashSet::from(["name".to_string()]);
        let sanitized = sanitize_vlp_target_properties(props, true, Some(&label_props));

        assert_eq!(sanitized, vec!["name".to_string()]);
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
}
