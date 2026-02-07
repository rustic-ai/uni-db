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
use crate::query::df_graph::bind_zero_length_path::BindZeroLengthPathExec;
use crate::query::df_graph::traverse::{
    GraphVariableLengthTraverseExec, GraphVariableLengthTraverseMainExec,
};
use crate::query::df_graph::{
    GraphExecutionContext, GraphExtIdLookupExec, GraphScanExec, GraphShortestPathExec,
    GraphTraverseExec, GraphTraverseMainExec, GraphUnwindExec, GraphVectorKnnExec, L0Context,
};
use crate::query::planner::{
    LogicalPlan, aggregate_column_name, classify_window_expressions, collect_properties_from_plan,
};
use anyhow::{Result, anyhow};
use arrow_schema::{Schema, SchemaRef};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr as DfExpr, ExprSchemable, SortExpr as DfSortExpr};
use datafusion::physical_expr::{create_physical_expr, create_physical_sort_exprs};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::filter::FilterExec;
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
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{Direction as AstDirection, Expr, SortItem};
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

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
    _storage: Arc<StorageManager>,

    /// Graph execution context for custom operators.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Schema for label/edge type lookups.
    schema: Arc<UniSchema>,

    /// Last flush version for staleness detection.
    last_flush_version: AtomicU64,

    /// Query parameters for expression translation.
    params: HashMap<String, serde_json::Value>,
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
        params: HashMap<String, serde_json::Value>,
    ) -> Self {
        let graph_ctx = Arc::new(GraphExecutionContext::new(
            storage.clone(),
            l0,
            property_manager,
        ));

        Self {
            session_ctx,
            _storage: storage,
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
                    let mut schema_props: Vec<String> = self
                        .schema
                        .properties
                        .get(schema_name)
                        .map(|p| p.keys().cloned().collect())
                        .unwrap_or_default();
                    schema_props.sort();
                    schema_props
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
        params: HashMap<String, serde_json::Value>,
    ) -> Self {
        let graph_ctx = Arc::new(GraphExecutionContext::with_l0_context(
            storage.clone(),
            l0_context,
            property_manager,
        ));

        Self {
            session_ctx,
            _storage: storage,
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
        collect_variable_kinds(plan, &mut variable_kinds);
        TranslationContext {
            parameters: self.params.clone(),
            variable_labels: HashMap::new(),
            variable_kinds,
        }
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
                optional: _,
            } => {
                if labels.len() > 1 {
                    // Multi-label: use main table with intersection semantics
                    self.plan_multi_label_scan(labels, variable, filter.as_ref(), all_properties)
                } else {
                    // Single-label: use per-label table
                    self.plan_scan(*label_id, variable, filter.as_ref(), all_properties)
                }
            }

            // ScanMainByLabels is now supported via schemaless scan
            LogicalPlan::ScanMainByLabels {
                labels,
                variable,
                filter,
                optional: _,
            } => {
                if labels.len() > 1 {
                    // Multi-label schemaless scan
                    self.plan_multi_label_scan(labels, variable, filter.as_ref(), all_properties)
                } else if let Some(label_name) = labels.first() {
                    // Single label schemaless scan
                    self.plan_schemaless_scan(label_name, variable, filter.as_ref(), all_properties)
                } else {
                    // Empty labels - should not happen, fallback to scan all
                    self.plan_scan_all(variable, filter.as_ref(), all_properties)
                }
            }

            // ScanAll is now supported via schemaless scan with empty label
            LogicalPlan::ScanAll {
                variable,
                filter,
                optional: _,
            } => self.plan_scan_all(variable, filter.as_ref(), all_properties),

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
                ..
            } => {
                // Determine if this is a VLP pattern (same logic as plan_traverse)
                let is_variable_length =
                    path_variable.is_some() || *min_hops != 1 || *max_hops != 1;

                if is_variable_length {
                    self.plan_traverse_main_by_type_vlp(
                        input,
                        type_names,
                        direction.clone(),
                        source_variable,
                        target_variable,
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
                all_properties,
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
                input, predicate, ..
            } => self.plan_filter(input, predicate, all_properties),

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

            LogicalPlan::Apply { .. } => Err(anyhow!(
                "Apply (correlated subquery) not yet supported in DataFusion engine"
            )),

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
            } => self.plan_vector_knn(*label_id, variable, property, query.clone(), *k, *threshold),

            LogicalPlan::InvertedIndexLookup { .. } => Err(anyhow!(
                "Full-text search not yet supported in DataFusion engine"
            )),

            LogicalPlan::AllShortestPaths { .. } => Err(anyhow!(
                "allShortestPaths not yet supported in DataFusion engine"
            )),

            LogicalPlan::QuantifiedPattern { .. } => Err(anyhow!(
                "Quantified patterns not yet supported in DataFusion engine"
            )),

            LogicalPlan::RecursiveCTE { .. } => Err(anyhow!(
                "Recursive CTEs not yet supported in DataFusion engine"
            )),

            LogicalPlan::ProcedureCall { .. } => Err(anyhow!(
                "Procedure calls not yet supported in DataFusion engine"
            )),

            LogicalPlan::SubqueryCall { .. } => Err(anyhow!(
                "CALL subqueries not yet supported in DataFusion engine"
            )),

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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(filter_expr) = filter else {
            return Ok(plan);
        };

        let mut variable_kinds = HashMap::new();
        variable_kinds.insert(variable.to_string(), VariableKind::Node);
        let ctx = TranslationContext {
            parameters: self.params.clone(),
            variable_labels: HashMap::new(),
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

        self.apply_scan_filter(lookup_plan, variable, filter)
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

    /// Plan a vector KNN search.
    fn plan_vector_knn(
        &self,
        label_id: u16,
        variable: &str,
        property: &str,
        query_expr: Expr,
        k: usize,
        threshold: Option<f32>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let label_name = self
            .schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Unknown label ID: {}", label_id))?;

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
        );

        Ok(Arc::new(knn))
    }

    /// Plan a vertex scan.
    fn plan_scan(
        &self,
        label_id: u16,
        variable: &str,
        filter: Option<&Expr>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let label_name = self
            .schema
            .label_name_by_id(label_id)
            .ok_or_else(|| anyhow!("Unknown label ID: {}", label_id))?;

        // Resolve properties collected from the entire plan tree, expanding "*" wildcards
        let properties = self.resolve_properties(variable, label_name, all_properties);

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
            .map_or(false, |p| p.contains("*"))
        {
            scan_plan = self.add_structural_projection(scan_plan, variable, &properties)?;
        }

        self.apply_scan_filter(scan_plan, variable, filter)
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
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();

        let mut scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_schemaless_vertex_scan(
                self.graph_ctx.clone(),
                label_name.to_string(),
                variable.to_string(),
                properties.clone(),
                None, // Filter will be applied as FilterExec on top
            ));

        // If we need the full object (structural access), add a Struct projection
        if all_properties
            .get(variable)
            .map_or(false, |p| p.contains("*"))
        {
            scan_plan = self.add_structural_projection(scan_plan, variable, &properties)?;
        }

        self.apply_scan_filter(scan_plan, variable, filter)
    }

    /// Plan a multi-label vertex scan using the main vertices table.
    ///
    /// For patterns like `(n:A:B)`, scans vertices with ALL labels (intersection).
    fn plan_multi_label_scan(
        &self,
        labels: &[String],
        variable: &str,
        filter: Option<&Expr>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();

        let mut scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_multi_label_vertex_scan(
                self.graph_ctx.clone(),
                labels.to_vec(),
                variable.to_string(),
                properties.clone(),
                None,
            ));

        // If we need the full object (structural access), add a Struct projection
        if all_properties
            .get(variable)
            .map_or(false, |p| p.contains("*"))
        {
            scan_plan = self.add_structural_projection(scan_plan, variable, &properties)?;
        }

        self.apply_scan_filter(scan_plan, variable, filter)
    }

    /// Plan a scan of all vertices regardless of label.
    ///
    /// This is used for `MATCH (n)` without a label filter.
    /// Uses the schemaless scan with an empty label to signal "scan all".
    fn plan_scan_all(
        &self,
        variable: &str,
        filter: Option<&Expr>,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let properties: Vec<String> = all_properties
            .get(variable)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();

        let mut scan_plan: Arc<dyn ExecutionPlan> =
            Arc::new(GraphScanExec::new_schemaless_all_scan(
                self.graph_ctx.clone(),
                variable.to_string(),
                properties.clone(),
                None, // Filter will be applied as FilterExec on top
            ));

        // If we need the full object (structural access), add a Struct projection
        if all_properties
            .get(variable)
            .map_or(false, |p| p.contains("*"))
        {
            scan_plan = self.add_structural_projection(scan_plan, variable, &properties)?;
        }

        self.apply_scan_filter(scan_plan, variable, filter)
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
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let source_col = format!("{}._vid", source_variable);

        // Determine if this is a VLP pattern: either hops differ from 1..1, or path_variable is set.
        // The planner sets path_variable when range is present (e.g., *1..1 is VLP even though min=max=1).
        let is_variable_length = path_variable.is_some() || min_hops != 1 || max_hops != 1;

        let traverse_plan: Arc<dyn ExecutionPlan> = if !is_variable_length {
            // Extract edge properties for pushdown hydration, expanding "*" wildcards
            let edge_properties: Vec<String> = if let Some(edge_var) = step_variable {
                let has_wildcard = all_properties
                    .get(edge_var)
                    .is_some_and(|props| props.contains("*"));
                if has_wildcard {
                    // Expand to all properties across all matching edge types
                    edge_type_ids
                        .iter()
                        .filter_map(|eid| self.schema.edge_type_name_by_id(*eid))
                        .flat_map(|name| {
                            self.schema
                                .properties
                                .get(name)
                                .map(|p| p.keys().cloned().collect::<Vec<_>>())
                                .unwrap_or_default()
                        })
                        .collect()
                } else {
                    all_properties
                        .get(edge_var)
                        .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
                        .unwrap_or_default()
                }
            } else {
                Vec::new()
            };

            // Extract target vertex properties, expanding "*" wildcards
            let target_label_name_str = self.schema.label_name_by_id(target_label_id).unwrap_or("");
            let target_properties =
                self.resolve_properties(target_variable, target_label_name_str, all_properties);

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
            let used_edge_columns: Vec<String> = input_plan
                .schema()
                .fields()
                .iter()
                .filter_map(|f| {
                    let name = f.name();
                    if name.ends_with("._eid") || name.starts_with("__eid_to_") {
                        Some(name.clone())
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
                } else if min_hops == 0 {
                    // min_hops=0 but no path variable - just return input as-is
                    // (the target is the same as source for zero-length)
                    return Ok(input_plan);
                } else {
                    // No edges to traverse and min_hops > 0 means no results
                    return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                        input_plan.schema(),
                    )));
                }
            }
            if edge_type_ids.len() != 1 {
                return Err(anyhow!(
                    "Variable-length traversal only supports single edge type"
                ));
            }

            Arc::new(GraphVariableLengthTraverseExec::new(
                input_plan,
                source_col,
                edge_type_ids[0],
                adj_direction,
                min_hops,
                max_hops,
                target_variable.to_string(),
                path_variable.map(|s| s.to_string()),
                self.graph_ctx.clone(),
                optional,
            ))
        };

        // Apply target filter if present
        if let Some(filter_expr) = target_filter {
            // Build context with variable kinds for this traverse
            let mut variable_kinds = HashMap::new();
            variable_kinds.insert(source_variable.to_string(), VariableKind::Node);
            variable_kinds.insert(target_variable.to_string(), VariableKind::Node);
            if let Some(sv) = step_variable {
                variable_kinds.insert(sv.to_string(), VariableKind::Edge);
            }
            if let Some(pv) = path_variable {
                variable_kinds.insert(pv.to_string(), VariableKind::Path);
            }
            let ctx = TranslationContext {
                parameters: self.params.clone(),
                variable_labels: HashMap::new(),
                variable_kinds,
            };
            let df_filter = cypher_expr_to_df(filter_expr, Some(&ctx))?;
            let final_filter = if optional {
                // For OPTIONAL MATCH, allow NULL rows through (unmatched rows have NULL target VID)
                let target_vid_col = format!("{}._vid", target_variable);
                let is_null = DfExpr::IsNull(Box::new(DfExpr::Column(
                    datafusion::common::Column::from_name(&target_vid_col),
                )));
                DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                    Box::new(df_filter),
                    datafusion::logical_expr::Operator::Or,
                    Box::new(is_null),
                ))
            } else {
                df_filter
            };
            let schema = traverse_plan.schema();
            let session = self.session_ctx.read();
            let physical_filter =
                self.create_physical_filter_expr(&final_filter, &schema, &session)?;
            Ok(Arc::new(FilterExec::try_new(
                physical_filter,
                traverse_plan,
            )?))
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
        let source_col = format!("{}._vid", source_variable);

        // Extract edge properties for schemaless edges (all treated as Utf8/JSON)
        let edge_properties: Vec<String> = if let Some(edge_var) = step_variable {
            all_properties
                .get(edge_var)
                .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Extract target vertex properties
        let target_properties: Vec<String> = all_properties
            .get(target_variable)
            .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        // Create the schemaless traversal execution plan
        let traverse_plan = Arc::new(GraphTraverseMainExec::new(
            input_plan,
            source_col,
            type_names.to_vec(),
            adj_direction,
            target_variable.to_string(),
            step_variable.map(|s| s.to_string()),
            edge_properties,
            target_properties,
            self.graph_ctx.clone(),
            optional,
        ));

        Ok(traverse_plan)
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
        min_hops: usize,
        max_hops: usize,
        path_variable: Option<&str>,
        optional: bool,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;

        let adj_direction = convert_direction(direction);
        let source_col = format!("{}._vid", source_variable);

        // Extract target vertex properties
        let target_properties: Vec<String> = all_properties
            .get(target_variable)
            .map(|props| props.iter().filter(|p| *p != "*").cloned().collect())
            .unwrap_or_default();

        let traverse_plan = Arc::new(GraphVariableLengthTraverseMainExec::new(
            input_plan,
            source_col,
            type_names.to_vec(),
            adj_direction,
            min_hops,
            max_hops,
            target_variable.to_string(),
            path_variable.map(|s| s.to_string()),
            target_properties,
            self.graph_ctx.clone(),
            optional,
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
    fn plan_filter(
        &self,
        input: &LogicalPlan,
        predicate: &Expr,
        all_properties: &HashMap<String, HashSet<String>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_plan = self.plan_internal(input, all_properties)?;
        let schema = input_plan.schema();

        let ctx = self.translation_context_for_plan(input);
        let df_predicate = cypher_expr_to_df(predicate, Some(&ctx))?;

        let session = self.session_ctx.read();
        let physical_predicate =
            self.create_physical_filter_expr(&df_predicate, &schema, &session)?;

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
        let input_plan = input_plan;
        let schema = input_plan.schema();
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

        let session = self.session_ctx.read();
        let state = session.state();

        // Use DefaultPhysicalPlanner to properly resolve UDFs
        use datafusion::physical_planner::PhysicalPlanner;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();

        // Build translation context with variable kinds if we have a logical plan
        let ctx = context_plan.map(|p| self.translation_context_for_plan(p));

        let mut exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = Vec::new();

        for (expr, alias) in projections {
            // Handle whole-node/relationship projection: RETURN n
            // Always trigger fallback for bare variable projections.
            // The execute_subplan path properly materializes full Node/Edge objects,
            // while DataFusion expansion returns individual columns which breaks
            // the type system (user expects Value::Node, not individual properties).
            if let Expr::Variable(var_name) = expr {
                let prefix = format!("{}.", var_name);
                let matching_fields: Vec<_> = schema
                    .fields()
                    .iter()
                    .filter(|f| f.name().starts_with(&prefix))
                    .collect();

                // If there are any matching columns, fallback to execute_subplan
                // which materializes full Node/Edge objects.
                if !matching_fields.is_empty() {
                    return Err(anyhow::anyhow!(
                        "Bare variable '{}' requires fallback to materialize full node/edge object",
                        var_name
                    ));
                }
                // Fall through to normal translation if no matching columns at all
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

            let df_expr = cypher_expr_to_df(expr, ctx.as_ref())?;
            // Resolve DummyUdf placeholders to registered UDFs
            let resolved_expr = Self::resolve_udfs(&df_expr, &state)?;
            let physical_expr = planner.create_physical_expr(&resolved_expr, &df_schema, &state)?;

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
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

        let session = self.session_ctx.read();
        let state = session.state();

        // Use DefaultPhysicalPlanner to properly resolve UDFs
        use datafusion::physical_planner::PhysicalPlanner;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();

        // Build translation context with variable kinds from the input plan
        let ctx = self.translation_context_for_plan(input);

        // Translate group by expressions
        let mut group_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
            Vec::new();
        for expr in group_by {
            let df_expr = cypher_expr_to_df(expr, Some(&ctx))?;
            // Resolve DummyUdf placeholders to registered UDFs
            let resolved_expr = Self::resolve_udfs(&df_expr, &state)?;
            let physical_expr = planner.create_physical_expr(&resolved_expr, &df_schema, &state)?;
            let name = expr.to_string_repr();
            group_exprs.push((physical_expr, name));
        }

        let physical_group_by = PhysicalGroupBy::new_single(group_exprs);

        // Translate aggregates
        let aggr_exprs = self.translate_aggregates(aggregates, &schema, &state, &ctx)?;

        // Filter expressions must match aggregate expressions in length.
        let filter_exprs = vec![None; aggr_exprs.len()];

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
    ) -> Result<Vec<Arc<AggregateFunctionExpr>>> {
        use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};

        let mut result = Vec::new();

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
                    if matches!(
                        args.first(),
                        Some(uni_cypher::ast::Expr::Variable(_))
                            | Some(uni_cypher::ast::Expr::Wildcard)
                    ) {
                        count(datafusion::logical_expr::lit(1))
                    } else {
                        count(get_arg()?)
                    }
                }
                "sum" => sum(datafusion::logical_expr::cast(
                    get_arg()?,
                    datafusion::arrow::datatypes::DataType::Float64,
                )),
                "avg" => avg(datafusion::logical_expr::cast(
                    get_arg()?,
                    datafusion::arrow::datatypes::DataType::Float64,
                )),
                "min" => min(get_arg()?),
                "max" => max(get_arg()?),
                "collect" => datafusion::functions_aggregate::array_agg::array_agg(get_arg()?),
                _ => return Err(anyhow!("Unsupported aggregate function: {}", name)),
            };

            // Apply DISTINCT if needed
            let df_agg = if *distinct {
                use datafusion::prelude::ExprFunctionExt;
                df_agg.distinct().build().map_err(|e| anyhow!("{}", e))?
            } else {
                df_agg
            };

            // Convert to physical aggregate
            let physical_agg = self.create_physical_aggregate(&df_agg, schema, state)?;
            result.push(physical_agg);
        }

        Ok(result)
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
        let df_sort_exprs: Vec<DfSortExpr> = order_by
            .iter()
            .map(|item| {
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
                let asc = item.ascending;
                let nulls_first = !asc; // Standard SQL behavior: nulls last for ASC, first for DESC

                Ok(DfSortExpr::new(df_expr, asc, nulls_first))
            })
            .collect::<Result<Vec<_>>>()?;

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
        let coerced_expr = Self::apply_type_coercion(&resolved_expr, &df_schema)?;

        // Use SessionState's create_physical_expr to properly resolve UDFs
        use datafusion::physical_planner::PhysicalPlanner;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        let physical = planner.create_physical_expr(&coerced_expr, &df_schema, &state)?;

        Ok(physical)
    }

    /// Resolve DummyUdf placeholders to actual registered UDFs from SessionState.
    fn resolve_udfs(expr: &DfExpr, state: &datafusion::execution::SessionState) -> Result<DfExpr> {
        use datafusion::logical_expr::Expr as DfExpr;

        match expr {
            DfExpr::ScalarFunction(func) => {
                // Check if this is a DummyUdf that needs to be resolved
                let udf_name = func.func.name();

                // Resolve args recursively regardless of registration status
                let resolved_args: Vec<DfExpr> = func
                    .args
                    .iter()
                    .map(|arg| Self::resolve_udfs(arg, state))
                    .collect::<Result<Vec<_>>>()?;

                // Use registered UDF if available, otherwise keep original
                let func_ref = match state.scalar_functions().get(udf_name) {
                    Some(registered_udf) => registered_udf.clone(),
                    None => func.func.clone(),
                };

                Ok(DfExpr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: func_ref,
                        args: resolved_args,
                    },
                ))
            }
            // Recursively resolve UDFs in other expression types
            DfExpr::BinaryExpr(binary) => {
                Ok(DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                    left: Box::new(Self::resolve_udfs(&binary.left, state)?),
                    op: binary.op,
                    right: Box::new(Self::resolve_udfs(&binary.right, state)?),
                }))
            }
            DfExpr::Not(inner) => Ok(DfExpr::Not(Box::new(Self::resolve_udfs(inner, state)?))),
            DfExpr::IsNull(inner) => {
                Ok(DfExpr::IsNull(Box::new(Self::resolve_udfs(inner, state)?)))
            }
            DfExpr::IsNotNull(inner) => Ok(DfExpr::IsNotNull(Box::new(Self::resolve_udfs(
                inner, state,
            )?))),
            DfExpr::Negative(inner) => Ok(DfExpr::Negative(Box::new(Self::resolve_udfs(
                inner, state,
            )?))),
            // For other expression types, return as-is
            _ => Ok(expr.clone()),
        }
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
            let col_expr =
                Arc::new(datafusion::physical_expr::expressions::Column::new(field.name(), i));
            proj_exprs.push((col_expr, field.name().clone()));
        }

        // 2. Add the named_struct AS variable
        let mut struct_args = Vec::with_capacity(properties.len() * 2);
        for prop in properties {
            struct_args.push(lit(prop.clone()));
            struct_args.push(DfExpr::Column(datafusion::common::Column::from_name(format!(
                "{}.{}",
                variable, prop
            ))));
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

    /// Create a physical aggregate expression.
    fn create_physical_aggregate(
        &self,
        expr: &DfExpr,
        schema: &SchemaRef,
        state: &SessionState,
    ) -> Result<Arc<AggregateFunctionExpr>> {
        use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;

        // Build a DFSchema from the Arrow schema for the function call
        let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

        // The function returns (AggregateFunctionExpr, Option<filter>, Vec<ordering>)
        let (agg_expr, _filter, _ordering) = create_aggregate_expr_and_maybe_filter(
            expr,
            &df_schema,
            schema.as_ref(),
            state.execution_props(),
        )?;
        Ok(agg_expr)
    }

    /// Apply type coercion to a DataFusion expression.
    ///
    /// Resolves numeric type mismatches (e.g., Int32 vs Int64, Boolean vs Int64)
    /// by inserting explicit CAST nodes. This is needed because our schema may
    /// declare properties as one numeric type while literals are a different type.
    fn apply_type_coercion(expr: &DfExpr, schema: &datafusion::common::DFSchema) -> Result<DfExpr> {
        use datafusion::logical_expr::Operator;
        match expr {
            DfExpr::BinaryExpr(binary) => {
                let left = Self::apply_type_coercion(&binary.left, schema)?;
                let right = Self::apply_type_coercion(&binary.right, schema)?;

                // For comparison and arithmetic operators, coerce numeric types
                let is_comparison_or_arithmetic = matches!(
                    binary.op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::Plus
                        | Operator::Minus
                        | Operator::Multiply
                        | Operator::Divide
                        | Operator::Modulo
                );

                if is_comparison_or_arithmetic {
                    let left_type = left.get_type(schema).ok();
                    let right_type = right.get_type(schema).ok();

                    if let (Some(lt), Some(rt)) = (&left_type, &right_type)
                        && lt != rt
                        && lt.is_numeric()
                        && rt.is_numeric()
                    {
                        // Coerce to the wider numeric type
                        let target = wider_numeric_type(lt, rt);
                        let coerced_left = if *lt != target {
                            datafusion::logical_expr::cast(left, target.clone())
                        } else {
                            left
                        };
                        let coerced_right = if *rt != target {
                            datafusion::logical_expr::cast(right, target)
                        } else {
                            right
                        };
                        return Ok(DfExpr::BinaryExpr(
                            datafusion::logical_expr::expr::BinaryExpr::new(
                                Box::new(coerced_left),
                                binary.op,
                                Box::new(coerced_right),
                            ),
                        ));
                    }
                }

                Ok(DfExpr::BinaryExpr(
                    datafusion::logical_expr::expr::BinaryExpr::new(
                        Box::new(left),
                        binary.op,
                        Box::new(right),
                    ),
                ))
            }
            // For other expression types, return as-is
            _ => Ok(expr.clone()),
        }
    }
}

/// Returns the wider of two numeric DataTypes for type coercion.
///
/// Follows standard numeric promotion rules:
/// - Any Float type wins over Int types
/// - Float64 > Float32
/// - Int64 > Int32 > Int16 > Int8
fn wider_numeric_type(
    a: &datafusion::arrow::datatypes::DataType,
    b: &datafusion::arrow::datatypes::DataType,
) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;

    fn numeric_rank(dt: &DataType) -> u8 {
        match dt {
            DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 => 3,
            DataType::Int64 | DataType::UInt64 => 4,
            DataType::Float16 => 5,
            DataType::Float32 => 6,
            DataType::Float64 => 7,
            _ => 0,
        }
    }

    if numeric_rank(a) >= numeric_rank(b) {
        a.clone()
    } else {
        b.clone()
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
            ..
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(source_variable.clone(), VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            if let Some(sv) = step_variable {
                kinds.insert(sv.clone(), VariableKind::Edge);
            }
            if let Some(pv) = path_variable {
                kinds.insert(pv.clone(), VariableKind::Path);
            }
        }
        LogicalPlan::TraverseMainByType {
            input,
            source_variable,
            target_variable,
            step_variable,
            path_variable,
            ..
        } => {
            collect_variable_kinds(input, kinds);
            kinds.insert(source_variable.clone(), VariableKind::Node);
            kinds.insert(target_variable.clone(), VariableKind::Node);
            if let Some(sv) = step_variable {
                kinds.insert(sv.clone(), VariableKind::Edge);
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
        // Leaf nodes with no variables or not applicable
        LogicalPlan::Empty
        | LogicalPlan::LoadCsv { .. }
        | LogicalPlan::ProcedureCall { .. }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
