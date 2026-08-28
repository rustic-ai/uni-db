// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::df_expr::{TranslationContext, VariableKind, cypher_expr_to_df};
use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{execute_subplan_with_outer_vars, extract_row_params};
use crate::query::df_graph::comprehension::ListComprehensionExecExpr;
use crate::query::df_graph::pattern_comprehension::{
    PatternComprehensionExecExpr, analyze_pattern, build_inner_schema, collect_inner_properties,
};
use crate::query::df_graph::pattern_exists::{
    PatternExistsExecExpr, extract_pattern_from_exists_query, extract_target_property_predicates,
};
use crate::query::df_graph::quantifier::{QuantifierExecExpr, QuantifierType};
use crate::query::df_graph::raw_bytes_marker;
use crate::query::df_graph::reduce::ReduceExecExpr;
use crate::query::df_graph::similar_to_expr::SimilarToExecExpr;
use crate::query::df_graph::unwind::arrow_to_json_value;
use crate::query::planner::QueryPlanner;
use crate::query::similar_to::SimilarToError;
use anyhow::{Result, anyhow};
use arrow_array::builder::BooleanBuilder;
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::expressions::binary;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use uni_common::Value;
use uni_common::core::schema::{DistanceMetric, IndexDefinition, Schema as UniSchema};
use uni_cypher::ast::{
    BinaryOp, Clause, CypherLiteral, Expr, MatchClause, Query, ReturnClause, ReturnItem, SortItem,
    Statement, UnaryOp, UnwindClause, WithClause,
};
use uni_store::storage::manager::StorageManager;

/// Check if a data type represents CypherValue (LargeBinary) or BTIC (FixedSizeBinary(24)).
/// Both need to route through Cypher UDFs for comparison operators.
fn is_cypher_value_type(dt: Option<&DataType>) -> bool {
    dt.is_some_and(|t| matches!(t, DataType::LargeBinary | DataType::FixedSizeBinary(24)))
}

/// Resolve the element type for a list expression.
///
/// Extracts the element type from List/LargeList/Null/LargeBinary data types.
/// Falls back to the provided fallback type for LargeBinary, and returns an
/// error with the provided context for unsupported types.
///
/// # Errors
///
/// Returns an error if the data type is not a recognized list type.
fn resolve_list_element_type(
    list_data_type: &DataType,
    large_binary_fallback: DataType,
    context: &str,
) -> Result<DataType> {
    match list_data_type {
        DataType::List(field) | DataType::LargeList(field) => Ok(field.data_type().clone()),
        DataType::Null => Ok(DataType::Null),
        DataType::LargeBinary => Ok(large_binary_fallback),
        _ => Err(anyhow!(
            "{} input must be a list, got {:?}",
            context,
            list_data_type
        )),
    }
}

/// Physical expression wrapper that converts LargeList<T> to LargeBinary (CypherValue).
///
/// Used in CASE expressions to unify branch types when mixing typed lists
/// (e.g., from list comprehensions) with CypherValue-encoded lists.
#[derive(Debug)]
struct LargeListToCypherValueExpr {
    child: Arc<dyn PhysicalExpr>,
}

impl LargeListToCypherValueExpr {
    fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }
}

impl std::fmt::Display for LargeListToCypherValueExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LargeListToCypherValue({})", self.child)
    }
}

impl PartialEq for LargeListToCypherValueExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.child, &other.child)
    }
}

impl Eq for LargeListToCypherValueExpr {}

impl std::hash::Hash for LargeListToCypherValueExpr {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {
        // Hash based on type since we can't hash PhysicalExpr
        std::any::type_name::<Self>().hash(_state);
    }
}

impl PartialEq<dyn std::any::Any> for LargeListToCypherValueExpr {
    fn eq(&self, other: &dyn std::any::Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for LargeListToCypherValueExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> datafusion::error::Result<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion::error::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        use datafusion::arrow::compute::cast;
        use datafusion::logical_expr::ColumnarValue;

        let child_result = self.child.evaluate(batch)?;
        let child_array = child_result.into_array(batch.num_rows())?;

        // Normalize List → LargeList (pattern from quantifier.rs:182-189)
        let list_array = if let DataType::List(field) = child_array.data_type() {
            let target_type = DataType::LargeList(field.clone());
            cast(&child_array, &target_type).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "List to LargeList cast failed: {e}"
                ))
            })?
        } else {
            child_array.clone()
        };

        // If already LargeBinary, pass through
        if list_array.data_type() == &DataType::LargeBinary {
            return Ok(ColumnarValue::Array(list_array));
        }

        // Convert LargeList to CypherValue
        if let Some(large_list) = list_array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeListArray>()
        {
            let cv_array =
                crate::query::df_graph::common::typed_large_list_to_cv_array(large_list)?;
            Ok(ColumnarValue::Array(cv_array))
        } else {
            Err(datafusion::error::DataFusionError::Execution(format!(
                "Expected List or LargeList, got {:?}",
                list_array.data_type()
            )))
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "LargeListToCypherValueExpr expects exactly 1 child".to_string(),
            ));
        }
        Ok(Arc::new(LargeListToCypherValueExpr::new(
            children[0].clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "LargeListToCypherValue({})", self.child)
    }
}

/// Compiler for converting Cypher expressions directly to DataFusion Physical Expressions.
pub struct CypherPhysicalExprCompiler<'a> {
    state: &'a SessionState,
    translation_ctx: Option<&'a TranslationContext>,
    graph_ctx: Option<Arc<GraphExecutionContext>>,
    uni_schema: Option<Arc<UniSchema>>,
    /// Session context for EXISTS subquery execution.
    session_ctx: Option<Arc<RwLock<SessionContext>>>,
    /// Storage manager for EXISTS subquery execution.
    storage: Option<Arc<StorageManager>>,
    /// Query parameters for EXISTS subquery execution.
    params: HashMap<String, Value>,
    /// Entity variable names from outer scopes (for nested EXISTS).
    /// A target variable in a pattern predicate that appears here is a
    /// correlated reference requiring per-row evaluation, not a fresh binding.
    outer_entity_vars: HashSet<String>,
}

impl<'a> CypherPhysicalExprCompiler<'a> {
    pub fn new(state: &'a SessionState, translation_ctx: Option<&'a TranslationContext>) -> Self {
        Self {
            state,
            translation_ctx,
            graph_ctx: None,
            uni_schema: None,
            session_ctx: None,
            storage: None,
            params: HashMap::new(),
            outer_entity_vars: HashSet::new(),
        }
    }

    /// Build a scoped compiler that excludes the given variables from the translation context.
    ///
    /// When compiling inner expressions for list comprehensions, reduce, or quantifiers,
    /// loop variables must be removed from `variable_kinds` so that property access on
    /// them does not incorrectly generate flat columns that don't exist in the inner schema.
    ///
    /// If none of the `exclude_vars` are present in the current context, the returned
    /// compiler simply reuses `self`'s translation context unchanged.
    ///
    /// The caller must own `scoped_ctx_slot` and keep it alive for the returned compiler's
    /// lifetime.
    fn scoped_compiler<'b>(
        &'b self,
        exclude_vars: &[&str],
        scoped_ctx_slot: &'b mut Option<TranslationContext>,
    ) -> CypherPhysicalExprCompiler<'b>
    where
        'a: 'b,
    {
        let needs_scoping = self.translation_ctx.is_some_and(|ctx| {
            exclude_vars
                .iter()
                .any(|v| ctx.variable_kinds.contains_key(*v))
        });

        let ctx_ref = if needs_scoping {
            let ctx = self.translation_ctx.unwrap();
            let mut new_kinds = ctx.variable_kinds.clone();
            for v in exclude_vars {
                new_kinds.remove(*v);
            }
            *scoped_ctx_slot = Some(TranslationContext {
                parameters: ctx.parameters.clone(),
                outer_values: ctx.outer_values.clone(),
                variable_labels: ctx.variable_labels.clone(),
                variable_kinds: new_kinds,
                node_variable_hints: ctx.node_variable_hints.clone(),
                mutation_edge_hints: ctx.mutation_edge_hints.clone(),
                statement_time: ctx.statement_time,
            });
            scoped_ctx_slot.as_ref()
        } else {
            self.translation_ctx
        };

        CypherPhysicalExprCompiler {
            state: self.state,
            translation_ctx: ctx_ref,
            graph_ctx: self.graph_ctx.clone(),
            uni_schema: self.uni_schema.clone(),
            session_ctx: self.session_ctx.clone(),
            storage: self.storage.clone(),
            params: self.params.clone(),
            outer_entity_vars: self.outer_entity_vars.clone(),
        }
    }

    /// Build a compiler that treats `entity_vars` as graph entities.
    ///
    /// The mirror of [`Self::scoped_compiler`]. A pattern comprehension compiles
    /// its predicate and map expression against an *inner* schema whose entity
    /// columns are flat — `x._vid`, `x.name` — but the translation context it
    /// inherits describes only the outer scope. `translate_property_access`
    /// picks between `Column("x.name")` and `index(Column("x"), 'name')` on
    /// exactly that context, so without this the inner variables compile to an
    /// index into a bare `x` the inner schema does not carry (#189).
    ///
    /// A context is created even when there is none to inherit: absent context
    /// means "not a graph entity", which is the wrong answer here rather than an
    /// unknown one.
    ///
    /// The caller must own `scoped_ctx_slot` and keep it alive for the returned
    /// compiler's lifetime.
    fn entity_scoped_compiler<'b>(
        &'b self,
        entity_vars: &[(String, VariableKind)],
        scoped_ctx_slot: &'b mut Option<TranslationContext>,
    ) -> CypherPhysicalExprCompiler<'b>
    where
        'a: 'b,
    {
        let ctx_ref = if entity_vars.is_empty() {
            self.translation_ctx
        } else {
            let mut ctx = self.translation_ctx.cloned().unwrap_or_default();
            for (var, kind) in entity_vars {
                ctx.variable_kinds.insert(var.clone(), *kind);
            }
            *scoped_ctx_slot = Some(ctx);
            scoped_ctx_slot.as_ref()
        };

        CypherPhysicalExprCompiler {
            state: self.state,
            translation_ctx: ctx_ref,
            graph_ctx: self.graph_ctx.clone(),
            uni_schema: self.uni_schema.clone(),
            session_ctx: self.session_ctx.clone(),
            storage: self.storage.clone(),
            params: self.params.clone(),
            outer_entity_vars: self.outer_entity_vars.clone(),
        }
    }

    /// Attach graph context and schema for pattern comprehension support.
    pub fn with_graph_ctx(
        mut self,
        graph_ctx: Arc<GraphExecutionContext>,
        uni_schema: Arc<UniSchema>,
    ) -> Self {
        self.graph_ctx = Some(graph_ctx);
        self.uni_schema = Some(uni_schema);
        self
    }

    /// Attach full subquery context for EXISTS support.
    pub fn with_subquery_ctx(
        mut self,
        graph_ctx: Arc<GraphExecutionContext>,
        uni_schema: Arc<UniSchema>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        params: HashMap<String, Value>,
        outer_entity_vars: HashSet<String>,
    ) -> Self {
        self.graph_ctx = Some(graph_ctx);
        self.uni_schema = Some(uni_schema);
        self.session_ctx = Some(session_ctx);
        self.storage = Some(storage);
        self.params = params;
        self.outer_entity_vars = outer_entity_vars;
        self
    }

    /// Compile a Cypher expression into a DataFusion PhysicalExpr.
    pub fn compile(&self, expr: &Expr, input_schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        match expr {
            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => self.compile_list_comprehension(
                variable,
                list,
                where_clause.as_deref(),
                map_expr,
                input_schema,
            ),
            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => self.compile_quantifier(quantifier, variable, list, predicate, input_schema),
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr: expression,
            } => self.compile_reduce(accumulator, init, variable, list, expression, input_schema),
            // For BinaryOp, check if children contain custom expressions or CypherValue types
            Expr::BinaryOp { left, op, right } => {
                self.compile_binary_op_dispatch(left, op, right, input_schema)
            }
            Expr::UnaryOp { op, expr: inner } => {
                if matches!(op, UnaryOp::Not) {
                    let mut inner_phy = self.compile(inner, input_schema)?;
                    if let Ok(DataType::LargeBinary) = inner_phy.data_type(input_schema) {
                        inner_phy = self.wrap_with_cv_to_bool(inner_phy)?;
                    }
                    self.compile_unary_op(op, inner_phy, input_schema)
                } else if Self::contains_custom_expr(inner) {
                    let inner_phy = self.compile(inner, input_schema)?;
                    self.compile_unary_op(op, inner_phy, input_schema)
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            Expr::IsNull(inner) => {
                if Self::contains_custom_expr(inner) {
                    let inner_phy = self.compile(inner, input_schema)?;
                    Ok(datafusion::physical_expr::expressions::is_null(inner_phy)
                        .map_err(|e| anyhow!("Failed to create is_null: {}", e))?)
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            Expr::IsNotNull(inner) => {
                if Self::contains_custom_expr(inner) {
                    let inner_phy = self.compile(inner, input_schema)?;
                    Ok(
                        datafusion::physical_expr::expressions::is_not_null(inner_phy)
                            .map_err(|e| anyhow!("Failed to create is_not_null: {}", e))?,
                    )
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }
            // In operator is Expr::In { expr, list }
            Expr::In {
                expr: left,
                list: right,
            } => {
                if Self::contains_custom_expr(left) || Self::contains_custom_expr(right) {
                    let left_phy = self.compile(left, input_schema)?;
                    let right_phy = self.compile(right, input_schema)?;

                    let left_type = left_phy
                        .data_type(input_schema)
                        .unwrap_or(DataType::LargeBinary);
                    let right_type = right_phy
                        .data_type(input_schema)
                        .unwrap_or(DataType::LargeBinary);

                    self.plan_binary_udf("_cypher_in", left_phy, right_phy, left_type, right_type)?
                        .ok_or_else(|| anyhow!("_cypher_in UDF not found"))
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }

            // A raw-`Bytes` list literal (e.g. `[b.data]`): compile the `make_array`
            // then mark its child field so element-extraction (`head`/`index`/…) and
            // the final read decode each element as raw `Bytes` rather than via the
            // tagged codec. CV-routed / mixed lists carry no marker (analyzer is
            // conservative), so this never disturbs pattern-comprehension/VLP.
            Expr::List(_) if raw_bytes_marker::is_markable_list(expr, input_schema) => {
                let inner = self.compile_standard(expr, input_schema)?;
                Ok(Arc::new(raw_bytes_marker::RawBytesMarkerExpr::list_child(
                    inner,
                )))
            }

            // Recursively check other composite types if necessary.
            Expr::List(items) if items.iter().any(Self::contains_custom_expr) => Err(anyhow!(
                "List literals containing comprehensions not yet supported in compiler"
            )),
            Expr::Map(entries) if entries.iter().any(|(_, v)| Self::contains_custom_expr(v)) => {
                Err(anyhow!(
                    "Map literals containing comprehensions not yet supported in compiler"
                ))
            }

            // Property access on a struct column — e.g. `x.a` where `x` is Struct
            Expr::Property(base, prop) => self.compile_property_access(base, prop, input_schema),

            // Bracket access on a struct column — e.g. `x['a']` where `x` is Struct
            Expr::ArrayIndex { array, index } => {
                self.compile_array_index(array, index, input_schema)
            }

            // Pattern comprehension: [(a)-[:REL]->(b) WHERE pred | expr]
            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => self.compile_pattern_comprehension(
                path_variable,
                pattern,
                where_clause.as_deref(),
                map_expr,
                input_schema,
            ),

            // EXISTS subquery: vectorized for pattern predicates, per-row for general EXISTS.
            Expr::Exists {
                query,
                from_pattern_predicate,
            } => {
                if *from_pattern_predicate {
                    match self.compile_pattern_exists(query, input_schema) {
                        Ok(expr) => Ok(expr),
                        Err(e) => {
                            log::debug!(
                                "Pattern exists vectorization failed, falling back to subquery: {e}"
                            );
                            self.compile_exists(query)
                        }
                    }
                } else {
                    self.compile_exists(query)
                }
            }

            // The other two members of the subquery family. They reach here
            // rather than the aggregate machinery because they are scalar
            // subqueries, evaluated once per outer row.
            Expr::CountSubquery(query) => {
                self.compile_scalar_subquery(query, SubqueryResult::Count)
            }
            Expr::CollectSubquery(query) => {
                self.compile_scalar_subquery(query, SubqueryResult::Collect)
            }

            // FunctionCall wrapping a custom expression (e.g. size(comprehension))
            // A coalesce that mixes a raw-`Bytes` arg with a non-raw, non-null one:
            // rewrite to an explicit CASE whose THEN/ELSE values are CypherValue-
            // encoded (while WHEN tests the original args for null), so the output
            // column is uniformly codec-decodable and raw bytes round-trip to
            // `Value::Bytes`. A uniformly raw-or-null coalesce is left alone and marked
            // at the projection (see `raw_bytes_marker::is_raw_scalar`).
            Expr::FunctionCall { name, args, .. }
                if name.eq_ignore_ascii_case("coalesce")
                    && args.len() > 1
                    && raw_bytes_marker::coalesce_needs_cv_unify(args, input_schema) =>
            {
                let cv_wrap = |a: &Expr| Expr::FunctionCall {
                    name: "_cypher_scalar_to_cv".to_string(),
                    args: vec![a.clone()],
                    distinct: false,
                    window_spec: None,
                };
                let (last, init) = args.split_last().unwrap();
                let when_then = init
                    .iter()
                    .map(|a| (Expr::IsNotNull(Box::new(a.clone())), cv_wrap(a)))
                    .collect();
                let case = Expr::Case {
                    expr: None,
                    when_then,
                    else_expr: Some(Box::new(cv_wrap(last))),
                };
                self.compile_standard(&case, input_schema)
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
                ..
            } => {
                if name.eq_ignore_ascii_case("similar_to") {
                    return self.compile_similar_to(args, input_schema);
                }
                // Route through the recursive custom-args path when an argument is a
                // raw-`Bytes` list literal, so its `make_array` child gets marked (the
                // list-consumer UDFs `head`/`last`/`tail`/… then decode raw bytes).
                if args.iter().any(Self::contains_custom_expr)
                    || args
                        .iter()
                        .any(|a| raw_bytes_marker::is_markable_list(a, input_schema))
                {
                    self.compile_function_with_custom_args(name, args, *distinct, input_schema)
                } else {
                    self.compile_standard(expr, input_schema)
                }
            }

            // CASE expression - dispatch based on whether it contains custom expressions
            Expr::Case {
                expr: case_operand,
                when_then,
                else_expr,
            } => {
                // Check if operand or any branch contains custom expressions
                let has_custom = case_operand
                    .as_deref()
                    .is_some_and(Self::contains_custom_expr)
                    || when_then.iter().any(|(w, t)| {
                        Self::contains_custom_expr(w) || Self::contains_custom_expr(t)
                    })
                    || else_expr.as_deref().is_some_and(Self::contains_custom_expr);

                if has_custom {
                    // Use compile_case() for CypherValue boolean conversion and
                    // LargeList/LargeBinary type unification
                    self.compile_case(case_operand, when_then, else_expr, input_schema)
                } else {
                    // Standard compilation path - goes through apply_type_coercion which handles:
                    // 1. Simple CASE → Generic CASE rewriting with cross-type equality
                    // 2. Type coercion for CASE result branches
                    // 3. Numeric widening for comparisons
                    self.compile_standard(expr, input_schema)
                }
            }

            // LabelCheck: delegate to standard compilation (uses cypher_expr_to_df)
            Expr::LabelCheck { .. } => self.compile_standard(expr, input_schema),

            // Default to standard compilation for leaf nodes or non-custom trees
            _ => self.compile_standard(expr, input_schema),
        }
    }

    /// Dispatch binary op compilation, checking for custom expressions and CypherValue types.
    fn compile_binary_op_dispatch(
        &self,
        left: &Expr,
        op: &BinaryOp,
        right: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if matches!(op, BinaryOp::Eq | BinaryOp::NotEq)
            && let (Expr::Variable(lv), Expr::Variable(rv)) = (left, right)
            && let Some(ctx) = self.translation_ctx
            && let (Some(lk), Some(rk)) = (ctx.variable_kinds.get(lv), ctx.variable_kinds.get(rv))
        {
            let identity_prop = match (lk, rk) {
                (VariableKind::Node, VariableKind::Node) => Some("_vid"),
                (VariableKind::Edge, VariableKind::Edge) => Some("_eid"),
                _ => None,
            };

            if let Some(id_prop) = identity_prop {
                return self.compile_standard(
                    &Expr::BinaryOp {
                        left: Box::new(Expr::Property(
                            Box::new(Expr::Variable(lv.clone())),
                            id_prop.to_string(),
                        )),
                        op: *op,
                        right: Box::new(Expr::Property(
                            Box::new(Expr::Variable(rv.clone())),
                            id_prop.to_string(),
                        )),
                    },
                    input_schema,
                );
            }
        }

        // XOR and Pow: always route through compile_standard.
        // compile_binary_op does not support these operators. The standard path
        // correctly maps XOR → _cypher_xor UDF and Pow → power() function.
        if matches!(op, BinaryOp::Xor | BinaryOp::Pow) {
            return self.compile_standard(
                &Expr::BinaryOp {
                    left: Box::new(left.clone()),
                    op: *op,
                    right: Box::new(right.clone()),
                },
                input_schema,
            );
        }

        if Self::contains_custom_expr(left) || Self::contains_custom_expr(right) {
            let left_phy = self.compile(left, input_schema)?;
            let right_phy = self.compile(right, input_schema)?;
            return self.compile_binary_op(op, left_phy, right_phy, input_schema);
        }

        // For Add with a list-producing operand (AST-level detection),
        // compile through the standard path which uses cypher_expr_to_df
        // to correctly route list + scalar to _cypher_list_concat.
        if *op == BinaryOp::Add && (Self::is_list_producing(left) || Self::is_list_producing(right))
        {
            return self.compile_standard(
                &Expr::BinaryOp {
                    left: Box::new(left.clone()),
                    op: *op,
                    right: Box::new(right.clone()),
                },
                input_schema,
            );
        }

        // Compile sub-expressions to check their types. If either operand
        // produces LargeBinary (CypherValue), standard Arrow kernels will fail at
        // runtime for comparisons. Route through compile_binary_op which
        // dispatches to Cypher comparison UDFs.
        let left_phy = self.compile(left, input_schema)?;
        let right_phy = self.compile(right, input_schema)?;
        let left_dt = left_phy.data_type(input_schema).ok();
        let right_dt = right_phy.data_type(input_schema).ok();
        let has_cv =
            is_cypher_value_type(left_dt.as_ref()) || is_cypher_value_type(right_dt.as_ref());

        if has_cv {
            // CypherValue types need special handling via compile_binary_op
            self.compile_binary_op(op, left_phy, right_phy, input_schema)
        } else {
            // Standard types: use compile_standard to get proper type coercion
            // (e.g., Int64 == Float64 requires coercion to work)
            self.compile_standard(
                &Expr::BinaryOp {
                    left: Box::new(left.clone()),
                    op: *op,
                    right: Box::new(right.clone()),
                },
                input_schema,
            )
        }
    }

    /// Try to compile struct field access for a variable.
    ///
    /// Returns `Some(expr)` if the variable is a Struct column and can be accessed,
    /// `None` if fallback to standard compilation is needed.
    ///
    /// Entity structs (`node_struct_fields` / `edge_struct_fields`) expose only
    /// their internal fields — `_vid`/`_eid`, `_labels`/`_type_name`, endpoints —
    /// as real struct fields; user properties live inside the `properties`
    /// CypherValue blob. A name that is not a struct field is therefore looked up
    /// in that blob before falling back to Cypher's missing-key null, so
    /// `[e IN r | e.tag]` over a native `List<Struct(edge)>` agrees with
    /// `[e IN relationships(p) | e.tag]` over the CypherValue-encoded list.
    fn try_compile_struct_field(
        &self,
        var_name: &str,
        field_name: &str,
        input_schema: &Schema,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        let col_idx = input_schema.index_of(var_name).ok()?;
        let DataType::Struct(struct_fields) = input_schema.field(col_idx).data_type() else {
            return None;
        };
        let col_expr: Arc<dyn PhysicalExpr> = Arc::new(
            datafusion::physical_expr::expressions::Column::new(var_name, col_idx),
        );

        if let Some(field_idx) = struct_fields.iter().position(|f| f.name() == field_name) {
            let output_type = struct_fields[field_idx].data_type().clone();
            return Some(Arc::new(StructFieldAccessExpr::new(
                col_expr,
                field_idx,
                output_type,
            )));
        }

        if let Some(expr) = self.try_compile_struct_property(&col_expr, struct_fields, field_name) {
            return Some(expr);
        }

        // Cypher semantics: accessing a missing key returns null. Type it as
        // LargeBinary (an encoded CypherValue null) rather than `ScalarValue::Null`
        // so the value composes with the CypherValue list/comparison machinery —
        // an untyped null cannot be encoded into a result list.
        Some(Arc::new(
            datafusion::physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::LargeBinary(None),
            ),
        ))
    }

    /// Look a name up inside an entity struct's `properties` CypherValue blob.
    ///
    /// Returns `None` when the struct has no such blob (an ordinary struct, where
    /// a missing field really is a missing key).
    fn try_compile_struct_property(
        &self,
        col_expr: &Arc<dyn PhysicalExpr>,
        struct_fields: &arrow_schema::Fields,
        field_name: &str,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        let props_idx = struct_fields
            .iter()
            .position(|f| f.name() == "properties" && *f.data_type() == DataType::LargeBinary)?;

        let props_expr: Arc<dyn PhysicalExpr> = Arc::new(StructFieldAccessExpr::new(
            col_expr.clone(),
            props_idx,
            DataType::LargeBinary,
        ));
        let key_expr: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::Utf8(Some(field_name.to_string())),
            ));

        self.plan_binary_udf(
            "index",
            props_expr,
            key_expr,
            DataType::LargeBinary,
            DataType::Utf8,
        )
        .ok()
        .flatten()
    }

    /// Compile property access on a struct column (e.g. `x.a` where `x` is Struct).
    fn compile_property_access(
        &self,
        base: &Expr,
        prop: &str,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if let Expr::Variable(var_name) = base {
            // 1. Try struct field access (e.g. `x.a` where `x` is a Struct column)
            if let Some(expr) = self.try_compile_struct_field(var_name, prop, input_schema) {
                return Ok(expr);
            }
            // 2. Try flat column "{var}.{prop}" (for pattern comprehension inner schemas)
            let flat_col = format!("{}.{}", var_name, prop);
            if let Ok(col_idx) = input_schema.index_of(&flat_col) {
                return Ok(Arc::new(
                    datafusion::physical_expr::expressions::Column::new(&flat_col, col_idx),
                ));
            }
        }
        self.compile_standard(
            &Expr::Property(Box::new(base.clone()), prop.to_string()),
            input_schema,
        )
    }

    /// Evaluate a pattern comprehension as `MATCH <pattern> WHERE <w> RETURN <map>`,
    /// run per outer row.
    ///
    /// Used when the pattern has no node bound in the outer scope. The synthesized
    /// query is the comprehension's own semantics written out longhand, so the
    /// existing correlated-subquery machinery — `rewrite_query_correlated`,
    /// `plan_with_scope`, `execute_subplan_with_outer_vars` — evaluates it without
    /// needing to understand pattern comprehensions at all.
    fn compile_pattern_comprehension_as_subquery(
        &self,
        pattern: &uni_cypher::ast::Pattern,
        where_clause: Option<&Expr>,
        map_expr: &Expr,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use uni_cypher::ast::{Clause, MatchClause, Query, ReturnClause, ReturnItem, Statement};

        let err = |dep: &str| anyhow!("Pattern comprehension fallback requires {}", dep);

        let query = Query::Single(Statement {
            clauses: vec![
                Clause::Match(MatchClause {
                    optional: false,
                    pattern: pattern.clone(),
                    where_clause: where_clause.cloned(),
                    for_update: false,
                }),
                Clause::Return(ReturnClause {
                    distinct: false,
                    items: vec![ReturnItem::Expr {
                        expr: map_expr.clone(),
                        alias: Some("__pc_item".to_string()),
                        source_text: None,
                    }],
                    order_by: None,
                    skip: None,
                    limit: None,
                }),
            ],
        });

        let mut pattern_vars: HashSet<String> = HashSet::new();
        for path in &pattern.paths {
            if let Some(v) = &path.variable {
                pattern_vars.insert(v.clone());
            }
            for elem in &path.elements {
                match elem {
                    uni_cypher::ast::PatternElement::Node(n) => {
                        if let Some(v) = &n.variable {
                            pattern_vars.insert(v.clone());
                        }
                    }
                    uni_cypher::ast::PatternElement::Relationship(r) => {
                        if let Some(v) = &r.variable {
                            pattern_vars.insert(v.clone());
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(Arc::new(PatternComprehensionSubqueryExpr {
            query,
            pattern_vars,
            graph_ctx: self
                .graph_ctx
                .clone()
                .ok_or_else(|| err("GraphExecutionContext"))?,
            session_ctx: self
                .session_ctx
                .clone()
                .ok_or_else(|| err("SessionContext"))?,
            storage: self.storage.clone().ok_or_else(|| err("StorageManager"))?,
            uni_schema: self.uni_schema.clone().ok_or_else(|| err("UniSchema"))?,
            params: self.params.clone(),
            outer_entity_vars: self.outer_entity_vars.clone(),
        }))
    }

    /// Compile bracket access on a struct column (e.g. `x['a']` where `x` is Struct).
    fn compile_array_index(
        &self,
        array: &Expr,
        index: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if let Expr::Variable(var_name) = array
            && let Expr::Literal(CypherLiteral::String(prop)) = index
            && let Some(expr) = self.try_compile_struct_field(var_name, prop, input_schema)
        {
            return Ok(expr);
        }
        // Indexing into a raw-`Bytes` list literal (e.g. `[b.data][0]`): route through
        // the recursive custom-args path (UDF `index`) so the inner `make_array` child
        // is marked and the extracted element decodes as raw `Bytes`.
        if raw_bytes_marker::is_markable_list(array, input_schema) {
            return self.compile_function_with_custom_args(
                "index",
                &[array.clone(), index.clone()],
                false,
                input_schema,
            );
        }
        self.compile_standard(
            &Expr::ArrayIndex {
                array: Box::new(array.clone()),
                index: Box::new(index.clone()),
            },
            input_schema,
        )
    }

    /// Compile an `EXISTS { … }` subquery expression.
    fn compile_exists(&self, query: &Query) -> Result<Arc<dyn PhysicalExpr>> {
        self.compile_scalar_subquery(query, SubqueryResult::Exists)
    }

    /// Compile `EXISTS { … }` / `COUNT { … }` / `COLLECT { … }`.
    ///
    /// All three are per-row scalar subqueries over the same body and differ
    /// only in what they report, so they share one operator. `COUNT` and
    /// `COLLECT` used to be classified as *aggregates* by the AST, which routed
    /// them into the planner's aggregate list where the physical planner
    /// expects an `Expr::FunctionCall`; every such query died with `Expected
    /// aggregate function, got: CountSubquery(…)`. `EXISTS` was never
    /// classified that way, which is the only reason it worked.
    fn compile_scalar_subquery(
        &self,
        query: &Query,
        mode: SubqueryResult,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let form = match mode {
            SubqueryResult::Exists => "EXISTS",
            SubqueryResult::Count => "COUNT",
            SubqueryResult::Collect => "COLLECT",
        };
        // A subquery in an expression position cannot write.
        if has_mutation_clause(query) {
            return Err(anyhow!(
                "SyntaxError: InvalidClauseComposition - {form} subquery cannot contain updating clauses"
            ));
        }

        let err = |dep: &str| anyhow!("{form} requires {}", dep);

        let graph_ctx = self
            .graph_ctx
            .clone()
            .ok_or_else(|| err("GraphExecutionContext"))?;
        let uni_schema = self.uni_schema.clone().ok_or_else(|| err("UniSchema"))?;
        let session_ctx = self
            .session_ctx
            .clone()
            .ok_or_else(|| err("SessionContext"))?;
        let storage = self.storage.clone().ok_or_else(|| err("StorageManager"))?;

        Ok(Arc::new(ExistsExecExpr::new(
            query.clone(),
            mode,
            graph_ctx,
            session_ctx,
            storage,
            uni_schema,
            self.params.clone(),
            self.outer_entity_vars.clone(),
        )))
    }

    /// Compile a pattern-predicate EXISTS into a vectorized `PatternExistsExecExpr`.
    ///
    /// Returns `Err` for unsupported patterns (triggers fallback to `ExistsExecExpr`).
    fn compile_pattern_exists(
        &self,
        query: &Query,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let pattern = extract_pattern_from_exists_query(query)?;

        let graph_ctx = self
            .graph_ctx
            .as_ref()
            .ok_or_else(|| anyhow!("Pattern exists requires GraphExecutionContext"))?;
        let uni_schema = self
            .uni_schema
            .as_ref()
            .ok_or_else(|| anyhow!("Pattern exists requires UniSchema"))?;

        let (anchor_col, steps) = analyze_pattern(&pattern, input_schema, uni_schema)?;

        // Detect bound and correlated targets.
        // - Bound: target variable has `._vid` column in input schema → check specific VID.
        // - Correlated: target has a named variable NOT in the schema (from an outer EXISTS
        //   scope, passed as a parameter). We can't resolve it → bail out.
        let mut bound_target_columns: Vec<Option<String>> = Vec::with_capacity(steps.len());
        for step in &steps {
            if let Some(var) = &step.target_variable {
                let vid_col = format!("{}._vid", var);
                // Bound when the target is materialized in the input: either a flat
                // `{var}._vid` column, or the whole-node column `{var}` (a struct or
                // CypherValue blob, e.g. from `OPTIONAL MATCH (m)` in schemaless
                // mode). The exec resolves `{var}._vid` by stripping the suffix and
                // extracting `_vid` from that node column, and treats a null node as
                // a NULL bound target (pattern is NULL, not "any neighbor").
                if input_schema.column_with_name(&vid_col).is_some()
                    || input_schema.column_with_name(var).is_some()
                {
                    bound_target_columns.push(Some(vid_col));
                } else if self.outer_entity_vars.contains(var) {
                    // Named variable from outer scope — correlated, can't vectorize.
                    anyhow::bail!(
                        "Pattern target '{}' is a correlated reference from an outer scope",
                        var
                    );
                } else {
                    // Fresh binding introduced by the pattern — not correlated.
                    bound_target_columns.push(None);
                }
            } else {
                bound_target_columns.push(None);
            }
        }

        let property_preds = extract_target_property_predicates(&pattern, &steps, uni_schema)?;

        Ok(Arc::new(PatternExistsExecExpr::new(
            graph_ctx.clone(),
            anchor_col,
            steps,
            Arc::new(input_schema.clone()),
            property_preds,
            bound_target_columns,
            self.params.clone(),
        )))
    }

    /// Compile a `similar_to(sources, queries [, options])` call into a `SimilarToExecExpr`.
    fn compile_similar_to(
        &self,
        args: &[Expr],
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if args.len() < 2 || args.len() > 3 {
            return Err(SimilarToError::InvalidArity { count: args.len() }.into());
        }

        let graph_ctx = self
            .graph_ctx
            .clone()
            .ok_or(SimilarToError::NoGraphContext)?;

        // Extract variable name and property names from the source expression.
        let source_variable = extract_source_variable(&args[0]);
        let source_property_names = extract_property_names(&args[0]);

        // Normalize sources and queries: if args[0] is a List, split into
        // individual source/query pairs for multi-source scoring.
        let (source_exprs, query_exprs) = normalize_similar_to_args(&args[0], &args[1]);

        // Compile each source and query expression
        let source_children: Vec<Arc<dyn PhysicalExpr>> = source_exprs
            .iter()
            .map(|e| self.compile(e, input_schema))
            .collect::<Result<Vec<_>>>()?;
        let query_children: Vec<Arc<dyn PhysicalExpr>> = query_exprs
            .iter()
            .map(|e| self.compile(e, input_schema))
            .collect::<Result<Vec<_>>>()?;

        // Compile options if present
        let options_child = if args.len() == 3 {
            Some(self.compile(&args[2], input_schema)?)
        } else {
            None
        };

        // Resolve the source variable's label so metrics are looked up per
        // `(label, property)` rather than by property name alone (which would
        // let the first-declared index win across labels sharing a property).
        let source_label = source_variable.as_ref().and_then(|var| {
            self.translation_ctx
                .and_then(|ctx| ctx.variable_labels.get(var))
                .map(String::as_str)
        });

        // Resolve per-source distance metrics from schema at compile time.
        let source_metrics: Vec<Option<DistanceMetric>> = source_property_names
            .iter()
            .map(|prop_name| {
                prop_name.as_ref().and_then(|prop| {
                    self.uni_schema
                        .as_ref()
                        .and_then(|schema| resolve_metric_for_property(schema, source_label, prop))
                })
            })
            .collect();

        Ok(Arc::new(SimilarToExecExpr::new(
            source_children,
            query_children,
            options_child,
            graph_ctx,
            source_variable,
            source_property_names,
            source_metrics,
        )))
    }

    /// Does the comprehension body need a `{var}._vid` column materialized from
    /// its CypherValue loop variable?
    ///
    /// A nested pattern needs an anchor, and the anchor is found by looking for
    /// `{var}._vid` in the input schema. The loop variable of a comprehension is
    /// a single CypherValue column, so that column has to be derived.
    ///
    /// A pattern *predicate* needs this exactly as much as a pattern
    /// *comprehension* does, and only the latter was checked: with
    /// `[q IN ps WHERE (q)-[:KNOWS]-(:P)]` no `q._vid` was produced, the predicate
    /// could not bind its anchor, and it **failed open** — every element passed
    /// the filter. A wrong count, with no error.
    ///
    /// The pattern-predicate case is answered conservatively rather than by
    /// walking the subquery for a reference to the variable: an unnecessary vid
    /// column is one nullable column nothing reads, while a missing one is a
    /// silent wrong answer.
    fn needs_vid_extraction_for_variable(
        variable: &str,
        map_expr: &Expr,
        where_clause: Option<&Expr>,
    ) -> bool {
        fn expr_has_pattern_comp_referencing(expr: &Expr, var: &str) -> bool {
            match expr {
                Expr::PatternComprehension { pattern, .. } => {
                    // Check if pattern uses the variable
                    pattern.paths.iter().any(|path| {
                        path.elements.iter().any(|elem| match elem {
                            uni_cypher::ast::PatternElement::Node(n) => {
                                n.variable.as_deref() == Some(var)
                            }
                            uni_cypher::ast::PatternElement::Relationship(r) => {
                                r.variable.as_deref() == Some(var)
                            }
                            _ => false,
                        })
                    })
                }
                Expr::FunctionCall { args, .. } => args
                    .iter()
                    .any(|a| expr_has_pattern_comp_referencing(a, var)),
                Expr::BinaryOp { left, right, .. } => {
                    expr_has_pattern_comp_referencing(left, var)
                        || expr_has_pattern_comp_referencing(right, var)
                }
                Expr::UnaryOp { expr: e, .. } | Expr::Property(e, _) => {
                    expr_has_pattern_comp_referencing(e, var)
                }
                Expr::List(items) => items
                    .iter()
                    .any(|i| expr_has_pattern_comp_referencing(i, var)),
                Expr::ListComprehension {
                    list,
                    map_expr,
                    where_clause,
                    ..
                } => {
                    expr_has_pattern_comp_referencing(list, var)
                        || expr_has_pattern_comp_referencing(map_expr, var)
                        || where_clause
                            .as_ref()
                            .is_some_and(|w| expr_has_pattern_comp_referencing(w, var))
                }
                // A bare pattern predicate. Its pattern lives inside a whole
                // subquery rather than beside it, so this does not try to prove
                // the variable is referenced — see the note above.
                Expr::Exists {
                    from_pattern_predicate: true,
                    ..
                } => true,
                _ => false,
            }
        }

        expr_has_pattern_comp_referencing(map_expr, variable)
            || where_clause.is_some_and(|w| expr_has_pattern_comp_referencing(w, variable))
    }

    /// Check if an expression tree contains nodes that require custom compilation.
    pub fn contains_custom_expr(expr: &Expr) -> bool {
        match expr {
            Expr::ListComprehension { .. } => true,
            Expr::Quantifier { .. } => true,
            Expr::Reduce { .. } => true,
            Expr::PatternComprehension { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_custom_expr(left) || Self::contains_custom_expr(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_custom_expr(expr),
            Expr::FunctionCall { name, args, .. } => {
                name.eq_ignore_ascii_case("similar_to")
                    || args.iter().any(Self::contains_custom_expr)
            }
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                when_then
                    .iter()
                    .any(|(w, t)| Self::contains_custom_expr(w) || Self::contains_custom_expr(t))
                    || else_expr.as_deref().is_some_and(Self::contains_custom_expr)
            }
            Expr::List(items) => items.iter().any(Self::contains_custom_expr),
            Expr::Map(entries) => entries.iter().any(|(_, v)| Self::contains_custom_expr(v)),
            Expr::IsNull(e) | Expr::IsNotNull(e) => Self::contains_custom_expr(e),
            Expr::In { expr: l, list: r } => {
                Self::contains_custom_expr(l) || Self::contains_custom_expr(r)
            }
            Expr::Exists { .. } => true,
            // Same reason as EXISTS: a per-row subquery has no DataFusion
            // translation and must be compiled as a custom physical expression.
            Expr::CountSubquery(_) | Expr::CollectSubquery(_) => true,
            Expr::LabelCheck { expr, .. } => Self::contains_custom_expr(expr),
            _ => false,
        }
    }

    /// Check if an expression statically produces a list value.
    /// Used to route `Add` operations to `_cypher_list_concat` instead of arithmetic.
    fn is_list_producing(expr: &Expr) -> bool {
        match expr {
            Expr::List(_) => true,
            Expr::ListComprehension { .. } => true,
            Expr::ArraySlice { .. } => true,
            // Add with a list-producing child produces a list
            Expr::BinaryOp {
                left,
                op: BinaryOp::Add,
                right,
            } => Self::is_list_producing(left) || Self::is_list_producing(right),
            Expr::FunctionCall { name, .. } => {
                // Functions known to return lists
                matches!(
                    name.as_str(),
                    "range"
                        | "tail"
                        | "reverse"
                        | "collect"
                        | "keys"
                        | "labels"
                        | "nodes"
                        | "relationships"
                )
            }
            _ => false,
        }
    }

    fn compile_standard(
        &self,
        expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // Pre-resolve Property(Variable(v), p) → Variable("v.p") when the flat
        // column "v.p" exists in the input schema.  This prevents DataFusion's
        // standard path from compiling it as ArrayIndex(column(v), "p"), which
        // fails when the column stores a VID integer rather than a Map/Struct.
        let resolved = Self::resolve_flat_column_properties(expr, input_schema);
        let df_expr = cypher_expr_to_df(&resolved, self.translation_ctx)?;
        let resolved_expr = self.resolve_udfs(df_expr)?;

        let df_schema = datafusion::common::DFSchema::try_from(input_schema.clone())?;

        // Apply type coercion to resolve type mismatches
        let coerced_expr = crate::query::df_expr::apply_type_coercion(&resolved_expr, &df_schema)?;

        // Re-resolve UDFs after coercion (coercion may introduce new dummy UDF calls)
        let coerced_expr = self.resolve_udfs(coerced_expr)?;

        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        planner
            .create_physical_expr(&coerced_expr, &df_schema, self.state)
            .map_err(|e| anyhow!("DataFusion planning failed: {}", e))
    }

    /// Recursively resolve `Property(Variable(v), p)` → `Variable("v.p")` when
    /// the flat column `"v.p"` exists in `schema`.  This ensures that property
    /// access on graph-scan variables is compiled as a direct column reference
    /// rather than an ArrayIndex UDF call, which would fail when the variable
    /// column holds a VID integer instead of a Map/Node struct.
    fn resolve_flat_column_properties(expr: &Expr, schema: &Schema) -> Expr {
        match expr {
            Expr::Property(base, prop) => {
                if let Expr::Variable(var) = base.as_ref() {
                    let flat_col = format!("{}.{}", var, prop);
                    if schema.index_of(&flat_col).is_ok() {
                        return Expr::Variable(flat_col);
                    }
                }
                // Recurse into the base expression
                Expr::Property(
                    Box::new(Self::resolve_flat_column_properties(base, schema)),
                    prop.clone(),
                )
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(Self::resolve_flat_column_properties(left, schema)),
                op: *op,
                right: Box::new(Self::resolve_flat_column_properties(right, schema)),
            },
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => Expr::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|a| Self::resolve_flat_column_properties(a, schema))
                    .collect(),
                distinct: *distinct,
                window_spec: window_spec.clone(),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(Self::resolve_flat_column_properties(inner, schema)),
            },
            Expr::List(items) => Expr::List(
                items
                    .iter()
                    .map(|i| Self::resolve_flat_column_properties(i, schema))
                    .collect(),
            ),
            // For all other expression types, return as-is (literals, variables, etc.)
            other => other.clone(),
        }
    }

    /// Resolve UDFs in DataFusion expression using the session state registry.
    ///
    /// Uses `TreeNode::transform_up` to traverse the entire expression tree,
    /// ensuring UDFs inside Cast, Case, InList, Between, etc. are all resolved.
    fn resolve_udfs(
        &self,
        expr: datafusion::logical_expr::Expr,
    ) -> Result<datafusion::logical_expr::Expr> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use datafusion::logical_expr::Expr as DfExpr;

        let result = expr
            .transform_up(|node| {
                if let DfExpr::ScalarFunction(ref func) = node {
                    let udf_name = func.func.name();
                    if let Some(registered_udf) = self.state.scalar_functions().get(udf_name) {
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
            .map_err(|e| anyhow!("Failed to resolve UDFs: {}", e))?;
        Ok(result.data)
    }

    fn compile_list_comprehension(
        &self,
        variable: &str,
        list: &Expr,
        where_clause: Option<&Expr>,
        map_expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let input_list_phy = self.compile(list, input_schema)?;

        // Resolve input list type
        let list_data_type = input_list_phy.data_type(input_schema)?;
        let inner_data_type = resolve_list_element_type(
            &list_data_type,
            DataType::LargeBinary,
            "List comprehension",
        )?;

        // Create inner schema with loop variable (shadow outer variable if same name)
        let mut fields = input_schema.fields().to_vec();
        let loop_var_field = Arc::new(Field::new(variable, inner_data_type.clone(), true));

        if let Some(pos) = fields.iter().position(|f| f.name() == variable) {
            fields[pos] = loop_var_field;
        } else {
            fields.push(loop_var_field);
        }

        // Check if we need VID extraction for nested pattern comprehensions
        let needs_vid_extraction =
            Self::needs_vid_extraction_for_variable(variable, map_expr, where_clause);
        if needs_vid_extraction && inner_data_type == DataType::LargeBinary {
            // Add a {variable}._vid field for VID extraction
            let vid_field = Arc::new(Field::new(
                format!("{}._vid", variable),
                DataType::UInt64,
                true,
            ));
            fields.push(vid_field);
        }

        let inner_schema = Arc::new(Schema::new(fields));

        // Compile inner expressions with scoped translation context
        let mut scoped_ctx = None;
        let inner_compiler = self.scoped_compiler(&[variable], &mut scoped_ctx);

        let predicate_phy = if let Some(pred) = where_clause {
            Some(inner_compiler.compile(pred, &inner_schema)?)
        } else {
            None
        };

        let map_phy = inner_compiler.compile(map_expr, &inner_schema)?;
        let output_item_type = map_phy.data_type(&inner_schema)?;

        Ok(Arc::new(ListComprehensionExecExpr::new(
            input_list_phy,
            map_phy,
            predicate_phy,
            variable.to_string(),
            Arc::new(input_schema.clone()),
            output_item_type,
            needs_vid_extraction,
        )))
    }

    fn compile_reduce(
        &self,
        accumulator: &str,
        initial: &Expr,
        variable: &str,
        list: &Expr,
        reduce_expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let list_phy = self.compile(list, input_schema)?;

        let initial_phy = self.compile(initial, input_schema)?;
        let acc_type = initial_phy.data_type(input_schema)?;

        let list_data_type = list_phy.data_type(input_schema)?;
        // For LargeBinary (CypherValue arrays), use the accumulator type as element type so the
        // reduce body expression compiles correctly (e.g. acc + x where both are Int64).
        let inner_data_type =
            resolve_list_element_type(&list_data_type, acc_type.clone(), "Reduce")?;

        // Create inner schema with accumulator and loop variable (shadow outer variables if same names)
        let mut fields = input_schema.fields().to_vec();

        let acc_field = Arc::new(Field::new(accumulator, acc_type, true));
        if let Some(pos) = fields.iter().position(|f| f.name() == accumulator) {
            fields[pos] = acc_field;
        } else {
            fields.push(acc_field);
        }

        let var_field = Arc::new(Field::new(variable, inner_data_type, true));
        if let Some(pos) = fields.iter().position(|f| f.name() == variable) {
            fields[pos] = var_field;
        } else {
            fields.push(var_field);
        }

        let inner_schema = Arc::new(Schema::new(fields));

        // Compile reduce expression with scoped translation context
        let mut scoped_ctx = None;
        let reduce_compiler = self.scoped_compiler(&[accumulator, variable], &mut scoped_ctx);

        let reduce_phy = reduce_compiler.compile(reduce_expr, &inner_schema)?;
        let output_type = reduce_phy.data_type(&inner_schema)?;

        Ok(Arc::new(ReduceExecExpr::new(
            accumulator.to_string(),
            initial_phy,
            variable.to_string(),
            list_phy,
            reduce_phy,
            Arc::new(input_schema.clone()),
            output_type,
        )))
    }

    fn compile_quantifier(
        &self,
        quantifier: &uni_cypher::ast::Quantifier,
        variable: &str,
        list: &Expr,
        predicate: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let input_list_phy = self.compile(list, input_schema)?;

        // Resolve element type from list type
        let list_data_type = input_list_phy.data_type(input_schema)?;
        let inner_data_type =
            resolve_list_element_type(&list_data_type, DataType::LargeBinary, "Quantifier")?;

        // Create inner schema with loop variable
        // If a field with the same name exists in the outer schema, replace it (shadow it)
        // to ensure the loop variable takes precedence.
        let mut fields = input_schema.fields().to_vec();
        let loop_var_field = Arc::new(Field::new(variable, inner_data_type, true));

        // Find and replace existing field with same name, or append if not found
        if let Some(pos) = fields.iter().position(|f| f.name() == variable) {
            fields[pos] = loop_var_field;
        } else {
            fields.push(loop_var_field);
        }

        let inner_schema = Arc::new(Schema::new(fields));

        // Compile predicate with a scoped translation context that removes the loop variable
        // from variable_kinds, so property access on the loop variable doesn't incorrectly
        // generate flat columns (like "x.name") that don't exist in the inner schema.
        let mut scoped_ctx = None;
        let pred_compiler = self.scoped_compiler(&[variable], &mut scoped_ctx);

        let mut predicate_phy = pred_compiler.compile(predicate, &inner_schema)?;

        // Wrap CypherValue predicates with _cv_to_bool for proper boolean evaluation
        if let Ok(DataType::LargeBinary) = predicate_phy.data_type(&inner_schema) {
            predicate_phy = self.wrap_with_cv_to_bool(predicate_phy)?;
        }

        let qt = match quantifier {
            uni_cypher::ast::Quantifier::All => QuantifierType::All,
            uni_cypher::ast::Quantifier::Any => QuantifierType::Any,
            uni_cypher::ast::Quantifier::Single => QuantifierType::Single,
            uni_cypher::ast::Quantifier::None => QuantifierType::None,
        };

        Ok(Arc::new(QuantifierExecExpr::new(
            input_list_phy,
            predicate_phy,
            variable.to_string(),
            Arc::new(input_schema.clone()),
            qt,
        )))
    }

    fn compile_pattern_comprehension(
        &self,
        path_variable: &Option<String>,
        pattern: &uni_cypher::ast::Pattern,
        where_clause: Option<&Expr>,
        map_expr: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let err = |dep: &str| anyhow!("Pattern comprehension requires {}", dep);

        let graph_ctx = self
            .graph_ctx
            .as_ref()
            .ok_or_else(|| err("GraphExecutionContext"))?;
        let uni_schema = self.uni_schema.as_ref().ok_or_else(|| err("UniSchema"))?;

        // 1. Analyze pattern to get anchor column and traversal steps.
        //
        // Failure here means no pattern node is bound in the outer scope, so
        // there is nothing to expand CSR adjacency *from*. That is not an invalid
        // query — `[(a:P)-[:KNOWS]->(b:P) | a.name]` is legal openCypher — it is
        // only outside what the vectorized operator can do. Fall back to
        // evaluating the comprehension as a correlated subquery, mirroring what
        // the `Expr::Exists` arm above already does when `compile_pattern_exists`
        // cannot vectorize a pattern predicate.
        let (anchor_col, steps) = match analyze_pattern(pattern, input_schema, uni_schema) {
            Ok(analysis) => analysis,
            Err(e) => {
                log::debug!(
                    "Pattern comprehension vectorization failed, falling back to subquery: {e}"
                );
                return self.compile_pattern_comprehension_as_subquery(
                    pattern,
                    where_clause,
                    map_expr,
                );
            }
        };

        // 2. Collect needed properties from where_clause and map_expr
        let (vertex_props, edge_props) = collect_inner_properties(where_clause, map_expr, &steps);

        // 3. Build inner schema
        let inner_schema = build_inner_schema(
            input_schema,
            &steps,
            &vertex_props,
            &edge_props,
            path_variable.as_deref(),
        );

        // 4. Compile predicate and map_expr against inner schema.
        //
        // The pattern's own variables are entities of the inner scope, and the
        // inner schema carries them as flat `x._vid` / `x.name` columns. The
        // inherited context knows nothing about them, so they are registered
        // before compiling — otherwise a property access on one compiles as an
        // index into a bare `x` that the inner schema does not have (#189).
        let entity_vars: Vec<(String, VariableKind)> = steps
            .iter()
            .flat_map(|s| {
                s.target_variable
                    .clone()
                    .map(|v| (v, VariableKind::Node))
                    .into_iter()
                    .chain(s.edge_variable.clone().map(|v| (v, VariableKind::Edge)))
            })
            .collect();
        let mut inner_ctx_slot = None;
        let inner_compiler = self.entity_scoped_compiler(&entity_vars, &mut inner_ctx_slot);

        let pred_phy = where_clause
            .map(|p| inner_compiler.compile(p, &inner_schema))
            .transpose()?;
        let map_phy = inner_compiler.compile(map_expr, &inner_schema)?;
        let output_type = map_phy.data_type(&inner_schema)?;

        // 5. Return expression
        Ok(Arc::new(PatternComprehensionExecExpr::new(
            graph_ctx.clone(),
            anchor_col,
            steps,
            path_variable.clone(),
            pred_phy,
            map_phy,
            Arc::new(input_schema.clone()),
            Arc::new(inner_schema),
            output_type,
            vertex_props,
            edge_props,
        )))
    }

    /// Compile a function call whose arguments contain custom expressions.
    ///
    /// Recursively compiles each argument via `self.compile()`, then looks up
    /// the corresponding UDF in the session registry and builds the physical
    /// expression.
    fn compile_function_with_custom_args(
        &self,
        name: &str,
        args: &[Expr],
        _distinct: bool,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // 1. Recursively compile each argument
        let compiled_args: Vec<Arc<dyn PhysicalExpr>> = args
            .iter()
            .map(|arg| self.compile(arg, input_schema))
            .collect::<Result<Vec<_>>>()?;

        // 2. Resolve UDF name and look it up in the registry
        let udf_name = Self::cypher_fn_to_udf(name);
        let udf = self
            .state
            .scalar_functions()
            .get(udf_name.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "UDF '{}' not found in registry for function '{}'",
                    udf_name,
                    name
                )
            })?;

        // 3. Build operand type list and dummy columns from compiled args
        let placeholders: &[&str] = &["__arg0__", "__arg1__", "__arg2__", "__argN__"];
        let operand_types: Vec<(&str, DataType)> = compiled_args
            .iter()
            .enumerate()
            .map(|(i, arg)| {
                let dt = arg.data_type(input_schema).unwrap_or(DataType::LargeBinary);
                let placeholder = placeholders[i.min(3)];
                (placeholder, dt)
            })
            .collect();

        let dummy_cols: Vec<datafusion::logical_expr::Expr> = operand_types
            .iter()
            .map(|(name, _)| {
                datafusion::logical_expr::Expr::Column(datafusion::common::Column::new(
                    None::<String>,
                    *name,
                ))
            })
            .collect();

        let udf_expr = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction {
                func: udf.clone(),
                args: dummy_cols,
            },
        );

        // 4. Plan and rebind
        self.plan_udf_physical_expr(
            &udf_expr,
            &operand_types,
            compiled_args,
            &format!("function {}", name),
        )
    }

    /// Map a Cypher function name to the registered UDF name.
    ///
    /// Mirrors the mapping in `translate_function_call` from `df_expr.rs`.
    /// The registered UDF names are always lowercase.
    fn cypher_fn_to_udf(name: &str) -> String {
        match name.to_uppercase().as_str() {
            "SIZE" | "LENGTH" => "_cypher_size".to_string(),
            "REVERSE" => "_cypher_reverse".to_string(),
            "TOSTRING" => "tostring".to_string(),
            "TOBOOLEAN" | "TOBOOL" | "TOBOOLEANORNULL" => "toboolean".to_string(),
            "TOINTEGER" | "TOINT" | "TOINTEGERORNULL" => "tointeger".to_string(),
            "TOFLOAT" | "TOFLOATORNULL" => "tofloat".to_string(),
            "HEAD" => "head".to_string(),
            "LAST" => "last".to_string(),
            "TAIL" => "_cypher_tail".to_string(),
            "KEYS" => "keys".to_string(),
            "TYPE" => "type".to_string(),
            "PROPERTIES" => "properties".to_string(),
            "LABELS" => "labels".to_string(),
            "COALESCE" => "coalesce".to_string(),
            "ID" => "id".to_string(),
            // Fallback: lowercase the name (matches dummy_udf_expr behavior)
            _ => name.to_lowercase(),
        }
    }

    /// Compile a CASE expression with custom sub-expressions in branches.
    ///
    /// Recursively compiles the operand, each when/then pair, and the else
    /// branch, then builds a `CaseExpr` physical expression.
    ///
    /// Applies two fixes for TCK compliance:
    /// 1. Wraps CypherValue WHEN conditions with `_cv_to_bool` for proper boolean evaluation
    /// 2. Unifies branch types when mixing LargeList and LargeBinary to avoid cast errors
    fn compile_case(
        &self,
        operand: &Option<Box<Expr>>,
        when_then: &[(Expr, Expr)],
        else_expr: &Option<Box<Expr>>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let operand_phy = operand
            .as_deref()
            .map(|e| self.compile(e, input_schema))
            .transpose()?;

        let mut when_then_phy: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = when_then
            .iter()
            .map(|(w, t)| {
                let w_phy = self.compile(w, input_schema)?;
                let t_phy = self.compile(t, input_schema)?;
                Ok((w_phy, t_phy))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut else_phy = else_expr
            .as_deref()
            .map(|e| self.compile(e, input_schema))
            .transpose()?;

        // Wrap CypherValue WHEN conditions with _cv_to_bool for proper boolean evaluation
        for (w_phy, _) in &mut when_then_phy {
            if matches!(w_phy.data_type(input_schema), Ok(DataType::LargeBinary)) {
                *w_phy = self.wrap_with_cv_to_bool(w_phy.clone())?;
            }
        }

        // Unify branch types when mixing LargeList and LargeBinary
        let branch_types: Vec<DataType> = when_then_phy
            .iter()
            .map(|(_, t)| t.data_type(input_schema))
            .chain(else_phy.iter().map(|e| e.data_type(input_schema)))
            .filter_map(Result::ok)
            .collect();

        let has_large_binary = branch_types.contains(&DataType::LargeBinary);
        let has_list = branch_types
            .iter()
            .any(|dt| matches!(dt, DataType::List(_) | DataType::LargeList(_)));

        // If we have both, wrap List/LargeList branches with LargeListToCypherValueExpr
        if has_large_binary && has_list {
            for (_, t_phy) in &mut when_then_phy {
                if let Ok(dt) = t_phy.data_type(input_schema)
                    && matches!(dt, DataType::List(_) | DataType::LargeList(_))
                {
                    *t_phy = Arc::new(LargeListToCypherValueExpr::new(t_phy.clone()));
                }
            }
            if let Some(e_phy) = else_phy.take() {
                if let Ok(dt) = e_phy.data_type(input_schema)
                    && matches!(dt, DataType::List(_) | DataType::LargeList(_))
                {
                    else_phy = Some(Arc::new(LargeListToCypherValueExpr::new(e_phy)));
                } else {
                    else_phy = Some(e_phy);
                }
            }
        }

        let case_expr = datafusion::physical_expr::expressions::CaseExpr::try_new(
            operand_phy,
            when_then_phy,
            else_phy,
        )
        .map_err(|e| anyhow!("Failed to create CASE expression: {}", e))?;

        Ok(Arc::new(case_expr))
    }

    fn compile_binary_op(
        &self,
        op: &BinaryOp,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion::logical_expr::Operator;

        // String operators use custom physical expr for safe type handling
        let string_op = match op {
            BinaryOp::StartsWith => Some(StringOp::StartsWith),
            BinaryOp::EndsWith => Some(StringOp::EndsWith),
            BinaryOp::Contains => Some(StringOp::Contains),
            _ => None,
        };
        if let Some(sop) = string_op {
            return Ok(Arc::new(CypherStringMatchExpr::new(left, right, sop)));
        }

        let df_op = match op {
            BinaryOp::Add => Operator::Plus,
            BinaryOp::Sub => Operator::Minus,
            BinaryOp::Mul => Operator::Multiply,
            BinaryOp::Div => Operator::Divide,
            BinaryOp::Mod => Operator::Modulo,
            BinaryOp::Eq => Operator::Eq,
            BinaryOp::NotEq => Operator::NotEq,
            BinaryOp::Gt => Operator::Gt,
            BinaryOp::GtEq => Operator::GtEq,
            BinaryOp::Lt => Operator::Lt,
            BinaryOp::LtEq => Operator::LtEq,
            BinaryOp::And => Operator::And,
            BinaryOp::Or => Operator::Or,
            BinaryOp::Xor => {
                return Err(anyhow!(
                    "XOR not supported via binary helper, use bitwise_xor"
                ));
            }
            BinaryOp::Regex => Operator::RegexMatch,
            BinaryOp::ApproxEq => {
                return Err(anyhow!(
                    "ApproxEq (~=) not yet supported in physical compiler"
                ));
            }
            BinaryOp::Pow => return Err(anyhow!("POW not yet supported in physical compiler")),
            _ => return Err(anyhow!("Unsupported binary op in compiler: {:?}", op)),
        };

        // When either operand is LargeBinary (CypherValue), standard Arrow comparison
        // kernels can't handle the type mismatch. Route through Cypher comparison
        // UDFs which decode CypherValue to Value for comparison.
        let mut left = left;
        let mut right = right;
        let mut left_type = left.data_type(input_schema).ok();
        let mut right_type = right.data_type(input_schema).ok();

        // Type unification: if one side is LargeList and the other is LargeBinary,
        // convert LargeList to LargeBinary for consistent handling
        let left_is_list = matches!(
            left_type.as_ref(),
            Some(DataType::List(_) | DataType::LargeList(_))
        );
        let right_is_list = matches!(
            right_type.as_ref(),
            Some(DataType::List(_) | DataType::LargeList(_))
        );

        if left_is_list && is_cypher_value_type(right_type.as_ref()) {
            left = Arc::new(LargeListToCypherValueExpr::new(left));
            left_type = Some(DataType::LargeBinary);
        } else if right_is_list && is_cypher_value_type(left_type.as_ref()) {
            right = Arc::new(LargeListToCypherValueExpr::new(right));
            right_type = Some(DataType::LargeBinary);
        }

        let has_cv =
            is_cypher_value_type(left_type.as_ref()) || is_cypher_value_type(right_type.as_ref());

        if has_cv {
            if let Some(result) = self.compile_cv_comparison(
                df_op,
                left.clone(),
                right.clone(),
                &left_type,
                &right_type,
            )? {
                return Ok(result);
            }
            if let Some(result) = self.compile_cv_list_concat(
                left.clone(),
                right.clone(),
                &left_type,
                &right_type,
                df_op,
            )? {
                return Ok(result);
            }
            if let Some(result) = self.compile_cv_arithmetic(
                df_op,
                left.clone(),
                right.clone(),
                &left_type,
                &right_type,
            )? {
                return Ok(result);
            }
        }

        // Use DataFusion's binary physical expression creator which handles coercion
        binary(left, df_op, right, input_schema)
            .map_err(|e| anyhow!("Failed to create binary expression: {}", e))
    }

    /// Compile CypherValue comparison using Cypher UDFs.
    fn compile_cv_comparison(
        &self,
        df_op: datafusion::logical_expr::Operator,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        left_type: &Option<DataType>,
        right_type: &Option<DataType>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        use datafusion::logical_expr::Operator;

        let udf_name = match df_op {
            Operator::Eq => "_cypher_equal",
            Operator::NotEq => "_cypher_not_equal",
            Operator::Gt => "_cypher_gt",
            Operator::GtEq => "_cypher_gt_eq",
            Operator::Lt => "_cypher_lt",
            Operator::LtEq => "_cypher_lt_eq",
            _ => return Ok(None),
        };

        self.plan_binary_udf(
            udf_name,
            left,
            right,
            left_type.clone().unwrap_or(DataType::LargeBinary),
            right_type.clone().unwrap_or(DataType::LargeBinary),
        )
    }

    /// Compile CypherValue list concatenation.
    fn compile_cv_list_concat(
        &self,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        left_type: &Option<DataType>,
        right_type: &Option<DataType>,
        df_op: datafusion::logical_expr::Operator,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        use datafusion::logical_expr::Operator;

        if df_op != Operator::Plus {
            return Ok(None);
        }

        // List concat when at least one side is a list (CypherValue or Arrow List)
        let is_list = |t: &Option<DataType>| {
            t.as_ref()
                .is_some_and(|dt| matches!(dt, DataType::LargeBinary | DataType::List(_)))
        };

        if !is_list(left_type) && !is_list(right_type) {
            return Ok(None);
        }

        self.plan_binary_udf(
            "_cypher_list_concat",
            left,
            right,
            left_type.clone().unwrap_or(DataType::LargeBinary),
            right_type.clone().unwrap_or(DataType::LargeBinary),
        )
    }

    /// Compile CypherValue arithmetic.
    ///
    /// Routes arithmetic operations through CypherValue-aware UDFs when at least
    /// one operand is LargeBinary (CypherValue-encoded).
    fn compile_cv_arithmetic(
        &self,
        df_op: datafusion::logical_expr::Operator,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        left_type: &Option<DataType>,
        right_type: &Option<DataType>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        use datafusion::logical_expr::Operator;

        let udf_name = match df_op {
            Operator::Plus => "_cypher_add",
            Operator::Minus => "_cypher_sub",
            Operator::Multiply => "_cypher_mul",
            Operator::Divide => "_cypher_div",
            Operator::Modulo => "_cypher_mod",
            _ => return Ok(None),
        };

        self.plan_binary_udf(
            udf_name,
            left,
            right,
            left_type.clone().unwrap_or(DataType::LargeBinary),
            right_type.clone().unwrap_or(DataType::LargeBinary),
        )
    }

    /// Plan a UDF expression with dummy schema columns, then rebind to actual physical expressions.
    fn plan_udf_physical_expr(
        &self,
        udf_expr: &datafusion::logical_expr::Expr,
        operand_types: &[(&str, DataType)],
        children: Vec<Arc<dyn PhysicalExpr>>,
        error_context: &str,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let tmp_schema = Schema::new(
            operand_types
                .iter()
                .map(|(name, dt)| Arc::new(Field::new(*name, dt.clone(), true)))
                .collect::<Vec<_>>(),
        );
        let df_schema = datafusion::common::DFSchema::try_from(tmp_schema)?;
        let planner = datafusion::physical_planner::DefaultPhysicalPlanner::default();
        let udf_phy = planner
            .create_physical_expr(udf_expr, &df_schema, self.state)
            .map_err(|e| anyhow!("Failed to create {} expr: {}", error_context, e))?;
        udf_phy
            .with_new_children(children)
            .map_err(|e| anyhow!("Failed to rebind {} children: {}", error_context, e))
    }

    /// Wrap a LargeBinary (CypherValue) expression with `_cv_to_bool` conversion.
    ///
    /// Used when a CypherValue expression needs to be used as a boolean (e.g., in WHEN clauses).
    fn wrap_with_cv_to_bool(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let Some(udf) = self.state.scalar_functions().get("_cv_to_bool") else {
            return Err(anyhow!("_cv_to_bool UDF not found"));
        };

        let dummy_col = datafusion::logical_expr::Expr::Column(datafusion::common::Column::new(
            None::<String>,
            "__cv__",
        ));
        let udf_expr = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction {
                func: udf.clone(),
                args: vec![dummy_col],
            },
        );

        self.plan_udf_physical_expr(
            &udf_expr,
            &[("__cv__", DataType::LargeBinary)],
            vec![expr],
            "CypherValue to bool",
        )
    }

    /// Plan a binary UDF with the given name and operand types.
    fn plan_binary_udf(
        &self,
        udf_name: &str,
        left: Arc<dyn PhysicalExpr>,
        right: Arc<dyn PhysicalExpr>,
        left_type: DataType,
        right_type: DataType,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let Some(udf) = self.state.scalar_functions().get(udf_name) else {
            return Ok(None);
        };
        let udf_expr = datafusion::logical_expr::Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction {
                func: udf.clone(),
                args: vec![
                    datafusion::logical_expr::Expr::Column(datafusion::common::Column::new(
                        None::<String>,
                        "__left__",
                    )),
                    datafusion::logical_expr::Expr::Column(datafusion::common::Column::new(
                        None::<String>,
                        "__right__",
                    )),
                ],
            },
        );
        let result = self.plan_udf_physical_expr(
            &udf_expr,
            &[("__left__", left_type), ("__right__", right_type)],
            vec![left, right],
            udf_name,
        )?;
        Ok(Some(result))
    }

    fn compile_unary_op(
        &self,
        op: &UnaryOp,
        expr: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match op {
            UnaryOp::Not => datafusion::physical_expr::expressions::not(expr),
            UnaryOp::Neg => datafusion::physical_expr::expressions::negative(expr, input_schema),
        }
        .map_err(|e| anyhow!("Failed to create unary expression: {}", e))
    }
}

// ---------------------------------------------------------------------------
// similar_to() AST helpers
// ---------------------------------------------------------------------------

/// Extract the base variable name from a source expression.
///
/// For `d.embedding` → `"d"`, for `[d.embedding, d.content]` → `"d"`.
fn extract_source_variable(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Property(inner, _) => extract_source_variable(inner),
        Expr::Variable(name) => Some(name.clone()),
        Expr::List(items) => items.first().and_then(extract_source_variable),
        _ => None,
    }
}

/// Extract property names from source expressions.
///
/// For `d.embedding` → `["embedding"]`, for `[d.embedding, d.content]` → `["embedding", "content"]`.
fn extract_property_names(expr: &Expr) -> Vec<Option<String>> {
    match expr {
        Expr::Property(_, prop) => vec![Some(prop.clone())],
        Expr::List(items) => items
            .iter()
            .map(|item| {
                if let Expr::Property(_, prop) = item {
                    Some(prop.clone())
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![None],
    }
}

/// Normalize similar_to arguments for multi-source scoring.
///
/// If source arg is a `List`, returns the individual items as separate source/query pairs.
/// Otherwise returns the args as-is (single pair).
fn normalize_similar_to_args<'e>(
    sources: &'e Expr,
    queries: &'e Expr,
) -> (Vec<&'e Expr>, Vec<&'e Expr>) {
    match (sources, queries) {
        // Multi-source: [d.embedding, d.content] paired with [query_vec, query_text]
        (Expr::List(src_items), Expr::List(qry_items)) if src_items.len() == qry_items.len() => {
            (src_items.iter().collect(), qry_items.iter().collect())
        }
        // Multi-source with broadcast query: [d.embedding, d.content] paired with single query
        (Expr::List(src_items), _) if src_items.len() > 1 => {
            let queries_broadcast: Vec<&Expr> = vec![queries; src_items.len()];
            (src_items.iter().collect(), queries_broadcast)
        }
        // Single source
        _ => (vec![sources], vec![queries]),
    }
}

use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StringOp {
    StartsWith,
    EndsWith,
    Contains,
}

impl std::fmt::Display for StringOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StringOp::StartsWith => write!(f, "STARTS WITH"),
            StringOp::EndsWith => write!(f, "ENDS WITH"),
            StringOp::Contains => write!(f, "CONTAINS"),
        }
    }
}

#[derive(Debug, Eq)]
struct CypherStringMatchExpr {
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: StringOp,
}

impl PartialEq for CypherStringMatchExpr {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.left.eq(&other.left) && self.right.eq(&other.right)
    }
}

impl std::hash::Hash for CypherStringMatchExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        self.left.hash(state);
        self.right.hash(state);
    }
}

impl CypherStringMatchExpr {
    fn new(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>, op: StringOp) -> Self {
        Self { left, right, op }
    }
}

impl std::fmt::Display for CypherStringMatchExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl DisplayAs for CypherStringMatchExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PhysicalExpr for CypherStringMatchExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(arrow_schema::DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        use crate::query::df_udfs::invoke_cypher_string_op;
        use arrow_schema::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::logical_expr::ScalarFunctionArgs;

        let left_val = self.left.evaluate(batch)?;
        let right_val = self.right.evaluate(batch)?;

        let args = ScalarFunctionArgs {
            args: vec![left_val, right_val],
            number_rows: batch.num_rows(),
            return_field: Arc::new(Field::new("result", arrow_schema::DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
            arg_fields: vec![], // Not used by invoke_cypher_string_op
        };

        match self.op {
            StringOp::StartsWith => {
                invoke_cypher_string_op(&args, "starts_with", |s, p| s.starts_with(p))
            }
            StringOp::EndsWith => {
                invoke_cypher_string_op(&args, "ends_with", |s, p| s.ends_with(p))
            }
            StringOp::Contains => invoke_cypher_string_op(&args, "contains", |s, p| s.contains(p)),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CypherStringMatchExpr::new(
            children[0].clone(),
            children[1].clone(),
            self.op,
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PartialEq<dyn PhysicalExpr> for CypherStringMatchExpr {
    fn eq(&self, other: &dyn PhysicalExpr) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<CypherStringMatchExpr>() {
            self == other
        } else {
            false
        }
    }
}

/// Physical expression for extracting a field from a struct column.
///
/// Used when list comprehension iterates over a list of structs (maps)
/// and accesses a field, e.g., `[x IN [{a: 1}] | x.a]`.
#[derive(Debug, Eq)]
struct StructFieldAccessExpr {
    /// Expression producing the struct column.
    input: Arc<dyn PhysicalExpr>,
    /// Index of the field within the struct.
    field_idx: usize,
    /// Output data type of the extracted field.
    output_type: arrow_schema::DataType,
}

impl PartialEq for StructFieldAccessExpr {
    fn eq(&self, other: &Self) -> bool {
        self.field_idx == other.field_idx
            && self.input.eq(&other.input)
            && self.output_type == other.output_type
    }
}

impl std::hash::Hash for StructFieldAccessExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.field_idx.hash(state);
    }
}

impl StructFieldAccessExpr {
    fn new(
        input: Arc<dyn PhysicalExpr>,
        field_idx: usize,
        output_type: arrow_schema::DataType,
    ) -> Self {
        Self {
            input,
            field_idx,
            output_type,
        }
    }
}

impl std::fmt::Display for StructFieldAccessExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}]", self.input, self.field_idx)
    }
}

impl DisplayAs for StructFieldAccessExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PartialEq<dyn PhysicalExpr> for StructFieldAccessExpr {
    fn eq(&self, other: &dyn PhysicalExpr) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self.field_idx == other.field_idx && self.input.eq(&other.input)
        } else {
            false
        }
    }
}

impl PhysicalExpr for StructFieldAccessExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(self.output_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        use arrow_array::StructArray;

        let input_val = self.input.evaluate(batch)?;
        let array = input_val.into_array(batch.num_rows())?;

        let struct_array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "StructFieldAccessExpr: input is not a StructArray".to_string(),
                )
            })?;

        let field_col = struct_array.column(self.field_idx).clone();
        Ok(datafusion::physical_plan::ColumnarValue::Array(field_col))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(StructFieldAccessExpr::new(
            children[0].clone(),
            self.field_idx,
            self.output_type.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

// ---------------------------------------------------------------------------
// EXISTS subquery physical expression
// ---------------------------------------------------------------------------

/// Physical expression that evaluates an EXISTS subquery per row.
///
/// For each input row, plans and executes the subquery with the row's columns
/// injected as parameters. Returns `true` if the subquery produces any rows.
///
/// NOT EXISTS is handled by the caller wrapping this in a NOT expression.
/// Nested EXISTS works because `execute_subplan` creates a full planner that
/// handles nested EXISTS recursively.
/// What a per-row scalar subquery reports about its result set.
///
/// `EXISTS { … }`, `COUNT { … }` and `COLLECT { … }` differ only in this:
/// each plans the same body, runs it once per outer row, and then asks a
/// different question of the rows that come back. Sharing one operator is what
/// keeps them from drifting — correlated-variable extraction, the nested
/// runtime, and mutation rejection are subtle and were solved once for EXISTS.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SubqueryResult {
    /// Whether any row came back.
    Exists,
    /// How many rows came back.
    Count,
    /// The values of the single column the body returns.
    Collect,
}

struct ExistsExecExpr {
    query: Query,
    /// Which of the three subquery forms this is.
    mode: SubqueryResult,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<SessionContext>>,
    storage: Arc<StorageManager>,
    uni_schema: Arc<UniSchema>,
    params: HashMap<String, Value>,
    /// Entity variable names from outer scopes, threaded into nested subplans
    /// so the inner expression compiler can detect correlated references.
    outer_entity_vars: HashSet<String>,
}

impl std::fmt::Debug for ExistsExecExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExistsExecExpr").finish_non_exhaustive()
    }
}

impl ExistsExecExpr {
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors the subquery's dependencies"
    )]
    fn new(
        query: Query,
        mode: SubqueryResult,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        uni_schema: Arc<UniSchema>,
        params: HashMap<String, Value>,
        outer_entity_vars: HashSet<String>,
    ) -> Self {
        Self {
            query,
            mode,
            graph_ctx,
            session_ctx,
            storage,
            uni_schema,
            params,
            outer_entity_vars,
        }
    }
}

impl std::fmt::Display for ExistsExecExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EXISTS(<subquery>)")
    }
}

impl PartialEq<dyn PhysicalExpr> for ExistsExecExpr {
    fn eq(&self, _other: &dyn PhysicalExpr) -> bool {
        false
    }
}

impl PartialEq for ExistsExecExpr {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Eq for ExistsExecExpr {}

impl std::hash::Hash for ExistsExecExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        "ExistsExecExpr".hash(state);
    }
}

impl DisplayAs for ExistsExecExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PhysicalExpr for ExistsExecExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(match self.mode {
            SubqueryResult::Exists => DataType::Boolean,
            SubqueryResult::Count => DataType::Int64,
            // The same item type the pattern-comprehension fallback produces,
            // so a list from `COLLECT { … }` decodes exactly like one from
            // `[(a)-[:R]->(x) | x]`.
            SubqueryResult::Collect => DataType::LargeList(comprehension_item_field()),
        })
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        let num_rows = batch.num_rows();
        let mode = self.mode;
        // Row count and, in `Collect` mode, the body's single column. Gathered
        // rather than appended to a typed builder because the builder's type
        // depends on the mode.
        let mut per_row: Vec<(usize, Vec<Value>)> = Vec::with_capacity(num_rows);

        // 7.2: Extract entity variable names from batch schema.
        // Entity columns follow the pattern "varname._vid" (flattened) or are struct columns.
        // We pass ONLY entity base names (e.g., "p", "n", "m") as vars_in_scope so the
        // subquery planner treats them as bound (Imported) variables. The initial Project
        // then creates Parameter("n") AS "n" etc. — the traverse reads the VID from this
        // column via resolve_source_vid_col's bare-name fallback.
        //
        // We intentionally do NOT include raw column names (n._vid, n._labels, etc.) in
        // vars_in_scope to avoid duplicate column conflicts with traverse output columns.
        // Parameter expressions read directly from sub_params, not from plan columns.
        let schema = batch.schema();
        let mut entity_vars: HashSet<String> = HashSet::new();
        for field in schema.fields() {
            let name = field.name();
            if let Some(base) = name.strip_suffix("._vid") {
                entity_vars.insert(base.to_string());
            }
            if matches!(field.data_type(), DataType::Struct(_)) {
                entity_vars.insert(name.to_string());
            }
            // Also detect bare VID columns from parent EXISTS parameter projections.
            // In nested EXISTS, a parent level projects "n" as a bare Int64/UInt64 VID.
            // Simple identifier (no dots, no leading underscore) + integer type = VID.
            if !name.contains('.')
                && !name.starts_with('_')
                && matches!(field.data_type(), DataType::Int64 | DataType::UInt64)
            {
                entity_vars.insert(name.to_string());
            }
        }
        let vars_in_scope: Vec<String> = entity_vars.iter().cloned().collect();

        // 7.3: Rewrite correlated property accesses to parameter references.
        // e.g., `n.prop` where `n` is an outer entity → `$param("n.prop")`
        let rewritten_query = rewrite_query_correlated(&self.query, &entity_vars);

        // 7.4: Plan ONCE — the rewritten query is parameterized, same for all rows.
        let planner = QueryPlanner::new(self.uni_schema.clone());
        let logical_plan = match planner.plan_with_scope(rewritten_query, vars_in_scope) {
            Ok(plan) => plan,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "EXISTS subquery planning failed: {}",
                    e
                )));
            }
        };

        // Execute all rows on a dedicated thread with a single tokio runtime.
        // The runtime must be created and dropped on this thread (not in an async context).
        let graph_ctx = self.graph_ctx.clone();
        let session_ctx = self.session_ctx.clone();
        let storage = self.storage.clone();
        let uni_schema = self.uni_schema.clone();
        let base_params = self.params.clone();

        let result = std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "Failed to create runtime for EXISTS: {}",
                            e
                        ))
                    })?;

                // Merge stored outer entity vars with batch-extracted vars
                // so the inner planner's compiler can detect correlated refs.
                let mut combined_entity_vars = self.outer_entity_vars.clone();
                combined_entity_vars.extend(entity_vars.iter().cloned());

                for row_idx in 0..num_rows {
                    let row_params = extract_row_params(batch, row_idx);
                    let mut sub_params = base_params.clone();
                    sub_params.extend(row_params);

                    // Add entity variable → VID value mappings so that
                    // Parameter("n") resolves to the VID for traversal sources.
                    for var in &entity_vars {
                        let vid_key = format!("{}._vid", var);
                        if let Some(vid_val) = sub_params.get(&vid_key).cloned() {
                            sub_params.insert(var.clone(), vid_val);
                        }
                    }

                    let (batches, _plan) = rt.block_on(execute_subplan_with_outer_vars(
                        &logical_plan,
                        &sub_params,
                        &HashMap::new(), // No outer values for EXISTS subquery
                        &graph_ctx,
                        &session_ctx,
                        &storage,
                        &uni_schema,
                        &combined_entity_vars,
                        None, // EXISTS compile-time rejects mutations (expr_compiler:693)
                    ))?;

                    let matched: usize = batches.iter().map(|b| b.num_rows()).sum();
                    let values = if mode == SubqueryResult::Collect {
                        collect_single_column(&batches)?
                    } else {
                        Vec::new()
                    };
                    per_row.push((matched, values));
                }

                Ok::<_, datafusion::error::DataFusionError>(())
            })
            .join()
            .unwrap_or_else(|_| {
                Err(datafusion::error::DataFusionError::Execution(
                    "EXISTS subquery thread panicked".to_string(),
                ))
            })
        });

        if let Err(e) = result {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "EXISTS subquery execution failed: {}",
                e
            )));
        }

        let array: arrow_array::ArrayRef = match self.mode {
            SubqueryResult::Exists => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for (matched, _) in &per_row {
                    builder.append_value(*matched > 0);
                }
                Arc::new(builder.finish())
            }
            SubqueryResult::Count => {
                let mut builder = arrow_array::builder::Int64Builder::with_capacity(num_rows);
                for (matched, _) in &per_row {
                    builder.append_value(*matched as i64);
                }
                Arc::new(builder.finish())
            }
            SubqueryResult::Collect => {
                let mut values = arrow_array::builder::LargeBinaryBuilder::new();
                let mut offsets: Vec<i64> = Vec::with_capacity(num_rows + 1);
                offsets.push(0);
                let mut total: i64 = 0;
                for (_, row_values) in &per_row {
                    for v in row_values {
                        values.append_value(uni_common::cypher_value_codec::encode(v));
                        total += 1;
                    }
                    offsets.push(total);
                }
                Arc::new(arrow_array::LargeListArray::new(
                    comprehension_item_field(),
                    datafusion::arrow::buffer::OffsetBuffer::new(
                        datafusion::arrow::buffer::ScalarBuffer::from(offsets),
                    ),
                    Arc::new(values.finish()),
                    None,
                ))
            }
        };
        Ok(datafusion::physical_plan::ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "ExistsExecExpr has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

// ---------------------------------------------------------------------------
// Unanchored pattern comprehension
// ---------------------------------------------------------------------------

/// A pattern comprehension evaluated as a correlated subquery, one row at a time.
///
/// The vectorized operator (`PatternComprehensionExecExpr`) anchors on a pattern
/// node that is already bound in the outer scope and walks CSR adjacency from its
/// VID. When every pattern variable is fresh there is no such node, and
/// `analyze_pattern` fails.
///
/// That is not a reason to reject the query: `[(a:P)-[:KNOWS]->(b:P) | a.name]`
/// is legal openCypher, and pattern *predicates* in the same position already
/// worked — `compile_pattern_exists` falls back to `ExistsExecExpr` when
/// anchoring fails (see the `Expr::Exists` arm of `compile`). Comprehensions
/// simply had no equivalent, which is the asymmetry this closes.
///
/// This is deliberately the *slow* path: the subquery is planned once and then
/// executed per outer row, so an uncorrelated comprehension pays for a full
/// evaluation on every row even though its value is identical each time. Making
/// that case cheap is a planner rewrite (hoist it, evaluate once, broadcast) and
/// is separate work; correctness comes first, and it gives that work a working
/// baseline to be measured against.
struct PatternComprehensionSubqueryExpr {
    query: Query,
    /// Names the pattern itself binds. An outer column of the same name must not
    /// be declared in scope, or the planner would treat the pattern's fresh
    /// binding as a reference to the outer one and silently change the query.
    pattern_vars: HashSet<String>,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<SessionContext>>,
    storage: Arc<StorageManager>,
    uni_schema: Arc<UniSchema>,
    params: HashMap<String, Value>,
    outer_entity_vars: HashSet<String>,
}

impl std::fmt::Debug for PatternComprehensionSubqueryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PatternComprehensionSubqueryExpr")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for PatternComprehensionSubqueryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PatternComprehensionSubquery")
    }
}

impl PartialEq for PatternComprehensionSubqueryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.query == other.query
    }
}

impl Eq for PatternComprehensionSubqueryExpr {}

impl Hash for PatternComprehensionSubqueryExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{:?}", self.query).hash(state);
    }
}

/// The values of the single column a `COLLECT { … }` body returns.
///
/// openCypher requires the body to end in a `RETURN` of exactly one column, so
/// anything else is a query error rather than an arbitrary pick — silently
/// taking the first of several would make the answer depend on column order.
fn collect_single_column(
    batches: &[arrow_array::RecordBatch],
) -> datafusion::error::Result<Vec<Value>> {
    let mut out = Vec::new();
    for batch in batches {
        if batch.num_columns() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "COLLECT {{ … }} must return exactly one column, got {}",
                batch.num_columns()
            )));
        }
        let column = batch.column(0);
        for row in 0..batch.num_rows() {
            out.push(arrow_to_json_value(column, row));
        }
    }
    Ok(out)
}

/// The item type of the produced list.
///
/// `LargeBinary` CypherValue, matching what the vectorized path produces for the
/// common case of a property projection: `build_inner_schema` types vertex and
/// edge properties as `LargeBinary`, so `[... | a.name]` is a
/// `LargeList(LargeBinary)` there too. A fixed item type is required at plan
/// time and the map expression's type is not known on this path, so encoding
/// every element as a CypherValue is the one choice that cannot be wrong for a
/// particular map expression.
fn comprehension_item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::LargeBinary, true))
}

impl PhysicalExpr for PatternComprehensionSubqueryExpr {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn data_type(
        &self,
        _input_schema: &Schema,
    ) -> datafusion::error::Result<arrow_schema::DataType> {
        Ok(DataType::LargeList(comprehension_item_field()))
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::physical_plan::ColumnarValue> {
        let num_rows = batch.num_rows();

        // Identify the outer scope's entity variables from the batch schema, the
        // same way `ExistsExecExpr` does, so correlated references inside the
        // comprehension resolve against the current row.
        let schema = batch.schema();
        let mut entity_vars: HashSet<String> = HashSet::new();
        for field in schema.fields() {
            let name = field.name();
            if let Some(base) = name.strip_suffix("._vid") {
                entity_vars.insert(base.to_string());
            }
            if matches!(field.data_type(), DataType::Struct(_)) {
                entity_vars.insert(name.to_string());
            }
            if !name.contains('.')
                && !name.starts_with('_')
                && matches!(field.data_type(), DataType::Int64 | DataType::UInt64)
            {
                entity_vars.insert(name.to_string());
            }
        }
        // Everything the outer row carries under a bare name is in scope for the
        // subquery, not just entity variables: a comprehension may correlate
        // through any value, and `startNode(r).id` — how LDBC IC14 correlates —
        // reaches `r` inside a function call rather than as a property access, so
        // the property-access rewrite below never sees it. `extract_row_params`
        // already supplies these by column name, and `arrow_to_json_value`
        // decodes a CypherValue column, so an edge arrives as an edge.
        let mut vars_in_scope: HashSet<String> = entity_vars.clone();
        for field in schema.fields() {
            let name = field.name();
            if !name.contains('.') && !name.starts_with('_') {
                vars_in_scope.insert(name.clone());
            }
        }
        // A pattern variable shadows an outer column of the same name.
        for v in &self.pattern_vars {
            vars_in_scope.remove(v);
        }
        let vars_in_scope: Vec<String> = vars_in_scope.into_iter().collect();

        // Only genuine entity variables drive the property-access rewrite; a
        // shadowed name must not be rewritten to an outer parameter either.
        let correlated_vars: HashSet<String> = entity_vars
            .difference(&self.pattern_vars)
            .cloned()
            .collect();
        let rewritten_query = rewrite_query_correlated(&self.query, &correlated_vars);

        // Planned once; the rewrite turned correlated references into parameters,
        // so the plan is the same for every row.
        let planner = QueryPlanner::new(self.uni_schema.clone());
        let logical_plan = planner
            .plan_with_scope(rewritten_query, vars_in_scope)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "pattern comprehension subquery planning failed: {e}"
                ))
            })?;

        let graph_ctx = self.graph_ctx.clone();
        let session_ctx = self.session_ctx.clone();
        let storage = self.storage.clone();
        let uni_schema = self.uni_schema.clone();
        let base_params = self.params.clone();

        // One list per outer row, flattened into `values` with `offsets` marking
        // the boundaries — the LargeListArray layout.
        let mut values: Vec<Option<Vec<u8>>> = Vec::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(num_rows + 1);
        offsets.push(0);

        let result = std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "failed to create runtime for pattern comprehension: {e}"
                        ))
                    })?;

                let mut combined_entity_vars = self.outer_entity_vars.clone();
                combined_entity_vars.extend(correlated_vars.iter().cloned());

                for row_idx in 0..num_rows {
                    let row_params = extract_row_params(batch, row_idx);
                    let mut sub_params = base_params.clone();
                    sub_params.extend(row_params);
                    for var in &correlated_vars {
                        let vid_key = format!("{}._vid", var);
                        if let Some(vid_val) = sub_params.get(&vid_key).cloned() {
                            sub_params.insert(var.clone(), vid_val);
                        }
                    }

                    let (batches, _plan) = rt.block_on(execute_subplan_with_outer_vars(
                        &logical_plan,
                        &sub_params,
                        &HashMap::new(),
                        &graph_ctx,
                        &session_ctx,
                        &storage,
                        &uni_schema,
                        &combined_entity_vars,
                        None,
                    ))?;

                    // The synthesized query returns exactly one column — the
                    // comprehension's map expression.
                    for b in &batches {
                        if b.num_columns() == 0 {
                            continue;
                        }
                        let col = b.column(0);
                        for i in 0..b.num_rows() {
                            let v = arrow_to_json_value(col.as_ref(), i);
                            values.push(Some(uni_common::cypher_value_codec::encode(&v)));
                        }
                    }
                    offsets.push(values.len() as i64);
                }

                Ok::<_, datafusion::error::DataFusionError>(())
            })
            .join()
            .unwrap_or_else(|_| {
                Err(datafusion::error::DataFusionError::Execution(
                    "pattern comprehension subquery thread panicked".to_string(),
                ))
            })
        });
        result?;

        let values_array = arrow_array::LargeBinaryArray::from_iter(values);
        // No null buffer: a row that matched nothing gets an empty list, never
        // null — the same contract the vectorized path has.
        let list = arrow_array::LargeListArray::new(
            comprehension_item_field(),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(values_array),
            None,
        );
        Ok(datafusion::physical_plan::ColumnarValue::Array(Arc::new(
            list,
        )))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "PatternComprehensionSubqueryExpr has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl PartialEq<dyn Any> for PatternComprehensionSubqueryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .is_some_and(|o| self.query == o.query)
    }
}

// ---------------------------------------------------------------------------
// EXISTS subquery helpers
// ---------------------------------------------------------------------------

/// Check if a Query contains any mutation clauses (CREATE, SET, DELETE, REMOVE, MERGE).
/// EXISTS subqueries must be read-only per OpenCypher spec.
fn has_mutation_clause(query: &Query) -> bool {
    match query {
        Query::Single(stmt) => stmt.clauses.iter().any(|c| {
            matches!(
                c,
                Clause::Create(_)
                    | Clause::Delete(_)
                    | Clause::Set(_)
                    | Clause::Remove(_)
                    | Clause::Merge(_)
            ) || has_mutation_in_clause_exprs(c)
        }),
        Query::Union { left, right, .. } => has_mutation_clause(left) || has_mutation_clause(right),
        _ => false,
    }
}

/// Check if a clause contains nested EXISTS with mutations (recursive).
fn has_mutation_in_clause_exprs(clause: &Clause) -> bool {
    let check_expr = |e: &Expr| -> bool { has_mutation_in_expr(e) };

    match clause {
        Clause::Match(m) => m.where_clause.as_ref().is_some_and(check_expr),
        Clause::With(w) => {
            w.where_clause.as_ref().is_some_and(check_expr)
                || w.items.iter().any(|item| match item {
                    ReturnItem::Expr { expr, .. } => has_mutation_in_expr(expr),
                    ReturnItem::All => false,
                })
        }
        Clause::Return(r) => r.items.iter().any(|item| match item {
            ReturnItem::Expr { expr, .. } => has_mutation_in_expr(expr),
            ReturnItem::All => false,
        }),
        _ => false,
    }
}

/// Check if an expression tree contains an EXISTS with mutation clauses.
fn has_mutation_in_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Exists { query, .. } => has_mutation_clause(query),
        _ => {
            let mut found = false;
            expr.for_each_child(&mut |child| {
                if has_mutation_in_expr(child) {
                    found = true;
                }
            });
            found
        }
    }
}

/// Rewrite a Query AST to replace correlated property accesses with parameter references.
///
/// For each `Property(Variable(v), key)` where `v` is an outer-scope entity variable,
/// replaces it with `Parameter("{v}.{key}")`. This enables plan-once optimization since
/// the rewritten query is parameterized (same structure for every row).
fn rewrite_query_correlated(query: &Query, outer_vars: &HashSet<String>) -> Query {
    match query {
        Query::Single(stmt) => Query::Single(Statement {
            clauses: stmt
                .clauses
                .iter()
                .map(|c| rewrite_clause_correlated(c, outer_vars))
                .collect(),
        }),
        Query::Union { left, right, all } => Query::Union {
            left: Box::new(rewrite_query_correlated(left, outer_vars)),
            right: Box::new(rewrite_query_correlated(right, outer_vars)),
            all: *all,
        },
        other => other.clone(),
    }
}

/// Rewrite expressions within a clause for correlated property access.
fn rewrite_clause_correlated(clause: &Clause, outer_vars: &HashSet<String>) -> Clause {
    match clause {
        Clause::Match(m) => Clause::Match(MatchClause {
            optional: m.optional,
            pattern: m.pattern.clone(),
            where_clause: m
                .where_clause
                .as_ref()
                .map(|e| rewrite_expr_correlated(e, outer_vars)),
            for_update: m.for_update,
        }),
        Clause::With(w) => Clause::With(WithClause {
            distinct: w.distinct,
            items: w
                .items
                .iter()
                .map(|item| rewrite_return_item(item, outer_vars))
                .collect(),
            order_by: w.order_by.as_ref().map(|items| {
                items
                    .iter()
                    .map(|si| SortItem {
                        expr: rewrite_expr_correlated(&si.expr, outer_vars),
                        ascending: si.ascending,
                    })
                    .collect()
            }),
            skip: w
                .skip
                .as_ref()
                .map(|e| rewrite_expr_correlated(e, outer_vars)),
            limit: w
                .limit
                .as_ref()
                .map(|e| rewrite_expr_correlated(e, outer_vars)),
            where_clause: w
                .where_clause
                .as_ref()
                .map(|e| rewrite_expr_correlated(e, outer_vars)),
        }),
        Clause::Return(r) => Clause::Return(ReturnClause {
            distinct: r.distinct,
            items: r
                .items
                .iter()
                .map(|item| rewrite_return_item(item, outer_vars))
                .collect(),
            order_by: r.order_by.as_ref().map(|items| {
                items
                    .iter()
                    .map(|si| SortItem {
                        expr: rewrite_expr_correlated(&si.expr, outer_vars),
                        ascending: si.ascending,
                    })
                    .collect()
            }),
            skip: r
                .skip
                .as_ref()
                .map(|e| rewrite_expr_correlated(e, outer_vars)),
            limit: r
                .limit
                .as_ref()
                .map(|e| rewrite_expr_correlated(e, outer_vars)),
        }),
        Clause::Unwind(u) => Clause::Unwind(UnwindClause {
            expr: rewrite_expr_correlated(&u.expr, outer_vars),
            variable: u.variable.clone(),
        }),
        other => other.clone(),
    }
}

fn rewrite_return_item(item: &ReturnItem, outer_vars: &HashSet<String>) -> ReturnItem {
    match item {
        ReturnItem::All => ReturnItem::All,
        ReturnItem::Expr {
            expr,
            alias,
            source_text,
        } => ReturnItem::Expr {
            expr: rewrite_expr_correlated(expr, outer_vars),
            alias: alias.clone(),
            source_text: source_text.clone(),
        },
    }
}

/// Rewrite a single expression: Property(Variable(v), key) → Parameter("{v}.{key}")
/// when v is an outer-scope entity variable. Handles nested EXISTS recursively.
fn rewrite_expr_correlated(expr: &Expr, outer_vars: &HashSet<String>) -> Expr {
    match expr {
        // Core rewrite: n.prop → $param("n.prop") when n is an outer entity
        Expr::Property(base, key) => {
            if let Expr::Variable(v) = base.as_ref()
                && outer_vars.contains(v)
            {
                return Expr::Parameter(format!("{}.{}", v, key));
            }
            Expr::Property(
                Box::new(rewrite_expr_correlated(base, outer_vars)),
                key.clone(),
            )
        }
        // Nested EXISTS — recurse into the subquery body
        Expr::Exists {
            query,
            from_pattern_predicate,
        } => Expr::Exists {
            query: Box::new(rewrite_query_correlated(query, outer_vars)),
            from_pattern_predicate: *from_pattern_predicate,
        },
        // CountSubquery and CollectSubquery — recurse into subquery body
        Expr::CountSubquery(query) => {
            Expr::CountSubquery(Box::new(rewrite_query_correlated(query, outer_vars)))
        }
        Expr::CollectSubquery(query) => {
            Expr::CollectSubquery(Box::new(rewrite_query_correlated(query, outer_vars)))
        }
        // All other expressions: recursively transform children
        other => other
            .clone()
            .map_children(&mut |child| rewrite_expr_correlated(&child, outer_vars)),
    }
}

/// Look up the `DistanceMetric` for a vector-indexed property on a label.
///
/// When `label` is known, an index matching both `(label, property)` is
/// preferred so distinct labels that reuse a property name (e.g. `emb`) resolve
/// to their own metric instead of the first-declared index winning. When the
/// label is unknown the search falls back to the first property-name match.
///
/// Returns `None` if no vector index exists for the property.
fn resolve_metric_for_property(
    schema: &UniSchema,
    label: Option<&str>,
    property: &str,
) -> Option<DistanceMetric> {
    if let Some(label) = label {
        for idx in &schema.indexes {
            if let IndexDefinition::Vector(config) = idx
                && config.label == label
                && config.property == property
            {
                return Some(config.metric.clone());
            }
        }
    }
    // Fall back to a property-only match when the source label is unknown.
    for idx in &schema.indexes {
        if let IndexDefinition::Vector(config) = idx
            && config.property == property
        {
            return Some(config.metric.clone());
        }
    }
    None
}
