// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Translation layer from Cypher expressions to DataFusion expressions.
//!
//! This module provides [`cypher_expr_to_df`] which converts Cypher AST expressions
//! into DataFusion physical expressions suitable for use in DataFusion execution plans.
//!
//! # Property Naming Convention
//!
//! Properties are materialized as columns with the naming convention `{variable}.{property}`.
//! For example, `n.age` becomes column `"n.age"`.
//!
//! # Supported Expressions
//!
//! - Identifiers and property access
//! - Literal values (numbers, strings, booleans, null)
//! - Binary operators (comparison, arithmetic, boolean)
//! - Unary operators (NOT, negation)
//! - IS NULL / IS NOT NULL
//! - String operations (CONTAINS, STARTS WITH, ENDS WITH)
//! - IN list checks
//! - CASE expressions
//!
//! # Unsupported Expressions
//!
//! Some Cypher expressions require custom handling and are not yet supported:
//! - List comprehensions
//! - Reduce expressions
//! - Subqueries (EXISTS, scalar subqueries)
//! - Approximate equality (~=) for vectors

use anyhow::{Result, anyhow};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, col, lit};
use datafusion::prelude::ExprFunctionExt;
use std::hash::{Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;
use uni_common::Value;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, MapProjectionItem, UnaryOp};

// Internal column names for graph entities
const COL_VID: &str = "_vid";
const COL_EID: &str = "_eid";
const COL_LABELS: &str = "_labels";
const COL_TYPE: &str = "_type";

/// Infer the common Arrow DataType from a list of ScalarValues, ignoring nulls.
fn infer_common_scalar_type(scalars: &[ScalarValue]) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;

    let non_null: Vec<_> = scalars
        .iter()
        .filter(|s| !matches!(s, ScalarValue::Null))
        .collect();

    if non_null.is_empty() {
        return DataType::Null;
    }

    // Check for homogeneous types
    if non_null.iter().all(|s| matches!(s, ScalarValue::Int64(_))) {
        DataType::Int64
    } else if non_null
        .iter()
        .all(|s| matches!(s, ScalarValue::Float64(_) | ScalarValue::Int64(_)))
    {
        DataType::Float64
    } else if non_null.iter().all(|s| matches!(s, ScalarValue::Utf8(_))) {
        DataType::Utf8
    } else if non_null
        .iter()
        .all(|s| matches!(s, ScalarValue::Boolean(_)))
    {
        DataType::Boolean
    } else {
        // Mixed types - use LargeBinary (CypherValue) to preserve type information
        DataType::LargeBinary
    }
}

/// Check if a DataFusion expression is a string literal.
fn is_string_literal(e: &DfExpr) -> bool {
    matches!(e, DfExpr::Literal(ScalarValue::Utf8(_), _))
}

/// CypherValue list UDF names (LargeBinary-encoded lists).
const CYPHER_LIST_FUNCS: &[&str] = &[
    "_make_cypher_list",
    "_cypher_list_concat",
    "_cypher_list_append",
];

/// Check if a DataFusion expression is a CypherValue-encoded list (LargeBinary).
fn is_cypher_list_expr(e: &DfExpr) -> bool {
    matches!(e, DfExpr::Literal(ScalarValue::LargeBinary(_), _))
        || matches!(e, DfExpr::ScalarFunction(f) if CYPHER_LIST_FUNCS.contains(&f.func.name()))
}

/// Check if a DataFusion expression produces a list value (native or CypherValue).
fn is_list_expr(e: &DfExpr) -> bool {
    is_cypher_list_expr(e)
        || matches!(e, DfExpr::Literal(ScalarValue::List(_), _))
        || matches!(e, DfExpr::ScalarFunction(f) if f.func.name() == "make_array")
}

/// Type of a variable in the query context.
///
/// Used to determine the identity column when a bare variable is referenced
/// (e.g., `n` in `RETURN n` should resolve to `n._vid` for nodes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableKind {
    /// Node variable - identity is `_vid`
    Node,
    /// Edge/relationship variable - identity is `_eid`
    Edge,
    /// Edge list variable (r in `[r*]`) - List<Edge>
    EdgeList,
    /// Path variable - kept as-is (struct with nodes/relationships)
    Path,
}

impl VariableKind {
    /// Return the appropriate edge variable kind based on whether the
    /// pattern is variable-length (`[r*]` -> `EdgeList`) or single-hop
    /// (`[r]` -> `Edge`).
    pub fn edge_for(is_variable_length: bool) -> Self {
        if is_variable_length {
            Self::EdgeList
        } else {
            Self::Edge
        }
    }
}

/// Convert a Cypher expression to a DataFusion expression.
///
/// Translates the Cypher AST representation into DataFusion's expression model
/// for use in filter predicates, projections, and aggregations.
///
/// # Arguments
///
/// * `expr` - The Cypher expression to translate
/// * `context` - Optional translation context for resolving variables
///
/// # Errors
///
/// Returns an error if the expression contains unsupported constructs such as
/// list comprehensions, reduce expressions, or subqueries.
///
/// # Examples
///
/// ```ignore
/// use uni_query::query::ast::{Expr, Operator};
/// use uni_query::query::df_expr::cypher_expr_to_df;
///
/// // Simple property comparison: n.age > 30
/// let cypher_expr = Expr::BinaryOp {
///     left: Box::new(Expr::Property(
///         Box::new(Expr::Variable("n".to_string())),
///         "age".to_string(),
///     )),
///     op: BinaryOp::Gt,
///     right: Box::new(Expr::Literal(serde_json::json!(30))),
/// };
///
/// let df_expr = cypher_expr_to_df(&cypher_expr, None)?;
/// // Result: col("n.age") > lit(30)
/// ```
pub fn cypher_expr_to_df(expr: &Expr, context: Option<&TranslationContext>) -> Result<DfExpr> {
    match expr {
        Expr::PatternComprehension { .. } => Err(anyhow!(
            "Pattern comprehensions require fallback executor (graph traversal)"
        )),
        // TODO: Resolve wildcard to concrete expressions per DataFusion guidance
        // See: https://github.com/apache/datafusion/issues/7765
        #[allow(deprecated)]
        Expr::Wildcard => Ok(DfExpr::Wildcard {
            qualifier: None,
            options: Default::default(),
        }),

        Expr::Variable(name) => {
            // Use Column::from_name() to avoid treating dots as table.column qualifiers.
            // When the variable kind is known (Node, Edge, or Path), return
            // the column representing the whole entity. The struct is built by
            // add_structural_projection() in the planner.
            if let Some(ctx) = context
                && ctx.variable_kinds.contains_key(name)
            {
                return Ok(DfExpr::Column(Column::from_name(name)));
            }

            // Check if the variable name matches a parameter (e.g., CTE working table
            // injected as a parameter). This allows `WHERE x IN hierarchy` to resolve
            // `hierarchy` from params when it's not a schema column.
            if let Some(ctx) = context
                && let Some(value) = ctx.parameters.get(name)
            {
                return value_to_scalar(value).map(lit);
            }

            // Fallback for unknown variables
            Ok(DfExpr::Column(Column::from_name(name)))
        }

        Expr::Property(base, prop) => translate_property_access(base, prop, context),

        Expr::ArrayIndex { array, index } => {
            // If array is a variable and index is a string literal, convert to column access
            // e.g., n['name'] -> n.name column
            if let Ok(var_name) = extract_variable_name(array)
                && let Expr::Literal(CypherLiteral::String(prop_name)) = index.as_ref()
            {
                let col_name = format!("{}.{}", var_name, prop_name);
                return Ok(DfExpr::Column(Column::from_name(col_name)));
            }

            let array_expr = cypher_expr_to_df(array, context)?;
            let index_expr = cypher_expr_to_df(index, context)?;

            // Use custom index UDF to support dynamic Map and List access
            Ok(dummy_udf_expr("index", vec![array_expr, index_expr]))
        }

        Expr::ArraySlice { array, start, end } => {
            // Cypher uses 0-based slicing: [start..end) (end is exclusive)
            // DataFusion array_slice uses 1-based indexing: slice(arr, start, end)
            let array_expr = cypher_expr_to_df(array, context)?;

            let start_expr = if let Some(s) = start {
                let s_expr = cypher_expr_to_df(s, context)?;
                // Convert 0-based to 1-based
                s_expr + lit(1i64)
            } else {
                // Default to start from beginning
                lit(1i64)
            };

            let end_expr = if let Some(e) = end {
                // Cypher end is exclusive, DataFusion end is inclusive
                // So we don't need to adjust (Cypher's exclusive end == DataFusion's inclusive end - 1 + 1)
                cypher_expr_to_df(e, context)?
            } else {
                // Slice to end - use array_length (cast UInt64 → Int64 for array_slice compatibility)
                cast_expr(
                    datafusion::functions_nested::expr_fn::array_length(array_expr.clone()),
                    datafusion::arrow::datatypes::DataType::Int64,
                )
            };

            // If the array is CypherValue-encoded (LargeBinary), use _cypher_list_slice UDF
            // instead of DataFusion's array_slice which rejects LargeBinary input
            if is_cypher_list_expr(&array_expr) {
                Ok(dummy_udf_expr(
                    "_cypher_list_slice",
                    vec![array_expr, start_expr, end_expr],
                ))
            } else {
                Ok(datafusion::functions_nested::expr_fn::array_slice(
                    array_expr, start_expr, end_expr, None,
                ))
            }
        }

        Expr::Parameter(name) => {
            // Parameters should be resolved by the context
            if let Some(ctx) = context
                && let Some(value) = ctx.parameters.get(name)
            {
                return value_to_scalar(value).map(lit);
            }
            Err(anyhow!("Unresolved parameter: ${}", name))
        }

        Expr::Literal(value) => {
            let scalar = cypher_literal_to_scalar(value)?;
            Ok(lit(scalar))
        }

        Expr::List(items) => translate_list_literal(items, context),

        Expr::Map(entries) => {
            if entries.is_empty() {
                // Empty map {} — encode as LargeBinary CypherValue since named_struct() needs args
                let cv_bytes = uni_common::cypher_value_codec::encode(&uni_common::Value::Map(
                    Default::default(),
                ));
                return Ok(lit(ScalarValue::LargeBinary(Some(cv_bytes))));
            }
            // Use named_struct to create a Struct type in DataFusion.
            // This supports dynamic values and correct Map return types (instead of JSON strings).
            let mut args = Vec::with_capacity(entries.len() * 2);
            for (key, val_expr) in entries {
                args.push(lit(key.clone()));
                args.push(cypher_expr_to_df(val_expr, context)?);
            }
            Ok(datafusion::functions::expr_fn::named_struct(args))
        }

        Expr::IsNull(inner) => {
            let inner_expr = cypher_expr_to_df(inner, context)?;
            Ok(inner_expr.is_null())
        }

        Expr::IsNotNull(inner) => {
            let inner_expr = cypher_expr_to_df(inner, context)?;
            Ok(inner_expr.is_not_null())
        }

        Expr::IsUnique(_) => {
            // IS UNIQUE is only valid in constraint definitions, not in query expressions
            Err(anyhow!(
                "IS UNIQUE can only be used in constraint definitions"
            ))
        }

        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => {
            // If this function has a window spec, it should have been computed by a Window node
            // below in the plan. Treat it as a column reference to that computed result.
            if window_spec.is_some() {
                // The column name is the string representation of the window function
                let col_name = expr.to_string_repr();
                Ok(col(&col_name))
            } else {
                translate_function_call(name, args, *distinct, context)
            }
        }

        Expr::In { expr, list } => translate_in_expression(expr, list, context),

        Expr::BinaryOp { left, op, right } => {
            let left_expr = cypher_expr_to_df(left, context)?;
            let right_expr = cypher_expr_to_df(right, context)?;
            translate_binary_op(left_expr, op, right_expr)
        }

        Expr::UnaryOp { op, expr: inner } => {
            let inner_expr = cypher_expr_to_df(inner, context)?;
            match op {
                UnaryOp::Not => Ok(inner_expr.not()),
                UnaryOp::Neg => Ok(DfExpr::Negative(Box::new(inner_expr))),
            }
        }

        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => translate_case_expression(expr, when_then, else_expr, context),

        Expr::Reduce { .. } => Err(anyhow!(
            "Reduce expressions not yet supported in DataFusion translation"
        )),

        Expr::Exists { .. } => Err(anyhow!(
            "EXISTS subqueries not yet supported in DataFusion translation"
        )),

        Expr::CountSubquery(_) => Err(anyhow!(
            "Count subqueries not yet supported in DataFusion translation"
        )),

        Expr::CollectSubquery(_) => Err(anyhow!(
            "COLLECT subqueries not yet supported in DataFusion translation"
        )),

        Expr::Quantifier { .. } => {
            // Quantifier expressions (ALL/ANY/SINGLE/NONE) cannot be translated to
            // DataFusion logical expressions because they require lambda iteration.
            // They are handled via CypherPhysicalExprCompiler → QuantifierExecExpr.
            // This path is only hit from the schemaless filter fallback.
            Err(anyhow!(
                "Quantifier expressions (ALL/ANY/SINGLE/NONE) require physical compilation \
                 via CypherPhysicalExprCompiler"
            ))
        }

        Expr::ListComprehension { .. } => {
            // List comprehensions require lambda/higher-order functions similar to quantifiers.
            //
            // Example: [x IN list WHERE x > 0 | x * 2] requires:
            //   1. Iterating over array elements
            //   2. Filtering based on predicate (optional)
            //   3. Mapping each element through projection expression
            //
            // This is equivalent to: list_filter(list, x -> x > 0).map(x -> x * 2)
            //
            // DESIGN DECISION: Intentionally fail here and let execution fall back to the
            // fallback executor, which will have comprehension support.
            Err(anyhow!(
                "List comprehensions not yet supported in DataFusion translation - requires lambda functions"
            ))
        }

        Expr::ValidAt { .. } => {
            // VALID_AT should have been transformed to a function call in the planner
            // before reaching DataFusion translation.
            Err(anyhow!(
                "VALID_AT expression should have been transformed to function call in planner"
            ))
        }

        Expr::MapProjection { base, items } => translate_map_projection(base, items, context),
    }
}

/// Context for expression translation.
///
/// Provides parameter values and schema information for resolving expressions.
#[derive(Debug, Default, Clone)]
pub struct TranslationContext {
    /// Parameter values for query parameterization.
    pub parameters: std::collections::HashMap<String, Value>,

    /// Known variable to label mapping (for type inference).
    pub variable_labels: std::collections::HashMap<String, String>,

    /// Variable kinds (node, edge, path) for identity column resolution.
    pub variable_kinds: std::collections::HashMap<String, VariableKind>,
}

impl TranslationContext {
    /// Create a new empty translation context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a parameter value.
    pub fn with_parameter(mut self, name: impl Into<String>, value: Value) -> Self {
        self.parameters.insert(name.into(), value);
        self
    }

    /// Add a variable to label mapping.
    pub fn with_variable_label(mut self, var: impl Into<String>, label: impl Into<String>) -> Self {
        self.variable_labels.insert(var.into(), label.into());
        self
    }
}

/// Extract the variable name from an expression chain.
fn extract_variable_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Variable(name) => Ok(name.clone()),
        Expr::Property(base, _) => extract_variable_name(base),
        _ => Err(anyhow!(
            "Cannot extract variable name from expression: {:?}",
            expr
        )),
    }
}

/// Translate a property access expression (e.g., `n.name`) to DataFusion.
fn translate_property_access(
    base: &Expr,
    prop: &str,
    context: Option<&TranslationContext>,
) -> Result<DfExpr> {
    // Check if this is a duration property accessor (e.g., dur.days, dur.seconds).
    // If the base is not a known graph entity (node/edge) and the property name
    // is a valid duration accessor, emit a _duration_property UDF call.
    if let Ok(var_name) = extract_variable_name(base) {
        let is_graph_entity = context
            .and_then(|ctx| ctx.variable_kinds.get(&var_name))
            .is_some_and(|k| matches!(k, VariableKind::Node | VariableKind::Edge));

        if !is_graph_entity && crate::query::datetime::is_duration_accessor(prop) {
            let base_expr = DfExpr::Column(Column::from_name(var_name));
            return Ok(dummy_udf_expr(
                "_duration_property",
                vec![base_expr, lit(prop.to_string())],
            ));
        }

        // Standard property access: "{variable}.{property}" column reference.
        let col_name = format!("{}.{}", var_name, prop);

        // Check if this property is available as a correlated parameter
        // (e.g., in CALL subqueries where outer columns are injected as params).
        if let Some(ctx) = context
            && let Some(value) = ctx.parameters.get(&col_name)
        {
            return value_to_scalar(value).map(lit);
        }

        if is_graph_entity {
            // Graph entity: use flat column reference "{variable}.{property}".
            // Scans/traversals materialize node/edge props as separate columns.
            Ok(DfExpr::Column(Column::from_name(col_name)))
        } else {
            // Non-graph variable (map from WITH, UNWIND element, aliased value):
            // use index UDF for dynamic field access at runtime.
            let base_expr = DfExpr::Column(Column::from_name(var_name));
            Ok(dummy_udf_expr(
                "index",
                vec![base_expr, lit(prop.to_string())],
            ))
        }
    } else {
        // Base is a complex expression (e.g., function call result,
        // array index, parameter).
        // Try duration accessor first.
        if crate::query::datetime::is_duration_accessor(prop) {
            let base_expr = cypher_expr_to_df(base, context)?;
            return Ok(dummy_udf_expr(
                "_duration_property",
                vec![base_expr, lit(prop.to_string())],
            ));
        }

        // Special case: Parameter base (e.g., $session.tenant_id).
        // Resolve at compile time for correct typing.
        if let Expr::Parameter(param_name) = base {
            if let Some(ctx) = context
                && let Some(value) = ctx.parameters.get(param_name)
            {
                if let Value::Map(map) = value {
                    let extracted = map.get(prop).cloned().unwrap_or(Value::Null);
                    return value_to_scalar(&extracted).map(lit);
                }
                return Ok(lit(ScalarValue::Null));
            }
            return Err(anyhow!("Unresolved parameter: ${}", param_name));
        }

        // General fallback: evaluate base, use index UDF for property access.
        let base_expr = cypher_expr_to_df(base, context)?;
        Ok(dummy_udf_expr(
            "index",
            vec![base_expr, lit(prop.to_string())],
        ))
    }
}

/// Translate a list literal expression to DataFusion.
fn translate_list_literal(items: &[Expr], context: Option<&TranslationContext>) -> Result<DfExpr> {
    // Check for mixed types or nested lists which cause issues in DataFusion
    let mut has_string = false;
    let mut has_bool = false;
    let mut has_list = false;
    let mut has_map = false;
    let mut has_numeric = false;

    for item in items {
        match item {
            Expr::Literal(CypherLiteral::Float(_)) | Expr::Literal(CypherLiteral::Integer(_)) => {
                has_numeric = true
            }
            Expr::Literal(CypherLiteral::String(_)) => has_string = true,
            Expr::Literal(CypherLiteral::Bool(_)) => has_bool = true,
            Expr::List(_) => has_list = true,
            Expr::Map(_) => has_map = true,
            // Treat Null as compatible with anything
            // If complex expr (e.g. Variable), assume compatibility or let DF handle it
            _ => {}
        }
    }

    // Check distinct non-null types count
    let types_count = has_numeric as u8 + has_string as u8 + has_bool as u8 + has_map as u8;

    // Mixed types or nested lists: encode as LargeBinary CypherValue
    if has_list || has_map || types_count > 1 {
        // Try to convert all items to JSON values for CypherValue encoding
        if let Some(json_array) = try_items_to_json(items) {
            let uni_val: uni_common::Value = serde_json::Value::Array(json_array).into();
            let cv_bytes = uni_common::cypher_value_codec::encode(&uni_val);
            return Ok(lit(ScalarValue::LargeBinary(Some(cv_bytes))));
        }
        // Non-literal items (e.g. variables): delegate to _make_cypher_list UDF
        let mut df_args = Vec::with_capacity(items.len());
        for item in items {
            df_args.push(cypher_expr_to_df(item, context)?);
        }
        return Ok(dummy_udf_expr("_make_cypher_list", df_args));
    }

    // Use make_array to create a List type in DataFusion.
    // This supports dynamic values and performs type coercion for mixed numeric types.
    let mut df_args = Vec::with_capacity(items.len());
    let mut has_float = false;
    let mut has_int = false;
    let mut has_other = false;

    for item in items {
        match item {
            Expr::Literal(CypherLiteral::Float(_)) => has_float = true,
            Expr::Literal(CypherLiteral::Integer(_)) => has_int = true,
            _ => has_other = true,
        }
        df_args.push(cypher_expr_to_df(item, context)?);
    }

    if df_args.is_empty() {
        // Empty list with null type
        let empty_arr =
            ScalarValue::new_list_nullable(&[], &datafusion::arrow::datatypes::DataType::Null);
        Ok(lit(ScalarValue::List(empty_arr)))
    } else if has_float && has_int && !has_other {
        // Promote all to Float64 for numeric consistency in Arrow
        let promoted_args = df_args
            .into_iter()
            .map(|e| cast_expr(e, datafusion::arrow::datatypes::DataType::Float64))
            .collect();
        Ok(datafusion::functions_nested::expr_fn::make_array(
            promoted_args,
        ))
    } else {
        Ok(datafusion::functions_nested::expr_fn::make_array(df_args))
    }
}

/// Translate an IN expression to DataFusion.
fn translate_in_expression(
    expr: &Expr,
    list: &Expr,
    context: Option<&TranslationContext>,
) -> Result<DfExpr> {
    // When the left side is a node/edge variable and the right side is a
    // dynamic array (e.g., CTE variable), rewrite to compare by identity
    // column (_vid for nodes, _eid for edges). Cast to Int64 to match the
    // list element type from parameter injection.
    let left_expr = if let Expr::Variable(var) = expr
        && let Some(ctx) = context
        && let Some(kind) = ctx.variable_kinds.get(var)
    {
        match kind {
            VariableKind::Node | VariableKind::Edge => {
                let id_col = match kind {
                    VariableKind::Node => COL_VID,
                    _ => COL_EID,
                };
                cast_expr(
                    DfExpr::Column(Column::from_name(format!("{}.{}", var, id_col))),
                    datafusion::arrow::datatypes::DataType::Int64,
                )
            }
            _ => cypher_expr_to_df(expr, context)?,
        }
    } else {
        cypher_expr_to_df(expr, context)?
    };

    // When the right side is a literal list, route through _cypher_in UDF
    // which handles mixed-type comparisons and Cypher null semantics correctly.
    // DataFusion's native in_list() requires homogeneous types and would fail
    // for cases like `1 IN ['1', 2]`.
    if let Expr::List(items) = list {
        if let Some(json_array) = try_items_to_json(items) {
            // All-literal list -> encode directly as CypherValue (no round-trip through string)
            let uni_val: uni_common::Value = serde_json::Value::Array(json_array).into();
            let cv_bytes = uni_common::cypher_value_codec::encode(&uni_val);
            let list_literal = lit(ScalarValue::LargeBinary(Some(cv_bytes)));
            Ok(dummy_udf_expr("_cypher_in", vec![left_expr, list_literal]))
        } else {
            // Has variables → build list at runtime via _make_cypher_list
            let expanded: Vec<DfExpr> = items
                .iter()
                .map(|item| cypher_expr_to_df(item, context))
                .collect::<Result<Vec<_>>>()?;
            let list_expr = dummy_udf_expr("_make_cypher_list", expanded);
            Ok(dummy_udf_expr("_cypher_in", vec![left_expr, list_expr]))
        }
    } else {
        let right_expr = cypher_expr_to_df(list, context)?;

        // Use _cypher_in UDF for dynamic arrays. This handles all list
        // representations (native List, Utf8 json-encoded, LargeBinary CypherValue)
        // uniformly via Value-level conversion, and implements full Cypher
        // 3-valued IN semantics (null propagation).
        if matches!(right_expr, DfExpr::Literal(ScalarValue::Null, _)) {
            return Ok(lit(ScalarValue::Boolean(None)));
        }

        Ok(dummy_udf_expr("_cypher_in", vec![left_expr, right_expr]))
    }
}

/// Translate a CASE expression to DataFusion.
fn translate_case_expression(
    operand: &Option<Box<Expr>>,
    when_then: &[(Expr, Expr)],
    else_expr: &Option<Box<Expr>>,
    context: Option<&TranslationContext>,
) -> Result<DfExpr> {
    let mut case_builder = if let Some(match_expr) = operand {
        let match_df = cypher_expr_to_df(match_expr, context)?;
        datafusion::logical_expr::case(match_df)
    } else {
        datafusion::logical_expr::when(
            cypher_expr_to_df(&when_then[0].0, context)?,
            cypher_expr_to_df(&when_then[0].1, context)?,
        )
    };

    let start_idx = if operand.is_some() { 0 } else { 1 };
    for (when_expr, then_expr) in when_then.iter().skip(start_idx) {
        let when_df = cypher_expr_to_df(when_expr, context)?;
        let then_df = cypher_expr_to_df(then_expr, context)?;
        case_builder = case_builder.when(when_df, then_df);
    }

    if let Some(else_e) = else_expr {
        let else_df = cypher_expr_to_df(else_e, context)?;
        Ok(case_builder.otherwise(else_df)?)
    } else {
        Ok(case_builder.end()?)
    }
}

/// Translate a map projection expression to DataFusion.
fn translate_map_projection(
    base: &Expr,
    items: &[MapProjectionItem],
    context: Option<&TranslationContext>,
) -> Result<DfExpr> {
    let mut args = Vec::new();
    for item in items {
        match item {
            MapProjectionItem::Property(prop) => {
                args.push(lit(prop.clone()));
                let prop_expr = cypher_expr_to_df(
                    &Expr::Property(Box::new(base.clone()), prop.clone()),
                    context,
                )?;
                args.push(prop_expr);
            }
            MapProjectionItem::LiteralEntry(key, expr) => {
                args.push(lit(key.clone()));
                args.push(cypher_expr_to_df(expr, context)?);
            }
            MapProjectionItem::Variable(var) => {
                args.push(lit(var.clone()));
                args.push(DfExpr::Column(Column::from_name(var)));
            }
            MapProjectionItem::AllProperties => {
                args.push(lit("__all__"));
                args.push(cypher_expr_to_df(base, context)?);
            }
        }
    }
    Ok(dummy_udf_expr("_map_project", args))
}

/// Try to convert a slice of Cypher expressions to JSON values.
/// Returns `None` if any item is not a compile-time-evaluable literal/list/map.
fn try_expr_to_json(expr: &Expr) -> Option<serde_json::Value> {
    match expr {
        Expr::Literal(CypherLiteral::Null) => Some(serde_json::Value::Null),
        Expr::Literal(CypherLiteral::Bool(b)) => Some(serde_json::Value::Bool(*b)),
        Expr::Literal(CypherLiteral::Integer(i)) => {
            Some(serde_json::Value::Number(serde_json::Number::from(*i)))
        }
        Expr::Literal(CypherLiteral::Float(f)) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .or(Some(serde_json::Value::Null)),
        Expr::Literal(CypherLiteral::String(s)) => Some(serde_json::Value::String(s.clone())),
        Expr::List(items) => try_items_to_json(items).map(serde_json::Value::Array),
        Expr::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (k, v) in entries {
                map.insert(k.clone(), try_expr_to_json(v)?);
            }
            Some(serde_json::Value::Object(map))
        }
        _ => None,
    }
}

/// Try to convert a list of Cypher expressions to JSON values.
fn try_items_to_json(items: &[Expr]) -> Option<Vec<serde_json::Value>> {
    items.iter().map(try_expr_to_json).collect()
}

/// Convert a CypherLiteral to a DataFusion scalar value.
fn cypher_literal_to_scalar(lit: &CypherLiteral) -> Result<ScalarValue> {
    match lit {
        CypherLiteral::Null => Ok(ScalarValue::Null),
        CypherLiteral::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        CypherLiteral::Integer(i) => Ok(ScalarValue::Int64(Some(*i))),
        CypherLiteral::Float(f) => Ok(ScalarValue::Float64(Some(*f))),
        CypherLiteral::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
    }
}

/// Convert a `uni_common::Value` to a DataFusion scalar value.
fn value_to_scalar(value: &Value) -> Result<ScalarValue> {
    match value {
        Value::Null => Ok(ScalarValue::Null),
        Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        Value::Int(i) => Ok(ScalarValue::Int64(Some(*i))),
        Value::Float(f) => Ok(ScalarValue::Float64(Some(*f))),
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        Value::List(items) => {
            // Recursively convert items
            let scalars: Result<Vec<ScalarValue>> = items.iter().map(value_to_scalar).collect();
            let scalars = scalars?;

            // Determine common type (simple inference), ignoring nulls
            let data_type = infer_common_scalar_type(&scalars);

            // Convert scalars to the target type if needed
            let typed_scalars: Vec<ScalarValue> = scalars
                .into_iter()
                .map(|s| {
                    if matches!(s, ScalarValue::Null) {
                        return ScalarValue::try_from(&data_type).unwrap_or(ScalarValue::Null);
                    }

                    match (s, &data_type) {
                        (
                            ScalarValue::Int64(Some(v)),
                            datafusion::arrow::datatypes::DataType::Float64,
                        ) => ScalarValue::Float64(Some(v as f64)),
                        (s, datafusion::arrow::datatypes::DataType::LargeBinary) => {
                            // Convert scalar to JSON-like string bytes
                            let s_str = s.to_string();
                            ScalarValue::LargeBinary(Some(s_str.into_bytes()))
                        }
                        (s, datafusion::arrow::datatypes::DataType::Utf8) => {
                            // Coerce anything to String if target is Utf8 (mixed list)
                            if matches!(s, ScalarValue::Utf8(_)) {
                                s
                            } else {
                                ScalarValue::Utf8(Some(s.to_string()))
                            }
                        }
                        (s, _) => s,
                    }
                })
                .collect();

            // Construct list
            if typed_scalars.is_empty() {
                Ok(ScalarValue::List(ScalarValue::new_list_nullable(
                    &[],
                    &data_type,
                )))
            } else {
                Ok(ScalarValue::List(ScalarValue::new_list(
                    &typed_scalars,
                    &data_type,
                    true,
                )))
            }
        }
        Value::Map(map) => {
            // Convert Map to ScalarValue::Struct
            // Sort keys to ensure deterministic field order
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by_key(|(k, _)| *k);

            let mut fields_arrays = Vec::with_capacity(entries.len());

            for (k, v) in entries {
                let scalar = value_to_scalar(v)?;
                let dt = scalar.data_type();
                let field = Arc::new(datafusion::arrow::datatypes::Field::new(k, dt, true));
                let array = scalar.to_array()?;
                fields_arrays.push((field, array));
            }

            Ok(ScalarValue::Struct(Arc::new(
                datafusion::arrow::array::StructArray::from(fields_arrays),
            )))
        }
        Value::Bytes(b) => Ok(ScalarValue::LargeBinary(Some(b.clone()))),
        // For complex graph types, fall back to JSON encoding
        other => {
            let json_val: serde_json::Value = other.clone().into();
            let json_str = serde_json::to_string(&json_val)
                .map_err(|e| anyhow!("Failed to serialize value: {}", e))?;
            Ok(ScalarValue::LargeBinary(Some(json_str.into_bytes())))
        }
    }
}

/// Translate a binary operator expression.
fn translate_binary_op(left: DfExpr, op: &BinaryOp, right: DfExpr) -> Result<DfExpr> {
    match op {
        // Comparison operators — native DF for vectorized Arrow performance.
        // Null-type and cross-type cases are handled by apply_type_coercion;
        // CypherValue (LargeBinary) operands are routed to UDFs by the physical compiler.
        BinaryOp::Eq => Ok(left.eq(right)),
        BinaryOp::NotEq => Ok(left.not_eq(right)),
        BinaryOp::Lt => Ok(left.lt(right)),
        BinaryOp::LtEq => Ok(left.lt_eq(right)),
        BinaryOp::Gt => Ok(left.gt(right)),
        BinaryOp::GtEq => Ok(left.gt_eq(right)),

        // Boolean operators
        BinaryOp::And => Ok(left.and(right)),
        BinaryOp::Or => Ok(left.or(right)),
        BinaryOp::Xor => {
            // Use UDF for 3-valued XOR logic (null propagation)
            Ok(dummy_udf_expr("_cypher_xor", vec![left, right]))
        }

        // Arithmetic operators
        BinaryOp::Add => {
            if is_string_literal(&left) || is_string_literal(&right) {
                Ok(datafusion::functions::string::expr_fn::concat(vec![
                    left, right,
                ]))
            } else if is_list_expr(&left) || is_list_expr(&right) {
                Ok(dummy_udf_expr("_cypher_list_concat", vec![left, right]))
            } else {
                Ok(left + right)
            }
        }
        BinaryOp::Sub => Ok(left - right),
        BinaryOp::Mul => Ok(left * right),
        BinaryOp::Div => Ok(left / right),
        BinaryOp::Mod => Ok(left % right),
        BinaryOp::Pow => {
            // Cast operands to Float64 to prevent integer overflow panics
            // and ensure Float return type per Cypher semantics.
            let left_f = datafusion::logical_expr::cast(
                left,
                datafusion::arrow::datatypes::DataType::Float64,
            );
            let right_f = datafusion::logical_expr::cast(
                right,
                datafusion::arrow::datatypes::DataType::Float64,
            );
            Ok(datafusion::functions::math::expr_fn::power(left_f, right_f))
        }

        // String operators - use Cypher UDFs for safe type handling
        BinaryOp::Contains => Ok(dummy_udf_expr("_cypher_contains", vec![left, right])),
        BinaryOp::StartsWith => Ok(dummy_udf_expr("_cypher_starts_with", vec![left, right])),
        BinaryOp::EndsWith => Ok(dummy_udf_expr("_cypher_ends_with", vec![left, right])),

        BinaryOp::Regex => {
            Ok(datafusion::functions::expr_fn::regexp_match(left, right, None).is_not_null())
        }

        BinaryOp::ApproxEq => Err(anyhow!(
            "Vector similarity operator (~=) cannot be pushed down to DataFusion"
        )),
    }
}

/// Require at least N arguments, returning an error with the function name if insufficient.
/// When `count` is 1, uses singular "argument" in the error message.
fn require_args(df_args: &[DfExpr], count: usize, func_name: &str) -> Result<()> {
    if df_args.len() < count {
        let noun = if count == 1 { "argument" } else { "arguments" };
        return Err(anyhow!("{} requires {} {}", func_name, count, noun));
    }
    Ok(())
}

/// Shorthand for `require_args(df_args, 1, func_name)`.
fn require_arg(df_args: &[DfExpr], func_name: &str) -> Result<()> {
    require_args(df_args, 1, func_name)
}

/// Get the first argument, cloned.
fn first_arg(df_args: &[DfExpr]) -> DfExpr {
    df_args[0].clone()
}

/// Create a cast expression to the specified data type.
fn cast_expr(expr: DfExpr, data_type: datafusion::arrow::datatypes::DataType) -> DfExpr {
    DfExpr::Cast(datafusion::logical_expr::Cast {
        expr: Box::new(expr),
        data_type,
    })
}

/// Apply a single-argument math function with Float64 casting.
///
/// This is a common pattern for trig functions and other math operations
/// that require Float64 input for Int64 compatibility.
fn apply_unary_math_f64<F>(df_args: &[DfExpr], func_name: &str, math_fn: F) -> Result<DfExpr>
where
    F: FnOnce(DfExpr) -> DfExpr,
{
    require_arg(df_args, func_name)?;
    Ok(math_fn(cast_expr(
        first_arg(df_args),
        datafusion::arrow::datatypes::DataType::Float64,
    )))
}

/// Apply DISTINCT modifier to an aggregate expression if needed.
fn maybe_distinct(expr: DfExpr, distinct: bool, name: &str) -> Result<DfExpr> {
    if distinct {
        expr.distinct()
            .build()
            .map_err(|e| anyhow!("Failed to build {} DISTINCT: {}", name, e))
    } else {
        Ok(expr)
    }
}

/// Try to translate an aggregate function (COUNT, SUM, AVG, MIN, MAX, COLLECT).
fn translate_aggregate_function(
    name_upper: &str,
    df_args: &[DfExpr],
    distinct: bool,
) -> Option<Result<DfExpr>> {
    // Helper macro: check arg count, return Some(Err) on failure
    macro_rules! check1 {
        ($name:expr) => {
            if let Err(e) = require_arg(df_args, $name) {
                return Some(Err(e));
            }
        };
    }

    match name_upper {
        "COUNT" => {
            let expr = if df_args.is_empty() {
                datafusion::functions_aggregate::count::count(lit(1i64))
            } else {
                datafusion::functions_aggregate::count::count(first_arg(df_args))
            };
            Some(maybe_distinct(expr, distinct, "COUNT"))
        }
        "SUM" => {
            check1!("SUM");
            let expr = datafusion::functions_aggregate::sum::sum(first_arg(df_args));
            Some(maybe_distinct(expr, distinct, "SUM"))
        }
        "AVG" => {
            check1!("AVG");
            let expr = datafusion::functions_aggregate::average::avg(first_arg(df_args));
            Some(maybe_distinct(expr, distinct, "AVG"))
        }
        "MIN" => {
            check1!("MIN");
            Some(Ok(datafusion::functions_aggregate::min_max::min(
                first_arg(df_args),
            )))
        }
        "MAX" => {
            check1!("MAX");
            Some(Ok(datafusion::functions_aggregate::min_max::max(
                first_arg(df_args),
            )))
        }
        "COLLECT" => {
            check1!("COLLECT");
            Some(Ok(datafusion::functions_aggregate::array_agg::array_agg(
                first_arg(df_args),
            )))
        }
        _ => None,
    }
}

/// Try to translate a string function.
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_string_function(name_upper: &str, df_args: Vec<DfExpr>) -> Option<Result<DfExpr>> {
    // Helper macros to reduce boilerplate in argument validation
    macro_rules! check1 {
        ($name:expr) => {
            if let Err(e) = require_arg(&df_args, $name) {
                return Some(Err(e));
            }
        };
    }
    macro_rules! check_n {
        ($n:expr, $name:expr) => {
            if let Err(e) = require_args(&df_args, $n, $name) {
                return Some(Err(e));
            }
        };
    }

    match name_upper {
        "TOSTRING" => {
            check1!("toString");
            Some(Ok(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Utf8,
            )))
        }
        "TOINTEGER" | "TOINT" => {
            check1!("toInteger");
            Some(Ok(dummy_udf_expr("toInteger", df_args)))
        }
        "TOFLOAT" => {
            check1!("toFloat");
            Some(Ok(dummy_udf_expr("toFloat", df_args)))
        }
        "TOBOOLEAN" | "TOBOOL" => {
            check1!("toBoolean");
            Some(Ok(dummy_udf_expr("toBoolean", df_args)))
        }
        "UPPER" | "TOUPPER" => {
            check1!("upper");
            Some(Ok(datafusion::functions::string::expr_fn::upper(
                first_arg(&df_args),
            )))
        }
        "LOWER" | "TOLOWER" => {
            check1!("lower");
            Some(Ok(datafusion::functions::string::expr_fn::lower(
                first_arg(&df_args),
            )))
        }
        "SUBSTRING" => {
            check_n!(2, "substring");
            // Cypher is 0-based, DataFusion substr is 1-based
            let substr_expr = datafusion::functions::unicode::expr_fn::substr(
                df_args[0].clone(),
                df_args[1].clone() + lit(1i64),
            );
            if df_args.len() == 3 {
                Some(Ok(datafusion::functions::unicode::expr_fn::left(
                    substr_expr,
                    df_args[2].clone(),
                )))
            } else {
                Some(Ok(substr_expr))
            }
        }
        "TRIM" => {
            check1!("TRIM");
            Some(Ok(datafusion::functions::string::expr_fn::btrim(vec![
                first_arg(&df_args),
            ])))
        }
        "LTRIM" => {
            check1!("LTRIM");
            Some(Ok(datafusion::functions::string::expr_fn::ltrim(vec![
                first_arg(&df_args),
            ])))
        }
        "RTRIM" => {
            check1!("RTRIM");
            Some(Ok(datafusion::functions::string::expr_fn::rtrim(vec![
                first_arg(&df_args),
            ])))
        }
        "LEFT" => {
            check_n!(2, "left");
            Some(Ok(datafusion::functions::unicode::expr_fn::left(
                df_args[0].clone(),
                df_args[1].clone(),
            )))
        }
        "RIGHT" => {
            check_n!(2, "right");
            Some(Ok(datafusion::functions::unicode::expr_fn::right(
                df_args[0].clone(),
                df_args[1].clone(),
            )))
        }
        "REPLACE" => {
            check_n!(3, "replace");
            Some(Ok(datafusion::functions::string::expr_fn::replace(
                df_args[0].clone(),
                df_args[1].clone(),
                df_args[2].clone(),
            )))
        }
        "REVERSE" => {
            check1!("reverse");
            Some(Ok(dummy_udf_expr("_cypher_reverse", df_args)))
        }
        "SPLIT" => {
            check_n!(2, "split");
            Some(Ok(datafusion::functions_nested::expr_fn::string_to_array(
                df_args[0].clone(),
                df_args[1].clone(),
                lit(datafusion::common::ScalarValue::Utf8(None)),
            )))
        }
        "SIZE" | "LENGTH" => {
            check1!(name_upper);
            Some(Ok(dummy_udf_expr("_cypher_size", df_args)))
        }
        _ => None,
    }
}

/// Try to translate a math function.
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_math_function(name_upper: &str, df_args: &[DfExpr]) -> Option<Result<DfExpr>> {
    use datafusion::functions::math::expr_fn;

    // Helper macros for argument validation
    macro_rules! check1 {
        ($name:expr) => {
            if let Err(e) = require_arg(df_args, $name) {
                return Some(Err(e));
            }
        };
    }
    macro_rules! check_n {
        ($n:expr, $name:expr) => {
            if let Err(e) = require_args(df_args, $n, $name) {
                return Some(Err(e));
            }
        };
    }

    // Helper: apply a unary math function that takes a single Float64 arg
    let unary_f64 =
        |name: &str, f: fn(DfExpr) -> DfExpr| Some(apply_unary_math_f64(df_args, name, f));

    match name_upper {
        "ABS" => {
            check1!("abs");
            Some(Ok(expr_fn::abs(first_arg(df_args))))
        }
        "CEIL" | "CEILING" => {
            check1!("ceil");
            Some(Ok(expr_fn::ceil(first_arg(df_args))))
        }
        "FLOOR" => {
            check1!("floor");
            Some(Ok(expr_fn::floor(first_arg(df_args))))
        }
        "ROUND" => {
            check1!("round");
            let args = if df_args.len() == 1 {
                vec![first_arg(df_args)]
            } else {
                vec![df_args[0].clone(), df_args[1].clone()]
            };
            Some(Ok(expr_fn::round(args)))
        }
        "SIGN" => unary_f64("sign", expr_fn::signum),
        "SQRT" => unary_f64("sqrt", expr_fn::sqrt),
        "LOG" | "LN" => unary_f64("log", expr_fn::ln),
        "LOG10" => unary_f64("log10", expr_fn::log10),
        "EXP" => unary_f64("exp", expr_fn::exp),
        "SIN" => unary_f64("sin", expr_fn::sin),
        "COS" => unary_f64("cos", expr_fn::cos),
        "TAN" => unary_f64("tan", expr_fn::tan),
        "ASIN" => unary_f64("asin", expr_fn::asin),
        "ACOS" => unary_f64("acos", expr_fn::acos),
        "ATAN" => unary_f64("atan", expr_fn::atan),
        "ATAN2" => {
            check_n!(2, "atan2");
            let cast_f64 =
                |e: DfExpr| cast_expr(e, datafusion::arrow::datatypes::DataType::Float64);
            Some(Ok(expr_fn::atan2(
                cast_f64(df_args[0].clone()),
                cast_f64(df_args[1].clone()),
            )))
        }
        "RAND" | "RANDOM" => Some(Ok(expr_fn::random())),
        "E" if df_args.is_empty() => Some(Ok(lit(std::f64::consts::E))),
        "PI" if df_args.is_empty() => Some(Ok(lit(std::f64::consts::PI))),
        _ => None,
    }
}

/// Try to translate a temporal function.
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_temporal_function(
    name_upper: &str,
    name: &str,
    df_args: Vec<DfExpr>,
) -> Option<Result<DfExpr>> {
    match name_upper {
        "DATE"
        | "TIME"
        | "LOCALTIME"
        | "LOCALDATETIME"
        | "DATETIME"
        | "DURATION"
        | "YEAR"
        | "MONTH"
        | "DAY"
        | "HOUR"
        | "MINUTE"
        | "SECOND"
        | "DURATION.BETWEEN"
        | "DURATION.INMONTHS"
        | "DURATION.INDAYS"
        | "DURATION.INSECONDS"
        | "DATETIME.FROMEPOCH"
        | "DATETIME.FROMEPOCHMILLIS"
        | "DATE.TRUNCATE"
        | "TIME.TRUNCATE"
        | "DATETIME.TRUNCATE"
        | "LOCALDATETIME.TRUNCATE"
        | "LOCALTIME.TRUNCATE"
        | "DATETIME.TRANSACTION"
        | "DATETIME.STATEMENT"
        | "DATETIME.REALTIME"
        | "DATE.TRANSACTION"
        | "DATE.STATEMENT"
        | "DATE.REALTIME"
        | "TIME.TRANSACTION"
        | "TIME.STATEMENT"
        | "TIME.REALTIME"
        | "LOCALTIME.TRANSACTION"
        | "LOCALTIME.STATEMENT"
        | "LOCALTIME.REALTIME"
        | "LOCALDATETIME.TRANSACTION"
        | "LOCALDATETIME.STATEMENT"
        | "LOCALDATETIME.REALTIME" => Some(Ok(dummy_udf_expr(name, df_args))),
        _ => None,
    }
}

/// Try to translate a list function (HEAD, LAST, TAIL, RANGE).
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_list_function(name_upper: &str, df_args: Vec<DfExpr>) -> Option<Result<DfExpr>> {
    match name_upper {
        "HEAD" => {
            if let Err(e) = require_arg(&df_args, "head") {
                return Some(Err(e));
            }
            Some(Ok(datafusion::functions_nested::expr_fn::array_element(
                first_arg(&df_args),
                lit(1i64),
            )))
        }
        "LAST" => {
            if let Err(e) = require_arg(&df_args, "last") {
                return Some(Err(e));
            }
            Some(Ok(datafusion::functions_nested::expr_fn::array_element(
                first_arg(&df_args),
                lit(-1i64),
            )))
        }
        "TAIL" => {
            if let Err(e) = require_arg(&df_args, "tail") {
                return Some(Err(e));
            }
            Some(Ok(dummy_udf_expr("_cypher_tail", df_args)))
        }
        "RANGE" => {
            if let Err(e) = require_args(&df_args, 2, "range") {
                return Some(Err(e));
            }
            Some(Ok(dummy_udf_expr("range", df_args)))
        }
        _ => None,
    }
}

/// Try to translate a graph function (ID, LABELS, KEYS, TYPE, PROPERTIES, etc.).
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_graph_function(
    name_upper: &str,
    name: &str,
    df_args: Vec<DfExpr>,
    args: &[Expr],
    context: Option<&TranslationContext>,
) -> Option<Result<DfExpr>> {
    match name_upper {
        "ID" => {
            // When called with a bare variable (ID(n)), rewrite to the internal
            // identity column reference (_vid for nodes, _eid for edges).
            if let Some(Expr::Variable(var)) = args.first() {
                let id_suffix = if let Some(ctx) = context
                    && ctx.variable_kinds.get(var) == Some(&VariableKind::Edge)
                {
                    COL_EID
                } else {
                    COL_VID
                };
                Some(Ok(DfExpr::Column(Column::from_name(format!(
                    "{}.{}",
                    var, id_suffix
                )))))
            } else {
                Some(Ok(dummy_udf_expr("id", df_args)))
            }
        }
        "LABELS" | "KEYS" => {
            // labels(n)/keys(n) expect the struct column representing the whole entity.
            // The struct is built by add_structural_projection() and exposed as Column("n").
            // df_args already has the correct resolution via the Variable case which
            // returns Column("n") when variable_kinds context is present.
            Some(Ok(dummy_udf_expr(name, df_args)))
        }
        "TYPE" => {
            // type(r) returns the edge type name as a string.
            // When context provides the edge type via variable_labels, emit a string literal.
            if let Some(Expr::Variable(var)) = args.first()
                && let Some(ctx) = context
                && let Some(label) = ctx.variable_labels.get(var)
            {
                return Some(Ok(lit(label.clone())));
            }
            // Fallback: use _type column from traverse output (schemaless edges)
            if let Some(Expr::Variable(var)) = args.first() {
                return Some(Ok(DfExpr::Column(Column::from_name(format!(
                    "{}.{}",
                    var, COL_TYPE
                )))));
            }
            Some(Ok(dummy_udf_expr("type", df_args)))
        }
        "PROPERTIES" => {
            // properties(n) receives the struct column representing the entity,
            // same as keys(n). The struct is built by add_structural_projection().
            Some(Ok(dummy_udf_expr(name, df_args)))
        }
        "UNI.TEMPORAL.VALIDAT" => {
            // Expand uni.temporal.validAt(entity, start_prop, end_prop, timestamp)
            // into: entity.start_prop <= timestamp AND (entity.end_prop IS NULL OR entity.end_prop > timestamp)
            if let (
                Some(Expr::Variable(var)),
                Some(Expr::Literal(CypherLiteral::String(start_prop))),
                Some(Expr::Literal(CypherLiteral::String(end_prop))),
                Some(ts_expr),
            ) = (args.first(), args.get(1), args.get(2), args.get(3))
            {
                let start_col =
                    DfExpr::Column(Column::from_name(format!("{}.{}", var, start_prop)));
                let end_col = DfExpr::Column(Column::from_name(format!("{}.{}", var, end_prop)));
                let ts = match cypher_expr_to_df(ts_expr, context) {
                    Ok(ts) => ts,
                    Err(e) => return Some(Err(e)),
                };

                // start_prop <= timestamp
                let start_check = start_col.lt_eq(ts.clone());
                // end_prop IS NULL OR end_prop > timestamp
                let end_null = DfExpr::IsNull(Box::new(end_col.clone()));
                let end_after = end_col.gt(ts);
                let end_check = end_null.or(end_after);

                Some(Ok(start_check.and(end_check)))
            } else {
                // Fallback: pass through as dummy UDF
                Some(Ok(dummy_udf_expr(name, df_args)))
            }
        }
        "NODES" | "RELATIONSHIPS" => Some(Ok(dummy_udf_expr(name, df_args))),
        "HASLABEL" => {
            if let Err(e) = require_args(&df_args, 2, "hasLabel") {
                return Some(Err(e));
            }
            // First arg should be a variable, second should be the label string
            if let Some(Expr::Variable(var)) = args.first() {
                if let Some(Expr::Literal(CypherLiteral::String(label))) = args.get(1) {
                    // Translate to: array_has({var}._labels, '{label}')
                    let labels_col =
                        DfExpr::Column(Column::from_name(format!("{}.{}", var, COL_LABELS)));
                    Some(Ok(datafusion::functions_nested::expr_fn::array_has(
                        labels_col,
                        lit(label.clone()),
                    )))
                } else {
                    // Can't translate with non-string label - force fallback
                    Some(Err(anyhow::anyhow!(
                        "hasLabel requires string literal as second argument for DataFusion translation"
                    )))
                }
            } else {
                // Can't translate without variable - force fallback
                Some(Err(anyhow::anyhow!(
                    "hasLabel requires variable as first argument for DataFusion translation"
                )))
            }
        }
        _ => None,
    }
}

/// Translate a function call to DataFusion.
fn translate_function_call(
    name: &str,
    args: &[Expr],
    distinct: bool,
    context: Option<&TranslationContext>,
) -> Result<DfExpr> {
    let df_args: Vec<DfExpr> = args
        .iter()
        .map(|arg| cypher_expr_to_df(arg, context))
        .collect::<Result<Vec<_>>>()?;

    let name_upper = name.to_uppercase();

    // Try each function category in order
    if let Some(result) = translate_aggregate_function(&name_upper, &df_args, distinct) {
        return result;
    }

    if let Some(result) = translate_string_function(&name_upper, df_args.clone()) {
        return result;
    }

    if let Some(result) = translate_math_function(&name_upper, &df_args) {
        return result;
    }

    if let Some(result) = translate_temporal_function(&name_upper, name, df_args.clone()) {
        return result;
    }

    if let Some(result) = translate_list_function(&name_upper, df_args.clone()) {
        return result;
    }

    if let Some(result) =
        translate_graph_function(&name_upper, name, df_args.clone(), args, context)
    {
        return result;
    }

    // Null handling functions (standalone)
    match name_upper.as_str() {
        "COALESCE" => {
            require_arg(&df_args, "coalesce")?;
            return Ok(datafusion::functions::expr_fn::coalesce(df_args));
        }
        "NULLIF" => {
            require_args(&df_args, 2, "nullif")?;
            return Ok(datafusion::functions::expr_fn::nullif(
                df_args[0].clone(),
                df_args[1].clone(),
            ));
        }
        _ => {}
    }

    // Unknown function - try as a UDF
    Ok(dummy_udf_expr(name, df_args))
}

/// Dummy UDF placeholder for graph-specific functions.
///
/// These functions should be properly registered in the SessionContext.
/// This is a placeholder that will fail at execution time if not replaced.
#[derive(Debug)]
struct DummyUdf {
    name: String,
    signature: datafusion::logical_expr::Signature,
}

impl DummyUdf {
    fn new(name: String) -> Self {
        Self {
            name,
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for DummyUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for DummyUdf {}

impl Hash for DummyUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

/// Helper to create a DummyUdf wrapped in a ScalarFunction expression.
fn dummy_udf_expr(name: &str, args: Vec<DfExpr>) -> DfExpr {
    DfExpr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
        func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
            DummyUdf::new(name.to_lowercase()),
        )),
        args,
    })
}

impl datafusion::logical_expr::ScalarUDFImpl for DummyUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        // Return null for placeholder - real UDF should override
        Ok(datafusion::arrow::datatypes::DataType::Null)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Err(datafusion::error::DataFusionError::Plan(format!(
            "UDF '{}' is not registered. Register it via SessionContext.",
            self.name
        )))
    }
}

/// Collect all property accesses from an expression tree.
///
/// Returns a list of (variable, property) pairs needed for column projection.
pub fn collect_properties(expr: &Expr) -> Vec<(String, String)> {
    let mut properties = Vec::new();
    collect_properties_recursive(expr, &mut properties);
    properties.sort();
    properties.dedup();
    properties
}

fn collect_properties_recursive(expr: &Expr, properties: &mut Vec<(String, String)>) {
    match expr {
        Expr::PatternComprehension { .. } => {}
        Expr::Property(base, prop) => {
            if let Ok(var_name) = extract_variable_name(base) {
                properties.push((var_name, prop.clone()));
            }
            collect_properties_recursive(base, properties);
        }
        Expr::ArrayIndex { array, index } => {
            if let Ok(var_name) = extract_variable_name(array)
                && let Expr::Literal(CypherLiteral::String(prop_name)) = index.as_ref()
            {
                properties.push((var_name, prop_name.clone()));
            }
            collect_properties_recursive(array, properties);
            collect_properties_recursive(index, properties);
        }
        Expr::ArraySlice { array, start, end } => {
            collect_properties_recursive(array, properties);
            if let Some(s) = start {
                collect_properties_recursive(s, properties);
            }
            if let Some(e) = end {
                collect_properties_recursive(e, properties);
            }
        }
        Expr::List(items) => {
            for item in items {
                collect_properties_recursive(item, properties);
            }
        }
        Expr::Map(entries) => {
            for (_, value) in entries {
                collect_properties_recursive(value, properties);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::IsUnique(inner) => {
            collect_properties_recursive(inner, properties);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_properties_recursive(arg, properties);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_properties_recursive(left, properties);
            collect_properties_recursive(right, properties);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_properties_recursive(expr, properties);
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = expr {
                collect_properties_recursive(e, properties);
            }
            for (when_e, then_e) in when_then {
                collect_properties_recursive(when_e, properties);
                collect_properties_recursive(then_e, properties);
            }
            if let Some(e) = else_expr {
                collect_properties_recursive(e, properties);
            }
        }
        Expr::Reduce {
            init, list, expr, ..
        } => {
            collect_properties_recursive(init, properties);
            collect_properties_recursive(list, properties);
            collect_properties_recursive(expr, properties);
        }
        Expr::Quantifier {
            list, predicate, ..
        } => {
            collect_properties_recursive(list, properties);
            collect_properties_recursive(predicate, properties);
        }
        Expr::ListComprehension {
            list,
            where_clause,
            map_expr,
            ..
        } => {
            collect_properties_recursive(list, properties);
            if let Some(filter) = where_clause {
                collect_properties_recursive(filter, properties);
            }
            collect_properties_recursive(map_expr, properties);
        }
        Expr::In { expr, list } => {
            collect_properties_recursive(expr, properties);
            collect_properties_recursive(list, properties);
        }
        Expr::ValidAt {
            entity, timestamp, ..
        } => {
            collect_properties_recursive(entity, properties);
            collect_properties_recursive(timestamp, properties);
        }
        Expr::MapProjection { base, items } => {
            collect_properties_recursive(base, properties);
            for item in items {
                match item {
                    uni_cypher::ast::MapProjectionItem::Property(prop) => {
                        if let Ok(var_name) = extract_variable_name(base) {
                            properties.push((var_name, prop.clone()));
                        }
                    }
                    uni_cypher::ast::MapProjectionItem::AllProperties => {
                        if let Ok(var_name) = extract_variable_name(base) {
                            properties.push((var_name, "*".to_string()));
                        }
                    }
                    uni_cypher::ast::MapProjectionItem::LiteralEntry(_, expr) => {
                        collect_properties_recursive(expr, properties);
                    }
                    uni_cypher::ast::MapProjectionItem::Variable(_) => {}
                }
            }
        }
        // Terminal nodes and subqueries (which have their own scope)
        Expr::Wildcard | Expr::Variable(_) | Expr::Parameter(_) | Expr::Literal(_) => {}
        Expr::Exists { .. } | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {}
    }
}

/// Returns the wider of two numeric DataTypes for type coercion.
///
/// Follows standard numeric promotion rules:
/// - Any Float type wins over Int types
/// - Float64 > Float32
/// - Int64 > Int32 > Int16 > Int8
pub fn wider_numeric_type(
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

/// Check if an expression contains a division operator anywhere in its tree.
/// Used to detect expressions that may produce NaN (e.g., 0.0/0.0).
fn contains_division(expr: &DfExpr) -> bool {
    match expr {
        DfExpr::BinaryExpr(b) => {
            b.op == datafusion::logical_expr::Operator::Divide
                || contains_division(&b.left)
                || contains_division(&b.right)
        }
        DfExpr::Cast(c) => contains_division(&c.expr),
        DfExpr::TryCast(c) => contains_division(&c.expr),
        _ => false,
    }
}

/// Apply type coercion to a DataFusion expression.
///
/// Resolves numeric type mismatches (e.g., Int32 vs Int64, Boolean vs Int64)
/// by inserting explicit CAST nodes. This is needed because our schema may
/// declare properties as one numeric type while literals are a different type.
pub fn apply_type_coercion(expr: &DfExpr, schema: &datafusion::common::DFSchema) -> Result<DfExpr> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::ExprSchemable;
    use datafusion::logical_expr::Operator;

    match expr {
        DfExpr::BinaryExpr(binary) => {
            let left = apply_type_coercion(&binary.left, schema)?;
            let right = apply_type_coercion(&binary.right, schema)?;

            // For comparison and arithmetic operators, coerce numeric types
            let is_comparison = matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            );

            let is_arithmetic = matches!(
                binary.op,
                Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulo
            );

            // AND/OR with Null or Utf8 operands: cast to Boolean so Arrow kernel doesn't crash.
            // UNWIND over json_encoded columns can produce Utf8 "true"/"false" values.
            if matches!(binary.op, Operator::And | Operator::Or) {
                let left_type = left.get_type(schema).ok();
                let right_type = right.get_type(schema).ok();
                let left_needs_cast = left_type.as_ref().is_some_and(|t| {
                    t.is_null() || matches!(t, DataType::Utf8 | DataType::LargeUtf8)
                });
                let right_needs_cast = right_type.as_ref().is_some_and(|t| {
                    t.is_null() || matches!(t, DataType::Utf8 | DataType::LargeUtf8)
                });
                if left_needs_cast || right_needs_cast {
                    let coerced_left = if left_needs_cast {
                        datafusion::logical_expr::cast(left, DataType::Boolean)
                    } else {
                        left
                    };
                    let coerced_right = if right_needs_cast {
                        datafusion::logical_expr::cast(right, DataType::Boolean)
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

            if is_comparison || is_arithmetic {
                let left_type = left.get_type(schema).ok();
                let right_type = right.get_type(schema).ok();

                // String + anything → concat (Cypher uses + for concatenation)
                // EXCEPT when involving LargeBinary (CypherValue), which needs special UDF handling
                if binary.op == Operator::Plus
                    && let (Some(lt), Some(rt)) = (&left_type, &right_type)
                {
                    let left_is_lb = matches!(lt, DataType::LargeBinary);
                    let right_is_lb = matches!(rt, DataType::LargeBinary);
                    let left_is_string = matches!(lt, DataType::Utf8 | DataType::LargeUtf8);
                    let right_is_string = matches!(rt, DataType::Utf8 | DataType::LargeUtf8);
                    if (left_is_string || right_is_string) && !left_is_lb && !right_is_lb {
                        return Ok(datafusion::functions::string::expr_fn::concat(vec![
                            left, right,
                        ]));
                    }
                }

                // Handle Null-typed operands: cast the null side to match the
                // other operand's type so Arrow doesn't reject the type pair.
                if let (Some(lt), Some(rt)) = (&left_type, &right_type) {
                    let left_is_null = lt.is_null();
                    let right_is_null = rt.is_null();
                    if left_is_null && right_is_null {
                        // Both null: result is always null for comparisons
                        return Ok(lit(ScalarValue::Boolean(None)));
                    }
                    if left_is_null || right_is_null {
                        let target = if left_is_null { rt } else { lt };
                        let coerced_left = if left_is_null {
                            datafusion::logical_expr::cast(left, target.clone())
                        } else {
                            left
                        };
                        let coerced_right = if right_is_null {
                            datafusion::logical_expr::cast(right, target.clone())
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

                // 0. LargeBinary (CypherValue) handling — before type-mismatch check since
                //    both-LB is same-type but still needs special handling
                if let (Some(lt), Some(rt)) = (&left_type, &right_type) {
                    let left_is_lb = matches!(lt, DataType::LargeBinary);
                    let right_is_lb = matches!(rt, DataType::LargeBinary);

                    // List concatenation / append via Plus operator
                    if binary.op == Operator::Plus {
                        // Both CypherValue → list concat (both could be CypherValue lists)
                        if left_is_lb && right_is_lb {
                            return Ok(dummy_udf_expr("_cypher_list_concat", vec![left, right]));
                        }
                        // Native List types → list concat or append
                        let left_is_native_list =
                            matches!(lt, DataType::List(_) | DataType::LargeList(_));
                        let right_is_native_list =
                            matches!(rt, DataType::List(_) | DataType::LargeList(_));
                        if left_is_native_list && right_is_native_list {
                            return Ok(dummy_udf_expr("_cypher_list_concat", vec![left, right]));
                        }
                        if left_is_native_list || right_is_native_list {
                            return Ok(dummy_udf_expr("_cypher_list_append", vec![left, right]));
                        }
                        // LB + typed scalar (e.g., LB + Int64) falls through
                        // to the CypherValue decode path below
                    }

                    if left_is_lb && right_is_lb && is_comparison {
                        // Both LargeBinary: route comparison to _cypher_* UDFs
                        let udf_name = match binary.op {
                            Operator::Eq => "_cypher_equal",
                            Operator::NotEq => "_cypher_not_equal",
                            Operator::Lt => "_cypher_lt",
                            Operator::LtEq => "_cypher_lt_eq",
                            Operator::Gt => "_cypher_gt",
                            Operator::GtEq => "_cypher_gt_eq",
                            _ => unreachable!(),
                        };
                        return Ok(dummy_udf_expr(udf_name, vec![left, right]));
                    }

                    // Mixed LB/typed comparisons: route through Cypher comparison UDFs
                    // The expression compiler will encode typed literals to CypherValue at compile time
                    if (left_is_lb || right_is_lb) && is_comparison {
                        let udf_name = match binary.op {
                            Operator::Eq => "_cypher_equal",
                            Operator::NotEq => "_cypher_not_equal",
                            Operator::Lt => "_cypher_lt",
                            Operator::LtEq => "_cypher_lt_eq",
                            Operator::Gt => "_cypher_gt",
                            Operator::GtEq => "_cypher_gt_eq",
                            _ => {
                                // Not a comparison op we handle - fall through
                                return Ok(DfExpr::BinaryExpr(binary.clone()));
                            }
                        };
                        return Ok(dummy_udf_expr(udf_name, vec![left, right]));
                    }

                    // Mixed LB/typed arithmetic: route through CypherValue arithmetic UDFs
                    if (left_is_lb || right_is_lb) && is_arithmetic {
                        let udf_name = match binary.op {
                            Operator::Plus => "_cypher_add",
                            Operator::Minus => "_cypher_sub",
                            Operator::Multiply => "_cypher_mul",
                            Operator::Divide => "_cypher_div",
                            Operator::Modulo => "_cypher_mod",
                            _ => unreachable!(),
                        };
                        return Ok(dummy_udf_expr(udf_name, vec![left, right]));
                    }

                    // Struct (map/node/edge) comparisons: route to _cypher_equal UDFs
                    // which handle identity-based comparison (_vid/_eid) and null-in-map semantics.
                    if matches!(lt, DataType::Struct(_))
                        && matches!(rt, DataType::Struct(_))
                        && is_comparison
                    {
                        let udf_name = match binary.op {
                            Operator::Eq => "_cypher_equal",
                            Operator::NotEq => "_cypher_not_equal",
                            // Cypher doesn't define ordering for maps/nodes/edges
                            _ => {
                                return Ok(lit(ScalarValue::Boolean(None)));
                            }
                        };
                        return Ok(dummy_udf_expr(udf_name, vec![left, right]));
                    }

                    // LargeBinary vs Struct comparison: route to _cypher_equal for
                    // cross-format map equality (e.g., {} encoded as CypherValue vs {k: null} as Struct)
                    if is_comparison
                        && ((matches!(lt, DataType::LargeBinary)
                            && matches!(rt, DataType::Struct(_)))
                            || (matches!(lt, DataType::Struct(_))
                                && matches!(rt, DataType::LargeBinary)))
                    {
                        let udf_name = match binary.op {
                            Operator::Eq => "_cypher_equal",
                            Operator::NotEq => "_cypher_not_equal",
                            _ => {
                                return Ok(lit(ScalarValue::Boolean(None)));
                            }
                        };
                        return Ok(dummy_udf_expr(udf_name, vec![left, right]));
                    }
                }

                // NaN-aware comparisons: when a division expression is involved,
                // route to _cypher_* UDFs which handle NaN correctly (NaN != NaN, NaN not ordered).
                if is_comparison && (contains_division(&left) || contains_division(&right)) {
                    let udf_name = match binary.op {
                        Operator::Eq => "_cypher_equal",
                        Operator::NotEq => "_cypher_not_equal",
                        Operator::Lt => "_cypher_lt",
                        Operator::LtEq => "_cypher_lt_eq",
                        Operator::Gt => "_cypher_gt",
                        Operator::GtEq => "_cypher_gt_eq",
                        _ => unreachable!(),
                    };
                    return Ok(dummy_udf_expr(udf_name, vec![left, right]));
                }

                if let (Some(lt), Some(rt)) = (&left_type, &right_type)
                    && lt != rt
                {
                    // 1. Numeric Coercion
                    if lt.is_numeric() && rt.is_numeric() {
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

                    // 2. Timestamp vs Utf8: cast Utf8 side to the Timestamp type
                    if is_comparison {
                        match (lt, rt) {
                            (
                                ts @ DataType::Timestamp(..),
                                DataType::Utf8 | DataType::LargeUtf8,
                            ) => {
                                return Ok(DfExpr::BinaryExpr(
                                    datafusion::logical_expr::expr::BinaryExpr::new(
                                        Box::new(left),
                                        binary.op,
                                        Box::new(datafusion::logical_expr::cast(right, ts.clone())),
                                    ),
                                ));
                            }
                            (
                                DataType::Utf8 | DataType::LargeUtf8,
                                ts @ DataType::Timestamp(..),
                            ) => {
                                return Ok(DfExpr::BinaryExpr(
                                    datafusion::logical_expr::expr::BinaryExpr::new(
                                        Box::new(datafusion::logical_expr::cast(left, ts.clone())),
                                        binary.op,
                                        Box::new(right),
                                    ),
                                ));
                            }
                            _ => {}
                        }
                    }

                    // 3. List comparison with different numeric inner types: coerce to wider
                    if is_comparison
                        && let (DataType::List(l_field), DataType::List(r_field)) = (lt, rt)
                    {
                        let l_inner = l_field.data_type();
                        let r_inner = r_field.data_type();
                        if l_inner.is_numeric() && r_inner.is_numeric() && l_inner != r_inner {
                            let target_inner = wider_numeric_type(l_inner, r_inner);
                            let target_type =
                                DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                                    "item",
                                    target_inner,
                                    true,
                                )));
                            let coerced_left =
                                datafusion::logical_expr::cast(left, target_type.clone());
                            let coerced_right = datafusion::logical_expr::cast(right, target_type);
                            return Ok(DfExpr::BinaryExpr(
                                datafusion::logical_expr::expr::BinaryExpr::new(
                                    Box::new(coerced_left),
                                    binary.op,
                                    Box::new(coerced_right),
                                ),
                            ));
                        }
                    }

                    // 4. Cross-Type Comparison
                    if is_comparison && !lt.is_null() && !rt.is_null() {
                        let is_list_vs_nonlist = match (lt, rt) {
                            // List vs non-list is always incompatible
                            (DataType::List(_) | DataType::LargeList(_), other)
                            | (other, DataType::List(_) | DataType::LargeList(_))
                                if !matches!(other, DataType::List(_) | DataType::LargeList(_)) =>
                            {
                                true
                            }
                            _ => false,
                        };

                        if is_list_vs_nonlist {
                            // List vs non-list: ordering is null, equality is false
                            match binary.op {
                                Operator::Eq => return Ok(lit(false)),
                                Operator::NotEq => return Ok(lit(true)),
                                _ => {
                                    return Ok(lit(ScalarValue::Boolean(None)));
                                }
                            }
                        }

                        // Scalar cross-type comparison: incompatible types yield false/true/null.
                        // Skip LargeBinary (CypherValue, handled by overflow rewriting),
                        // Timestamp/Utf8 (handled above), List/Struct (handled by UDF routing below).
                        if !matches!(lt, DataType::LargeBinary)
                            && !matches!(rt, DataType::LargeBinary)
                            && !matches!(lt, DataType::List(_) | DataType::LargeList(_))
                            && !matches!(rt, DataType::List(_) | DataType::LargeList(_))
                            && !matches!(lt, DataType::Struct(_))
                            && !matches!(rt, DataType::Struct(_))
                            && !matches!(
                                (lt, rt),
                                (
                                    DataType::Timestamp(..),
                                    DataType::Utf8 | DataType::LargeUtf8
                                ) | (
                                    DataType::Utf8 | DataType::LargeUtf8,
                                    DataType::Timestamp(..)
                                )
                            )
                        {
                            match binary.op {
                                Operator::Eq => return Ok(lit(false)),
                                Operator::NotEq => return Ok(lit(true)),
                                _ => return Ok(lit(ScalarValue::Boolean(None))),
                            }
                        }
                    }
                }

                // 5. List ordering: rewrite Lt/LtEq/Gt/GtEq on lists to _cypher_list_compare UDF
                if is_comparison
                    && let (Some(lt), Some(rt)) = (&left_type, &right_type)
                    && matches!(
                        binary.op,
                        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                    )
                    && matches!(lt, DataType::List(_) | DataType::LargeList(_))
                    && matches!(rt, DataType::List(_) | DataType::LargeList(_))
                {
                    let op_str = match binary.op {
                        Operator::Lt => "lt",
                        Operator::LtEq => "lteq",
                        Operator::Gt => "gt",
                        Operator::GtEq => "gteq",
                        _ => unreachable!(),
                    };
                    return Ok(dummy_udf_expr(
                        "_cypher_list_compare",
                        vec![left, right, lit(op_str)],
                    ));
                }

                // 6. List equality: route Eq/NotEq on lists to _cypher_equal/_cypher_not_equal
                // for 3-valued null element comparison (e.g., [null] = [1] → null)
                if matches!(binary.op, Operator::Eq | Operator::NotEq)
                    && let (Some(lt), Some(rt)) = (&left_type, &right_type)
                    && matches!(lt, DataType::List(_) | DataType::LargeList(_))
                    && matches!(rt, DataType::List(_) | DataType::LargeList(_))
                {
                    let udf_name = if binary.op == Operator::Eq {
                        "_cypher_equal"
                    } else {
                        "_cypher_not_equal"
                    };
                    return Ok(dummy_udf_expr(udf_name, vec![left, right]));
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
        DfExpr::ScalarFunction(func) => {
            // Recursively coerce arguments
            let coerced_args: Vec<DfExpr> = func
                .args
                .iter()
                .map(|a| apply_type_coercion(a, schema))
                .collect::<Result<Vec<_>>>()?;

            if func.func.name().eq_ignore_ascii_case("coalesce") && coerced_args.len() > 1 {
                use datafusion::logical_expr::ExprSchemable;
                let types: Vec<_> = coerced_args
                    .iter()
                    .filter_map(|a| a.get_type(schema).ok())
                    .collect();
                let has_mixed_types = types.windows(2).any(|w| w[0] != w[1]);
                if has_mixed_types {
                    let unified_args = coerced_args
                        .into_iter()
                        .map(|a| datafusion::logical_expr::cast(a, DataType::Utf8))
                        .collect();
                    return Ok(DfExpr::ScalarFunction(
                        datafusion::logical_expr::expr::ScalarFunction {
                            func: func.func.clone(),
                            args: unified_args,
                        },
                    ));
                }
            }

            Ok(DfExpr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: func.func.clone(),
                    args: coerced_args,
                },
            ))
        }
        // CASE expression: recurse into operand, when/then pairs, and else branch
        DfExpr::Case(case) => {
            let coerced_operand = case
                .expr
                .as_ref()
                .map(|e| apply_type_coercion(e, schema).map(Box::new))
                .transpose()?;
            let coerced_when_then = case
                .when_then_expr
                .iter()
                .map(|(w, t)| {
                    let cw = apply_type_coercion(w, schema)?;
                    let ct = apply_type_coercion(t, schema)?;
                    Ok((Box::new(cw), Box::new(ct)))
                })
                .collect::<Result<Vec<_>>>()?;
            let coerced_else = case
                .else_expr
                .as_ref()
                .map(|e| apply_type_coercion(e, schema).map(Box::new))
                .transpose()?;
            Ok(DfExpr::Case(datafusion::logical_expr::expr::Case {
                expr: coerced_operand,
                when_then_expr: coerced_when_then,
                else_expr: coerced_else,
            }))
        }
        // NOT: recurse into inner expression and cast Null/Utf8/LargeBinary to Boolean
        DfExpr::Not(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            let inner_type = coerced_inner.get_type(schema).ok();
            let final_inner = if inner_type
                .as_ref()
                .is_some_and(|t| t.is_null() || matches!(t, DataType::Utf8 | DataType::LargeUtf8))
            {
                datafusion::logical_expr::cast(coerced_inner, DataType::Boolean)
            } else if inner_type
                .as_ref()
                .is_some_and(|t| matches!(t, DataType::LargeBinary))
            {
                dummy_udf_expr("_cv_to_bool", vec![coerced_inner])
            } else {
                coerced_inner
            };
            Ok(DfExpr::Not(Box::new(final_inner)))
        }
        // IS NULL / IS NOT NULL: recurse into inner expression
        DfExpr::IsNull(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            Ok(coerced_inner.is_null())
        }
        DfExpr::IsNotNull(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            Ok(coerced_inner.is_not_null())
        }
        // Negation: recurse into inner expression
        DfExpr::Negative(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            Ok(DfExpr::Negative(Box::new(coerced_inner)))
        }
        // Cast: recurse into inner expression
        DfExpr::Cast(cast) => {
            let coerced_inner = apply_type_coercion(&cast.expr, schema)?;
            Ok(DfExpr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(coerced_inner),
                cast.data_type.clone(),
            )))
        }
        DfExpr::TryCast(cast) => {
            let coerced_inner = apply_type_coercion(&cast.expr, schema)?;
            Ok(DfExpr::TryCast(datafusion::logical_expr::TryCast::new(
                Box::new(coerced_inner),
                cast.data_type.clone(),
            )))
        }
        // Alias: recurse into inner expression
        DfExpr::Alias(alias) => {
            let coerced_inner = apply_type_coercion(&alias.expr, schema)?;
            Ok(coerced_inner.alias(alias.name.clone()))
        }
        // For other expression types, return as-is
        _ => Ok(expr.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_literal_translation() {
        let expr = Expr::Literal(CypherLiteral::Integer(42));
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{:?}", result);
        // Check that it's a literal with value 42
        assert!(s.contains("Literal"));
        assert!(s.contains("Int64(42)"));
    }

    #[test]
    fn test_property_access_no_context_uses_index() {
        // Without context, variable is not a known graph entity → index UDF
        let expr = Expr::Property(Box::new(Expr::Variable("n".to_string())), "age".to_string());
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("index"),
            "expected index UDF for non-graph variable, got: {s}"
        );
    }

    #[test]
    fn test_comparison_operator() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "age".to_string(),
            )),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(CypherLiteral::Integer(30))),
        };
        let result = cypher_expr_to_df(&expr, None).unwrap();
        // Should produce: n.age > 30
        let s = format!("{:?}", result);
        assert!(s.contains("age"));
        assert!(s.contains("30"));
    }

    #[test]
    fn test_boolean_operators() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "age".to_string(),
                )),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(18))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "active".to_string(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(CypherLiteral::Bool(true))),
            }),
        };
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{:?}", result);
        assert!(s.contains("And"));
    }

    #[test]
    fn test_is_null() {
        let expr = Expr::IsNull(Box::new(Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "email".to_string(),
        )));
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{:?}", result);
        assert!(s.contains("IsNull"));
    }

    #[test]
    fn test_collect_properties() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Property(
                Box::new(Expr::Variable("n".to_string())),
                "name".to_string(),
            )),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Property(
                Box::new(Expr::Variable("m".to_string())),
                "name".to_string(),
            )),
        };

        let props = collect_properties(&expr);
        assert_eq!(props.len(), 2);
        assert!(props.contains(&("m".to_string(), "name".to_string())));
        assert!(props.contains(&("n".to_string(), "name".to_string())));
    }

    #[test]
    fn test_function_call() {
        let expr = Expr::FunctionCall {
            name: "count".to_string(),
            args: vec![Expr::Wildcard],
            distinct: false,
            window_spec: None,
        };
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{:?}", result);
        assert!(s.to_lowercase().contains("count"));
    }

    // ====================================================================
    // apply_type_coercion tests
    // ====================================================================

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::Operator;

    /// Build a DFSchema with the given column names and types.
    fn make_schema(cols: &[(&str, DataType)]) -> datafusion::common::DFSchema {
        let fields: Vec<_> = cols
            .iter()
            .map(|(name, dt)| Arc::new(Field::new(*name, dt.clone(), true)))
            .collect();
        let schema = Schema::new(fields);
        datafusion::common::DFSchema::try_from(schema).unwrap()
    }

    /// Check that an expression contains a specific UDF name.
    fn contains_udf(expr: &DfExpr, name: &str) -> bool {
        let s = format!("{}", expr);
        s.contains(name)
    }

    /// Check that an expression is a binary expr with the given operator.
    fn is_binary_op(expr: &DfExpr, expected_op: Operator) -> bool {
        matches!(expr, DfExpr::BinaryExpr(b) if b.op == expected_op)
    }

    #[test]
    fn test_coercion_lb_eq_int64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("i", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Eq,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(
            contains_udf(&result, "_cypher_equal"),
            "expected _cypher_equal, got: {result}"
        );
    }

    #[test]
    fn test_coercion_lb_noteq_int64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("i", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::NotEq,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_not_equal"));
    }

    #[test]
    fn test_coercion_lb_lt_int64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("i", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Lt,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_lt"));
    }

    #[test]
    fn test_coercion_lb_eq_float64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("f", DataType::Float64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Eq,
            Box::new(col("f")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_equal"));
    }

    #[test]
    fn test_coercion_lb_eq_utf8() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("s", DataType::Utf8)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Eq,
            Box::new(col("s")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_equal"));
    }

    #[test]
    fn test_coercion_lb_eq_bool() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("b", DataType::Boolean)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Eq,
            Box::new(col("b")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_equal"));
    }

    #[test]
    fn test_coercion_int64_eq_lb() {
        // Typed on LEFT, LB on RIGHT
        let schema = make_schema(&[("i", DataType::Int64), ("lb", DataType::LargeBinary)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("i")),
            Operator::Eq,
            Box::new(col("lb")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_equal"));
    }

    #[test]
    fn test_coercion_float64_gt_lb() {
        let schema = make_schema(&[("f", DataType::Float64), ("lb", DataType::LargeBinary)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("f")),
            Operator::Gt,
            Box::new(col("lb")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(contains_udf(&result, "_cypher_gt"));
    }

    #[test]
    fn test_coercion_both_lb_eq() {
        let schema = make_schema(&[
            ("lb1", DataType::LargeBinary),
            ("lb2", DataType::LargeBinary),
        ]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb1")),
            Operator::Eq,
            Box::new(col("lb2")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_equal"));
    }

    #[test]
    fn test_coercion_both_lb_lt() {
        let schema = make_schema(&[
            ("lb1", DataType::LargeBinary),
            ("lb2", DataType::LargeBinary),
        ]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb1")),
            Operator::Lt,
            Box::new(col("lb2")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_lt"));
    }

    #[test]
    fn test_coercion_both_lb_noteq() {
        let schema = make_schema(&[
            ("lb1", DataType::LargeBinary),
            ("lb2", DataType::LargeBinary),
        ]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb1")),
            Operator::NotEq,
            Box::new(col("lb2")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_not_equal"));
    }

    #[test]
    fn test_coercion_lb_plus_int64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("i", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Plus,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_add"));
    }

    #[test]
    fn test_coercion_lb_minus_int64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("i", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Minus,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_sub"));
    }

    #[test]
    fn test_coercion_lb_multiply_float64() {
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("f", DataType::Float64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Multiply,
            Box::new(col("f")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_mul"));
    }

    #[test]
    fn test_coercion_int64_plus_lb() {
        let schema = make_schema(&[("i", DataType::Int64), ("lb", DataType::LargeBinary)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("i")),
            Operator::Plus,
            Box::new(col("lb")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(contains_udf(&result, "_cypher_add"));
    }

    #[test]
    fn test_coercion_lb_plus_utf8() {
        // LargeBinary + Utf8 → should route through _cypher_add (handles string concat at runtime)
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("s", DataType::Utf8)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Plus,
            Box::new(col("s")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Should route through _cypher_add which handles string concat
        assert!(contains_udf(&result, "_cypher_add"));
    }

    #[test]
    fn test_coercion_and_null_bool() {
        let schema = make_schema(&[("b", DataType::Boolean)]);
        // Null AND Boolean
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(lit(ScalarValue::Null)),
            Operator::And,
            Box::new(col("b")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        let s = format!("{}", result);
        // Should have CAST(Null AS Boolean)
        assert!(
            s.contains("CAST") || s.contains("Boolean"),
            "expected cast to Boolean, got: {s}"
        );
        assert!(is_binary_op(&result, Operator::And));
    }

    #[test]
    fn test_coercion_bool_and_null() {
        let schema = make_schema(&[("b", DataType::Boolean)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("b")),
            Operator::And,
            Box::new(lit(ScalarValue::Null)),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(is_binary_op(&result, Operator::And));
    }

    #[test]
    fn test_coercion_or_null_bool() {
        let schema = make_schema(&[("b", DataType::Boolean)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(lit(ScalarValue::Null)),
            Operator::Or,
            Box::new(col("b")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(is_binary_op(&result, Operator::Or));
    }

    #[test]
    fn test_coercion_null_and_null() {
        let schema = make_schema(&[]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(lit(ScalarValue::Null)),
            Operator::And,
            Box::new(lit(ScalarValue::Null)),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(is_binary_op(&result, Operator::And));
    }

    #[test]
    fn test_coercion_bool_and_bool_noop() {
        let schema = make_schema(&[("a", DataType::Boolean), ("b", DataType::Boolean)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("a")),
            Operator::And,
            Box::new(col("b")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        // Should be unchanged — still a plain AND
        assert!(is_binary_op(&result, Operator::And));
        let s = format!("{}", result);
        assert!(!s.contains("CAST"), "should not contain CAST: {s}");
    }

    #[test]
    fn test_coercion_case_when_lb() {
        // CASE WHEN Col(LB) = Lit(42) THEN 'a' ELSE 'b' END
        let schema = make_schema(&[("lb", DataType::LargeBinary)]);
        let when_cond = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Eq,
            Box::new(lit(42_i64)),
        ));
        let case_expr = DfExpr::Case(datafusion::logical_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(when_cond), Box::new(lit("a")))],
            else_expr: Some(Box::new(lit("b"))),
        });
        let result = apply_type_coercion(&case_expr, &schema).unwrap();
        let s = format!("{}", result);
        // Mixed LB/typed comparisons now route to Cypher comparison UDFs
        assert!(
            s.contains("_cypher_equal"),
            "CASE WHEN should have _cypher_equal, got: {s}"
        );
    }

    #[test]
    #[ignore = "Arithmetic UDFs not yet implemented - Phase 5 optional work"]
    fn test_coercion_case_then_lb() {
        // CASE WHEN true THEN Col(LB) + 1 ELSE 0 END
        let schema = make_schema(&[("lb", DataType::LargeBinary)]);
        let then_expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Plus,
            Box::new(lit(1_i64)),
        ));
        let case_expr = DfExpr::Case(datafusion::logical_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(lit(true)), Box::new(then_expr))],
            else_expr: Some(Box::new(lit(0_i64))),
        });
        let result = apply_type_coercion(&case_expr, &schema).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("_cypher_add"),
            "CASE THEN should have _cypher_add, got: {s}"
        );
    }

    #[test]
    #[ignore = "Arithmetic UDFs not yet implemented - Phase 5 optional work"]
    fn test_coercion_case_else_lb() {
        // CASE WHEN true THEN 1 ELSE Col(LB) + 2 END
        let schema = make_schema(&[("lb", DataType::LargeBinary)]);
        let else_expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Plus,
            Box::new(lit(2_i64)),
        ));
        let case_expr = DfExpr::Case(datafusion::logical_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(lit(true)), Box::new(lit(1_i64)))],
            else_expr: Some(Box::new(else_expr)),
        });
        let result = apply_type_coercion(&case_expr, &schema).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("_cypher_add"),
            "CASE ELSE should have _cypher_add, got: {s}"
        );
    }

    #[test]
    fn test_coercion_int64_eq_int64_noop() {
        let schema = make_schema(&[("a", DataType::Int64), ("b", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("a")),
            Operator::Eq,
            Box::new(col("b")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(is_binary_op(&result, Operator::Eq));
        let s = format!("{}", result);
        assert!(
            !s.contains("_cypher_value"),
            "should not contain cypher_value decode: {s}"
        );
    }

    #[test]
    fn test_coercion_both_lb_plus() {
        // LB + LB → _cypher_list_concat
        let schema = make_schema(&[
            ("lb1", DataType::LargeBinary),
            ("lb2", DataType::LargeBinary),
        ]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb1")),
            Operator::Plus,
            Box::new(col("lb2")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(
            contains_udf(&result, "_cypher_list_concat"),
            "expected _cypher_list_concat, got: {result}"
        );
    }

    #[test]
    fn test_coercion_native_list_plus_scalar() {
        // List<Int32> + Int32 → _cypher_list_append
        let schema = make_schema(&[
            (
                "lst",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            ),
            ("i", DataType::Int32),
        ]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lst")),
            Operator::Plus,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(
            contains_udf(&result, "_cypher_list_append"),
            "expected _cypher_list_append, got: {result}"
        );
    }

    #[test]
    #[ignore = "Arithmetic UDFs not yet implemented - Phase 5 optional work"]
    fn test_coercion_lb_plus_int64_unchanged() {
        // Regression: LB + Int64 should route to _cypher_add, NOT list append
        let schema = make_schema(&[("lb", DataType::LargeBinary), ("i", DataType::Int64)]);
        let expr = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
            Box::new(col("lb")),
            Operator::Plus,
            Box::new(col("i")),
        ));
        let result = apply_type_coercion(&expr, &schema).unwrap();
        assert!(
            contains_udf(&result, "_cypher_add"),
            "expected _cypher_add, got: {result}"
        );
    }

    // ====================================================================
    // Mixed-list compilation tests
    // ====================================================================

    #[test]
    fn test_mixed_list_with_variables_compiles() {
        // A list containing a variable and mixed literals should compile via _make_cypher_list UDF
        let expr = Expr::List(vec![
            Expr::Variable("n".to_string()),
            Expr::Literal(CypherLiteral::Integer(1)),
            Expr::Literal(CypherLiteral::String("hello".to_string())),
        ]);
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("_make_cypher_list"),
            "expected _make_cypher_list UDF call, got: {s}"
        );
    }

    #[test]
    fn test_literal_only_mixed_list_uses_cv_fastpath() {
        // A list of only mixed-type literals should use the CypherValue fast path (Literal, not UDF)
        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Integer(1)),
            Expr::Literal(CypherLiteral::String("hi".to_string())),
            Expr::Literal(CypherLiteral::Bool(true)),
        ]);
        let result = cypher_expr_to_df(&expr, None).unwrap();
        assert!(
            matches!(result, DfExpr::Literal(..)),
            "expected Literal (CypherValue fast path), got: {result}"
        );
    }

    // ====================================================================
    // IN operator routing tests
    // ====================================================================

    #[test]
    fn test_in_mixed_literal_list_uses_cypher_in() {
        // `1 IN ['1', 2]` should route through _cypher_in UDF, not in_list
        let expr = Expr::In {
            expr: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            list: Box::new(Expr::List(vec![
                Expr::Literal(CypherLiteral::String("1".to_string())),
                Expr::Literal(CypherLiteral::Integer(2)),
            ])),
        };
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("_cypher_in"),
            "expected _cypher_in UDF for mixed-type IN list, got: {s}"
        );
    }

    #[test]
    fn test_in_homogeneous_literal_list_uses_cypher_in() {
        // `1 IN [2, 3]` should also route through _cypher_in UDF
        let expr = Expr::In {
            expr: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            list: Box::new(Expr::List(vec![
                Expr::Literal(CypherLiteral::Integer(2)),
                Expr::Literal(CypherLiteral::Integer(3)),
            ])),
        };
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("_cypher_in"),
            "expected _cypher_in UDF for homogeneous IN list, got: {s}"
        );
    }

    #[test]
    fn test_in_list_with_variables_uses_make_cypher_list() {
        // `1 IN [x, 2]` should use _make_cypher_list + _cypher_in
        let expr = Expr::In {
            expr: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            list: Box::new(Expr::List(vec![
                Expr::Variable("x".to_string()),
                Expr::Literal(CypherLiteral::Integer(2)),
            ])),
        };
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("_cypher_in"),
            "expected _cypher_in UDF, got: {s}"
        );
        assert!(
            s.contains("_make_cypher_list"),
            "expected _make_cypher_list for variable-containing list, got: {s}"
        );
    }

    // ====================================================================
    // Property access routing tests
    // ====================================================================

    #[test]
    fn test_property_on_graph_entity_uses_column() {
        // When context marks `n` as a Node, property access should use flat column
        let mut ctx = TranslationContext::new();
        ctx.variable_kinds
            .insert("n".to_string(), VariableKind::Node);

        let expr = Expr::Property(
            Box::new(Expr::Variable("n".to_string())),
            "name".to_string(),
        );
        let result = cypher_expr_to_df(&expr, Some(&ctx)).unwrap();
        let s = format!("{:?}", result);
        assert!(
            s.contains("Column") && s.contains("n.name"),
            "expected flat column 'n.name' for graph entity, got: {s}"
        );
    }

    #[test]
    fn test_property_on_non_graph_var_uses_index() {
        // When variable is not in variable_kinds (e.g., map from WITH), use index UDF
        let ctx = TranslationContext::new();

        let expr = Expr::Property(
            Box::new(Expr::Variable("map".to_string())),
            "name".to_string(),
        );
        let result = cypher_expr_to_df(&expr, Some(&ctx)).unwrap();
        let s = format!("{}", result);
        assert!(
            s.contains("index"),
            "expected index UDF for non-graph variable, got: {s}"
        );
    }
}
