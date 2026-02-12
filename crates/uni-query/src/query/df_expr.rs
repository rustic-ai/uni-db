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
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;
use uni_common::Value;
use uni_common::core::schema::PropertyMeta;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, MapProjectionItem, UnaryOp};

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
    /// Path variable - kept as-is (struct with nodes/relationships)
    Path,
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
            // Direct identifier becomes a column reference
            // Use Column::from_name() to avoid treating dots as table.column qualifiers
            // This is critical for transformed property expressions like "e.salary"
            //
            // When variable kind is known, resolve to identity column:
            // - Node variables: n → n._vid
            // - Edge variables: r → r._eid
            // - Path variables: p → p (kept as-is, struct column)
            if let Some(ctx) = context
                && let Some(kind) = ctx.variable_kinds.get(name)
            {
                return match kind {
                    VariableKind::Node | VariableKind::Edge => {
                        // Return the Struct column representing the whole entity.
                        // This column is added by the hybrid planner when structural access is needed.
                        Ok(DfExpr::Column(Column::from_name(name)))
                    }
                    VariableKind::Path => Ok(DfExpr::Column(Column::from_name(name))),
                };
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

        Expr::Property(base, prop) => {
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

                Ok(DfExpr::Column(Column::from_name(col_name)))
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
                if let Expr::Parameter(param_name) = base.as_ref() {
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
                Ok(dummy_udf_expr("index", vec![base_expr, lit(prop.clone())]))
            }
        }

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

            Ok(datafusion::functions_nested::expr_fn::array_slice(
                array_expr, start_expr, end_expr, None,
            ))
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

        Expr::List(items) => {
            // Check for mixed types or nested lists which cause issues in DataFusion
            let mut has_string = false;
            let mut has_bool = false;
            let mut has_list = false;
            let mut has_map = false;
            let mut has_numeric = false;

            for item in items {
                match item {
                    Expr::Literal(CypherLiteral::Float(_))
                    | Expr::Literal(CypherLiteral::Integer(_)) => has_numeric = true,
                    Expr::Literal(CypherLiteral::String(_)) => has_string = true,
                    Expr::Literal(CypherLiteral::Bool(_)) => has_bool = true,
                    Expr::List(_) => has_list = true,
                    Expr::Map(_) => has_map = true,
                    // Treat Null as compatible with anything
                    // If complex expr (e.g. Variable), assume compatibility or let DF handle it
                    _ => {}
                }
            }

            // Reject mixed types that can't be coerced (e.g. String + Numeric)
            // Nested lists are problematic in general for make_array if types differ
            if has_list {
                // For now, reject lists of lists to force fallback, as verifying inner types is hard
                return Err(anyhow!(
                    "Nested lists not supported in DataFusion translation"
                ));
            }

            // Check distinct non-null types count
            let types_count = (if has_numeric { 1 } else { 0 })
                + (if has_string { 1 } else { 0 })
                + (if has_bool { 1 } else { 0 })
                + (if has_map { 1 } else { 0 });

            if types_count > 1 {
                return Err(anyhow!(
                    "Mixed type lists (e.g. [1, 'a']) not supported in DataFusion translation"
                ));
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
                let empty_arr = ScalarValue::new_list_nullable(
                    &[],
                    &datafusion::arrow::datatypes::DataType::Null,
                );
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

        Expr::Map(entries) => {
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

        Expr::In { expr, list } => {
            // When the left side is a node/edge variable and the right side is a
            // dynamic array (e.g., CTE variable), rewrite to compare by identity
            // column (_vid for nodes, _eid for edges). Cast to Int64 to match the
            // list element type from parameter injection.
            let left_expr = if let Expr::Variable(var) = expr.as_ref()
                && let Some(ctx) = context
                && let Some(kind) = ctx.variable_kinds.get(var)
            {
                match kind {
                    VariableKind::Node => {
                        use datafusion::logical_expr::Cast;
                        DfExpr::Cast(Cast::new(
                            Box::new(DfExpr::Column(Column::from_name(format!("{}._vid", var)))),
                            datafusion::arrow::datatypes::DataType::Int64,
                        ))
                    }
                    VariableKind::Edge => {
                        use datafusion::logical_expr::Cast;
                        DfExpr::Cast(Cast::new(
                            Box::new(DfExpr::Column(Column::from_name(format!("{}._eid", var)))),
                            datafusion::arrow::datatypes::DataType::Int64,
                        ))
                    }
                    _ => cypher_expr_to_df(expr, context)?,
                }
            } else {
                cypher_expr_to_df(expr, context)?
            };

            // When the right side is a literal list, expand to individual items
            // for IN-list. Otherwise, use array_has for array column membership.
            if let Expr::List(items) = list.as_ref() {
                let expanded: Vec<DfExpr> = items
                    .iter()
                    .map(|item| cypher_expr_to_df(item, context))
                    .collect::<Result<Vec<_>>>()?;
                Ok(datafusion::prelude::in_list(left_expr, expanded, false))
            } else {
                let right_expr = cypher_expr_to_df(list, context)?;

                // Implement Cypher IN semantics for dynamic arrays (e.g. variables/parameters)
                // 1. If rhs IS NULL -> NULL
                // 2. If lhs IS NULL:
                //    - If rhs is empty -> FALSE
                //    - Else -> NULL
                // 3. If lhs IS NOT NULL:
                //    - If array_has(rhs, lhs) -> TRUE
                //    - If array_has(rhs, NULL) -> NULL
                //    - Else -> FALSE

                use datafusion::arrow::datatypes::DataType;
                use datafusion::functions_nested::expr_fn::{array_has, array_length};
                use datafusion::logical_expr::{Cast, when};

                // If rhs is literal null, return null immediately to avoid type errors in array functions
                if matches!(right_expr, DfExpr::Literal(ScalarValue::Null, _)) {
                    return Ok(lit(ScalarValue::Boolean(None)));
                }

                let rhs_is_null = right_expr.clone().is_null();
                let lhs_is_null = left_expr.clone().is_null();

                // Ensure rhs is a list for array_length/array_has to satisfy planner
                // If it's Null type, cast to List(Null)
                // Note: This cast might only be needed if type inference fails, but good for safety
                // We use the original expr for logic, but maybe we need a "typed" version

                let len = array_length(right_expr.clone());
                // Check if 0 (handle UInt64 return type of array_length)
                // We use cast to Int64 to compare with 0 safely
                let len_i64 = DfExpr::Cast(Cast::new(Box::new(len), DataType::Int64));
                let rhs_empty = len_i64.eq(lit(0i64));

                // Check if array contains null using our custom UDF which handles it robustly
                // Use real UDF directly since this is created post-resolution
                let has_null_udf = crate::query::df_udfs::create_has_null_udf();
                let has_null_expr =
                    DfExpr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
                        func: std::sync::Arc::new(has_null_udf),
                        args: vec![right_expr.clone()],
                    });

                let branch_lhs_null =
                    when(rhs_empty, lit(false)).otherwise(lit(ScalarValue::Boolean(None)))?;

                // If lhs is NOT null:
                // 1. Found exact match -> true
                // 2. Found null in list -> null
                // 3. Not found and no nulls -> false

                let branch_lhs_not_null =
                    when(array_has(right_expr.clone(), left_expr.clone()), lit(true))
                        .when(has_null_expr, lit(ScalarValue::Boolean(None)))
                        .otherwise(lit(false))?;

                Ok(when(rhs_is_null, lit(ScalarValue::Boolean(None)))
                    .when(lhs_is_null, branch_lhs_null)
                    .otherwise(branch_lhs_not_null)?)
            }
        }

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
        } => {
            let mut case_builder = if let Some(match_expr) = expr {
                let match_df = cypher_expr_to_df(match_expr, context)?;
                datafusion::logical_expr::case(match_df)
            } else {
                datafusion::logical_expr::when(
                    cypher_expr_to_df(&when_then[0].0, context)?,
                    cypher_expr_to_df(&when_then[0].1, context)?,
                )
            };

            let start_idx = if expr.is_some() { 0 } else { 1 };
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

        Expr::Reduce { .. } => Err(anyhow!(
            "Reduce expressions not yet supported in DataFusion translation"
        )),

        Expr::Exists(_) => Err(anyhow!(
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

        Expr::MapProjection { base, items } => {
            let mut args = Vec::new();
            for item in items {
                match item {
                    MapProjectionItem::Property(prop) => {
                        args.push(lit(prop.clone()));
                        let prop_expr = cypher_expr_to_df(
                            &Expr::Property(base.clone(), prop.clone()),
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
            let non_null_scalars: Vec<&ScalarValue> = scalars
                .iter()
                .filter(|s| !matches!(s, ScalarValue::Null))
                .collect();

            let data_type = if non_null_scalars.is_empty() {
                datafusion::arrow::datatypes::DataType::Null
            } else if non_null_scalars
                .iter()
                .all(|s| matches!(s, ScalarValue::Int64(_)))
            {
                datafusion::arrow::datatypes::DataType::Int64
            } else if non_null_scalars
                .iter()
                .all(|s| matches!(s, ScalarValue::Float64(_) | ScalarValue::Int64(_)))
            {
                datafusion::arrow::datatypes::DataType::Float64
            } else if non_null_scalars
                .iter()
                .all(|s| matches!(s, ScalarValue::Utf8(_)))
            {
                datafusion::arrow::datatypes::DataType::Utf8
            } else if non_null_scalars
                .iter()
                .all(|s| matches!(s, ScalarValue::Boolean(_)))
            {
                datafusion::arrow::datatypes::DataType::Boolean
            } else {
                // Mixed types - use LargeBinary (JSON) to preserve type information
                datafusion::arrow::datatypes::DataType::LargeBinary
            };

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
        // Comparison operators
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
            // XOR = (A OR B) AND NOT (A AND B)
            let a_or_b = left.clone().or(right.clone());
            let a_and_b = left.and(right);
            Ok(a_or_b.and(a_and_b.not()))
        }

        // Arithmetic operators
        BinaryOp::Add => {
            // Check if either operand is a string literal — use concat instead
            let is_string_lit = |e: &DfExpr| matches!(e, DfExpr::Literal(ScalarValue::Utf8(_), _));
            if is_string_lit(&left) || is_string_lit(&right) {
                Ok(datafusion::functions::string::expr_fn::concat(vec![
                    left, right,
                ]))
            } else {
                Ok(left + right)
            }
        }
        BinaryOp::Sub => Ok(left - right),
        BinaryOp::Mul => Ok(left * right),
        BinaryOp::Div => Ok(left / right),
        BinaryOp::Mod => Ok(left % right),
        BinaryOp::Pow => Ok(datafusion::functions::math::expr_fn::power(left, right)),

        // String operators
        BinaryOp::Contains => {
            // Use _cypher_contains UDF for safe type handling
            Ok(dummy_udf_expr("_cypher_contains", vec![left, right]))
        }
        BinaryOp::StartsWith => {
            // Use _cypher_starts_with UDF for safe type handling
            Ok(dummy_udf_expr("_cypher_starts_with", vec![left, right]))
        }
        BinaryOp::EndsWith => {
            // Use _cypher_ends_with UDF for safe type handling
            Ok(dummy_udf_expr("_cypher_ends_with", vec![left, right]))
        }

        // Regex
        BinaryOp::Regex => {
            Ok(datafusion::functions::expr_fn::regexp_match(left, right, None).is_not_null())
        }

        // Vector similarity operator cannot be pushed down to DataFusion
        BinaryOp::ApproxEq => Err(anyhow::anyhow!(
            "Vector similarity operator (~=) cannot be pushed down to DataFusion; it must be evaluated in the query engine"
        )),
    }
}

/// Require at least one argument, returning an error with the function name if empty.
fn require_arg(df_args: &[DfExpr], func_name: &str) -> Result<()> {
    if df_args.is_empty() {
        return Err(anyhow!("{} requires an argument", func_name));
    }
    Ok(())
}

/// Require at least N arguments, returning an error with the function name if insufficient.
fn require_args(df_args: &[DfExpr], count: usize, func_name: &str) -> Result<()> {
    if df_args.len() < count {
        return Err(anyhow!("{} requires {} arguments", func_name, count));
    }
    Ok(())
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
    match name_upper.as_str() {
        // Aggregate functions
        "COUNT" => {
            if df_args.is_empty() {
                Ok(datafusion::functions_aggregate::count::count(lit(1i64)))
            } else if distinct {
                datafusion::functions_aggregate::count::count(first_arg(&df_args))
                    .distinct()
                    .build()
                    .map_err(|e| anyhow!("Failed to build COUNT DISTINCT: {}", e))
            } else {
                Ok(datafusion::functions_aggregate::count::count(first_arg(
                    &df_args,
                )))
            }
        }
        "SUM" => {
            require_arg(&df_args, "SUM")?;
            let base_expr = datafusion::functions_aggregate::sum::sum(first_arg(&df_args));
            if distinct {
                base_expr
                    .distinct()
                    .build()
                    .map_err(|e| anyhow!("Failed to build SUM DISTINCT: {}", e))
            } else {
                Ok(base_expr)
            }
        }
        "AVG" => {
            require_arg(&df_args, "AVG")?;
            let base_expr = datafusion::functions_aggregate::average::avg(first_arg(&df_args));
            if distinct {
                base_expr
                    .distinct()
                    .build()
                    .map_err(|e| anyhow!("Failed to build AVG DISTINCT: {}", e))
            } else {
                Ok(base_expr)
            }
        }
        "MIN" => {
            require_arg(&df_args, "MIN")?;
            Ok(datafusion::functions_aggregate::min_max::min(first_arg(
                &df_args,
            )))
        }
        "MAX" => {
            require_arg(&df_args, "MAX")?;
            Ok(datafusion::functions_aggregate::min_max::max(first_arg(
                &df_args,
            )))
        }
        "COLLECT" => {
            require_arg(&df_args, "COLLECT")?;
            Ok(datafusion::functions_aggregate::array_agg::array_agg(
                first_arg(&df_args),
            ))
        }

        // Type conversion functions
        "TOSTRING" => {
            require_arg(&df_args, "toString")?;
            Ok(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Utf8,
            ))
        }
        "TOINTEGER" | "TOINT" => {
            require_arg(&df_args, "toInteger")?;
            Ok(dummy_udf_expr("toInteger", df_args))
        }
        "TOFLOAT" => {
            require_arg(&df_args, "toFloat")?;
            Ok(dummy_udf_expr("toFloat", df_args))
        }
        "TOBOOLEAN" | "TOBOOL" => {
            require_arg(&df_args, "toBoolean")?;
            Ok(dummy_udf_expr("toBoolean", df_args))
        }

        // String case functions
        "UPPER" | "TOUPPER" => {
            require_arg(&df_args, "upper")?;
            Ok(datafusion::functions::string::expr_fn::upper(first_arg(
                &df_args,
            )))
        }
        "LOWER" | "TOLOWER" => {
            require_arg(&df_args, "lower")?;
            Ok(datafusion::functions::string::expr_fn::lower(first_arg(
                &df_args,
            )))
        }
        "SUBSTRING" => {
            require_args(&df_args, 2, "substring")?;
            // substring(str, start, length?)
            // Cypher is 0-based, DataFusion substr is 1-based.
            let str_expr = df_args[0].clone();
            let start_expr = df_args[1].clone() + lit(1i64);

            let substr_expr = datafusion::functions::unicode::expr_fn::substr(str_expr, start_expr);

            if df_args.len() == 3 {
                Ok(datafusion::functions::unicode::expr_fn::left(
                    substr_expr,
                    df_args[2].clone(),
                ))
            } else {
                Ok(substr_expr)
            }
        }
        // Trim functions
        "TRIM" => {
            require_arg(&df_args, "TRIM")?;
            Ok(datafusion::functions::string::expr_fn::btrim(vec![
                first_arg(&df_args),
            ]))
        }
        "LTRIM" => {
            require_arg(&df_args, "LTRIM")?;
            Ok(datafusion::functions::string::expr_fn::ltrim(vec![
                first_arg(&df_args),
            ]))
        }
        "RTRIM" => {
            require_arg(&df_args, "RTRIM")?;
            Ok(datafusion::functions::string::expr_fn::rtrim(vec![
                first_arg(&df_args),
            ]))
        }

        // String manipulation functions
        "LEFT" => {
            require_args(&df_args, 2, "left")?;
            Ok(datafusion::functions::unicode::expr_fn::left(
                df_args[0].clone(),
                df_args[1].clone(),
            ))
        }
        "RIGHT" => {
            require_args(&df_args, 2, "right")?;
            Ok(datafusion::functions::unicode::expr_fn::right(
                df_args[0].clone(),
                df_args[1].clone(),
            ))
        }
        "REPLACE" => {
            require_args(&df_args, 3, "replace")?;
            Ok(datafusion::functions::string::expr_fn::replace(
                df_args[0].clone(),
                df_args[1].clone(),
                df_args[2].clone(),
            ))
        }
        "REVERSE" => {
            require_arg(&df_args, "reverse")?;
            Ok(datafusion::functions::unicode::expr_fn::reverse(first_arg(
                &df_args,
            )))
        }
        "SPLIT" => {
            require_args(&df_args, 2, "split")?;
            // Use DataFusion's string_to_array function
            // Third argument is optional null_value string (we don't need it for Cypher)
            Ok(datafusion::functions_nested::expr_fn::string_to_array(
                df_args[0].clone(),
                df_args[1].clone(),
                lit(datafusion::common::ScalarValue::Utf8(None)), // No special null handling
            ))
        }
        "SIZE" | "LENGTH" => {
            require_arg(&df_args, name)?;
            // Use our custom _cypher_size UDF that dispatches at runtime based
            // on whether the argument is a list, string, JSONB blob, or map.
            Ok(dummy_udf_expr("_cypher_size", df_args))
        }

        // Single-argument math functions
        "ABS" => {
            require_arg(&df_args, "abs")?;
            Ok(datafusion::functions::math::expr_fn::abs(first_arg(
                &df_args,
            )))
        }
        "CEIL" | "CEILING" => {
            require_arg(&df_args, "ceil")?;
            Ok(datafusion::functions::math::expr_fn::ceil(first_arg(
                &df_args,
            )))
        }
        "FLOOR" => {
            require_arg(&df_args, "floor")?;
            Ok(datafusion::functions::math::expr_fn::floor(first_arg(
                &df_args,
            )))
        }
        "ROUND" => {
            require_arg(&df_args, "round")?;
            let args = if df_args.len() == 1 {
                vec![first_arg(&df_args)]
            } else {
                vec![df_args[0].clone(), df_args[1].clone()]
            };
            Ok(datafusion::functions::math::expr_fn::round(args))
        }
        // Cast to Float64 for Int64 compatibility (DataFusion signum doesn't support Int64)
        "SIGN" => apply_unary_math_f64(
            &df_args,
            "sign",
            datafusion::functions::math::expr_fn::signum,
        ),
        "SQRT" => {
            apply_unary_math_f64(&df_args, "sqrt", datafusion::functions::math::expr_fn::sqrt)
        }
        "LOG" | "LN" => {
            apply_unary_math_f64(&df_args, "log", datafusion::functions::math::expr_fn::ln)
        }
        "LOG10" => apply_unary_math_f64(
            &df_args,
            "log10",
            datafusion::functions::math::expr_fn::log10,
        ),
        "EXP" => apply_unary_math_f64(&df_args, "exp", datafusion::functions::math::expr_fn::exp),

        // Trigonometric functions - cast args to Float64 for Int64 compatibility
        "SIN" => apply_unary_math_f64(&df_args, "sin", datafusion::functions::math::expr_fn::sin),
        "COS" => apply_unary_math_f64(&df_args, "cos", datafusion::functions::math::expr_fn::cos),
        "TAN" => apply_unary_math_f64(&df_args, "tan", datafusion::functions::math::expr_fn::tan),
        "ASIN" => {
            apply_unary_math_f64(&df_args, "asin", datafusion::functions::math::expr_fn::asin)
        }
        "ACOS" => {
            apply_unary_math_f64(&df_args, "acos", datafusion::functions::math::expr_fn::acos)
        }
        "ATAN" => {
            apply_unary_math_f64(&df_args, "atan", datafusion::functions::math::expr_fn::atan)
        }
        "ATAN2" => {
            require_args(&df_args, 2, "atan2")?;
            Ok(datafusion::functions::math::expr_fn::atan2(
                cast_expr(
                    df_args[0].clone(),
                    datafusion::arrow::datatypes::DataType::Float64,
                ),
                cast_expr(
                    df_args[1].clone(),
                    datafusion::arrow::datatypes::DataType::Float64,
                ),
            ))
        }
        "RAND" | "RANDOM" => Ok(datafusion::functions::math::expr_fn::random()),

        // Constants
        "E" if df_args.is_empty() => Ok(lit(std::f64::consts::E)),
        "PI" if df_args.is_empty() => Ok(lit(std::f64::consts::PI)),

        // Temporal constructors and functions — handled by registered UDFs.
        // The TemporalUdf delegates to eval_datetime_function() in datetime.rs.
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
        | "LOCALDATETIME.REALTIME" => Ok(dummy_udf_expr(name, df_args)),

        // Null handling
        "COALESCE" => {
            require_arg(&df_args, "coalesce")?;
            Ok(datafusion::functions::expr_fn::coalesce(df_args))
        }
        "NULLIF" => {
            require_args(&df_args, 2, "nullif")?;
            Ok(datafusion::functions::expr_fn::nullif(
                df_args[0].clone(),
                df_args[1].clone(),
            ))
        }

        // List functions
        "HEAD" => {
            require_arg(&df_args, "head")?;
            Ok(datafusion::functions_nested::expr_fn::array_element(
                first_arg(&df_args),
                lit(1i64),
            ))
        }
        "LAST" => {
            require_arg(&df_args, "last")?;
            Ok(datafusion::functions_nested::expr_fn::array_element(
                first_arg(&df_args),
                lit(-1i64),
            ))
        }
        "TAIL" => {
            require_arg(&df_args, "tail")?;
            let arr = first_arg(&df_args);
            let len = cast_expr(
                datafusion::functions_nested::expr_fn::array_length(arr.clone()),
                datafusion::arrow::datatypes::DataType::Int64,
            );
            Ok(datafusion::functions_nested::expr_fn::array_slice(
                arr,
                lit(2i64),
                len,
                None,
            ))
        }
        "RANGE" => {
            require_args(&df_args, 2, "range")?;
            Ok(dummy_udf_expr("range", df_args))
        }

        // Graph-specific functions (registered as UDFs)
        "ID" => {
            // When called with a bare variable (ID(n)), rewrite to the internal
            // identity column reference (_vid for nodes, _eid for edges).
            if let Some(Expr::Variable(var)) = args.first() {
                let id_suffix = if let Some(ctx) = context
                    && ctx.variable_kinds.get(var) == Some(&VariableKind::Edge)
                {
                    "_eid"
                } else {
                    "_vid"
                };
                Ok(DfExpr::Column(Column::from_name(format!(
                    "{}.{}",
                    var, id_suffix
                ))))
            } else {
                Ok(dummy_udf_expr("id", df_args))
            }
        }
        "LABELS" | "KEYS" => {
            // labels(n)/keys(n) expect the struct column representing the whole entity.
            // The struct is built by add_structural_projection() and exposed as Column("n").
            // df_args already has the correct resolution via the Variable case which
            // returns Column("n") when variable_kinds context is present.
            Ok(dummy_udf_expr(name, df_args))
        }
        "TYPE" => {
            // type(r) returns the edge type name as a string.
            // When context provides the edge type via variable_labels, emit a string literal.
            if let Some(Expr::Variable(var)) = args.first()
                && let Some(ctx) = context
                && let Some(label) = ctx.variable_labels.get(var)
            {
                return Ok(lit(label.clone()));
            }
            // Fallback: use _type column from traverse output (schemaless edges)
            if let Some(Expr::Variable(var)) = args.first() {
                return Ok(DfExpr::Column(Column::from_name(format!("{}._type", var))));
            }
            Ok(dummy_udf_expr("type", df_args))
        }
        "PROPERTIES" => {
            // properties(n) receives the struct column representing the entity,
            // same as keys(n). The struct is built by add_structural_projection().
            Ok(dummy_udf_expr(name, df_args))
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
                let ts = cypher_expr_to_df(ts_expr, context)?;

                // start_prop <= timestamp
                let start_check = start_col.lt_eq(ts.clone());
                // end_prop IS NULL OR end_prop > timestamp
                let end_null = DfExpr::IsNull(Box::new(end_col.clone()));
                let end_after = end_col.gt(ts);
                let end_check = end_null.or(end_after);

                Ok(start_check.and(end_check))
            } else {
                // Fallback: pass through as dummy UDF
                Ok(dummy_udf_expr(name, df_args))
            }
        }
        "NODES" | "RELATIONSHIPS" => Ok(dummy_udf_expr(name, df_args)),

        // Label predicate: hasLabel(n, 'Label') translates to array_has(n._labels, 'Label')
        "HASLABEL" => {
            require_args(&df_args, 2, "hasLabel")?;
            // First arg should be a variable, second should be the label string
            if let Some(Expr::Variable(var)) = args.first() {
                if let Some(Expr::Literal(CypherLiteral::String(label))) = args.get(1) {
                    // Translate to: array_has({var}._labels, '{label}')
                    let labels_col = DfExpr::Column(Column::from_name(format!("{}._labels", var)));
                    Ok(datafusion::functions_nested::expr_fn::array_has(
                        labels_col,
                        lit(label.clone()),
                    ))
                } else {
                    // Can't translate with non-string label - force fallback
                    Err(anyhow::anyhow!(
                        "hasLabel requires string literal as second argument for DataFusion translation"
                    ))
                }
            } else {
                // Can't translate without variable - force fallback
                Err(anyhow::anyhow!(
                    "hasLabel requires variable as first argument for DataFusion translation"
                ))
            }
        }

        // Unknown function - try as a UDF
        _ => Ok(dummy_udf_expr(name, df_args)),
    }
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

/// Rewrite overflow property references in a DataFusion filter expression.
///
/// Properties that are NOT in the label's schema are "overflow" properties stored
/// in the `overflow_json` LargeBinary (JSONB) column. This function rewrites
/// column references like `{var}.{prop}` to `json_get_*(var.overflow_json, 'prop')`
/// calls so DataFusion can evaluate the filter against the JSONB data.
///
/// The appropriate `json_get_*` UDF is chosen based on the literal type being compared:
/// - `ScalarValue::Utf8` → `json_get_string`
/// - `ScalarValue::Int64` → `json_get_int`
/// - `ScalarValue::Float64` → `json_get_float`
/// - `ScalarValue::Boolean` → `json_get_bool`
///
/// For IS NULL / IS NOT NULL and string operations, `json_get_string` is used.
pub fn rewrite_overflow_filters(
    expr: DfExpr,
    variable: &str,
    label_props: Option<&HashMap<String, PropertyMeta>>,
) -> Result<DfExpr> {
    rewrite_overflow_filters_with_source(expr, variable, label_props, "overflow_json")
}

/// Like `rewrite_overflow_filters`, but with a custom source column suffix.
///
/// For registered labels, the source column is `overflow_json`.
/// For schemaless scans, the source column is `_all_props`.
pub fn rewrite_overflow_filters_with_source(
    expr: DfExpr,
    variable: &str,
    label_props: Option<&HashMap<String, PropertyMeta>>,
    source_col_suffix: &str,
) -> Result<DfExpr> {
    match expr {
        DfExpr::BinaryExpr(binary) => {
            use datafusion::logical_expr::Operator;

            // For AND/OR, recurse into both sides
            if matches!(binary.op, Operator::And | Operator::Or) {
                let left = rewrite_overflow_filters_with_source(
                    *binary.left,
                    variable,
                    label_props,
                    source_col_suffix,
                )?;
                let right = rewrite_overflow_filters_with_source(
                    *binary.right,
                    variable,
                    label_props,
                    source_col_suffix,
                )?;
                return Ok(DfExpr::BinaryExpr(
                    datafusion::logical_expr::expr::BinaryExpr::new(
                        Box::new(left),
                        binary.op,
                        Box::new(right),
                    ),
                ));
            }

            // For comparison operators, check if one side is an overflow column
            if matches!(
                binary.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::Gt
                    | Operator::LtEq
                    | Operator::GtEq
            ) {
                // Try column on left, literal on right
                if let Some((prop, rewritten)) = try_rewrite_column_literal_with_source(
                    &binary.left,
                    &binary.right,
                    variable,
                    label_props,
                    source_col_suffix,
                ) {
                    let _ = prop;
                    return Ok(DfExpr::BinaryExpr(
                        datafusion::logical_expr::expr::BinaryExpr::new(
                            Box::new(rewritten),
                            binary.op,
                            binary.right,
                        ),
                    ));
                }
                // Try column on right, literal on left
                if let Some((prop, rewritten)) = try_rewrite_column_literal_with_source(
                    &binary.right,
                    &binary.left,
                    variable,
                    label_props,
                    source_col_suffix,
                ) {
                    let _ = prop;
                    return Ok(DfExpr::BinaryExpr(
                        datafusion::logical_expr::expr::BinaryExpr::new(
                            binary.left,
                            binary.op,
                            Box::new(rewritten),
                        ),
                    ));
                }
            }

            Ok(DfExpr::BinaryExpr(binary))
        }
        DfExpr::Not(inner) => {
            let rewritten = rewrite_overflow_filters_with_source(
                *inner,
                variable,
                label_props,
                source_col_suffix,
            )?;
            Ok(DfExpr::Not(Box::new(rewritten)))
        }
        DfExpr::IsNull(inner) => {
            if let Some(rewritten) = try_rewrite_overflow_column_with_source(
                &inner,
                variable,
                label_props,
                "json_get_string",
                source_col_suffix,
            ) {
                Ok(DfExpr::IsNull(Box::new(rewritten)))
            } else {
                Ok(DfExpr::IsNull(inner))
            }
        }
        DfExpr::IsNotNull(inner) => {
            if let Some(rewritten) = try_rewrite_overflow_column_with_source(
                &inner,
                variable,
                label_props,
                "json_get_string",
                source_col_suffix,
            ) {
                Ok(DfExpr::IsNotNull(Box::new(rewritten)))
            } else {
                Ok(DfExpr::IsNotNull(inner))
            }
        }
        DfExpr::ScalarFunction(func) => {
            // For string operations like _cypher_contains, _cypher_starts_with, _cypher_ends_with,
            // rewrite overflow column args to json_get_string
            let func_name = func.func.name();
            if matches!(
                func_name,
                "_cypher_contains" | "_cypher_starts_with" | "_cypher_ends_with"
            ) && !func.args.is_empty()
            {
                let mut new_args = func.args;
                if let Some(rewritten) = try_rewrite_overflow_column_with_source(
                    &new_args[0],
                    variable,
                    label_props,
                    "json_get_string",
                    source_col_suffix,
                ) {
                    new_args[0] = rewritten;
                }
                return Ok(DfExpr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: func.func,
                        args: new_args,
                    },
                ));
            }
            Ok(DfExpr::ScalarFunction(func))
        }
        other => Ok(other),
    }
}

/// Check if a DfExpr is a column reference to an overflow property for the given variable.
///
/// Returns the property name if this is `{variable}.{prop}` where `prop` is NOT in the
/// label schema (i.e., it's an overflow property).
fn is_overflow_column(
    expr: &DfExpr,
    variable: &str,
    label_props: Option<&HashMap<String, PropertyMeta>>,
) -> Option<String> {
    if let DfExpr::Column(col) = expr {
        let col_name = &col.name;
        let prefix = format!("{}.", variable);
        if let Some(prop) = col_name.strip_prefix(&prefix) {
            // System columns are never overflow
            if prop.starts_with('_') {
                return None;
            }
            // If no label_props, we can't determine overflow
            let props = label_props?;
            if !props.contains_key(prop) {
                return Some(prop.to_string());
            }
        }
    }
    None
}

/// Try to rewrite a column reference to a json_get_* UDF call if it's an overflow property.
fn try_rewrite_overflow_column_with_source(
    expr: &DfExpr,
    variable: &str,
    label_props: Option<&HashMap<String, PropertyMeta>>,
    udf_name: &str,
    source_col_suffix: &str,
) -> Option<DfExpr> {
    let prop = is_overflow_column(expr, variable, label_props)?;
    let overflow_col = DfExpr::Column(Column::from_name(format!(
        "{}.{}",
        variable, source_col_suffix
    )));
    Some(dummy_udf_expr(udf_name, vec![overflow_col, lit(prop)]))
}

/// Try to rewrite (column, literal) pair where column is an overflow property.
fn try_rewrite_column_literal_with_source(
    col_expr: &DfExpr,
    lit_expr: &DfExpr,
    variable: &str,
    label_props: Option<&HashMap<String, PropertyMeta>>,
    source_col_suffix: &str,
) -> Option<(String, DfExpr)> {
    let prop = is_overflow_column(col_expr, variable, label_props)?;

    // Choose UDF based on the literal type
    let udf_name = match lit_expr {
        DfExpr::Literal(ScalarValue::Utf8(_), _) => "json_get_string",
        DfExpr::Literal(ScalarValue::Int64(_), _) => "json_get_int",
        DfExpr::Literal(ScalarValue::Float64(_), _) => "json_get_float",
        DfExpr::Literal(ScalarValue::Boolean(_), _) => "json_get_bool",
        // Default to string for other/unknown types
        _ => "json_get_string",
    };

    let overflow_col = DfExpr::Column(Column::from_name(format!(
        "{}.{}",
        variable, source_col_suffix
    )));
    let rewritten = dummy_udf_expr(udf_name, vec![overflow_col, lit(prop.clone())]);
    Some((prop, rewritten))
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
        Expr::Exists(_) | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {}
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

            if is_comparison || is_arithmetic {
                let left_type = left.get_type(schema).ok();
                let right_type = right.get_type(schema).ok();

                // String + anything → concat (Cypher uses + for concatenation)
                if binary.op == Operator::Plus
                    && let (Some(lt), Some(rt)) = (&left_type, &right_type)
                {
                    let left_is_string = matches!(lt, DataType::Utf8 | DataType::LargeUtf8);
                    let right_is_string = matches!(rt, DataType::Utf8 | DataType::LargeUtf8);
                    if left_is_string || right_is_string {
                        return Ok(datafusion::functions::string::expr_fn::concat(vec![
                            left, right,
                        ]));
                    }
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
                        let is_list_mismatch = match (lt, rt) {
                            (DataType::List(l_field), DataType::List(r_field))
                            | (DataType::LargeList(l_field), DataType::LargeList(r_field))
                            | (DataType::List(l_field), DataType::LargeList(r_field))
                            | (DataType::LargeList(l_field), DataType::List(r_field)) => {
                                let l_inner = l_field.data_type();
                                let r_inner = r_field.data_type();
                                let compatible = l_inner == r_inner
                                    || l_inner == &DataType::Null
                                    || r_inner == &DataType::Null
                                    || (l_inner.is_numeric() && r_inner.is_numeric());
                                !compatible
                            }
                            (DataType::List(_), _)
                            | (DataType::LargeList(_), _)
                            | (_, DataType::List(_))
                            | (_, DataType::LargeList(_)) => true,
                            _ => false,
                        };

                        if is_list_mismatch {
                            return Ok(datafusion::logical_expr::lit(
                                datafusion::common::ScalarValue::Boolean(None),
                            ));
                        }

                        // Scalar cross-type comparison: incompatible types yield false/true/null.
                        // Skip LargeBinary (JSONB, handled by overflow rewriting) and
                        // Timestamp/Utf8 (handled above).
                        if !is_list_mismatch
                            && !matches!(lt, DataType::LargeBinary)
                            && !matches!(rt, DataType::LargeBinary)
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
    fn test_property_access() {
        let expr = Expr::Property(Box::new(Expr::Variable("n".to_string())), "age".to_string());
        let result = cypher_expr_to_df(&expr, None).unwrap();
        let s = format!("{:?}", result);
        // DataFusion interprets "n.age" as table="n", name="age"
        assert!(s.contains("Column"));
        assert!(s.contains("age"));
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
}
