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
use serde_json::Value;
use std::hash::{Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, UnaryOp};

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
                    VariableKind::Node => {
                        Ok(DfExpr::Column(Column::from_name(format!("{}._vid", name))))
                    }
                    VariableKind::Edge => {
                        Ok(DfExpr::Column(Column::from_name(format!("{}._eid", name))))
                    }
                    VariableKind::Path => Ok(DfExpr::Column(Column::from_name(name))),
                };
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
                    return Ok(DfExpr::ScalarFunction(
                        datafusion::logical_expr::expr::ScalarFunction {
                            func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                                DummyUdf::new("_duration_property".to_string()),
                            )),
                            args: vec![base_expr, lit(prop.to_string())],
                        },
                    ));
                }

                // Standard property access: "{variable}.{property}" column reference.
                let col_name = format!("{}.{}", var_name, prop);
                Ok(DfExpr::Column(Column::from_name(col_name)))
            } else {
                // Base is a complex expression (e.g., function call result).
                // Try duration accessor first, otherwise fall back to column.
                if crate::query::datetime::is_duration_accessor(prop) {
                    let base_expr = cypher_expr_to_df(base, context)?;
                    return Ok(DfExpr::ScalarFunction(
                        datafusion::logical_expr::expr::ScalarFunction {
                            func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                                DummyUdf::new("_duration_property".to_string()),
                            )),
                            args: vec![base_expr, lit(prop.to_string())],
                        },
                    ));
                }
                let var_name = extract_variable_name(base)?;
                let col_name = format!("{}.{}", var_name, prop);
                Ok(DfExpr::Column(Column::from_name(col_name)))
            }
        }

        Expr::ArrayIndex { array, index } => {
            // Cypher uses 0-based indexing and supports negative indices
            // Convert to 1-based for DataFusion, handle negatives specially
            let array_expr = cypher_expr_to_df(array, context)?;
            let index_expr = cypher_expr_to_df(index, context)?;

            // For Cypher:
            // - Positive indices are 0-based: [0, 1, 2, ...]
            // - Negative indices count from end: [-1 is last, -2 is second-to-last]
            // DataFusion array_element uses 1-based indexing and supports negatives
            // So Cypher index N -> DataFusion index N+1 (for non-negative)
            // And Cypher index -N -> DataFusion index -N (same semantics)

            // Check if index is negative by comparing with 0
            let adjusted_index = datafusion::logical_expr::case(index_expr.clone())
                .when(index_expr.clone().lt(lit(0i64)), index_expr.clone())
                .otherwise(index_expr + lit(1i64))?;

            Ok(datafusion::functions_nested::expr_fn::array_element(
                array_expr,
                adjusted_index,
            ))
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
                // Slice to end - use array_length
                datafusion::functions_nested::expr_fn::array_length(array_expr.clone())
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
                return json_value_to_scalar(value).map(lit);
            }
            Err(anyhow!("Unresolved parameter: ${}", name))
        }

        Expr::Literal(value) => {
            let scalar = cypher_literal_to_scalar(value)?;
            Ok(lit(scalar))
        }

        Expr::List(items) => {
            // Create a scalar list value
            let values: Vec<ScalarValue> = items
                .iter()
                .map(|item| {
                    if let Expr::Literal(v) = item {
                        cypher_literal_to_scalar(v)
                    } else {
                        Err(anyhow!("Non-literal list elements not supported"))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            if values.is_empty() {
                // Empty list with null type
                let empty_arr = ScalarValue::new_list_nullable(
                    &[],
                    &datafusion::arrow::datatypes::DataType::Null,
                );
                Ok(lit(ScalarValue::List(empty_arr)))
            } else {
                // Infer type from first element (nullable = true)
                let list_arr = ScalarValue::new_list(
                    &values,
                    &values[0].data_type(),
                    true, // nullable
                );
                Ok(lit(ScalarValue::List(list_arr)))
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
            let left_expr = cypher_expr_to_df(expr, context)?;

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
                // For array columns/expressions, use array_has(array, element)
                Ok(datafusion::functions_nested::expr_fn::array_has(
                    right_expr, left_expr,
                ))
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
            // Quantifier expressions require lambda/higher-order functions which DataFusion
            // does not yet support (tracked in https://github.com/apache/datafusion/issues/14205).
            //
            // Example: ALL(x IN list WHERE x > 0) requires:
            //   1. Iterating over array elements
            //   2. Binding each element to variable 'x'
            //   3. Evaluating predicate 'x > 0' with that binding
            //
            // This is equivalent to: list_filter(list, x -> x > 0).length() == list.length()
            //
            // DataFusion v50.3.0 has:
            //   ✅ bool_and/bool_or aggregates
            //   ✅ unnest() for expanding arrays
            //   ✅ array_element, array_slice, array_length
            //   ❌ Lambda functions for predicates
            //
            // DESIGN DECISION: Intentionally fail here and let execution fall back to the
            // fallback executor, which has full quantifier support (see read.rs:663-713).
            // This is the correct behavior until DataFusion adds lambda support.
            Err(anyhow!(
                "Quantifier expressions (ALL/ANY/SINGLE/NONE) not supported - requires DataFusion lambda functions (Issue #14205)"
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

        Expr::MapProjection { .. } => {
            // Map projection is evaluated in the executor, not pushed down to DataFusion
            Err(anyhow!(
                "Map projection cannot be pushed down to DataFusion"
            ))
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

/// Convert a JSON value to a DataFusion scalar value.
fn json_value_to_scalar(value: &Value) -> Result<ScalarValue> {
    match value {
        Value::Null => Ok(ScalarValue::Null),
        Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                Err(anyhow!("Unsupported number type: {}", n))
            }
        }
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        Value::Array(_) => Err(anyhow!("Array literals should use Expr::List")),
        Value::Object(_) => Err(anyhow!("Object literals should use Expr::Map")),
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
            // CONTAINS -> LIKE '%value%'
            // Need to wrap right in CONCAT('%', right, '%')
            let pattern =
                datafusion::functions::string::expr_fn::concat(vec![lit("%"), right, lit("%")]);
            Ok(left.like(pattern))
        }
        BinaryOp::StartsWith => {
            // STARTS WITH -> LIKE 'value%'
            let pattern = datafusion::functions::string::expr_fn::concat(vec![right, lit("%")]);
            Ok(left.like(pattern))
        }
        BinaryOp::EndsWith => {
            // ENDS WITH -> LIKE '%value'
            let pattern = datafusion::functions::string::expr_fn::concat(vec![lit("%"), right]);
            Ok(left.like(pattern))
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
            Ok(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Int64,
            ))
        }
        "TOFLOAT" => {
            require_arg(&df_args, "toFloat")?;
            Ok(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            ))
        }
        "TOBOOLEAN" | "TOBOOL" => {
            require_arg(&df_args, "toBoolean")?;
            Ok(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Boolean,
            ))
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
        "SUBSTRING" | "SUBSTR" => {
            require_args(&df_args, 2, "substring")?;
            // Cypher uses 0-based indexing, DataFusion uses 1-based
            // Convert by adding 1 to the start index
            let adjusted_start = df_args[1].clone() + lit(1i64);

            // For 3-arg case, use left() on substr() result
            if df_args.len() == 2 {
                Ok(datafusion::functions::unicode::expr_fn::substr(
                    df_args[0].clone(),
                    adjusted_start,
                ))
            } else {
                // substr(string, start) gives us from start to end
                // then left(result, length) gives us the first length chars
                let substr_result = datafusion::functions::unicode::expr_fn::substr(
                    df_args[0].clone(),
                    adjusted_start,
                );
                Ok(datafusion::functions::unicode::expr_fn::left(
                    substr_result,
                    df_args[2].clone(),
                ))
            }
        }
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
            // Polymorphic: use array_length for lists, character_length for strings.
            // At expression translation time we don't know the type, so wrap in a
            // CASE on whether array_length returns non-null. If the argument is a
            // string, array_length returns null, so we fall back to character_length.
            let arg = first_arg(&df_args);
            let arr_len = datafusion::functions_nested::expr_fn::array_length(arg.clone());
            Ok(datafusion::functions::expr_fn::coalesce(vec![
                arr_len,
                cast_expr(
                    datafusion::functions::unicode::expr_fn::character_length(arg),
                    datafusion::arrow::datatypes::DataType::Int64,
                ),
            ]))
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
        "SIGN" => {
            require_arg(&df_args, "sign")?;
            // Cast to Float64 for Int64 compatibility (DataFusion signum doesn't support Int64)
            Ok(datafusion::functions::math::expr_fn::signum(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "SQRT" => {
            require_arg(&df_args, "sqrt")?;
            Ok(datafusion::functions::math::expr_fn::sqrt(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "LOG" | "LN" => {
            require_arg(&df_args, "log")?;
            Ok(datafusion::functions::math::expr_fn::ln(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "LOG10" => {
            require_arg(&df_args, "log10")?;
            Ok(datafusion::functions::math::expr_fn::log10(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "EXP" => {
            require_arg(&df_args, "exp")?;
            Ok(datafusion::functions::math::expr_fn::exp(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }

        // Trigonometric functions — cast args to Float64 for Int64 compatibility
        "SIN" => {
            require_arg(&df_args, "sin")?;
            Ok(datafusion::functions::math::expr_fn::sin(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "COS" => {
            require_arg(&df_args, "cos")?;
            Ok(datafusion::functions::math::expr_fn::cos(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "TAN" => {
            require_arg(&df_args, "tan")?;
            Ok(datafusion::functions::math::expr_fn::tan(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "ASIN" => {
            require_arg(&df_args, "asin")?;
            Ok(datafusion::functions::math::expr_fn::asin(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "ACOS" => {
            require_arg(&df_args, "acos")?;
            Ok(datafusion::functions::math::expr_fn::acos(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
        }
        "ATAN" => {
            require_arg(&df_args, "atan")?;
            Ok(datafusion::functions::math::expr_fn::atan(cast_expr(
                first_arg(&df_args),
                datafusion::arrow::datatypes::DataType::Float64,
            )))
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
        | "LOCALDATETIME.REALTIME" => Ok(DfExpr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction {
                func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                    DummyUdf::new(name.to_lowercase()),
                )),
                args: df_args,
            },
        )),

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
            let len = datafusion::functions_nested::expr_fn::array_length(arr.clone());
            Ok(datafusion::functions_nested::expr_fn::array_slice(
                arr,
                lit(2i64),
                len,
                None,
            ))
        }
        "RANGE" => {
            require_args(&df_args, 2, "range")?;
            // Use the range UDF registered in the session context
            Ok(DfExpr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                        DummyUdf::new("range".to_string()),
                    )),
                    args: df_args,
                },
            ))
        }

        // Graph-specific functions (registered as UDFs)
        "ID" => {
            // When called with a bare variable (ID(n)), rewrite to the internal
            // _vid column reference. The IdUdf is just a pass-through, so we can
            // skip it entirely and return the column directly.
            // For edge variables, _vid won't exist and will fall back to legacy.
            if let Some(Expr::Variable(var)) = args.first() {
                Ok(DfExpr::Column(Column::from_name(format!("{}._vid", var))))
            } else {
                Ok(DfExpr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction {
                        func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                            DummyUdf::new("id".to_string()),
                        )),
                        args: df_args,
                    },
                ))
            }
        }
        "TYPE" | "LABELS" | "KEYS" | "PROPERTIES" | "UNI.TEMPORAL.VALIDAT" => {
            // Rewrite bare variable arg to _vid column reference
            let rewritten_args = if let Some(Expr::Variable(var)) = args.first() {
                let vid_col = DfExpr::Column(Column::from_name(format!("{}._vid", var)));
                let mut new_args = vec![vid_col];
                new_args.extend(df_args.into_iter().skip(1));
                new_args
            } else {
                df_args
            };
            Ok(DfExpr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction {
                    func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                        DummyUdf::new(name.to_lowercase()),
                    )),
                    args: rewritten_args,
                },
            ))
        }
        "NODES" | "RELATIONSHIPS" => Ok(DfExpr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction {
                func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                    DummyUdf::new(name.to_lowercase()),
                )),
                args: df_args,
            },
        )),

        // Label predicate: hasLabel(n, 'Label') translates to n._label = 'Label'
        "HASLABEL" => {
            require_args(&df_args, 2, "hasLabel")?;
            // First arg should be a variable, second should be the label string
            if let Some(Expr::Variable(var)) = args.first() {
                if let Some(Expr::Literal(CypherLiteral::String(label))) = args.get(1) {
                    // Translate to: {var}._label = '{label}'
                    let label_col = DfExpr::Column(Column::from_name(format!("{}._label", var)));
                    Ok(label_col.eq(lit(label.clone())))
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
        _ => Ok(DfExpr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction {
                func: Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                    DummyUdf::new(name.to_lowercase()),
                )),
                args: df_args,
            },
        )),
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
                if let uni_cypher::ast::MapProjectionItem::LiteralEntry(_, expr) = item {
                    collect_properties_recursive(expr, properties);
                }
            }
        }
        // Terminal nodes and subqueries (which have their own scope)
        Expr::Wildcard | Expr::Variable(_) | Expr::Parameter(_) | Expr::Literal(_) => {}
        Expr::Exists(_) | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {}
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
