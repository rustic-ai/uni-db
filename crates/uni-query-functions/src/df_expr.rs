// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

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
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, col, expr::InList, lit,
};
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

/// Returns true if the type is a primitive (non-compound) type for coercion purposes.
///
/// Compound types (LargeBinary, Struct, List, LargeList) require special handling
/// via UDFs and cannot use the standard coercion paths.
fn is_primitive_type(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    !matches!(
        dt,
        datafusion::arrow::datatypes::DataType::LargeBinary
            | datafusion::arrow::datatypes::DataType::Struct(_)
            | datafusion::arrow::datatypes::DataType::List(_)
            | datafusion::arrow::datatypes::DataType::LargeList(_)
    )
}

/// Extract a named field from a struct expression using DataFusion's `get_field` function.
pub fn struct_getfield(expr: DfExpr, field_name: &str) -> DfExpr {
    use datafusion::logical_expr::ScalarUDF;
    DfExpr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(
            datafusion::functions::core::getfield::GetFieldFunc::new(),
        )),
        vec![expr, lit(field_name)],
    ))
}

/// Extract the `nanos_since_epoch` field from a DateTime struct expression.
pub fn extract_datetime_nanos(expr: DfExpr) -> DfExpr {
    struct_getfield(expr, "nanos_since_epoch")
}

/// Extract the UTC-normalized time in nanoseconds from a Time struct expression.
///
/// Cypher Time stores `nanos_since_midnight` as *local* time nanoseconds. To compare
/// two Times correctly, we need to normalize to UTC by computing:
/// `nanos_since_midnight - (offset_seconds * 1_000_000_000)`
///
/// This ensures that `12:00+01:00` and `11:00Z` (same UTC instant) are equal.
pub fn extract_time_nanos(expr: DfExpr) -> DfExpr {
    use datafusion::logical_expr::Operator;

    let nanos_local = struct_getfield(expr.clone(), "nanos_since_midnight");
    let offset_seconds = struct_getfield(expr, "offset_seconds");

    // Normalize to UTC: nanos_since_midnight - (offset_seconds * 1_000_000_000)
    // nanos_since_midnight is Time64(Nanosecond); cast to Int64 for arithmetic.
    // offset_seconds is Int32; cast to Int64, multiply by 1B, subtract from nanos.
    let nanos_local_i64 = cast_expr(nanos_local, datafusion::arrow::datatypes::DataType::Int64);
    let offset_nanos = DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
        Box::new(cast_expr(
            offset_seconds,
            datafusion::arrow::datatypes::DataType::Int64,
        )),
        Operator::Multiply,
        Box::new(lit(1_000_000_000_i64)),
    ));

    DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
        Box::new(nanos_local_i64),
        Operator::Minus,
        Box::new(offset_nanos),
    ))
}

/// Normalize a datetime string literal to RFC3339 format for Arrow timestamp parsing.
///
/// Arrow's timestamp parser requires explicit seconds (`HH:MM:SS`), but our Cypher
/// datetime formatting omits `:00` seconds when both seconds and nanos are zero
/// (e.g. `2021-06-01T00:00Z`). This function inserts `:00` seconds when missing so
/// the string can be cast to Arrow Timestamp.
fn normalize_datetime_literal(expr: DfExpr) -> DfExpr {
    if let DfExpr::Literal(ScalarValue::Utf8(Some(ref s)), _) = expr
        && let Some(normalized) = normalize_datetime_str(s)
    {
        return lit(normalized);
    }
    expr
}

/// Insert `:00` seconds into a datetime string like `2021-06-01T00:00Z` that has
/// only `HH:MM` after the `T` separator (no seconds component).
pub fn normalize_datetime_str(s: &str) -> Option<String> {
    // Must be at least YYYY-MM-DDTHH:MM (16 chars) with T at position 10
    if s.len() < 16 || s.as_bytes().get(10) != Some(&b'T') {
        return None;
    }
    let b = s.as_bytes();
    if !(b[11].is_ascii_digit()
        && b[12].is_ascii_digit()
        && b[13] == b':'
        && b[14].is_ascii_digit()
        && b[15].is_ascii_digit())
    {
        return None;
    }
    // If there's already a seconds component (char at 16 is ':'), no normalization needed
    if b.len() > 16 && b[16] == b':' {
        return None;
    }
    // Insert :00 after HH:MM
    let mut normalized = String::with_capacity(s.len() + 3);
    normalized.push_str(&s[..16]);
    normalized.push_str(":00");
    if s.len() > 16 {
        normalized.push_str(&s[16..]);
    }
    Some(normalized)
}

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

/// Entity kind of a variable in the physical query context.
///
/// Used to determine the identity column when a bare variable is referenced
/// (e.g., `n` in `RETURN n` should resolve to `n._vid` for nodes).
///
/// This is the physical-layer counterpart to `VariableType` (in the `uni-query`
/// crate's `planner` module), which includes additional variants
/// (`Scalar`, `ScalarLiteral`, `Imported`)
/// for logical planning. `VariableKind` only tracks graph-entity types needed
/// for physical expression compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariableKind {
    /// Node variable - identity is `_vid`
    Node,
    /// Edge/relationship variable - identity is `_eid`
    Edge,
    /// Edge list variable (r in `[r*]`) - `List<Edge>`
    EdgeList,
    /// Node list variable — a GQL group variable bound by a quantified path
    /// pattern - `List<Node>`. Resolved as a bare column like [`Self::EdgeList`].
    NodeList,
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
        // count(*) is the only Cypher path that produces Expr::Wildcard here;
        // RETURN * expansion is handled at the planner level.  Map to literal 1
        // so count(*) → count(1), avoiding the deprecated DfExpr::Wildcard.
        Expr::Wildcard => Ok(DfExpr::Literal(
            datafusion::common::ScalarValue::Int32(Some(1)),
            None,
        )),

        Expr::Variable(name) => {
            // Priority 1: Known structural variable (Node/Edge/Path)
            // Use Column::from_name() to avoid treating dots as table.column qualifiers.
            // When the variable kind is known, return the column representing the whole
            // entity. The struct is built by add_structural_projection() in the planner.
            if let Some(ctx) = context
                && ctx.variable_kinds.contains_key(name)
            {
                return Ok(DfExpr::Column(Column::from_name(name)));
            }

            // Priority 2: Correlated outer values (from Apply input rows)
            // These take precedence over parameters to prevent YIELD columns from
            // shadowing user query parameters. For example, if a procedure yields a
            // column named 'vid' and the user has a $vid parameter, the variable 'vid'
            // should resolve to the YIELD column, not the user parameter.
            if let Some(ctx) = context
                && let Some(value) = ctx.outer_values.get(name)
            {
                return value_to_scalar(value).map(lit);
            }

            // Priority 3: Query parameters / CTE working tables
            // Check if the variable name matches a parameter (e.g., CTE working table
            // injected as a parameter). This allows `WHERE x IN hierarchy` to resolve
            // `hierarchy` from params when it's not a schema column.
            if let Some(ctx) = context
                && let Some(value) = ctx.parameters.get(name)
            {
                // Handle batched correlation parameters: Value::List converts to IN list
                // ONLY for correlation keys (ending with ._vid), not general list parameters
                match value {
                    Value::List(values) if name.ends_with("._vid") => {
                        // Batch mode for correlation parameters: generate IN list
                        let literals = values
                            .iter()
                            .map(|v| value_to_scalar(v).map(lit))
                            .collect::<Result<Vec<_>>>()?;
                        return Ok(DfExpr::InList(InList {
                            expr: Box::new(DfExpr::Column(Column::from_name(name))),
                            list: literals,
                            negated: false,
                        }));
                    }
                    other_value => return value_to_scalar(other_value).map(lit),
                }
            }

            // Priority 4: Column fallback
            // If none of the above match, treat it as a column reference.
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
            // Pass raw 0-based indices to _cypher_list_slice which handles
            // null bounds, negative indices, and clamping.
            let array_expr = cypher_expr_to_df(array, context)?;

            let start_expr = match start {
                Some(s) => cypher_expr_to_df(s, context)?,
                None => lit(0i64),
            };

            let end_expr = match end {
                Some(e) => cypher_expr_to_df(e, context)?,
                None => lit(i64::MAX),
            };

            // Always use _cypher_list_slice UDF — it handles CypherValue-encoded
            // lists, null bounds, and negative index resolution correctly.
            Ok(dummy_udf_expr(
                "_cypher_list_slice",
                vec![array_expr, start_expr, end_expr],
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

        Expr::IsNull(inner) => translate_null_check(inner, context, true),

        Expr::IsNotNull(inner) => translate_null_check(inner, context, false),

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
            "EXISTS subqueries are handled by the physical expression compiler, \
             not the DataFusion logical expression translator"
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

        Expr::LabelCheck { expr, labels } => {
            if let Expr::Variable(var) = expr.as_ref() {
                // Check if variable is an edge (uses _type) or node (uses _labels)
                let is_edge = context
                    .and_then(|ctx| ctx.variable_kinds.get(var))
                    .is_some_and(|k| matches!(k, VariableKind::Edge));

                if is_edge {
                    // Edges have a single type: check _type_name = label
                    // For conjunctive labels on edges (e.g., r:A:B), this is always false
                    // since edges have exactly one type
                    if labels.len() > 1 {
                        Ok(lit(false))
                    } else {
                        let type_col =
                            DfExpr::Column(Column::from_name(format!("{}.{}", var, COL_TYPE)));
                        // CASE WHEN _type IS NULL THEN NULL ELSE _type = 'label' END
                        Ok(DfExpr::Case(datafusion::logical_expr::Case {
                            expr: None,
                            when_then_expr: vec![(
                                Box::new(type_col.clone().is_null()),
                                Box::new(DfExpr::Literal(ScalarValue::Boolean(None), None)),
                            )],
                            else_expr: Some(Box::new(type_col.eq(lit(labels[0].clone())))),
                        }))
                    }
                } else {
                    // Node: check _labels array contains all specified labels
                    let labels_col =
                        DfExpr::Column(Column::from_name(format!("{}.{}", var, COL_LABELS)));
                    let checks = labels
                        .iter()
                        .map(|label| {
                            datafusion::functions_nested::expr_fn::array_has(
                                labels_col.clone(),
                                lit(label.clone()),
                            )
                        })
                        .reduce(|acc, check| acc.and(check));
                    // Wrap in CASE WHEN _labels IS NULL THEN NULL ELSE ... END
                    Ok(DfExpr::Case(datafusion::logical_expr::Case {
                        expr: None,
                        when_then_expr: vec![(
                            Box::new(labels_col.is_null()),
                            Box::new(DfExpr::Literal(ScalarValue::Boolean(None), None)),
                        )],
                        else_expr: Some(Box::new(checks.unwrap())),
                    }))
                }
            } else {
                Err(anyhow!(
                    "LabelCheck on non-variable expression not yet supported in DataFusion"
                ))
            }
        }
    }
}

/// Context for expression translation.
///
/// Provides parameter values and schema information for resolving expressions.
#[derive(Debug, Clone)]
pub struct TranslationContext {
    /// Parameter values for query parameterization.
    pub parameters: std::collections::HashMap<String, Value>,

    /// Correlated outer values from Apply input rows (for subquery correlation).
    /// These take precedence over parameters during variable resolution to prevent
    /// YIELD columns from shadowing user query parameters.
    pub outer_values: std::collections::HashMap<String, Value>,

    /// Known variable to label mapping (for type inference).
    pub variable_labels: std::collections::HashMap<String, String>,

    /// Variable kinds (node, edge, path) for identity column resolution.
    pub variable_kinds: std::collections::HashMap<String, VariableKind>,

    /// Node variable names from CREATE/MERGE patterns (separate from variable_kinds
    /// to avoid affecting property access translation). Used by startNode/endNode UDFs.
    pub node_variable_hints: Vec<String>,

    /// Edge variable names from CREATE/MERGE patterns. Used by `id()` to resolve
    /// edge identity as `_eid` instead of the default `_vid`.
    pub mutation_edge_hints: Vec<String>,

    /// Frozen statement clock for consistent temporal function evaluation.
    /// All bare temporal constructors (`time()`, `datetime()`, etc.) and their
    /// `.statement()`/`.transaction()` variants use this frozen instant so that
    /// `duration.inSeconds(time(), time())` returns zero.
    pub statement_time: chrono::DateTime<chrono::Utc>,
}

impl Default for TranslationContext {
    fn default() -> Self {
        Self {
            parameters: std::collections::HashMap::new(),
            outer_values: std::collections::HashMap::new(),
            variable_labels: std::collections::HashMap::new(),
            variable_kinds: std::collections::HashMap::new(),
            node_variable_hints: Vec::new(),
            mutation_edge_hints: Vec::new(),
            statement_time: chrono::Utc::now(),
        }
    }
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

/// Translate IS NULL / IS NOT NULL, resolving entity variables to their identity column.
fn translate_null_check(
    inner: &Expr,
    context: Option<&TranslationContext>,
    is_null: bool,
) -> Result<DfExpr> {
    if let Expr::Variable(var) = inner
        && let Some(ctx) = context
        && let Some(kind) = ctx.variable_kinds.get(var)
    {
        let col_name = match kind {
            VariableKind::Node => format!("{}.{}", var, COL_VID),
            VariableKind::Edge => format!("{}.{}", var, COL_EID),
            VariableKind::Path | VariableKind::EdgeList | VariableKind::NodeList => var.clone(),
        };
        let col_expr = DfExpr::Column(Column::from_name(col_name));
        return Ok(if is_null {
            col_expr.is_null()
        } else {
            col_expr.is_not_null()
        });
    }

    let inner_expr = cypher_expr_to_df(inner, context)?;
    Ok(if is_null {
        inner_expr.is_null()
    } else {
        inner_expr.is_not_null()
    })
}

/// Try to translate a property access as a temporal/duration accessor.
///
/// Returns `Some(expr)` if `prop` is a duration or temporal accessor,
/// `None` otherwise.
fn try_temporal_accessor(base_expr: DfExpr, prop: &str) -> Option<DfExpr> {
    if crate::datetime::is_duration_accessor(prop) {
        Some(dummy_udf_expr(
            "_duration_property",
            vec![base_expr, lit(prop.to_string())],
        ))
    } else if crate::datetime::is_temporal_accessor(prop) {
        Some(dummy_udf_expr(
            "_temporal_property",
            vec![base_expr, lit(prop.to_string())],
        ))
    } else {
        None
    }
}

/// Translate a property access expression (e.g., `n.name`) to DataFusion.
fn translate_property_access(
    base: &Expr,
    prop: &str,
    context: Option<&TranslationContext>,
) -> Result<DfExpr> {
    if let Ok(var_name) = extract_variable_name(base) {
        let is_graph_entity = context
            .and_then(|ctx| ctx.variable_kinds.get(&var_name))
            .is_some_and(|k| matches!(k, VariableKind::Node | VariableKind::Edge));

        if !is_graph_entity
            && let Some(expr) =
                try_temporal_accessor(DfExpr::Column(Column::from_name(&var_name)), prop)
        {
            return Ok(expr);
        }

        let col_name = format!("{}.{}", var_name, prop);

        // Check if this property is available as a correlated parameter
        // (e.g., in CALL subqueries where outer columns are injected as params).
        if let Some(ctx) = context
            && let Some(value) = ctx.parameters.get(&col_name)
        {
            // Handle batched correlation parameters: Value::List converts to IN list
            // ONLY for correlation keys (ending with ._vid), not general list parameters
            match value {
                Value::List(values) if col_name.ends_with("._vid") => {
                    let literals = values
                        .iter()
                        .map(|v| value_to_scalar(v).map(lit))
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(DfExpr::InList(InList {
                        expr: Box::new(DfExpr::Column(Column::from_name(&col_name))),
                        list: literals,
                        negated: false,
                    }));
                }
                other_value => return value_to_scalar(other_value).map(lit),
            }
        }

        // Nested property access on non-graph variable (e.g., m.a.b where m is a map):
        // recursively translate the base expression and chain index() calls.
        if !is_graph_entity && matches!(base, Expr::Property(_, _)) {
            let base_expr = cypher_expr_to_df(base, context)?;
            return Ok(dummy_udf_expr(
                "index",
                vec![base_expr, lit(prop.to_string())],
            ));
        }

        if is_graph_entity {
            Ok(DfExpr::Column(Column::from_name(col_name)))
        } else {
            let base_expr = DfExpr::Column(Column::from_name(var_name));
            Ok(dummy_udf_expr(
                "index",
                vec![base_expr, lit(prop.to_string())],
            ))
        }
    } else {
        // Base is a complex expression (e.g., function call result, array index, parameter).
        if let Some(expr) = try_temporal_accessor(cypher_expr_to_df(base, context)?, prop) {
            return Ok(expr);
        }

        // Special case: Parameter base (e.g., $session.tenant_id).
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
    let mut has_graph_entity = false;
    let mut has_temporal = false;

    for item in items {
        match item {
            Expr::Literal(CypherLiteral::Float(_)) | Expr::Literal(CypherLiteral::Integer(_)) => {
                has_numeric = true
            }
            Expr::Literal(CypherLiteral::String(_)) => has_string = true,
            Expr::Literal(CypherLiteral::Bool(_)) => has_bool = true,
            Expr::List(_) => has_list = true,
            Expr::Map(_) => has_map = true,
            // Check if a variable is a graph entity (Node/Edge/Path) — these have struct
            // Arrow types that cannot be unified with scalar types in make_array.
            Expr::Variable(name)
                if context
                    .and_then(|ctx| ctx.variable_kinds.get(name))
                    .is_some() =>
            {
                has_graph_entity = true;
            }
            // Temporal function calls produce Timestamp/Date32/Struct types that
            // make_array cannot unify. Route through _make_cypher_list instead.
            Expr::FunctionCall { name, .. } => {
                let upper = name.to_uppercase();
                if matches!(
                    upper.as_str(),
                    "DATE"
                        | "TIME"
                        | "LOCALTIME"
                        | "LOCALDATETIME"
                        | "DATETIME"
                        | "DURATION"
                        | "DATE.TRUNCATE"
                        | "TIME.TRUNCATE"
                        | "DATETIME.TRUNCATE"
                        | "LOCALDATETIME.TRUNCATE"
                        | "LOCALTIME.TRUNCATE"
                ) {
                    has_temporal = true;
                }
            }
            // Treat Null as compatible with anything
            _ => {}
        }
    }

    // Check distinct non-null types count
    let types_count = has_numeric as u8 + has_string as u8 + has_bool as u8 + has_map as u8;

    // Mixed types, nested lists, graph entities, or temporal function calls:
    // encode as LargeBinary CypherValue to avoid Arrow type unification failures.
    if has_list || has_map || types_count > 1 || has_graph_entity || has_temporal {
        // Try to convert all items to JSON values for CypherValue encoding
        if let Some(json_array) = try_items_to_json(items) {
            let uni_val: uni_common::Value = serde_json::Value::Array(json_array).into();
            let cv_bytes = uni_common::cypher_value_codec::encode(&uni_val);
            return Ok(lit(ScalarValue::LargeBinary(Some(cv_bytes))));
        }
        // Non-literal items (e.g. variables): delegate to _make_cypher_list UDF
        let df_args: Vec<DfExpr> = items
            .iter()
            .map(|item| cypher_expr_to_df(item, context))
            .collect::<Result<_>>()?;
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
        // When the list contains Null-typed literals mixed with typed values,
        // cast the nulls to the dominant non-null type so that make_array's
        // planned return type matches its runtime return type (DF 52+).
        let non_null_type = df_args.iter().find_map(|e| {
            if let DfExpr::Literal(sv, _) = e {
                let dt = sv.data_type();
                if dt != datafusion::arrow::datatypes::DataType::Null {
                    return Some(dt);
                }
            }
            None
        });
        if let Some(ref target_type) = non_null_type {
            let coerced = df_args
                .into_iter()
                .map(|e| {
                    if matches!(&e, DfExpr::Literal(sv, _) if sv.data_type() == datafusion::arrow::datatypes::DataType::Null)
                    {
                        cast_expr(e, target_type.clone())
                    } else {
                        e
                    }
                })
                .collect();
            Ok(datafusion::functions_nested::expr_fn::make_array(coerced))
        } else {
            Ok(datafusion::functions_nested::expr_fn::make_array(df_args))
        }
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
                    VariableKind::Edge => COL_EID,
                    _ => unreachable!(),
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
        CypherLiteral::Bytes(b) => Ok(ScalarValue::LargeBinary(Some(b.clone()))),
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
                        // Already-binary scalars (e.g. from Value::Bytes) must pass
                        // through verbatim — stringifying them would base64/Debug-
                        // mangle the raw bytes (#100 family).
                        (
                            s @ ScalarValue::LargeBinary(_),
                            datafusion::arrow::datatypes::DataType::LargeBinary,
                        ) => s,
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

            if entries.is_empty() {
                return Ok(ScalarValue::Struct(Arc::new(
                    datafusion::arrow::array::StructArray::new_empty_fields(1, None),
                )));
            }

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
        Value::Temporal(tv) => {
            use uni_common::TemporalValue;
            match tv {
                TemporalValue::Date { days_since_epoch } => {
                    Ok(ScalarValue::Date32(Some(*days_since_epoch)))
                }
                TemporalValue::LocalTime {
                    nanos_since_midnight,
                } => Ok(ScalarValue::Time64Nanosecond(Some(*nanos_since_midnight))),
                TemporalValue::Time {
                    nanos_since_midnight,
                    offset_seconds,
                } => {
                    // Build single-row StructArray for ScalarValue
                    use arrow::array::{ArrayRef, Int32Array, StructArray, Time64NanosecondArray};
                    use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, TimeUnit};

                    let nanos_arr =
                        Arc::new(Time64NanosecondArray::from(vec![*nanos_since_midnight]))
                            as ArrayRef;
                    let offset_arr = Arc::new(Int32Array::from(vec![*offset_seconds])) as ArrayRef;

                    let fields = Fields::from(vec![
                        Field::new(
                            "nanos_since_midnight",
                            ArrowDataType::Time64(TimeUnit::Nanosecond),
                            true,
                        ),
                        Field::new("offset_seconds", ArrowDataType::Int32, true),
                    ]);

                    let struct_arr = StructArray::new(fields, vec![nanos_arr, offset_arr], None);
                    Ok(ScalarValue::Struct(Arc::new(struct_arr)))
                }
                TemporalValue::LocalDateTime { nanos_since_epoch } => Ok(
                    ScalarValue::TimestampNanosecond(Some(*nanos_since_epoch), None),
                ),
                TemporalValue::DateTime {
                    nanos_since_epoch,
                    offset_seconds,
                    timezone_name,
                } => {
                    // Build single-row StructArray for ScalarValue
                    use arrow::array::{
                        ArrayRef, Int32Array, StringArray, StructArray, TimestampNanosecondArray,
                    };
                    use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, TimeUnit};

                    let nanos_arr =
                        Arc::new(TimestampNanosecondArray::from(vec![*nanos_since_epoch]))
                            as ArrayRef;
                    let offset_arr = Arc::new(Int32Array::from(vec![*offset_seconds])) as ArrayRef;
                    let tz_arr =
                        Arc::new(StringArray::from(vec![timezone_name.clone()])) as ArrayRef;

                    let fields = Fields::from(vec![
                        Field::new(
                            "nanos_since_epoch",
                            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                            true,
                        ),
                        Field::new("offset_seconds", ArrowDataType::Int32, true),
                        Field::new("timezone_name", ArrowDataType::Utf8, true),
                    ]);

                    let struct_arr =
                        StructArray::new(fields, vec![nanos_arr, offset_arr, tz_arr], None);
                    Ok(ScalarValue::Struct(Arc::new(struct_arr)))
                }
                TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                } => Ok(ScalarValue::IntervalMonthDayNano(Some(
                    arrow::datatypes::IntervalMonthDayNano {
                        months: *months as i32,
                        days: *days as i32,
                        nanoseconds: *nanos,
                    },
                ))),
                TemporalValue::Btic { lo, hi, meta } => {
                    let btic = uni_btic::Btic::new(*lo, *hi, *meta)
                        .map_err(|e| anyhow::anyhow!("invalid BTIC value: {}", e))?;
                    let packed = uni_btic::encode::encode(&btic);
                    Ok(ScalarValue::FixedSizeBinary(24, Some(packed.to_vec())))
                }
            }
        }
        Value::Vector(v) => {
            // Encode as CypherValue LargeBinary so arrow_to_value_at decodes it correctly
            let cv_bytes = uni_common::cypher_value_codec::encode(&Value::Vector(v.clone()));
            Ok(ScalarValue::LargeBinary(Some(cv_bytes)))
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

        // Arithmetic operators — emitted as native DF ops here because types are
        // not yet known at translation time. The Int64×Int64 checked-UDF routing
        // (for overflow / division by zero) is applied later in
        // `coerce_binary_expr`, the type-coercion convergence point. The `+`
        // list-concat special case still routes to `_cypher_list_concat`.
        BinaryOp::Add => {
            if is_list_expr(&left) || is_list_expr(&right) {
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
            // Coerce operands to Float64 to prevent integer overflow panics and
            // ensure Float return type per Cypher semantics. Use the Cypher-aware
            // coercion rather than a raw cast so a cv-encoded numeric carried as
            // LargeBinary (schemaless property arithmetic) decodes instead of
            // hitting the unsupported `LargeBinary -> Float64` cast (issue #111
            // family).
            let left_f = crate::df_udfs::cypher_to_float64_expr(left);
            let right_f = crate::df_udfs::cypher_to_float64_expr(right);
            Ok(datafusion::functions::math::expr_fn::power(left_f, right_f))
        }

        // String operators - use Cypher UDFs for safe type handling
        BinaryOp::Contains => Ok(dummy_udf_expr("_cypher_contains", vec![left, right])),
        BinaryOp::StartsWith => Ok(dummy_udf_expr("_cypher_starts_with", vec![left, right])),
        BinaryOp::EndsWith => Ok(dummy_udf_expr("_cypher_ends_with", vec![left, right])),

        BinaryOp::Regex => {
            // Cypher `=~` is three-valued: `null =~ p` and `s =~ null` are null,
            // not false. `regexp_match(s, p) IS NOT NULL` alone collapses a null
            // operand to false (the match yields NULL both for "no match" and for
            // a null input), so guard the null-operand case explicitly and let
            // NULL keep propagating.
            let is_match =
                datafusion::functions::expr_fn::regexp_match(left.clone(), right.clone(), None)
                    .is_not_null();
            Ok(datafusion::logical_expr::when(
                left.is_null().or(right.is_null()),
                lit(ScalarValue::Boolean(None)),
            )
            .otherwise(is_match)?)
        }

        BinaryOp::ApproxEq => Err(anyhow!(
            "Vector similarity operator (~=) cannot be pushed down to DataFusion"
        )),
    }
}

/// Early-return `Some(Err(...))` from an `Option<Result<...>>` function if the args
/// slice has fewer than the required number of arguments.
///
/// Used by the `translate_*_function` family which returns `Option<Result<DfExpr>>`.
macro_rules! check_args {
    (1, $df_args:expr, $name:expr) => {
        if let Err(e) = require_arg($df_args, $name) {
            return Some(Err(e));
        }
    };
    ($n:expr, $df_args:expr, $name:expr) => {
        if let Err(e) = require_args($df_args, $n, $name) {
            return Some(Err(e));
        }
    };
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
pub fn cast_expr(expr: DfExpr, data_type: datafusion::arrow::datatypes::DataType) -> DfExpr {
    DfExpr::Cast(datafusion::logical_expr::Cast {
        expr: Box::new(expr),
        data_type,
    })
}

/// Wrap a `List<T>` or `LargeList<T>` expression as a `LargeBinary` CypherValue.
///
/// Arrow cannot cast `List<T>` → `LargeBinary` natively, so we route through
/// the `_cypher_list_to_cv` UDF. Used by `coerce_branch_to` when CASE branches
/// have mixed `LargeList<T>` and `LargeBinary` types.
pub fn list_to_large_binary_expr(expr: DfExpr) -> DfExpr {
    DfExpr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        Arc::new(crate::df_udfs::create_cypher_list_to_cv_udf()),
        vec![expr],
    ))
}

/// Wrap a native scalar expression (Int64, Float64, Utf8, Boolean, etc.) in the
/// `_cypher_scalar_to_cv` UDF so it becomes CypherValue-encoded LargeBinary.
/// Used to normalize mixed-type coalesce arguments.
pub fn scalar_to_large_binary_expr(expr: DfExpr) -> DfExpr {
    DfExpr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        Arc::new(crate::df_udfs::create_cypher_scalar_to_cv_udf()),
        vec![expr],
    ))
}

/// Build a `BinaryExpr` from left, operator, and right expressions.
fn binary_expr(left: DfExpr, op: datafusion::logical_expr::Operator, right: DfExpr) -> DfExpr {
    DfExpr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
        Box::new(left),
        op,
        Box::new(right),
    ))
}

/// Map a comparison operator to its `_cypher_*` UDF name.
///
/// Returns `None` for non-comparison operators, allowing callers to decide
/// whether to `unreachable!()` or fall through.
pub fn comparison_udf_name(op: datafusion::logical_expr::Operator) -> Option<&'static str> {
    use datafusion::logical_expr::Operator;
    match op {
        Operator::Eq => Some("_cypher_equal"),
        Operator::NotEq => Some("_cypher_not_equal"),
        Operator::Lt => Some("_cypher_lt"),
        Operator::LtEq => Some("_cypher_lt_eq"),
        Operator::Gt => Some("_cypher_gt"),
        Operator::GtEq => Some("_cypher_gt_eq"),
        _ => None,
    }
}

/// Map an arithmetic operator to its `_cypher_*` UDF name.
fn arithmetic_udf_name(op: datafusion::logical_expr::Operator) -> Option<&'static str> {
    use datafusion::logical_expr::Operator;
    match op {
        Operator::Plus => Some("_cypher_add"),
        Operator::Minus => Some("_cypher_sub"),
        Operator::Multiply => Some("_cypher_mul"),
        Operator::Divide => Some("_cypher_div"),
        Operator::Modulo => Some("_cypher_mod"),
        _ => None,
    }
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
    // Use the Cypher-aware Float64 coercion rather than a raw `CAST(_, Float64)`.
    // A typed value carried as `LargeBinary` (cv_encoded Duration/Btic/etc.) has
    // no DataFusion `LargeBinary -> Float64` kernel, so a blind cast fails at
    // PLAN time and — because Locy plans all rules together — poisons every
    // co-registered rule (issue #111). `cypher_to_float64_expr` decodes a
    // numeric CypherValue and yields a clean per-row NULL for non-numeric values
    // (e.g. `exp(duration)`), matching SIGN/ABS/AVG which already use it.
    Ok(math_fn(crate::df_udfs::cypher_to_float64_expr(first_arg(
        df_args,
    ))))
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
            check_args!(1, df_args, "SUM");
            let udaf = Arc::new(crate::df_udfs::create_cypher_sum_udaf());
            Some(maybe_distinct(
                udaf.call(vec![first_arg(df_args)]),
                distinct,
                "SUM",
            ))
        }
        "AVG" => {
            check_args!(1, df_args, "AVG");
            let coerced = crate::df_udfs::cypher_to_float64_expr(first_arg(df_args));
            let expr = datafusion::functions_aggregate::average::avg(coerced);
            Some(maybe_distinct(expr, distinct, "AVG"))
        }
        "MIN" => {
            check_args!(1, df_args, "MIN");
            let udaf = Arc::new(crate::df_udfs::create_cypher_min_udaf());
            Some(Ok(udaf.call(vec![first_arg(df_args)])))
        }
        "MAX" => {
            check_args!(1, df_args, "MAX");
            let udaf = Arc::new(crate::df_udfs::create_cypher_max_udaf());
            Some(Ok(udaf.call(vec![first_arg(df_args)])))
        }
        "PERCENTILEDISC" => {
            if df_args.len() != 2 {
                return Some(Err(anyhow!(
                    "percentileDisc() requires exactly 2 arguments"
                )));
            }
            let coerced = crate::df_udfs::cypher_to_float64_expr(df_args[0].clone());
            let udaf = Arc::new(crate::df_udfs::create_cypher_percentile_disc_udaf());
            Some(Ok(udaf.call(vec![coerced, df_args[1].clone()])))
        }
        "PERCENTILECONT" => {
            if df_args.len() != 2 {
                return Some(Err(anyhow!(
                    "percentileCont() requires exactly 2 arguments"
                )));
            }
            let coerced = crate::df_udfs::cypher_to_float64_expr(df_args[0].clone());
            let udaf = Arc::new(crate::df_udfs::create_cypher_percentile_cont_udaf());
            Some(Ok(udaf.call(vec![coerced, df_args[1].clone()])))
        }
        "COLLECT" => {
            check_args!(1, df_args, "COLLECT");
            Some(Ok(crate::df_udfs::create_cypher_collect_expr(
                first_arg(df_args),
                distinct,
            )))
        }
        // BTIC aggregates
        "BTIC_MIN" => {
            check_args!(1, df_args, "btic_min");
            let udaf = Arc::new(crate::df_udfs::create_btic_min_udaf());
            Some(Ok(udaf.call(vec![first_arg(df_args)])))
        }
        "BTIC_MAX" => {
            check_args!(1, df_args, "btic_max");
            let udaf = Arc::new(crate::df_udfs::create_btic_max_udaf());
            Some(Ok(udaf.call(vec![first_arg(df_args)])))
        }
        "BTIC_SPAN_AGG" => {
            check_args!(1, df_args, "btic_span_agg");
            let udaf = Arc::new(crate::df_udfs::create_btic_span_agg_udaf());
            Some(Ok(udaf.call(vec![first_arg(df_args)])))
        }
        "BTIC_COUNT_AT" => {
            if df_args.len() != 2 {
                return Some(Err(anyhow!("btic_count_at requires 2 arguments")));
            }
            let udaf = Arc::new(crate::df_udfs::create_btic_count_at_udaf());
            Some(Ok(udaf.call(df_args.to_vec())))
        }
        _ => None,
    }
}

/// Try to translate a string function.
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_string_function(name_upper: &str, df_args: &[DfExpr]) -> Option<Result<DfExpr>> {
    match name_upper {
        "TOSTRING" => {
            check_args!(1, df_args, "toString");
            Some(Ok(dummy_udf_expr("tostring", df_args.to_vec())))
        }
        "TOINTEGER" | "TOINT" => {
            check_args!(1, df_args, "toInteger");
            Some(Ok(dummy_udf_expr("toInteger", df_args.to_vec())))
        }
        "TOFLOAT" => {
            check_args!(1, df_args, "toFloat");
            Some(Ok(dummy_udf_expr("toFloat", df_args.to_vec())))
        }
        "TOBOOLEAN" | "TOBOOL" => {
            check_args!(1, df_args, "toBoolean");
            Some(Ok(dummy_udf_expr("toBoolean", df_args.to_vec())))
        }
        "UPPER" | "TOUPPER" => {
            check_args!(1, df_args, "upper");
            Some(Ok(datafusion::functions::string::expr_fn::upper(
                first_arg(df_args),
            )))
        }
        "LOWER" | "TOLOWER" => {
            check_args!(1, df_args, "lower");
            Some(Ok(datafusion::functions::string::expr_fn::lower(
                first_arg(df_args),
            )))
        }
        "SUBSTRING" => {
            check_args!(2, df_args, "substring");
            Some(Ok(dummy_udf_expr("_cypher_substring", df_args.to_vec())))
        }
        "TRIM" => {
            check_args!(1, df_args, "TRIM");
            Some(Ok(datafusion::functions::string::expr_fn::btrim(vec![
                first_arg(df_args),
            ])))
        }
        "LTRIM" => {
            check_args!(1, df_args, "LTRIM");
            Some(Ok(datafusion::functions::string::expr_fn::ltrim(vec![
                first_arg(df_args),
            ])))
        }
        "RTRIM" => {
            check_args!(1, df_args, "RTRIM");
            Some(Ok(datafusion::functions::string::expr_fn::rtrim(vec![
                first_arg(df_args),
            ])))
        }
        "LEFT" => {
            check_args!(2, df_args, "left");
            Some(Ok(datafusion::functions::unicode::expr_fn::left(
                df_args[0].clone(),
                df_args[1].clone(),
            )))
        }
        "RIGHT" => {
            check_args!(2, df_args, "right");
            Some(Ok(datafusion::functions::unicode::expr_fn::right(
                df_args[0].clone(),
                df_args[1].clone(),
            )))
        }
        "REPLACE" => {
            check_args!(3, df_args, "replace");
            Some(Ok(datafusion::functions::string::expr_fn::replace(
                df_args[0].clone(),
                df_args[1].clone(),
                df_args[2].clone(),
            )))
        }
        "REVERSE" => {
            check_args!(1, df_args, "reverse");
            Some(Ok(dummy_udf_expr("_cypher_reverse", df_args.to_vec())))
        }
        "SPLIT" => {
            check_args!(2, df_args, "split");
            Some(Ok(dummy_udf_expr("_cypher_split", df_args.to_vec())))
        }
        "SIZE" | "LENGTH" => {
            check_args!(1, df_args, name_upper);
            Some(Ok(dummy_udf_expr("_cypher_size", df_args.to_vec())))
        }
        _ => None,
    }
}

/// Try to translate a math function.
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_math_function(name_upper: &str, df_args: &[DfExpr]) -> Option<Result<DfExpr>> {
    use datafusion::functions::math::expr_fn;

    // Helper: apply a unary math function that takes a single Float64 arg
    let unary_f64 =
        |name: &str, f: fn(DfExpr) -> DfExpr| Some(apply_unary_math_f64(df_args, name, f));

    match name_upper {
        "ABS" => {
            check_args!(1, df_args, "abs");
            // Use Cypher-aware abs to handle cv_encoded (LargeBinary)
            // arguments from schemaless property arithmetic while
            // preserving integer/float type semantics.
            Some(Ok(crate::df_udfs::cypher_abs_expr(first_arg(df_args))))
        }
        "CEIL" | "CEILING" => {
            check_args!(1, df_args, "ceil");
            // Cypher-aware Float64 coercion: a cv-encoded numeric carried as
            // LargeBinary would otherwise fail `ceil`'s signature coercion
            // (issue #111 family). Same for FLOOR/ROUND below.
            Some(Ok(expr_fn::ceil(crate::df_udfs::cypher_to_float64_expr(
                first_arg(df_args),
            ))))
        }
        "FLOOR" => {
            check_args!(1, df_args, "floor");
            Some(Ok(expr_fn::floor(crate::df_udfs::cypher_to_float64_expr(
                first_arg(df_args),
            ))))
        }
        "ROUND" => {
            check_args!(1, df_args, "round");
            let args = if df_args.len() == 1 {
                vec![crate::df_udfs::cypher_to_float64_expr(first_arg(df_args))]
            } else {
                vec![
                    crate::df_udfs::cypher_to_float64_expr(df_args[0].clone()),
                    df_args[1].clone(),
                ]
            };
            Some(Ok(expr_fn::round(args)))
        }
        "SIGN" => {
            check_args!(1, df_args, "sign");
            let coerced = crate::df_udfs::cypher_to_float64_expr(first_arg(df_args));
            Some(Ok(expr_fn::signum(coerced)))
        }
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
            check_args!(2, df_args, "atan2");
            // Cypher-aware Float64 coercion (see `apply_unary_math_f64`): avoids
            // the unsupported `LargeBinary -> Float64` cast on cv_encoded args.
            Some(Ok(expr_fn::atan2(
                crate::df_udfs::cypher_to_float64_expr(df_args[0].clone()),
                crate::df_udfs::cypher_to_float64_expr(df_args[1].clone()),
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
    df_args: &[DfExpr],
    context: Option<&TranslationContext>,
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
        | "LOCALDATETIME.REALTIME" => {
            // Try constant-folding first: if all args are literals, evaluate at planning time.
            // For zero-arg temporal constructors (statement clock), use the frozen
            // statement_time from the translation context.
            let stmt_time = context.map(|c| c.statement_time);
            if can_constant_fold(name_upper, df_args)
                && let Ok(folded) = try_constant_fold_temporal(name_upper, df_args, stmt_time)
            {
                return Some(Ok(folded));
            }
            Some(Ok(dummy_udf_expr(name, df_args.to_vec())))
        }
        _ => None,
    }
}

/// Check if a temporal function call can be constant-folded (all args are literals).
fn can_constant_fold(name: &str, args: &[DfExpr]) -> bool {
    // `.realtime()` variants must always read the wall clock — never constant-fold.
    if name.contains("REALTIME") {
        return false;
    }
    // Zero-arg temporal constructors (time(), date(), datetime(), localtime(),
    // localdatetime()) represent the OpenCypher *statement clock* — they return the
    // same value within a single statement.  Constant-folding at planning time is
    // correct because planning IS the start of the statement.
    //
    // `.statement()` and `.transaction()` variants are semantically identical for
    // single-statement transactions (the common case) and can also be folded.
    if args.is_empty() {
        return matches!(
            name,
            "DATE"
                | "TIME"
                | "LOCALTIME"
                | "LOCALDATETIME"
                | "DATETIME"
                | "DATE.STATEMENT"
                | "TIME.STATEMENT"
                | "LOCALTIME.STATEMENT"
                | "LOCALDATETIME.STATEMENT"
                | "DATETIME.STATEMENT"
                | "DATE.TRANSACTION"
                | "TIME.TRANSACTION"
                | "LOCALTIME.TRANSACTION"
                | "LOCALDATETIME.TRANSACTION"
                | "DATETIME.TRANSACTION"
        );
    }
    // All args must be constant expressions (literals or named_struct with all-literal args)
    args.iter().all(is_constant_expr)
}

/// Check if a DataFusion expression is a constant (evaluable at planning time).
fn is_constant_expr(expr: &DfExpr) -> bool {
    match expr {
        DfExpr::Literal(_, _) => true,
        DfExpr::ScalarFunction(func) => {
            // named_struct with all-literal args is constant
            func.args.iter().all(is_constant_expr)
        }
        _ => false,
    }
}

/// Try to constant-fold a temporal function call by evaluating it at planning time.
/// Returns a `DfExpr::Literal` with the resulting scalar value.
///
/// For zero-arg temporal constructors (statement clock), uses the frozen `stmt_time`
/// so that all occurrences of `time()` etc. within a single statement return the same value.
fn try_constant_fold_temporal(
    name: &str,
    args: &[DfExpr],
    stmt_time: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<DfExpr> {
    // Extract DfExpr args → Value args
    let val_args: Vec<Value> = args
        .iter()
        .map(extract_constant_value)
        .collect::<Result<_>>()?;

    // For zero-arg temporal constructors, use the frozen statement clock
    let result = if val_args.is_empty() {
        if let Some(frozen) = stmt_time {
            crate::datetime::eval_datetime_function_with_clock(name, &val_args, frozen)?
        } else {
            crate::datetime::eval_datetime_function(name, &val_args)?
        }
    } else {
        crate::datetime::eval_datetime_function(name, &val_args)?
    };

    // Convert Value::Temporal → ScalarValue
    let scalar = value_to_scalar(&result)?;
    Ok(DfExpr::Literal(scalar, None))
}

/// Extract a constant Value from a DfExpr that is known to be constant.
fn extract_constant_value(expr: &DfExpr) -> Result<Value> {
    use crate::df_udfs::scalar_to_value;
    match expr {
        DfExpr::Literal(sv, _) => scalar_to_value(sv).map_err(|e| anyhow::anyhow!("{}", e)),
        DfExpr::ScalarFunction(func) => {
            // named_struct(lit("key1"), lit(val1), lit("key2"), lit(val2), ...)
            // → Value::Map({key1: val1, key2: val2, ...})
            let mut map = std::collections::HashMap::new();
            let pairs: Vec<&DfExpr> = func.args.iter().collect();
            for chunk in pairs.chunks(2) {
                if let [key_expr, val_expr] = chunk {
                    // Key should be a string literal
                    let key = match key_expr {
                        DfExpr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
                        DfExpr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
                        _ => return Err(anyhow::anyhow!("Expected string key in struct")),
                    };
                    let val = extract_constant_value(val_expr)?;
                    map.insert(key, val);
                } else {
                    return Err(anyhow::anyhow!("Odd number of args in named_struct"));
                }
            }
            Ok(Value::Map(map))
        }
        _ => Err(anyhow::anyhow!(
            "Cannot extract constant value from expression"
        )),
    }
}

/// Try to translate a BTIC function (btic_lo, btic_hi, btic_overlaps, etc.).
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_btic_function(
    name_upper: &str,
    name: &str,
    df_args: &[DfExpr],
) -> Option<Result<DfExpr>> {
    // Accept the dot-namespaced spelling `btic.<fn>` as an alias for the
    // underscore-registered UDF `btic_<fn>` — mirroring `duration.<fn>` /
    // `datetime()`. Without this, `btic.contains(...)` (as written in #113) is
    // unrecognized, falls through translation, and the planner produces a
    // Null-typed predicate ("Filter predicate must return BOOLEAN, got Null").
    let canonical_upper = name_upper
        .strip_prefix("BTIC.")
        .map(|rest| format!("BTIC_{rest}"));
    let lookup_upper = canonical_upper.as_deref().unwrap_or(name_upper);
    if crate::expr_eval::is_btic_function(lookup_upper) {
        // The registered UDF names are lowercase underscore (`btic_contains`).
        let canonical_name = name
            .strip_prefix("btic.")
            .or_else(|| name.strip_prefix("BTIC."))
            .map(|rest| format!("btic_{}", rest.to_lowercase()));
        let udf_name = canonical_name.as_deref().unwrap_or(name);
        Some(Ok(dummy_udf_expr(udf_name, df_args.to_vec())))
    } else {
        None
    }
}

/// Try to translate a list function (HEAD, LAST, TAIL, RANGE).
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_list_function(name_upper: &str, df_args: &[DfExpr]) -> Option<Result<DfExpr>> {
    match name_upper {
        "HEAD" => {
            check_args!(1, df_args, "head");
            Some(Ok(dummy_udf_expr("head", df_args.to_vec())))
        }
        "LAST" => {
            check_args!(1, df_args, "last");
            Some(Ok(dummy_udf_expr("last", df_args.to_vec())))
        }
        "TAIL" => {
            check_args!(1, df_args, "tail");
            Some(Ok(dummy_udf_expr("_cypher_tail", df_args.to_vec())))
        }
        "RANGE" => {
            check_args!(2, df_args, "range");
            Some(Ok(dummy_udf_expr("range", df_args.to_vec())))
        }
        _ => None,
    }
}

/// Try to translate a graph function (ID, LABELS, KEYS, TYPE, PROPERTIES, etc.).
/// Returns `Some(result)` if the function name matches, `None` otherwise.
fn translate_graph_function(
    name_upper: &str,
    name: &str,
    df_args: &[DfExpr],
    args: &[Expr],
    context: Option<&TranslationContext>,
) -> Option<Result<DfExpr>> {
    match name_upper {
        "ID" => {
            // When called with a bare variable (ID(n)), rewrite to the internal
            // identity column reference (_vid for nodes, _eid for edges).
            if let Some(Expr::Variable(var)) = args.first() {
                let is_edge = context.is_some_and(|ctx| {
                    ctx.variable_kinds.get(var) == Some(&VariableKind::Edge)
                        || ctx.mutation_edge_hints.iter().any(|h| h == var)
                });
                let id_suffix = if is_edge { COL_EID } else { COL_VID };
                Some(Ok(DfExpr::Column(Column::from_name(format!(
                    "{}.{}",
                    var, id_suffix
                )))))
            } else {
                Some(Ok(dummy_udf_expr("id", df_args.to_vec())))
            }
        }
        "CREATED_AT" | "UPDATED_AT" => {
            // Rewrite `created_at(n)` / `updated_at(n)` to the underlying
            // `n._created_at` / `n._updated_at` column reference. Same column
            // name on vertex and edge tables, so no node/edge dispatch needed.
            if let Some(Expr::Variable(var)) = args.first() {
                let suffix = if name_upper == "CREATED_AT" {
                    "_created_at"
                } else {
                    "_updated_at"
                };
                Some(Ok(DfExpr::Column(Column::from_name(format!(
                    "{}.{}",
                    var, suffix
                )))))
            } else {
                Some(Ok(dummy_udf_expr(name, df_args.to_vec())))
            }
        }
        "LABELS" | "KEYS" => {
            // labels(n)/keys(n) expect the struct column representing the whole entity.
            // The struct is built by add_structural_projection() and exposed as Column("n").
            // df_args already has the correct resolution via the Variable case which
            // returns Column("n") when variable_kinds context is present.
            Some(Ok(dummy_udf_expr(name, df_args.to_vec())))
        }
        "TYPE" => {
            // type(r) returns the edge type name as a string.
            // When context provides the edge type via variable_labels, emit a string literal.
            // Wrap in CASE WHEN to handle null (OPTIONAL MATCH produces null relationships).
            if let Some(Expr::Variable(var)) = args.first()
                && let Some(ctx) = context
                && let Some(label) = ctx.variable_labels.get(var)
            {
                // Use CASE WHEN r._eid IS NOT NULL THEN 'TYPE' ELSE NULL END
                // so that null relationships from OPTIONAL MATCH return null.
                let eid_col = DfExpr::Column(Column::from_name(format!("{}._eid", var)));
                return Some(Ok(DfExpr::Case(datafusion::logical_expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(eid_col.is_not_null()),
                        Box::new(lit(label.clone())),
                    )],
                    else_expr: Some(Box::new(lit(ScalarValue::Utf8(None)))),
                })));
            }
            // Use _type column only when the variable is a known edge in the context.
            // Non-edge variables (e.g. loop variables in list comprehensions) must go
            // through the type() UDF which handles CypherValue-encoded inputs.
            if let Some(Expr::Variable(var)) = args.first()
                && context
                    .is_some_and(|ctx| ctx.variable_kinds.get(var) == Some(&VariableKind::Edge))
            {
                return Some(Ok(DfExpr::Column(Column::from_name(format!(
                    "{}.{}",
                    var, COL_TYPE
                )))));
            }
            Some(Ok(dummy_udf_expr("type", df_args.to_vec())))
        }
        "PROPERTIES" => {
            // properties(n) receives the struct column representing the entity,
            // same as keys(n). The struct is built by add_structural_projection().
            Some(Ok(dummy_udf_expr(name, df_args.to_vec())))
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
                Some(Ok(dummy_udf_expr(name, df_args.to_vec())))
            }
        }
        "STARTNODE" | "ENDNODE" => {
            // startNode(r)/endNode(r): pass edge + all known node variables
            // so the UDF can find the matching node by VID at runtime.
            let mut udf_args = df_args.to_vec();
            let mut seen = std::collections::HashSet::new();
            if let Some(ctx) = context {
                // Add node variables from MATCH (registered in variable_kinds)
                for (var, kind) in &ctx.variable_kinds {
                    if matches!(kind, VariableKind::Node) && seen.insert(var.clone()) {
                        udf_args.push(DfExpr::Column(Column::from_name(var.clone())));
                    }
                }
                // Add node variables from CREATE/MERGE patterns (not in variable_kinds
                // to avoid affecting ID/TYPE/HASLABEL dotted-column resolution)
                for var in &ctx.node_variable_hints {
                    if seen.insert(var.clone()) {
                        udf_args.push(DfExpr::Column(Column::from_name(var.clone())));
                    }
                }
            }
            Some(Ok(dummy_udf_expr(&name_upper.to_lowercase(), udf_args)))
        }
        "NODES" | "RELATIONSHIPS" => Some(Ok(dummy_udf_expr(name, df_args.to_vec()))),
        "HASLABEL" => {
            if let Err(e) = require_args(df_args, 2, "hasLabel") {
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

    // Try each function category in order.
    // All category functions borrow df_args to avoid unnecessary cloning;
    // they only clone individual elements when they match a function name.
    if let Some(result) = translate_aggregate_function(&name_upper, &df_args, distinct) {
        return result;
    }

    if let Some(result) = translate_string_function(&name_upper, &df_args) {
        return result;
    }

    if let Some(result) = translate_math_function(&name_upper, &df_args) {
        return result;
    }

    if let Some(result) = translate_temporal_function(&name_upper, name, &df_args, context) {
        return result;
    }

    if let Some(result) = translate_btic_function(&name_upper, name, &df_args) {
        return result;
    }

    if let Some(result) = translate_list_function(&name_upper, &df_args) {
        return result;
    }

    if let Some(result) = translate_graph_function(&name_upper, name, &df_args, args, context) {
        return result;
    }

    // Null handling functions (standalone)
    match name_upper.as_str() {
        "COALESCE" => {
            require_arg(&df_args, "coalesce")?;
            // DF 52+ rewrites coalesce → CASE WHEN during simplification, but
            // our plans may bypass the optimizer. Build the CASE directly:
            //   CASE WHEN a1 IS NOT NULL THEN a1
            //        WHEN a2 IS NOT NULL THEN a2 ... ELSE last END
            if df_args.len() == 1 {
                return Ok(df_args.into_iter().next().unwrap());
            }
            let n = df_args.len();
            let (init, last) = df_args.split_at(n - 1);
            let mut builder = datafusion::logical_expr::conditional_expressions::CaseBuilder::new(
                None,
                vec![],
                vec![],
                None,
            );
            for arg in init {
                builder.when(arg.clone().is_not_null(), arg.clone());
            }
            return Ok(builder.otherwise(last[0].clone())?);
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

    // Similarity functions → registered UDFs
    match name_upper.as_str() {
        "SIMILAR_TO" | "VECTOR_SIMILARITY" | "SPARSE_SIMILAR_TO" => {
            return Ok(dummy_udf_expr(&name_upper.to_lowercase(), df_args));
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
    ret_type: datafusion::arrow::datatypes::DataType,
}

impl DummyUdf {
    fn new(name: String) -> Self {
        let ret_type = dummy_udf_return_type(&name);
        Self {
            name,
            signature: datafusion::logical_expr::Signature::variadic_any(
                datafusion::logical_expr::Volatility::Immutable,
            ),
            ret_type,
        }
    }
}

/// Infer the return type for a DummyUdf placeholder based on UDF name.
///
/// This is critical for `apply_type_coercion` which creates DummyUdf nodes
/// and may process their parents before `resolve_udfs` runs. Without correct
/// return types for arithmetic UDFs, the coercion logic mis-routes nested
/// expressions (e.g., treating a CypherValue arithmetic result as a literal
/// null, leading to invalid Cast insertions like Cast(LargeBinary→Int64)).
///
/// Only arithmetic/list/map UDFs return LargeBinary here. All other UDFs
/// (comparisons, conversions, etc.) return Null — the default that preserves
/// existing coercion behavior (including chained comparison support like
/// `1 < n.num <= 3` where the parser doesn't decompose into AND).
fn dummy_udf_return_type(name: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    match name {
        // CypherValue arithmetic UDFs — these produce LargeBinary-encoded results
        // and may appear as children of outer arithmetic/comparison expressions
        // within a single apply_type_coercion pass.
        "_cypher_add"
        | "_cypher_sub"
        | "_cypher_mul"
        | "_cypher_div"
        | "_cypher_mod"
        | "_cypher_list_concat"
        | "_cypher_list_append"
        | "_make_cypher_list"
        | "_map_project"
        | "_cypher_list_to_cv"
        | "_cypher_tail" => DataType::LargeBinary,
        // Everything else: return Null to preserve existing coercion behavior.
        // The second resolve_udfs pass will replace DummyUdf with the real UDF
        // which has the correct return type.
        _ => DataType::Null,
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
pub fn dummy_udf_expr(name: &str, args: Vec<DfExpr>) -> DfExpr {
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
        arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        // Arithmetic UDFs are type-preserving (Int64×Int64 → Int64,
        // float/mixed → Float64, CypherValue → LargeBinary): resolve their
        // return type dynamically from arg_types so logical type-coercion agrees
        // with the resolved UDF's actual output and avoids a schema mismatch.
        match self.name.as_str() {
            "_cypher_add" | "_cypher_sub" | "_cypher_mul" | "_cypher_div" | "_cypher_mod" => {
                Ok(crate::df_udfs::cypher_arith_return_type(arg_types))
            }
            // Other UDFs keep the UDF-name-based return type so that
            // apply_type_coercion can correctly route nested expressions before
            // resolve_udfs runs.
            _ => Ok(self.ret_type.clone()),
        }
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
        Expr::LabelCheck { expr, .. } => {
            collect_properties_recursive(expr, properties);
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

/// Fallback type resolution for column expressions when `get_type` fails
/// (e.g., due to "Ambiguous reference" from structural projections creating
/// both a flat `var._vid` column and a struct `var` with a `_vid` field).
///
/// Looks up the column name directly in the schema's fields by exact name match.
fn resolve_column_type_fallback(
    expr: &DfExpr,
    schema: &datafusion::common::DFSchema,
) -> Option<datafusion::arrow::datatypes::DataType> {
    if let DfExpr::Column(col) = expr {
        let col_name = &col.name;
        // Find the first field matching by exact name (prefer flat columns)
        for (_, field) in schema.iter() {
            if field.name() == col_name {
                return Some(field.data_type().clone());
            }
        }
    }
    None
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

    match expr {
        DfExpr::BinaryExpr(binary) => coerce_binary_expr(binary, schema),
        DfExpr::ScalarFunction(func) => coerce_scalar_function(func, schema),
        DfExpr::Case(case) => coerce_case_expr(case, schema),
        DfExpr::InList(in_list) => {
            let coerced_expr = apply_type_coercion(&in_list.expr, schema)?;
            let coerced_list = in_list
                .list
                .iter()
                .map(|e| apply_type_coercion(e, schema))
                .collect::<Result<Vec<_>>>()?;
            let expr_type = coerced_expr
                .get_type(schema)
                .map_err(|e| anyhow!("Failed to get IN expr type: {}", e))?;
            crate::cypher_type_coerce::build_cypher_in_list(
                coerced_expr,
                &expr_type,
                coerced_list,
                in_list.negated,
                schema,
            )
        }
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
        DfExpr::IsNull(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            Ok(coerced_inner.is_null())
        }
        DfExpr::IsNotNull(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            Ok(coerced_inner.is_not_null())
        }
        DfExpr::Negative(inner) => {
            let coerced_inner = apply_type_coercion(inner, schema)?;
            let inner_type = coerced_inner.get_type(schema).ok();
            if matches!(inner_type.as_ref(), Some(DataType::LargeBinary)) {
                Ok(dummy_udf_expr(
                    "_cypher_mul",
                    vec![coerced_inner, lit(ScalarValue::Int64(Some(-1)))],
                ))
            } else {
                Ok(DfExpr::Negative(Box::new(coerced_inner)))
            }
        }
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
        DfExpr::Alias(alias) => {
            let coerced_inner = apply_type_coercion(&alias.expr, schema)?;
            Ok(coerced_inner.alias(alias.name.clone()))
        }
        DfExpr::AggregateFunction(agg) => coerce_aggregate_function(agg, schema),
        _ => Ok(expr.clone()),
    }
}

/// Coerce AND/OR operands to Boolean when they are Null, Utf8, or LargeBinary.
fn coerce_logical_operands(
    left: DfExpr,
    right: DfExpr,
    op: datafusion::logical_expr::Operator,
    schema: &datafusion::common::DFSchema,
) -> Option<DfExpr> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::ExprSchemable;

    if !matches!(
        op,
        datafusion::logical_expr::Operator::And | datafusion::logical_expr::Operator::Or
    ) {
        return None;
    }
    let left_type = left.get_type(schema).ok();
    let right_type = right.get_type(schema).ok();
    let left_needs_cast = left_type
        .as_ref()
        .is_some_and(|t| t.is_null() || matches!(t, DataType::Utf8 | DataType::LargeUtf8));
    let right_needs_cast = right_type
        .as_ref()
        .is_some_and(|t| t.is_null() || matches!(t, DataType::Utf8 | DataType::LargeUtf8));
    let left_is_lb = left_type
        .as_ref()
        .is_some_and(|t| matches!(t, DataType::LargeBinary));
    let right_is_lb = right_type
        .as_ref()
        .is_some_and(|t| matches!(t, DataType::LargeBinary));
    if !(left_needs_cast || right_needs_cast || left_is_lb || right_is_lb) {
        return None;
    }
    let coerced_left = if left_is_lb {
        dummy_udf_expr("_cv_to_bool", vec![left])
    } else if left_needs_cast {
        datafusion::logical_expr::cast(left, DataType::Boolean)
    } else {
        left
    };
    let coerced_right = if right_is_lb {
        dummy_udf_expr("_cv_to_bool", vec![right])
    } else if right_needs_cast {
        datafusion::logical_expr::cast(right, DataType::Boolean)
    } else {
        right
    };
    Some(binary_expr(coerced_left, op, coerced_right))
}

/// Handle LargeBinary (CypherValue) operands in binary expressions.
/// Returns `Some(expr)` if the operation was handled, `None` to fall through.
#[expect(
    clippy::too_many_arguments,
    reason = "Binary coercion needs all context"
)]
fn coerce_large_binary_ops(
    left: &DfExpr,
    right: &DfExpr,
    left_type: &datafusion::arrow::datatypes::DataType,
    right_type: &datafusion::arrow::datatypes::DataType,
    left_is_null: bool,
    op: datafusion::logical_expr::Operator,
    is_comparison: bool,
    is_arithmetic: bool,
) -> Option<Result<DfExpr>> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::Operator;

    let left_is_lb = matches!(left_type, DataType::LargeBinary) || left_is_null;
    let right_is_lb = matches!(right_type, DataType::LargeBinary) || (right_type.is_null());

    if op == Operator::Plus {
        if left_is_lb && right_is_lb {
            return Some(Ok(dummy_udf_expr(
                "_cypher_add",
                vec![left.clone(), right.clone()],
            )));
        }
        let left_is_native_list = matches!(left_type, DataType::List(_) | DataType::LargeList(_));
        let right_is_native_list = matches!(right_type, DataType::List(_) | DataType::LargeList(_));
        if left_is_native_list && right_is_native_list {
            return Some(Ok(dummy_udf_expr(
                "_cypher_list_concat",
                vec![left.clone(), right.clone()],
            )));
        }
        if left_is_native_list || right_is_native_list {
            return Some(Ok(dummy_udf_expr(
                "_cypher_list_append",
                vec![left.clone(), right.clone()],
            )));
        }
    }

    if (left_is_lb || right_is_lb) && is_comparison {
        if let Some(udf_name) = comparison_udf_name(op) {
            return Some(Ok(dummy_udf_expr(
                udf_name,
                vec![left.clone(), right.clone()],
            )));
        }
        return Some(Ok(binary_expr(left.clone(), op, right.clone())));
    }

    if (left_is_lb || right_is_lb) && is_arithmetic {
        let udf_name =
            arithmetic_udf_name(op).expect("is_arithmetic guarantees a valid arithmetic operator");
        return Some(Ok(dummy_udf_expr(
            udf_name,
            vec![left.clone(), right.clone()],
        )));
    }

    None
}

/// Handle DateTime/Time/Timestamp struct comparisons.
fn coerce_temporal_comparisons(
    left: DfExpr,
    right: DfExpr,
    left_type: &datafusion::arrow::datatypes::DataType,
    right_type: &datafusion::arrow::datatypes::DataType,
    op: datafusion::logical_expr::Operator,
    is_comparison: bool,
) -> Option<DfExpr> {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::logical_expr::Operator;

    if !is_comparison {
        return None;
    }

    // DateTime struct comparisons
    if uni_common::core::schema::is_datetime_struct(left_type)
        && uni_common::core::schema::is_datetime_struct(right_type)
    {
        return Some(binary_expr(
            extract_datetime_nanos(left),
            op,
            extract_datetime_nanos(right),
        ));
    }

    // Time struct comparisons
    if uni_common::core::schema::is_time_struct(left_type)
        && uni_common::core::schema::is_time_struct(right_type)
    {
        return Some(binary_expr(
            extract_time_nanos(left),
            op,
            extract_time_nanos(right),
        ));
    }

    // Mixed Timestamp <-> DateTime struct comparisons
    let left_is_ts = matches!(left_type, DataType::Timestamp(TimeUnit::Nanosecond, _));
    let right_is_ts = matches!(right_type, DataType::Timestamp(TimeUnit::Nanosecond, _));

    if (left_is_ts && uni_common::core::schema::is_datetime_struct(right_type))
        || (uni_common::core::schema::is_datetime_struct(left_type) && right_is_ts)
    {
        let left_nanos = if uni_common::core::schema::is_datetime_struct(left_type) {
            extract_datetime_nanos(left)
        } else {
            left
        };
        let right_nanos = if uni_common::core::schema::is_datetime_struct(right_type) {
            extract_datetime_nanos(right)
        } else {
            right
        };
        let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        return Some(binary_expr(
            cast_expr(left_nanos, ts_type.clone()),
            op,
            cast_expr(right_nanos, ts_type),
        ));
    }

    // Duration vs temporal (date/time/datetime/timestamp) equality should not
    // require a common physical type. Cypher treats different temporal classes
    // as non-equal; ordering comparisons return null.
    let left_is_duration = matches!(left_type, DataType::Interval(_));
    let right_is_duration = matches!(right_type, DataType::Interval(_));
    let left_is_temporal_like = uni_common::core::schema::is_datetime_struct(left_type)
        || uni_common::core::schema::is_time_struct(left_type)
        || matches!(
            left_type,
            DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
        );
    let right_is_temporal_like = uni_common::core::schema::is_datetime_struct(right_type)
        || uni_common::core::schema::is_time_struct(right_type)
        || matches!(
            right_type,
            DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
        );

    if (left_is_duration && right_is_temporal_like) || (right_is_duration && left_is_temporal_like)
    {
        return Some(match op {
            Operator::Eq => lit(false),
            Operator::NotEq => lit(true),
            _ => lit(ScalarValue::Boolean(None)),
        });
    }

    None
}

/// Handle type-mismatched binary expressions: numeric coercion, timestamp vs string,
/// list inner type coercion, and unified primitive coercion.
fn coerce_mismatched_types(
    left: DfExpr,
    right: DfExpr,
    left_type: &datafusion::arrow::datatypes::DataType,
    right_type: &datafusion::arrow::datatypes::DataType,
    op: datafusion::logical_expr::Operator,
    is_comparison: bool,
) -> Option<Result<DfExpr>> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::Operator;

    if left_type == right_type {
        return None;
    }

    // Numeric coercion
    if left_type.is_numeric() && right_type.is_numeric() {
        if left_type == &DataType::Int64
            && right_type == &DataType::UInt64
            && matches!(&left, DfExpr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0)
        {
            let coerced_left = datafusion::logical_expr::cast(left, DataType::UInt64);
            return Some(Ok(binary_expr(coerced_left, op, right)));
        }
        if left_type == &DataType::UInt64
            && right_type == &DataType::Int64
            && matches!(&right, DfExpr::Literal(ScalarValue::Int64(Some(v)), _) if *v >= 0)
        {
            let coerced_right = datafusion::logical_expr::cast(right, DataType::UInt64);
            return Some(Ok(binary_expr(left, op, coerced_right)));
        }
        let target = wider_numeric_type(left_type, right_type);
        let coerced_left = if *left_type != target {
            datafusion::logical_expr::cast(left, target.clone())
        } else {
            left
        };
        let coerced_right = if *right_type != target {
            datafusion::logical_expr::cast(right, target)
        } else {
            right
        };
        return Some(Ok(binary_expr(coerced_left, op, coerced_right)));
    }

    // Timestamp vs Utf8
    if is_comparison {
        match (left_type, right_type) {
            (ts @ DataType::Timestamp(..), DataType::Utf8 | DataType::LargeUtf8) => {
                let right = normalize_datetime_literal(right);
                return Some(Ok(binary_expr(
                    left,
                    op,
                    datafusion::logical_expr::cast(right, ts.clone()),
                )));
            }
            (DataType::Utf8 | DataType::LargeUtf8, ts @ DataType::Timestamp(..)) => {
                let left = normalize_datetime_literal(left);
                return Some(Ok(binary_expr(
                    datafusion::logical_expr::cast(left, ts.clone()),
                    op,
                    right,
                )));
            }
            _ => {}
        }
    }

    // List comparison with different numeric inner types
    if is_comparison
        && let (DataType::List(l_field), DataType::List(r_field)) = (left_type, right_type)
    {
        let l_inner = l_field.data_type();
        let r_inner = r_field.data_type();
        if l_inner.is_numeric() && r_inner.is_numeric() && l_inner != r_inner {
            let target_inner = wider_numeric_type(l_inner, r_inner);
            let target_type = DataType::List(Arc::new(datafusion::arrow::datatypes::Field::new(
                "item",
                target_inner,
                true,
            )));
            return Some(Ok(binary_expr(
                datafusion::logical_expr::cast(left, target_type.clone()),
                op,
                datafusion::logical_expr::cast(right, target_type),
            )));
        }
    }

    // Unified primitive type coercion
    if is_primitive_type(left_type) && is_primitive_type(right_type) {
        if op == Operator::Plus {
            return Some(crate::cypher_type_coerce::build_cypher_plus(
                left, left_type, right, right_type,
            ));
        }
        if is_comparison {
            return Some(Ok(crate::cypher_type_coerce::build_cypher_comparison(
                left, left_type, right, right_type, op,
            )));
        }
    }

    None
}

/// Handle list comparisons: ordering via UDF and equality via _cypher_equal/_cypher_not_equal.
fn coerce_list_comparisons(
    left: DfExpr,
    right: DfExpr,
    left_type: &datafusion::arrow::datatypes::DataType,
    right_type: &datafusion::arrow::datatypes::DataType,
    op: datafusion::logical_expr::Operator,
    is_comparison: bool,
) -> Option<DfExpr> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::Operator;

    if !is_comparison {
        return None;
    }

    let left_is_list = matches!(left_type, DataType::List(_) | DataType::LargeList(_));
    let right_is_list = matches!(right_type, DataType::List(_) | DataType::LargeList(_));

    // List ordering
    if left_is_list
        && right_is_list
        && matches!(
            op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        )
    {
        let op_str = match op {
            Operator::Lt => "lt",
            Operator::LtEq => "lteq",
            Operator::Gt => "gt",
            Operator::GtEq => "gteq",
            _ => unreachable!(),
        };
        return Some(dummy_udf_expr(
            "_cypher_list_compare",
            vec![left, right, lit(op_str)],
        ));
    }

    // List equality
    if left_is_list && right_is_list && matches!(op, Operator::Eq | Operator::NotEq) {
        let udf_name =
            comparison_udf_name(op).expect("Eq|NotEq is always a valid comparison operator");
        return Some(dummy_udf_expr(udf_name, vec![left, right]));
    }

    // Cross-type comparison: List vs non-List
    if (left_is_list != right_is_list)
        && !matches!(left_type, DataType::Null)
        && !matches!(right_type, DataType::Null)
    {
        return Some(match op {
            Operator::Eq => lit(false),
            Operator::NotEq => lit(true),
            _ => lit(ScalarValue::Boolean(None)),
        });
    }

    None
}

/// Coerce a binary expression's operands for type compatibility.
fn coerce_binary_expr(
    binary: &datafusion::logical_expr::expr::BinaryExpr,
    schema: &datafusion::common::DFSchema,
) -> Result<DfExpr> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::ExprSchemable;
    use datafusion::logical_expr::Operator;

    let left = apply_type_coercion(&binary.left, schema)?;
    let right = apply_type_coercion(&binary.right, schema)?;

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
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo
    );

    // AND/OR with Null, Utf8, or LargeBinary operands: coerce to Boolean.
    if let Some(result) = coerce_logical_operands(left.clone(), right.clone(), binary.op, schema) {
        return Ok(result);
    }

    if is_comparison || is_arithmetic {
        let left_type = match left.get_type(schema) {
            Ok(t) => t,
            Err(e) => {
                if let Some(t) = resolve_column_type_fallback(&left, schema) {
                    t
                } else {
                    log::warn!("Failed to get left type in binary expr: {}", e);
                    return Ok(binary_expr(left, binary.op, right));
                }
            }
        };
        let right_type = match right.get_type(schema) {
            Ok(t) => t,
            Err(e) => {
                if let Some(t) = resolve_column_type_fallback(&right, schema) {
                    t
                } else {
                    log::warn!("Failed to get right type in binary expr: {}", e);
                    return Ok(binary_expr(left, binary.op, right));
                }
            }
        };

        // Handle Null-typed operands
        let left_is_null = left_type.is_null();
        let right_is_null = right_type.is_null();
        if left_is_null && right_is_null {
            return Ok(lit(ScalarValue::Boolean(None)));
        }
        if left_is_null || right_is_null {
            let target = if left_is_null {
                &right_type
            } else {
                &left_type
            };
            if !matches!(target, DataType::LargeBinary) {
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
                return Ok(binary_expr(coerced_left, binary.op, coerced_right));
            }
        }

        // LargeBinary (CypherValue) handling
        if let Some(result) = coerce_large_binary_ops(
            &left,
            &right,
            &left_type,
            &right_type,
            left_is_null,
            binary.op,
            is_comparison,
            is_arithmetic,
        ) {
            return result;
        }

        // DateTime/Time/Timestamp struct comparisons
        if let Some(result) = coerce_temporal_comparisons(
            left.clone(),
            right.clone(),
            &left_type,
            &right_type,
            binary.op,
            is_comparison,
        ) {
            return Ok(result);
        }

        // Struct or LargeBinary/Struct comparisons
        let either_struct =
            matches!(left_type, DataType::Struct(_)) || matches!(right_type, DataType::Struct(_));
        let either_lb_or_struct = (matches!(left_type, DataType::LargeBinary)
            || matches!(left_type, DataType::Struct(_)))
            && (matches!(right_type, DataType::LargeBinary)
                || matches!(right_type, DataType::Struct(_)));
        if is_comparison && either_struct && either_lb_or_struct {
            if let Some(udf_name) = comparison_udf_name(binary.op) {
                return Ok(dummy_udf_expr(udf_name, vec![left, right]));
            }
            return Ok(lit(ScalarValue::Boolean(None)));
        }

        // NaN-aware comparisons
        if is_comparison && (contains_division(&left) || contains_division(&right)) {
            let udf_name = comparison_udf_name(binary.op)
                .expect("is_comparison guarantees a valid comparison operator");
            return Ok(dummy_udf_expr(udf_name, vec![left, right]));
        }

        // String concatenation via Plus
        if binary.op == Operator::Plus
            && (crate::cypher_type_coerce::is_string_type(&left_type)
                || crate::cypher_type_coerce::is_string_type(&right_type))
            && is_primitive_type(&left_type)
            && is_primitive_type(&right_type)
        {
            return crate::cypher_type_coerce::build_cypher_plus(
                left,
                &left_type,
                right,
                &right_type,
            );
        }

        // Type mismatch handling
        if let Some(result) = coerce_mismatched_types(
            left.clone(),
            right.clone(),
            &left_type,
            &right_type,
            binary.op,
            is_comparison,
        ) {
            return result;
        }

        // List comparisons
        if let Some(result) = coerce_list_comparisons(
            left.clone(),
            right.clone(),
            &left_type,
            &right_type,
            binary.op,
            is_comparison,
        ) {
            return Ok(result);
        }

        // Int64×Int64 arithmetic convergence point (projections + WITH/group-by):
        // route `+`/`-`/`*`/`/`/`%` on two statically-Int64 operands through the
        // type-preserving checked Cypher UDFs so integer overflow / i64::MIN/-1 /
        // division by zero error instead of silently wrapping (native Arrow int
        // kernels wrap). Only integers can overflow, so the gate is surgical:
        // float/string/mixed/non-numeric arithmetic falls through to the native
        // path below, which preserves the native compile-time type errors and
        // NaN-aware comparison routing that the UDF would otherwise bypass.
        if let Some(name) = arithmetic_udf_name(binary.op)
            && left_type == DataType::Int64
            && right_type == DataType::Int64
            && !is_list_expr(&left)
            && !is_list_expr(&right)
        {
            return Ok(dummy_udf_expr(name, vec![left, right]));
        }
    }

    Ok(binary_expr(left, binary.op, right))
}

/// Coerce scalar function arguments, handling mixed-type coalesce specially.
fn coerce_scalar_function(
    func: &datafusion::logical_expr::expr::ScalarFunction,
    schema: &datafusion::common::DFSchema,
) -> Result<DfExpr> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::ExprSchemable;

    let coerced_args: Vec<DfExpr> = func
        .args
        .iter()
        .map(|a| apply_type_coercion(a, schema))
        .collect::<Result<Vec<_>>>()?;

    // A Cypher list literal is heterogeneous by definition, but `make_array`
    // requires one child type. `translate_list_literal` routes mixed lists to
    // `_make_cypher_list`, yet it decides *syntactically* — `TranslationContext`
    // carries no field types, so `[u.name, u.classYear]` looks uniform to it and
    // lowers to `make_array` whatever the columns hold. Here the `DFSchema` is
    // available, so the same decision can be made on actual types.
    //
    // Worth doing even though a plan-time error would be survivable: `make_array`
    // over `[Utf8, Int64]` *plans* successfully and then trips an assertion in
    // Arrow's `MutableArrayData`, which aborts the process. Only lists that are
    // genuinely mixed are rewritten, so the native path — and with it the
    // `uni_raw_bytes` list-child marking invariant, which `is_markable_list`
    // already declines to apply to mixed lists — is untouched.
    if func.func.name().eq_ignore_ascii_case("make_array") && coerced_args.len() > 1 {
        let types: Vec<_> = coerced_args
            .iter()
            .filter_map(|a| a.get_type(schema).ok())
            .filter(|t| !matches!(t, DataType::Null))
            .collect();
        let has_mixed_types = types.windows(2).any(|w| w[0] != w[1]);
        if has_mixed_types {
            // `_make_cypher_list` is `VariadicAny` and converts every argument
            // through `Value`, so the arguments need no casting first.
            return Ok(dummy_udf_expr("_make_cypher_list", coerced_args));
        }
    }

    if func.func.name().eq_ignore_ascii_case("coalesce") && coerced_args.len() > 1 {
        let types: Vec<_> = coerced_args
            .iter()
            .filter_map(|a| a.get_type(schema).ok())
            .collect();
        let has_mixed_types = types.windows(2).any(|w| w[0] != w[1]);
        if has_mixed_types {
            // Only cast to Utf8 when all types are string-like.
            // Struct (DateTime), LargeBinary (CypherValue), List, and other
            // non-string types cannot be safely cast to Utf8.
            let all_string_like = types
                .iter()
                .all(|t| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Null));
            let unified_args: Vec<DfExpr> = if all_string_like {
                coerced_args
                    .into_iter()
                    .map(|a| datafusion::logical_expr::cast(a, DataType::Utf8))
                    .collect()
            } else {
                // Convert all to LargeBinary (CypherValue encoding).
                coerced_args
                    .into_iter()
                    .zip(types.iter())
                    .map(|(arg, t)| match t {
                        DataType::LargeBinary | DataType::Null => arg,
                        DataType::List(_) | DataType::LargeList(_) => {
                            list_to_large_binary_expr(arg)
                        }
                        _ => scalar_to_large_binary_expr(arg),
                    })
                    .collect()
            };
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

/// Coerce CASE expression: recurse into sub-expressions, rewrite simple CASE to generic,
/// and coerce result types.
fn coerce_case_expr(
    case: &datafusion::logical_expr::expr::Case,
    schema: &datafusion::common::DFSchema,
) -> Result<DfExpr> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::ExprSchemable;

    let coerced_operand = case
        .expr
        .as_ref()
        .map(|e| apply_type_coercion(e, schema).map(Box::new))
        .transpose()?;
    // A searched CASE (`CASE WHEN <bool> THEN ...`, no operand) has boolean WHEN
    // conditions, so a schemaless LargeBinary CypherValue WHEN is coerced to bool.
    // A simple CASE (`CASE <operand> WHEN <value> THEN ...`) instead compares the
    // operand against each WHEN *value* — that value is NOT a boolean. Wrapping it
    // in `_cv_to_bool` (whose UDF return type is Null) makes the comparison the
    // generic rewrite builds collapse to a literal NULL, so the branch can never
    // match; leave simple-CASE WHEN values untouched.
    let is_searched_case = case.expr.is_none();
    let coerced_when_then = case
        .when_then_expr
        .iter()
        .map(|(w, t)| {
            let cw = apply_type_coercion(w, schema)?;
            let cw = if is_searched_case {
                match cw.get_type(schema).ok() {
                    Some(DataType::LargeBinary) => dummy_udf_expr("_cv_to_bool", vec![cw]),
                    _ => cw,
                }
            } else {
                cw
            };
            let ct = apply_type_coercion(t, schema)?;
            Ok((Box::new(cw), Box::new(ct)))
        })
        .collect::<Result<Vec<_>>>()?;
    let coerced_else = case
        .else_expr
        .as_ref()
        .map(|e| apply_type_coercion(e, schema).map(Box::new))
        .transpose()?;

    let mut result_case = if let Some(operand) = coerced_operand {
        crate::cypher_type_coerce::rewrite_simple_case_to_generic(
            *operand,
            coerced_when_then,
            coerced_else,
            schema,
        )?
    } else {
        datafusion::logical_expr::expr::Case {
            expr: None,
            when_then_expr: coerced_when_then,
            else_expr: coerced_else,
        }
    };

    crate::cypher_type_coerce::coerce_case_results(&mut result_case, schema)?;

    Ok(DfExpr::Case(result_case))
}

/// Coerce aggregate function arguments, order-by, and filter expressions.
fn coerce_aggregate_function(
    agg: &datafusion::logical_expr::expr::AggregateFunction,
    schema: &datafusion::common::DFSchema,
) -> Result<DfExpr> {
    let coerced_args: Vec<DfExpr> = agg
        .params
        .args
        .iter()
        .map(|a| apply_type_coercion(a, schema))
        .collect::<Result<Vec<_>>>()?;
    let coerced_order_by: Vec<datafusion::logical_expr::SortExpr> = agg
        .params
        .order_by
        .iter()
        .map(|s| {
            let coerced_expr = apply_type_coercion(&s.expr, schema)?;
            Ok(datafusion::logical_expr::SortExpr {
                expr: coerced_expr,
                asc: s.asc,
                nulls_first: s.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let coerced_filter = agg
        .params
        .filter
        .as_ref()
        .map(|f| apply_type_coercion(f, schema).map(Box::new))
        .transpose()?;
    Ok(DfExpr::AggregateFunction(
        datafusion::logical_expr::expr::AggregateFunction {
            func: agg.func.clone(),
            params: datafusion::logical_expr::expr::AggregateFunctionParams {
                args: coerced_args,
                distinct: agg.params.distinct,
                filter: coerced_filter,
                order_by: coerced_order_by,
                null_treatment: agg.params.null_treatment,
            },
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, Int32Array, StringArray, Time64NanosecondArray, TimestampNanosecondArray,
    };
    use uni_common::TemporalValue;
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
        // LB + LB → _cypher_add (handles both list concat and numeric add via eval_add)
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
            contains_udf(&result, "_cypher_add"),
            "expected _cypher_add, got: {result}"
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

    #[test]
    fn test_value_to_scalar_non_empty_map_becomes_struct() {
        let mut map = std::collections::HashMap::new();
        map.insert("k".to_string(), Value::Int(1));
        let scalar = value_to_scalar(&Value::Map(map)).unwrap();
        assert!(
            matches!(scalar, ScalarValue::Struct(_)),
            "expected Struct scalar for map input"
        );
    }

    #[test]
    fn test_value_to_scalar_empty_map_becomes_struct() {
        let scalar = value_to_scalar(&Value::Map(Default::default())).unwrap();
        assert!(
            matches!(scalar, ScalarValue::Struct(_)),
            "empty map should produce an empty Struct scalar"
        );
    }

    #[test]
    fn test_value_to_scalar_null_is_untyped_null() {
        let scalar = value_to_scalar(&Value::Null).unwrap();
        assert!(
            matches!(scalar, ScalarValue::Null),
            "expected untyped Null scalar for Value::Null"
        );
    }

    #[test]
    fn test_value_to_scalar_datetime_produces_struct() {
        // Test that DateTime produces correct 3-field Struct
        let datetime = Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch: 441763200000000000, // 1984-01-01T00:00:00Z
            offset_seconds: 3600,                  // +01:00
            timezone_name: Some("Europe/Paris".to_string()),
        });

        let scalar = value_to_scalar(&datetime).unwrap();

        // Should produce ScalarValue::Struct with 3 fields
        if let ScalarValue::Struct(struct_arr) = scalar {
            assert_eq!(struct_arr.len(), 1, "expected single-row struct array");
            assert_eq!(struct_arr.num_columns(), 3, "expected 3 fields");

            // Verify field names
            let fields = struct_arr.fields();
            assert_eq!(fields[0].name(), "nanos_since_epoch");
            assert_eq!(fields[1].name(), "offset_seconds");
            assert_eq!(fields[2].name(), "timezone_name");

            // Verify field values
            let nanos_col = struct_arr.column(0);
            let offset_col = struct_arr.column(1);
            let tz_col = struct_arr.column(2);

            if let Some(nanos_arr) = nanos_col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
            {
                assert_eq!(nanos_arr.value(0), 441763200000000000);
            } else {
                panic!("Expected TimestampNanosecondArray for nanos field");
            }

            if let Some(offset_arr) = offset_col.as_any().downcast_ref::<Int32Array>() {
                assert_eq!(offset_arr.value(0), 3600);
            } else {
                panic!("Expected Int32Array for offset field");
            }

            if let Some(tz_arr) = tz_col.as_any().downcast_ref::<StringArray>() {
                assert_eq!(tz_arr.value(0), "Europe/Paris");
            } else {
                panic!("Expected StringArray for timezone_name field");
            }
        } else {
            panic!(
                "Expected ScalarValue::Struct for DateTime, got {:?}",
                scalar
            );
        }
    }

    #[test]
    fn test_value_to_scalar_datetime_with_null_timezone() {
        // Test DateTime with no timezone name (offset-only)
        let datetime = Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch: 1704067200000000000, // 2024-01-01T00:00:00Z
            offset_seconds: -18000,                 // -05:00
            timezone_name: None,
        });

        let scalar = value_to_scalar(&datetime).unwrap();

        if let ScalarValue::Struct(struct_arr) = scalar {
            assert_eq!(struct_arr.num_columns(), 3);

            // Verify timezone_name is null
            let tz_col = struct_arr.column(2);
            if let Some(tz_arr) = tz_col.as_any().downcast_ref::<StringArray>() {
                assert!(tz_arr.is_null(0), "expected null timezone_name");
            } else {
                panic!("Expected StringArray for timezone_name field");
            }
        } else {
            panic!("Expected ScalarValue::Struct for DateTime");
        }
    }

    #[test]
    fn test_value_to_scalar_time_produces_struct() {
        // Test that Time produces correct 2-field Struct
        let time = Value::Temporal(TemporalValue::Time {
            nanos_since_midnight: 37845000000000, // 10:30:45
            offset_seconds: 3600,                 // +01:00
        });

        let scalar = value_to_scalar(&time).unwrap();

        // Should produce ScalarValue::Struct with 2 fields
        if let ScalarValue::Struct(struct_arr) = scalar {
            assert_eq!(struct_arr.len(), 1, "expected single-row struct array");
            assert_eq!(struct_arr.num_columns(), 2, "expected 2 fields");

            // Verify field names
            let fields = struct_arr.fields();
            assert_eq!(fields[0].name(), "nanos_since_midnight");
            assert_eq!(fields[1].name(), "offset_seconds");

            // Verify field values
            let nanos_col = struct_arr.column(0);
            let offset_col = struct_arr.column(1);

            if let Some(nanos_arr) = nanos_col.as_any().downcast_ref::<Time64NanosecondArray>() {
                assert_eq!(nanos_arr.value(0), 37845000000000);
            } else {
                panic!("Expected Time64NanosecondArray for nanos_since_midnight field");
            }

            if let Some(offset_arr) = offset_col.as_any().downcast_ref::<Int32Array>() {
                assert_eq!(offset_arr.value(0), 3600);
            } else {
                panic!("Expected Int32Array for offset field");
            }
        } else {
            panic!("Expected ScalarValue::Struct for Time, got {:?}", scalar);
        }
    }

    #[test]
    fn test_value_to_scalar_time_boundary_values() {
        // Test Time with boundary values
        let midnight = Value::Temporal(TemporalValue::Time {
            nanos_since_midnight: 0,
            offset_seconds: 0,
        });

        let scalar = value_to_scalar(&midnight).unwrap();

        if let ScalarValue::Struct(struct_arr) = scalar {
            let nanos_col = struct_arr.column(0);
            if let Some(nanos_arr) = nanos_col.as_any().downcast_ref::<Time64NanosecondArray>() {
                assert_eq!(nanos_arr.value(0), 0);
            } else {
                panic!("Expected Time64NanosecondArray");
            }
        } else {
            panic!("Expected ScalarValue::Struct for Time");
        }
    }
}
