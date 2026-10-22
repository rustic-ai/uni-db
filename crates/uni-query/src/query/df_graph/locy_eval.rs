// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! In-memory expression evaluation for Locy commands.
//!
//! Ported from `uni-locy/src/orchestrator/eval.rs`. Used by SLG, QUERY, EXPLAIN,
//! ASSUME, ABDUCE, and DERIVE in the native command dispatch path.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use uni_common::Value;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, UnaryOp};
use uni_cypher::locy_ast::{LocyBinaryOp, LocyExpr};
use uni_locy::{FactRow, LocyError};
use uni_query_functions::df_udfs::{canonical_cypher_value, cypher_value_order};
use uni_query_functions::expr_eval::cypher_numeric_cmp;

/// Evaluate a Locy expression (which may contain prev references) given current bindings
/// and optional previous-iteration values.
pub fn eval_locy_expr(
    expr: &LocyExpr,
    bindings: &FactRow,
    prev_values: Option<&FactRow>,
) -> Result<Value, LocyError> {
    match expr {
        LocyExpr::PrevRef(field) => Ok(prev_values
            .and_then(|prev| prev.get(field).cloned())
            .unwrap_or(Value::Null)),
        LocyExpr::Cypher(cypher_expr) => eval_expr(cypher_expr, bindings),
        LocyExpr::BinaryOp { left, op, right } => {
            let l = eval_locy_expr(left, bindings, prev_values)?;
            let r = eval_locy_expr(right, bindings, prev_values)?;
            eval_locy_binary_op(&l, op, &r)
        }
        LocyExpr::UnaryOp(op, inner) => {
            let v = eval_locy_expr(inner, bindings, prev_values)?;
            eval_unary_op(op, &v)
        }
    }
}

/// Evaluates a Locy filter condition (`WHERE` of `QUERY`, `DERIVE`, `ABDUCE`,
/// `EXPLAIN RULE`, or a rule body's target-dependent condition).
///
/// True keeps the row; false and NULL ("unknown") drop it, as in Cypher. An
/// evaluation error, or a value that is not a boolean, is an error. Every
/// caller used to map both to "drop the row", so a malformed or unsupported
/// condition silently returned fewer rows — for `DERIVE`, wrote fewer facts,
/// and for `ABDUCE`, reported a conclusion as no longer holding.
///
/// # Errors
///
/// [`LocyError`] when the condition fails to evaluate or is not boolean.
pub fn eval_condition(expr: &Expr, bindings: &FactRow, context: &str) -> Result<bool, LocyError> {
    match eval_expr(expr, bindings)? {
        Value::Bool(b) => Ok(b),
        Value::Null => Ok(false),
        other => Err(LocyError::TypeError {
            message: format!("{context} must be a boolean, got {other:?}"),
        }),
    }
}

/// Evaluate a Cypher expression given variable bindings.
pub fn eval_expr(expr: &Expr, bindings: &FactRow) -> Result<Value, LocyError> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Variable(name) => Ok(bindings.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Property(expr, property) => {
            let base = eval_expr(expr, bindings)?;
            Ok(get_property(&base, property))
        }
        Expr::BinaryOp { left, op, right } => {
            // A non-boolean literal operand of a logical operator is the type
            // error Cypher's planner raises at compile time, whatever the
            // other operand: `true OR 1` is an error, though `true OR n.i` is
            // true.
            if matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::Xor)
                && let Some(operand) = [left, right]
                    .into_iter()
                    .find(|e| crate::query::planner::is_non_boolean_literal(e))
            {
                return Err(LocyError::TypeError {
                    message: format!("{op} requires boolean arguments, got {operand:?}"),
                });
            }
            let l = eval_expr(left, bindings)?;
            let r = eval_expr(right, bindings)?;
            eval_binary_op(&l, op, &r)
        }
        Expr::UnaryOp { op, expr } => {
            if matches!(op, UnaryOp::Not) && crate::query::planner::is_non_boolean_literal(expr) {
                return Err(LocyError::TypeError {
                    message: format!("NOT requires a boolean argument, got {expr:?}"),
                });
            }
            let v = eval_expr(expr, bindings)?;
            eval_unary_op(op, &v)
        }
        Expr::FunctionCall { name, args, .. } => {
            let evaluated_args: Result<Vec<Value>, _> =
                args.iter().map(|a| eval_expr(a, bindings)).collect();
            let evaluated_args = evaluated_args?;
            let upper = name.to_uppercase();
            if upper == "STARTNODE" || upper == "ENDNODE" {
                return endpoint_node(&upper, &evaluated_args, bindings);
            }
            eval_function(name, &evaluated_args)
        }
        Expr::Parameter(name) => {
            bindings
                .get(name)
                .cloned()
                .ok_or_else(|| LocyError::EvaluationError {
                    message: uni_common::UniError::parameter_missing_message([name]),
                })
        }
        Expr::IsNull(inner) => {
            let v = eval_expr(inner, bindings)?;
            Ok(Value::Bool(v.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let v = eval_expr(inner, bindings)?;
            Ok(Value::Bool(!v.is_null()))
        }
        Expr::List(items) => {
            let vals: Result<Vec<Value>, _> =
                items.iter().map(|i| eval_expr(i, bindings)).collect();
            Ok(Value::List(vals?))
        }
        Expr::Map(entries) => {
            let mut map = HashMap::new();
            for (k, v) in entries {
                map.insert(k.clone(), eval_expr(v, bindings)?);
            }
            Ok(Value::Map(map))
        }
        Expr::Case {
            expr: operand,
            when_then,
            else_expr,
        } => {
            // Searched CASE (no operand): the first WHEN whose condition is
            // truthy yields its THEN. Simple CASE (with operand): the first WHEN
            // whose value equals the operand yields its THEN. Otherwise the ELSE
            // branch, or Null when absent. Equality reuses `eval_binary_op` so
            // null/type semantics match the rest of the engine.
            let operand_val = match operand {
                Some(o) => Some(eval_expr(o, bindings)?),
                None => None,
            };
            for (when, then) in when_then {
                let when_val = eval_expr(when, bindings)?;
                let matched = match &operand_val {
                    Some(o) => eval_binary_op(o, &BinaryOp::Eq, &when_val)?
                        .as_bool()
                        .unwrap_or(false),
                    None => when_val.as_bool().unwrap_or(false),
                };
                if matched {
                    return eval_expr(then, bindings);
                }
            }
            match else_expr {
                Some(e) => eval_expr(e, bindings),
                None => Ok(Value::Null),
            }
        }
        Expr::In { expr, list } => {
            let needle = eval_expr(expr, bindings)?;
            let haystack = eval_expr(list, bindings)?;
            // `IN` is three-valued: null when nothing matched but a comparison
            // was unknown. The hand-rolled `any(values_equal)` this replaced
            // could only answer true or false, so `NOT (x IN ['a', null])`
            // came back true and kept a row Cypher drops (#216).
            //
            // Route through the same helper the two Cypher paths use rather
            // than growing a third `IN`: `eval_in_op` also brings exact-i64
            // comparison, NaN handling, recursive list/map descent and the
            // raw-id fallback (an entity tested against a list of bare vids),
            // none of which `values_equal` does.
            match haystack {
                // `x IN null` is null, not an error. The list expression is
                // allowed to evaluate to null — a missing property, an unset
                // parameter — and Cypher propagates rather than failing.
                Value::Null => Ok(Value::Null),
                haystack => {
                    uni_query_functions::expr_eval::eval_in_op(&needle, &haystack).map_err(|e| {
                        LocyError::EvaluationError {
                            message: e.to_string(),
                        }
                    })
                }
            }
        }
        _ => Err(LocyError::EvaluationError {
            message: format!("unsupported expression in in-memory evaluation: {expr:?}"),
        }),
    }
}

pub(crate) fn literal_to_value(lit: &CypherLiteral) -> Value {
    match lit {
        CypherLiteral::Null => Value::Null,
        CypherLiteral::Bool(b) => Value::Bool(*b),
        CypherLiteral::Integer(i) => Value::Int(*i),
        CypherLiteral::Float(f) => Value::Float(*f),
        CypherLiteral::String(s) => Value::String(s.clone()),
        CypherLiteral::Bytes(b) => Value::Bytes(b.clone()),
    }
}

fn get_property(value: &Value, property: &str) -> Value {
    match value {
        Value::Node(n) => n.properties.get(property).cloned().unwrap_or(Value::Null),
        Value::Edge(e) => e.properties.get(property).cloned().unwrap_or(Value::Null),
        Value::Map(m) => m.get(property).cloned().unwrap_or(Value::Null),
        // Temporal component accessors (`d.days`, `dt.year`, …) so that the
        // in-memory evaluator matches Cypher. Without this, a Duration produced
        // by `duration.inDays(...)` in a YIELD value column had `.days` resolve
        // to Null in the SLG path (issue #111, decision C). Mirrors the
        // `_duration_property` / `_temporal_property` UDFs used in the DataFusion
        // path; an unknown component falls back to Null.
        Value::Temporal(uni_common::TemporalValue::Duration { .. }) => {
            crate::query::datetime::eval_duration_accessor(&value.to_string(), property)
                .unwrap_or(Value::Null)
        }
        Value::Temporal(_) => crate::query::datetime::eval_temporal_accessor_value(value, property)
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Evaluates `NOT` or unary `-` with Cypher's NULL and overflow rules.
///
/// Shared by both `eval_locy_expr` and `eval_expr`. `-i64::MIN` is an
/// overflow error, as in Cypher; it used to panic in a debug build and wrap
/// in a release one. A duration negates component-wise, as in Cypher.
fn eval_unary_op(op: &UnaryOp, v: &Value) -> Result<Value, LocyError> {
    use uni_common::TemporalValue;
    let overflow = || LocyError::EvaluationError {
        message: format!("integer overflow negating {v:?}"),
    };
    match (op, v) {
        (_, Value::Null) => Ok(Value::Null),
        (UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
        (UnaryOp::Not, _) => Err(LocyError::TypeError {
            message: format!("NOT requires boolean, got {v:?}"),
        }),
        (UnaryOp::Neg, Value::Int(i)) => i.checked_neg().map(Value::Int).ok_or_else(overflow),
        (UnaryOp::Neg, Value::Float(f)) => Ok(Value::Float(-f)),
        (
            UnaryOp::Neg,
            Value::Temporal(TemporalValue::Duration {
                months,
                days,
                nanos,
            }),
        ) => match (
            months.checked_neg(),
            days.checked_neg(),
            nanos.checked_neg(),
        ) {
            (Some(months), Some(days), Some(nanos)) => {
                Ok(Value::Temporal(TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                }))
            }
            _ => Err(overflow()),
        },
        (UnaryOp::Neg, _) => Err(LocyError::TypeError {
            message: format!("negation requires numeric, got {v:?}"),
        }),
    }
}

fn eval_locy_binary_op(left: &Value, op: &LocyBinaryOp, right: &Value) -> Result<Value, LocyError> {
    eval_binary_op(
        left,
        &crate::query::locy_planner::locy_op_to_cypher_op(op),
        right,
    )
}

/// Evaluates a binary operator over two values, as Cypher does.
///
/// Delegates to the value-level evaluator the Cypher executors share
/// ([`crate::query::expr_eval::eval_binary_op`]), so `+` concatenates lists
/// and strings, adds a duration to a temporal and rejects integer overflow;
/// `=` over a list holding NULL is NULL; `=~` matches a regex; `AND`/`OR` are
/// Kleene logic with a type error for a non-boolean operand no other operand
/// decides; and durations do not order. Locy used to keep its own copy, in
/// which `+` was numeric only (`'a' + 'b'` failed with "numeric op requires
/// number"), integer arithmetic wrapped, and `=~` was unsupported.
fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value, LocyError> {
    crate::query::expr_eval::eval_binary_op(left, op, right).map_err(|e| {
        LocyError::EvaluationError {
            message: e.to_string(),
        }
    })
}

/// `startNode(e)` / `endNode(e)`: the endpoint as the row binds it, or a node
/// carrying only its identity when the row binds no such node, as the Cypher
/// executor answers.
fn endpoint_node(name: &str, args: &[Value], bindings: &FactRow) -> Result<Value, LocyError> {
    let [edge] = args else {
        return Err(LocyError::EvaluationError {
            message: format!("{}() requires exactly 1 argument", name.to_lowercase()),
        });
    };
    let Some((src, dst)) = edge.edge_endpoints() else {
        return Ok(Value::Null);
    };
    let Some(vid) = (if name == "STARTNODE" { src } else { dst }) else {
        return Ok(Value::Null);
    };
    Ok(bindings
        .values()
        .find(|v| v.entity_vid() == Some(vid) && v.entity_labels().is_some())
        .cloned()
        .unwrap_or_else(|| {
            Value::Node(uni_common::Node {
                vid,
                labels: Vec::new(),
                properties: std::collections::HashMap::new(),
            })
        }))
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value, LocyError> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "TOINTEGER" | "TOINT" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Int(i) => Ok(Value::Int(*i)),
                Value::Float(f) => Ok(Value::Int(*f as i64)),
                // A string that is not a number converts to NULL, as in
                // Cypher; it used to be an error.
                Value::String(s) => Ok(s
                    .trim()
                    .parse::<i64>()
                    .map(Value::Int)
                    .or_else(|_| s.trim().parse::<f64>().map(|f| Value::Int(f as i64)))
                    .unwrap_or(Value::Null)),
                _ => Ok(Value::Null),
            }
        }
        "TOFLOAT" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Float(f) => Ok(Value::Float(*f)),
                Value::Int(i) => Ok(Value::Float(*i as f64)),
                Value::String(s) => Ok(s
                    .trim()
                    .parse::<f64>()
                    .map(Value::Float)
                    .unwrap_or(Value::Null)),
                _ => Ok(Value::Null),
            }
        }
        "TOSTRING" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::String(s) => Ok(Value::String(s.clone())),
                Value::Int(i) => Ok(Value::String(i.to_string())),
                Value::Float(f) => Ok(Value::String(f.to_string())),
                Value::Bool(b) => Ok(Value::String(b.to_string())),
                Value::Null => Ok(Value::Null),
                // `toString` over a map, node or path handed the *user* a
                // `Debug` rendering whose field order varied per call.
                other => Ok(Value::String(other.canonical_string())),
            }
        }
        "ABS" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Int(i) => Ok(Value::Int(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                _ => Ok(Value::Null),
            }
        }
        "COALESCE" => {
            for a in args {
                if !a.is_null() {
                    return Ok(a.clone());
                }
            }
            Ok(Value::Null)
        }
        "SIMILAR_TO" | "VECTOR_SIMILARITY" => {
            if args.len() < 2 {
                return Err(LocyError::EvaluationError {
                    message: format!("{name} requires at least 2 arguments"),
                });
            }
            // In Locy context, handle pure vector-vector case directly.
            // Storage-dependent cases (auto-embed, FTS) are not available
            // in the Locy in-memory evaluator.
            crate::query::similar_to::eval_similar_to_pure(&args[0], &args[1]).map_err(|e| {
                LocyError::EvaluationError {
                    message: e.to_string(),
                }
            })
        }
        // A registered third-party Locy predicate (filter/fuzzy) takes priority
        // over the generic scalar path — otherwise a plugin predicate name would
        // die as an "unknown function". This single interception serves every
        // in-memory eval path (SLG, DERIVE, delta, QUERY) since they all route
        // through `eval_expr`/`eval_function`.
        _ => {
            if let Some(result) = try_dispatch_locy_predicate(name, args) {
                return result;
            }
            // Delegate to the full Cypher scalar function evaluator so that every
            // function available in Cypher (temporal, math, string, spatial, …) is
            // automatically available in Locy. Both sides use uni_common::Value, so
            // no type conversion is needed.
            crate::query::expr_eval::eval_scalar_function(name, args, None).map_err(|e| {
                LocyError::EvaluationError {
                    message: e.to_string(),
                }
            })
        }
    }
}

/// Resolve `name` to a registered
/// [`LocyPredicate`](uni_plugin::traits::locy::LocyPredicate) and evaluate it
/// over the single-row `args`, or return `None` when no predicate is
/// registered.
///
/// Resolution uses the same convention-agnostic `candidate_splits` lookup as
/// Locy aggregates and scalars, against the session-local plugin registry. This
/// is what makes the otherwise-dead `LocyPredicate` trait reachable from Locy.
fn try_dispatch_locy_predicate(name: &str, args: &[Value]) -> Option<Result<Value, LocyError>> {
    let session_pr = crate::query::df_udfs_plugin::current_session_plugin_registry()?;
    let entry = uni_plugin::QName::candidate_splits(name)
        .find_map(|cand| session_pr.locy_predicate(&cand))?;
    Some(invoke_locy_predicate(&entry, name, args))
}

/// Bridge Locy `Value` args into a one-row Arrow batch, invoke the predicate,
/// and read the single result cell back as a `Value` (bool, or float for a
/// fuzzy predicate participating in PROB chains).
fn invoke_locy_predicate(
    entry: &uni_plugin::registry::LocyPredicateEntry,
    name: &str,
    args: &[Value],
) -> Result<Value, LocyError> {
    use arrow_array::Array;

    let columnar: Vec<datafusion::logical_expr::ColumnarValue> =
        args.iter().map(value_to_single_row_columnar).collect();

    let err = |e: uni_plugin::FnError| LocyError::EvaluationError {
        message: format!("{name}: {e}"),
    };

    // Fuzzy predicates return a per-row score; plain filters a boolean.
    if entry.signature.supports_fuzzy
        && let Some(res) = entry.predicate.evaluate_fuzzy(&columnar, 1)
    {
        let arr = res.map_err(err)?;
        return Ok(if arr.is_null(0) {
            Value::Null
        } else {
            Value::Float(arr.value(0))
        });
    }

    let arr = entry.predicate.evaluate(&columnar, 1).map_err(err)?;
    Ok(if arr.is_null(0) {
        Value::Null
    } else {
        Value::Bool(arr.value(0))
    })
}

/// Encode one Locy `Value` as a length-1 Arrow column for predicate input.
///
/// Scalars map to their natural Arrow type; a plain `Null` becomes a null
/// `Int64` cell, and any composite value is carried as a CypherValue-encoded
/// `LargeBinary` cell (which a predicate can decode).
fn value_to_single_row_columnar(v: &Value) -> datafusion::logical_expr::ColumnarValue {
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray,
    };
    use std::sync::Arc;

    let arr: ArrayRef = match v {
        Value::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
        Value::Int(i) => Arc::new(Int64Array::from(vec![*i])),
        Value::Float(f) => Arc::new(Float64Array::from(vec![*f])),
        Value::String(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Value::Null => Arc::new(Int64Array::from(vec![None::<i64>])),
        other => {
            let bytes = uni_common::cypher_value_codec::encode(other);
            Arc::new(LargeBinaryArray::from(vec![Some(bytes.as_slice())]))
        }
    };
    datafusion::logical_expr::ColumnarValue::Array(arr)
}

/// Compare two values for equality (Cypher semantics).
///
/// Numbers compare by exact value, at any depth of lists and maps: `1` equals
/// `1.0`, and `2^53 + 1` does not equal `2^53.0`, which widening the integer
/// to `f64` made equal. Unlike Cypher's `=`, NULL equals NULL and NaN equals
/// NaN: this is key matching, not a predicate.
pub fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => {
            canonical_cypher_value(a.clone()) == canonical_cypher_value(b.clone())
        }
        (Value::List(_) | Value::Map(_), Value::List(_) | Value::Map(_))
            if a.entity_ref().is_none() && b.entity_ref().is_none() =>
        {
            canonical_cypher_value(a.clone()) == canonical_cypher_value(b.clone())
        }
        _ => {
            // Entities compare by identity here too. Falling through to derived
            // equality compared a node's vid, labels *and* whole property map,
            // so the same entity hydrated differently on either side — or
            // arriving in the other encoding — came back unequal. `IN` on the
            // in-memory path routes through this function, so that was a silent
            // membership miss rather than an error.
            match (a.entity_ref(), b.entity_ref()) {
                (Some(x), Some(y)) => x == y,
                (Some(_), None) | (None, Some(_)) => false,
                (None, None) => a == b,
            }
        }
    }
}

/// Compare two values for join equality in IS-ref matching.
///
/// For graph entities (`Value::Node`, `Value::Edge`), compares by identity
/// (VID/EID) rather than full structural equality. This is necessary because
/// the same node may have different property sets across different query
/// executions (e.g., schema mode adds `overflow_json: Null` in some paths
/// but not others). For non-graph values, falls back to `values_equal`.
pub fn values_equal_for_join(a: &Value, b: &Value) -> bool {
    // `values_equal` now answers the identity question itself, through
    // `entity_ref`, which also covers the map encoding and the mixed pairings
    // the two arms here used to miss. Kept as a named entry point because the
    // IS-ref call sites read better for it.
    values_equal(a, b)
}

/// Order two values as Cypher's `ORDER BY` does.
///
/// A `QUERY`'s `ORDER BY` and its `min` / `max` answer as Cypher's do, so
/// they share Cypher's orderability ([`cypher_value_order`]): numbers by
/// exact value, NULL last, and values of different types by type. This was
/// [`value_less_than`] both ways, under which `2^53 + 1` tied with `2^53.0`
/// (both widened to `f64`), and any two values of different types tied, so
/// the sort left them in arrival order.
pub fn value_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    cypher_value_order(a, b)
}

/// Whether `a` orders before `b`, for two values of one type.
///
/// Numbers compare by exact value. Values of different types, or of a type
/// with no order here, are never less.
pub fn value_less_than(a: &Value, b: &Value) -> bool {
    if let Some(ord) = cypher_numeric_cmp(a, b) {
        return ord == Some(std::cmp::Ordering::Less);
    }
    match (a, b) {
        (Value::String(x), Value::String(y)) => x < y,
        // `false` sorts before `true`, matching Cypher boolean ordering.
        (Value::Bool(x), Value::Bool(y)) => !x & y,
        (Value::Temporal(x), Value::Temporal(y)) => {
            temporal_less_than(x, y) == std::cmp::Ordering::Less
        }
        _ => false,
    }
}

/// Order two temporal values, matching the executor's `ORDER BY` semantics.
///
/// Same-kind values compare by their canonical instant; mixed kinds fall back
/// to a stable per-variant rank so the ordering is total and deterministic.
fn temporal_less_than(
    left: &uni_common::TemporalValue,
    right: &uni_common::TemporalValue,
) -> std::cmp::Ordering {
    use uni_common::TemporalValue as T;
    match (left, right) {
        (
            T::Date {
                days_since_epoch: l,
            },
            T::Date {
                days_since_epoch: r,
            },
        ) => l.cmp(r),
        (
            T::LocalTime {
                nanos_since_midnight: l,
            },
            T::LocalTime {
                nanos_since_midnight: r,
            },
        ) => l.cmp(r),
        (
            T::Time {
                nanos_since_midnight: lm,
                offset_seconds: lo,
            },
            T::Time {
                nanos_since_midnight: rm,
                offset_seconds: ro,
            },
        ) => {
            let l_utc = *lm as i128 - (*lo as i128) * 1_000_000_000;
            let r_utc = *rm as i128 - (*ro as i128) * 1_000_000_000;
            l_utc.cmp(&r_utc)
        }
        (
            T::LocalDateTime {
                nanos_since_epoch: l,
            },
            T::LocalDateTime {
                nanos_since_epoch: r,
            },
        ) => l.cmp(r),
        (
            T::DateTime {
                nanos_since_epoch: l,
                ..
            },
            T::DateTime {
                nanos_since_epoch: r,
                ..
            },
        ) => l.cmp(r),
        (
            T::Duration {
                months: lm,
                days: ld,
                nanos: ln,
            },
            T::Duration {
                months: rm,
                days: rd,
                nanos: rn,
            },
        ) => (*lm, *ld, *ln).cmp(&(*rm, *rd, *rn)),
        _ => temporal_variant_rank(left).cmp(&temporal_variant_rank(right)),
    }
}

/// Stable ordering rank for mixed-kind temporal comparisons.
fn temporal_variant_rank(v: &uni_common::TemporalValue) -> u8 {
    use uni_common::TemporalValue as T;
    match v {
        T::Date { .. } => 0,
        T::LocalTime { .. } => 1,
        T::Time { .. } => 2,
        T::LocalDateTime { .. } => 3,
        T::DateTime { .. } => 4,
        T::Duration { .. } => 5,
        T::Btic { .. } => 6,
    }
}

/// Compare two values with NULL handling (NULLS LAST, matching Cypher semantics).
pub fn value_compare(a: &Value, b: &Value, null_last: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let null_order = if null_last {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => null_order,
        (false, true) => null_order.reverse(),
        (false, false) => value_cmp(a, b),
    }
}

/// Convert a slice of Arrow RecordBatches into a vector of Locy rows (HashMap<String, Value>).
///
/// Handles DateTime and Time struct types via `uni_common` schema helpers so that
/// temporal values round-trip correctly through the Arrow → Value conversion.
///
/// Node/edge struct columns (`_vid`/`_labels`/`_all_props`) are normalized to
/// `Value::Node` / `Value::Edge` and dotted helper columns (e.g. `a._vid`) are
/// stripped, matching the behaviour of `Executor::record_batches_to_rows`.
pub fn record_batches_to_locy_rows(batches: &[RecordBatch]) -> Vec<FactRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row = HashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                // Phase B Slice 3 (post-Slice-3 follow-up): strip
                // synthetic property-feature columns (`__feat_*`)
                // emitted by `extract_model_invocations` so they
                // don't leak into user-visible FactRows.
                if field.name().starts_with("__feat_") {
                    continue;
                }
                let column = batch.column(col_idx);
                // Recover the Uni DataType so LargeBinary / struct columns decode
                // correctly. `Bytes`, `CypherValue` and `Duration` all map to Arrow
                // `LargeBinary` and are indistinguishable here, so raw `Bytes`
                // columns are tagged with `uni_raw_bytes` at scan time
                // (`scan.rs::property_field`). Without the hint a raw `Bytes` value
                // is run through the CypherValue codec and decodes to `Null`.
                //
                // This mirrors the Cypher read path (`executor/read.rs`), which has
                // had the check since issue #93; the Locy copy never did, so a
                // typed `Bytes` property came back `Null` from `derived` while the
                // same property read through Cypher was intact.
                let data_type = if field
                    .metadata()
                    .get("uni_raw_bytes")
                    .is_some_and(|v| v == "true")
                {
                    Some(&uni_common::DataType::Bytes)
                } else if uni_common::core::schema::is_datetime_struct(field.data_type()) {
                    Some(&uni_common::DataType::DateTime)
                } else if uni_common::core::schema::is_time_struct(field.data_type()) {
                    Some(&uni_common::DataType::Time)
                } else {
                    None
                };
                // Documented exception (#233 class): `record_batches_to_locy_rows`
                // is infallible by signature with 9+ callers across the Locy
                // evaluation stack, so a failure cannot be reported from here.
                // Logged, then degraded — the honest cost of not rippling the
                // signature through that stack in this change.
                let value = match uni_store::storage::arrow_convert::arrow_to_value(
                    column.as_ref(),
                    row_idx,
                    data_type,
                ) {
                    Ok(v) => v.canonical_entity(),
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            column = %field.name(),
                            "Locy fact row: column failed to decode"
                        );
                        uni_common::Value::Null
                    }
                };
                row.insert(field.name().clone(), value);
            }
            normalize_graph_row(&mut row);
            rows.push(row);
        }
    }
    rows
}

/// Post-process a raw Arrow-converted row so that graph entities are represented
/// as `Value::Node` / `Value::Edge` and dotted helper columns are removed.
///
/// RecordBatches from graph scans emit both a bare struct column (e.g. `a`) and
/// exploded helper columns (`a._vid`, `a._labels`, `a._all_props`). The bare
/// column is `Value::Map({_vid, _labels, _all_props})` after `arrow_to_value`.
/// This function detects these maps and converts them to proper `Value::Node` or
/// `Value::Edge`, then strips the helpers.
pub(crate) fn normalize_graph_row(row: &mut FactRow) {
    // Detect bare graph-entity variables: keys without '.' that are Map values
    // containing the internal `_vid` or `_eid` field.
    let entity_vars: Vec<String> = row
        .keys()
        .filter(|k| {
            !k.contains('.')
                && match row.get(*k) {
                    Some(Value::Map(m)) => m.contains_key("_vid") || m.contains_key("_eid"),
                    _ => false,
                }
        })
        .cloned()
        .collect();

    for var in &entity_vars {
        // Merge any dotted helper columns into the bare map (they should already
        // be present from the struct, but merge to be safe).
        let prefix = format!("{}.", var);
        let helper_keys: Vec<String> = row
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        for key in &helper_keys {
            let prop_name = &key[prefix.len()..];
            if let Some(val) = row.get(key).cloned()
                && let Some(Value::Map(m)) = row.get_mut(var)
            {
                m.entry(prop_name.to_string()).or_insert(val);
            }
        }
        // Remove dotted helpers
        for key in helper_keys {
            row.remove(&key);
        }

        // Convert map → Value::Node or Value::Edge
        if let Some(Value::Map(map)) = row.remove(var) {
            row.insert(var.clone(), map_to_graph_entity(map));
        }
    }
}

/// Convert a map with internal graph fields to `Value::Node` or `Value::Edge`.
fn map_to_graph_entity(map: HashMap<String, Value>) -> Value {
    use uni_common::core::id::Vid;
    use uni_common::value::{Edge, Node};

    // Edge. Requiring `Value::Int` under `_eid` specifically meant an `_id`
    // spelling or the string form `"Eid(7)"` fell back out as an unconverted
    // map, and the `as u64` cast turned a negative id into `u64::MAX` (#234).
    // The accessor knows this decoder's `_src_vid`/`_dst_vid` endpoint
    // spelling, so an edge here is never mistaken for a vertex.
    if let Some(uni_common::value::EntityRef::Edge(eid)) =
        uni_common::value::entity_ref_from_map(&map)
    {
        let edge_type = Value::Map(map.clone())
            .edge_type_ref()
            .and_then(|t| t.name().map(str::to_string))
            .unwrap_or_default();
        let src = match map.get("_src_vid") {
            Some(Value::Int(i)) => Vid::new(*i as u64),
            _ => Vid::new(0),
        };
        let dst = match map.get("_dst_vid") {
            Some(Value::Int(i)) => Vid::new(*i as u64),
            _ => Vid::new(0),
        };
        let properties = extract_properties_from_map(&map);
        return Value::Edge(Edge {
            eid,
            edge_type,
            src,
            dst,
            properties,
        });
    }

    // Node: carries a vertex id. Requiring `Value::Int` meant an `_id`/`vid`
    // spelling, or the string form `"Vid(7)"`, fell straight back out as an
    // unconverted map — so any downstream `Value::Node` match silently saw
    // nothing. The `Int` cast also turned a negative id into `u64::MAX`
    // rather than rejecting it (#234). The edge branch above has already
    // returned, so a vertex answer here is the only one that applies.
    if let Some(uni_common::value::EntityRef::Vertex(vid)) =
        uni_common::value::entity_ref_from_map(&map)
    {
        let labels = match map.get("_labels") {
            Some(Value::List(list)) => list
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        let properties = extract_properties_from_map(&map);
        return Value::Node(Node {
            vid,
            labels,
            properties,
        });
    }

    Value::Map(map)
}

/// Extract user-visible properties from a raw graph-entity map.
///
/// The container is found structurally, by
/// [`uni_common::value::entity_property_container`]: `_all_props` (decoded
/// from the CypherValue codec by `arrow_to_value`), whose entries are joined
/// by the inline schema-defined property columns beside it, or a serialised
/// entity's `properties` map. No user name is excluded beyond the reserved
/// ones — an inline property called `properties` used to be dropped.
fn extract_properties_from_map(map: &HashMap<String, Value>) -> HashMap<String, Value> {
    let container = uni_common::value::entity_property_container(map);
    let mut properties: HashMap<String, Value> = container
        .into_iter()
        .flatten()
        .filter(|(k, _)| is_user_visible_property(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Inline non-internal keys (schema-defined property columns), unless the
    // map is a serialised entity whose other keys are its own fields.
    let serialised = container.is_some() && !map.contains_key("_all_props");
    if !serialised {
        for (k, v) in map {
            if is_user_visible_property(k) {
                properties.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
    }

    properties
}

/// Whether a storage column name is a real, user-visible node/edge property.
///
/// `overflow_json` is a materialized storage column holding non-schema
/// properties, not a property itself; it surfaced in user-visible properties
/// on the SLG `QUERY` path while the fixpoint path filtered it — the two
/// surfaces disagreeing about the same node. A leading `_` alone does not make
/// a name internal: a user property `_foo` is visible like any other.
///
/// The one reserved-name rule every read path shares
/// (`uni_common::core::schema::is_reserved_property_name`).
fn is_user_visible_property(name: &str) -> bool {
    !uni_common::core::schema::is_reserved_property_name(name)
}

#[cfg(test)]
mod predicate_dispatch_tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array};
    use datafusion::logical_expr::{ColumnarValue, Volatility};
    use std::sync::Arc;
    use uni_plugin::FnError;
    use uni_plugin::traits::locy::{BatchHint, LocyPredicate, PredSignature};
    use uni_plugin::traits::scalar::ArgType;

    /// Test predicate: `is_positive(f64) -> bool`.
    #[derive(Debug)]
    struct IsPositive {
        sig: PredSignature,
    }

    impl IsPositive {
        fn new() -> Self {
            Self {
                sig: PredSignature {
                    args: vec![ArgType::Primitive(arrow_schema::DataType::Float64)],
                    volatility: Volatility::Immutable,
                    supports_fuzzy: false,
                    batch_hint: BatchHint::Small,
                },
            }
        }
    }

    impl LocyPredicate for IsPositive {
        fn signature(&self) -> &PredSignature {
            &self.sig
        }
        fn evaluate(&self, args: &[ColumnarValue], rows: usize) -> Result<BooleanArray, FnError> {
            let ColumnarValue::Array(a) = &args[0] else {
                return Err(FnError::new(0, "expected array arg"));
            };
            let f = a
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| FnError::new(0, "expected Float64Array"))?;
            Ok((0..rows).map(|i| Some(f.value(i) > 0.0)).collect())
        }
    }

    fn entry() -> uni_plugin::registry::LocyPredicateEntry {
        uni_plugin::registry::LocyPredicateEntry {
            plugin: uni_plugin::PluginId::new("test"),
            signature: IsPositive::new().sig,
            predicate: Arc::new(IsPositive::new()),
        }
    }

    #[test]
    fn invoke_bridges_value_to_bool() {
        let e = entry();
        assert_eq!(
            invoke_locy_predicate(&e, "is_positive", &[Value::Float(2.0)]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            invoke_locy_predicate(&e, "is_positive", &[Value::Float(-1.0)]).unwrap(),
            Value::Bool(false)
        );
        // Int coerces through the natural-type bridge only for Float here; an
        // int arg would arrive as Int64Array and the predicate would reject it,
        // surfacing a clean evaluation error rather than a silent wrong answer.
        assert!(invoke_locy_predicate(&e, "is_positive", &[Value::Int(5)]).is_err());
    }

    #[test]
    fn dispatch_returns_none_without_session_registry() {
        // Outside a session scope there is no registry, so dispatch declines and
        // the caller falls back to the scalar path.
        assert!(try_dispatch_locy_predicate("anything.at_all", &[Value::Int(1)]).is_none());
    }
}

/// The `LocyExpr` operator path (`ALONG`, `prev.field`) answers as the Cypher
/// operators do; `locy::locy_operator_parity` covers the `Expr` path end to
/// end against Cypher.
#[cfg(test)]
mod operator_tests {
    use super::*;

    fn lit(v: CypherLiteral) -> Box<LocyExpr> {
        Box::new(LocyExpr::Cypher(Expr::Literal(v)))
    }

    fn binary(left: Box<LocyExpr>, op: LocyBinaryOp, right: Box<LocyExpr>) -> LocyExpr {
        LocyExpr::BinaryOp { left, op, right }
    }

    #[test]
    fn along_add_concatenates_strings_onto_prev() {
        // ALONG p = prev.p + '>b'
        let expr = binary(
            Box::new(LocyExpr::PrevRef("p".to_string())),
            LocyBinaryOp::Add,
            lit(CypherLiteral::String(">b".to_string())),
        );
        let prev = FactRow::from([("p".to_string(), Value::String("a".to_string()))]);
        let got = eval_locy_expr(&expr, &FactRow::new(), Some(&prev));
        assert_eq!(got.ok(), Some(Value::String("a>b".to_string())));
    }

    #[test]
    fn along_add_rejects_integer_overflow() {
        let expr = binary(
            lit(CypherLiteral::Integer(i64::MAX)),
            LocyBinaryOp::Add,
            lit(CypherLiteral::Integer(1)),
        );
        assert!(eval_locy_expr(&expr, &FactRow::new(), None).is_err());
    }

    #[test]
    fn along_pow_is_a_float() {
        let expr = binary(
            lit(CypherLiteral::Integer(2)),
            LocyBinaryOp::Pow,
            lit(CypherLiteral::Integer(3)),
        );
        assert_eq!(
            eval_locy_expr(&expr, &FactRow::new(), None).ok(),
            Some(Value::Float(8.0))
        );
    }

    #[test]
    fn negating_i64_min_is_an_error() {
        assert!(eval_unary_op(&UnaryOp::Neg, &Value::Int(i64::MIN)).is_err());
    }
}

/// Numbers compare by exact value in Locy's own comparisons; the end-to-end
/// checks are `locy::locy_numeric_exact`.
#[cfg(test)]
mod numeric_tests {
    use super::*;
    use std::cmp::Ordering;

    const TWO_53: i64 = 1 << 53;
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;

    #[test]
    fn values_equal_is_exact_between_integers_and_floats() {
        assert!(!values_equal(
            &Value::Int(TWO_53 + 1),
            &Value::Float(TWO_53 as f64)
        ));
        assert!(!values_equal(
            &Value::Float(TWO_53 as f64),
            &Value::Int(TWO_53 + 1)
        ));
        assert!(!values_equal(&Value::Int(i64::MAX), &Value::Float(TWO_63)));
        assert!(values_equal(
            &Value::Int(TWO_53),
            &Value::Float(TWO_53 as f64)
        ));
        assert!(values_equal(&Value::Int(1), &Value::Float(1.0)));
        assert!(values_equal(
            &Value::List(vec![Value::Int(1)]),
            &Value::List(vec![Value::Float(1.0)])
        ));
        assert!(!values_equal(
            &Value::List(vec![Value::Int(TWO_53 + 1)]),
            &Value::List(vec![Value::Float(TWO_53 as f64)])
        ));
    }

    #[test]
    fn value_order_is_exact_between_integers_and_floats() {
        let (big, big_f) = (Value::Int(TWO_53 + 1), Value::Float(TWO_53 as f64));
        assert!(value_less_than(&big_f, &big));
        assert!(!value_less_than(&big, &big_f));
        assert_eq!(value_cmp(&big, &big_f), Ordering::Greater);
        let (max, max_f) = (Value::Int(i64::MAX), Value::Float(TWO_63));
        assert!(value_less_than(&max, &max_f));
        assert_eq!(value_cmp(&max, &max_f), Ordering::Less);
        assert_eq!(
            value_cmp(&Value::Int(1), &Value::Float(1.0)),
            Ordering::Equal
        );
    }
}
