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
            let l = eval_expr(left, bindings)?;
            let r = eval_expr(right, bindings)?;
            eval_binary_op(&l, op, &r)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, bindings)?;
            eval_unary_op(op, &v)
        }
        Expr::FunctionCall { name, args, .. } => {
            let evaluated_args: Result<Vec<Value>, _> =
                args.iter().map(|a| eval_expr(a, bindings)).collect();
            eval_function(name, &evaluated_args?)
        }
        Expr::Parameter(name) => Ok(bindings.get(name).cloned().unwrap_or(Value::Null)),
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
            match eval_expr(list, bindings)? {
                Value::List(items) => Ok(Value::Bool(
                    items.iter().any(|item| values_equal(&needle, item)),
                )),
                other => Err(LocyError::EvaluationError {
                    message: format!("IN expects a list on the right-hand side, got {other:?}"),
                }),
            }
        }
        _ => Err(LocyError::EvaluationError {
            message: format!("unsupported expression in in-memory evaluation: {expr:?}"),
        }),
    }
}

/// Evaluate an aggregate function over a group of rows.
pub fn eval_aggregate_over_group(
    func_name: &str,
    arg_expr: &Expr,
    group: &[FactRow],
    rule_name: &str,
    fold_name: &str,
) -> Result<Value, LocyError> {
    let upper = func_name.to_uppercase();
    match upper.as_str() {
        "SUM" => {
            let mut total = 0.0_f64;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if let Some(f) = v.as_f64() {
                    total += f;
                }
            }
            if total == total.floor() && total.abs() < i64::MAX as f64 {
                Ok(Value::Int(total as i64))
            } else {
                Ok(Value::Float(total))
            }
        }
        "MSUM" => {
            let mut total = 0.0_f64;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if let Some(f) = v.as_f64() {
                    if f < 0.0 {
                        return Err(LocyError::MsumNegativeValue {
                            rule: rule_name.to_string(),
                            fold: fold_name.to_string(),
                            value: f,
                        });
                    }
                    total += f;
                }
            }
            if total == total.floor() && total.abs() < i64::MAX as f64 {
                Ok(Value::Int(total as i64))
            } else {
                Ok(Value::Float(total))
            }
        }
        "COUNT" | "MCOUNT" => {
            let count = group
                .iter()
                .filter(|row| {
                    eval_expr(arg_expr, row)
                        .map(|v| !v.is_null())
                        .unwrap_or(false)
                })
                .count();
            Ok(Value::Int(count as i64))
        }
        "MIN" | "MMIN" => {
            let mut min_val: Option<Value> = None;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if v.is_null() {
                    continue;
                }
                min_val = Some(match min_val {
                    None => v,
                    Some(cur) => {
                        if value_less_than(&v, &cur) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(min_val.unwrap_or(Value::Null))
        }
        "MAX" | "MMAX" => {
            let mut max_val: Option<Value> = None;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if v.is_null() {
                    continue;
                }
                max_val = Some(match max_val {
                    None => v,
                    Some(cur) => {
                        if value_less_than(&cur, &v) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(max_val.unwrap_or(Value::Null))
        }
        "AVG" => {
            let mut total = 0.0_f64;
            let mut count = 0;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if let Some(f) = v.as_f64() {
                    total += f;
                    count += 1;
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(total / count as f64))
            }
        }
        "COLLECT" => {
            let mut vals = Vec::new();
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if !v.is_null() {
                    vals.push(v);
                }
            }
            Ok(Value::List(vals))
        }
        _ => Err(LocyError::EvaluationError {
            message: format!("unknown aggregate function: {func_name}"),
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

/// Evaluate a unary operator on a value.
///
/// Shared by both `eval_locy_expr` and `eval_expr` to avoid duplicating
/// NOT/negation logic.
fn eval_unary_op(op: &UnaryOp, v: &Value) -> Result<Value, LocyError> {
    match op {
        UnaryOp::Not => match v {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            Value::Null => Ok(Value::Null),
            _ => Err(LocyError::TypeError {
                message: format!("NOT requires boolean, got {v:?}"),
            }),
        },
        UnaryOp::Neg => match v {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Null => Ok(Value::Null),
            _ => Err(LocyError::TypeError {
                message: format!("negation requires numeric, got {v:?}"),
            }),
        },
    }
}

fn eval_locy_binary_op(left: &Value, op: &LocyBinaryOp, right: &Value) -> Result<Value, LocyError> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match op {
        LocyBinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        LocyBinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        LocyBinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        LocyBinaryOp::Div => {
            let r = right.as_f64().unwrap_or(0.0);
            if r == 0.0 {
                return Err(LocyError::EvaluationError {
                    message: "division by zero".to_string(),
                });
            }
            numeric_op(left, right, |a, b| a / b, |a, b| a / b)
        }
        LocyBinaryOp::Mod => {
            // Guard against modulo by zero: an integer `a % 0` panics in Rust,
            // so return a clean evaluation error instead of aborting the query.
            if right.as_f64().unwrap_or(0.0) == 0.0 {
                return Err(LocyError::EvaluationError {
                    message: "modulo by zero".to_string(),
                });
            }
            numeric_op(left, right, |a, b| a % b, |a, b| a % b)
        }
        LocyBinaryOp::Pow => {
            let l = left.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("pow requires numeric, got {left:?}"),
            })?;
            let r = right.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("pow requires numeric, got {right:?}"),
            })?;
            Ok(Value::Float(l.powf(r)))
        }
        LocyBinaryOp::And => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a && b)),
            _ => Ok(Value::Null),
        },
        LocyBinaryOp::Or => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a || b)),
            _ => Ok(Value::Null),
        },
        LocyBinaryOp::Xor => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a ^ b)),
            _ => Ok(Value::Null),
        },
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value, LocyError> {
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOp::Eq => Ok(Value::Bool(left.is_null() && right.is_null())),
            BinaryOp::NotEq => Ok(Value::Bool(!(left.is_null() && right.is_null()))),
            _ => Ok(Value::Null),
        };
    }
    match op {
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            // Guard against integer division by zero, which panics in Rust.
            if right.as_f64().unwrap_or(0.0) == 0.0 {
                return Err(LocyError::EvaluationError {
                    message: "division by zero".to_string(),
                });
            }
            numeric_op(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinaryOp::Mod => {
            // Guard against integer modulo by zero, which panics in Rust.
            if right.as_f64().unwrap_or(0.0) == 0.0 {
                return Err(LocyError::EvaluationError {
                    message: "modulo by zero".to_string(),
                });
            }
            numeric_op(left, right, |a, b| a % b, |a, b| a % b)
        }
        BinaryOp::Pow => {
            let l = left.as_f64().unwrap_or(0.0);
            let r = right.as_f64().unwrap_or(0.0);
            Ok(Value::Float(l.powf(r)))
        }
        BinaryOp::Eq => Ok(Value::Bool(values_equal(left, right))),
        BinaryOp::NotEq => Ok(Value::Bool(!values_equal(left, right))),
        BinaryOp::Lt => Ok(Value::Bool(value_less_than(left, right))),
        BinaryOp::LtEq => Ok(Value::Bool(
            value_less_than(left, right) || values_equal(left, right),
        )),
        BinaryOp::Gt => Ok(Value::Bool(value_less_than(right, left))),
        BinaryOp::GtEq => Ok(Value::Bool(
            value_less_than(right, left) || values_equal(left, right),
        )),
        BinaryOp::And => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a && b)),
            _ => Ok(Value::Null),
        },
        BinaryOp::Or => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a || b)),
            _ => Ok(Value::Null),
        },
        BinaryOp::Xor => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a ^ b)),
            _ => Ok(Value::Null),
        },
        BinaryOp::Contains => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(Value::Bool(l.contains(r))),
            _ => Ok(Value::Null),
        },
        BinaryOp::StartsWith => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(Value::Bool(l.starts_with(r))),
            _ => Ok(Value::Null),
        },
        BinaryOp::EndsWith => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(Value::Bool(l.ends_with(r))),
            _ => Ok(Value::Null),
        },
        _ => Err(LocyError::EvaluationError {
            message: format!("unsupported binary op in in-memory evaluation: {op:?}"),
        }),
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Value, LocyError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(int_op(*a, *b))),
        _ => {
            let a = left.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("numeric op requires number, got {left:?}"),
            })?;
            let b = right.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("numeric op requires number, got {right:?}"),
            })?;
            Ok(Value::Float(float_op(a, b)))
        }
    }
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value, LocyError> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "TOINTEGER" | "TOINT" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Int(i) => Ok(Value::Int(*i)),
                Value::Float(f) => Ok(Value::Int(*f as i64)),
                Value::String(s) => {
                    s.parse::<i64>()
                        .map(Value::Int)
                        .map_err(|_| LocyError::TypeError {
                            message: format!("cannot convert '{s}' to integer"),
                        })
                }
                _ => Ok(Value::Null),
            }
        }
        "TOFLOAT" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Float(f) => Ok(Value::Float(*f)),
                Value::Int(i) => Ok(Value::Float(*i as f64)),
                Value::String(s) => {
                    s.parse::<f64>()
                        .map(Value::Float)
                        .map_err(|_| LocyError::TypeError {
                            message: format!("cannot convert '{s}' to float"),
                        })
                }
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
                _ => Ok(Value::String(format!("{v:?}"))),
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

/// Resolve `name` to a registered [`LocyPredicate`] and evaluate it over the
/// single-row `args`, or return `None` when no predicate is registered.
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

/// Resolve `name` to a registered [`LocyGenerator`] and evaluate it over the
/// single-row `args`, returning the emitted output tuples (one inner `Vec` per
/// generated row, each of length `signature().outputs.len()`). Returns `None`
/// when no generator with that name is registered.
///
/// Mirrors [`try_dispatch_locy_predicate`] — same `candidate_splits` lookup
/// against the session-local plugin registry — but is table-valued (1:N).
pub(crate) fn dispatch_locy_generator(
    name: &str,
    args: &[Value],
) -> Option<Result<Vec<Vec<Value>>, LocyError>> {
    let session_pr = crate::query::df_udfs_plugin::current_session_plugin_registry()?;
    let entry = uni_plugin::QName::candidate_splits(name)
        .find_map(|cand| session_pr.locy_generator(&cand))?;
    Some(invoke_locy_generator(&entry, name, args))
}

/// Bridge Locy `Value` args into a one-row Arrow batch, invoke the generator,
/// and read its table-valued output back as `Vec<Vec<Value>>` (one inner `Vec`
/// per emitted tuple, columns in `signature().outputs` order).
fn invoke_locy_generator(
    entry: &uni_plugin::registry::LocyGeneratorEntry,
    name: &str,
    args: &[Value],
) -> Result<Vec<Vec<Value>>, LocyError> {
    let columnar: Vec<datafusion::logical_expr::ColumnarValue> =
        args.iter().map(value_to_single_row_columnar).collect();

    let err = |e: uni_plugin::FnError| LocyError::EvaluationError {
        message: format!("{name}: {e}"),
    };

    let out = entry.generator.generate(&columnar, 1).map_err(err)?;
    let emitted = out.row_map.len();
    let mut tuples = Vec::with_capacity(emitted);
    for i in 0..emitted {
        let mut tuple = Vec::with_capacity(out.columns.len());
        for col in &out.columns {
            tuple.push(uni_store::storage::arrow_convert::arrow_to_value(
                col, i, None,
            ));
        }
        tuples.push(tuple);
    }
    Ok(tuples)
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
pub fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Float(y)) => (*x as f64) == *y,
        (Value::Float(x), Value::Int(y)) => *x == (*y as f64),
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

/// Compare two values returning an Ordering.
pub fn value_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    if value_less_than(a, b) {
        std::cmp::Ordering::Less
    } else if value_less_than(b, a) {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Equal
    }
}

/// Compare two values for ordering (less than).
pub fn value_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Int(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Int(y)) => *x < (*y as f64),
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
                let value = uni_store::storage::arrow_convert::arrow_to_value(
                    column.as_ref(),
                    row_idx,
                    data_type,
                );
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
    use uni_common::core::id::{Eid, Vid};
    use uni_common::value::{Edge, Node};

    // Edge: has _eid
    if let Some(eid_val) = map.get("_eid") {
        let eid = match eid_val {
            Value::Int(i) => Eid::new(*i as u64),
            _ => return Value::Map(map),
        };
        let edge_type = match map.get("_type") {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };
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
/// Properties are stored in `_all_props` (deserialized by `arrow_to_value` from
/// the LargeBinary CypherValue codec). Any non-internal keys at the top level
/// are also included as schema-defined column properties.
fn extract_properties_from_map(map: &HashMap<String, Value>) -> HashMap<String, Value> {
    let mut properties = HashMap::new();

    // Primary source: _all_props contains all properties from storage
    if let Some(Value::Map(all_props)) = map.get("_all_props") {
        for (k, v) in all_props {
            if is_user_visible_property(k) {
                properties.insert(k.clone(), v.clone());
            }
        }
    }

    // Secondary: inline non-internal keys (schema-defined property columns)
    for (k, v) in map {
        if is_user_visible_property(k) && k != "properties" {
            properties.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    properties
}

/// Whether a storage column name is a real, user-visible node/edge property.
///
/// `overflow_json` is a materialized storage column holding non-schema
/// properties, not a property itself. It does not start with `_`, so the
/// leading-underscore convention alone let it through and it surfaced in
/// user-visible properties on the SLG `QUERY` path while the fixpoint path
/// filtered it — the two surfaces disagreeing about the same node.
///
/// Mirrors the predicate the scan layer already applies
/// (`df_graph/scan.rs`, the projected-property filter).
fn is_user_visible_property(name: &str) -> bool {
    !name.starts_with('_') && name != "overflow_json" && name != "_all_props"
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
