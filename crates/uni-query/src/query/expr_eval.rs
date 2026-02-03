// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Expression evaluation helper functions.
//!
//! This module extracts high-complexity expression evaluation logic from the main executor
//! to reduce cognitive complexity and improve maintainability.

use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::cmp::Ordering;

use crate::query::datetime::{
    CypherDuration, add_duration_to_date, add_duration_to_datetime, datetime_difference,
    duration_to_micros, eval_datetime_function, is_date_value, is_datetime_value,
    is_duration_value, parse_datetime_utc,
};
use crate::query::spatial::eval_spatial_function;
use uni_cypher::ast::BinaryOp;

/// Evaluate a binary operation on two already-evaluated values.
///
/// This function handles all binary operators (Eq, NotEq, And, Or, Gt, Lt, etc.)
/// and returns the result of the operation.
pub fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
    match op {
        BinaryOp::Eq => Ok(Value::Bool(left == right)),
        BinaryOp::NotEq => Ok(Value::Bool(left != right)),
        BinaryOp::And => {
            let l = left
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for AND left operand"))?;
            let r = right
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for AND right operand"))?;
            Ok(Value::Bool(l && r))
        }
        BinaryOp::Or => {
            let l = left
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for OR left operand"))?;
            let r = right
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for OR right operand"))?;
            Ok(Value::Bool(l || r))
        }
        BinaryOp::Xor => {
            let l = left
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for XOR left operand"))?;
            let r = right
                .as_bool()
                .ok_or_else(|| anyhow!("Expected bool for XOR right operand"))?;
            Ok(Value::Bool(l ^ r))
        }
        BinaryOp::Gt => eval_comparison(left, right, |ordering| ordering.is_gt()),
        BinaryOp::Lt => eval_comparison(left, right, |ordering| ordering.is_lt()),
        BinaryOp::GtEq => eval_comparison(left, right, |ordering| ordering.is_ge()),
        BinaryOp::LtEq => eval_comparison(left, right, |ordering| ordering.is_le()),
        BinaryOp::Contains => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left side of CONTAINS must be a string"))?;
            let r = right
                .as_str()
                .ok_or_else(|| anyhow!("Right side of CONTAINS must be a string"))?;
            Ok(Value::Bool(l.contains(r)))
        }
        BinaryOp::StartsWith => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left side of STARTS WITH must be a string"))?;
            let r = right
                .as_str()
                .ok_or_else(|| anyhow!("Right side of STARTS WITH must be a string"))?;
            Ok(Value::Bool(l.starts_with(r)))
        }
        BinaryOp::EndsWith => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left side of ENDS WITH must be a string"))?;
            let r = right
                .as_str()
                .ok_or_else(|| anyhow!("Right side of ENDS WITH must be a string"))?;
            Ok(Value::Bool(l.ends_with(r)))
        }
        BinaryOp::Add => {
            if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                // Determine if result should be int or float
                if left.is_i64() && right.is_i64() {
                    Ok(json!(left.as_i64().unwrap() + right.as_i64().unwrap()))
                } else {
                    Ok(json!(l + r))
                }
            } else if let (Value::String(l), Value::String(r)) = (left, right) {
                Ok(Value::String(format!("{}{}", l, r)))
            } else if is_datetime_value(left) && is_duration_value(right) {
                // datetime + duration
                let dt_str = left.as_str().unwrap();
                let micros = duration_to_micros(right)?;
                Ok(Value::String(add_duration_to_datetime(dt_str, micros)?))
            } else if is_date_value(left) && is_duration_value(right) {
                // date + duration
                let dt_str = left.as_str().unwrap();
                let micros = duration_to_micros(right)?;
                Ok(Value::String(add_duration_to_date(dt_str, micros)?))
            } else if is_duration_value(left) && is_datetime_value(right) {
                // duration + datetime
                let dt_str = right.as_str().unwrap();
                let micros = duration_to_micros(left)?;
                Ok(Value::String(add_duration_to_datetime(dt_str, micros)?))
            } else if is_duration_value(left) && is_date_value(right) {
                // duration + date
                let dt_str = right.as_str().unwrap();
                let micros = duration_to_micros(left)?;
                Ok(Value::String(add_duration_to_date(dt_str, micros)?))
            } else if is_duration_value(left) && is_duration_value(right) {
                // duration + duration
                let total_micros = duration_to_micros(left)? + duration_to_micros(right)?;
                let duration = CypherDuration::from_micros(total_micros);
                Ok(Value::String(duration.to_iso8601()))
            } else {
                Err(anyhow!("Invalid types for addition"))
            }
        }
        BinaryOp::Sub => {
            if is_datetime_value(left) && is_duration_value(right) {
                // datetime - duration
                let dt_str = left.as_str().unwrap();
                let micros = duration_to_micros(right)?;
                Ok(Value::String(add_duration_to_datetime(dt_str, -micros)?))
            } else if is_date_value(left) && is_duration_value(right) {
                // date - duration
                let dt_str = left.as_str().unwrap();
                let micros = duration_to_micros(right)?;
                Ok(Value::String(add_duration_to_date(dt_str, -micros)?))
            } else if is_datetime_value(left) && is_datetime_value(right) {
                // datetime - datetime -> duration (return ISO 8601)
                let dt1 = left.as_str().unwrap();
                let dt2 = right.as_str().unwrap();
                let micros = datetime_difference(dt1, dt2)?;
                let duration = CypherDuration::from_micros(micros);
                Ok(Value::String(duration.to_iso8601()))
            } else if is_duration_value(left) && is_duration_value(right) {
                // duration - duration
                let diff_micros = duration_to_micros(left)? - duration_to_micros(right)?;
                let duration = CypherDuration::from_micros(diff_micros);
                Ok(Value::String(duration.to_iso8601()))
            } else {
                eval_numeric_op(left, right, |a, b| a - b)
            }
        }
        BinaryOp::Mul => eval_numeric_op(left, right, |a, b| a * b),
        BinaryOp::Div => eval_numeric_op(left, right, |a, b| a / b),
        BinaryOp::Mod => eval_numeric_op(left, right, |a, b| a % b),
        BinaryOp::Pow => eval_numeric_op(left, right, |a, b| a.powf(b)),
        BinaryOp::Regex => {
            // Handle NULL operands per Cypher semantics
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left operand of =~ must be a string"))?;
            let pattern = right
                .as_str()
                .ok_or_else(|| anyhow!("Right operand of =~ must be a regex pattern string"))?;
            let re = regex::Regex::new(pattern)
                .map_err(|e| anyhow!("Invalid regex pattern '{}': {}", pattern, e))?;
            Ok(Value::Bool(re.is_match(l)))
        }
        BinaryOp::ApproxEq => {
            // Delegate to existing vector similarity implementation
            eval_vector_similarity(left, right)
        }
    }
}

/// Evaluate IN operator.
pub fn eval_in_op(left: &Value, right: &Value) -> Result<Value> {
    if let Value::Array(arr) = right {
        // Check exact match first
        if arr.contains(left) {
            return Ok(Value::Bool(true));
        }

        // Fallback: Check for Node Object vs VID mismatch
        // If left is a Node Object with _vid
        if let Value::Object(map) = left
            && let Some(vid_val) = map.get("_vid")
        {
            // Check if arr contains this VID (as Number or String "label:offset")
            for item in arr {
                // If item is String "label:offset"
                if let Value::String(s) = item {
                    // Convert vid_val to VID string
                    if let Some(vid_u64) = vid_val.as_u64() {
                        let vid = uni_common::core::id::Vid::from(vid_u64);
                        if s == &vid.to_string() {
                            return Ok(Value::Bool(true));
                        }
                    }
                }
                // If item is Number (raw VID)
                if let Value::Number(n) = item
                    && let Some(vid_u64) = vid_val.as_u64()
                    && let Some(n_u64) = n.as_u64()
                    && vid_u64 == n_u64
                {
                    return Ok(Value::Bool(true));
                }
            }
        }

        Ok(Value::Bool(false))
    } else {
        Err(anyhow!("Right side of IN must be a list"))
    }
}

fn eval_numeric_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    let (l, r) = match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => (l, r),
        _ => return Err(anyhow!("Arithmetic operation requires numbers")),
    };
    let result = op(l, r);
    // Return integer if result has no fractional part and both inputs were integers
    if result.fract() == 0.0 && left.is_i64() && right.is_i64() {
        Ok(json!(result as i64))
    } else {
        Ok(json!(result))
    }
}

/// Helper for comparisons between two values.
fn eval_comparison<F>(left: &Value, right: &Value, check: F) -> Result<Value>
where
    F: Fn(Ordering) -> bool,
{
    if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
        return Ok(Value::Bool(check(l.cmp(&r))));
    }
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        let ord = l
            .partial_cmp(&r)
            .ok_or_else(|| anyhow!("Cannot compare NaN"))?;
        return Ok(Value::Bool(check(ord)));
    }
    if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
        return Ok(Value::Bool(check(l.cmp(r))));
    }
    Err(anyhow!("Comparison only supported for numbers and strings"))
}

// ============================================================================
// List/Collection function helpers
// ============================================================================

fn eval_size(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(json!(arr.len())),
        Value::Object(map) => Ok(json!(map.len())),
        Value::String(s) => Ok(json!(s.len())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("size() expects a List, Map, or String")),
    }
}

fn eval_keys(arg: &Value) -> Result<Value> {
    match arg {
        Value::Object(map) => Ok(json!(map.keys().collect::<Vec<_>>())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("keys() expects a Map")),
    }
}

fn eval_head(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(arr.first().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("head() expects a List")),
    }
}

fn eval_tail(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => {
            if arr.is_empty() {
                Ok(json!([]))
            } else {
                Ok(json!(arr[1..]))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("tail() expects a List")),
    }
}

fn eval_last(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(arr.last().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("last() expects a List")),
    }
}

fn eval_length(arg: &Value) -> Result<Value> {
    match arg {
        Value::Array(arr) => Ok(json!(arr.len())),
        Value::String(s) => Ok(json!(s.len())),
        Value::Object(map) => {
            // Path object?
            if map.contains_key("nodes")
                && map.contains_key("relationships")
                && let Some(Value::Array(rels)) = map.get("relationships")
            {
                return Ok(json!(rels.len()));
            }
            Ok(Value::Null)
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("length() expects a List, String, or Path")),
    }
}

fn eval_nodes(arg: &Value) -> Result<Value> {
    match arg {
        Value::Object(map) => {
            if let Some(nodes) = map.get("nodes") {
                Ok(nodes.clone())
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("nodes() expects a Path")),
    }
}

fn eval_relationships(arg: &Value) -> Result<Value> {
    match arg {
        Value::Object(map) => {
            if let Some(rels) = map.get("relationships") {
                Ok(rels.clone())
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("relationships() expects a Path")),
    }
}

/// Evaluate list/collection functions: SIZE, KEYS, HEAD, TAIL, LAST, LENGTH, NODES, RELATIONSHIPS
fn eval_list_function(name: &str, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("{}() requires 1 argument", name));
    }
    match name {
        "SIZE" => eval_size(&args[0]),
        "KEYS" => eval_keys(&args[0]),
        "HEAD" => eval_head(&args[0]),
        "TAIL" => eval_tail(&args[0]),
        "LAST" => eval_last(&args[0]),
        "LENGTH" => eval_length(&args[0]),
        "NODES" => eval_nodes(&args[0]),
        "RELATIONSHIPS" => eval_relationships(&args[0]),
        _ => Err(anyhow!("Unknown list function: {}", name)),
    }
}

// ============================================================================
// Type conversion function helpers
// ============================================================================

fn eval_tointeger(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(json!(i))
            } else if let Some(f) = n.as_f64() {
                Ok(json!(f as i64))
            } else {
                Ok(Value::Null)
            }
        }
        Value::String(s) => Ok(s.parse::<i64>().map(|i| json!(i)).unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

fn eval_tofloat(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f)).unwrap_or(Value::Null)),
        Value::String(s) => Ok(s.parse::<f64>().map(|f| json!(f)).unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

fn eval_tostring(arg: &Value) -> Result<Value> {
    match arg {
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Number(n) => Ok(Value::String(n.to_string())),
        Value::Bool(b) => Ok(Value::String(b.to_string())),
        Value::Null => Ok(Value::Null),
        other => Ok(Value::String(other.to_string())),
    }
}

fn eval_toboolean(arg: &Value) -> Result<Value> {
    match arg {
        Value::Bool(b) => Ok(Value::Bool(*b)),
        Value::String(s) => {
            let lower = s.to_lowercase();
            if lower == "true" {
                Ok(Value::Bool(true))
            } else if lower == "false" {
                Ok(Value::Bool(false))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Ok(Value::Null),
    }
}

/// Evaluate type conversion functions: TOINTEGER, TOFLOAT, TOSTRING, TOBOOLEAN
fn eval_type_function(name: &str, args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("{}() requires 1 argument", name));
    }
    match name {
        "TOINTEGER" | "TOINT" => eval_tointeger(&args[0]),
        "TOFLOAT" => eval_tofloat(&args[0]),
        "TOSTRING" => eval_tostring(&args[0]),
        "TOBOOLEAN" | "TOBOOL" => eval_toboolean(&args[0]),
        _ => Err(anyhow!("Unknown type function: {}", name)),
    }
}

// ============================================================================
// Math function helpers
// ============================================================================

fn eval_abs(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(json!(i.abs()))
            } else if let Some(f) = n.as_f64() {
                Ok(json!(f.abs()))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("abs() expects a number")),
    }
}

fn eval_ceil(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f.ceil())).unwrap_or(arg.clone())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("ceil() expects a number")),
    }
}

fn eval_floor(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f.floor())).unwrap_or(arg.clone())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("floor() expects a number")),
    }
}

fn eval_round(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => Ok(n.as_f64().map(|f| json!(f.round())).unwrap_or(arg.clone())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("round() expects a number")),
    }
}

fn eval_sqrt(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f < 0.0 {
                    Ok(Value::Null)
                } else {
                    Ok(json!(f.sqrt()))
                }
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sqrt() expects a number")),
    }
}

fn eval_sign(arg: &Value) -> Result<Value> {
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f > 0.0 {
                    Ok(json!(1))
                } else if f < 0.0 {
                    Ok(json!(-1))
                } else {
                    Ok(json!(0))
                }
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sign() expects a number")),
    }
}

fn eval_log(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "log", f64::ln)
}

fn eval_log10(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "log10", f64::log10)
}

fn eval_exp(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "exp", f64::exp)
}

fn eval_power(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("power() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::Number(base), Value::Number(exp)) => {
            if let (Some(b), Some(e)) = (base.as_f64(), exp.as_f64()) {
                Ok(json!(b.powf(e)))
            } else {
                Ok(Value::Null)
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(anyhow!("power() expects numeric arguments")),
    }
}

/// Apply a unary numeric operation, handling null and type checking.
fn eval_unary_numeric_op<F>(arg: &Value, func_name: &str, op: F) -> Result<Value>
where
    F: Fn(f64) -> f64,
{
    match arg {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(json!(op(f)))
            } else {
                Ok(Value::Null)
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("{}() expects a number", func_name)),
    }
}

fn eval_sin(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "sin", f64::sin)
}

fn eval_cos(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "cos", f64::cos)
}

fn eval_tan(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "tan", f64::tan)
}

fn eval_asin(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "asin", f64::asin)
}

fn eval_acos(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "acos", f64::acos)
}

fn eval_atan(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "atan", f64::atan)
}

fn eval_atan2(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("atan2() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::Number(y), Value::Number(x)) => {
            if let (Some(y_val), Some(x_val)) = (y.as_f64(), x.as_f64()) {
                Ok(json!(y_val.atan2(x_val)))
            } else {
                Ok(Value::Null)
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(anyhow!("atan2() expects numeric arguments")),
    }
}

fn eval_degrees(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "degrees", f64::to_degrees)
}

fn eval_radians(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "radians", f64::to_radians)
}

fn eval_haversin(arg: &Value) -> Result<Value> {
    // haversin(x) = (1 - cos(x)) / 2
    eval_unary_numeric_op(arg, "haversin", |f| (1.0 - f.cos()) / 2.0)
}

/// Helper to require exactly one argument for a function.
fn require_one_math_arg<'a>(name: &str, args: &'a [Value]) -> Result<&'a Value> {
    if args.len() != 1 {
        return Err(anyhow!("{} requires 1 argument", name));
    }
    Ok(&args[0])
}

/// Evaluate math functions: ABS, CEIL, FLOOR, ROUND, SQRT, SIGN, LOG, LOG10, EXP, POWER, SIN, COS, TAN, etc.
fn eval_math_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        // Single-argument functions
        "ABS" => eval_abs(require_one_math_arg(name, args)?),
        "CEIL" => eval_ceil(require_one_math_arg(name, args)?),
        "FLOOR" => eval_floor(require_one_math_arg(name, args)?),
        "ROUND" => eval_round(require_one_math_arg(name, args)?),
        "SQRT" => eval_sqrt(require_one_math_arg(name, args)?),
        "SIGN" => eval_sign(require_one_math_arg(name, args)?),
        "LOG" => eval_log(require_one_math_arg(name, args)?),
        "LOG10" => eval_log10(require_one_math_arg(name, args)?),
        "EXP" => eval_exp(require_one_math_arg(name, args)?),
        "SIN" => eval_sin(require_one_math_arg(name, args)?),
        "COS" => eval_cos(require_one_math_arg(name, args)?),
        "TAN" => eval_tan(require_one_math_arg(name, args)?),
        "ASIN" => eval_asin(require_one_math_arg(name, args)?),
        "ACOS" => eval_acos(require_one_math_arg(name, args)?),
        "ATAN" => eval_atan(require_one_math_arg(name, args)?),
        "DEGREES" => eval_degrees(require_one_math_arg(name, args)?),
        "RADIANS" => eval_radians(require_one_math_arg(name, args)?),
        "HAVERSIN" => eval_haversin(require_one_math_arg(name, args)?),
        // Two-argument functions
        "POWER" | "POW" => eval_power(args),
        "ATAN2" => eval_atan2(args),
        // Zero-argument constants
        "PI" => {
            if !args.is_empty() {
                return Err(anyhow!("PI takes no arguments"));
            }
            Ok(json!(std::f64::consts::PI))
        }
        "E" => {
            if !args.is_empty() {
                return Err(anyhow!("E takes no arguments"));
            }
            Ok(json!(std::f64::consts::E))
        }
        "RAND" => {
            if !args.is_empty() {
                return Err(anyhow!("RAND takes no arguments"));
            }
            use rand::Rng;
            let mut rng = rand::thread_rng();
            Ok(json!(rng.gen_range(0.0..1.0)))
        }
        _ => Err(anyhow!("Unknown math function: {}", name)),
    }
}

// ============================================================================
// String function helpers
// ============================================================================

fn require_one_arg<'a>(name: &str, args: &'a [Value]) -> Result<&'a Value> {
    if args.len() != 1 {
        return Err(anyhow!("{}() requires 1 argument", name));
    }
    Ok(&args[0])
}

/// Apply a unary string operation, handling null and type checking.
fn eval_unary_string_op<F>(arg: &Value, func_name: &str, op: F) -> Result<Value>
where
    F: FnOnce(&str) -> String,
{
    match arg {
        Value::String(s) => Ok(Value::String(op(s))),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("{}() expects a string", func_name)),
    }
}

fn eval_toupper(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("toUpper", args)?;
    eval_unary_string_op(arg, "toUpper", |s| s.to_uppercase())
}

fn eval_tolower(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("toLower", args)?;
    eval_unary_string_op(arg, "toLower", |s| s.to_lowercase())
}

fn eval_trim(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("trim", args)?;
    eval_unary_string_op(arg, "trim", |s| s.trim().to_string())
}

fn eval_ltrim(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("ltrim", args)?;
    eval_unary_string_op(arg, "ltrim", |s| s.trim_start().to_string())
}

fn eval_rtrim(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("rtrim", args)?;
    eval_unary_string_op(arg, "rtrim", |s| s.trim_end().to_string())
}

fn eval_reverse(args: &[Value]) -> Result<Value> {
    let arg = require_one_arg("reverse", args)?;
    match arg {
        Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
        Value::Array(arr) => Ok(Value::Array(arr.iter().rev().cloned().collect())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("reverse() expects a string or list")),
    }
}

fn eval_replace(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(anyhow!("replace() requires 3 arguments"));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::String(s), Value::String(search), Value::String(replacement)) => Ok(Value::String(
            s.replace(search.as_str(), replacement.as_str()),
        )),
        (Value::Null, _, _) => Ok(Value::Null),
        _ => Err(anyhow!("replace() expects string arguments")),
    }
}

fn eval_split(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("split() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(delimiter)) => {
            let parts: Vec<Value> = s
                .split(delimiter.as_str())
                .map(|p| Value::String(p.to_string()))
                .collect();
            Ok(Value::Array(parts))
        }
        (Value::Null, _) => Ok(Value::Null),
        _ => Err(anyhow!("split() expects string arguments")),
    }
}

fn eval_substring(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("substring() requires 2 or 3 arguments"));
    }
    match &args[0] {
        Value::String(s) => {
            let start = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("substring() start must be an integer"))?
                as usize;
            let len = if args.len() == 3 {
                args[2]
                    .as_i64()
                    .ok_or_else(|| anyhow!("substring() length must be an integer"))?
                    as usize
            } else {
                s.len().saturating_sub(start)
            };
            let chars: Vec<char> = s.chars().collect();
            let end = (start + len).min(chars.len());
            let result: String = chars[start.min(chars.len())..end].iter().collect();
            Ok(Value::String(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("substring() expects a string")),
    }
}

fn eval_left(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("left() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Number(n)) => {
            let len = n.as_i64().unwrap_or(0) as usize;
            Ok(Value::String(s.chars().take(len).collect()))
        }
        (Value::Null, _) => Ok(Value::Null),
        _ => Err(anyhow!("left() expects a string and integer")),
    }
}

fn eval_right(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("right() requires 2 arguments"));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Number(n)) => {
            let len = n.as_i64().unwrap_or(0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(len);
            Ok(Value::String(chars[start..].iter().collect()))
        }
        (Value::Null, _) => Ok(Value::Null),
        _ => Err(anyhow!("right() expects a string and integer")),
    }
}

fn eval_lpad(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("lpad() requires 2 or 3 arguments"));
    }
    let s = match &args[0] {
        Value::String(s) => s,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("lpad() expects a string as first argument")),
    };
    let len = match &args[1] {
        Value::Number(n) => n.as_i64().unwrap_or(0) as usize,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("lpad() expects an integer as second argument")),
    };
    // Limit max length to prevent OOM
    if len > 1_000_000 {
        return Err(anyhow!("lpad() length exceeds maximum limit of 1,000,000"));
    }
    let pad_str = if args.len() == 3 {
        match &args[2] {
            Value::String(p) => p.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err(anyhow!("lpad() expects a string as third argument")),
        }
    } else {
        " "
    };

    let s_chars: Vec<char> = s.chars().collect();
    if s_chars.len() >= len {
        Ok(Value::String(s_chars.into_iter().take(len).collect()))
    } else {
        let pad_chars: Vec<char> = pad_str.chars().collect();
        if pad_chars.is_empty() {
            // If pad string is empty, we can't pad. Return truncated or original?
            // Postgres returns original string if pad is empty? No, it probably does nothing or errors.
            // Let's assume standard behavior: return original if len > s.len but pad is empty?
            // Actually, if pad is empty, we can't reach target length.
            // Return original string?
            return Ok(Value::String(s.clone()));
        }
        let needed = len - s_chars.len();
        let mut result = String::with_capacity(len);

        let full_pads = needed / pad_chars.len();
        let partial_pad = needed % pad_chars.len();

        for _ in 0..full_pads {
            result.push_str(pad_str);
        }
        result.extend(pad_chars.into_iter().take(partial_pad));
        result.push_str(s);

        Ok(Value::String(result))
    }
}

fn eval_rpad(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("rpad() requires 2 or 3 arguments"));
    }
    let s = match &args[0] {
        Value::String(s) => s,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("rpad() expects a string as first argument")),
    };
    let len = match &args[1] {
        Value::Number(n) => n.as_i64().unwrap_or(0) as usize,
        Value::Null => return Ok(Value::Null),
        _ => return Err(anyhow!("rpad() expects an integer as second argument")),
    };
    // Limit max length to prevent OOM
    if len > 1_000_000 {
        return Err(anyhow!("rpad() length exceeds maximum limit of 1,000,000"));
    }
    let pad_str = if args.len() == 3 {
        match &args[2] {
            Value::String(p) => p.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err(anyhow!("rpad() expects a string as third argument")),
        }
    } else {
        " "
    };

    let s_chars: Vec<char> = s.chars().collect();
    if s_chars.len() >= len {
        Ok(Value::String(s_chars.into_iter().take(len).collect()))
    } else {
        let mut result = String::from(s);
        let pad_chars: Vec<char> = pad_str.chars().collect();
        if pad_chars.is_empty() {
            return Ok(Value::String(s.clone()));
        }

        let needed = len - s_chars.len();
        let full_pads = needed / pad_chars.len();
        let partial_pad = needed % pad_chars.len();

        for _ in 0..full_pads {
            result.push_str(pad_str);
        }
        result.extend(pad_chars.into_iter().take(partial_pad));

        Ok(Value::String(result))
    }
}

/// Evaluate string functions: TOUPPER, TOLOWER, TRIM, LTRIM, RTRIM, REVERSE, REPLACE, SPLIT, SUBSTRING, LEFT, RIGHT, LPAD, RPAD
fn eval_string_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "TOUPPER" | "UPPER" => eval_toupper(args),
        "TOLOWER" | "LOWER" => eval_tolower(args),
        "TRIM" => eval_trim(args),
        "LTRIM" => eval_ltrim(args),
        "RTRIM" => eval_rtrim(args),
        "REVERSE" => eval_reverse(args),
        "REPLACE" => eval_replace(args),
        "SPLIT" => eval_split(args),
        "SUBSTRING" => eval_substring(args),
        "LEFT" => eval_left(args),
        "RIGHT" => eval_right(args),
        "LPAD" => eval_lpad(args),
        "RPAD" => eval_rpad(args),
        _ => Err(anyhow!("Unknown string function: {}", name)),
    }
}

/// Evaluate the RANGE function
fn eval_range_function(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("range() requires 2 or 3 arguments"));
    }
    let start = args[0]
        .as_i64()
        .ok_or_else(|| anyhow!("range() start must be an integer"))?;
    let end = args[1]
        .as_i64()
        .ok_or_else(|| anyhow!("range() end must be an integer"))?;
    let step = if args.len() == 3 {
        args[2]
            .as_i64()
            .ok_or_else(|| anyhow!("range() step must be an integer"))?
    } else {
        1
    };
    if step == 0 {
        return Err(anyhow!("range() step cannot be zero"));
    }
    let mut result = Vec::new();
    let mut i = start;
    if step > 0 {
        while i <= end {
            result.push(json!(i));
            i += step;
        }
    } else {
        while i >= end {
            result.push(json!(i));
            i += step;
        }
    }
    Ok(Value::Array(result))
}

/// Evaluate a built-in scalar function.
///
/// This handles functions like COALESCE, NULLIF, SIZE, KEYS, HEAD, TAIL, etc.
/// Functions that require argument evaluation (like COALESCE) take pre-evaluated args.
pub fn eval_scalar_function(name: &str, args: &[Value]) -> Result<Value> {
    let name_upper = name.to_uppercase();

    // Null-handling functions
    match name_upper.as_str() {
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            return Ok(Value::Null);
        }
        "NULLIF" => {
            if args.len() != 2 {
                return Err(anyhow!("NULLIF requires 2 arguments"));
            }
            return if args[0] == args[1] {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            };
        }
        _ => {}
    }

    // List/Collection functions
    if matches!(
        name_upper.as_str(),
        "SIZE" | "KEYS" | "HEAD" | "TAIL" | "LAST" | "LENGTH" | "NODES" | "RELATIONSHIPS"
    ) {
        return eval_list_function(&name_upper, args);
    }

    // Type conversion functions
    if matches!(
        name_upper.as_str(),
        "TOINTEGER" | "TOINT" | "TOFLOAT" | "TOSTRING" | "TOBOOLEAN" | "TOBOOL"
    ) {
        return eval_type_function(&name_upper, args);
    }

    // Math functions
    if matches!(
        name_upper.as_str(),
        "ABS"
            | "CEIL"
            | "FLOOR"
            | "ROUND"
            | "SQRT"
            | "SIGN"
            | "LOG"
            | "LOG10"
            | "EXP"
            | "POWER"
            | "POW"
            | "SIN"
            | "COS"
            | "TAN"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "ATAN2"
            | "DEGREES"
            | "RADIANS"
            | "HAVERSIN"
            | "PI"
            | "E"
            | "RAND"
    ) {
        return eval_math_function(&name_upper, args);
    }

    // String functions
    if matches!(
        name_upper.as_str(),
        "TOUPPER"
            | "UPPER"
            | "TOLOWER"
            | "LOWER"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "REVERSE"
            | "REPLACE"
            | "SPLIT"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
    ) {
        return eval_string_function(&name_upper, args);
    }

    // Date/Time functions
    if matches!(
        name_upper.as_str(),
        "DATE"
            | "TIME"
            | "DATETIME"
            | "LOCALDATETIME"
            | "LOCALTIME"
            | "DURATION"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            // Epoch functions
            | "DATETIME.FROMEPOCH"
            | "DATETIME.FROMEPOCHMILLIS"
            // Truncate functions
            | "DATE.TRUNCATE"
            | "TIME.TRUNCATE"
            | "DATETIME.TRUNCATE"
            | "LOCALDATETIME.TRUNCATE"
            | "LOCALTIME.TRUNCATE"
            // Transaction/statement/realtime functions
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
            | "LOCALDATETIME.REALTIME"
            // Duration between functions
            | "DURATION.BETWEEN"
            | "DURATION.INMONTHS"
            | "DURATION.INDAYS"
            | "DURATION.INSECONDS"
    ) {
        return eval_datetime_function(&name_upper, args);
    }

    // Spatial functions
    if matches!(
        name_upper.as_str(),
        "POINT" | "DISTANCE" | "POINT.WITHINBBOX"
    ) {
        return eval_spatial_function(&name_upper, args);
    }

    // Range function
    if name_upper == "RANGE" {
        return eval_range_function(args);
    }

    if name_upper == "UNI.TEMPORAL.VALIDAT" {
        return eval_valid_at(args);
    }

    if name_upper == "VECTOR_DISTANCE" {
        if args.len() < 2 || args.len() > 3 {
            return Err(anyhow!("vector_distance requires 2 or 3 arguments"));
        }
        let metric = if args.len() == 3 {
            args[2].as_str().ok_or(anyhow!("metric must be string"))?
        } else {
            "cosine"
        };
        return eval_vector_distance(&args[0], &args[1], metric);
    }

    // Bitwise functions (uni_bitwise_*)
    if matches!(
        name_upper.as_str(),
        "UNI_BITWISE_OR"
            | "UNI_BITWISE_AND"
            | "UNI_BITWISE_XOR"
            | "UNI_BITWISE_NOT"
            | "UNI_BITWISE_SHIFTLEFT"
            | "UNI_BITWISE_SHIFTRIGHT"
    ) {
        return eval_bitwise_function(&name_upper, args);
    }

    Err(anyhow!("Function {} not implemented or is aggregate", name))
}

/// Evaluate uni.temporal.validAt(node, start_prop, end_prop, time)
///
/// Checks if a node/edge was valid at a given point in time using half-open interval
/// semantics: `[valid_from, valid_to)` where `valid_from <= time < valid_to`.
///
/// If `valid_to` is NULL or missing, the interval is open-ended (valid indefinitely).
/// If `valid_from` is NULL or missing, the entity is considered invalid.
fn eval_valid_at(args: &[Value]) -> Result<Value> {
    if args.len() != 4 {
        return Err(anyhow!(
            "validAt requires 4 arguments: node, start_prop, end_prop, time"
        ));
    }

    let node_map = match &args[0] {
        Value::Object(map) => map,
        Value::Null => return Ok(Value::Bool(false)),
        _ => {
            return Err(anyhow!(
                "validAt expects a Node or Edge (Object) as first argument"
            ));
        }
    };

    let start_prop = args[1]
        .as_str()
        .ok_or_else(|| anyhow!("start_prop must be a string"))?;
    let end_prop = args[2]
        .as_str()
        .ok_or_else(|| anyhow!("end_prop must be a string"))?;

    let time_str = match &args[3] {
        Value::String(s) => s,
        _ => return Err(anyhow!("time argument must be a datetime string")),
    };

    let query_time = parse_datetime_utc(time_str)
        .map_err(|_| anyhow!("Invalid query time format: {}", time_str))?;

    let valid_from_val = node_map.get(start_prop);
    let valid_from = match valid_from_val {
        Some(Value::String(s)) => parse_datetime_utc(s)
            .map_err(|_| anyhow!("Invalid datetime in property {}: {}", start_prop, s))?,
        Some(Value::Null) | None => return Ok(Value::Bool(false)),
        _ => return Err(anyhow!("Property {} must be a datetime string", start_prop)),
    };

    let valid_to_val = node_map.get(end_prop);
    let valid_to = match valid_to_val {
        Some(Value::String(s)) => Some(
            parse_datetime_utc(s)
                .map_err(|_| anyhow!("Invalid datetime in property {}: {}", end_prop, s))?,
        ),
        Some(Value::Null) | None => None,
        _ => {
            return Err(anyhow!(
                "Property {} must be a datetime string or null",
                end_prop
            ));
        }
    };

    // Half-open interval: [valid_from, valid_to)
    let is_valid = valid_from <= query_time && valid_to.map(|vt| query_time < vt).unwrap_or(true);

    Ok(Value::Bool(is_valid))
}

/// Evaluate vector similarity between two vectors (cosine similarity).
pub fn eval_vector_similarity(v1: &Value, v2: &Value) -> Result<Value> {
    let (arr1, arr2) = match (v1, v2) {
        (Value::Array(a1), Value::Array(a2)) => (a1, a2),
        _ => return Err(anyhow!("vector_similarity arguments must be arrays")),
    };

    if arr1.len() != arr2.len() {
        return Err(anyhow!(
            "Vector dimensions mismatch: {} vs {}",
            arr1.len(),
            arr2.len()
        ));
    }

    let mut dot = 0.0;
    let mut norm1_sq = 0.0;
    let mut norm2_sq = 0.0;

    for (v1_elem, v2_elem) in arr1.iter().zip(arr2.iter()) {
        let f1 = v1_elem
            .as_f64()
            .ok_or_else(|| anyhow!("Vector element not a number"))?;
        let f2 = v2_elem
            .as_f64()
            .ok_or_else(|| anyhow!("Vector element not a number"))?;
        dot += f1 * f2;
        norm1_sq += f1 * f1;
        norm2_sq += f2 * f2;
    }

    let mag1 = norm1_sq.sqrt();
    let mag2 = norm2_sq.sqrt();

    let sim = if mag1 == 0.0 || mag2 == 0.0 {
        0.0
    } else {
        dot / (mag1 * mag2)
    };

    Ok(json!(sim))
}

/// Evaluate vector distance between two vectors.
pub fn eval_vector_distance(v1: &Value, v2: &Value, metric: &str) -> Result<Value> {
    let (arr1, arr2) = match (v1, v2) {
        (Value::Array(a1), Value::Array(a2)) => (a1, a2),
        _ => return Err(anyhow!("vector_distance arguments must be arrays")),
    };

    if arr1.len() != arr2.len() {
        return Err(anyhow!(
            "Vector dimensions mismatch: {} vs {}",
            arr1.len(),
            arr2.len()
        ));
    }

    // Helper to get f64 iterator
    let iter1 = arr1
        .iter()
        .map(|v| v.as_f64().ok_or(anyhow!("Vector element not a number")));
    let iter2 = arr2
        .iter()
        .map(|v| v.as_f64().ok_or(anyhow!("Vector element not a number")));

    match metric.to_lowercase().as_str() {
        "cosine" => {
            // Cosine distance = 1 - cosine similarity
            let mut dot = 0.0;
            let mut norm1_sq = 0.0;
            let mut norm2_sq = 0.0;

            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                dot += f1 * f2;
                norm1_sq += f1 * f1;
                norm2_sq += f2 * f2;
            }

            let mag1 = norm1_sq.sqrt();
            let mag2 = norm2_sq.sqrt();

            if mag1 == 0.0 || mag2 == 0.0 {
                Ok(json!(1.0))
            } else {
                let sim = dot / (mag1 * mag2);
                // Clamp to [-1, 1] to avoid numerical errors
                let sim = sim.clamp(-1.0, 1.0);
                Ok(json!(1.0 - sim))
            }
        }
        "euclidean" | "l2" => {
            let mut sum_sq_diff = 0.0;
            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                let diff = f1 - f2;
                sum_sq_diff += diff * diff;
            }
            Ok(json!(sum_sq_diff.sqrt()))
        }
        "dot" | "inner_product" => {
            let mut dot = 0.0;
            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                dot += f1 * f2;
            }
            Ok(json!(1.0 - dot))
        }
        _ => Err(anyhow!("Unknown metric: {}", metric)),
    }
}

/// Check if a function name is a known scalar function (not aggregate).
pub fn is_scalar_function(name: &str) -> bool {
    let name_upper = name.to_uppercase();
    matches!(
        name_upper.as_str(),
        "COALESCE"
            | "NULLIF"
            | "SIZE"
            | "KEYS"
            | "HEAD"
            | "TAIL"
            | "LAST"
            | "LENGTH"
            | "NODES"
            | "RELATIONSHIPS"
            | "TOINTEGER"
            | "TOINT"
            | "TOFLOAT"
            | "TOSTRING"
            | "TOBOOLEAN"
            | "TOBOOL"
            | "ABS"
            | "CEIL"
            | "FLOOR"
            | "ROUND"
            | "SQRT"
            | "SIGN"
            | "LOG"
            | "LOG10"
            | "EXP"
            | "POWER"
            | "POW"
            | "SIN"
            | "COS"
            | "TAN"
            | "ASIN"
            | "ACOS"
            | "ATAN"
            | "ATAN2"
            | "DEGREES"
            | "RADIANS"
            | "HAVERSIN"
            | "PI"
            | "E"
            | "RAND"
            | "TOUPPER"
            | "UPPER"
            | "TOLOWER"
            | "LOWER"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "REVERSE"
            | "REPLACE"
            | "SPLIT"
            | "SUBSTRING"
            | "LEFT"
            | "RIGHT"
            | "LPAD"
            | "RPAD"
            | "RANGE"
            | "UNI.VALIDAT"
            | "VALIDAT"
            | "VECTOR_SIMILARITY"
            | "VECTOR_DISTANCE"
            | "DATE"
            | "TIME"
            | "DATETIME"
            | "DURATION"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "ID"
            | "ELEMENTID"
            | "TYPE"
            | "LABELS"
            | "PROPERTIES"
            | "STARTNODE"
            | "ENDNODE"
            | "ANY"
            | "ALL"
            | "NONE"
            | "SINGLE"
    )
}

/// Evaluate bitwise functions (uni_bitwise_*)
fn eval_bitwise_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "UNI_BITWISE_OR" => {
            if args.len() != 2 {
                return Err(anyhow!("uni_bitwise_or requires exactly 2 arguments"));
            }
            let l = args[0]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_or requires integer arguments"))?;
            let r = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_or requires integer arguments"))?;
            Ok(json!(l | r))
        }

        "UNI_BITWISE_AND" => {
            if args.len() != 2 {
                return Err(anyhow!("uni_bitwise_and requires exactly 2 arguments"));
            }
            let l = args[0]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_and requires integer arguments"))?;
            let r = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_and requires integer arguments"))?;
            Ok(json!(l & r))
        }

        "UNI_BITWISE_XOR" => {
            if args.len() != 2 {
                return Err(anyhow!("uni_bitwise_xor requires exactly 2 arguments"));
            }
            let l = args[0]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_xor requires integer arguments"))?;
            let r = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_xor requires integer arguments"))?;
            Ok(json!(l ^ r))
        }

        "UNI_BITWISE_NOT" => {
            if args.len() != 1 {
                return Err(anyhow!("uni_bitwise_not requires exactly 1 argument"));
            }
            let val = args[0]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_not requires integer argument"))?;
            Ok(json!(!val))
        }

        "UNI_BITWISE_SHIFTLEFT" => {
            if args.len() != 2 {
                return Err(anyhow!(
                    "uni_bitwise_shiftLeft requires exactly 2 arguments"
                ));
            }
            let val = args[0]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_shiftLeft requires integer arguments"))?;
            let n = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_shiftLeft requires integer arguments"))?;
            Ok(json!(val << n))
        }

        "UNI_BITWISE_SHIFTRIGHT" => {
            if args.len() != 2 {
                return Err(anyhow!(
                    "uni_bitwise_shiftRight requires exactly 2 arguments"
                ));
            }
            let val = args[0]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_shiftRight requires integer arguments"))?;
            let n = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("uni_bitwise_shiftRight requires integer arguments"))?;
            Ok(json!(val >> n))
        }

        _ => Err(anyhow!("Unknown bitwise function: {}", name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_binary_op_eq() {
        assert_eq!(
            eval_binary_op(&json!(1), &BinaryOp::Eq, &json!(1)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!(1), &BinaryOp::Eq, &json!(2)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_comparison() {
        assert_eq!(
            eval_binary_op(&json!(5), &BinaryOp::Gt, &json!(3)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!(5), &BinaryOp::Lt, &json!(3)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_xor() {
        // true XOR true = false
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Xor, &Value::Bool(true)).unwrap(),
            Value::Bool(false)
        );
        // true XOR false = true
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Xor, &Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
        // false XOR true = true
        assert_eq!(
            eval_binary_op(&Value::Bool(false), &BinaryOp::Xor, &Value::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        // false XOR false = false
        assert_eq!(
            eval_binary_op(&Value::Bool(false), &BinaryOp::Xor, &Value::Bool(false)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_contains() {
        assert_eq!(
            eval_binary_op(&json!("hello world"), &BinaryOp::Contains, &json!("world")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_scalar_function_size() {
        assert_eq!(
            eval_scalar_function("SIZE", &[json!([1, 2, 3])]).unwrap(),
            json!(3)
        );
    }

    #[test]
    fn test_scalar_function_head() {
        assert_eq!(
            eval_scalar_function("HEAD", &[json!([1, 2, 3])]).unwrap(),
            json!(1)
        );
    }

    #[test]
    fn test_scalar_function_coalesce() {
        assert_eq!(
            eval_scalar_function("COALESCE", &[Value::Null, json!(1), json!(2)]).unwrap(),
            json!(1)
        );
    }

    #[test]
    fn test_vector_similarity() {
        let v1 = json!([1.0, 0.0]);
        let v2 = json!([1.0, 0.0]);
        let result = eval_vector_similarity(&v1, &v2).unwrap();
        assert_eq!(result.as_f64().unwrap(), 1.0);
    }

    #[test]
    fn test_regex_match() {
        // Basic regex match
        assert_eq!(
            eval_binary_op(&json!("hello world"), &BinaryOp::Regex, &json!("hello.*")).unwrap(),
            Value::Bool(true)
        );

        // No match
        assert_eq!(
            eval_binary_op(&json!("hello world"), &BinaryOp::Regex, &json!("^world")).unwrap(),
            Value::Bool(false)
        );

        // Case sensitive
        assert_eq!(
            eval_binary_op(&json!("Hello"), &BinaryOp::Regex, &json!("hello")).unwrap(),
            Value::Bool(false)
        );

        // Case insensitive with flag
        assert_eq!(
            eval_binary_op(&json!("Hello"), &BinaryOp::Regex, &json!("(?i)hello")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_regex_null_handling() {
        // Left operand is null
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Regex, &json!(".*")).unwrap(),
            Value::Null
        );

        // Right operand is null
        assert_eq!(
            eval_binary_op(&json!("hello"), &BinaryOp::Regex, &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_regex_invalid_pattern() {
        // Invalid regex pattern should return error
        let result = eval_binary_op(&json!("hello"), &BinaryOp::Regex, &json!("[invalid"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid regex"));
    }

    #[test]
    fn test_regex_special_characters() {
        // Email pattern with escaped dots
        assert_eq!(
            eval_binary_op(
                &json!("test@example.com"),
                &BinaryOp::Regex,
                &json!(r"^[\w.-]+@[\w.-]+\.\w+$")
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Phone number pattern
        assert_eq!(
            eval_binary_op(
                &json!("123-456-7890"),
                &BinaryOp::Regex,
                &json!(r"^\d{3}-\d{3}-\d{4}$")
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Non-matching phone
        assert_eq!(
            eval_binary_op(
                &json!("1234567890"),
                &BinaryOp::Regex,
                &json!(r"^\d{3}-\d{3}-\d{4}$")
            )
            .unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_regex_anchors() {
        // Start anchor
        assert_eq!(
            eval_binary_op(&json!("hello world"), &BinaryOp::Regex, &json!("^hello")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!("say hello"), &BinaryOp::Regex, &json!("^hello")).unwrap(),
            Value::Bool(false)
        );

        // End anchor
        assert_eq!(
            eval_binary_op(&json!("hello world"), &BinaryOp::Regex, &json!("world$")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!("world hello"), &BinaryOp::Regex, &json!("world$")).unwrap(),
            Value::Bool(false)
        );

        // Full match with both anchors
        assert_eq!(
            eval_binary_op(&json!("hello"), &BinaryOp::Regex, &json!("^hello$")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&json!("hello world"), &BinaryOp::Regex, &json!("^hello$")).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_temporal_arithmetic() {
        // datetime + duration (1 hour)
        let dt = json!("2024-01-15T10:00:00Z");
        let dur = json!(3_600_000_000_i64);
        let result = eval_binary_op(&dt, &BinaryOp::Add, &dur).unwrap();
        assert!(result.as_str().unwrap().contains("11:00:00"));

        // date + duration (1 day)
        let d = json!("2024-01-01");
        let dur_day = json!(86_400_000_000_i64);
        let result = eval_binary_op(&d, &BinaryOp::Add, &dur_day).unwrap();
        assert_eq!(result.as_str().unwrap(), "2024-01-02");

        // datetime - datetime (returns ISO 8601 duration)
        let dt1 = json!("2024-01-02T00:00:00Z");
        let dt2 = json!("2024-01-01T00:00:00Z");
        let result = eval_binary_op(&dt1, &BinaryOp::Sub, &dt2).unwrap();
        // Result is now ISO 8601 duration string (1 day = P1D)
        let dur_str = result.as_str().unwrap();
        assert!(dur_str.starts_with('P'));
        assert!(dur_str.contains("1D")); // 1 day
    }

    // Bitwise operator tests removed - bitwise operations now use functions (uni_bitwise_*)
    // See bitwise_functions_test.rs for comprehensive bitwise function tests

    #[test]
    fn test_temporal_arithmetic_edge_cases() {
        // Negative duration (subtracting time)
        let dt = json!("2024-01-15T10:00:00Z");
        let neg_dur = json!(-3_600_000_000_i64); // -1 hour
        let result = eval_binary_op(&dt, &BinaryOp::Add, &neg_dur).unwrap();
        assert!(result.as_str().unwrap().contains("09:00:00"));

        // Duration subtraction resulting in negative duration
        let dur1 = json!("PT1H"); // 1 hour as ISO 8601
        let dur2 = json!("PT2H"); // 2 hours as ISO 8601
        let result = eval_binary_op(&dur1, &BinaryOp::Sub, &dur2).unwrap();
        // Result is ISO 8601 duration string (negative 1 hour)
        // Note: chrono Duration doesn't support negative durations well in ISO 8601 format
        // but the result should be a valid duration string
        assert!(result.as_str().is_some());

        // Zero duration addition
        let dt = json!("2024-01-15T10:00:00Z");
        let zero_dur = json!(0_i64);
        let result = eval_binary_op(&dt, &BinaryOp::Add, &zero_dur).unwrap();
        assert!(result.as_str().unwrap().contains("10:00:00"));

        // Date crossing year boundary
        let d = json!("2023-12-31");
        let one_day = json!(86_400_000_000_i64);
        let result = eval_binary_op(&d, &BinaryOp::Add, &one_day).unwrap();
        assert_eq!(result.as_str().unwrap(), "2024-01-01");

        // Same datetime subtraction yields zero duration
        let dt1 = json!("2024-01-15T10:00:00Z");
        let dt2 = json!("2024-01-15T10:00:00Z");
        let result = eval_binary_op(&dt1, &BinaryOp::Sub, &dt2).unwrap();
        // Zero duration should be "PT0S" or similar
        let dur_str = result.as_str().unwrap();
        assert!(dur_str.starts_with('P'));

        // Leap year handling
        let leap_day = json!("2024-02-28");
        let one_day = json!(86_400_000_000_i64);
        let result = eval_binary_op(&leap_day, &BinaryOp::Add, &one_day).unwrap();
        assert_eq!(result.as_str().unwrap(), "2024-02-29");
    }

    #[test]
    fn test_regex_empty_string() {
        // Empty string matches empty pattern
        assert_eq!(
            eval_binary_op(&json!(""), &BinaryOp::Regex, &json!("^$")).unwrap(),
            Value::Bool(true)
        );

        // Empty string doesn't match non-empty pattern
        assert_eq!(
            eval_binary_op(&json!(""), &BinaryOp::Regex, &json!(".+")).unwrap(),
            Value::Bool(false)
        );

        // Non-empty string matches .* (matches anything including empty)
        assert_eq!(
            eval_binary_op(&json!("hello"), &BinaryOp::Regex, &json!(".*")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_regex_type_errors() {
        // Non-string left operand
        let result = eval_binary_op(&json!(123), &BinaryOp::Regex, &json!("\\d+"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be a string"));

        // Non-string right operand (pattern)
        let result = eval_binary_op(&json!("hello"), &BinaryOp::Regex, &json!(123));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("pattern string"));
    }
}
