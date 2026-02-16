// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Expression evaluation helper functions.
//!
//! This module extracts high-complexity expression evaluation logic from the main executor
//! to reduce cognitive complexity and improve maintainability.

use anyhow::{Result, anyhow};
use std::cmp::Ordering;
use uni_common::Value;

use uni_common::TemporalValue;

use crate::query::datetime::{
    CypherDuration, TemporalType, add_cypher_duration_to_date, add_cypher_duration_to_datetime,
    add_cypher_duration_to_localdatetime, add_cypher_duration_to_localtime,
    add_cypher_duration_to_time, classify_temporal, eval_datetime_function, is_duration_value,
    parse_datetime_utc, parse_duration_from_value, parse_duration_to_cypher,
};
use crate::query::spatial::eval_spatial_function;
use uni_cypher::ast::BinaryOp;

/// Evaluate a binary operation on two already-evaluated values.
///
/// This function handles all binary operators (Eq, NotEq, And, Or, Gt, Lt, etc.)
/// and returns the result of the operation.
pub fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
    // Null propagation for most operators (except AND/OR which have three-valued logic)
    if !matches!(op, BinaryOp::And | BinaryOp::Or) && (left.is_null() || right.is_null()) {
        return Ok(Value::Null);
    }

    match op {
        BinaryOp::Eq => Ok(match cypher_eq(left, right) {
            Some(b) => Value::Bool(b),
            None => Value::Null,
        }),
        BinaryOp::NotEq => Ok(match cypher_eq(left, right) {
            Some(b) => Value::Bool(!b),
            None => Value::Null,
        }),
        BinaryOp::And => {
            // Three-valued logic: false dominates, null propagates with true
            match (left.as_bool(), right.as_bool()) {
                (Some(false), _) | (_, Some(false)) => Ok(Value::Bool(false)),
                (Some(true), Some(true)) => Ok(Value::Bool(true)),
                _ if left.is_null() || right.is_null() => Ok(Value::Null),
                _ => Err(anyhow!(
                    "InvalidArgumentType: Expected bool for AND operands"
                )),
            }
        }
        BinaryOp::Or => {
            // Three-valued logic: true dominates, null propagates with false
            match (left.as_bool(), right.as_bool()) {
                (Some(true), _) | (_, Some(true)) => Ok(Value::Bool(true)),
                (Some(false), Some(false)) => Ok(Value::Bool(false)),
                _ if left.is_null() || right.is_null() => Ok(Value::Null),
                _ => Err(anyhow!(
                    "InvalidArgumentType: Expected bool for OR operands"
                )),
            }
        }
        BinaryOp::Xor => {
            // Three-valued logic: any null operand returns null
            match (left.as_bool(), right.as_bool()) {
                (Some(l), Some(r)) => Ok(Value::Bool(l ^ r)),
                _ if left.is_null() || right.is_null() => Ok(Value::Null),
                _ => Err(anyhow!(
                    "InvalidArgumentType: Expected bool for XOR operands"
                )),
            }
        }
        BinaryOp::Gt => eval_comparison(left, right, |ordering| ordering.is_gt()),
        BinaryOp::Lt => eval_comparison(left, right, |ordering| ordering.is_lt()),
        BinaryOp::GtEq => eval_comparison(left, right, |ordering| ordering.is_ge()),
        BinaryOp::LtEq => eval_comparison(left, right, |ordering| ordering.is_le()),
        BinaryOp::Contains => eval_string_predicate(left, right, "CONTAINS", |l, r| l.contains(r)),
        BinaryOp::StartsWith => {
            eval_string_predicate(left, right, "STARTS WITH", |l, r| l.starts_with(r))
        }
        BinaryOp::EndsWith => {
            eval_string_predicate(left, right, "ENDS WITH", |l, r| l.ends_with(r))
        }
        BinaryOp::Add => eval_add(left, right),
        BinaryOp::Sub => eval_sub(left, right),
        BinaryOp::Mul => eval_mul(left, right),
        BinaryOp::Div => eval_div(left, right),
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

/// Deep equality comparison with Cypher-compliant numeric coercion and 3-valued logic.
/// Returns Some(bool) for True/False, and None for Null/Unknown.
pub fn cypher_eq(left: &Value, right: &Value) -> Option<bool> {
    if left.is_null() || right.is_null() {
        return None;
    }

    // Exact integer equality — avoid f64 precision loss for large i64 values
    if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
        return Some(l == r);
    }

    // Mixed numeric equality (1 = 1.0)
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        if l.is_nan() || r.is_nan() {
            return Some(false);
        }
        return Some(l == r);
    }

    // Structural equality for Lists
    if let (Value::List(l), Value::List(r)) = (left, right) {
        if l.len() != r.len() {
            return Some(false);
        }
        let mut has_null = false;
        for (lv, rv) in l.iter().zip(r.iter()) {
            match cypher_eq(lv, rv) {
                Some(false) => return Some(false),
                None => has_null = true,
                Some(true) => {}
            }
        }
        return if has_null { None } else { Some(true) };
    }

    // Structural equality for Maps
    if let (Value::Map(l), Value::Map(r)) = (left, right) {
        // If both are nodes (have _vid), compare by _vid ONLY
        if let (Some(vid_l), Some(vid_r)) = (l.get("_vid"), r.get("_vid")) {
            return Some(vid_l == vid_r);
        }
        // If both are edges (have _eid), compare by _eid ONLY
        if let (Some(eid_l), Some(eid_r)) = (l.get("_eid"), r.get("_eid")) {
            return Some(eid_l == eid_r);
        }

        if l.len() != r.len() {
            return Some(false);
        }

        let mut has_null = false;
        for (k, lv) in l {
            if let Some(rv) = r.get(k) {
                match cypher_eq(lv, rv) {
                    Some(false) => return Some(false),
                    None => has_null = true,
                    Some(true) => {}
                }
            } else {
                return Some(false);
            }
        }
        return if has_null { None } else { Some(true) };
    }

    // Fallback to standard equality for other types (String, Bool)
    Some(left == right)
}

/// Evaluate IN operator.
pub fn eval_in_op(left: &Value, right: &Value) -> Result<Value> {
    if let Value::List(arr) = right {
        let mut has_null = false;
        // Check exact match using cypher_eq (handles numeric coercion and node identity)
        for item in arr {
            match cypher_eq(left, item) {
                Some(true) => return Ok(Value::Bool(true)),
                None => has_null = true,
                _ => {}
            }
        }

        // Fallback: Check for Node Object vs VID mismatch
        if let Value::Map(map) = left
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
                // If item is Int (raw VID)
                if let Value::Int(n) = item
                    && let Some(vid_u64) = vid_val.as_u64()
                {
                    let n_u64 = *n as u64;
                    if vid_u64 == n_u64 {
                        return Ok(Value::Bool(true));
                    }
                }

                if item.is_null() {
                    has_null = true;
                }
            }
        }

        if has_null {
            Ok(Value::Null)
        } else {
            Ok(Value::Bool(false))
        }
    } else {
        Err(anyhow!("Right side of IN must be a list"))
    }
}

fn eval_string_predicate(
    left: &Value,
    right: &Value,
    op_name: &str,
    check: fn(&str, &str) -> bool,
) -> Result<Value> {
    let l = left
        .as_str()
        .ok_or_else(|| anyhow!("Left side of {} must be a string", op_name))?;
    let r = right
        .as_str()
        .ok_or_else(|| anyhow!("Right side of {} must be a string", op_name))?;
    Ok(Value::Bool(check(l, r)))
}

fn eval_numeric_op<F>(left: &Value, right: &Value, op: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    // Cypher null propagation: null op anything = null
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    let (l, r) = match (left.as_f64(), right.as_f64()) {
        (Some(l), Some(r)) => (l, r),
        _ => return Err(anyhow!("Arithmetic operation requires numbers")),
    };
    let result = op(l, r);
    // Return integer if result has no fractional part and both inputs were integers
    if !result.is_nan()
        && !result.is_infinite()
        && result.fract() == 0.0
        && left.is_i64()
        && right.is_i64()
    {
        Ok(Value::Int(result as i64))
    } else {
        Ok(Value::Float(result))
    }
}

// ============================================================================
// Temporal-aware arithmetic operations
// ============================================================================

/// Add a duration to a temporal value, dispatching by temporal type.
/// Accepts both Value::Temporal and Value::String temporal values.
fn add_temporal_duration_to_value(val: &Value, dur: &CypherDuration) -> Result<Value> {
    match val {
        Value::Temporal(tv) => add_temporal_duration_typed(tv, dur),
        Value::String(s) => {
            let ttype = classify_temporal(s)
                .ok_or_else(|| anyhow!("Cannot classify temporal value: {}", s))?;
            let result_str = match ttype {
                TemporalType::Date => add_cypher_duration_to_date(s, dur)?,
                TemporalType::LocalTime => add_cypher_duration_to_localtime(s, dur)?,
                TemporalType::Time => add_cypher_duration_to_time(s, dur)?,
                TemporalType::LocalDateTime => add_cypher_duration_to_localdatetime(s, dur)?,
                TemporalType::DateTime => add_cypher_duration_to_datetime(s, dur)?,
                TemporalType::Duration => return Err(anyhow!("Cannot add duration to duration this way")),
            };
            Ok(Value::String(result_str))
        }
        _ => Err(anyhow!("Expected temporal value for duration arithmetic")),
    }
}

/// Add a CypherDuration to a typed TemporalValue, returning a new Value::Temporal.
fn add_temporal_duration_typed(tv: &TemporalValue, dur: &CypherDuration) -> Result<Value> {
    // Convert to string, perform the operation, and re-parse the result.
    // This reuses the existing well-tested string-based arithmetic.
    let s = tv.to_string();
    let ttype = tv.temporal_type();
    let result_str = match ttype {
        TemporalType::Date => add_cypher_duration_to_date(&s, dur)?,
        TemporalType::LocalTime => add_cypher_duration_to_localtime(&s, dur)?,
        TemporalType::Time => add_cypher_duration_to_time(&s, dur)?,
        TemporalType::LocalDateTime => add_cypher_duration_to_localdatetime(&s, dur)?,
        TemporalType::DateTime => add_cypher_duration_to_datetime(&s, dur)?,
        TemporalType::Duration => return Err(anyhow!("Cannot add duration to duration this way")),
    };
    // Re-parse through the datetime constructor to get a Value::Temporal
    let args = [Value::String(result_str)];
    match ttype {
        TemporalType::Date => eval_datetime_function("DATE", &args),
        TemporalType::LocalTime => eval_datetime_function("LOCALTIME", &args),
        TemporalType::Time => eval_datetime_function("TIME", &args),
        TemporalType::LocalDateTime => eval_datetime_function("LOCALDATETIME", &args),
        TemporalType::DateTime => eval_datetime_function("DATETIME", &args),
        TemporalType::Duration => unreachable!(),
    }
}

/// Evaluate addition with temporal-aware dispatch.
fn eval_add(left: &Value, right: &Value) -> Result<Value> {
    // Null propagation
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // List concatenation: list + list, list + scalar, scalar + list
    match (left, right) {
        (Value::List(l), Value::List(r)) => {
            let mut result = l.clone();
            result.extend(r.iter().cloned());
            return Ok(Value::List(result));
        }
        (Value::List(l), _) => {
            let mut result = l.clone();
            result.push(right.clone());
            return Ok(Value::List(result));
        }
        (_, Value::List(r)) => {
            let mut result = vec![left.clone()];
            result.extend(r.iter().cloned());
            return Ok(Value::List(result));
        }
        _ => {}
    }

    // Numeric addition
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        if left.is_i64() && right.is_i64() {
            return Ok(Value::Int(left.as_i64().unwrap() + right.as_i64().unwrap()));
        }
        return Ok(Value::Float(l + r));
    }

    // Temporal + Duration (Value::Temporal)
    if let Value::Temporal(tv) = left {
        if let Value::Temporal(TemporalValue::Duration { months, days, nanos }) = right {
            let dur = CypherDuration::new(*months, *days, *nanos);
            return add_temporal_duration_typed(tv, &dur);
        }
        if is_duration_value(right) {
            let dur = parse_duration_from_value(right)?;
            return add_temporal_duration_typed(tv, &dur);
        }
        if right.is_number() && !matches!(tv, TemporalValue::Duration { .. }) {
            let dur = parse_duration_from_value(right)?;
            return add_temporal_duration_typed(tv, &dur);
        }
    }
    // Duration + Temporal (Value::Temporal)
    if let Value::Temporal(tv) = right {
        if let Value::Temporal(TemporalValue::Duration { months, days, nanos }) = left {
            let dur = CypherDuration::new(*months, *days, *nanos);
            return add_temporal_duration_typed(tv, &dur);
        }
        if is_duration_value(left) {
            let dur = parse_duration_from_value(left)?;
            return add_temporal_duration_typed(tv, &dur);
        }
        if left.is_number() && !matches!(tv, TemporalValue::Duration { .. }) {
            let dur = parse_duration_from_value(left)?;
            return add_temporal_duration_typed(tv, &dur);
        }
    }
    // Duration + Duration (Value::Temporal)
    if let (Value::Temporal(TemporalValue::Duration { months: m1, days: d1, nanos: n1 }),
            Value::Temporal(TemporalValue::Duration { months: m2, days: d2, nanos: n2 })) = (left, right) {
        return Ok(Value::Temporal(TemporalValue::Duration {
            months: m1 + m2,
            days: d1 + d2,
            nanos: n1 + n2,
        }));
    }

    // String concatenation (with temporal awareness for backward compat)
    if let (Value::String(l), Value::String(r)) = (left, right) {
        let l_type = classify_temporal(l);
        let r_type = classify_temporal(r);

        match (l_type, r_type) {
            // temporal + duration
            (Some(lt), Some(TemporalType::Duration)) if lt != TemporalType::Duration => {
                let dur = parse_duration_to_cypher(r)?;
                return add_temporal_duration_to_value(left, &dur);
            }
            // duration + temporal
            (Some(TemporalType::Duration), Some(rt)) if rt != TemporalType::Duration => {
                let dur = parse_duration_to_cypher(l)?;
                return add_temporal_duration_to_value(right, &dur);
            }
            // duration + duration (component-wise)
            (Some(TemporalType::Duration), Some(TemporalType::Duration)) => {
                let d1 = parse_duration_to_cypher(l)?;
                let d2 = parse_duration_to_cypher(r)?;
                return Ok(Value::String(d1.add(&d2).to_iso8601()));
            }
            // Not temporal: string concatenation
            _ => return Ok(Value::String(format!("{}{}", l, r))),
        }
    }

    // temporal string + integer microseconds
    if let Value::String(_) = left
        && right.is_number()
        && !matches!(classify_value_temporal(left), Some(TemporalType::Duration))
        && classify_value_temporal(left).is_some()
    {
        let dur = parse_duration_from_value(right)?;
        return add_temporal_duration_to_value(left, &dur);
    }
    // integer microseconds + temporal string
    if let Value::String(_) = right
        && left.is_number()
        && !matches!(classify_value_temporal(right), Some(TemporalType::Duration))
        && classify_value_temporal(right).is_some()
    {
        let dur = parse_duration_from_value(left)?;
        return add_temporal_duration_to_value(right, &dur);
    }

    Err(anyhow!("Invalid types for addition"))
}

/// Classify a Value's temporal type (works for both Temporal and String).
fn classify_value_temporal(val: &Value) -> Option<TemporalType> {
    match val {
        Value::Temporal(tv) => Some(tv.temporal_type()),
        Value::String(s) => classify_temporal(s),
        _ => None,
    }
}

/// Evaluate subtraction with temporal-aware dispatch.
fn eval_sub(left: &Value, right: &Value) -> Result<Value> {
    // Null propagation
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // Temporal - Duration (Value::Temporal)
    if let Value::Temporal(tv) = left {
        if let Value::Temporal(TemporalValue::Duration { months, days, nanos }) = right {
            let dur = CypherDuration::new(-months, -days, -nanos);
            return add_temporal_duration_typed(tv, &dur);
        }
        if is_duration_value(right) {
            let dur = parse_duration_from_value(right)?.negate();
            return add_temporal_duration_typed(tv, &dur);
        }
        if right.is_number() && !matches!(tv, TemporalValue::Duration { .. }) {
            let dur = parse_duration_from_value(right)?.negate();
            return add_temporal_duration_typed(tv, &dur);
        }
    }
    // Duration - Duration (Value::Temporal)
    if let (Value::Temporal(TemporalValue::Duration { months: m1, days: d1, nanos: n1 }),
            Value::Temporal(TemporalValue::Duration { months: m2, days: d2, nanos: n2 })) = (left, right) {
        return Ok(Value::Temporal(TemporalValue::Duration {
            months: m1 - m2,
            days: d1 - d2,
            nanos: n1 - n2,
        }));
    }
    // Same temporal type - temporal difference
    if let (Value::Temporal(l), Value::Temporal(r)) = (left, right)
        && l.temporal_type() == r.temporal_type()
        && l.temporal_type() != TemporalType::Duration
    {
        let args = [left.clone(), right.clone()];
        return crate::query::datetime::eval_datetime_function("DURATION.BETWEEN", &args);
    }

    // String temporal - duration (backward compat)
    if let (Value::String(l), Value::String(r)) = (left, right) {
        let l_type = classify_temporal(l);
        let r_type = classify_temporal(r);

        match (l_type, r_type) {
            (Some(lt), Some(TemporalType::Duration)) if lt != TemporalType::Duration => {
                let dur = parse_duration_to_cypher(r)?.negate();
                return add_temporal_duration_to_value(left, &dur);
            }
            (Some(TemporalType::Duration), Some(TemporalType::Duration)) => {
                let d1 = parse_duration_to_cypher(l)?;
                let d2 = parse_duration_to_cypher(r)?;
                return Ok(Value::String(d1.sub(&d2).to_iso8601()));
            }
            (Some(lt), Some(rt))
                if lt != TemporalType::Duration && rt != TemporalType::Duration && lt == rt =>
            {
                let args = [left.clone(), right.clone()];
                return crate::query::datetime::eval_datetime_function("DURATION.BETWEEN", &args);
            }
            _ => {}
        }
    }

    // temporal string - integer microseconds
    if let Value::String(_) = left
        && right.is_number()
        && classify_value_temporal(left).is_some_and(|t| t != TemporalType::Duration)
    {
        let dur = parse_duration_from_value(right)?.negate();
        return add_temporal_duration_to_value(left, &dur);
    }

    eval_numeric_op(left, right, |a, b| a - b)
}

/// Evaluate multiplication with duration support.
fn eval_mul(left: &Value, right: &Value) -> Result<Value> {
    // Null propagation
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // Value::Temporal duration * number
    if let Value::Temporal(TemporalValue::Duration { months, days, nanos }) = left
        && let Some(factor) = right.as_f64()
    {
        let dur = CypherDuration::new(*months, *days, *nanos).multiply(factor);
        return Ok(Value::Temporal(TemporalValue::Duration {
            months: dur.months,
            days: dur.days,
            nanos: dur.nanos,
        }));
    }
    // number * Value::Temporal duration
    if let Value::Temporal(TemporalValue::Duration { months, days, nanos }) = right
        && let Some(factor) = left.as_f64()
    {
        let dur = CypherDuration::new(*months, *days, *nanos).multiply(factor);
        return Ok(Value::Temporal(TemporalValue::Duration {
            months: dur.months,
            days: dur.days,
            nanos: dur.nanos,
        }));
    }

    // String duration * number (backward compat)
    if let (Value::String(s), Some(factor)) = (left, right.as_f64())
        && is_duration_value(left)
    {
        let dur = parse_duration_to_cypher(s)?;
        return Ok(Value::String(dur.multiply(factor).to_iso8601()));
    }
    // number * string duration
    if let (Some(factor), Value::String(s)) = (left.as_f64(), right)
        && is_duration_value(right)
    {
        let dur = parse_duration_to_cypher(s)?;
        return Ok(Value::String(dur.multiply(factor).to_iso8601()));
    }

    eval_numeric_op(left, right, |a, b| a * b)
}

/// Evaluate division with duration support.
fn eval_div(left: &Value, right: &Value) -> Result<Value> {
    // Null propagation
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // Value::Temporal duration / number
    if let Value::Temporal(TemporalValue::Duration { months, days, nanos }) = left
        && let Some(divisor) = right.as_f64()
    {
        let dur = CypherDuration::new(*months, *days, *nanos).divide(divisor);
        return Ok(Value::Temporal(TemporalValue::Duration {
            months: dur.months,
            days: dur.days,
            nanos: dur.nanos,
        }));
    }

    // String duration / number (backward compat)
    if let (Value::String(s), Some(divisor)) = (left, right.as_f64())
        && is_duration_value(left)
    {
        let dur = parse_duration_to_cypher(s)?;
        return Ok(Value::String(dur.divide(divisor).to_iso8601()));
    }

    eval_numeric_op(left, right, |a, b| a / b)
}

/// Helper for comparisons between two values with temporal awareness and structural support.
///
/// Per Cypher semantics:
/// - NULL compared with anything returns NULL
/// - Incompatible types (e.g., string vs int) return NULL, not an error
fn eval_comparison<F>(left: &Value, right: &Value, check: F) -> Result<Value>
where
    F: Fn(Ordering) -> bool,
{
    // Handle NULL inputs - any comparison with NULL returns NULL
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // Handle NaN - NaN vs number returns false, NaN vs non-number returns null (cross-type)
    let left_nan = left.as_f64().is_some_and(|f| f.is_nan());
    let right_nan = right.as_f64().is_some_and(|f| f.is_nan());
    if left_nan || right_nan {
        if left_nan && right_nan {
            return Ok(Value::Bool(false));
        }
        let other = if left_nan { right } else { left };
        if other.as_f64().is_some() {
            return Ok(Value::Bool(false)); // NaN vs number
        }
        return Ok(Value::Null); // NaN vs non-number (cross-type)
    }

    let ord = cypher_partial_cmp(left, right);
    match ord {
        Some(o) => Ok(Value::Bool(check(o))),
        None => Ok(Value::Null),
    }
}

/// Deep partial comparison with Cypher-compliant numeric coercion and structural support.
fn cypher_partial_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    if left.is_null() || right.is_null() {
        return None;
    }

    // Exact integer ordering — avoid f64 precision loss for large i64 values
    if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
        return Some(l.cmp(&r));
    }

    // Number vs Number
    if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
        return l.partial_cmp(&r);
    }

    // Temporal vs Temporal — direct numeric comparison, no parsing
    if let (Value::Temporal(l), Value::Temporal(r)) = (left, right) {
        return temporal_partial_cmp(l, r);
    }

    // Temporal vs String (cross-type) — compare via string representation
    if let (Value::Temporal(_), Value::String(rs)) = (left, right) {
        let ls = left.to_string();
        if let (Some(lt), Some(rt)) = (classify_temporal(&ls), classify_temporal(rs))
            && lt == rt
        {
            return temporal_string_cmp(&ls, rs, lt);
        }
        return None;
    }
    if let (Value::String(ls), Value::Temporal(_)) = (left, right) {
        let rs = right.to_string();
        if let (Some(lt), Some(rt)) = (classify_temporal(ls), classify_temporal(&rs))
            && lt == rt
        {
            return temporal_string_cmp(ls, &rs, lt);
        }
        return None;
    }

    // String vs String
    if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
        // Temporal-aware comparison
        if let (Some(lt), Some(rt)) = (classify_temporal(l), classify_temporal(r))
            && lt == rt
        {
            let res = temporal_string_cmp(l, r, lt);
            if res.is_some() {
                return res;
            }
        }
        return l.partial_cmp(r);
    }

    // Boolean vs Boolean
    if let (Some(l), Some(r)) = (left.as_bool(), right.as_bool()) {
        return l.partial_cmp(&r);
    }

    // Array vs Array (Lexicographic)
    if let (Value::List(l), Value::List(r)) = (left, right) {
        for (lv, rv) in l.iter().zip(r.iter()) {
            match cypher_partial_cmp(lv, rv) {
                Some(Ordering::Equal) => continue,
                other => return other,
            }
        }
        return l.len().partial_cmp(&r.len());
    }

    // Maps are not orderable in Cypher, only comparable for equality
    None
}

/// Compare two TemporalValues directly using numeric representation.
fn temporal_partial_cmp(left: &TemporalValue, right: &TemporalValue) -> Option<Ordering> {
    match (left, right) {
        (TemporalValue::Date { days_since_epoch: l }, TemporalValue::Date { days_since_epoch: r }) => {
            Some(l.cmp(r))
        }
        (TemporalValue::LocalTime { nanos_since_midnight: l }, TemporalValue::LocalTime { nanos_since_midnight: r }) => {
            Some(l.cmp(r))
        }
        (TemporalValue::Time { nanos_since_midnight: lm, offset_seconds: lo },
         TemporalValue::Time { nanos_since_midnight: rm, offset_seconds: ro }) => {
            // Compare in UTC: local_nanos - offset
            let l_utc = *lm as i128 - (*lo as i128) * 1_000_000_000;
            let r_utc = *rm as i128 - (*ro as i128) * 1_000_000_000;
            Some(l_utc.cmp(&r_utc))
        }
        (TemporalValue::LocalDateTime { nanos_since_epoch: l }, TemporalValue::LocalDateTime { nanos_since_epoch: r }) => {
            Some(l.cmp(r))
        }
        (TemporalValue::DateTime { nanos_since_epoch: l, .. }, TemporalValue::DateTime { nanos_since_epoch: r, .. }) => {
            // Both are in UTC, so direct comparison
            Some(l.cmp(r))
        }
        // Durations are not orderable
        (TemporalValue::Duration { .. }, TemporalValue::Duration { .. }) => None,
        // Different temporal types are not comparable
        _ => None,
    }
}

/// Compare two temporal strings of the same type.
fn temporal_string_cmp(l: &str, r: &str, ttype: TemporalType) -> Option<Ordering> {
    match ttype {
        TemporalType::Date => {
            let ld = chrono::NaiveDate::parse_from_str(l, "%Y-%m-%d").ok();
            let rd = chrono::NaiveDate::parse_from_str(r, "%Y-%m-%d").ok();
            ld.and_then(|l| rd.map(|r| l.cmp(&r)))
        }
        TemporalType::LocalTime => {
            let lt = parse_time_for_cmp(l).ok();
            let rt = parse_time_for_cmp(r).ok();
            lt.and_then(|l| rt.map(|r| l.cmp(&r)))
        }
        TemporalType::Time => {
            let ln = time_with_tz_to_utc_nanos(l).ok();
            let rn = time_with_tz_to_utc_nanos(r).ok();
            ln.and_then(|l| rn.map(|r| l.cmp(&r)))
        }
        TemporalType::LocalDateTime => {
            let ldt = parse_local_datetime_for_cmp(l).ok();
            let rdt = parse_local_datetime_for_cmp(r).ok();
            ldt.and_then(|l| rdt.map(|r| l.cmp(&r)))
        }
        TemporalType::DateTime => {
            let ldt = parse_datetime_utc(l).ok();
            let rdt = parse_datetime_utc(r).ok();
            ldt.and_then(|l| rdt.map(|r| l.cmp(&r)))
        }
        TemporalType::Duration => None, // Durations are not orderable
    }
}

/// Parse a time string for comparison.
fn parse_time_for_cmp(s: &str) -> Result<chrono::NaiveTime> {
    chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M:%S"))
        .or_else(|_| chrono::NaiveTime::parse_from_str(s, "%H:%M"))
        .map_err(|_| anyhow!("Cannot parse time: {}", s))
}

/// Parse a local datetime string for comparison.
fn parse_local_datetime_for_cmp(s: &str) -> Result<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M"))
        .map_err(|_| anyhow!("Cannot parse localdatetime: {}", s))
}

const NANOS_PER_SECOND_CMP: i64 = 1_000_000_000;

/// Normalize a time-with-timezone string to UTC nanoseconds for comparison.
fn time_with_tz_to_utc_nanos(s: &str) -> Result<i64> {
    use chrono::Timelike;
    let (_, time, tz_info) = crate::query::datetime::parse_datetime_with_tz(s)?;
    let local_nanos = time.hour() as i64 * 3_600 * NANOS_PER_SECOND_CMP
        + time.minute() as i64 * 60 * NANOS_PER_SECOND_CMP
        + time.second() as i64 * NANOS_PER_SECOND_CMP
        + time.nanosecond() as i64;

    // Subtract timezone offset to get UTC
    let offset_secs: i64 = match tz_info {
        Some(ref tz) => {
            let today = chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
            let ndt = chrono::NaiveDateTime::new(today, time);
            tz.offset_for_local(&ndt)?.local_minus_utc() as i64
        }
        None => 0,
    };

    Ok(local_nanos - offset_secs * NANOS_PER_SECOND_CMP)
}

// ============================================================================
// List/Collection function helpers
// ============================================================================

fn eval_size(arg: &Value) -> Result<Value> {
    match arg {
        Value::List(arr) => Ok(Value::Int(arr.len() as i64)),
        Value::Map(map) => Ok(Value::Int(map.len() as i64)),
        Value::String(s) => Ok(Value::Int(s.len() as i64)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("size() expects a List, Map, or String")),
    }
}

fn eval_keys(arg: &Value) -> Result<Value> {
    match arg {
        Value::Map(map) => {
            let mut keys: Vec<&String> = map.keys().filter(|k| !k.starts_with('_')).collect();
            keys.sort();
            Ok(Value::List(
                keys.into_iter().map(|k| Value::String(k.clone())).collect(),
            ))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("keys() expects a Map")),
    }
}

fn eval_head(arg: &Value) -> Result<Value> {
    match arg {
        Value::List(arr) => Ok(arr.first().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("head() expects a List")),
    }
}

fn eval_tail(arg: &Value) -> Result<Value> {
    match arg {
        Value::List(arr) => {
            if arr.is_empty() {
                Ok(Value::List(vec![]))
            } else {
                Ok(Value::List(arr[1..].to_vec()))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("tail() expects a List")),
    }
}

fn eval_last(arg: &Value) -> Result<Value> {
    match arg {
        Value::List(arr) => Ok(arr.last().cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("last() expects a List")),
    }
}

fn eval_length(arg: &Value) -> Result<Value> {
    match arg {
        Value::List(arr) => Ok(Value::Int(arr.len() as i64)),
        Value::String(s) => Ok(Value::Int(s.len() as i64)),
        Value::Path(p) => Ok(Value::Int(p.edges.len() as i64)),
        Value::Map(map) => {
            // Path object encoded as map (legacy fallback)
            if map.contains_key("nodes")
                && map.contains_key("relationships")
                && let Some(Value::List(rels)) = map.get("relationships")
            {
                return Ok(Value::Int(rels.len() as i64));
            }
            Ok(Value::Null)
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("length() expects a List, String, or Path")),
    }
}

fn eval_nodes(arg: &Value) -> Result<Value> {
    match arg {
        Value::Path(p) => Ok(Value::List(
            p.nodes.iter().map(|n| Value::Node(n.clone())).collect(),
        )),
        Value::Map(map) => {
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
        Value::Path(p) => Ok(Value::List(
            p.edges.iter().map(|e| Value::Edge(e.clone())).collect(),
        )),
        Value::Map(map) => {
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
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Int(*f as i64)),
        Value::String(s) => Ok(s.parse::<i64>().map(Value::Int).unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!(
            "InvalidArgumentValue: toInteger() cannot convert type"
        )),
    }
}

fn eval_tofloat(arg: &Value) -> Result<Value> {
    match arg {
        Value::Int(i) => Ok(Value::Float(*i as f64)),
        Value::Float(f) => Ok(Value::Float(*f)),
        Value::String(s) => Ok(s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!(
            "InvalidArgumentValue: toFloat() cannot convert type"
        )),
    }
}

fn eval_tostring(arg: &Value) -> Result<Value> {
    match arg {
        Value::String(s) => Ok(Value::String(s.clone())),
        Value::Int(i) => Ok(Value::String(i.to_string())),
        Value::Float(f) => {
            // Match Cypher convention: whole floats display with ".0"
            if f.fract() == 0.0 && f.is_finite() {
                Ok(Value::String(format!("{f:.1}")))
            } else {
                Ok(Value::String(f.to_string()))
            }
        }
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
        _ => Err(anyhow!(
            "InvalidArgumentValue: toBoolean() cannot convert type"
        )),
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
        Value::Int(i) => Ok(Value::Int(i.abs())),
        Value::Float(f) => Ok(Value::Float(f.abs())),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("abs() expects a number")),
    }
}

fn eval_ceil(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "ceil", f64::ceil)
}

fn eval_floor(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "floor", f64::floor)
}

fn eval_round(arg: &Value) -> Result<Value> {
    eval_unary_numeric_op(arg, "round", f64::round)
}

fn eval_sqrt(arg: &Value) -> Result<Value> {
    match arg {
        Value::Int(i) => {
            let f = *i as f64;
            if f < 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(f.sqrt()))
            }
        }
        Value::Float(f) => {
            if *f < 0.0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(f.sqrt()))
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sqrt() expects a number")),
    }
}

fn eval_sign(arg: &Value) -> Result<Value> {
    match arg {
        Value::Int(i) => {
            if *i > 0 {
                Ok(Value::Int(1))
            } else if *i < 0 {
                Ok(Value::Int(-1))
            } else {
                Ok(Value::Int(0))
            }
        }
        Value::Float(f) => {
            if *f > 0.0 {
                Ok(Value::Int(1))
            } else if *f < 0.0 {
                Ok(Value::Int(-1))
            } else {
                Ok(Value::Int(0))
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
        (a, b) if a.is_number() && b.is_number() => {
            let base = a.as_f64().unwrap();
            let exp = b.as_f64().unwrap();
            Ok(Value::Float(base.powf(exp)))
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
        Value::Int(i) => Ok(Value::Float(op(*i as f64))),
        Value::Float(f) => Ok(Value::Float(op(*f))),
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
        (a, b) if a.is_number() && b.is_number() => {
            let y_val = a.as_f64().unwrap();
            let x_val = b.as_f64().unwrap();
            Ok(Value::Float(y_val.atan2(x_val)))
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
fn require_one_arg<'a>(name: &str, args: &'a [Value]) -> Result<&'a Value> {
    if args.len() != 1 {
        return Err(anyhow!("{} requires 1 argument", name));
    }
    Ok(&args[0])
}

/// Evaluate math functions: ABS, CEIL, FLOOR, ROUND, SQRT, SIGN, LOG, LOG10, EXP, POWER, SIN, COS, TAN, etc.
fn eval_math_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        // Single-argument functions
        "ABS" => eval_abs(require_one_arg(name, args)?),
        "CEIL" => eval_ceil(require_one_arg(name, args)?),
        "FLOOR" => eval_floor(require_one_arg(name, args)?),
        "ROUND" => eval_round(require_one_arg(name, args)?),
        "SQRT" => eval_sqrt(require_one_arg(name, args)?),
        "SIGN" => eval_sign(require_one_arg(name, args)?),
        "LOG" => eval_log(require_one_arg(name, args)?),
        "LOG10" => eval_log10(require_one_arg(name, args)?),
        "EXP" => eval_exp(require_one_arg(name, args)?),
        "SIN" => eval_sin(require_one_arg(name, args)?),
        "COS" => eval_cos(require_one_arg(name, args)?),
        "TAN" => eval_tan(require_one_arg(name, args)?),
        "ASIN" => eval_asin(require_one_arg(name, args)?),
        "ACOS" => eval_acos(require_one_arg(name, args)?),
        "ATAN" => eval_atan(require_one_arg(name, args)?),
        "DEGREES" => eval_degrees(require_one_arg(name, args)?),
        "RADIANS" => eval_radians(require_one_arg(name, args)?),
        "HAVERSIN" => eval_haversin(require_one_arg(name, args)?),
        // Two-argument functions
        "POWER" | "POW" => eval_power(args),
        "ATAN2" => eval_atan2(args),
        // Zero-argument constants
        "PI" => {
            if !args.is_empty() {
                return Err(anyhow!("PI takes no arguments"));
            }
            Ok(Value::Float(std::f64::consts::PI))
        }
        "E" => {
            if !args.is_empty() {
                return Err(anyhow!("E takes no arguments"));
            }
            Ok(Value::Float(std::f64::consts::E))
        }
        "RAND" => {
            if !args.is_empty() {
                return Err(anyhow!("RAND takes no arguments"));
            }
            use rand::Rng;
            let mut rng = rand::thread_rng();
            Ok(Value::Float(rng.gen_range(0.0..1.0)))
        }
        _ => Err(anyhow!("Unknown math function: {}", name)),
    }
}

// ============================================================================
// String function helpers
// ============================================================================

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
        Value::List(arr) => Ok(Value::List(arr.iter().rev().cloned().collect())),
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
            Ok(Value::List(parts))
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
        (Value::String(s), n) if n.is_number() => {
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
        (Value::String(s), n) if n.is_number() => {
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
        Value::Int(n) => *n as usize,
        Value::Float(f) => *f as i64 as usize,
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
        Value::Int(n) => *n as usize,
        Value::Float(f) => *f as i64 as usize,
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
            result.push(Value::Int(i));
            i += step;
        }
    } else {
        while i >= end {
            result.push(Value::Int(i));
            i += step;
        }
    }
    Ok(Value::List(result))
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
        Value::Map(map) => map,
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
        (Value::List(a1), Value::List(a2)) => (a1, a2),
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

    Ok(Value::Float(sim))
}

/// Evaluate vector distance between two vectors.
pub fn eval_vector_distance(v1: &Value, v2: &Value, metric: &str) -> Result<Value> {
    let (arr1, arr2) = match (v1, v2) {
        (Value::List(a1), Value::List(a2)) => (a1, a2),
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
                Ok(Value::Float(1.0))
            } else {
                let sim = dot / (mag1 * mag2);
                // Clamp to [-1, 1] to avoid numerical errors
                let sim = sim.clamp(-1.0, 1.0);
                Ok(Value::Float(1.0 - sim))
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
            Ok(Value::Float(sum_sq_diff.sqrt()))
        }
        "dot" | "inner_product" => {
            let mut dot = 0.0;
            for (r1, r2) in iter1.zip(iter2) {
                let f1 = r1?;
                let f2 = r2?;
                dot += f1 * f2;
            }
            Ok(Value::Float(1.0 - dot))
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
    let require_int = |v: &Value, fname: &str| -> Result<i64> {
        v.as_i64()
            .ok_or_else(|| anyhow!("{} requires integer arguments", fname))
    };

    let bitwise_binary = |fname: &str, op: fn(i64, i64) -> i64| -> Result<Value> {
        if args.len() != 2 {
            return Err(anyhow!("{} requires exactly 2 arguments", fname));
        }
        let l = require_int(&args[0], fname)?;
        let r = require_int(&args[1], fname)?;
        Ok(Value::Int(op(l, r)))
    };

    match name {
        "UNI_BITWISE_OR" => bitwise_binary("uni_bitwise_or", |l, r| l | r),
        "UNI_BITWISE_AND" => bitwise_binary("uni_bitwise_and", |l, r| l & r),
        "UNI_BITWISE_XOR" => bitwise_binary("uni_bitwise_xor", |l, r| l ^ r),
        "UNI_BITWISE_SHIFTLEFT" => bitwise_binary("uni_bitwise_shiftLeft", |l, r| l << r),
        "UNI_BITWISE_SHIFTRIGHT" => bitwise_binary("uni_bitwise_shiftRight", |l, r| l >> r),
        "UNI_BITWISE_NOT" => {
            if args.len() != 1 {
                return Err(anyhow!("uni_bitwise_not requires exactly 1 argument"));
            }
            Ok(Value::Int(!require_int(&args[0], "uni_bitwise_not")?))
        }
        _ => Err(anyhow!("Unknown bitwise function: {}", name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Helper to create string values in tests (replaces s("..."))
    fn s(v: &str) -> Value {
        Value::String(v.into())
    }
    /// Helper to create int values in tests (replaces json!(i))
    fn i(v: i64) -> Value {
        Value::Int(v)
    }

    #[test]
    fn test_binary_op_eq() {
        assert_eq!(
            eval_binary_op(&i(1), &BinaryOp::Eq, &i(1)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&i(1), &BinaryOp::Eq, &i(2)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_binary_op_comparison() {
        assert_eq!(
            eval_binary_op(&i(5), &BinaryOp::Gt, &i(3)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&i(5), &BinaryOp::Lt, &i(3)).unwrap(),
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
            eval_binary_op(&s("hello world"), &BinaryOp::Contains, &s("world")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_scalar_function_size() {
        assert_eq!(
            eval_scalar_function("SIZE", &[Value::List(vec![i(1), i(2), i(3)])]).unwrap(),
            Value::Int(3)
        );
    }

    #[test]
    fn test_scalar_function_head() {
        assert_eq!(
            eval_scalar_function("HEAD", &[Value::List(vec![i(1), i(2), i(3)])]).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn test_scalar_function_coalesce() {
        assert_eq!(
            eval_scalar_function("COALESCE", &[Value::Null, Value::Int(1), Value::Int(2)]).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn test_vector_similarity() {
        let v1 = Value::List(vec![Value::Float(1.0), Value::Float(0.0)]);
        let v2 = Value::List(vec![Value::Float(1.0), Value::Float(0.0)]);
        let result = eval_vector_similarity(&v1, &v2).unwrap();
        assert_eq!(result.as_f64().unwrap(), 1.0);
    }

    #[test]
    fn test_regex_match() {
        // Basic regex match
        assert_eq!(
            eval_binary_op(&s("hello world"), &BinaryOp::Regex, &s("hello.*")).unwrap(),
            Value::Bool(true)
        );

        // No match
        assert_eq!(
            eval_binary_op(&s("hello world"), &BinaryOp::Regex, &s("^world")).unwrap(),
            Value::Bool(false)
        );

        // Case sensitive
        assert_eq!(
            eval_binary_op(&s("Hello"), &BinaryOp::Regex, &s("hello")).unwrap(),
            Value::Bool(false)
        );

        // Case insensitive with flag
        assert_eq!(
            eval_binary_op(&s("Hello"), &BinaryOp::Regex, &s("(?i)hello")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_regex_null_handling() {
        // Left operand is null
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Regex, &s(".*")).unwrap(),
            Value::Null
        );

        // Right operand is null
        assert_eq!(
            eval_binary_op(&s("hello"), &BinaryOp::Regex, &Value::Null).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_regex_invalid_pattern() {
        // Invalid regex pattern should return error
        let result = eval_binary_op(&s("hello"), &BinaryOp::Regex, &s("[invalid"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid regex"));
    }

    #[test]
    fn test_regex_special_characters() {
        // Email pattern with escaped dots
        assert_eq!(
            eval_binary_op(
                &s("test@example.com"),
                &BinaryOp::Regex,
                &s(r"^[\w.-]+@[\w.-]+\.\w+$")
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Phone number pattern
        assert_eq!(
            eval_binary_op(
                &s("123-456-7890"),
                &BinaryOp::Regex,
                &s(r"^\d{3}-\d{3}-\d{4}$")
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Non-matching phone
        assert_eq!(
            eval_binary_op(
                &s("1234567890"),
                &BinaryOp::Regex,
                &s(r"^\d{3}-\d{3}-\d{4}$")
            )
            .unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_regex_anchors() {
        // Start anchor
        assert_eq!(
            eval_binary_op(&s("hello world"), &BinaryOp::Regex, &s("^hello")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&s("say hello"), &BinaryOp::Regex, &s("^hello")).unwrap(),
            Value::Bool(false)
        );

        // End anchor
        assert_eq!(
            eval_binary_op(&s("hello world"), &BinaryOp::Regex, &s("world$")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&s("world hello"), &BinaryOp::Regex, &s("world$")).unwrap(),
            Value::Bool(false)
        );

        // Full match with both anchors
        assert_eq!(
            eval_binary_op(&s("hello"), &BinaryOp::Regex, &s("^hello$")).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&s("hello world"), &BinaryOp::Regex, &s("^hello$")).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_temporal_arithmetic() {
        // datetime + duration (1 hour)
        let dt = s("2024-01-15T10:00:00Z");
        let dur = Value::Int(3_600_000_000_i64);
        let result = eval_binary_op(&dt, &BinaryOp::Add, &dur).unwrap();
        assert!(result.to_string().contains("11:00"));

        // date + duration (1 day)
        let d = s("2024-01-01");
        let dur_day = Value::Int(86_400_000_000_i64);
        let result = eval_binary_op(&d, &BinaryOp::Add, &dur_day).unwrap();
        assert_eq!(result.to_string(), "2024-01-02");

        // datetime - datetime (returns ISO 8601 duration)
        let dt1 = s("2024-01-02T00:00:00Z");
        let dt2 = s("2024-01-01T00:00:00Z");
        let result = eval_binary_op(&dt1, &BinaryOp::Sub, &dt2).unwrap();
        // Result is now ISO 8601 duration string (1 day = PT24H for datetime types)
        let dur_str = result.to_string();
        assert!(dur_str.starts_with('P'));
        assert!(dur_str.contains("24H")); // 24 hours
    }

    // Bitwise operator tests removed - bitwise operations now use functions (uni_bitwise_*)
    // See bitwise_functions_test.rs for comprehensive bitwise function tests

    #[test]
    fn test_temporal_arithmetic_edge_cases() {
        // Negative duration (subtracting time)
        let dt = s("2024-01-15T10:00:00Z");
        let neg_dur = Value::Int(-3_600_000_000_i64); // -1 hour
        let result = eval_binary_op(&dt, &BinaryOp::Add, &neg_dur).unwrap();
        assert!(result.to_string().contains("09:00"));

        // Duration subtraction resulting in negative duration
        let dur1 = s("PT1H"); // 1 hour as ISO 8601
        let dur2 = s("PT2H"); // 2 hours as ISO 8601
        let result = eval_binary_op(&dur1, &BinaryOp::Sub, &dur2).unwrap();
        // Result is ISO 8601 duration string (negative 1 hour)
        let dur_str = result.to_string();
        assert!(dur_str.starts_with('P') || dur_str.starts_with("-P"));

        // Zero duration addition
        let dt = s("2024-01-15T10:00:00Z");
        let zero_dur = Value::Int(0_i64);
        let result = eval_binary_op(&dt, &BinaryOp::Add, &zero_dur).unwrap();
        assert!(result.to_string().contains("10:00"));

        // Date crossing year boundary
        let d = s("2023-12-31");
        let one_day = Value::Int(86_400_000_000_i64);
        let result = eval_binary_op(&d, &BinaryOp::Add, &one_day).unwrap();
        assert_eq!(result.to_string(), "2024-01-01");

        // Same datetime subtraction yields zero duration
        let dt1 = s("2024-01-15T10:00:00Z");
        let dt2 = s("2024-01-15T10:00:00Z");
        let result = eval_binary_op(&dt1, &BinaryOp::Sub, &dt2).unwrap();
        // Zero duration should be "PT0S" or similar
        let dur_str = result.to_string();
        assert!(dur_str.starts_with('P'));

        // Leap year handling
        let leap_day = s("2024-02-28");
        let one_day = Value::Int(86_400_000_000_i64);
        let result = eval_binary_op(&leap_day, &BinaryOp::Add, &one_day).unwrap();
        assert_eq!(result.to_string(), "2024-02-29");
    }

    #[test]
    fn test_regex_empty_string() {
        // Empty string matches empty pattern
        assert_eq!(
            eval_binary_op(&s(""), &BinaryOp::Regex, &s("^$")).unwrap(),
            Value::Bool(true)
        );

        // Empty string doesn't match non-empty pattern
        assert_eq!(
            eval_binary_op(&s(""), &BinaryOp::Regex, &s(".+")).unwrap(),
            Value::Bool(false)
        );

        // Non-empty string matches .* (matches anything including empty)
        assert_eq!(
            eval_binary_op(&s("hello"), &BinaryOp::Regex, &s(".*")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_regex_type_errors() {
        // Non-string left operand
        let result = eval_binary_op(&Value::Int(123), &BinaryOp::Regex, &s("\\d+"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be a string"));

        // Non-string right operand (pattern)
        let result = eval_binary_op(&s("hello"), &BinaryOp::Regex, &Value::Int(123));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("pattern string"));
    }

    #[test]
    fn test_and_null_handling() {
        // Three-valued logic: false dominates, null propagates with true

        // false AND null = false (false dominates)
        assert_eq!(
            eval_binary_op(&Value::Bool(false), &BinaryOp::And, &Value::Null).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::And, &Value::Bool(false)).unwrap(),
            Value::Bool(false)
        );

        // true AND null = null
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::And, &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::And, &Value::Bool(true)).unwrap(),
            Value::Null
        );

        // null AND null = null
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::And, &Value::Null).unwrap(),
            Value::Null
        );

        // Non-null cases still work
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::And, &Value::Bool(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::And, &Value::Bool(false)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_or_null_handling() {
        // Three-valued logic: true dominates, null propagates with false

        // true OR null = true (true dominates)
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Or, &Value::Null).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Or, &Value::Bool(true)).unwrap(),
            Value::Bool(true)
        );

        // false OR null = null
        assert_eq!(
            eval_binary_op(&Value::Bool(false), &BinaryOp::Or, &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Or, &Value::Bool(false)).unwrap(),
            Value::Null
        );

        // null OR null = null
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Or, &Value::Null).unwrap(),
            Value::Null
        );

        // Non-null cases still work
        assert_eq!(
            eval_binary_op(&Value::Bool(false), &BinaryOp::Or, &Value::Bool(false)).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Or, &Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_nan_comparison_with_non_numeric() {
        let nan = Value::Float(f64::NAN);

        // NaN > number → false
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::Gt, &i(1)).unwrap(),
            Value::Bool(false)
        );

        // NaN > NaN → false
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::Gt, &nan).unwrap(),
            Value::Bool(false)
        );

        // NaN > string → null (cross-type)
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::Gt, &s("a")).unwrap(),
            Value::Null
        );

        // string < NaN → null (cross-type)
        assert_eq!(
            eval_binary_op(&s("a"), &BinaryOp::Lt, &nan).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_nan_equality_with_non_numeric() {
        let nan = Value::Float(f64::NAN);

        // NaN = NaN → false
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::Eq, &nan).unwrap(),
            Value::Bool(false)
        );

        // NaN <> NaN → true
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::NotEq, &nan).unwrap(),
            Value::Bool(true)
        );

        // NaN = 'a' → false (structural mismatch at cypher_eq fallback)
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::Eq, &s("a")).unwrap(),
            Value::Bool(false)
        );

        // NaN <> 'a' → true
        assert_eq!(
            eval_binary_op(&nan, &BinaryOp::NotEq, &s("a")).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_large_integer_equality() {
        // These two values are distinct as i64 but collide when cast to f64
        let a = Value::Int(4611686018427387905_i64);
        let b = Value::Int(4611686018427387900_i64);

        assert_eq!(
            eval_binary_op(&a, &BinaryOp::Eq, &b).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            eval_binary_op(&a, &BinaryOp::Eq, &a).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_large_integer_ordering() {
        let a = Value::Int(4611686018427387905_i64);
        let b = Value::Int(4611686018427387900_i64);

        assert_eq!(
            eval_binary_op(&a, &BinaryOp::Gt, &b).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&b, &BinaryOp::Lt, &a).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_int_float_equality_still_works() {
        // Regression: 1 = 1.0 must still be true
        assert_eq!(
            eval_binary_op(&i(1), &BinaryOp::Eq, &Value::Float(1.0)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&i(1), &BinaryOp::NotEq, &Value::Float(1.0)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_xor_null_handling() {
        // Three-valued logic: any null operand returns null

        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Xor, &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(&Value::Bool(false), &BinaryOp::Xor, &Value::Null).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Xor, &Value::Bool(true)).unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_binary_op(&Value::Null, &BinaryOp::Xor, &Value::Null).unwrap(),
            Value::Null
        );

        // Non-null cases still work
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Xor, &Value::Bool(false)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_binary_op(&Value::Bool(true), &BinaryOp::Xor, &Value::Bool(true)).unwrap(),
            Value::Bool(false)
        );
    }
}
