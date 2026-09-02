// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Expression evaluation helper functions.
//!
//! This module extracts high-complexity expression evaluation logic from the main executor
//! to reduce cognitive complexity and improve maintainability.

use anyhow::{Result, anyhow};
use std::cmp::Ordering;
use uni_common::{TemporalValue, Value};

use crate::datetime::{
    CypherDuration, TemporalType, add_cypher_duration_to_date, add_cypher_duration_to_datetime,
    add_cypher_duration_to_localdatetime, add_cypher_duration_to_localtime,
    add_cypher_duration_to_time, classify_temporal, eval_datetime_function, is_duration_value,
    parse_datetime_utc, parse_duration_from_value, parse_duration_to_cypher,
};
use crate::df_udfs::checked_int_op;
use crate::spatial::eval_spatial_function;
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
        BinaryOp::Eq => Ok(cypher_eq(left, right).map_or(Value::Null, Value::Bool)),
        BinaryOp::NotEq => Ok(cypher_eq(left, right).map_or(Value::Null, |b| Value::Bool(!b))),
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
        BinaryOp::Mod => {
            // Integer % integer uses checked remainder (rejects modulo by zero,
            // stays exact above 2^53). Float operands keep the f64 path.
            if let (Value::Int(l), Value::Int(r)) = (left, right) {
                checked_int_op(*l, *r, &BinaryOp::Mod)
                    .map(Value::Int)
                    .ok_or_else(|| anyhow!("division by zero"))
            } else {
                eval_numeric_op(left, right, |a, b| a % b)
            }
        }
        BinaryOp::Pow => eval_numeric_op(left, right, |a, b| a.powf(b)),
        BinaryOp::Regex => {
            let l = left
                .as_str()
                .ok_or_else(|| anyhow!("Left operand of =~ must be a string"))?;
            let pattern = right
                .as_str()
                .ok_or_else(|| anyhow!("Right operand of =~ must be a regex pattern string"))?;
            let matched = with_cached_regex(pattern, |re| re.is_match(l))?;
            Ok(Value::Bool(matched))
        }
        BinaryOp::ApproxEq => eval_vector_similarity(left, right),
    }
}

/// Maximum compiled patterns kept per thread before the cache is reset.
const REGEX_CACHE_CAP: usize = 64;

thread_local! {
    /// Per-thread compiled-regex cache for the interpreted `=~` path, which
    /// previously recompiled the pattern on every row. Thread-local avoids
    /// any cross-query lock; per-query pattern counts are tiny, so a full
    /// clear at capacity is simpler than LRU and effectively never fires.
    static REGEX_CACHE: std::cell::RefCell<std::collections::HashMap<String, regex::Regex>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Run `f` against the compiled regex for `pattern`, compiling and caching on
/// first use. Only successful compiles are cached — an invalid pattern errors
/// identically on every row.
fn with_cached_regex<T>(pattern: &str, f: impl FnOnce(&regex::Regex) -> T) -> Result<T> {
    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(re) = cache.get(pattern) {
            return Ok(f(re));
        }
        let re = regex::Regex::new(pattern)
            .map_err(|e| anyhow!("Invalid regex pattern '{}': {}", pattern, e))?;
        if cache.len() >= REGEX_CACHE_CAP {
            cache.clear();
        }
        let re = cache.entry(pattern.to_string()).or_insert(re);
        Ok(f(re))
    })
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

    // Graph entities compare by identity, never structurally.
    //
    // Two nodes are the same node iff they have the same id — properties and
    // labels are attributes of that one entity, not part of what distinguishes
    // it. Without this arm the derived `PartialEq` on `Node` compared the vid,
    // the labels *and* the whole property map, so the same node hydrated with
    // different property sets on either side of the comparison comes back
    // unequal. That is a well-formed wrong answer with no error: `n IN [n]`
    // returned false.
    //
    // The `Value::Map` arm below already does exactly this for the legacy
    // `_vid` / `_eid` map encoding of an entity; the typed `Node` / `Edge`
    // variants were simply never given the same treatment. `=` did not expose
    // it because the planner lowers node equality to a VID comparison — only
    // the paths routed through `cypher_eq` (`IN`, list membership) inherited
    // the structural comparison.
    match (left, right) {
        (Value::Node(l), Value::Node(r)) => return Some(l.vid == r.vid),
        (Value::Edge(l), Value::Edge(r)) => return Some(l.eid == r.eid),
        // A node is never equal to an edge, whatever their ids.
        (Value::Node(_), Value::Edge(_)) | (Value::Edge(_), Value::Node(_)) => {
            return Some(false);
        }
        _ => {}
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

        // Fallback: Check for Node Object vs VID mismatch.
        // When left is a node map, compare its _vid against list items that may
        // be raw VID integers or "label:offset" strings.
        if let Value::Map(map) = left
            && let Some(vid_val) = map.get("_vid")
            && let Some(vid_u64) = vid_val.as_u64()
        {
            let vid = uni_common::core::id::Vid::from(vid_u64);
            let vid_str = vid.to_string();
            for item in arr {
                match item {
                    Value::String(s) if s == &vid_str => return Ok(Value::Bool(true)),
                    Value::Int(n) if *n as u64 == vid_u64 => return Ok(Value::Bool(true)),
                    _ => {}
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
        Value::Map(map) => {
            if let Some(tv) = temporal_from_map_wrapper(map) {
                add_temporal_duration_typed(&tv, dur)
            } else {
                Err(anyhow!("Expected temporal value for duration arithmetic"))
            }
        }
        Value::String(s) => {
            if let Some(tv) = temporal_from_json_wrapper_str(s) {
                return add_temporal_duration_typed(&tv, dur);
            }
            let ttype = classify_temporal(s)
                .ok_or_else(|| anyhow!("Cannot classify temporal value: {}", s))?;
            let result_str = match ttype {
                TemporalType::Date => add_cypher_duration_to_date(s, dur)?,
                TemporalType::LocalTime => add_cypher_duration_to_localtime(s, dur)?,
                TemporalType::Time => add_cypher_duration_to_time(s, dur)?,
                TemporalType::LocalDateTime => add_cypher_duration_to_localdatetime(s, dur)?,
                TemporalType::DateTime => add_cypher_duration_to_datetime(s, dur)?,
                TemporalType::Duration => {
                    return Err(anyhow!("Cannot add duration to Duration this way"));
                }
                TemporalType::Btic => {
                    return Err(anyhow!(
                        "TypeError: Cannot add duration to BTIC interval. \
                         BTIC represents a time interval, not a point. \
                         Use btic_span() to combine intervals or construct a new btic() literal."
                    ));
                }
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
        TemporalType::Duration => {
            return Err(anyhow!("Cannot add duration to Duration this way"));
        }
        TemporalType::Btic => {
            return Err(anyhow!(
                "TypeError: Cannot add duration to BTIC interval. \
                 BTIC represents a time interval, not a point. \
                 Use btic_span() to combine intervals or construct a new btic() literal."
            ));
        }
    };
    // Re-parse through the datetime constructor to get a Value::Temporal
    let args = [Value::String(result_str)];
    match ttype {
        TemporalType::Date => eval_datetime_function("DATE", &args),
        TemporalType::LocalTime => eval_datetime_function("LOCALTIME", &args),
        TemporalType::Time => eval_datetime_function("TIME", &args),
        TemporalType::LocalDateTime => eval_datetime_function("LOCALDATETIME", &args),
        TemporalType::DateTime => eval_datetime_function("DATETIME", &args),
        TemporalType::Duration | TemporalType::Btic => unreachable!(),
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
        if let (Some(li), Some(ri)) = (left.as_i64(), right.as_i64())
            && left.is_i64()
            && right.is_i64()
        {
            return checked_int_op(li, ri, &BinaryOp::Add)
                .map(Value::Int)
                .ok_or_else(|| anyhow!("integer overflow"));
        }
        return Ok(Value::Float(l + r));
    }

    // Temporal string + Duration / Duration + Temporal string
    if let Value::String(s) = left
        && classify_temporal(s).is_some_and(|t| t != TemporalType::Duration)
        && let Ok(dur) = parse_duration_from_value(right)
    {
        return add_temporal_duration_to_value(left, &dur);
    }
    if let Value::String(s) = right
        && classify_temporal(s).is_some_and(|t| t != TemporalType::Duration)
        && let Ok(dur) = parse_duration_from_value(left)
    {
        return add_temporal_duration_to_value(right, &dur);
    }

    // Temporal + Duration (supports typed temporals and map-wrapped temporals)
    if let Some(tv) = temporal_from_value(left)
        && !matches!(tv, TemporalValue::Duration { .. })
        && (is_duration_value(right) || right.is_number())
    {
        let dur = parse_duration_from_value(right)?;
        return add_temporal_duration_typed(&tv, &dur);
    }
    // Duration + Temporal
    if let Some(tv) = temporal_from_value(right)
        && !matches!(tv, TemporalValue::Duration { .. })
        && (is_duration_value(left) || left.is_number())
    {
        let dur = parse_duration_from_value(left)?;
        return add_temporal_duration_typed(&tv, &dur);
    }
    // Duration + Duration
    if let (
        Some(TemporalValue::Duration {
            months: m1,
            days: d1,
            nanos: n1,
        }),
        Some(TemporalValue::Duration {
            months: m2,
            days: d2,
            nanos: n2,
        }),
    ) = (temporal_from_value(left), temporal_from_value(right))
    {
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
        && classify_value_temporal(left).is_some_and(|t| t != TemporalType::Duration)
    {
        let dur = parse_duration_from_value(right)?;
        return add_temporal_duration_to_value(left, &dur);
    }
    // integer microseconds + temporal string
    if let Value::String(_) = right
        && left.is_number()
        && classify_value_temporal(right).is_some_and(|t| t != TemporalType::Duration)
    {
        let dur = parse_duration_from_value(left)?;
        return add_temporal_duration_to_value(right, &dur);
    }

    Err(anyhow!(
        "Invalid types for addition: left={:?}, right={:?}",
        left,
        right
    ))
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
    if let Value::Temporal(tv) = left
        && !matches!(tv, TemporalValue::Duration { .. })
    {
        if let Value::Temporal(TemporalValue::Duration {
            months,
            days,
            nanos,
        }) = right
        {
            let dur = CypherDuration::new(-months, -days, -nanos);
            return add_temporal_duration_typed(tv, &dur);
        }
        if is_duration_value(right) || right.is_number() {
            let dur = parse_duration_from_value(right)?.negate();
            return add_temporal_duration_typed(tv, &dur);
        }
    }
    // Duration - Duration (Value::Temporal)
    if let (
        Value::Temporal(TemporalValue::Duration {
            months: m1,
            days: d1,
            nanos: n1,
        }),
        Value::Temporal(TemporalValue::Duration {
            months: m2,
            days: d2,
            nanos: n2,
        }),
    ) = (left, right)
    {
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
        return crate::datetime::eval_datetime_function("DURATION.BETWEEN", &args);
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
                return crate::datetime::eval_datetime_function("DURATION.BETWEEN", &args);
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

    // Integer - integer uses checked subtraction (rejects overflow, stays exact above 2^53).
    if let (Value::Int(l), Value::Int(r)) = (left, right) {
        return checked_int_op(*l, *r, &BinaryOp::Sub)
            .map(Value::Int)
            .ok_or_else(|| anyhow!("integer overflow"));
    }

    eval_numeric_op(left, right, |a, b| a - b)
}

/// Extract a CypherDuration from a Value, if it is a duration type.
///
/// Handles both `Value::Temporal(Duration { .. })` and duration strings.
fn extract_cypher_duration(val: &Value) -> Option<Result<(CypherDuration, bool)>> {
    match val {
        Value::Temporal(TemporalValue::Duration {
            months,
            days,
            nanos,
        }) => Some(Ok((CypherDuration::new(*months, *days, *nanos), true))),
        Value::String(s) if is_duration_value(val) => {
            Some(parse_duration_to_cypher(s).map(|d| (d, false)))
        }
        _ => None,
    }
}

/// Convert a `CypherDuration` result back to the appropriate `Value` type.
///
/// `is_temporal` indicates whether the source was a `Value::Temporal` (returns temporal)
/// or a `Value::String` (returns ISO 8601 string).
fn duration_to_value(result: CypherDuration, is_temporal: bool) -> Value {
    if is_temporal {
        result.to_temporal_value()
    } else {
        Value::String(result.to_iso8601())
    }
}

/// Evaluate multiplication with duration support.
fn eval_mul(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // duration * number (either side)
    if let Some(dur_result) = extract_cypher_duration(left)
        && let Some(factor) = right.as_f64()
    {
        let (dur, is_temporal) = dur_result?;
        return Ok(duration_to_value(dur.multiply(factor), is_temporal));
    }
    if let Some(dur_result) = extract_cypher_duration(right)
        && let Some(factor) = left.as_f64()
    {
        let (dur, is_temporal) = dur_result?;
        return Ok(duration_to_value(dur.multiply(factor), is_temporal));
    }

    // Integer * integer uses checked multiplication (rejects overflow, stays exact above 2^53).
    if let (Value::Int(l), Value::Int(r)) = (left, right) {
        return checked_int_op(*l, *r, &BinaryOp::Mul)
            .map(Value::Int)
            .ok_or_else(|| anyhow!("integer overflow"));
    }

    eval_numeric_op(left, right, |a, b| a * b)
}

/// Evaluate division with duration support.
fn eval_div(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    // duration / number (left side only -- division is not commutative)
    if let Some(dur_result) = extract_cypher_duration(left)
        && let Some(divisor) = right.as_f64()
    {
        let (dur, is_temporal) = dur_result?;
        return Ok(duration_to_value(dur.divide(divisor), is_temporal));
    }

    // OpenCypher: integer / integer = integer (truncated toward zero)
    if let (Value::Int(l), Value::Int(r)) = (left, right) {
        return if *r == 0 {
            Err(anyhow!("Division by zero"))
        } else {
            Ok(Value::Int(l / r))
        };
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

    let left_temporal = temporal_from_value(left);
    let right_temporal = temporal_from_value(right);
    if let (Some(l), Some(r)) = (&left_temporal, &right_temporal) {
        return temporal_partial_cmp(l, r);
    }
    if let (Some(_), Value::String(rs)) = (&left_temporal, right) {
        let ls = left.to_string();
        if let (Some(lt), Some(rt)) = (classify_temporal(&ls), classify_temporal(rs))
            && lt == rt
        {
            return temporal_string_cmp(&ls, rs, lt);
        }
        return None;
    }
    if let (Value::String(ls), Some(_)) = (left, &right_temporal) {
        let rs = right.to_string();
        if let (Some(lt), Some(rt)) = (classify_temporal(ls), classify_temporal(&rs))
            && lt == rt
        {
            return temporal_string_cmp(ls, &rs, lt);
        }
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

    // String vs String (includes temporal string comparison for ISO-format strings)
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
pub(crate) fn temporal_partial_cmp(
    left: &TemporalValue,
    right: &TemporalValue,
) -> Option<Ordering> {
    match (left, right) {
        (
            TemporalValue::Date {
                days_since_epoch: l,
            },
            TemporalValue::Date {
                days_since_epoch: r,
            },
        ) => Some(l.cmp(r)),
        (
            TemporalValue::LocalTime {
                nanos_since_midnight: l,
            },
            TemporalValue::LocalTime {
                nanos_since_midnight: r,
            },
        ) => Some(l.cmp(r)),
        (
            TemporalValue::Time {
                nanos_since_midnight: lm,
                offset_seconds: lo,
            },
            TemporalValue::Time {
                nanos_since_midnight: rm,
                offset_seconds: ro,
            },
        ) => {
            // Compare in UTC: local_nanos - offset
            let l_utc = *lm as i128 - (*lo as i128) * 1_000_000_000;
            let r_utc = *rm as i128 - (*ro as i128) * 1_000_000_000;
            Some(l_utc.cmp(&r_utc))
        }
        (
            TemporalValue::LocalDateTime {
                nanos_since_epoch: l,
            },
            TemporalValue::LocalDateTime {
                nanos_since_epoch: r,
            },
        ) => Some(l.cmp(r)),
        (
            TemporalValue::DateTime {
                nanos_since_epoch: l,
                ..
            },
            TemporalValue::DateTime {
                nanos_since_epoch: r,
                ..
            },
        ) => {
            // Both are in UTC, so direct comparison
            Some(l.cmp(r))
        }
        // BTIC intervals: use canonical (lo, hi, meta) total order from Btic::Ord
        (
            TemporalValue::Btic {
                lo: l_lo,
                hi: l_hi,
                meta: l_meta,
            },
            TemporalValue::Btic {
                lo: r_lo,
                hi: r_hi,
                meta: r_meta,
            },
        ) => match (
            uni_btic::Btic::new(*l_lo, *l_hi, *l_meta),
            uni_btic::Btic::new(*r_lo, *r_hi, *r_meta),
        ) {
            (Ok(l), Ok(r)) => Some(l.cmp(&r)),
            _ => None,
        },
        // Durations are not orderable
        (TemporalValue::Duration { .. }, TemporalValue::Duration { .. }) => None,
        // Different temporal types are not comparable
        _ => None,
    }
}

/// Extract a `TemporalValue` from any `Value` variant that can represent one.
///
/// Handles `Value::Temporal`, `Value::Map` (JSON-serialized temporal wrappers),
/// and `Value::String` — first tries JSON wrapper format
/// (`{"Date":{"days_since_epoch":0}}`), then falls back to human-readable
/// ISO 8601 strings like `"2024-01-15"` or `"12:35:15+05:00"`.
pub fn temporal_from_value(v: &Value) -> Option<TemporalValue> {
    match v {
        Value::Temporal(tv) => Some(tv.clone()),
        Value::Map(map) => temporal_from_map_wrapper(map),
        Value::String(s) => {
            temporal_from_json_wrapper_str(s).or_else(|| temporal_from_human_readable_str(s))
        }
        _ => None,
    }
}

/// Parse a human-readable ISO 8601 temporal string (e.g. `"12:35:15+05:00"`,
/// `"2024-01-15"`) into a `TemporalValue` by classifying and evaluating it.
pub fn temporal_from_human_readable_str(s: &str) -> Option<TemporalValue> {
    let fn_name = match classify_temporal(s)? {
        TemporalType::Date => "DATE",
        TemporalType::LocalTime => "LOCALTIME",
        TemporalType::Time => "TIME",
        TemporalType::LocalDateTime => "LOCALDATETIME",
        TemporalType::DateTime => "DATETIME",
        TemporalType::Duration => "DURATION",
        TemporalType::Btic => return None, // BTIC literals are parsed via property type context
    };
    match eval_datetime_function(fn_name, &[Value::String(s.to_string())]).ok()? {
        Value::Temporal(tv) => Some(tv),
        _ => None,
    }
}

/// Try to interpret a map as a temporal value.
///
/// Recognizes single-entry maps with a temporal type key (`Date`, `Time`, etc.)
/// whose value is a map of the appropriate fields. Returns `None` if the map
/// does not match any temporal pattern.
pub fn temporal_from_map_wrapper(
    map: &std::collections::HashMap<String, Value>,
) -> Option<TemporalValue> {
    if map.len() != 1 {
        return None;
    }

    let as_i32 = |v: &Value| v.as_i64().and_then(|n| i32::try_from(n).ok());
    let as_i64 = |v: &Value| v.as_i64();

    if let Some(Value::Map(inner)) = map.get("Date") {
        let days = inner.get("days_since_epoch").and_then(as_i32)?;
        return Some(TemporalValue::Date {
            days_since_epoch: days,
        });
    }
    if let Some(Value::Map(inner)) = map.get("LocalTime") {
        let nanos = inner.get("nanos_since_midnight").and_then(as_i64)?;
        return Some(TemporalValue::LocalTime {
            nanos_since_midnight: nanos,
        });
    }
    if let Some(Value::Map(inner)) = map.get("Time") {
        let nanos = inner.get("nanos_since_midnight").and_then(as_i64)?;
        let offset = inner.get("offset_seconds").and_then(as_i32)?;
        return Some(TemporalValue::Time {
            nanos_since_midnight: nanos,
            offset_seconds: offset,
        });
    }
    if let Some(Value::Map(inner)) = map.get("LocalDateTime") {
        let nanos = inner.get("nanos_since_epoch").and_then(as_i64)?;
        return Some(TemporalValue::LocalDateTime {
            nanos_since_epoch: nanos,
        });
    }
    if let Some(Value::Map(inner)) = map.get("DateTime") {
        let nanos = inner.get("nanos_since_epoch").and_then(as_i64)?;
        let offset = inner.get("offset_seconds").and_then(as_i32)?;
        let timezone_name = match inner.get("timezone_name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        };
        return Some(TemporalValue::DateTime {
            nanos_since_epoch: nanos,
            offset_seconds: offset,
            timezone_name,
        });
    }
    if let Some(Value::Map(inner)) = map.get("Duration") {
        let months = inner.get("months").and_then(as_i64)?;
        let days = inner.get("days").and_then(as_i64)?;
        let nanos = inner.get("nanos").and_then(as_i64)?;
        return Some(TemporalValue::Duration {
            months,
            days,
            nanos,
        });
    }
    None
}

fn temporal_from_json_wrapper_str(s: &str) -> Option<TemporalValue> {
    let parsed: serde_json::Value = serde_json::from_str(s).ok()?;
    let obj = parsed.as_object()?;
    if obj.len() != 1 {
        return None;
    }

    let as_i32 = |o: &serde_json::Map<String, serde_json::Value>, key: &str| {
        o.get(key)
            .and_then(serde_json::Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
    };
    let as_i64 = |o: &serde_json::Map<String, serde_json::Value>, key: &str| {
        o.get(key).and_then(serde_json::Value::as_i64)
    };

    if let Some(inner) = obj.get("Date").and_then(serde_json::Value::as_object) {
        return Some(TemporalValue::Date {
            days_since_epoch: as_i32(inner, "days_since_epoch")?,
        });
    }
    if let Some(inner) = obj.get("LocalTime").and_then(serde_json::Value::as_object) {
        return Some(TemporalValue::LocalTime {
            nanos_since_midnight: as_i64(inner, "nanos_since_midnight")?,
        });
    }
    if let Some(inner) = obj.get("Time").and_then(serde_json::Value::as_object) {
        return Some(TemporalValue::Time {
            nanos_since_midnight: as_i64(inner, "nanos_since_midnight")?,
            offset_seconds: as_i32(inner, "offset_seconds")?,
        });
    }
    if let Some(inner) = obj
        .get("LocalDateTime")
        .and_then(serde_json::Value::as_object)
    {
        return Some(TemporalValue::LocalDateTime {
            nanos_since_epoch: as_i64(inner, "nanos_since_epoch")?,
        });
    }
    if let Some(inner) = obj.get("DateTime").and_then(serde_json::Value::as_object) {
        return Some(TemporalValue::DateTime {
            nanos_since_epoch: as_i64(inner, "nanos_since_epoch")?,
            offset_seconds: as_i32(inner, "offset_seconds")?,
            timezone_name: inner
                .get("timezone_name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        });
    }
    if let Some(inner) = obj.get("Duration").and_then(serde_json::Value::as_object) {
        return Some(TemporalValue::Duration {
            months: as_i64(inner, "months")?,
            days: as_i64(inner, "days")?,
            nanos: as_i64(inner, "nanos")?,
        });
    }
    None
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
        TemporalType::Btic => None,     // BTIC comparison uses packed byte ordering
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
    let (_, time, tz_info) = crate::datetime::parse_datetime_with_tz(s)?;
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
        // Character count, not byte length: Cypher size()/length() count
        // codepoints (matching the DataFusion `cypher_size_scalar` path), so a
        // non-ASCII string like "café" is 4, not 5.
        Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("size() expects a List, Map, or String")),
    }
}

fn eval_keys(arg: &Value) -> Result<Value> {
    match arg {
        Value::Map(map) => {
            // Entities (nodes/edges) are detected by internal fields (_vid, _eid).
            // For entities, null-valued properties don't exist (REMOVE sets them to Null).
            // For plain maps, null-valued keys are valid and must be included.
            let is_entity =
                map.contains_key("_vid") || map.contains_key("_eid") || map.contains_key("_labels");
            let mut keys: Vec<&String> = map
                .iter()
                .filter(|(k, v)| !k.starts_with('_') && (!is_entity || !v.is_null()))
                .map(|(k, _)| k)
                .collect();
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
        Value::List(arr) => Ok(Value::List(arr.get(1..).unwrap_or_default().to_vec())),
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
        // Character count, not byte length: Cypher size()/length() count
        // codepoints (matching the DataFusion `cypher_size_scalar` path), so a
        // non-ASCII string like "café" is 4, not 5.
        Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
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
        Value::Map(map) => Ok(map.get("nodes").cloned().unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("nodes() expects a Path")),
    }
}

fn eval_relationships(arg: &Value) -> Result<Value> {
    match arg {
        Value::Path(p) => Ok(Value::List(
            p.edges.iter().map(|e| Value::Edge(e.clone())).collect(),
        )),
        Value::Map(map) => Ok(map.get("relationships").cloned().unwrap_or(Value::Null)),
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

/// Return the Cypher type name for a `Value`, used in error messages.
pub(crate) fn cypher_type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "Null",
        Value::Bool(_) => "Boolean",
        Value::Int(_) => "Integer",
        Value::Float(_) => "Float",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Node(_) => "Node",
        Value::Edge(_) => "Relationship",
        Value::Path(_) => "Path",
        Value::Vector(_) => "Vector",
        Value::Temporal(_) => "Temporal",
        _ => "Unknown",
    }
}

/// Adapt an interpreter error into a DataFusion error, preserving the message.
///
/// The messages carry openCypher error-code prefixes (`TypeError:`,
/// `ArgumentError:`) that TCK scenarios match on, so they must survive verbatim.
pub(crate) fn to_datafusion_err(e: anyhow::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(e.to_string())
}

pub(crate) fn eval_tointeger(arg: &Value) -> Result<Value> {
    match arg {
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Int(*f as i64)),
        // openCypher truncates numeric strings that carry a fractional part:
        // TCK TypeConversion2 [5] asserts toInteger('2.9') == 2 and [4] that
        // toInteger('1.7') == 1. Non-numeric strings yield null, not an error.
        Value::String(s) => Ok(s
            .parse::<i64>()
            .map(Value::Int)
            .or_else(|_| s.parse::<f64>().map(|f| Value::Int(f as i64)))
            .unwrap_or(Value::Null)),
        Value::Null => Ok(Value::Null),
        other => Err(anyhow!(
            "TypeError: InvalidArgumentValue - toInteger() does not accept {} values",
            cypher_type_name(other)
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

pub(crate) fn eval_tostring(arg: &Value) -> Result<Value> {
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
        Value::Temporal(t) => Ok(Value::String(t.to_string())),
        Value::Null => Ok(Value::Null),
        // TCK TypeConversion4 [10] requires a TypeError for List, Map, Node,
        // Relationship and Path rather than a stringified rendering.
        other => Err(anyhow!(
            "TypeError: InvalidArgumentValue - toString() does not accept {} values",
            cypher_type_name(other)
        )),
    }
}

pub(crate) fn eval_toboolean(arg: &Value) -> Result<Value> {
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
        // Integers coerce (0 == false). TCK TypeConversion1 [5] lists Float,
        // List, Map, Node, Relationship and Path as invalid but deliberately
        // omits Integer.
        Value::Int(i) => Ok(Value::Bool(*i != 0)),
        other => Err(anyhow!(
            "TypeError: InvalidArgumentValue - toBoolean() does not accept {} values",
            cypher_type_name(other)
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

fn eval_sqrt(arg: &Value) -> Result<Value> {
    match arg {
        v if v.is_number() => {
            let f = v.as_f64().unwrap();
            if f < 0.0 {
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
        Value::Int(i) => Ok(Value::Int(i.signum())),
        // `f64::signum` returns +/-1.0 for +/-0.0, but Cypher's `sign(0.0)` must
        // be 0. Classify explicitly so both signed zeros map to 0.
        Value::Float(f) => {
            let s = if *f > 0.0 {
                1
            } else if *f < 0.0 {
                -1
            } else {
                0
            };
            Ok(Value::Int(s))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(anyhow!("sign() expects a number")),
    }
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

/// Helper to require exactly one argument for a function.
fn require_one_arg<'a>(name: &str, args: &'a [Value]) -> Result<&'a Value> {
    if args.len() != 1 {
        return Err(anyhow!("{} requires 1 argument", name));
    }
    Ok(&args[0])
}

/// Evaluate math functions: ABS, CEIL, FLOOR, ROUND, SQRT, SIGN, LOG, LOG10, EXP, POWER, SIN, COS, TAN, etc.
///
/// Single-argument trig/math functions that simply delegate to `eval_unary_numeric_op`
/// are inlined here to reduce unnecessary indirection.
fn eval_math_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        // Single-argument functions with dedicated implementations
        "ABS" => eval_abs(require_one_arg(name, args)?),
        "CEIL" => eval_unary_numeric_op(require_one_arg(name, args)?, "ceil", f64::ceil),
        "FLOOR" => eval_unary_numeric_op(require_one_arg(name, args)?, "floor", f64::floor),
        "ROUND" => eval_unary_numeric_op(require_one_arg(name, args)?, "round", f64::round),
        "SQRT" => eval_sqrt(require_one_arg(name, args)?),
        "SIGN" => eval_sign(require_one_arg(name, args)?),
        "LOG" => eval_unary_numeric_op(require_one_arg(name, args)?, "log", f64::ln),
        "LOG10" => eval_unary_numeric_op(require_one_arg(name, args)?, "log10", f64::log10),
        "EXP" => eval_unary_numeric_op(require_one_arg(name, args)?, "exp", f64::exp),
        "SIN" => eval_unary_numeric_op(require_one_arg(name, args)?, "sin", f64::sin),
        "COS" => eval_unary_numeric_op(require_one_arg(name, args)?, "cos", f64::cos),
        "TAN" => eval_unary_numeric_op(require_one_arg(name, args)?, "tan", f64::tan),
        "ASIN" => eval_unary_numeric_op(require_one_arg(name, args)?, "asin", f64::asin),
        "ACOS" => eval_unary_numeric_op(require_one_arg(name, args)?, "acos", f64::acos),
        "ATAN" => eval_unary_numeric_op(require_one_arg(name, args)?, "atan", f64::atan),
        "DEGREES" => {
            eval_unary_numeric_op(require_one_arg(name, args)?, "degrees", f64::to_degrees)
        }
        "RADIANS" => {
            eval_unary_numeric_op(require_one_arg(name, args)?, "radians", f64::to_radians)
        }
        "HAVERSIN" => eval_unary_numeric_op(require_one_arg(name, args)?, "haversin", |f| {
            (1.0 - f.cos()) / 2.0
        }),
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
            use rand::RngExt;
            let mut rng = rand::rng();
            Ok(Value::Float(rng.random_range(0.0..1.0)))
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

pub fn eval_split(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("split() requires 2 arguments"));
    }
    // Cypher propagates null from *any* argument, not just the first.
    if args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(delimiter)) => {
            let parts: Vec<Value> = s
                .split(delimiter.as_str())
                .map(|p| Value::String(p.to_string()))
                .collect();
            Ok(Value::List(parts))
        }
        _ => Err(anyhow!("split() expects string arguments")),
    }
}

pub(crate) fn eval_substring(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("substring() requires 2 or 3 arguments"));
    }
    if args.iter().any(Value::is_null) {
        return Ok(Value::Null);
    }
    match &args[0] {
        Value::String(s) => {
            let start_i = args[1]
                .as_i64()
                .ok_or_else(|| anyhow!("substring() start must be an integer"))?;
            // Cypher requires a non-negative start; reject before the `as usize`
            // cast (which would otherwise turn -1 into a huge index and panic).
            if start_i < 0 {
                return Err(anyhow!(
                    "ArgumentError: NegativeIntegerArgument - substring start must be non-negative"
                ));
            }
            let start = start_i as usize;
            let len = if args.len() == 3 {
                let len_i = args[2]
                    .as_i64()
                    .ok_or_else(|| anyhow!("substring() length must be an integer"))?;
                // A negative length used to cast to `usize::MAX` and then clamp,
                // silently returning the whole tail instead of erroring.
                if len_i < 0 {
                    return Err(anyhow!(
                        "ArgumentError: NegativeIntegerArgument - substring length must be non-negative"
                    ));
                }
                len_i as usize
            } else {
                s.len().saturating_sub(start)
            };
            let chars: Vec<char> = s.chars().collect();
            // `saturating_add` so `start + len` cannot overflow `usize`.
            let end = start.saturating_add(len).min(chars.len());
            let result: String = chars[start.min(chars.len())..end].iter().collect();
            Ok(Value::String(result))
        }
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

/// Shared implementation for lpad/rpad. `pad_left` controls direction.
fn eval_pad(func_name: &str, args: &[Value], pad_left: bool) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(anyhow!("{}() requires 2 or 3 arguments", func_name));
    }
    let s = match &args[0] {
        Value::String(s) => s,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(anyhow!(
                "{}() expects a string as first argument",
                func_name
            ));
        }
    };
    let len = match &args[1] {
        Value::Int(n) => *n as usize,
        Value::Float(f) => *f as i64 as usize,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(anyhow!(
                "{}() expects an integer as second argument",
                func_name
            ));
        }
    };
    if len > 1_000_000 {
        return Err(anyhow!(
            "{}() length exceeds maximum limit of 1,000,000",
            func_name
        ));
    }
    let pad_str = if args.len() == 3 {
        match &args[2] {
            Value::String(p) => p.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(anyhow!(
                    "{}() expects a string as third argument",
                    func_name
                ));
            }
        }
    } else {
        " "
    };

    let s_chars: Vec<char> = s.chars().collect();
    if s_chars.len() >= len {
        return Ok(Value::String(s_chars.into_iter().take(len).collect()));
    }

    let pad_chars: Vec<char> = pad_str.chars().collect();
    if pad_chars.is_empty() {
        return Ok(Value::String(s.clone()));
    }

    let needed = len - s_chars.len();
    let full_pads = needed / pad_chars.len();
    let partial_pad = needed % pad_chars.len();

    let mut padding = String::with_capacity(needed);
    for _ in 0..full_pads {
        padding.push_str(pad_str);
    }
    padding.extend(pad_chars.into_iter().take(partial_pad));

    let result = if pad_left {
        format!("{}{}", padding, s)
    } else {
        format!("{}{}", s, padding)
    };
    Ok(Value::String(result))
}

fn eval_lpad(args: &[Value]) -> Result<Value> {
    eval_pad("lpad", args, true)
}

fn eval_rpad(args: &[Value]) -> Result<Value> {
    eval_pad("rpad", args, false)
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
            // Stop cleanly at the i64 boundary instead of panicking on overflow.
            match i.checked_add(step) {
                Some(n) => i = n,
                None => break,
            }
        }
    } else {
        while i >= end {
            result.push(Value::Int(i));
            match i.checked_add(step) {
                Some(n) => i = n,
                None => break,
            }
        }
    }
    Ok(Value::List(result))
}

/// Evaluate a built-in scalar function.
///
/// This handles functions like COALESCE, NULLIF, SIZE, KEYS, HEAD, TAIL, etc.
/// Functions that require argument evaluation (like COALESCE) take pre-evaluated args.
pub fn eval_scalar_function(
    name: &str,
    args: &[Value],
    custom_fns: Option<&crate::custom_functions::CustomFunctionRegistry>,
) -> Result<Value> {
    let name_upper = name.to_uppercase();

    match name_upper.as_str() {
        // Null-handling functions
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "NULLIF" => {
            if args.len() != 2 {
                return Err(anyhow!("NULLIF requires 2 arguments"));
            }
            if args[0] == args[1] {
                Ok(Value::Null)
            } else {
                Ok(args[0].clone())
            }
        }

        // List/Collection functions
        "SIZE" | "KEYS" | "HEAD" | "TAIL" | "LAST" | "LENGTH" | "NODES" | "RELATIONSHIPS" => {
            eval_list_function(&name_upper, args)
        }

        // Type conversion functions
        "TOINTEGER" | "TOINT" | "TOFLOAT" | "TOSTRING" | "TOBOOLEAN" | "TOBOOL" => {
            eval_type_function(&name_upper, args)
        }

        // Math functions
        "ABS" | "CEIL" | "FLOOR" | "ROUND" | "SQRT" | "SIGN" | "LOG" | "LOG10" | "EXP"
        | "POWER" | "POW" | "SIN" | "COS" | "TAN" | "ASIN" | "ACOS" | "ATAN" | "ATAN2"
        | "DEGREES" | "RADIANS" | "HAVERSIN" | "PI" | "E" | "RAND" => {
            eval_math_function(&name_upper, args)
        }

        // String functions
        "TOUPPER" | "UPPER" | "TOLOWER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE"
        | "REPLACE" | "SPLIT" | "SUBSTRING" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" => {
            eval_string_function(&name_upper, args)
        }

        // Date/Time/BTIC functions
        "DATE"
        | "TIME"
        | "DATETIME"
        | "LOCALDATETIME"
        | "LOCALTIME"
        | "DURATION"
        | "BTIC"
        | "YEAR"
        | "MONTH"
        | "DAY"
        | "HOUR"
        | "MINUTE"
        | "SECOND"
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
        | "LOCALDATETIME.REALTIME"
        | "DURATION.BETWEEN"
        | "DURATION.INMONTHS"
        | "DURATION.INDAYS"
        | "DURATION.INSECONDS" => eval_datetime_function(&name_upper, args),

        // Spatial functions
        "POINT" | "DISTANCE" | "POINT.WITHINBBOX" => eval_spatial_function(&name_upper, args),

        "RANGE" => eval_range_function(args),

        "UNI.TEMPORAL.VALIDAT" => eval_valid_at(args),

        "VECTOR_DISTANCE" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(anyhow!("vector_distance requires 2 or 3 arguments"));
            }
            let metric = if args.len() == 3 {
                args[2].as_str().ok_or(anyhow!("metric must be string"))?
            } else {
                "cosine"
            };
            eval_vector_distance(&args[0], &args[1], metric)
        }

        // Bitwise functions
        "UNI_BITWISE_OR"
        | "UNI_BITWISE_AND"
        | "UNI_BITWISE_XOR"
        | "UNI_BITWISE_NOT"
        | "UNI_BITWISE_SHIFTLEFT"
        | "UNI_BITWISE_SHIFTRIGHT" => eval_bitwise_function(&name_upper, args),

        // Similarity functions — pure vector-vector case only (no storage access).
        // Storage-dependent cases (auto-embed, FTS) are handled in read.rs.
        "SIMILAR_TO" => {
            if args.len() < 2 {
                return Err(anyhow!("similar_to requires at least 2 arguments"));
            }
            crate::similar_to::eval_similar_to_pure(&args[0], &args[1])
        }

        "VECTOR_SIMILARITY" => {
            if args.len() != 2 {
                return Err(anyhow!("vector_similarity takes 2 arguments"));
            }
            eval_vector_similarity(&args[0], &args[1])
        }

        "SPARSE_SIMILAR_TO" => {
            if args.len() != 2 {
                return Err(anyhow!("sparse_similar_to takes 2 arguments"));
            }
            crate::similar_to::eval_sparse_similar_to_pure(&args[0], &args[1])
        }

        // BTIC functions — dispatch via is_btic_function check
        _ if is_btic_function(&name_upper) => eval_btic_function(&name_upper, args),

        _ => {
            // Fall back to custom function registry before reporting an error.
            if let Some(func) = custom_fns.and_then(|r| r.get(name)) {
                return func(args).map_err(|e| anyhow!("{}", e));
            }
            Err(anyhow!("Function {} not implemented or is aggregate", name))
        }
    }
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
    let is_valid = valid_from <= query_time && valid_to.is_none_or(|vt| query_time < vt);

    Ok(Value::Bool(is_valid))
}

/// Coerce a vector-valued argument into `Vec<f64>` for elementwise vector math.
///
/// Accepts both a `Value::Vector` (a stored dense-vector property now decodes to
/// this with type fidelity) and a `Value::List` of numbers (a Cypher array
/// literal / parameter), so callers work uniformly across both surfaces.
///
/// # Errors
/// Returns an error if `v` is neither a vector nor a numeric list, or if any
/// list element is non-numeric.
fn vector_arg_to_f64(v: &Value) -> Result<Vec<f64>> {
    match v {
        Value::Vector(a) => Ok(a.iter().map(|&f| f as f64).collect()),
        // Binary-vector lanes as f64 byte values (`0.0..=255.0`); the binary
        // metrics (`hamming`/`jaccard`) cast each lane back to `u8` for popcount.
        Value::BinaryVector(b) => Ok(b.iter().map(|&byte| byte as f64).collect()),
        Value::List(a) => a
            .iter()
            .map(|e| {
                e.as_f64()
                    .ok_or_else(|| anyhow!("Vector element not a number"))
            })
            .collect(),
        _ => Err(anyhow!("vector argument must be an array")),
    }
}

/// Converts a binary-vector lane (an f64 byte value) back to `u8` for popcount.
///
/// # Errors
/// Returns an error if the lane is not an integer in `[0, 255]` — i.e. the
/// operands are not binary vectors, so `hamming`/`jaccard` do not apply.
fn lane_to_u8(lane: f64) -> Result<u8> {
    if lane.fract() == 0.0 && (0.0..=255.0).contains(&lane) {
        Ok(lane as u8)
    } else {
        Err(anyhow!(
            "hamming/jaccard require binary-vector operands (byte lanes 0..=255); got {lane}"
        ))
    }
}

/// Evaluate vector similarity between two vectors (cosine similarity).
pub fn eval_vector_similarity(v1: &Value, v2: &Value) -> Result<Value> {
    let arr1 = vector_arg_to_f64(v1)?;
    let arr2 = vector_arg_to_f64(v2)?;

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

    for (&f1, &f2) in arr1.iter().zip(arr2.iter()) {
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
    let arr1 = vector_arg_to_f64(v1)?;
    let arr2 = vector_arg_to_f64(v2)?;

    if arr1.len() != arr2.len() {
        return Err(anyhow!(
            "Vector dimensions mismatch: {} vs {}",
            arr1.len(),
            arr2.len()
        ));
    }

    let pairs = || arr1.iter().copied().zip(arr2.iter().copied());

    match metric.to_lowercase().as_str() {
        "cosine" => {
            // Cosine distance = 1 - cosine similarity
            let mut dot = 0.0;
            let mut norm1_sq = 0.0;
            let mut norm2_sq = 0.0;

            for (f1, f2) in pairs() {
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
            for (f1, f2) in pairs() {
                let diff = f1 - f2;
                sum_sq_diff += diff * diff;
            }
            Ok(Value::Float(sum_sq_diff.sqrt()))
        }
        "dot" | "inner_product" => {
            let mut dot = 0.0;
            for (f1, f2) in pairs() {
                dot += f1 * f2;
            }
            Ok(Value::Float(1.0 - dot))
        }
        "l1" | "manhattan" => {
            // Manhattan distance = Σ|xᵢ − yᵢ|.
            let mut sum_abs_diff = 0.0;
            for (f1, f2) in pairs() {
                sum_abs_diff += (f1 - f2).abs();
            }
            Ok(Value::Float(sum_abs_diff))
        }
        "hamming" => {
            // Bit differences over binary-vector lanes: Σ popcount(aᵢ ⊕ bᵢ). Each
            // lane is a byte value (`0..=255`); a non-byte lane is rejected.
            let mut bits = 0u32;
            for (f1, f2) in pairs() {
                let (b1, b2) = (lane_to_u8(f1)?, lane_to_u8(f2)?);
                bits += (b1 ^ b2).count_ones();
            }
            Ok(Value::Float(bits as f64))
        }
        "jaccard" => {
            // Bitwise Jaccard distance = 1 − |A ∩ B| / |A ∪ B|; two all-zero
            // vectors are defined as distance 0.
            let mut inter = 0u32;
            let mut union = 0u32;
            for (f1, f2) in pairs() {
                let (b1, b2) = (lane_to_u8(f1)?, lane_to_u8(f2)?);
                inter += (b1 & b2).count_ones();
                union += (b1 | b2).count_ones();
            }
            let dist = if union == 0 {
                0.0
            } else {
                1.0 - f64::from(inter) / f64::from(union)
            };
            Ok(Value::Float(dist))
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
            | "SIMILAR_TO"
            | "VECTOR_SIMILARITY"
            | "SPARSE_SIMILAR_TO"
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
    ) || is_btic_function(&name_upper)
}

/// Check if an uppercase function name is a known BTIC scalar function.
pub fn is_btic_function(name_upper: &str) -> bool {
    matches!(
        name_upper,
        "BTIC_LO"
            | "BTIC_HI"
            | "BTIC_DURATION"
            | "BTIC_CONTAINS_POINT"
            | "BTIC_OVERLAPS"
            | "BTIC_IS_INSTANT"
            | "BTIC_IS_UNBOUNDED"
            | "BTIC_IS_FINITE"
            | "BTIC_GRANULARITY"
            | "BTIC_LO_GRANULARITY"
            | "BTIC_HI_GRANULARITY"
            | "BTIC_CERTAINTY"
            | "BTIC_LO_CERTAINTY"
            | "BTIC_HI_CERTAINTY"
            | "BTIC_CONTAINS"
            | "BTIC_BEFORE"
            | "BTIC_AFTER"
            | "BTIC_MEETS"
            | "BTIC_ADJACENT"
            | "BTIC_DISJOINT"
            | "BTIC_EQUALS"
            | "BTIC_STARTS"
            | "BTIC_DURING"
            | "BTIC_FINISHES"
            | "BTIC_INTERSECTION"
            | "BTIC_SPAN"
            | "BTIC_GAP"
    )
}

/// Evaluate BTIC accessor and predicate functions.
pub fn eval_btic_function(name: &str, args: &[Value]) -> Result<Value> {
    match name {
        "BTIC_LO" => {
            let Some((lo, _, _)) = require_btic_1arg(name, args)? else {
                return Ok(Value::Null);
            };
            if lo == i64::MIN {
                return Ok(Value::Null);
            }
            Ok(ms_to_utc_datetime(lo))
        }
        "BTIC_HI" => {
            let Some((_, hi, _)) = require_btic_1arg(name, args)? else {
                return Ok(Value::Null);
            };
            if hi == i64::MAX {
                return Ok(Value::Null);
            }
            Ok(ms_to_utc_datetime(hi))
        }
        "BTIC_DURATION" => {
            let Some((lo, hi, _)) = require_btic_1arg(name, args)? else {
                return Ok(Value::Null);
            };
            if lo == i64::MIN || hi == i64::MAX {
                Ok(Value::Null)
            } else {
                Ok(Value::Int(hi - lo))
            }
        }
        "BTIC_IS_INSTANT" => {
            let Some((lo, hi, _)) = require_btic_1arg(name, args)? else {
                return Ok(Value::Null);
            };
            Ok(Value::Bool(hi == lo + 1))
        }
        "BTIC_IS_UNBOUNDED" => {
            let Some((lo, hi, _)) = require_btic_1arg(name, args)? else {
                return Ok(Value::Null);
            };
            Ok(Value::Bool(lo == i64::MIN || hi == i64::MAX))
        }
        "BTIC_IS_FINITE" => {
            let Some((lo, hi, _)) = require_btic_1arg(name, args)? else {
                return Ok(Value::Null);
            };
            Ok(Value::Bool(lo != i64::MIN && hi != i64::MAX))
        }
        "BTIC_GRANULARITY" | "BTIC_LO_GRANULARITY" => {
            let Some(btic) = require_btic_1arg_parsed(name, args)? else {
                return Ok(Value::Null);
            };
            Ok(Value::String(btic.lo_granularity().name().to_string()))
        }
        "BTIC_HI_GRANULARITY" => {
            let Some(btic) = require_btic_1arg_parsed(name, args)? else {
                return Ok(Value::Null);
            };
            Ok(Value::String(btic.hi_granularity().name().to_string()))
        }
        "BTIC_CERTAINTY" | "BTIC_LO_CERTAINTY" => {
            let Some(btic) = require_btic_1arg_parsed(name, args)? else {
                return Ok(Value::Null);
            };
            if name == "BTIC_CERTAINTY" {
                let lc = btic.lo_certainty();
                let hc = btic.hi_certainty();
                Ok(Value::String(lc.least_certain(hc).name().to_string()))
            } else {
                Ok(Value::String(btic.lo_certainty().name().to_string()))
            }
        }
        "BTIC_HI_CERTAINTY" => {
            let Some(btic) = require_btic_1arg_parsed(name, args)? else {
                return Ok(Value::Null);
            };
            Ok(Value::String(btic.hi_certainty().name().to_string()))
        }
        "BTIC_CONTAINS_POINT" => {
            if args.len() != 2 {
                return Err(anyhow!("btic_contains_point requires 2 arguments"));
            }
            if args[0].is_null() || args[1].is_null() {
                return Ok(Value::Null);
            }
            match &args[0] {
                Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
                    let point_ms = extract_point_ms(&args[1])?;
                    Ok(Value::Bool(*lo <= point_ms && point_ms < *hi))
                }
                _ => Err(anyhow!("btic_contains_point: first arg must be BTIC")),
            }
        }
        // All 2-arg (Btic, Btic) -> Bool predicates
        "BTIC_OVERLAPS" | "BTIC_CONTAINS" | "BTIC_BEFORE" | "BTIC_AFTER" | "BTIC_MEETS"
        | "BTIC_ADJACENT" | "BTIC_DISJOINT" | "BTIC_EQUALS" | "BTIC_STARTS" | "BTIC_DURING"
        | "BTIC_FINISHES" => eval_btic_binary_predicate(name, args),
        // Set operations: (Btic, Btic) -> Btic or Null
        "BTIC_INTERSECTION" => eval_btic_set_op(name, args, uni_btic::set_ops::intersection),
        "BTIC_SPAN" => eval_btic_set_op(name, args, |a, b| Some(uni_btic::set_ops::span(a, b))),
        "BTIC_GAP" => eval_btic_set_op(name, args, uni_btic::set_ops::gap),
        _ => Err(anyhow!("unknown BTIC function: {}", name)),
    }
}

/// Validate and extract raw BTIC fields (lo, hi, meta) from a 1-arg call.
/// Returns `Ok(None)` when the argument is null (caller should return `Value::Null`).
fn require_btic_1arg(name: &str, args: &[Value]) -> Result<Option<(i64, i64, u64)>> {
    if args.len() != 1 {
        return Err(anyhow!("{} requires 1 argument", name.to_lowercase()));
    }
    match &args[0] {
        Value::Temporal(TemporalValue::Btic { lo, hi, meta }) => Ok(Some((*lo, *hi, *meta))),
        Value::Null => Ok(None),
        _ => Err(anyhow!("{}: expected BTIC value", name.to_lowercase())),
    }
}

/// Validate and construct a parsed `Btic` from a 1-arg call.
/// Returns `Ok(None)` when the argument is null (caller should return `Value::Null`).
fn require_btic_1arg_parsed(name: &str, args: &[Value]) -> Result<Option<uni_btic::Btic>> {
    let Some((lo, hi, meta)) = require_btic_1arg(name, args)? else {
        return Ok(None);
    };
    uni_btic::Btic::new(lo, hi, meta)
        .map(Some)
        .map_err(|e| anyhow!("invalid BTIC: {}", e))
}

/// Convert milliseconds since epoch to a UTC DateTime value.
fn ms_to_utc_datetime(ms: i64) -> Value {
    Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: ms * 1_000_000,
        offset_seconds: 0,
        timezone_name: Some("UTC".to_string()),
    })
}

/// Evaluate a 2-arg BTIC predicate that takes (Btic, Btic) -> Bool.
fn eval_btic_binary_predicate(name: &str, args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("{} requires 2 arguments", name.to_lowercase()));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (
            Value::Temporal(TemporalValue::Btic {
                lo: a_lo, hi: a_hi, ..
            }),
            Value::Temporal(TemporalValue::Btic {
                lo: b_lo, hi: b_hi, ..
            }),
        ) => {
            let result = match name {
                "BTIC_OVERLAPS" => *a_lo < *b_hi && *b_lo < *a_hi,
                "BTIC_CONTAINS" => *a_lo <= *b_lo && *b_hi <= *a_hi,
                "BTIC_BEFORE" => *a_hi <= *b_lo,
                "BTIC_AFTER" => *b_hi <= *a_lo,
                "BTIC_MEETS" => *a_hi == *b_lo,
                "BTIC_ADJACENT" => *a_hi == *b_lo || *b_hi == *a_lo,
                "BTIC_DISJOINT" => *a_hi <= *b_lo || *b_hi <= *a_lo,
                "BTIC_EQUALS" => *a_lo == *b_lo && *a_hi == *b_hi,
                "BTIC_STARTS" => *a_lo == *b_lo && *a_hi < *b_hi,
                "BTIC_DURING" => *b_lo < *a_lo && *a_hi < *b_hi,
                "BTIC_FINISHES" => *a_hi == *b_hi && *b_lo < *a_lo,
                _ => unreachable!(),
            };
            Ok(Value::Bool(result))
        }
        _ => Err(anyhow!(
            "{}: both args must be BTIC values",
            name.to_lowercase()
        )),
    }
}

/// Evaluate a 2-arg BTIC set operation: (Btic, Btic) -> Btic or Null.
///
/// The `op` closure receives two parsed `Btic` values and returns `Some(result)`
/// for a valid result or `None` to produce `Value::Null`.
fn eval_btic_set_op<F>(name: &str, args: &[Value], op: F) -> Result<Value>
where
    F: FnOnce(&uni_btic::Btic, &uni_btic::Btic) -> Option<uni_btic::Btic>,
{
    if args.len() != 2 {
        return Err(anyhow!("{} requires 2 arguments", name.to_lowercase()));
    }
    if args[0].is_null() || args[1].is_null() {
        return Ok(Value::Null);
    }
    match (&args[0], &args[1]) {
        (
            Value::Temporal(TemporalValue::Btic {
                lo: a_lo,
                hi: a_hi,
                meta: a_meta,
            }),
            Value::Temporal(TemporalValue::Btic {
                lo: b_lo,
                hi: b_hi,
                meta: b_meta,
            }),
        ) => {
            let a = uni_btic::Btic::new(*a_lo, *a_hi, *a_meta)
                .map_err(|e| anyhow!("invalid BTIC: {}", e))?;
            let b = uni_btic::Btic::new(*b_lo, *b_hi, *b_meta)
                .map_err(|e| anyhow!("invalid BTIC: {}", e))?;
            match op(&a, &b) {
                Some(r) => Ok(Value::Temporal(TemporalValue::Btic {
                    lo: r.lo(),
                    hi: r.hi(),
                    meta: r.meta(),
                })),
                None => Ok(Value::Null),
            }
        }
        _ => Err(anyhow!(
            "{}: both args must be BTIC values",
            name.to_lowercase()
        )),
    }
}

/// Extract a point in time as milliseconds since epoch from a Value.
fn extract_point_ms(val: &Value) -> Result<i64> {
    match val {
        Value::Int(ms) => Ok(*ms),
        Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch, ..
        }) => Ok(*nanos_since_epoch / 1_000_000),
        Value::Temporal(TemporalValue::LocalDateTime {
            nanos_since_epoch, ..
        }) => Ok(*nanos_since_epoch / 1_000_000),
        Value::Temporal(TemporalValue::Date { days_since_epoch }) => {
            Ok(*days_since_epoch as i64 * 86_400_000)
        }
        _ => Err(anyhow!(
            "expected a temporal point value (datetime, integer ms), got {:?}",
            val
        )),
    }
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
    fn test_vector_distance_l1() {
        let a = Value::List(vec![Value::Float(0.0), Value::Float(0.0)]);
        let b = Value::List(vec![Value::Float(3.0), Value::Float(4.0)]);
        // Manhattan: |0-3| + |0-4| = 7 (vs. 5 under l2).
        let d = eval_vector_distance(&a, &b, "l1")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((d - 7.0).abs() < 1e-9, "l1 distance = 7, got {d}");
        // `manhattan` is an accepted alias.
        let d2 = eval_vector_distance(&a, &b, "manhattan")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((d2 - 7.0).abs() < 1e-9);
    }

    #[test]
    fn test_vector_distance_binary_hamming_jaccard() {
        // BinaryVector operands: Hamming = differing bits.
        let a = Value::BinaryVector(vec![0x00, 0xA5]);
        let b = Value::BinaryVector(vec![0xFF, 0xA5]);
        let h = eval_vector_distance(&a, &b, "hamming")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((h - 8.0).abs() < 1e-9, "hamming = 8, got {h}");

        // Jaccard on 0b1100 vs 0b1010 = 1 − 1/3 = 2/3.
        let a2 = Value::BinaryVector(vec![0b1100]);
        let b2 = Value::BinaryVector(vec![0b1010]);
        let j = eval_vector_distance(&a2, &b2, "jaccard")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((j - 2.0 / 3.0).abs() < 1e-9, "jaccard = 2/3, got {j}");

        // A list of byte-ints works as the literal operand form too.
        let la = Value::List(vec![Value::Int(0)]);
        let lb = Value::List(vec![Value::Int(255)]);
        let h2 = eval_vector_distance(&la, &lb, "hamming")
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((h2 - 8.0).abs() < 1e-9);

        // Non-byte operands are rejected for binary metrics.
        let bad = Value::List(vec![Value::Float(0.5)]);
        assert!(eval_vector_distance(&bad, &bad, "hamming").is_err());
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
            eval_scalar_function("SIZE", &[Value::List(vec![i(1), i(2), i(3)])], None).unwrap(),
            Value::Int(3)
        );
    }

    #[test]
    fn test_scalar_function_head() {
        assert_eq!(
            eval_scalar_function("HEAD", &[Value::List(vec![i(1), i(2), i(3)])], None).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn test_scalar_function_coalesce() {
        assert_eq!(
            eval_scalar_function(
                "COALESCE",
                &[Value::Null, Value::Int(1), Value::Int(2)],
                None
            )
            .unwrap(),
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

    // ========================================================================
    // Regression tests for verified bug #19: interpreted-path integer
    // arithmetic uses naive/f64-routed operations that overflow, lose
    // precision, or yield NaN instead of returning typed/checked errors.
    // These tests are RED on today's code and would pass once the arithmetic
    // is made checked and integer-typed.
    // ========================================================================

    /// Bug #19(a): integer `+` overflow must be an error, not a panic/wrap.
    ///
    /// `eval_add` computes `left.as_i64() + right.as_i64()` directly, so
    /// `i64::MAX + 1` panics in debug builds and wraps in release builds. A
    /// checked addition should surface this as an `Err`.
    #[test]
    fn test_int_add_overflow_is_error() {
        let result = eval_binary_op(&i(i64::MAX), &BinaryOp::Add, &i(1));
        assert!(
            result.is_err(),
            "expected i64::MAX + 1 to be an arithmetic error, got {result:?}"
        );
    }

    /// Bug #19(b): integer `*` must stay exact above 2^53.
    ///
    /// `eval_mul` routes integers through `eval_numeric_op`, which casts both
    /// operands to f64, losing precision beyond 2^53. `(2^53 + 1) * 1` should
    /// round-trip exactly as an integer.
    #[test]
    fn test_int_mul_precision_above_2pow53() {
        let big = (1i64 << 53) + 1;
        let result = eval_binary_op(&i(big), &BinaryOp::Mul, &i(1)).unwrap();
        assert_eq!(
            result,
            Value::Int(big),
            "expected exact integer multiplication, got {result:?}"
        );
    }

    /// Bug #19(c): integer `%` by zero must be a division-by-zero error.
    ///
    /// `BinaryOp::Mod` routes through `eval_numeric_op`, computing `1.0 % 0.0`
    /// which yields NaN and returns `Value::Float(NaN)` rather than erroring.
    #[test]
    fn test_int_mod_zero_is_error() {
        let result = eval_binary_op(&i(1), &BinaryOp::Mod, &i(0));
        assert!(
            result.is_err(),
            "expected 1 % 0 to be a division-by-zero error, got {result:?}"
        );
    }

    /// Bug #19(c): integer `%` must stay exact above 2^53.
    ///
    /// `BinaryOp::Mod` routes through `eval_numeric_op` (f64 cast), losing
    /// precision beyond 2^53. `((2^53 + 3) % 10)` should match the exact
    /// integer remainder.
    #[test]
    fn test_int_mod_precision_above_2pow53() {
        let big = (1i64 << 53) + 3;
        let result = eval_binary_op(&i(big), &BinaryOp::Mod, &i(10)).unwrap();
        assert_eq!(
            result,
            Value::Int(big % 10),
            "expected exact integer remainder, got {result:?}"
        );
    }

    /// Bug #19(d): `substring` with a negative start must not panic.
    ///
    /// `eval_substring` casts a negative start via `as usize` (→ `usize::MAX`),
    /// then `start + len` overflows and panics in debug builds. It should
    /// clamp or error, never panic.
    #[test]
    fn test_substring_negative_start_no_panic() {
        let result = eval_scalar_function("substring", &[s("hello"), i(-1), i(5)], None);
        // We only assert the absence of a panic; either Ok or Err is acceptable.
        let _ = result.is_ok();
    }

    /// Bug #19(e): `range` near `i64::MAX` must terminate, not panic.
    ///
    /// `eval_range_function` advances with naive `i += step`, so a range whose
    /// final increment crosses `i64::MAX` overflows and panics in debug
    /// builds. It should terminate cleanly.
    #[test]
    fn test_range_near_i64_max_no_panic() {
        let result = eval_scalar_function("range", &[i(i64::MAX - 1), i(i64::MAX)], None);
        // We only assert the absence of a panic; either Ok or Err is acceptable.
        let _ = result.is_ok();
    }
}
