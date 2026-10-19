// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `percentileDisc` and `percentileCont` over exact numbers.
//!
//! One definition for the DataFusion aggregates and the fallback executor's
//! accumulators, so the two paths cannot disagree. The inputs are kept as
//! the numbers they are ([`Value::Int`] or [`Value::Float`]), never widened
//! to `f64` first:
//!
//! - `percentileDisc` is a discrete percentile, so its answer is one of the
//!   inputs, with its type and its exact value. Read through `f64`, an
//!   integer input answered a float, and `2^53 + 1` came back `2^53`.
//! - `percentileCont` answers a float, interpolated between the two exact
//!   inputs around the percentile. Two integers are interpolated in integer
//!   arithmetic as far as it goes, so the result is rounded once.
//!
//! Both sort by exact value ([`number_order`]) and place percentile `p` at
//! position `p * (n - 1)`; `percentileDisc` rounds that position to the
//! nearest index.

use std::cmp::Ordering;

use uni_common::Value;

/// The `NumberOutOfRange` message for a percentile outside `[0, 1]`.
fn out_of_range(function: &str) -> String {
    format!(
        "ArgumentError: NumberOutOfRange - {function}(): percentile value must be between 0.0 and 1.0"
    )
}

/// Reads a percentile argument.
///
/// Returns `Ok(None)` for NULL, which leaves the percentile to a later row.
///
/// # Errors
///
/// The `NumberOutOfRange` message when the number is outside `[0, 1]` (or is
/// NaN), and a type error when the argument is not a number.
///
/// # Examples
///
/// ```
/// use uni_common::Value;
/// use uni_query_functions::percentile::read_percentile;
///
/// assert_eq!(read_percentile(&Value::Int(1), "percentileDisc"), Ok(Some(1.0)));
/// assert_eq!(read_percentile(&Value::Null, "percentileDisc"), Ok(None));
/// assert!(read_percentile(&Value::Float(1.1), "percentileDisc").is_err());
/// assert!(read_percentile(&Value::String("x".into()), "percentileCont").is_err());
/// ```
pub fn read_percentile(p: &Value, function: &str) -> Result<Option<f64>, String> {
    let p = match p {
        Value::Null => return Ok(None),
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        other => {
            return Err(format!(
                "{function}(): the percentile must be a number, got {}",
                crate::cypher_sum::describe(other)
            ));
        }
    };
    if (0.0..=1.0).contains(&p) {
        Ok(Some(p))
    } else {
        Err(out_of_range(function))
    }
}

/// Reads one input of a percentile aggregate.
///
/// Returns `Ok(None)` for NULL, which every aggregate skips.
///
/// # Errors
///
/// A type error when the value is not a number.
pub fn read_input(v: &Value, function: &str) -> Result<Option<Value>, String> {
    match v {
        Value::Null => Ok(None),
        Value::Int(_) | Value::Float(_) => Ok(Some(v.clone())),
        other => Err(format!(
            "{function}() takes numbers, got {}",
            crate::cypher_sum::describe(other)
        )),
    }
}

/// Orders two numbers by exact value, NaN above every other number as
/// `ORDER BY` places it. A value that is not a number orders as equal; the
/// readers above admit none.
///
/// # Examples
///
/// ```
/// use std::cmp::Ordering;
/// use uni_common::Value;
/// use uni_query_functions::percentile::number_order;
///
/// let big = Value::Int((1 << 53) + 1);
/// assert_eq!(number_order(&Value::Float(9007199254740992.0), &big), Ordering::Less);
/// assert_eq!(number_order(&Value::Float(f64::NAN), &Value::Int(i64::MAX)), Ordering::Greater);
/// ```
#[must_use]
pub fn number_order(a: &Value, b: &Value) -> Ordering {
    let is_nan = |v: &Value| matches!(v, Value::Float(f) if f.is_nan());
    match crate::expr_eval::cypher_numeric_cmp(a, b) {
        Some(Some(ord)) => ord,
        Some(None) => is_nan(a).cmp(&is_nan(b)),
        None => Ordering::Equal,
    }
}

/// `percentileDisc(x, p)` over `values`, which it sorts in place: the input
/// at index `round(p * (n - 1))`, unchanged. NULL when there are none.
///
/// Equal inputs keep their arrival order (the sort is stable), so of `1` and
/// `1.0` the one seen first is the one answered.
///
/// # Examples
///
/// ```
/// use uni_common::Value;
/// use uni_query_functions::percentile::percentile_disc;
///
/// let mut v = vec![Value::Int(3), Value::Int((1 << 53) + 1), Value::Int(1)];
/// assert_eq!(percentile_disc(&mut v, 1.0), Value::Int((1 << 53) + 1));
/// assert_eq!(percentile_disc(&mut v, 0.5), Value::Int(3));
/// assert_eq!(percentile_disc(&mut [], 0.5), Value::Null);
/// ```
pub fn percentile_disc(values: &mut [Value], p: f64) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(number_order);
    let n = values.len();
    let idx = (p * (n as f64 - 1.0)).round() as usize;
    values[idx.min(n - 1)].clone()
}

/// `percentileCont(x, p)` over `values`, which it sorts in place: the float
/// at position `p * (n - 1)`, interpolated linearly between the inputs on
/// either side. NULL when there are none.
///
/// # Examples
///
/// ```
/// use uni_common::Value;
/// use uni_query_functions::percentile::percentile_cont;
///
/// let mut v = vec![Value::Float(30.0), Value::Float(10.0), Value::Float(20.0)];
/// assert_eq!(percentile_cont(&mut v, 0.25), Value::Float(15.0));
/// // Halfway between 2^53 + 1 and 2^53 + 5 is 2^53 + 3, rounded once.
/// let two_53 = 1_i64 << 53;
/// let mut v = vec![Value::Int(two_53 + 1), Value::Int(two_53 + 5)];
/// assert_eq!(percentile_cont(&mut v, 0.5), Value::Float((two_53 + 3) as f64));
/// ```
pub fn percentile_cont(values: &mut [Value], p: f64) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(number_order);
    let n = values.len();
    let pos = p * (n as f64 - 1.0);
    let lower = (pos.floor() as usize).min(n - 1);
    let upper = (pos.ceil() as usize).min(n - 1);
    if lower == upper {
        return Value::Float(as_f64(&values[lower]));
    }
    Value::Float(interpolate(
        &values[lower],
        &values[upper],
        pos - lower as f64,
    ))
}

fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => f64::NAN,
    }
}

/// `lo + frac * (hi - lo)`. For two integers the step is split into its
/// whole part, added in integer arithmetic, and its fraction, so the sum is
/// rounded once rather than after each input was rounded to `f64`.
fn interpolate(lo: &Value, hi: &Value, frac: f64) -> f64 {
    if let (Value::Int(a), Value::Int(b)) = (lo, hi) {
        let step = frac * (i128::from(*b) - i128::from(*a)) as f64;
        let whole = step.floor();
        // `0 <= whole <= b - a`, so `a + whole` is within `[a, b]`.
        let base = i128::from(*a) + whole as i128;
        return base as f64 + (step - whole);
    }
    let (a, b) = (as_f64(lo), as_f64(hi));
    a + frac * (b - a)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_integers_interpolate_as_before() {
        for (a, b, frac) in [(1_i64, 2, 0.5), (10, 30, 0.25), (-7, 5, 0.3), (0, 3, 0.9)] {
            let want = a as f64 + frac * (b - a) as f64;
            assert_eq!(interpolate(&Value::Int(a), &Value::Int(b), frac), want);
        }
    }

    #[test]
    fn nan_sorts_last_and_types_are_kept() {
        let mut v = vec![
            Value::Float(f64::NAN),
            Value::Int(2),
            Value::Float(1.5),
            Value::Int(-1),
        ];
        assert_eq!(percentile_disc(&mut v, 0.0), Value::Int(-1));
        assert_eq!(percentile_disc(&mut v, 0.34), Value::Float(1.5));
        assert_eq!(percentile_disc(&mut v, 0.67), Value::Int(2));
        assert!(matches!(percentile_disc(&mut v, 1.0), Value::Float(f) if f.is_nan()));
    }

    #[test]
    fn readers_reject_what_is_not_a_number() {
        assert!(read_input(&Value::String("a".into()), "percentileDisc").is_err());
        assert_eq!(read_input(&Value::Null, "percentileDisc"), Ok(None));
        assert_eq!(
            read_percentile(&Value::Float(0.0), "percentileDisc"),
            Ok(Some(0.0))
        );
        assert!(read_percentile(&Value::Float(f64::NAN), "percentileDisc").is_err());
        assert!(read_percentile(&Value::Int(-1), "percentileDisc").is_err());
    }
}
