// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher's `sum` and `avg`: one running total, used by every evaluator.
//!
//! Both take numbers or durations (Neo4j). NULL is skipped, a sum over
//! nothing is 0, and an average over nothing is NULL. Integers stay integers
//! until one overflows, when the total continues as a float. Mixing numbers
//! and durations, or anything else (a string, a list, a node), is an error:
//! the DataFusion aggregate, the row executor, Locy's `QUERY` aggregates and
//! Locy's FOLD `SUM` / `AVG` each used to skip such a value, so `sum` over
//! strings or durations answered 0 and `avg` answered NULL.

// Rust guideline compliant

use crate::Value;
use crate::value::TemporalValue;
use anyhow::{Result, anyhow};

/// A running Cypher `sum`, also the numerator of `avg`.
///
/// # Examples
///
/// ```
/// use uni_common::Value;
/// use uni_common::cypher_sum::CypherSum;
///
/// let mut total = CypherSum::default();
/// total.add(&Value::Int(2)).unwrap();
/// total.add(&Value::Null).unwrap();
/// total.add(&Value::Int(3)).unwrap();
/// assert_eq!(total.sum(), Value::Int(5));
/// assert!(total.add(&Value::String("x".into())).is_err());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum CypherSum {
    /// No value added yet.
    #[default]
    Empty,
    /// Every value so far was an integer, and their sum fits.
    Int(i64),
    /// A float was added, or the integer sum overflowed.
    Float(f64),
    /// Every value so far was a duration.
    Duration {
        /// Total months.
        months: i64,
        /// Total days.
        days: i64,
        /// Total nanoseconds.
        nanos: i64,
    },
}

impl CypherSum {
    /// Adds one value.
    ///
    /// # Errors
    ///
    /// When `value` is neither NULL, a number nor a duration, when it mixes
    /// numbers and durations, or when a duration component overflows.
    pub fn add(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Null => Ok(()),
            Value::Int(i) => self.add_int(*i),
            Value::Float(f) => self.add_float(*f),
            Value::Temporal(TemporalValue::Duration {
                months,
                days,
                nanos,
            }) => self.add_duration(*months, *days, *nanos),
            other => Err(anyhow!(
                "sum() and avg() take numbers or durations, got {}",
                describe(other)
            )),
        }
    }

    /// Adds an integer.
    ///
    /// # Errors
    ///
    /// When the running total is a duration.
    #[inline]
    pub fn add_int(&mut self, i: i64) -> Result<()> {
        *self = match *self {
            Self::Empty => Self::Int(i),
            // On overflow the exact sum is rounded once, through i128: adding
            // the two as floats rounded each operand first, and could land a
            // unit in the last place away from the nearest float.
            Self::Int(total) => total.checked_add(i).map_or_else(
                || Self::Float((total as i128 + i as i128) as f64),
                Self::Int,
            ),
            Self::Float(total) => Self::Float(total + i as f64),
            Self::Duration { .. } => return Err(mixed()),
        };
        Ok(())
    }

    /// Adds a float.
    ///
    /// # Errors
    ///
    /// When the running total is a duration.
    #[inline]
    pub fn add_float(&mut self, f: f64) -> Result<()> {
        *self = match *self {
            Self::Empty => Self::Float(f),
            Self::Int(total) => Self::Float(total as f64 + f),
            Self::Float(total) => Self::Float(total + f),
            Self::Duration { .. } => return Err(mixed()),
        };
        Ok(())
    }

    /// Adds a duration, component by component.
    ///
    /// # Errors
    ///
    /// When the running total is a number, or a component overflows.
    pub fn add_duration(&mut self, months: i64, days: i64, nanos: i64) -> Result<()> {
        *self = match *self {
            Self::Empty => Self::Duration {
                months,
                days,
                nanos,
            },
            Self::Duration {
                months: m,
                days: d,
                nanos: n,
            } => {
                let overflow = || anyhow!("sum() of durations overflows");
                Self::Duration {
                    months: m.checked_add(months).ok_or_else(overflow)?,
                    days: d.checked_add(days).ok_or_else(overflow)?,
                    nanos: n.checked_add(nanos).ok_or_else(overflow)?,
                }
            }
            Self::Int(_) | Self::Float(_) => return Err(mixed()),
        };
        Ok(())
    }

    /// Adds another running total, as a partial aggregate merges.
    ///
    /// # Errors
    ///
    /// As [`CypherSum::add`].
    pub fn merge(&mut self, other: &CypherSum) -> Result<()> {
        match *other {
            Self::Empty => Ok(()),
            Self::Int(i) => self.add_int(i),
            Self::Float(f) => self.add_float(f),
            Self::Duration {
                months,
                days,
                nanos,
            } => self.add_duration(months, days, nanos),
        }
    }

    /// The sum: `Int(0)` over nothing.
    #[must_use]
    pub fn sum(&self) -> Value {
        match *self {
            Self::Empty => Value::Int(0),
            Self::Int(i) => Value::Int(i),
            Self::Float(f) => Value::Float(f),
            Self::Duration {
                months,
                days,
                nanos,
            } => duration(months, days, nanos),
        }
    }

    /// The average over `count` values: NULL over nothing, a float for
    /// numbers, a duration for durations (divided as `duration / n` is).
    #[must_use]
    pub fn avg(&self, count: u64) -> Value {
        if count == 0 {
            return Value::Null;
        }
        match *self {
            Self::Empty => Value::Null,
            Self::Int(i) => Value::Float(i as f64 / count as f64),
            Self::Float(f) => Value::Float(f / count as f64),
            Self::Duration {
                months,
                days,
                nanos,
            } => {
                let (months, days, nanos) =
                    crate::datetime::scale_duration(months, days, nanos, 1.0 / count as f64);
                duration(months, days, nanos)
            }
        }
    }

    /// The running total as a value, for a partial aggregate's state: NULL
    /// when empty.
    #[must_use]
    pub fn to_state(&self) -> Value {
        match self {
            Self::Empty => Value::Null,
            _ => self.sum(),
        }
    }

    /// Reads back a [`CypherSum::to_state`] value.
    ///
    /// # Errors
    ///
    /// When `state` is not one [`CypherSum::to_state`] produces.
    pub fn from_state(state: &Value) -> Result<CypherSum> {
        let mut total = CypherSum::Empty;
        total.add(state)?;
        Ok(total)
    }
}

fn duration(months: i64, days: i64, nanos: i64) -> Value {
    Value::Temporal(TemporalValue::Duration {
        months,
        days,
        nanos,
    })
}

fn mixed() -> anyhow::Error {
    anyhow!("sum() and avg() cannot mix numbers and durations")
}

/// The value as an error message names it: `the string "x"`, `a duration`.
#[must_use]
pub fn describe(value: &Value) -> String {
    match value {
        Value::String(s) => format!("the string {s:?}"),
        Value::Temporal(TemporalValue::Duration { .. }) => "a duration".to_string(),
        Value::Temporal(_) => "a temporal value that is not a duration".to_string(),
        other => format!("a {}", other.type_name()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // An exact reference value: `powi` has unspecified precision, including in Miri.
    const TWO_POW_63: f64 = (1_u64 << 63) as f64;

    fn total(values: &[Value]) -> Result<CypherSum> {
        let mut t = CypherSum::default();
        for v in values {
            t.add(v)?;
        }
        Ok(t)
    }

    #[test]
    fn numbers_sum_as_cypher_does() {
        assert_eq!(total(&[]).unwrap().sum(), Value::Int(0));
        assert_eq!(total(&[Value::Null]).unwrap().sum(), Value::Int(0));
        assert_eq!(
            total(&[Value::Int(1), Value::Int(2)]).unwrap().sum(),
            Value::Int(3)
        );
        assert_eq!(
            total(&[Value::Int(1), Value::Float(0.5)]).unwrap().sum(),
            Value::Float(1.5)
        );
        assert_eq!(
            total(&[Value::Int(i64::MAX), Value::Int(1)]).unwrap().sum(),
            Value::Float(i64::MAX as f64 + 1.0)
        );
    }

    /// Integers stay exact up to the end of the 64-bit range, past 2^53 where
    /// a float sum rounds, and only an overflow continues as a float.
    #[test]
    fn integers_are_exact_until_they_overflow() {
        let two_53 = 1_i64 << 53;
        assert_eq!(
            total(&[Value::Int(two_53), Value::Int(1)]).unwrap().sum(),
            Value::Int(two_53 + 1)
        );
        assert_eq!(
            total(&[Value::Int(i64::MAX - 1), Value::Int(1)])
                .unwrap()
                .sum(),
            Value::Int(i64::MAX)
        );
        assert_eq!(
            total(&[Value::Int(i64::MIN), Value::Int(-1)])
                .unwrap()
                .sum(),
            Value::Float(-TWO_POW_63)
        );
        // Once a float, a float, even back in range.
        assert_eq!(
            total(&[Value::Int(i64::MAX), Value::Int(1), Value::Int(-1)])
                .unwrap()
                .sum(),
            Value::Float(TWO_POW_63)
        );
    }

    /// The overflowing sum is the exact sum rounded once. Rounding each
    /// operand first turned `(2^62 + 1) + (2^62 + 1025)` into
    /// `2^62 + (2^62 + 1024)`, a tie that rounds to `2^63`; the exact
    /// `2^63 + 1026` is nearest to `2^63 + 2048`.
    #[test]
    fn an_overflowing_sum_is_rounded_once() {
        let a = (1_i64 << 62) + 1;
        let b = (1_i64 << 62) + 1025;
        let exact = TWO_POW_63 + 2048.0;
        // Keep the witness sensitive to rounding the operands before adding.
        assert_ne!(a as f64 + b as f64, exact);
        assert_eq!(
            total(&[Value::Int(a), Value::Int(b)]).unwrap().sum(),
            Value::Float(exact)
        );
    }

    #[test]
    fn durations_sum_and_average() {
        let t = total(&[duration(1, 2, 3), Value::Null, duration(1, 0, 5)]).unwrap();
        assert_eq!(t.sum(), duration(2, 2, 8));
        assert_eq!(t.avg(2), duration(1, 1, 4));
    }

    #[test]
    fn anything_else_is_an_error() {
        assert!(total(&[Value::String("12".into())]).is_err());
        assert!(total(&[Value::Bool(true)]).is_err());
        assert!(total(&[Value::Int(1), duration(0, 1, 0)]).is_err());
        assert!(total(&[duration(0, 1, 0), Value::Float(1.0)]).is_err());
    }

    #[test]
    fn average_over_nothing_is_null() {
        assert_eq!(CypherSum::default().avg(0), Value::Null);
        assert_eq!(total(&[Value::Int(3)]).unwrap().avg(2), Value::Float(1.5));
    }

    #[test]
    fn state_round_trips() {
        for t in [
            CypherSum::Empty,
            CypherSum::Int(4),
            CypherSum::Float(0.5),
            CypherSum::Duration {
                months: 1,
                days: 2,
                nanos: 3,
            },
        ] {
            assert_eq!(CypherSum::from_state(&t.to_state()).unwrap(), t);
        }
    }
}
