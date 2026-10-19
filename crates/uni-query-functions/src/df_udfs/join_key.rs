// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Hash-join bucket keys for Cypher values.
//!
//! [`encode_cypher_join_key`] maps a [`uni_common::Value`] to a byte string
//! used as a hash-join key when the two sides of an equi-join do not share a
//! natively comparable Arrow type (a `CypherValue` column against a string,
//! a float against an integer, and so on).
//!
//! The key is a *necessary* condition for equality, not a sufficient one:
//! whenever two values compare equal, their keys are equal. Two unequal values
//! may share a key, so the join that uses it must re-check the original
//! equality on every candidate pair. Above 2^53 an integer is keyed through
//! the float it rounds to, so `2^53 + 1` and `2^53 (float)` share a bucket
//! although `cypher_eq` compares them exactly and finds them unequal; the
//! re-check separates them, and the join answers exactly what a filter over
//! the cross product answers.
//!
//! The only key that is `None` is the one for `null`, which equals nothing.

use super::*;

/// Integers and integral floats below this magnitude are encoded exactly.
///
/// Every `i64` in `(-2^53, 2^53)` converts to `f64` without rounding, so in
/// that range an integer equals a float exactly when the float is integral and
/// has the same value. Above it, several integers round to the same float and
/// both are encoded through `f64`, merging them into one bucket.
const EXACT_INT_LIMIT: f64 = 9_007_199_254_740_992.0; // 2^53

const TAG_NULL: u8 = 0x00;
const TAG_BOOL: u8 = 0x01;
const TAG_INT: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_VERTEX: u8 = 0x05;
const TAG_EDGE: u8 = 0x06;
const TAG_LIST: u8 = 0x07;
const TAG_MAP: u8 = 0x08;
const TAG_INSTANT: u8 = 0x09;
const TAG_DURATION: u8 = 0x0A;
const TAG_BTIC: u8 = 0x0B;
const TAG_BYTES: u8 = 0x0C;
const TAG_OTHER: u8 = 0x0D;

/// Encode `value` as a hash-join bucket key, or `None` for `null`.
///
/// Equal values (under `cypher_eq`, and under Arrow's native equality for the
/// same value) always produce equal keys. Unequal values may collide, so a
/// join keyed on this must re-check the original predicate. See the module
/// documentation for why the key is deliberately coarse.
pub fn encode_cypher_join_key(value: &Value) -> Option<Vec<u8>> {
    if value.is_null() {
        return None;
    }
    let mut buf = Vec::with_capacity(16);
    encode_to_buf(value, &mut buf);
    Some(buf)
}

/// Encode an integer: exact below 2^53, through `f64` above it.
fn push_int(i: i64, buf: &mut Vec<u8>) {
    let f = i as f64;
    if f.abs() < EXACT_INT_LIMIT {
        buf.push(TAG_INT);
        buf.extend_from_slice(&i.to_be_bytes());
    } else {
        buf.push(TAG_FLOAT);
        buf.extend_from_slice(&f.to_bits().to_be_bytes());
    }
}

/// Encode a float so it lands in the same bucket as every number it equals.
///
/// An integral float below 2^53 shares the integer encoding (this also folds
/// `-0.0` into `0`). Every NaN shares one bucket: Cypher says `NaN = NaN` is
/// false, Arrow's native comparison says it is true, and the re-check decides.
fn push_float(f: f64, buf: &mut Vec<u8>) {
    if f.is_nan() {
        buf.push(TAG_FLOAT);
        buf.extend_from_slice(&f64::NAN.to_bits().to_be_bytes());
    } else if f.fract() == 0.0 && f.abs() < EXACT_INT_LIMIT {
        // In range and integral, so the conversion is exact.
        buf.push(TAG_INT);
        buf.extend_from_slice(&(f as i64).to_be_bytes());
    } else {
        buf.push(TAG_FLOAT);
        buf.extend_from_slice(&f.to_bits().to_be_bytes());
    }
}

/// Append a length prefix so nested encodings stay self-delimiting.
fn push_len(len: usize, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(len as u64).to_be_bytes());
}

/// Encode a temporal value by the quantity its equality is decided on.
///
/// Points in time are normalized to UTC nanoseconds, so a key never separates
/// two values that compare equal as instants, while values that differ only in
/// offset or zone name share a bucket and are told apart by the re-check.
fn push_temporal(t: &uni_common::TemporalValue, buf: &mut Vec<u8>) {
    use uni_common::TemporalValue as T;
    const NANOS_PER_DAY: i128 = 86_400 * 1_000_000_000;
    const NANOS_PER_SECOND: i128 = 1_000_000_000;
    let instant = |nanos: i128, buf: &mut Vec<u8>| {
        buf.push(TAG_INSTANT);
        buf.extend_from_slice(&nanos.to_be_bytes());
    };
    match t {
        T::Date { days_since_epoch } => instant(i128::from(*days_since_epoch) * NANOS_PER_DAY, buf),
        T::LocalTime {
            nanos_since_midnight,
        } => instant(i128::from(*nanos_since_midnight), buf),
        T::Time {
            nanos_since_midnight,
            offset_seconds,
        } => instant(
            i128::from(*nanos_since_midnight) - i128::from(*offset_seconds) * NANOS_PER_SECOND,
            buf,
        ),
        T::LocalDateTime { nanos_since_epoch }
        | T::DateTime {
            nanos_since_epoch, ..
        } => instant(i128::from(*nanos_since_epoch), buf),
        T::Duration {
            months,
            days,
            nanos,
        } => {
            buf.push(TAG_DURATION);
            buf.extend_from_slice(&months.to_be_bytes());
            buf.extend_from_slice(&days.to_be_bytes());
            buf.extend_from_slice(&nanos.to_be_bytes());
        }
        T::Btic { lo, hi, meta } => {
            buf.push(TAG_BTIC);
            buf.extend_from_slice(&lo.to_be_bytes());
            buf.extend_from_slice(&hi.to_be_bytes());
            buf.extend_from_slice(&meta.to_be_bytes());
        }
    }
}

/// Recursive key encoder. A nested `null` is encoded rather than dropped:
/// `[null] = [null]` is null in Cypher, which the re-check enforces.
fn encode_to_buf(value: &Value, buf: &mut Vec<u8>) {
    // Entities compare by identity, whichever of their two encodings arrives.
    if let Some(entity) = value.entity_ref() {
        match entity {
            EntityRef::Vertex(vid) => {
                buf.push(TAG_VERTEX);
                buf.extend_from_slice(&vid.as_u64().to_be_bytes());
            }
            EntityRef::Edge(eid) => {
                buf.push(TAG_EDGE);
                buf.extend_from_slice(&eid.as_u64().to_be_bytes());
            }
        }
        return;
    }
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Bool(b) => {
            buf.push(TAG_BOOL);
            buf.push(u8::from(*b));
        }
        Value::Int(i) => push_int(*i, buf),
        Value::Float(f) => push_float(*f, buf),
        Value::String(s) => {
            buf.push(TAG_STRING);
            push_len(s.len(), buf);
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            buf.push(TAG_BYTES);
            push_len(b.len(), buf);
            buf.extend_from_slice(b);
        }
        Value::List(items) => {
            buf.push(TAG_LIST);
            push_len(items.len(), buf);
            for item in items {
                encode_to_buf(item, buf);
            }
        }
        Value::Map(map) => {
            buf.push(TAG_MAP);
            push_len(map.len(), buf);
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for (k, v) in entries {
                push_len(k.len(), buf);
                buf.extend_from_slice(k.as_bytes());
                encode_to_buf(v, buf);
            }
        }
        Value::Temporal(t) => push_temporal(t, buf),
        // Paths, vectors and anything added later share one bucket per kind.
        // Coarse, but never wrong: the re-check compares them in full.
        other => {
            buf.push(TAG_OTHER);
            buf.extend_from_slice(other.type_name().as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use uni_common::TemporalValue;

    /// A corpus spanning every kind the key distinguishes, including the
    /// numeric edge cases where Cypher equality stops being transitive.
    fn corpus() -> Vec<Value> {
        let big = 1_i64 << 53;
        let mut values = vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(0),
            Value::Int(1),
            Value::Int(-1),
            Value::Int(big - 1),
            Value::Int(big),
            Value::Int(big + 1),
            Value::Int(i64::MAX),
            Value::Int(i64::MIN),
            Value::Float(0.0),
            Value::Float(-0.0),
            Value::Float(1.0),
            Value::Float(1.5),
            Value::Float(-1.0),
            Value::Float((big - 1) as f64),
            Value::Float(big as f64),
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(9.3e18),
            Value::String(String::new()),
            Value::String("1".into()),
            Value::String("a".into()),
            Value::String("true".into()),
            Value::Bytes(vec![1, 2]),
            Value::List(vec![]),
            Value::List(vec![Value::Int(1)]),
            Value::List(vec![Value::Float(1.0)]),
            Value::List(vec![Value::Int(1), Value::Null]),
            Value::List(vec![Value::String("1".into())]),
            Value::Map(HashMap::from([("k".to_string(), Value::Int(1))])),
            Value::Map(HashMap::from([("k".to_string(), Value::Float(1.0))])),
            Value::Map(HashMap::from([
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::String("x".into())),
            ])),
            Value::Map(HashMap::from([("_vid".to_string(), Value::Int(7))])),
            Value::Temporal(TemporalValue::Date {
                days_since_epoch: 1,
            }),
            Value::Temporal(TemporalValue::LocalDateTime {
                nanos_since_epoch: 86_400_000_000_000,
            }),
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 5,
                offset_seconds: 0,
                timezone_name: None,
            }),
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 5,
                offset_seconds: 3600,
                timezone_name: Some("Europe/Paris".into()),
            }),
            Value::Temporal(TemporalValue::Duration {
                months: 1,
                days: 0,
                nanos: 0,
            }),
            Value::Vector(vec![1.0, 2.0]),
        ];
        // Mirror every scalar inside a one-element list, so nested encoding is
        // held to the same contract as the top level.
        let nested: Vec<Value> = values
            .iter()
            .map(|v| Value::List(vec![v.clone()]))
            .collect();
        values.extend(nested);
        values
    }

    /// The contract the hash join relies on: equal values never land in
    /// different buckets. A miss here is a silently dropped join row.
    #[test]
    fn equal_values_always_share_a_key() {
        let values = corpus();
        let mut checked = 0;
        for l in &values {
            for r in &values {
                if cypher_eq(l, r) == Some(true) {
                    checked += 1;
                    assert_eq!(
                        encode_cypher_join_key(l),
                        encode_cypher_join_key(r),
                        "{l:?} = {r:?} holds, so their join keys must match"
                    );
                }
            }
        }
        // Discrimination floor: the cross-type pairs (1 = 1.0, 2^53 + 1 = 2^53
        // as a float, nested numerics) must actually be exercised.
        assert!(checked > values.len(), "only {checked} equal pairs checked");
    }

    /// The re-check exists for pairs Cypher says are unequal but the key
    /// merges. These are the ones it must merge; anything else colliding would
    /// be a needless loss of selectivity, so pin the common distinctions.
    #[test]
    fn key_separates_ordinary_distinct_values() {
        let key = |v: Value| encode_cypher_join_key(&v);
        assert_ne!(key(Value::Int(1)), key(Value::String("1".into())));
        assert_ne!(key(Value::Int(1)), key(Value::Float(1.5)));
        assert_ne!(key(Value::Int(1)), key(Value::Bool(true)));
        assert_ne!(
            key(Value::String("a".into())),
            key(Value::String("b".into()))
        );
        assert_ne!(key(Value::Int(1)), key(Value::Int(2)));
        assert_eq!(key(Value::Int(1)), key(Value::Float(1.0)));
        assert_eq!(key(Value::Float(-0.0)), key(Value::Int(0)));
        assert_eq!(key(Value::Null), None);
    }

    /// Integers above 2^53 that round to the same float share a bucket with
    /// it although none of them is equal to another; the re-check is what
    /// keeps them apart.
    #[test]
    fn integers_that_round_to_one_float_share_a_bucket() {
        let big = 1_i64 << 53;
        assert_eq!(
            cypher_eq(&Value::Int(big + 1), &Value::Int(big)),
            Some(false)
        );
        // Exact: `2^53 + 1` is not the float `2^53`.
        assert_eq!(
            cypher_eq(&Value::Int(big + 1), &Value::Float(big as f64)),
            Some(false)
        );
        assert_eq!(
            cypher_eq(&Value::Int(big), &Value::Float(big as f64)),
            Some(true)
        );
        assert_eq!(
            encode_cypher_join_key(&Value::Int(big + 1)),
            encode_cypher_join_key(&Value::Float(big as f64))
        );
    }
}
