// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Key derivation for keyed scalar indexes.
//!
//! A keyed index ([`IndexKey`]) indexes a property whose own column cannot
//! take a Lance scalar index (a nested or opaque Arrow column) through flat
//! *key columns* derived from it. The columns are filled at batch build
//! (`VertexDataset::build_record_batch_with_timestamps`, through
//! [`crate::storage::vertex::derived_rows`]) and by the backfill an index
//! creation runs; they are never stored in L0 and never hashed into the
//! content UID.
//!
//! The key of a stored value must agree with how `WHERE` compares the stored
//! value, so that a bound on the key admits every row the predicate admits.
//! [`derive`] gives the key of a stored value; [`constant_key`] the key of a
//! query constant, which is stricter: a constant of another type is never
//! turned into a bound.

// Rust guideline compliant

use uni_common::core::schema::{IndexKey, KeyKind};
use uni_common::{TemporalValue, Value};

/// One key column's value.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyValue {
    /// An Int64 key.
    Int(i64),
    /// A Float64 key (`-0.0` normalized to `+0.0`).
    Float(f64),
    /// A string key.
    Str(String),
}

impl KeyValue {
    /// The value as stored in the key column's property slot.
    #[must_use]
    pub fn into_value(self) -> Value {
        match self {
            Self::Int(i) => Value::Int(i),
            Self::Float(f) => Value::Float(f),
            Self::Str(s) => Value::String(s),
        }
    }
}

/// The key values of a stored `value` for `kind`, one per key column; `None`
/// in a slot for a NULL or absent property, or a value the column stores as
/// NULL.
///
/// | Kind | From | Key |
/// |---|---|---|
/// | `DateTimeInstant` | a DATETIME | its `nanos_since_epoch` |
/// | | a `localdatetime` stored in the column (offset NULL) | its `nanos_since_epoch`: `WHERE` treats it as the instant |
/// | `BticBounds` | a BTIC, or a BTIC literal string | its raw `lo`, `hi` (ms; sentinels `i64::MIN` / `i64::MAX` kept) |
///
/// Kinds not implemented yet derive no key (every slot `None`); no index of
/// those kinds is created.
#[must_use]
pub fn derive(kind: &KeyKind, value: Option<&Value>) -> Vec<Option<KeyValue>> {
    let slots = kind.column_roles().len();
    let Some(value) = value else {
        return vec![None; slots];
    };
    match kind {
        KeyKind::DateTimeInstant => vec![match value {
            // The DATETIME column stores exactly these two shapes (see
            // `values_to_datetime_struct_array`); anything else is NULL there.
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch, ..
            })
            | Value::Temporal(TemporalValue::LocalDateTime { nanos_since_epoch }) => {
                Some(KeyValue::Int(*nanos_since_epoch))
            }
            _ => None,
        }],
        // The BTIC column stores a `Btic` value or a BTIC literal string
        // (see `build_btic_column`); anything else is NULL there. The bounds
        // are kept raw: an unbounded side is its sentinel, which every
        // interval predicate compares as is.
        KeyKind::BticBounds => match btic_bounds(value) {
            Some((lo, hi)) => vec![Some(KeyValue::Int(lo)), Some(KeyValue::Int(hi))],
            None => vec![None; slots],
        },
        _ => vec![None; slots],
    }
}

/// The raw bounds of a value a BTIC column stores: a `Btic`, or a string the
/// column parses as a BTIC literal. `None` for anything else, an invalid
/// interval, or a string that does not parse (which the column refuses).
fn btic_bounds(value: &Value) -> Option<(i64, i64)> {
    match value {
        Value::Temporal(TemporalValue::Btic { lo, hi, meta }) => {
            uni_btic::Btic::new(*lo, *hi, *meta)
                .ok()
                .map(|b| (b.lo(), b.hi()))
        }
        Value::String(s) => uni_btic::parse::parse_btic_literal(s)
            .ok()
            .map(|b| (b.lo(), b.hi())),
        _ => None,
    }
}

/// The key of a query constant compared with a property indexed by `kind`,
/// or `None` when the constant is not of the property's own type (a
/// cross-type comparison, which `WHERE` answers without the key) or is NULL.
///
/// A `localdatetime` constant against a DATETIME property is incomparable in
/// `WHERE`, so it has no key, unlike a stored `localdatetime` (see
/// [`derive`]). Likewise a BTIC literal *string* is stored as an interval but
/// is not one as a constant.
#[must_use]
pub fn constant_key(kind: &KeyKind, value: &Value) -> Option<Vec<KeyValue>> {
    match (kind, value) {
        (
            KeyKind::DateTimeInstant,
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch, ..
            }),
        ) => Some(vec![KeyValue::Int(*nanos_since_epoch)]),
        // A BTIC constant; a string is not one (the interval predicates
        // refuse it, and `=` compares it as another type).
        (KeyKind::BticBounds, Value::Temporal(TemporalValue::Btic { .. })) => {
            let (lo, hi) = btic_bounds(value)?;
            Some(vec![KeyValue::Int(lo), KeyValue::Int(hi)])
        }
        _ => None,
    }
}

/// Fill `row`'s key columns for `key` from the value of `property` in
/// `source`.
pub fn fill_key_columns(
    key: &IndexKey,
    property: &str,
    source: &uni_common::Properties,
    row: &mut uni_common::Properties,
) {
    let values = derive(&key.kind, source.get(property));
    for (column, value) in key.columns.iter().zip(values) {
        row.insert(
            column.clone(),
            value.map_or(Value::Null, KeyValue::into_value),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn datetime(nanos: i64, offset: i32, zone: Option<&str>) -> Value {
        Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch: nanos,
            offset_seconds: offset,
            timezone_name: zone.map(str::to_string),
        })
    }

    #[test]
    fn datetime_key_is_the_instant() {
        let kind = KeyKind::DateTimeInstant;
        // The same instant at two offsets and in a zone has one key.
        for v in [
            datetime(1_000, 0, None),
            datetime(1_000, 3600, None),
            datetime(1_000, -18000, Some("America/New_York")),
        ] {
            assert_eq!(derive(&kind, Some(&v)), vec![Some(KeyValue::Int(1_000))]);
        }
        // 1 ns apart, and before the epoch.
        assert_eq!(
            derive(&kind, Some(&datetime(1_001, 0, None))),
            vec![Some(KeyValue::Int(1_001))]
        );
        assert_eq!(
            derive(&kind, Some(&datetime(-1, 0, None))),
            vec![Some(KeyValue::Int(-1))]
        );
    }

    /// A `localdatetime` stored in a DATETIME column is kept offset-NULL and
    /// `WHERE` compares its local nanos as the instant.
    #[test]
    fn datetime_key_of_an_offset_null_row_is_its_nanos() {
        let local = Value::Temporal(TemporalValue::LocalDateTime {
            nanos_since_epoch: 42,
        });
        assert_eq!(
            derive(&KeyKind::DateTimeInstant, Some(&local)),
            vec![Some(KeyValue::Int(42))]
        );
        // As a constant it is incomparable with the column: no bound.
        assert_eq!(constant_key(&KeyKind::DateTimeInstant, &local), None);
    }

    #[test]
    fn datetime_key_is_null_for_what_the_column_stores_null() {
        let kind = KeyKind::DateTimeInstant;
        assert_eq!(derive(&kind, None), vec![None]);
        assert_eq!(derive(&kind, Some(&Value::Null)), vec![None]);
        assert_eq!(
            derive(&kind, Some(&Value::String("2026-01-01".into()))),
            vec![None]
        );
        let date = Value::Temporal(TemporalValue::Date {
            days_since_epoch: 1,
        });
        assert_eq!(derive(&kind, Some(&date)), vec![None]);
        assert_eq!(constant_key(&kind, &date), None);
        assert_eq!(constant_key(&kind, &Value::Null), None);
        assert_eq!(constant_key(&kind, &Value::String("x".into())), None);
    }

    fn btic(lit: &str) -> Value {
        let b = uni_btic::parse::parse_btic_literal(lit).unwrap();
        Value::Temporal(TemporalValue::Btic {
            lo: b.lo(),
            hi: b.hi(),
            meta: b.meta(),
        })
    }

    #[test]
    fn btic_key_is_the_raw_bounds_sentinels_kept() {
        let kind = KeyKind::BticBounds;
        let key = |v: &Value| derive(&kind, Some(v));
        // 1985-01-01 .. 1986-01-01 in ms, whatever the granularity or
        // certainty.
        let y1985 = vec![
            Some(KeyValue::Int(473_385_600_000)),
            Some(KeyValue::Int(504_921_600_000)),
        ];
        for lit in ["1985", "~1985", "?1985", "1985-01/1985-12"] {
            assert_eq!(key(&btic(lit)), y1985, "{lit}");
        }
        // A literal string stored in the column has the same key.
        assert_eq!(key(&Value::String("1985".into())), y1985);
        assert_eq!(
            key(&btic("/")),
            vec![Some(KeyValue::Int(i64::MIN)), Some(KeyValue::Int(i64::MAX))]
        );
        assert_eq!(
            key(&btic("2020-03/"))[1],
            Some(KeyValue::Int(i64::MAX)),
            "a right-unbounded interval keeps its sentinel"
        );
        assert_eq!(key(&btic("/1980"))[0], Some(KeyValue::Int(i64::MIN)));
        // NULL, other types, unparsable strings and invalid intervals.
        for v in [
            Value::Null,
            Value::Int(1),
            Value::String("not a btic".into()),
            Value::Temporal(TemporalValue::Btic {
                lo: 5,
                hi: 5,
                meta: 0,
            }),
        ] {
            assert_eq!(key(&v), vec![None, None], "{v:?}");
        }
        assert_eq!(derive(&kind, None), vec![None, None]);
        // As a constant, only a BTIC value has a key.
        assert_eq!(
            constant_key(&kind, &btic("1985")),
            Some(vec![
                KeyValue::Int(473_385_600_000),
                KeyValue::Int(504_921_600_000)
            ])
        );
        assert_eq!(constant_key(&kind, &Value::String("1985".into())), None);
        assert_eq!(constant_key(&kind, &Value::Int(1)), None);
        assert_eq!(constant_key(&kind, &Value::Null), None);
    }

    #[test]
    fn fill_key_columns_writes_every_column() {
        let key = IndexKey::new(KeyKind::DateTimeInstant, "ts");
        let mut source = uni_common::Properties::new();
        source.insert("ts".into(), datetime(7, 0, None));
        let mut row = source.clone();
        fill_key_columns(&key, "ts", &source, &mut row);
        assert_eq!(row.get("__key_dt_ts"), Some(&Value::Int(7)));
        let mut empty = uni_common::Properties::new();
        fill_key_columns(&key, "ts", &uni_common::Properties::new(), &mut empty);
        assert_eq!(empty.get("__key_dt_ts"), Some(&Value::Null));
    }
}
