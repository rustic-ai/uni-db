// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Key derivation for keyed scalar indexes.
//!
//! A keyed index ([`IndexKey`]) indexes a property whose own column cannot
//! take a Lance scalar index (a nested or opaque Arrow column) through flat
//! *key columns* derived from it. The columns are filled at batch build
//! (`VertexDataset::build_record_batch_with_timestamps`, through
//! `vertex::derived_rows`) and by the backfill an index
//! creation runs; they are never stored in L0 and never hashed into the
//! content UID.
//!
//! The key of a stored value must agree with how `WHERE` compares the stored
//! value, so that a bound on the key admits every row the predicate admits.
//! [`derive`](fn@derive) gives the key of a stored value; [`constant_key`] the key of a
//! query constant, which is stricter: a constant of another type is never
//! turned into a bound.

// Rust guideline compliant

use uni_common::core::schema::{DataType, IndexKey, KeyKind};
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
/// | `TimeUtc` | a TIME | `nanos_since_midnight − offset_seconds · 10⁹` (no modulo) |
/// | | a `localtime` stored in the column (offset NULL) | NULL: `WHERE` compares it as NULL |
/// | `MapPath` | the MAP property's value (`m` for `m.k`) | the value at the path, converted as the map column stores it; NULL for a missing key |
/// | `PointCoords` | a point of the column's CRS, coordinates finite (geographic: in range) | `(x, y)` or `(longitude, latitude)`, `-0.0` as `+0.0`; `z` not keyed (see [`point_coords`]) |
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
        // The TIME column stores a TIME, or a `localtime` with its offset
        // NULL (see `values_to_time_struct_array`). `WHERE` compares
        // `nanos − offset · 10⁹`, which is NULL for the latter: such a row
        // matches no comparison with a TIME, so its key is NULL.
        KeyKind::TimeUtc => vec![time_utc(value).map(KeyValue::Int)],
        KeyKind::MapPath { path, value: ty } => vec![match (value, path.as_slice()) {
            (Value::Map(map), [k]) => map.get(k).and_then(|v| map_value_key(ty, v)),
            _ => None,
        }],
        KeyKind::PointCoords { .. } => match point_coords(kind, value) {
            Some((x, y)) => vec![Some(KeyValue::Float(x)), Some(KeyValue::Float(y))],
            None => vec![None; slots],
        },
        _ => vec![None; slots],
    }
}

/// `nanos_since_midnight − offset_seconds · 10⁹` of a TIME, with no modulo:
/// the value `WHERE` compares two TIMEs by (`extract_time_nanos`). Fits an
/// Int64: |nanos| < 8.7·10¹³ and |offset| ≤ 18 h.
fn time_utc(value: &Value) -> Option<i64> {
    match value {
        Value::Temporal(TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        }) => nanos_since_midnight.checked_sub(i64::from(*offset_seconds) * 1_000_000_000),
        _ => None,
    }
}

/// The key of a value stored in a typed MAP whose values the key column
/// holds as `ty`, converted as the map column stores it
/// (`build_map_column`), so the key is the value a read of `n.m.k` returns:
/// an INT32 map drops an out-of-range integer (NULL), a FLOAT32 map rounds
/// to `f32`, a float map stores an integer as a float. `None` for a value
/// the column stores as NULL.
fn map_value_key(ty: &DataType, value: &Value) -> Option<KeyValue> {
    match ty {
        DataType::String => value.as_str().map(|s| KeyValue::Str(s.to_string())),
        DataType::Int64 => value.as_i64().map(KeyValue::Int),
        DataType::Int32 => value
            .as_i64()
            .and_then(|n| i32::try_from(n).ok())
            .map(|n| KeyValue::Int(i64::from(n))),
        DataType::Float64 => value.as_f64().map(float_key),
        DataType::Float32 => value.as_f64().map(|f| float_key(f64::from(f as f32))),
        _ => None,
    }
}

/// A Float64 key, `-0.0` normalized to `+0.0` (Cypher compares them equal;
/// the storage order does not).
fn float_key(f: f64) -> KeyValue {
    KeyValue::Float(if f == 0.0 { 0.0 } else { f })
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

/// The keyed coordinates `(x, y)` / `(longitude, latitude)` of a value a
/// POINT column of `kind` stores, or `None` when the column stores it as
/// NULL or the key would not be sound.
///
/// The column stores a map carrying the layout's coordinates as numbers (a
/// 3-D column also needs `z`), with its `crs` or the layout's default (see
/// `values_to_point_struct_array`); anything else, a point of another CRS
/// included, is NULL there. A point is keyed only when:
///
/// - its CRS is the one `point(...)` gives the column ([`KeyKind::point_crs`]):
///   `distance` raises on a CRS mismatch, so a point of another CRS must
///   reach the residual whatever the bound, which a NULL key ensures;
/// - its keyed coordinates are finite, and a geographic one is in range
///   (latitude in [-90, 90], longitude in [-180, 180]), which the distance
///   bound's spherical reasoning assumes;
///
/// and `-0.0` is keyed as `+0.0`: `WHERE` compares floats numerically, the
/// storage filter by IEEE total order, which puts `-0.0` below `+0.0`.
#[must_use]
pub fn point_coords(kind: &KeyKind, value: &Value) -> Option<(f64, f64)> {
    let KeyKind::PointCoords {
        geographic,
        three_d,
    } = kind
    else {
        return None;
    };
    let map = value.as_object()?;
    let coord = |name: &str| map.get(name).and_then(Value::as_f64);
    let default_crs = match (geographic, three_d) {
        (true, _) => "WGS84",
        (false, false) => "cartesian",
        (false, true) => "cartesian-3d",
    };
    let crs = map
        .get("crs")
        .and_then(Value::as_str)
        .unwrap_or(default_crs);
    if Some(crs) != kind.point_crs() {
        return None;
    }
    let (x, y) = if *geographic {
        let (lat, lon) = (coord("latitude")?, coord("longitude")?);
        if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
            return None;
        }
        (lon, lat)
    } else {
        if *three_d {
            coord("z")?;
        }
        (coord("x")?, coord("y")?)
    };
    if !x.is_finite() || !y.is_finite() {
        return None;
    }
    // `-0.0 + 0.0` is `+0.0`; every other value is unchanged.
    Some((x + 0.0, y + 0.0))
}

/// The key of a query constant compared with a property indexed by `kind`,
/// or `None` when the constant is not of the property's own type (a
/// cross-type comparison, which `WHERE` answers without the key) or is NULL.
///
/// A `localdatetime` constant against a DATETIME property is incomparable in
/// `WHERE`, so it has no key, unlike a stored `localdatetime` (see
/// [`derive`](fn@derive)). Likewise a BTIC literal *string* is stored as an interval but
/// is not one as a constant. A point has no constant key: point `=` and
/// ordering are never served; its spatial predicates are bounded in
/// `uni_query_functions::key_bounds`.
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
        // A TIME constant; a `localtime` one is incomparable with a TIME.
        (KeyKind::TimeUtc, Value::Temporal(TemporalValue::Time { .. })) => {
            Some(vec![KeyValue::Int(time_utc(value)?)])
        }
        (KeyKind::MapPath { value: ty, .. }, _) => map_constant_key(ty, value).map(|k| vec![k]),
        _ => None,
    }
}

/// The key of a constant compared with a MAP path whose values are of type
/// `ty`: a string against STRING values, an integer against integer ones,
/// a finite float or an integer (exactly representable) against float ones.
/// NaN and the infinities have none: the storage filter has no literal for
/// them.
fn map_constant_key(ty: &DataType, value: &Value) -> Option<KeyValue> {
    /// Integers up to 2⁵³ convert to a float exactly.
    const EXACT: i64 = 1 << 53;
    match (ty, value) {
        (DataType::String, Value::String(s)) => Some(KeyValue::Str(s.clone())),
        (DataType::Int64 | DataType::Int32, Value::Int(i)) => Some(KeyValue::Int(*i)),
        (DataType::Float64 | DataType::Float32, Value::Float(f)) if f.is_finite() => {
            Some(float_key(*f))
        }
        (DataType::Float64 | DataType::Float32, Value::Int(i)) if i.abs() <= EXACT => {
            Some(float_key(*i as f64))
        }
        _ => None,
    }
}

/// Fill `row`'s key columns for `key` from the value of `property` in
/// `source` (for a key path `m.k`, from the MAP property `m`).
pub fn fill_key_columns(
    key: &IndexKey,
    property: &str,
    source: &uni_common::Properties,
    row: &mut uni_common::Properties,
) {
    let values = derive(&key.kind, source.get(key.kind.source_property(property)));
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

    fn time(nanos: i64, offset: i32) -> Value {
        Value::Temporal(TemporalValue::Time {
            nanos_since_midnight: nanos,
            offset_seconds: offset,
        })
    }

    const HOUR: i64 = 3_600_000_000_000;

    #[test]
    fn time_key_is_utc_nanoseconds_without_modulo() {
        let kind = KeyKind::TimeUtc;
        let key = |v: &Value| derive(&kind, Some(v));
        // 23:00-05:00 is 28 h UTC, not 04:00Z: `WHERE` takes no modulo.
        assert_eq!(
            key(&time(23 * HOUR, -5 * 3600)),
            vec![Some(KeyValue::Int(28 * HOUR))]
        );
        assert_eq!(key(&time(4 * HOUR, 0)), vec![Some(KeyValue::Int(4 * HOUR))]);
        // 10:00+14:00 is 20:00 the day before.
        assert_eq!(
            key(&time(10 * HOUR, 14 * 3600)),
            vec![Some(KeyValue::Int(-4 * HOUR))]
        );
        // The same instant at two offsets has one key.
        assert_eq!(key(&time(13 * HOUR, 3600)), key(&time(12 * HOUR, 0)));
        // A `localtime` stored in the column compares as NULL: no key.
        let local = Value::Temporal(TemporalValue::LocalTime {
            nanos_since_midnight: 12 * HOUR,
        });
        assert_eq!(key(&local), vec![None]);
        assert_eq!(constant_key(&kind, &local), None);
        for v in [Value::Null, Value::String("12:00".into()), Value::Int(1)] {
            assert_eq!(key(&v), vec![None], "{v:?}");
            assert_eq!(constant_key(&kind, &v), None, "{v:?}");
        }
        assert_eq!(derive(&kind, None), vec![None]);
        assert_eq!(
            constant_key(&kind, &time(23 * HOUR, -5 * 3600)),
            Some(vec![KeyValue::Int(28 * HOUR)])
        );
    }

    fn map(entries: &[(&str, Value)]) -> Value {
        Value::Map(
            entries
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        )
    }

    fn path_kind(value: DataType) -> KeyKind {
        KeyKind::MapPath {
            path: vec!["k".into()],
            value,
        }
    }

    #[test]
    fn map_path_key_is_the_value_as_the_column_stores_it() {
        let key = |ty: DataType, v: Value| derive(&path_kind(ty), Some(&map(&[("k", v)])));
        assert_eq!(
            key(DataType::Int64, Value::Int(5)),
            vec![Some(KeyValue::Int(5))]
        );
        // An INT32 map stores an out-of-range integer as NULL.
        assert_eq!(key(DataType::Int32, Value::Int(i64::MAX)), vec![None]);
        assert_eq!(
            key(DataType::Int32, Value::Int(-7)),
            vec![Some(KeyValue::Int(-7))]
        );
        // A float map stores an integer as a float, a FLOAT32 one rounded.
        assert_eq!(
            key(DataType::Float64, Value::Int(2)),
            vec![Some(KeyValue::Float(2.0))]
        );
        assert_eq!(
            key(DataType::Float32, Value::Float(0.1)),
            vec![Some(KeyValue::Float(f64::from(0.1_f32)))]
        );
        assert_eq!(
            key(DataType::Float64, Value::Float(-0.0)),
            vec![Some(KeyValue::Float(0.0))]
        );
        assert_eq!(
            key(DataType::String, Value::String("x".into())),
            vec![Some(KeyValue::Str("x".into()))]
        );
        // A value the column stores as NULL, a missing key, no map.
        assert_eq!(key(DataType::String, Value::Int(1)), vec![None]);
        assert_eq!(key(DataType::Int64, Value::Null), vec![None]);
        let kind = path_kind(DataType::Int64);
        assert_eq!(
            derive(&kind, Some(&map(&[("other", Value::Int(1))]))),
            vec![None]
        );
        assert_eq!(derive(&kind, Some(&Value::Int(1))), vec![None]);
        assert_eq!(derive(&kind, None), vec![None]);

        // As a constant: the values' type only (an exact integer against
        // floats), never NaN or an infinity.
        assert_eq!(
            constant_key(&kind, &Value::Int(3)),
            Some(vec![KeyValue::Int(3)])
        );
        assert_eq!(constant_key(&kind, &Value::Float(3.0)), None);
        let floats = path_kind(DataType::Float32);
        assert_eq!(
            constant_key(&floats, &Value::Int(3)),
            Some(vec![KeyValue::Float(3.0)])
        );
        assert_eq!(constant_key(&floats, &Value::Int(i64::MAX)), None);
        assert_eq!(constant_key(&floats, &Value::Float(f64::NAN)), None);
        assert_eq!(constant_key(&floats, &Value::Float(f64::INFINITY)), None);
        assert_eq!(
            constant_key(&path_kind(DataType::String), &Value::Int(1)),
            None
        );
    }

    #[test]
    fn fill_key_columns_reads_a_key_path_from_its_map() {
        let key = IndexKey::new(path_kind(DataType::Int64), "m");
        let mut source = uni_common::Properties::new();
        source.insert("m".into(), map(&[("k", Value::Int(9))]));
        let mut row = source.clone();
        fill_key_columns(&key, "m.k", &source, &mut row);
        assert_eq!(row.get(&key.columns[0]), Some(&Value::Int(9)));
    }

    fn point(entries: &[(&str, Value)]) -> Value {
        map(entries)
    }

    const GEO: KeyKind = KeyKind::PointCoords {
        geographic: true,
        three_d: false,
    };
    const XY: KeyKind = KeyKind::PointCoords {
        geographic: false,
        three_d: false,
    };
    const XYZ: KeyKind = KeyKind::PointCoords {
        geographic: false,
        three_d: true,
    };

    #[test]
    fn point_key_is_its_coordinates_negative_zero_normalized() {
        let f = Value::Float;
        let s = |v: &str| Value::String(v.into());
        let key = |kind: &KeyKind, v: &Value| derive(kind, Some(v));
        let fl = |a: f64, b: f64| vec![Some(KeyValue::Float(a)), Some(KeyValue::Float(b))];
        // Geographic: (longitude, latitude).
        let geo = point(&[
            ("crs", s("WGS84")),
            ("latitude", f(51.5)),
            ("longitude", f(-0.12)),
        ]);
        assert_eq!(key(&GEO, &geo), fl(-0.12, 51.5));
        // As read back from storage (no `type`), and with no `crs` (the
        // column's default).
        let no_crs = point(&[("latitude", f(1.0)), ("longitude", Value::Int(2))]);
        assert_eq!(key(&GEO, &no_crs), fl(2.0, 1.0));
        let xy = point(&[("crs", s("Cartesian")), ("x", f(-0.0)), ("y", f(3.0))]);
        let k = key(&XY, &xy);
        assert_eq!(k, fl(0.0, 3.0));
        let Some(KeyValue::Float(x)) = k[0] else {
            panic!()
        };
        assert!(x.is_sign_positive(), "-0.0 must be keyed as +0.0");
        // A 3-D point's z is not keyed, but must be there.
        let xyz = point(&[
            ("crs", s("Cartesian-3D")),
            ("x", f(1.0)),
            ("y", f(2.0)),
            ("z", f(9.0)),
        ]);
        assert_eq!(key(&XYZ, &xyz), fl(1.0, 2.0));
        let flat = point(&[("crs", s("Cartesian-3D")), ("x", f(1.0)), ("y", f(2.0))]);
        assert_eq!(key(&XYZ, &flat), vec![None, None]);
        // Another CRS, or the column's lowercase default, which `distance`
        // tells apart from `point(...)`'s: NULL keys.
        assert_eq!(key(&XY, &xyz), vec![None, None]);
        assert_eq!(key(&GEO, &xy), vec![None, None]);
        assert_eq!(key(&XY, &geo), vec![None, None]);
        assert_eq!(
            key(&XY, &point(&[("x", f(1.0)), ("y", f(2.0))])),
            vec![None, None]
        );
        // Out of range or non-finite.
        for bad in [
            point(&[("latitude", f(91.0)), ("longitude", f(0.0))]),
            point(&[("latitude", f(0.0)), ("longitude", f(180.5))]),
            point(&[("latitude", f(f64::NAN)), ("longitude", f(0.0))]),
        ] {
            assert_eq!(key(&GEO, &bad), vec![None, None], "{bad:?}");
        }
        for bad in [
            point(&[("crs", s("Cartesian")), ("x", f(f64::NAN)), ("y", f(0.0))]),
            point(&[
                ("crs", s("Cartesian")),
                ("x", f(0.0)),
                ("y", f(f64::INFINITY)),
            ]),
            point(&[("crs", s("Cartesian")), ("x", s("1")), ("y", f(0.0))]),
            Value::Null,
            Value::Float(1.0),
        ] {
            assert_eq!(key(&XY, &bad), vec![None, None], "{bad:?}");
        }
        assert_eq!(derive(&XY, None), vec![None, None]);
        // Edges of the geographic range are keyed.
        assert_eq!(
            key(
                &GEO,
                &point(&[("latitude", f(-90.0)), ("longitude", f(180.0))])
            ),
            fl(180.0, -90.0)
        );
        // No constant key: point `=` is never served.
        assert_eq!(constant_key(&XY, &xy), None);
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
