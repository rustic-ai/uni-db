// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bounds on the key columns of a keyed scalar index.
//!
//! A keyed index (`IndexKey`) indexes a property through derived key
//! columns. A predicate on the property with a constant side is turned into a
//! [`KeyBound`]: a condition on the key columns, by position, that every row
//! the predicate accepts satisfies. The bound goes to the storage scan's
//! filter only; the predicate itself is always applied above it, so a bound
//! may admit extra rows but never drops a match.
//!
//! The BTIC rules ([`btic_bound`], [`btic_accessor_bound`]) are derived from
//! the predicate definitions in `uni_btic::predicates`, which
//! `expr_eval::eval_btic_function` evaluates: every interval predicate
//! compares the raw bounds `lo`, `hi` (ms, the unbounded sentinels
//! `i64::MIN` / `i64::MAX` included), so with one side constant it is a range
//! (or a union of two ranges) on the key columns `[lo, hi]`.

// Rust guideline compliant

use uni_common::{TemporalValue, Value};
use uni_store::backend::types::{CmpOp, FilterExpr, Scalar};
use uni_store::storage::index_keys::KeyValue;

/// A condition on a keyed index's key columns, each named by its position in
/// `IndexKey::columns`.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyBound {
    /// Every key satisfies it.
    True,
    /// No key satisfies it.
    False,
    /// `column op value`.
    Cmp {
        /// The key column's position.
        column: usize,
        /// The comparison.
        op: CmpOp,
        /// The constant.
        value: KeyValue,
    },
    /// `column IN (values…)`.
    In {
        /// The key column's position.
        column: usize,
        /// The candidates, never empty.
        values: Vec<KeyValue>,
    },
    /// Every part holds.
    And(Vec<KeyBound>),
    /// Some part holds.
    Or(Vec<KeyBound>),
}

impl KeyBound {
    /// `column op value`, folded to [`Self::True`] / [`Self::False`] where an
    /// Int64 extreme decides it for every key (`lo >= i64::MIN`,
    /// `hi <= i64::MAX`, `hi > i64::MAX`, …).
    #[must_use]
    pub fn cmp(column: usize, op: CmpOp, value: KeyValue) -> Self {
        if let KeyValue::Int(v) = value {
            match (op, v) {
                (CmpOp::GtEq, i64::MIN) | (CmpOp::LtEq, i64::MAX) => return Self::True,
                (CmpOp::Lt, i64::MIN) | (CmpOp::Gt, i64::MAX) => return Self::False,
                _ => {}
            }
        }
        Self::Cmp { column, op, value }
    }

    /// `column op v` on an Int64 key column.
    #[must_use]
    pub fn int(column: usize, op: CmpOp, v: i64) -> Self {
        Self::cmp(column, op, KeyValue::Int(v))
    }

    /// The conjunction of `parts`, folded.
    #[must_use]
    pub fn and(parts: impl IntoIterator<Item = KeyBound>) -> Self {
        let mut kept = Vec::new();
        for part in parts {
            match part {
                Self::True => {}
                Self::False => return Self::False,
                Self::And(inner) => kept.extend(inner),
                other => kept.push(other),
            }
        }
        match kept.len() {
            0 => Self::True,
            1 => kept.pop().unwrap_or(Self::True),
            _ => Self::And(kept),
        }
    }

    /// The disjunction of `parts`, folded.
    #[must_use]
    pub fn or(parts: impl IntoIterator<Item = KeyBound>) -> Self {
        let mut kept = Vec::new();
        for part in parts {
            match part {
                Self::False => {}
                Self::True => return Self::True,
                Self::Or(inner) => kept.extend(inner),
                other => kept.push(other),
            }
        }
        match kept.len() {
            0 => Self::False,
            1 => kept.pop().unwrap_or(Self::False),
            _ => Self::Or(kept),
        }
    }

    /// The storage filter for this bound over the key columns `columns`.
    /// `None` when a position has no column.
    #[must_use]
    pub fn to_filter(&self, columns: &[String]) -> Option<FilterExpr> {
        let name = |c: &usize| columns.get(*c).cloned();
        Some(match self {
            Self::True => FilterExpr::Literal(true),
            Self::False => FilterExpr::Literal(false),
            Self::Cmp { column, op, value } => {
                FilterExpr::compare(name(column)?, *op, scalar(value))
            }
            Self::In { column, values } => {
                FilterExpr::one_of(name(column)?, values.iter().map(scalar))
            }
            Self::And(parts) => FilterExpr::And(
                parts
                    .iter()
                    .map(|p| p.to_filter(columns))
                    .collect::<Option<_>>()?,
            ),
            Self::Or(parts) => FilterExpr::Or(
                parts
                    .iter()
                    .map(|p| p.to_filter(columns))
                    .collect::<Option<_>>()?,
            ),
        })
    }

    /// Whether a row with these key values passes the bound. A NULL key
    /// passes: the storage filter always admits a NULL key (see
    /// `KeyedPredicate::lance_bound`).
    #[must_use]
    pub fn admits(&self, keys: &[Option<KeyValue>]) -> bool {
        let key = |c: &usize| keys.get(*c).cloned().flatten();
        match self {
            Self::True => true,
            Self::False => false,
            Self::Cmp { column, op, value } => match key(column) {
                None => true,
                Some(k) => compare(&k, value).is_some_and(|o| op_holds(*op, o)),
            },
            Self::In { column, values } => match key(column) {
                None => true,
                Some(k) => values
                    .iter()
                    .any(|v| compare(&k, v).is_some_and(|o| o.is_eq())),
            },
            Self::And(parts) => parts.iter().all(|p| p.admits(keys)),
            Self::Or(parts) => parts.iter().any(|p| p.admits(keys)),
        }
    }
}

fn scalar(value: &KeyValue) -> Scalar {
    match value {
        KeyValue::Int(i) => Scalar::Int(*i),
        KeyValue::Float(f) => Scalar::Float(*f),
        KeyValue::Str(s) => Scalar::Str(s.clone()),
    }
}

fn compare(a: &KeyValue, b: &KeyValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (KeyValue::Int(a), KeyValue::Int(b)) => Some(a.cmp(b)),
        (KeyValue::Float(a), KeyValue::Float(b)) => a.partial_cmp(b),
        (KeyValue::Str(a), KeyValue::Str(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn op_holds(op: CmpOp, o: std::cmp::Ordering) -> bool {
    match op {
        CmpOp::Eq => o.is_eq(),
        CmpOp::NotEq => o.is_ne(),
        CmpOp::Lt => o.is_lt(),
        CmpOp::LtEq => o.is_le(),
        CmpOp::Gt => o.is_gt(),
        CmpOp::GtEq => o.is_ge(),
    }
}

/// The position of a BTIC key's lower bound column.
pub const BTIC_LO: usize = 0;
/// The position of a BTIC key's upper bound column.
pub const BTIC_HI: usize = 1;

/// A BTIC predicate with one constant side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BticPredicate {
    /// `btic_overlaps`.
    Overlaps,
    /// `btic_contains`.
    Contains,
    /// `btic_during`.
    During,
    /// `btic_before`.
    Before,
    /// `btic_after`.
    After,
    /// `btic_meets`.
    Meets,
    /// `btic_starts`.
    Starts,
    /// `btic_finishes`.
    Finishes,
    /// `btic_equals`.
    Equals,
    /// `btic_disjoint`.
    Disjoint,
    /// `btic_adjacent`.
    Adjacent,
    /// `btic_contains_point(interval, point)`.
    ContainsPoint,
}

impl BticPredicate {
    /// The predicate a function name calls, ignoring case.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "btic_overlaps" => Self::Overlaps,
            "btic_contains" => Self::Contains,
            "btic_during" => Self::During,
            "btic_before" => Self::Before,
            "btic_after" => Self::After,
            "btic_meets" => Self::Meets,
            "btic_starts" => Self::Starts,
            "btic_finishes" => Self::Finishes,
            "btic_equals" => Self::Equals,
            "btic_disjoint" => Self::Disjoint,
            "btic_adjacent" => Self::Adjacent,
            "btic_contains_point" => Self::ContainsPoint,
            _ => return None,
        })
    }

    /// The function's name, upper-case, as `eval_btic_function` dispatches.
    #[must_use]
    pub fn function_name(self) -> &'static str {
        match self {
            Self::Overlaps => "BTIC_OVERLAPS",
            Self::Contains => "BTIC_CONTAINS",
            Self::During => "BTIC_DURING",
            Self::Before => "BTIC_BEFORE",
            Self::After => "BTIC_AFTER",
            Self::Meets => "BTIC_MEETS",
            Self::Starts => "BTIC_STARTS",
            Self::Finishes => "BTIC_FINISHES",
            Self::Equals => "BTIC_EQUALS",
            Self::Disjoint => "BTIC_DISJOINT",
            Self::Adjacent => "BTIC_ADJACENT",
            Self::ContainsPoint => "BTIC_CONTAINS_POINT",
        }
    }
}

/// The raw bounds of a BTIC constant, or `None` for any other value (the
/// predicates refuse it).
#[must_use]
pub fn btic_constant(value: &Value) -> Option<(i64, i64)> {
    match value {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => Some((*lo, *hi)),
        _ => None,
    }
}

/// A point as `btic_contains_point` converts it (`extract_point_ms` in
/// `expr_eval`): an integer is ms; a datetime or localdatetime is its
/// nanoseconds divided by 10^6, truncating toward zero; a date is its days
/// times 86 400 000. `None` for any other value (the predicate refuses it).
#[must_use]
pub fn btic_point_ms(value: &Value) -> Option<i64> {
    match value {
        Value::Int(ms) => Some(*ms),
        Value::Temporal(
            TemporalValue::DateTime {
                nanos_since_epoch, ..
            }
            | TemporalValue::LocalDateTime { nanos_since_epoch },
        ) => Some(*nanos_since_epoch / 1_000_000),
        Value::Temporal(TemporalValue::Date { days_since_epoch }) => {
            Some(i64::from(*days_since_epoch) * 86_400_000)
        }
        _ => None,
    }
}

/// The bound on `[lo, hi]` of a property `p` in `pred` against a constant
/// interval `[c_lo, c_hi]`; `property_first` when `p` is the first argument.
///
/// Each predicate, as defined in `uni_btic::predicates` (`a` the first
/// argument, `b` the second), with the property's bounds as `lo`, `hi`:
///
/// | Predicate | `p` first: `pred(p, c)` | `p` second: `pred(c, p)` |
/// |---|---|---|
/// | overlaps: `a.lo < b.hi ∧ b.lo < a.hi` | `lo < c.hi ∧ hi > c.lo` | the same |
/// | contains: `a.lo ≤ b.lo ∧ b.hi ≤ a.hi` | `lo ≤ c.lo ∧ hi ≥ c.hi` | `lo ≥ c.lo ∧ hi ≤ c.hi` |
/// | during: `b.lo < a.lo ∧ a.hi < b.hi` | `lo > c.lo ∧ hi < c.hi` | `lo < c.lo ∧ hi > c.hi` |
/// | before: `a.hi ≤ b.lo` | `hi ≤ c.lo` | `lo ≥ c.hi` |
/// | after: `b.hi ≤ a.lo` | `lo ≥ c.hi` | `hi ≤ c.lo` |
/// | meets: `a.hi = b.lo` | `hi = c.lo` | `lo = c.hi` |
/// | starts: `a.lo = b.lo ∧ a.hi < b.hi` | `lo = c.lo ∧ hi < c.hi` | `lo = c.lo ∧ hi > c.hi` |
/// | finishes: `a.hi = b.hi ∧ b.lo < a.lo` | `hi = c.hi ∧ lo > c.lo` | `hi = c.hi ∧ lo < c.lo` |
/// | equals: `a.lo = b.lo ∧ a.hi = b.hi` | `lo = c.lo ∧ hi = c.hi` | the same |
/// | disjoint: `a.hi ≤ b.lo ∨ b.hi ≤ a.lo` | `hi ≤ c.lo ∨ lo ≥ c.hi` | the same |
/// | adjacent: `a.hi = b.lo ∨ b.hi = a.lo` | `hi = c.lo ∨ lo = c.hi` | the same |
///
/// Every bound is the predicate itself on the raw bounds, so it is exact for
/// the key; only a NULL key, which the storage filter always admits, makes
/// it a superset. `contains_point` takes a point, not an interval: see
/// [`btic_point_bound`].
#[must_use]
pub fn btic_bound(pred: BticPredicate, property_first: bool, c_lo: i64, c_hi: i64) -> KeyBound {
    use BticPredicate as P;
    use CmpOp::{Eq, Gt, GtEq, Lt, LtEq};
    let lo = |op, v| KeyBound::int(BTIC_LO, op, v);
    let hi = |op, v| KeyBound::int(BTIC_HI, op, v);
    match (pred, property_first) {
        (P::Overlaps, _) => KeyBound::and([lo(Lt, c_hi), hi(Gt, c_lo)]),
        (P::Contains, true) => KeyBound::and([lo(LtEq, c_lo), hi(GtEq, c_hi)]),
        (P::Contains, false) => KeyBound::and([lo(GtEq, c_lo), hi(LtEq, c_hi)]),
        (P::During, true) => KeyBound::and([lo(Gt, c_lo), hi(Lt, c_hi)]),
        (P::During, false) => KeyBound::and([lo(Lt, c_lo), hi(Gt, c_hi)]),
        (P::Before, true) | (P::After, false) => hi(LtEq, c_lo),
        (P::After, true) | (P::Before, false) => lo(GtEq, c_hi),
        (P::Meets, true) => hi(Eq, c_lo),
        (P::Meets, false) => lo(Eq, c_hi),
        (P::Starts, true) => KeyBound::and([lo(Eq, c_lo), hi(Lt, c_hi)]),
        (P::Starts, false) => KeyBound::and([lo(Eq, c_lo), hi(Gt, c_hi)]),
        (P::Finishes, true) => KeyBound::and([hi(Eq, c_hi), lo(Gt, c_lo)]),
        (P::Finishes, false) => KeyBound::and([hi(Eq, c_hi), lo(Lt, c_lo)]),
        (P::Equals, _) => KeyBound::and([lo(Eq, c_lo), hi(Eq, c_hi)]),
        (P::Disjoint, _) => KeyBound::or([hi(LtEq, c_lo), lo(GtEq, c_hi)]),
        (P::Adjacent, _) => KeyBound::or([hi(Eq, c_lo), lo(Eq, c_hi)]),
        // Not an interval constant; never reached through `KeyedForm`.
        (P::ContainsPoint, _) => KeyBound::True,
    }
}

/// The bound of `btic_contains_point(p, t)`: `lo ≤ t ∧ t < hi`, with `t` the
/// point in ms as [`btic_point_ms`] converts it.
#[must_use]
pub fn btic_point_bound(t_ms: i64) -> KeyBound {
    KeyBound::and([
        KeyBound::int(BTIC_LO, CmpOp::LtEq, t_ms),
        KeyBound::int(BTIC_HI, CmpOp::Gt, t_ms),
    ])
}

/// Which bound a `btic_lo` / `btic_hi` accessor reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BticSide {
    /// `btic_lo`: the lower bound, NULL when it is `i64::MIN`.
    Lo,
    /// `btic_hi`: the upper bound, NULL when it is `i64::MAX`.
    Hi,
}

impl BticSide {
    /// The accessor a function name calls, ignoring case.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "btic_lo" => Some(Self::Lo),
            "btic_hi" => Some(Self::Hi),
            _ => None,
        }
    }

    /// The key column the accessor reads.
    #[must_use]
    pub fn column(self) -> usize {
        match self {
            Self::Lo => BTIC_LO,
            Self::Hi => BTIC_HI,
        }
    }
}

/// The bound of `btic_lo(p) op c` / `btic_hi(p) op c` for a datetime `c`;
/// `None` for any other constant (the accessor returns a UTC datetime, which
/// compares with an integer, a localdatetime or a date as another type) and
/// for `<>`.
///
/// The accessor is the bound `b` in ms as the datetime of `b · 10^6` ns, and a
/// datetime compares by instant, so with `N` the constant's nanoseconds:
/// `b·10^6 < N ⇔ b < ⌈N/10^6⌉`, `b·10^6 ≤ N ⇔ b ≤ ⌊N/10^6⌋`,
/// `b·10^6 > N ⇔ b > ⌊N/10^6⌋`, `b·10^6 ≥ N ⇔ b ≥ ⌈N/10^6⌉`, and `=` holds
/// only when `10^6` divides `N`. Exact for a finite bound; the accessor is
/// NULL for a sentinel, which the predicate rejects and the bound may admit,
/// so the bound is a superset there.
#[must_use]
pub fn btic_accessor_bound(side: BticSide, op: CmpOp, constant: &Value) -> Option<KeyBound> {
    let Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch, ..
    }) = constant
    else {
        return None;
    };
    let n = i128::from(*nanos_since_epoch);
    let floor = n.div_euclid(1_000_000);
    let ceil = -(-n).div_euclid(1_000_000);
    // Both fit in i64: |N / 10^6| < |N|.
    let as_i64 = |v: i128| i64::try_from(v).ok();
    let column = side.column();
    Some(match op {
        CmpOp::Lt => KeyBound::int(column, CmpOp::Lt, as_i64(ceil)?),
        CmpOp::LtEq => KeyBound::int(column, CmpOp::LtEq, as_i64(floor)?),
        CmpOp::Gt => KeyBound::int(column, CmpOp::Gt, as_i64(floor)?),
        CmpOp::GtEq => KeyBound::int(column, CmpOp::GtEq, as_i64(ceil)?),
        CmpOp::Eq if floor == ceil => KeyBound::int(column, CmpOp::Eq, as_i64(floor)?),
        CmpOp::Eq => KeyBound::False,
        CmpOp::NotEq => return None,
    })
}

/// The position of a POINT key's x / longitude column.
pub const POINT_X: usize = 0;
/// The position of a POINT key's y / latitude column.
pub const POINT_Y: usize = 1;

/// The Earth radius `distance` uses for WGS-84 points, in meters
/// (`EARTH_RADIUS_KM · 1000` in `spatial::eval_distance`).
pub const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// The relative widening of a distance bound. The engine computes a
/// distance, and this module a bound, each with a relative rounding error of
/// a few ulps (≈ 1e-15) on well-conditioned operations; widening by 1e-9
/// covers both a millionfold, for a cost of 1 mm per 1 000 km of radius.
const DISTANCE_REL_EPS: f64 = 1e-9;

/// The absolute widening of a geographic distance bound, in degrees
/// (≈ 0.1 mm). It covers rounding where relative error does not apply:
/// `to_radians` mapping two distinct latitudes to one radian value (`dlat`
/// computed as 0), and squares of tiny angle differences underflowing to 0.
const GEO_ABS_EPS_DEG: f64 = 1e-9;

/// The absolute widening of a Cartesian distance bound: `dx²` underflows to
/// zero below `|dx|` ≈ 1.5e-154, so the engine reports distance 0 for points
/// that far apart.
const CARTESIAN_ABS_EPS: f64 = 1e-150;

/// The largest `s` the longitude bound is computed from (`|dlon| ≤
/// 2·asin(s)`). `asin` is ill-conditioned near 1: an error of an ulp in `s`
/// there moves the result by ≈ 1e-8 rad, more than the widening covers; at
/// or above this `s` the longitude is left unbounded.
const LON_ASIN_MAX: f64 = 1.0 - 1e-6;

/// `column op v` on a Float64 key column, with `v` normalized (`-0.0` as
/// `+0.0`, as keys are) and an infinite `v` folded (keys are finite).
fn float_cmp(column: usize, op: CmpOp, v: f64) -> KeyBound {
    if v.is_infinite() {
        let holds = match op {
            CmpOp::GtEq | CmpOp::Gt => v < 0.0,
            CmpOp::LtEq | CmpOp::Lt => v > 0.0,
            CmpOp::Eq => false,
            CmpOp::NotEq => true,
        };
        return if holds {
            KeyBound::True
        } else {
            KeyBound::False
        };
    }
    KeyBound::cmp(column, op, KeyValue::Float(v + 0.0))
}

/// `lo ≤ column ≤ hi`, or [`KeyBound::False`] when `lo > hi`.
fn float_range(column: usize, lo: f64, hi: f64) -> KeyBound {
    if lo > hi {
        return KeyBound::False;
    }
    KeyBound::and([
        float_cmp(column, CmpOp::GtEq, lo),
        float_cmp(column, CmpOp::LtEq, hi),
    ])
}

/// A corner of `point.withinBBox` as the predicate reads it
/// (`get_geo_coords` / `get_cartesian_coords` in `spatial`): `(longitude,
/// latitude)` of a map with numeric `latitude` and `longitude`, or `(x, y)`
/// of one with numeric `x` and `y`, whatever its CRS (the predicate ignores
/// it). `None` when the predicate would raise for it, or for a non-finite
/// coordinate (no bound is rendered for one).
#[must_use]
pub fn point_corner(value: &Value, geographic: bool) -> Option<(f64, f64)> {
    let map = value.as_object()?;
    let coord = |name: &str| map.get(name).and_then(Value::as_f64);
    let (x, y) = if geographic {
        (coord("longitude")?, coord("latitude")?)
    } else {
        (coord("x")?, coord("y")?)
    };
    (x.is_finite() && y.is_finite()).then_some((x, y))
}

/// The center of `distance(p, c)` as the predicate reads it, `(x, y)` or
/// `(longitude, latitude)`, when `c` is a point of the column's CRS `crs`:
/// `distance` raises for two CRS names that differ, so a constant of
/// another CRS is never bounded and the residual raises as without the
/// index. A geographic center must be in range, finite, as a stored point
/// is keyed.
#[must_use]
pub fn point_center(value: &Value, geographic: bool, crs: &str) -> Option<(f64, f64)> {
    let map = value.as_object()?;
    if map.get("crs").and_then(Value::as_str) != Some(crs) {
        return None;
    }
    let (x, y) = point_corner(value, geographic)?;
    if geographic && (!(-90.0..=90.0).contains(&y) || !(-180.0..=180.0).contains(&x)) {
        return None;
    }
    Some((x, y))
}

/// The bound of `point.withinBBox(p, ll, ur)`, with the corners as
/// [`point_corner`] reads them.
///
/// The predicate (`spatial::eval_within_bbox`) tests inclusive ranges on the
/// stored coordinates, so the bound is exact on the key: `ll.x ≤ x ≤ ur.x ∧
/// ll.y ≤ y ≤ ur.y`, an empty range matching nothing. A geographic box whose
/// lower-left longitude exceeds its upper-right one crosses the antimeridian:
/// the predicate takes `lon ≥ ll.lon ∨ lon ≤ ur.lon`, and so does the bound.
/// The CRS names of the corners are not compared, as the predicate does not.
#[must_use]
pub fn point_bbox_bound(geographic: bool, ll: (f64, f64), ur: (f64, f64)) -> KeyBound {
    let x = if geographic && ll.0 > ur.0 {
        KeyBound::or([
            float_cmp(POINT_X, CmpOp::GtEq, ll.0),
            float_cmp(POINT_X, CmpOp::LtEq, ur.0),
        ])
    } else {
        float_range(POINT_X, ll.0, ur.0)
    };
    KeyBound::and([x, float_range(POINT_Y, ll.1, ur.1)])
}

/// A bound on the key admitting every stored point `p` with `distance(p, c)
/// < r` (`inclusive`: `≤ r`), for a center `c` of the column's CRS (see
/// [`point_center`]). A box, so a superset; the residual computes the
/// distance.
///
/// **Cartesian** (`spatial::eval_distance`: `√(dx² + dy² + dz²)`):
/// `|dx| ≤ d` and `|dy| ≤ d`, so `x ∈ [c.x − r', c.x + r']` and likewise
/// `y`, with `r' = r·(1 + 1e-9) + 1e-9·(|c.x| + |c.y|) + 1e-150`: the
/// relative term covers the rounding of `x − c.x` (relative to the
/// coordinates' magnitude) and of the sum, the absolute term the underflow of
/// tiny squares.
///
/// **Geographic** (haversine, `R` = [`EARTH_RADIUS_M`]): with `θ = r / R`,
/// `a = sin²(Δφ/2) + cos φ₁ cos φ₂ sin²(Δλ/2)` and `d = 2R·asin(√a)`:
/// - `a ≥ sin²(Δφ/2)` gives `|Δφ| ≤ θ`: latitude within `c.lat ± θ°`;
/// - `a ≥ cos φ₁ cos φ₂ sin²(Δλ/2)` and `|φ₂| ≤ |φ₁| + θ = m` give
///   `sin(|Δλ|/2) ≤ sin(θ/2) / √(cos φ₁ · cos m)`: longitude within
///   `c.lon ± 2·asin(…)`, taken modulo 360° (split in two ranges across the
///   antimeridian). Unbounded when `m` reaches a pole or the `asin`
///   argument reaches 1.
///
/// Both widened by `1e-9` relative and `1e-9°` absolute (see the constants).
/// `None` for a NaN radius (no bound is needed: the residual rejects every
/// row, and there is no literal for it); [`KeyBound::False`] for a radius no
/// distance meets (`< 0`, or `< 0` exclusive of 0).
#[must_use]
pub fn point_distance_bound(
    geographic: bool,
    center: (f64, f64),
    radius: f64,
    inclusive: bool,
) -> Option<KeyBound> {
    if radius.is_nan() {
        return None;
    }
    // A distance is never negative (or is NaN, which no comparison admits).
    if radius < 0.0 || (radius == 0.0 && !inclusive) {
        return Some(KeyBound::False);
    }
    let (cx, cy) = center;
    if !geographic {
        let r = radius * (1.0 + DISTANCE_REL_EPS)
            + DISTANCE_REL_EPS * (cx.abs() + cy.abs())
            + CARTESIAN_ABS_EPS;
        return Some(KeyBound::and([
            float_range(POINT_X, cx - r, cx + r),
            float_range(POINT_Y, cy - r, cy + r),
        ]));
    }
    let widen = |deg: f64| deg * (1.0 + DISTANCE_REL_EPS) + GEO_ABS_EPS_DEG;
    let theta = radius / EARTH_RADIUS_M;
    let dlat = widen(theta.to_degrees());
    // A keyed latitude is in [-90, 90]: a side past a pole bounds nothing.
    let lat = KeyBound::and([
        if cy - dlat > -90.0 {
            float_cmp(POINT_Y, CmpOp::GtEq, cy - dlat)
        } else {
            KeyBound::True
        },
        if cy + dlat < 90.0 {
            float_cmp(POINT_Y, CmpOp::LtEq, cy + dlat)
        } else {
            KeyBound::True
        },
    ]);
    let reach = cy.abs() + dlat;
    if reach >= 90.0 {
        return Some(lat);
    }
    let theta = widen(theta.to_degrees()).to_radians();
    let s = (theta / 2.0).sin() / (cy.to_radians().cos() * reach.to_radians().cos()).sqrt();
    // `s` may be NaN (a zero-width product): leave the longitude unbounded.
    if s.is_nan() || s >= LON_ASIN_MAX {
        return Some(lat);
    }
    let dlon = widen((2.0 * s.asin()).to_degrees());
    if dlon >= 180.0 {
        return Some(lat);
    }
    let (lo, hi) = (cx - dlon, cx + dlon);
    // A stored longitude is in [-180, 180] (other values are not keyed), and
    // the predicate's `sin²(Δλ/2)` is periodic in 360°.
    let lon = if lo <= -180.0 {
        KeyBound::or([
            float_cmp(POINT_X, CmpOp::GtEq, lo + 360.0),
            float_cmp(POINT_X, CmpOp::LtEq, hi),
        ])
    } else if hi >= 180.0 {
        KeyBound::or([
            float_cmp(POINT_X, CmpOp::GtEq, lo),
            float_cmp(POINT_X, CmpOp::LtEq, hi - 360.0),
        ])
    } else {
        float_range(POINT_X, lo, hi)
    };
    Some(KeyBound::and([lon, lat]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_eval::eval_btic_function;

    const PREDICATES: [BticPredicate; 11] = [
        BticPredicate::Overlaps,
        BticPredicate::Contains,
        BticPredicate::During,
        BticPredicate::Before,
        BticPredicate::After,
        BticPredicate::Meets,
        BticPredicate::Starts,
        BticPredicate::Finishes,
        BticPredicate::Equals,
        BticPredicate::Disjoint,
        BticPredicate::Adjacent,
    ];

    fn btic(lo: i64, hi: i64) -> Value {
        Value::Temporal(TemporalValue::Btic { lo, hi, meta: 0 })
    }

    /// A small deterministic generator: no extra dependency, reproducible.
    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// A bound from a small range, so that equal endpoints are common,
        /// or a sentinel.
        fn bound(&mut self) -> i64 {
            match self.next() % 12 {
                0 => i64::MIN,
                1 => i64::MAX,
                _ => (self.next() % 21) as i64 - 10,
            }
        }

        /// A valid interval: `lo < hi`.
        fn interval(&mut self) -> (i64, i64) {
            loop {
                let (a, b) = (self.bound(), self.bound());
                if a < b {
                    return (a, b);
                }
            }
        }
    }

    fn holds(v: Value) -> bool {
        matches!(v, Value::Bool(true))
    }

    /// Random rows and constants: whenever the exact predicate (the engine's
    /// evaluator) accepts a row, its bound admits the row's key, in both
    /// argument positions. Every BTIC bound is exact on the key, so it also
    /// admits nothing more.
    #[test]
    fn btic_bounds_admit_exactly_the_rows_the_predicate_accepts() {
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        for _ in 0..20_000 {
            let (lo, hi) = rng.interval();
            let (c_lo, c_hi) = rng.interval();
            let keys = [Some(KeyValue::Int(lo)), Some(KeyValue::Int(hi))];
            for pred in PREDICATES {
                for property_first in [true, false] {
                    let args = if property_first {
                        [btic(lo, hi), btic(c_lo, c_hi)]
                    } else {
                        [btic(c_lo, c_hi), btic(lo, hi)]
                    };
                    let exact = holds(eval_btic_function(pred.function_name(), &args).unwrap());
                    let bound = btic_bound(pred, property_first, c_lo, c_hi);
                    assert_eq!(
                        bound.admits(&keys),
                        exact,
                        "{pred:?} property_first={property_first} row=[{lo},{hi}) \
                         c=[{c_lo},{c_hi}) bound={bound:?}"
                    );
                }
            }
            let point = rng.bound();
            for t in [
                Value::Int(point),
                Value::Temporal(TemporalValue::DateTime {
                    nanos_since_epoch: ((rng.next() % 21) as i64 - 10) * 1_000_000
                        + (rng.next() % 3) as i64 * 500_000
                        - 500_000,
                    offset_seconds: 0,
                    timezone_name: None,
                }),
                Value::Temporal(TemporalValue::Date {
                    days_since_epoch: (rng.next() % 5) as i32 - 2,
                }),
            ] {
                let exact = holds(
                    eval_btic_function("BTIC_CONTAINS_POINT", &[btic(lo, hi), t.clone()]).unwrap(),
                );
                let bound = btic_point_bound(btic_point_ms(&t).unwrap());
                assert_eq!(
                    bound.admits(&keys),
                    exact,
                    "contains_point row=[{lo},{hi}) t={t:?}"
                );
            }
        }
    }

    /// The point conversions agree with the predicate's: a pre-epoch
    /// sub-millisecond datetime truncates toward zero.
    #[test]
    fn btic_point_conversion_truncates_toward_zero() {
        let dt = |n| {
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: n,
                offset_seconds: 3600,
                timezone_name: None,
            })
        };
        assert_eq!(btic_point_ms(&dt(-500_000)), Some(0));
        assert_eq!(btic_point_ms(&dt(-1_500_000)), Some(-1));
        assert_eq!(btic_point_ms(&dt(1_999_999)), Some(1));
        assert_eq!(
            btic_point_ms(&Value::Temporal(TemporalValue::LocalDateTime {
                nanos_since_epoch: -1
            })),
            Some(0)
        );
        assert_eq!(
            btic_point_ms(&Value::Temporal(TemporalValue::Date {
                days_since_epoch: -1
            })),
            Some(-86_400_000)
        );
        assert_eq!(btic_point_ms(&Value::Float(1.0)), None);
        assert_eq!(btic_point_ms(&Value::Null), None);
        // [0, 10) contains the instant -0.5 ms, as the predicate says.
        let inside = eval_btic_function("BTIC_CONTAINS_POINT", &[btic(0, 10), dt(-500_000)]);
        assert!(holds(inside.unwrap()));
        assert!(btic_point_bound(0).admits(&[Some(KeyValue::Int(0)), Some(KeyValue::Int(10))]));
    }

    /// `btic_lo(p) op c` / `btic_hi(p) op c` against the accessor's own
    /// value: for finite bounds the bound agrees with the comparison; for a
    /// sentinel (accessor NULL, predicate rejects) it may admit.
    #[test]
    fn btic_accessor_bounds_match_the_accessor_comparison() {
        let mut rng = XorShift(0xD1B5_4A32_D192_ED03);
        for _ in 0..20_000 {
            let b = (rng.next() % 21) as i64 - 10;
            let n = (rng.next() % 41) as i64 * 500_000 - 10_000_000 + (rng.next() % 3) as i64 - 1;
            let c = Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: n,
                offset_seconds: 0,
                timezone_name: None,
            });
            let accessor = i128::from(b) * 1_000_000;
            let n = i128::from(n);
            for (op, exact) in [
                (CmpOp::Lt, accessor < n),
                (CmpOp::LtEq, accessor <= n),
                (CmpOp::Gt, accessor > n),
                (CmpOp::GtEq, accessor >= n),
                (CmpOp::Eq, accessor == n),
            ] {
                for side in [BticSide::Lo, BticSide::Hi] {
                    let bound = btic_accessor_bound(side, op, &c).unwrap();
                    let mut keys = [Some(KeyValue::Int(0)), Some(KeyValue::Int(0))];
                    keys[side.column()] = Some(KeyValue::Int(b));
                    assert_eq!(bound.admits(&keys), exact, "{side:?} {b} {op:?} {n}");
                }
            }
        }
        assert!(btic_accessor_bound(BticSide::Lo, CmpOp::NotEq, &Value::Null).is_none());
        assert!(btic_accessor_bound(BticSide::Lo, CmpOp::Lt, &Value::Int(5)).is_none());
        let local = Value::Temporal(TemporalValue::LocalDateTime {
            nanos_since_epoch: 0,
        });
        assert!(btic_accessor_bound(BticSide::Hi, CmpOp::Lt, &local).is_none());
    }

    #[test]
    fn extremes_fold_and_render() {
        // `btic_contains(btic('/'), p)` holds for every interval.
        assert_eq!(
            btic_bound(BticPredicate::Contains, false, i64::MIN, i64::MAX),
            KeyBound::True
        );
        // `btic_overlaps(p, btic('2020-03/'))`: `lo < MAX` stays.
        let b = btic_bound(BticPredicate::Overlaps, true, 5, i64::MAX);
        let cols = ["lo".to_string(), "hi".to_string()];
        assert_eq!(
            b.to_filter(&cols),
            Some(FilterExpr::And(vec![
                FilterExpr::compare("lo", CmpOp::Lt, Scalar::Int(i64::MAX)),
                FilterExpr::compare("hi", CmpOp::Gt, Scalar::Int(5)),
            ]))
        );
        // A sentinel compared other than by the folded extremes still
        // renders (the SQL renderer respells `i64::MIN`).
        let b = btic_bound(BticPredicate::Before, true, i64::MIN, 0);
        assert!(b.to_filter(&cols).unwrap().to_sql().is_ok());
        assert_eq!(KeyBound::int(0, CmpOp::Lt, 1).to_filter(&[]), None);
    }
}

#[cfg(test)]
mod point_tests {
    use super::*;
    use crate::spatial::eval_spatial_function;
    use std::collections::HashMap;
    use uni_common::core::schema::KeyKind;
    use uni_store::storage::index_keys::derive;

    /// A small deterministic generator: no extra dependency, reproducible.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        /// Uniform in `[0, 1)`.
        fn unit(&mut self) -> f64 {
            (self.next() >> 11) as f64 / (1u64 << 53) as f64
        }

        fn range(&mut self, lo: f64, hi: f64) -> f64 {
            lo + (hi - lo) * self.unit()
        }

        /// Log-uniform in `[10^lo, 10^hi)`.
        fn log(&mut self, lo: f64, hi: f64) -> f64 {
            10f64.powf(self.range(lo, hi))
        }

        fn pick<T: Copy>(&mut self, items: &[T]) -> T {
            items[(self.next() % items.len() as u64) as usize]
        }
    }

    fn geo(lat: f64, lon: f64) -> Value {
        Value::Map(HashMap::from([
            ("type".to_string(), Value::String("Point".into())),
            ("crs".to_string(), Value::String("WGS84".into())),
            ("latitude".to_string(), Value::Float(lat)),
            ("longitude".to_string(), Value::Float(lon)),
        ]))
    }

    fn cartesian(x: f64, y: f64, z: Option<f64>) -> Value {
        let crs = if z.is_some() {
            "Cartesian-3D"
        } else {
            "Cartesian"
        };
        Value::Map(HashMap::from([
            ("type".to_string(), Value::String("Point".into())),
            ("crs".to_string(), Value::String(crs.into())),
            ("x".to_string(), Value::Float(x)),
            ("y".to_string(), Value::Float(y)),
            ("z".to_string(), z.map_or(Value::Null, Value::Float)),
        ]))
    }

    const GEO: KeyKind = KeyKind::PointCoords {
        geographic: true,
        three_d: false,
    };

    fn distance(a: &Value, b: &Value) -> f64 {
        match eval_spatial_function("DISTANCE", &[a.clone(), b.clone()]).unwrap() {
            Value::Float(d) => d,
            other => panic!("distance returned {other:?}"),
        }
    }

    fn within(p: &Value, ll: &Value, ur: &Value) -> bool {
        matches!(
            eval_spatial_function("POINT.WITHINBBOX", &[p.clone(), ll.clone(), ur.clone()]),
            Ok(Value::Bool(true))
        )
    }

    /// Assert that the distance bound of `(center, r)` admits `p` whenever
    /// the engine's distance, in either argument order, says it is within
    /// `r` (`<` and `<=`); returns whether `p` matched.
    fn check_distance(kind: &KeyKind, geographic: bool, center: &Value, p: &Value, r: f64) -> bool {
        // A NaN distance (near-antipodal rounding past `asin`'s domain)
        // meets no radius.
        if r.is_nan() {
            return false;
        }
        let crs = kind.point_crs().unwrap();
        let c = point_center(center, geographic, crs).expect("a center of the column's CRS");
        let keys = derive(kind, Some(p));
        assert!(keys.iter().all(Option::is_some), "{p:?} is keyed");
        let mut matched = false;
        for inclusive in [false, true] {
            let bound = point_distance_bound(geographic, c, r, inclusive).unwrap();
            for d in [distance(p, center), distance(center, p)] {
                let holds = if inclusive { d <= r } else { d < r };
                if holds {
                    matched = true;
                    assert!(
                        bound.admits(&keys),
                        "distance {d} {} {r} but the bound drops it: center {center:?} \
                         point {p:?} bound {bound:?}",
                        if inclusive { "<=" } else { "<" }
                    );
                }
            }
        }
        matched
    }

    /// A random geographic center, weighted toward the poles and the
    /// antimeridian, and the exact extremes.
    fn geo_center(rng: &mut Rng) -> (f64, f64) {
        let lat = match rng.next() % 6 {
            0 => rng.pick(&[90.0, -90.0, 0.0, -0.0]),
            1 => rng.range(89.0, 90.0) * rng.pick(&[1.0, -1.0]),
            _ => rng.range(-90.0, 90.0),
        };
        let lon = match rng.next() % 6 {
            0 => rng.pick(&[180.0, -180.0, 0.0, -0.0]),
            1 => rng.range(179.0, 180.0) * rng.pick(&[1.0, -1.0]),
            _ => rng.range(-180.0, 180.0),
        };
        (lat, lon)
    }

    /// Random centers, radii and points around them (most near the radius,
    /// where a bound that is too tight would show): every point within the
    /// engine's distance is admitted. Also each point at *exactly* the
    /// engine's distance from the center, `<=` that distance.
    #[test]
    fn geographic_distance_bound_admits_every_point_within_the_radius() {
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        let mut matched = 0usize;
        let mut admitted_far = 0usize;
        for _ in 0..4_000 {
            let (lat, lon) = geo_center(&mut rng);
            let center = geo(lat, lon);
            let r = match rng.next() % 8 {
                0 => 0.0,
                1 => rng.log(7.0, 7.4), // up to 25 000 km: past the antipode
                _ => rng.log(-3.0, 6.7),
            };
            let reach = (r / EARTH_RADIUS_M).to_degrees();
            for _ in 0..25 {
                // Within about twice the radius, wrapped into range.
                let s = rng.range(0.0, 2.2);
                let mut plat = lat + reach * s * rng.range(-1.0, 1.0);
                let mut plon =
                    lon + reach * s * rng.range(-1.0, 1.0) / lat.to_radians().cos().max(1e-6);
                if rng.next().is_multiple_of(10) {
                    plon = rng.range(-180.0, 180.0);
                }
                plat = plat.clamp(-90.0, 90.0);
                plon = ((plon + 180.0).rem_euclid(360.0)) - 180.0;
                let p = geo(plat, plon);
                matched += usize::from(check_distance(&GEO, true, &center, &p, r));
                // The point at exactly its distance.
                let d = distance(&p, &center);
                check_distance(&GEO, true, &center, &p, d);
                if r < 1e5 && d > 1e7 {
                    let c = point_center(&center, true, "WGS84").unwrap();
                    let bound = point_distance_bound(true, c, r, true).unwrap();
                    admitted_far += usize::from(bound.admits(&derive(&GEO, Some(&p))));
                }
            }
        }
        assert!(matched > 10_000, "too few matches to be a test: {matched}");
        // Away from the poles a small radius bounds tightly.
        assert!(
            admitted_far < 200,
            "a small radius admits far points: {admitted_far}"
        );
    }

    #[test]
    fn cartesian_distance_bound_admits_every_point_within_the_radius() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut matched = 0usize;
        for kind in [
            KeyKind::PointCoords {
                geographic: false,
                three_d: false,
            },
            KeyKind::PointCoords {
                geographic: false,
                three_d: true,
            },
        ] {
            let three_d = matches!(kind, KeyKind::PointCoords { three_d: true, .. });
            for _ in 0..4_000 {
                let scale = rng.pick(&[1e-160, 1e-3, 1.0, 1e3, 1e12]);
                let (cx, cy) = (scale * rng.range(-1.0, 1.0), scale * rng.range(-1.0, 1.0));
                let cz = three_d.then(|| scale * rng.range(-1.0, 1.0));
                let center = cartesian(rng.pick(&[cx, 0.0, -0.0]), cy, cz);
                let r = match rng.next() % 6 {
                    0 => 0.0,
                    _ => scale * rng.log(-12.0, 1.0),
                };
                for _ in 0..25 {
                    let s = rng.range(0.0, 2.2) * r;
                    let p = cartesian(
                        cx + s * rng.range(-1.0, 1.0),
                        cy + s * rng.range(-1.0, 1.0),
                        cz.map(|z| z + s * rng.range(-1.0, 1.0)),
                    );
                    matched += usize::from(check_distance(&kind, false, &center, &p, r));
                    let d = distance(&p, &center);
                    check_distance(&kind, false, &center, &p, d);
                }
            }
        }
        assert!(matched > 20_000, "too few matches to be a test: {matched}");
    }

    #[test]
    fn distance_bound_edges() {
        let c = (1.0, 2.0);
        assert_eq!(
            point_distance_bound(false, c, -1.0, true),
            Some(KeyBound::False)
        );
        assert_eq!(
            point_distance_bound(false, c, 0.0, false),
            Some(KeyBound::False)
        );
        assert_eq!(point_distance_bound(false, c, f64::NAN, true), None);
        // An infinite radius bounds nothing.
        assert_eq!(
            point_distance_bound(false, c, f64::INFINITY, true),
            Some(KeyBound::True)
        );
        // Near a pole the longitude is unbounded; across the antimeridian it
        // is two ranges.
        let polar = point_distance_bound(true, (10.0, 89.99), 2_000.0, true).unwrap();
        // Only `lat >= 89.97…`: the disk reaches the pole.
        assert!(
            matches!(
                polar,
                KeyBound::Cmp {
                    column: POINT_Y,
                    op: CmpOp::GtEq,
                    ..
                }
            ),
            "{polar:?}"
        );
        // Past the antipode nothing is bounded.
        assert_eq!(
            point_distance_bound(true, (0.0, 0.0), 2.5e7, true),
            Some(KeyBound::True)
        );
        assert!(polar.admits(&[Some(KeyValue::Float(-170.0)), Some(KeyValue::Float(89.995))]));
        let wrap = point_distance_bound(true, (179.99, 0.0), 10_000.0, true).unwrap();
        let admits =
            |lon: f64| wrap.admits(&[Some(KeyValue::Float(lon)), Some(KeyValue::Float(0.0))]);
        assert!(admits(179.95) && admits(180.0) && admits(-180.0) && admits(-179.95));
        assert!(!admits(0.0) && !admits(179.0) && !admits(-179.0));
        // Renders: the SQL dialect has no literal for an infinite bound.
        let cols = ["px".to_string(), "py".to_string()];
        assert!(wrap.to_filter(&cols).unwrap().to_sql().is_ok());
        assert!(
            point_distance_bound(false, (1e300, 0.0), 1e308, true)
                .unwrap()
                .to_filter(&cols)
                .unwrap()
                .to_sql()
                .is_ok()
        );
    }

    /// The constants are read as the predicates read them; a center of
    /// another CRS is not bounded.
    #[test]
    fn centers_and_corners() {
        let xy = cartesian(1.0, -0.0, None);
        assert_eq!(point_corner(&xy, false), Some((1.0, -0.0)));
        assert_eq!(point_center(&xy, false, "Cartesian"), Some((1.0, -0.0)));
        assert_eq!(point_center(&xy, false, "Cartesian-3D"), None);
        assert_eq!(point_corner(&xy, true), None);
        let g = geo(10.0, 20.0);
        assert_eq!(point_corner(&g, true), Some((20.0, 10.0)));
        assert_eq!(point_center(&g, true, "WGS84"), Some((20.0, 10.0)));
        assert_eq!(point_center(&g, false, "Cartesian"), None);
        // A corner's CRS is not compared (withinBBox does not).
        let plain = Value::Map(HashMap::from([
            ("x".to_string(), Value::Int(3)),
            ("y".to_string(), Value::Float(4.0)),
        ]));
        assert_eq!(point_corner(&plain, false), Some((3.0, 4.0)));
        assert_eq!(point_center(&plain, false, "Cartesian"), None);
        assert_eq!(point_corner(&cartesian(f64::NAN, 0.0, None), false), None);
        assert_eq!(point_center(&geo(0.0, 200.0), true, "WGS84"), None);
        assert_eq!(point_corner(&Value::Null, false), None);
        // `-0.0` is rendered as the keys are, `+0.0`.
        let b = point_bbox_bound(false, (-0.0, -0.0), (1.0, 1.0));
        assert_eq!(
            b.to_filter(&["px".to_string(), "py".to_string()])
                .unwrap()
                .to_sql()
                .unwrap()
                .matches("-0")
                .count(),
            0
        );
    }

    /// `withinBBox` against its bound, exhaustively over a small grid of
    /// coordinates (`-0.0` and the corners themselves included), geographic
    /// boxes crossing the antimeridian included: the bound is exact.
    #[test]
    fn bbox_bound_is_exact() {
        let grid = [-180.0, -10.0, -1.0, -0.0, 0.0, 1.0, 10.0, 180.0];
        let lat_grid = [-90.0, -1.0, -0.0, 0.0, 1.0, 90.0];
        for geographic in [false, true] {
            let kind = KeyKind::PointCoords {
                geographic,
                three_d: false,
            };
            let ys: &[f64] = if geographic { &lat_grid } else { &grid };
            let make = |x: f64, y: f64| {
                if geographic {
                    geo(y, x)
                } else {
                    cartesian(x, y, None)
                }
            };
            for &llx in &grid {
                for &urx in &grid {
                    for &(lly, ury) in &[(-1.0, 1.0), (0.0, -0.0), (1.0, -1.0), (-90.0, 90.0)] {
                        let (ll, ur) = (make(llx, lly), make(urx, ury));
                        let bound = point_bbox_bound(
                            geographic,
                            point_corner(&ll, geographic).unwrap(),
                            point_corner(&ur, geographic).unwrap(),
                        );
                        for &x in &grid {
                            for &y in ys {
                                let p = make(x, y);
                                assert_eq!(
                                    bound.admits(&derive(&kind, Some(&p))),
                                    within(&p, &ll, &ur),
                                    "geographic={geographic} p=({x},{y}) \
                                     ll=({llx},{lly}) ur=({urx},{ury}) {bound:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
