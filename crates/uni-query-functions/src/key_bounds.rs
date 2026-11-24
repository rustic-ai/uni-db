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
