use uni_common::value::{TemporalValue, Value};
use uni_query::query::expr_eval::{eval_scalar_function, is_scalar_function};

/// Helper: create a BTIC value for the year 1985.
fn btic_year_1985() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 473_385_600_000,         // 1985-01-01
        hi: 504_921_600_000,         // 1986-01-01
        meta: 0x7700_0000_0000_0000, // year/year, definite/definite
    })
}

/// Helper: create a fully-unbounded BTIC value.
fn btic_unbounded() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: i64::MIN,
        hi: i64::MAX,
        meta: 0,
    })
}

/// Helper: create a BTIC instant at epoch.
fn btic_epoch_instant() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 0,
        hi: 1,
        meta: 0x0000_0000_0000_0000,
    })
}

/// Helper: create a BTIC value with approximate certainty (500 BCE).
fn btic_500_bce_approx() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: -77_914_137_600_000,
        hi: -77_882_601_600_000,
        meta: 0x7750_0000_0000_0000, // year/year, approximate/approximate
    })
}

// -----------------------------------------------------------------------
// Scalar function discovery
// -----------------------------------------------------------------------

#[test]
fn btic_functions_are_recognized_as_scalar() {
    let names = [
        "btic_lo",
        "btic_hi",
        "btic_duration",
        "btic_contains_point",
        "btic_overlaps",
        "btic_is_instant",
        "btic_is_unbounded",
        "btic_is_finite",
        "btic_granularity",
        "btic_lo_granularity",
        "btic_hi_granularity",
        "btic_certainty",
        "btic_lo_certainty",
        "btic_hi_certainty",
    ];
    for name in names {
        assert!(
            is_scalar_function(name),
            "{name} should be recognized as scalar"
        );
    }
}

// -----------------------------------------------------------------------
// Accessors
// -----------------------------------------------------------------------

#[test]
fn btic_lo_returns_datetime() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_lo", &[val], None).unwrap();
    // lo = 473_385_600_000 ms → nanos = 473_385_600_000 * 1_000_000
    match result {
        Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch, ..
        }) => {
            assert_eq!(nanos_since_epoch, 473_385_600_000 * 1_000_000);
        }
        other => panic!("expected DateTime, got: {other:?}"),
    }
}

#[test]
fn btic_lo_returns_null_for_neg_inf() {
    let val = btic_unbounded();
    let result = eval_scalar_function("btic_lo", &[val], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_hi_returns_null_for_pos_inf() {
    let val = btic_unbounded();
    let result = eval_scalar_function("btic_hi", &[val], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_duration_finite() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_duration", &[val], None).unwrap();
    // 1985 is not a leap year: 365 days = 31_536_000_000 ms
    assert_eq!(result, Value::Int(504_921_600_000 - 473_385_600_000));
}

#[test]
fn btic_duration_unbounded_returns_null() {
    let val = btic_unbounded();
    let result = eval_scalar_function("btic_duration", &[val], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_is_instant_true() {
    let val = btic_epoch_instant();
    let result = eval_scalar_function("btic_is_instant", &[val], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_is_instant_false() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_is_instant", &[val], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

#[test]
fn btic_is_unbounded_true() {
    let val = btic_unbounded();
    let result = eval_scalar_function("btic_is_unbounded", &[val], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_is_unbounded_false() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_is_unbounded", &[val], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

#[test]
fn btic_is_finite_true() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_is_finite", &[val], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_is_finite_false() {
    let val = btic_unbounded();
    let result = eval_scalar_function("btic_is_finite", &[val], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

// -----------------------------------------------------------------------
// Granularity and certainty
// -----------------------------------------------------------------------

#[test]
fn btic_granularity_returns_year() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_granularity", &[val], None).unwrap();
    assert_eq!(result, Value::String("year".to_string()));
}

#[test]
fn btic_lo_granularity_vs_hi_granularity() {
    // Mixed granularity: month lo, day hi
    let meta = uni_btic::Btic::build_meta(
        uni_btic::Granularity::Month,
        uni_btic::Granularity::Day,
        uni_btic::Certainty::Definite,
        uni_btic::Certainty::Definite,
    );
    let val = Value::Temporal(TemporalValue::Btic {
        lo: 478_483_200_000,   // 1985-03-01
        hi: 1_718_496_000_000, // 2024-06-16
        meta,
    });
    let lo_g = eval_scalar_function("btic_lo_granularity", &[val.clone()], None).unwrap();
    let hi_g = eval_scalar_function("btic_hi_granularity", &[val], None).unwrap();
    assert_eq!(lo_g, Value::String("month".to_string()));
    assert_eq!(hi_g, Value::String("day".to_string()));
}

#[test]
fn btic_certainty_approximate() {
    let val = btic_500_bce_approx();
    let result = eval_scalar_function("btic_certainty", &[val], None).unwrap();
    assert_eq!(result, Value::String("approximate".to_string()));
}

#[test]
fn btic_certainty_definite() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_certainty", &[val], None).unwrap();
    assert_eq!(result, Value::String("definite".to_string()));
}

// -----------------------------------------------------------------------
// Predicates
// -----------------------------------------------------------------------

#[test]
fn btic_contains_point_inside() {
    let val = btic_year_1985();
    // Mid-1985: July 1, 1985 = 489_024_000_000 ms
    let point = Value::Int(489_024_000_000);
    let result = eval_scalar_function("btic_contains_point", &[val, point], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_contains_point_outside() {
    let val = btic_year_1985();
    // Jan 1, 1984 = 441_763_200_000 ms (before 1985)
    let point = Value::Int(441_763_200_000);
    let result = eval_scalar_function("btic_contains_point", &[val, point], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

#[test]
fn btic_contains_point_lo_inclusive() {
    let val = btic_year_1985();
    // Exactly at lo boundary (1985-01-01)
    let point = Value::Int(473_385_600_000);
    let result = eval_scalar_function("btic_contains_point", &[val, point], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_contains_point_hi_exclusive() {
    let val = btic_year_1985();
    // Exactly at hi boundary (1986-01-01) — should be excluded
    let point = Value::Int(504_921_600_000);
    let result = eval_scalar_function("btic_contains_point", &[val, point], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

#[test]
fn btic_contains_point_with_datetime() {
    let val = btic_year_1985();
    // July 1, 1985 as DateTime (nanos_since_epoch)
    let point = Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: 489_024_000_000 * 1_000_000,
        offset_seconds: 0,
        timezone_name: Some("UTC".to_string()),
    });
    let result = eval_scalar_function("btic_contains_point", &[val, point], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_overlaps_true() {
    let a = btic_year_1985(); // [1985, 1986)
    // [1985-06, 1990) overlaps with 1985
    let meta = 0x5500_0000_0000_0000u64; // month/month
    let b = Value::Temporal(TemporalValue::Btic {
        lo: 486_432_000_000, // 1985-06-01
        hi: 631_152_000_000, // 1990-01-01
        meta,
    });
    let result = eval_scalar_function("btic_overlaps", &[a, b], None).unwrap();
    assert_eq!(result, Value::Bool(true));
}

#[test]
fn btic_overlaps_false_adjacent() {
    let a = btic_year_1985(); // [1985, 1986)
    // [1986, 1987) is adjacent but not overlapping
    let meta = 0x7700_0000_0000_0000u64;
    let b = Value::Temporal(TemporalValue::Btic {
        lo: 504_921_600_000, // 1986-01-01
        hi: 536_457_600_000, // 1987-01-01
        meta,
    });
    let result = eval_scalar_function("btic_overlaps", &[a, b], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

#[test]
fn btic_overlaps_false_disjoint() {
    let a = btic_year_1985(); // [1985, 1986)
    // [2020, 2021)
    let meta = 0x7700_0000_0000_0000u64;
    let b = Value::Temporal(TemporalValue::Btic {
        lo: 1_577_836_800_000, // 2020-01-01
        hi: 1_609_459_200_000, // 2021-01-01
        meta,
    });
    let result = eval_scalar_function("btic_overlaps", &[a, b], None).unwrap();
    assert_eq!(result, Value::Bool(false));
}

// -----------------------------------------------------------------------
// NULL propagation
// -----------------------------------------------------------------------

#[test]
fn btic_functions_null_propagation() {
    let null = Value::Null;

    assert_eq!(
        eval_scalar_function("btic_lo", &[null.clone()], None).unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_scalar_function("btic_hi", &[null.clone()], None).unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_scalar_function("btic_duration", &[null.clone()], None).unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_scalar_function("btic_is_instant", &[null.clone()], None).unwrap(),
        Value::Null
    );

    let val = btic_year_1985();
    assert_eq!(
        eval_scalar_function("btic_contains_point", &[val.clone(), null.clone()], None).unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_scalar_function("btic_overlaps", &[val, null], None).unwrap(),
        Value::Null
    );
}

// -----------------------------------------------------------------------
// Error cases
// -----------------------------------------------------------------------

#[test]
fn btic_lo_wrong_type() {
    let result = eval_scalar_function("btic_lo", &[Value::Int(42)], None);
    assert!(result.is_err());
}

#[test]
fn btic_lo_wrong_arg_count() {
    let result = eval_scalar_function("btic_lo", &[], None);
    assert!(result.is_err());
}

#[test]
fn btic_overlaps_wrong_types() {
    let btic = btic_year_1985();
    let result = eval_scalar_function("btic_overlaps", &[btic, Value::Int(42)], None);
    assert!(result.is_err());
}

// =======================================================================
// Phase 2: Interval predicates
// =======================================================================

/// Helper: [1986, 1987)
fn year_1986() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 504_921_600_000, // 1986-01-01
        hi: 536_457_600_000, // 1987-01-01
        meta: 0x7700_0000_0000_0000,
    })
}

/// Helper: [1985-06, 1990)
fn mid85_to_90() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 486_432_000_000,         // 1985-06-01
        hi: 631_152_000_000,         // 1990-01-01
        meta: 0x5500_0000_0000_0000, // month/month
    })
}

/// Helper: [1985-03, 1985-09)  (strictly inside 1985)
fn mid_1985() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 478_483_200_000, // 1985-03-01
        hi: 494_294_400_000, // 1985-09-01
        meta: 0x5500_0000_0000_0000,
    })
}

/// Helper: [2020, 2021)
fn year_2020() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 1_577_836_800_000, // 2020-01-01
        hi: 1_609_459_200_000, // 2021-01-01
        meta: 0x7700_0000_0000_0000,
    })
}

// -- btic_contains --

#[test]
fn btic_contains_strict() {
    let outer = btic_year_1985();
    let inner = mid_1985();
    assert_eq!(
        eval_scalar_function("btic_contains", &[outer.clone(), inner.clone()], None).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_scalar_function("btic_contains", &[inner, outer], None).unwrap(),
        Value::Bool(false)
    );
}

#[test]
fn btic_contains_self() {
    let a = btic_year_1985();
    assert_eq!(
        eval_scalar_function("btic_contains", &[a.clone(), a], None).unwrap(),
        Value::Bool(true)
    );
}

// -- btic_before / btic_after --

#[test]
fn btic_before_true() {
    assert_eq!(
        eval_scalar_function("btic_before", &[btic_year_1985(), year_2020()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_before_adjacent() {
    // [1985,1986) before [1986,1987): a.hi=1986 <= b.lo=1986 → true
    assert_eq!(
        eval_scalar_function("btic_before", &[btic_year_1985(), year_1986()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_before_overlapping() {
    assert_eq!(
        eval_scalar_function("btic_before", &[btic_year_1985(), mid85_to_90()], None).unwrap(),
        Value::Bool(false)
    );
}

#[test]
fn btic_after_true() {
    assert_eq!(
        eval_scalar_function("btic_after", &[year_2020(), btic_year_1985()], None).unwrap(),
        Value::Bool(true)
    );
}

// -- btic_meets --

#[test]
fn btic_meets_true() {
    // [1985,1986) meets [1986,1987): a.hi == b.lo
    assert_eq!(
        eval_scalar_function("btic_meets", &[btic_year_1985(), year_1986()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_meets_false_gap() {
    assert_eq!(
        eval_scalar_function("btic_meets", &[btic_year_1985(), year_2020()], None).unwrap(),
        Value::Bool(false)
    );
}

// -- btic_adjacent --

#[test]
fn btic_adjacent_forward() {
    assert_eq!(
        eval_scalar_function("btic_adjacent", &[btic_year_1985(), year_1986()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_adjacent_reverse() {
    assert_eq!(
        eval_scalar_function("btic_adjacent", &[year_1986(), btic_year_1985()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_adjacent_gap() {
    assert_eq!(
        eval_scalar_function("btic_adjacent", &[btic_year_1985(), year_2020()], None).unwrap(),
        Value::Bool(false)
    );
}

// -- btic_disjoint --

#[test]
fn btic_disjoint_true() {
    assert_eq!(
        eval_scalar_function("btic_disjoint", &[btic_year_1985(), year_2020()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_disjoint_adjacent_is_disjoint() {
    assert_eq!(
        eval_scalar_function("btic_disjoint", &[btic_year_1985(), year_1986()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_disjoint_overlapping() {
    assert_eq!(
        eval_scalar_function("btic_disjoint", &[btic_year_1985(), mid85_to_90()], None).unwrap(),
        Value::Bool(false)
    );
}

// -- btic_equals --

#[test]
fn btic_equals_same_bounds() {
    assert_eq!(
        eval_scalar_function("btic_equals", &[btic_year_1985(), btic_year_1985()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_equals_same_bounds_different_meta() {
    // Same lo/hi but different granularity → temporally equal
    let a = btic_year_1985(); // year granularity
    let b = Value::Temporal(TemporalValue::Btic {
        lo: 473_385_600_000,
        hi: 504_921_600_000,
        meta: 0x4400_0000_0000_0000, // day/day granularity
    });
    assert_eq!(
        eval_scalar_function("btic_equals", &[a, b], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_equals_different_bounds() {
    assert_eq!(
        eval_scalar_function("btic_equals", &[btic_year_1985(), year_1986()], None).unwrap(),
        Value::Bool(false)
    );
}

// -- btic_starts / btic_during / btic_finishes --

#[test]
fn btic_starts_true() {
    // mid_1985 starts at 1985-03, year_1985 starts at 1985-01 → doesn't start
    // Need an interval that shares the lo with year_1985
    let starts_1985 = Value::Temporal(TemporalValue::Btic {
        lo: 473_385_600_000, // 1985-01-01 (same as year_1985)
        hi: 486_432_000_000, // 1985-06-01 (shorter)
        meta: 0x5500_0000_0000_0000,
    });
    assert_eq!(
        eval_scalar_function("btic_starts", &[starts_1985, btic_year_1985()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_during_true() {
    assert_eq!(
        eval_scalar_function("btic_during", &[mid_1985(), btic_year_1985()], None).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn btic_finishes_true() {
    let finishes_1985 = Value::Temporal(TemporalValue::Btic {
        lo: 486_432_000_000, // 1985-06-01 (later start)
        hi: 504_921_600_000, // 1986-01-01 (same as year_1985 hi)
        meta: 0x5500_0000_0000_0000,
    });
    assert_eq!(
        eval_scalar_function("btic_finishes", &[finishes_1985, btic_year_1985()], None).unwrap(),
        Value::Bool(true)
    );
}

// =======================================================================
// Phase 2: Set operations
// =======================================================================

#[test]
fn btic_intersection_overlapping() {
    let result = eval_scalar_function(
        "btic_intersection",
        &[btic_year_1985(), mid85_to_90()],
        None,
    )
    .unwrap();
    match result {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(lo, 486_432_000_000); // max(1985-01, 1985-06) = 1985-06
            assert_eq!(hi, 504_921_600_000); // min(1986-01, 1990-01) = 1986-01
        }
        other => panic!("expected BTIC, got {other:?}"),
    }
}

#[test]
fn btic_intersection_disjoint_returns_null() {
    let result =
        eval_scalar_function("btic_intersection", &[btic_year_1985(), year_2020()], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_intersection_adjacent_returns_null() {
    let result =
        eval_scalar_function("btic_intersection", &[btic_year_1985(), year_1986()], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_span_overlapping() {
    let result =
        eval_scalar_function("btic_span", &[btic_year_1985(), mid85_to_90()], None).unwrap();
    match result {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(lo, 473_385_600_000); // min(1985-01, 1985-06) = 1985-01
            assert_eq!(hi, 631_152_000_000); // max(1986-01, 1990-01) = 1990-01
        }
        other => panic!("expected BTIC, got {other:?}"),
    }
}

#[test]
fn btic_span_disjoint() {
    let result = eval_scalar_function("btic_span", &[btic_year_1985(), year_2020()], None).unwrap();
    match result {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(lo, 473_385_600_000); // 1985-01
            assert_eq!(hi, 1_609_459_200_000); // 2021-01
        }
        other => panic!("expected BTIC, got {other:?}"),
    }
}

// -- Phase 2 NULL propagation --

#[test]
fn phase2_null_propagation() {
    let null = Value::Null;
    let val = btic_year_1985();

    for fname in &[
        "btic_contains",
        "btic_before",
        "btic_after",
        "btic_meets",
        "btic_adjacent",
        "btic_disjoint",
        "btic_equals",
        "btic_starts",
        "btic_during",
        "btic_finishes",
        "btic_intersection",
        "btic_span",
    ] {
        assert_eq!(
            eval_scalar_function(fname, &[val.clone(), null.clone()], None).unwrap(),
            Value::Null,
            "{fname} should propagate NULL"
        );
        assert_eq!(
            eval_scalar_function(fname, &[null.clone(), val.clone()], None).unwrap(),
            Value::Null,
            "{fname} should propagate NULL (reversed)"
        );
    }
}

// -- Phase 2 scalar discovery --

#[test]
fn phase2_functions_are_recognized_as_scalar() {
    let names = [
        "btic_contains",
        "btic_before",
        "btic_after",
        "btic_meets",
        "btic_adjacent",
        "btic_disjoint",
        "btic_equals",
        "btic_starts",
        "btic_during",
        "btic_finishes",
        "btic_intersection",
        "btic_span",
    ];
    for name in names {
        assert!(
            is_scalar_function(name),
            "{name} should be recognized as scalar"
        );
    }
}

// =======================================================================
// Phase 3: btic_gap
// =======================================================================

#[test]
fn btic_gap_disjoint() {
    let result = eval_scalar_function("btic_gap", &[btic_year_1985(), year_2020()], None).unwrap();
    match result {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(lo, 504_921_600_000); // 1986-01-01 (end of 1985)
            assert_eq!(hi, 1_577_836_800_000); // 2020-01-01 (start of 2020)
        }
        other => panic!("expected BTIC, got {other:?}"),
    }
}

#[test]
fn btic_gap_overlapping_returns_null() {
    let result =
        eval_scalar_function("btic_gap", &[btic_year_1985(), mid85_to_90()], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_gap_adjacent_returns_null() {
    let result = eval_scalar_function("btic_gap", &[btic_year_1985(), year_1986()], None).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn btic_gap_null_propagation() {
    assert_eq!(
        eval_scalar_function("btic_gap", &[btic_year_1985(), Value::Null], None).unwrap(),
        Value::Null,
    );
}

#[test]
fn btic_gap_is_recognized_as_scalar() {
    assert!(is_scalar_function("btic_gap"));
}

// =======================================================================
// Per-bound certainty accessors
// =======================================================================

#[test]
fn btic_lo_certainty_definite() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_lo_certainty", &[val], None).unwrap();
    assert_eq!(result, Value::String("definite".to_string()));
}

#[test]
fn btic_lo_certainty_approximate() {
    let val = btic_500_bce_approx();
    let result = eval_scalar_function("btic_lo_certainty", &[val], None).unwrap();
    assert_eq!(result, Value::String("approximate".to_string()));
}

#[test]
fn btic_hi_certainty_definite() {
    let val = btic_year_1985();
    let result = eval_scalar_function("btic_hi_certainty", &[val], None).unwrap();
    assert_eq!(result, Value::String("definite".to_string()));
}

#[test]
fn btic_hi_certainty_approximate() {
    let val = btic_500_bce_approx();
    let result = eval_scalar_function("btic_hi_certainty", &[val], None).unwrap();
    assert_eq!(result, Value::String("approximate".to_string()));
}

#[test]
fn btic_mixed_certainty_lo_vs_hi() {
    // Approximate lo, Definite hi — verify per-bound accessors return different values
    let meta = uni_btic::Btic::build_meta(
        uni_btic::Granularity::Year,
        uni_btic::Granularity::Month,
        uni_btic::Certainty::Approximate,
        uni_btic::Certainty::Definite,
    );
    let val = Value::Temporal(TemporalValue::Btic {
        lo: 473_385_600_000,   // 1985-01-01
        hi: 1_719_792_000_000, // 2024-07-01
        meta,
    });

    let lo_c = eval_scalar_function("btic_lo_certainty", &[val.clone()], None).unwrap();
    let hi_c = eval_scalar_function("btic_hi_certainty", &[val.clone()], None).unwrap();
    let combined = eval_scalar_function("btic_certainty", &[val], None).unwrap();

    assert_eq!(lo_c, Value::String("approximate".to_string()));
    assert_eq!(hi_c, Value::String("definite".to_string()));
    // btic_certainty returns least_certain = approximate
    assert_eq!(combined, Value::String("approximate".to_string()));
}

#[test]
fn btic_certainty_functions_null_propagation() {
    let null = Value::Null;

    assert_eq!(
        eval_scalar_function("btic_certainty", &[null.clone()], None).unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_scalar_function("btic_lo_certainty", &[null.clone()], None).unwrap(),
        Value::Null
    );
    assert_eq!(
        eval_scalar_function("btic_hi_certainty", &[null], None).unwrap(),
        Value::Null
    );
}

// =======================================================================
// BTIC comparison operators
// =======================================================================

#[test]
fn btic_comparison_ordering() {
    use uni_cypher::ast::BinaryOp;
    use uni_query::query::expr_eval::eval_binary_op;

    let earlier = btic_year_1985(); // [1985, 1986)
    let later = year_2020(); // [2020, 2021)

    // earlier < later
    assert_eq!(
        eval_binary_op(&earlier, &BinaryOp::Lt, &later).unwrap(),
        Value::Bool(true)
    );
    // later > earlier
    assert_eq!(
        eval_binary_op(&later, &BinaryOp::Gt, &earlier).unwrap(),
        Value::Bool(true)
    );
    // earlier < earlier (equal → false)
    assert_eq!(
        eval_binary_op(&earlier, &BinaryOp::Lt, &earlier).unwrap(),
        Value::Bool(false)
    );
    // earlier <= earlier (equal → true)
    assert_eq!(
        eval_binary_op(&earlier, &BinaryOp::LtEq, &earlier).unwrap(),
        Value::Bool(true)
    );
    // earlier >= later (false)
    assert_eq!(
        eval_binary_op(&earlier, &BinaryOp::GtEq, &later).unwrap(),
        Value::Bool(false)
    );
    // earlier <> later (not equal → true)
    assert_eq!(
        eval_binary_op(&earlier, &BinaryOp::NotEq, &later).unwrap(),
        Value::Bool(true)
    );
    // earlier <> earlier (equal → false)
    assert_eq!(
        eval_binary_op(&earlier, &BinaryOp::NotEq, &earlier).unwrap(),
        Value::Bool(false)
    );
}

// -----------------------------------------------------------------------
// Bounds outside the datetime range
// -----------------------------------------------------------------------

#[test]
fn btic_lo_outside_datetime_range_is_an_error() {
    // 500 BCE in milliseconds is finite, but in nanoseconds it overflows i64.
    let err = eval_scalar_function("btic_lo", &[btic_500_bce_approx()], None).unwrap_err();
    assert!(
        err.to_string().contains("outside the datetime range"),
        "unexpected error: {err}"
    );
}

#[test]
fn btic_hi_outside_datetime_range_is_an_error() {
    let year_3000 = Value::Temporal(TemporalValue::Btic {
        lo: 32_503_680_000_000, // 3000-01-01
        hi: 32_535_216_000_000, // 3001-01-01
        meta: 0x7700_0000_0000_0000,
    });
    let err = eval_scalar_function("btic_hi", &[year_3000], None).unwrap_err();
    assert!(
        err.to_string().contains("outside the datetime range"),
        "unexpected error: {err}"
    );
}
