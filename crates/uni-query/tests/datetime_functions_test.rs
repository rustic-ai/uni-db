// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_common::unival;
use uni_query::query::expr_eval::eval_scalar_function;

#[test]
fn test_date_function() {
    let res = eval_scalar_function("DATE", &[unival!("2023-01-15")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15");

    let res = eval_scalar_function("DATE", &[unival!("2023-01-15 10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15");

    // Current date (no args)
    let res = eval_scalar_function("DATE", &[]).unwrap();
    assert!(res.to_string().len() == 10);
}

#[test]
fn test_time_function() {
    // Cypher time() always includes timezone (defaults to Z when unspecified).
    // TemporalValue::Time Display omits :SS when seconds and nanos are zero.
    let res = eval_scalar_function("TIME", &[unival!("10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "10:30Z");

    // Time with non-zero seconds includes seconds and timezone.
    let res = eval_scalar_function("TIME", &[unival!("10:30:45")]).unwrap();
    assert_eq!(res.to_string(), "10:30:45Z");

    // Time with explicit timezone preserves it.
    let res = eval_scalar_function("TIME", &[unival!("10:30:45+01:00")]).unwrap();
    assert_eq!(res.to_string(), "10:30:45+01:00");
}

#[test]
fn test_datetime_function() {
    // Standard Cypher datetime uses T separator (not space).
    // TemporalValue::DateTime Display omits :SS when seconds and nanos are zero.
    let res = eval_scalar_function("DATETIME", &[unival!("2023-01-15T10:30:00Z")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15T10:30Z");

    // Datetime with explicit timezone.
    let res = eval_scalar_function("DATETIME", &[unival!("2023-01-15T10:30:00+05:00")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15T10:30+05:00");
}

#[test]
fn test_extract_functions() {
    let dt = unival!("2023-01-15 10:30:45");

    assert_eq!(
        eval_scalar_function("YEAR", std::slice::from_ref(&dt))
            .unwrap()
            .as_i64()
            .unwrap(),
        2023
    );
    assert_eq!(
        eval_scalar_function("MONTH", std::slice::from_ref(&dt))
            .unwrap()
            .as_i64()
            .unwrap(),
        1
    );
    assert_eq!(
        eval_scalar_function("DAY", std::slice::from_ref(&dt))
            .unwrap()
            .as_i64()
            .unwrap(),
        15
    );
    assert_eq!(
        eval_scalar_function("HOUR", std::slice::from_ref(&dt))
            .unwrap()
            .as_i64()
            .unwrap(),
        10
    );
    assert_eq!(
        eval_scalar_function("MINUTE", std::slice::from_ref(&dt))
            .unwrap()
            .as_i64()
            .unwrap(),
        30
    );
    assert_eq!(
        eval_scalar_function("SECOND", std::slice::from_ref(&dt))
            .unwrap()
            .as_i64()
            .unwrap(),
        45
    );
}

#[test]
fn test_localdatetime_function() {
    // localdatetime() returns current local time as TemporalValue::LocalDateTime
    let res = eval_scalar_function("LOCALDATETIME", &[]).unwrap();
    let s = res.to_string();
    // Should contain T separator in display
    assert!(s.contains("T"), "Expected format with T separator");
    assert!(s.len() >= 16, "Expected at least YYYY-MM-DDTHH:MM");

    // Should work with string argument too
    // TemporalValue::LocalDateTime Display omits :SS when seconds and nanos are zero
    let res = eval_scalar_function("LOCALDATETIME", &[unival!("2023-01-15T10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15T10:30");
}

#[test]
fn test_localtime_function() {
    // localtime() returns current local time as TemporalValue::LocalTime
    let res = eval_scalar_function("LOCALTIME", &[]).unwrap();
    let s = res.to_string();
    // Should be in format HH:MM or HH:MM:SS
    assert!(s.contains(":"), "Expected time format with colons");
    assert!(s.len() >= 5, "Expected at least HH:MM");

    // Should work with string argument too
    // TemporalValue::LocalTime Display omits :SS when seconds and nanos are zero
    let res = eval_scalar_function("LOCALTIME", &[unival!("10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "10:30");
}

// ============================================================================
// Comprehensive format tests from TCK Temporal2.feature
// ============================================================================

#[test]
fn test_date_all_string_formats() {
    // TCK Scenario [1]: Should parse date from string
    let cases = [
        ("2015-07-21", "2015-07-21"),
        ("20150721", "2015-07-21"),
        ("2015-07", "2015-07-01"),
        ("201507", "2015-07-01"),
        ("2015-W30-2", "2015-07-21"),
        ("2015W302", "2015-07-21"),
        ("2015-W30", "2015-07-20"),
        ("2015W30", "2015-07-20"),
        ("2015-202", "2015-07-21"),
        ("2015202", "2015-07-21"),
        ("2015", "2015-01-01"),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("DATE", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("DATE({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "DATE({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}

#[test]
fn test_localtime_all_string_formats() {
    // TCK Scenario [2]: Should parse local time from string
    let cases = [
        ("21:40:32.142", "21:40:32.142"),
        ("214032.142", "21:40:32.142"),
        ("21:40:32", "21:40:32"),
        ("214032", "21:40:32"),
        ("21:40", "21:40"),
        ("2140", "21:40"),
        ("21", "21:00"),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("LOCALTIME", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("LOCALTIME({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "LOCALTIME({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}

#[test]
fn test_time_all_string_formats() {
    // TCK Scenario [3]: Should parse time from string
    let cases = [
        ("21:40:32.142+0100", "21:40:32.142+01:00"),
        ("214032.142Z", "21:40:32.142Z"),
        ("21:40:32+01:00", "21:40:32+01:00"),
        ("214032-0100", "21:40:32-01:00"),
        ("21:40-01:30", "21:40-01:30"),
        ("2140-00:00", "21:40Z"),
        ("2140-02", "21:40-02:00"),
        ("22+18:00", "22:00+18:00"),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("TIME", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("TIME({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "TIME({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}

#[test]
fn test_localdatetime_all_string_formats() {
    // TCK Scenario [4]: Should parse local date time from string
    let cases = [
        ("2015-07-21T21:40:32.142", "2015-07-21T21:40:32.142"),
        ("2015-W30-2T214032.142", "2015-07-21T21:40:32.142"),
        ("2015-202T21:40:32", "2015-07-21T21:40:32"),
        ("2015T214032", "2015-01-01T21:40:32"),
        ("20150721T21:40", "2015-07-21T21:40"),
        ("2015-W30T2140", "2015-07-20T21:40"),
        ("2015202T21", "2015-07-21T21:00"),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("LOCALDATETIME", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("LOCALDATETIME({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "LOCALDATETIME({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}

#[test]
fn test_datetime_all_string_formats() {
    // TCK Scenario [5]: Should parse date time from string
    let cases = [
        ("2015-07-21T21:40:32.142+0100", "2015-07-21T21:40:32.142+01:00"),
        ("2015-W30-2T214032.142Z", "2015-07-21T21:40:32.142Z"),
        ("2015-202T21:40:32+01:00", "2015-07-21T21:40:32+01:00"),
        ("2015T214032-0100", "2015-01-01T21:40:32-01:00"),
        ("20150721T21:40-01:30", "2015-07-21T21:40-01:30"),
        ("2015-W30T2140-00:00", "2015-07-20T21:40Z"),
        ("2015-W30T2140-02", "2015-07-20T21:40-02:00"),
        ("2015202T21+18:00", "2015-07-21T21:00+18:00"),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("DATETIME", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("DATETIME({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "DATETIME({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}

#[test]
fn test_datetime_named_timezone_formats() {
    // TCK Scenario [6]: Should parse date time with named time zone from string
    let cases = [
        (
            "2015-07-21T21:40:32.142+02:00[Europe/Stockholm]",
            "2015-07-21T21:40:32.142+02:00[Europe/Stockholm]",
        ),
        (
            "2015-07-21T21:40:32.142+0845[Australia/Eucla]",
            "2015-07-21T21:40:32.142+08:45[Australia/Eucla]",
        ),
        (
            "2015-07-21T21:40:32.142-04[America/New_York]",
            "2015-07-21T21:40:32.142-04:00[America/New_York]",
        ),
        (
            "2015-07-21T21:40:32.142[Europe/London]",
            "2015-07-21T21:40:32.142+01:00[Europe/London]",
        ),
        (
            "1818-07-21T21:40:32.142[Europe/Stockholm]",
            "1818-07-21T21:40:32.142+00:53:28[Europe/Stockholm]",
        ),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("DATETIME", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("DATETIME({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "DATETIME({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}

#[test]
fn test_duration_all_string_formats() {
    // TCK Scenario [7]: Should parse duration from string
    let cases = [
        ("P14DT16H12M", "P14DT16H12M"),
        ("P5M1.5D", "P5M1DT12H"),
        ("P0.75M", "P22DT19H51M49.5S"),
        ("PT0.75M", "PT45S"),
        ("P2.5W", "P17DT12H"),
        ("P12Y5M14DT16H12M70S", "P12Y5M14DT16H13M10S"),
        ("P2012-02-02T14:37:21.545", "P2012Y2M2DT14H37M21.545S"),
    ];

    for (input, expected) in &cases {
        let res = eval_scalar_function("DURATION", &[unival!(*input)])
            .unwrap_or_else(|e| panic!("DURATION({:?}) failed: {}", input, e));
        assert_eq!(
            res.to_string(),
            *expected,
            "DURATION({:?}) => {:?}, expected {:?}",
            input,
            res.to_string(),
            expected
        );
    }
}
