// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use uni_common::{Value, unival};
use uni_query::query::expr_eval::eval_scalar_function;

/// Helper: build a Value::Map from a vec of (key, value) pairs.
fn make_map(entries: Vec<(&str, Value)>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<HashMap<_, _>>(),
    )
}

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
        (
            "2015-07-21T21:40:32.142+0100",
            "2015-07-21T21:40:32.142+01:00",
        ),
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

// ============================================================================
// TCK Temporal3: Project Temporal Values from other Temporal Values
// ============================================================================

// ---------------------------------------------------------------------------
// Scenario [1]: Should select date
// ---------------------------------------------------------------------------

#[test]
fn test_project_date_from_date() {
    // Source: date({year: 1984, month: 11, day: 11})
    let source =
        eval_scalar_function("DATE", &[unival!({"year": 1984, "month": 11, "day": 11})]).unwrap();

    // date(other) → identity
    let res = eval_scalar_function("DATE", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "1984-11-11");

    // date({date: other}) → copy
    let res = eval_scalar_function("DATE", &[make_map(vec![("date", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "1984-11-11");

    // date({date: other, year: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("year", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "0028-11-11");

    // date({date: other, day: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("day", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-11-28");

    // date({date: other, week: 1})
    // 1984-01-08 = Sunday of ISO week 1 (dayOfWeek defaults to source's: Nov 11 1984 = Sunday = 7)
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("week", Value::from(1)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-01-08");

    // date({date: other, ordinalDay: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("ordinalDay", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-01-28");

    // date({date: other, quarter: 3})
    // Source Nov 11 is dayOfQuarter 42 in Q4 → Q3 day 42 → 1984-08-11
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("quarter", Value::from(3)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-08-11");
}

#[test]
fn test_project_date_from_localdatetime() {
    // Source: localdatetime({year: 1984, month: 11, day: 11, hour: 12, minute: 31, second: 14, nanosecond: 645876123})
    let source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "month": 11, "day": 11, "hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();

    // date(other) → extract date
    let res = eval_scalar_function("DATE", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "1984-11-11");

    // date({date: other})
    let res = eval_scalar_function("DATE", &[make_map(vec![("date", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "1984-11-11");

    // date({date: other, year: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("year", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "0028-11-11");

    // date({date: other, day: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("day", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-11-28");

    // date({date: other, week: 1})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("week", Value::from(1)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-01-08");

    // date({date: other, ordinalDay: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("ordinalDay", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-01-28");

    // date({date: other, quarter: 3})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("quarter", Value::from(3)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-08-11");
}

#[test]
fn test_project_date_from_datetime() {
    // Source: datetime({year: 1984, month: 11, day: 11, hour: 12, timezone: '+01:00'})
    let source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 11, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    // date(other)
    let res = eval_scalar_function("DATE", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "1984-11-11");

    // date({date: other})
    let res = eval_scalar_function("DATE", &[make_map(vec![("date", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "1984-11-11");

    // date({date: other, year: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("year", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "0028-11-11");

    // date({date: other, day: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("day", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-11-28");

    // date({date: other, week: 1})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("week", Value::from(1)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-01-08");

    // date({date: other, ordinalDay: 28})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("ordinalDay", Value::from(28)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-01-28");

    // date({date: other, quarter: 3})
    let res = eval_scalar_function(
        "DATE",
        &[make_map(vec![
            ("date", source.clone()),
            ("quarter", Value::from(3)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-08-11");
}

// ---------------------------------------------------------------------------
// Scenario [2]: Should select local time
// ---------------------------------------------------------------------------

#[test]
fn test_project_localtime_from_localtime() {
    // Source: localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123})
    let source = eval_scalar_function(
        "LOCALTIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();

    // localtime(other) → identity
    let res = eval_scalar_function("LOCALTIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876123");

    // localtime({time: other})
    let res =
        eval_scalar_function("LOCALTIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876123");

    // localtime({time: other, second: 42})
    let res = eval_scalar_function(
        "LOCALTIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645876123");
}

#[test]
fn test_project_localtime_from_time() {
    // Source: time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'})
    let source = eval_scalar_function(
        "TIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "microsecond": 645876, "timezone": "+01:00"})],
    )
    .unwrap();

    // localtime(other) → strips timezone
    let res = eval_scalar_function("LOCALTIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876");

    // localtime({time: other})
    let res =
        eval_scalar_function("LOCALTIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876");

    // localtime({time: other, second: 42})
    let res = eval_scalar_function(
        "LOCALTIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645876");
}

#[test]
fn test_project_localtime_from_localdatetime() {
    // Source: localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645})
    let source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    // localtime(other) → extract time
    let res = eval_scalar_function("LOCALTIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645");

    // localtime({time: other})
    let res =
        eval_scalar_function("LOCALTIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645");

    // localtime({time: other, second: 42})
    let res = eval_scalar_function(
        "LOCALTIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645");
}

#[test]
fn test_project_localtime_from_datetime() {
    // Source: datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'})
    let source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    // localtime(other) → extract time, strip tz
    let res = eval_scalar_function("LOCALTIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:00");

    // localtime({time: other})
    let res =
        eval_scalar_function("LOCALTIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:00");

    // localtime({time: other, second: 42})
    let res = eval_scalar_function(
        "LOCALTIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:00:42");
}

// ---------------------------------------------------------------------------
// Scenario [3]: Should select time
// ---------------------------------------------------------------------------

#[test]
fn test_project_time_from_localtime() {
    // Source: localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123})
    let source = eval_scalar_function(
        "LOCALTIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();

    // time(other) → defaults to Z (localtime has no tz)
    let res = eval_scalar_function("TIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876123Z");

    // time({time: other})
    let res = eval_scalar_function("TIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876123Z");

    // time({time: other, timezone: '+05:00'}) → assign timezone (no conversion, source has no tz)
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876123+05:00");

    // time({time: other, second: 42})
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645876123Z");

    // time({time: other, second: 42, timezone: '+05:00'})
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645876123+05:00");
}

#[test]
fn test_project_time_from_time() {
    // Source: time({hour: 12, minute: 31, second: 14, microsecond: 645876, timezone: '+01:00'})
    let source = eval_scalar_function(
        "TIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "microsecond": 645876, "timezone": "+01:00"})],
    )
    .unwrap();

    // time(other) → identity
    let res = eval_scalar_function("TIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876+01:00");

    // time({time: other})
    let res = eval_scalar_function("TIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645876+01:00");

    // time({time: other, timezone: '+05:00'}) → TIMEZONE CONVERSION: 12:31+01:00 → 16:31+05:00
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "16:31:14.645876+05:00");

    // time({time: other, second: 42})
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645876+01:00");

    // time({time: other, second: 42, timezone: '+05:00'}) → conversion: 12:31:42+01:00 → 16:31:42+05:00
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "16:31:42.645876+05:00");
}

#[test]
fn test_project_time_from_localdatetime() {
    // Source: localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645})
    let source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    // time(other) → defaults to Z
    let res = eval_scalar_function("TIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645Z");

    // time({time: other})
    let res = eval_scalar_function("TIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:31:14.645Z");

    // time({time: other, timezone: '+05:00'}) → assign (no conversion, source has no tz)
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:14.645+05:00");

    // time({time: other, second: 42})
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645Z");

    // time({time: other, second: 42, timezone: '+05:00'})
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:31:42.645+05:00");
}

#[test]
fn test_project_time_from_datetime() {
    // Source: datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'})
    // October 1984 in Stockholm = CET (UTC+1), so offset is +01:00
    let source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "Europe/Stockholm"})],
    )
    .unwrap();

    // time(other) → extract time with offset from named tz
    let res = eval_scalar_function("TIME", std::slice::from_ref(&source)).unwrap();
    assert_eq!(res.to_string(), "12:00+01:00");

    // time({time: other})
    let res = eval_scalar_function("TIME", &[make_map(vec![("time", source.clone())])]).unwrap();
    assert_eq!(res.to_string(), "12:00+01:00");

    // time({time: other, timezone: '+05:00'}) → conversion: 12:00+01:00 → 16:00+05:00
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "16:00+05:00");

    // time({time: other, second: 42})
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "12:00:42+01:00");

    // time({time: other, second: 42, timezone: '+05:00'}) → conversion: 12:00:42+01:00 → 16:00:42+05:00
    let res = eval_scalar_function(
        "TIME",
        &[make_map(vec![
            ("time", source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "16:00:42+05:00");
}

// ---------------------------------------------------------------------------
// Scenario [4]: Should select date into local date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_localdatetime_from_date() {
    // Source: date({year: 1984, month: 10, day: 11})
    let date_source =
        eval_scalar_function("DATE", &[unival!({"year": 1984, "month": 10, "day": 11})]).unwrap();

    // localdatetime({date: other, hour: 10, minute: 10, second: 10})
    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("date", date_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T10:10:10");

    // localdatetime({date: other, day: 28, hour: 10, minute: 10, second: 10})
    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("date", date_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-28T10:10:10");

    // Source: localdatetime({year: 1984, week: 10, dayOfWeek: 3, ...})
    let ldt_source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("date", ldt_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-07T10:10:10");

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("date", ldt_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-28T10:10:10");

    // Source: datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'})
    let dt_source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("date", dt_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T10:10:10");

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("date", dt_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-28T10:10:10");
}

// ---------------------------------------------------------------------------
// Scenario [5]: Should select time into local date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_localdatetime_from_time() {
    // localtime source
    let lt_source = eval_scalar_function(
        "LOCALTIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", lt_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645876123");

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", lt_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:42.645876123");

    // time source (with timezone) - localdatetime strips tz
    let t_source = eval_scalar_function(
        "TIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "microsecond": 645876, "timezone": "+01:00"})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", t_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645876");

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", t_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:42.645876");

    // localdatetime source
    let ldt_source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", ldt_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645");

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", ldt_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:42.645");

    // datetime source
    let dt_source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", dt_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00");

    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", dt_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00:42");
}

// ---------------------------------------------------------------------------
// Scenario [6]: Should select date and time into local date time
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::type_complexity)]
fn test_project_localdatetime_date_and_time() {
    // Date sources
    let date1 =
        eval_scalar_function("DATE", &[unival!({"year": 1984, "month": 10, "day": 11})]).unwrap();
    let date_ldt = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();
    let date_dt = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    // Time sources
    let time_lt = eval_scalar_function(
        "LOCALTIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();
    let time_t = eval_scalar_function(
        "TIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "microsecond": 645876, "timezone": "+01:00"})],
    )
    .unwrap();
    let time_ldt = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();
    let time_dt = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    // date({year:1984,month:10,day:11}) + localtime → localdatetime
    let cases_date1: Vec<(&Value, &str, Option<(i64, i64)>, &str)> = vec![
        (&time_lt, "localtime", None, "1984-10-11T12:31:14.645876123"),
        (
            &time_lt,
            "localtime+overrides",
            Some((28, 42)),
            "1984-10-28T12:31:42.645876123",
        ),
        (&time_t, "time", None, "1984-10-11T12:31:14.645876"),
        (
            &time_t,
            "time+overrides",
            Some((28, 42)),
            "1984-10-28T12:31:42.645876",
        ),
        (&time_ldt, "localdatetime", None, "1984-10-11T12:31:14.645"),
        (
            &time_ldt,
            "localdatetime+overrides",
            Some((28, 42)),
            "1984-10-28T12:31:42.645",
        ),
        (&time_dt, "datetime", None, "1984-10-11T12:00"),
        (
            &time_dt,
            "datetime+overrides",
            Some((28, 42)),
            "1984-10-28T12:00:42",
        ),
    ];

    for (time_src, label, overrides, expected) in &cases_date1 {
        let mut entries: Vec<(&str, Value)> =
            vec![("date", date1.clone()), ("time", (*time_src).clone())];
        if let Some((day, sec)) = overrides {
            entries.push(("day", Value::from(*day)));
            entries.push(("second", Value::from(*sec)));
        }
        let res = eval_scalar_function("LOCALDATETIME", &[make_map(entries)])
            .unwrap_or_else(|e| panic!("localdatetime(date1+{}) failed: {}", label, e));
        assert_eq!(res.to_string(), *expected, "date1+{}", label);
    }

    // localdatetime({year:1984,week:10,dayOfWeek:3,...}) as date source
    let cases_ldt: Vec<(&Value, &str, Option<(i64, i64)>, &str)> = vec![
        (&time_lt, "localtime", None, "1984-03-07T12:31:14.645876123"),
        (
            &time_lt,
            "localtime+overrides",
            Some((28, 42)),
            "1984-03-28T12:31:42.645876123",
        ),
        (&time_t, "time", None, "1984-03-07T12:31:14.645876"),
        (
            &time_t,
            "time+overrides",
            Some((28, 42)),
            "1984-03-28T12:31:42.645876",
        ),
        (&time_ldt, "localdatetime", None, "1984-03-07T12:31:14.645"),
        (
            &time_ldt,
            "localdatetime+overrides",
            Some((28, 42)),
            "1984-03-28T12:31:42.645",
        ),
        (&time_dt, "datetime", None, "1984-03-07T12:00"),
        (
            &time_dt,
            "datetime+overrides",
            Some((28, 42)),
            "1984-03-28T12:00:42",
        ),
    ];

    for (time_src, label, overrides, expected) in &cases_ldt {
        let mut entries: Vec<(&str, Value)> =
            vec![("date", date_ldt.clone()), ("time", (*time_src).clone())];
        if let Some((day, sec)) = overrides {
            entries.push(("day", Value::from(*day)));
            entries.push(("second", Value::from(*sec)));
        }
        let res = eval_scalar_function("LOCALDATETIME", &[make_map(entries)])
            .unwrap_or_else(|e| panic!("localdatetime(ldt+{}) failed: {}", label, e));
        assert_eq!(res.to_string(), *expected, "ldt+{}", label);
    }

    // datetime({year:1984,month:10,day:11,hour:12,timezone:'+01:00'}) as date source
    let cases_dt: Vec<(&Value, &str, Option<(i64, i64)>, &str)> = vec![
        (&time_lt, "localtime", None, "1984-10-11T12:31:14.645876123"),
        (
            &time_lt,
            "localtime+overrides",
            Some((28, 42)),
            "1984-10-28T12:31:42.645876123",
        ),
        (&time_t, "time", None, "1984-10-11T12:31:14.645876"),
        (
            &time_t,
            "time+overrides",
            Some((28, 42)),
            "1984-10-28T12:31:42.645876",
        ),
        (&time_ldt, "localdatetime", None, "1984-10-11T12:31:14.645"),
        (
            &time_ldt,
            "localdatetime+overrides",
            Some((28, 42)),
            "1984-10-28T12:31:42.645",
        ),
        (&time_dt, "datetime", None, "1984-10-11T12:00"),
        (
            &time_dt,
            "datetime+overrides",
            Some((28, 42)),
            "1984-10-28T12:00:42",
        ),
    ];

    for (time_src, label, overrides, expected) in &cases_dt {
        let mut entries: Vec<(&str, Value)> =
            vec![("date", date_dt.clone()), ("time", (*time_src).clone())];
        if let Some((day, sec)) = overrides {
            entries.push(("day", Value::from(*day)));
            entries.push(("second", Value::from(*sec)));
        }
        let res = eval_scalar_function("LOCALDATETIME", &[make_map(entries)])
            .unwrap_or_else(|e| panic!("localdatetime(dt+{}) failed: {}", label, e));
        assert_eq!(res.to_string(), *expected, "dt+{}", label);
    }
}

// ---------------------------------------------------------------------------
// Scenario [7]: Should select datetime into local date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_localdatetime_from_datetime() {
    // Source: localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645})
    let ldt_source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    // localdatetime(other) → identity
    let res = eval_scalar_function("LOCALDATETIME", std::slice::from_ref(&ldt_source)).unwrap();
    assert_eq!(res.to_string(), "1984-03-07T12:31:14.645");

    // localdatetime({datetime: other})
    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![("datetime", ldt_source.clone())])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-07T12:31:14.645");

    // localdatetime({datetime: other, day: 28, second: 42})
    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("datetime", ldt_source.clone()),
            ("day", Value::from(28)),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-28T12:31:42.645");

    // Source: datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: '+01:00'})
    let dt_source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    // localdatetime(other) → strips tz
    let res = eval_scalar_function("LOCALDATETIME", std::slice::from_ref(&dt_source)).unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00");

    // localdatetime({datetime: other})
    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![("datetime", dt_source.clone())])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00");

    // localdatetime({datetime: other, day: 28, second: 42})
    let res = eval_scalar_function(
        "LOCALDATETIME",
        &[make_map(vec![
            ("datetime", dt_source.clone()),
            ("day", Value::from(28)),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-28T12:00:42");
}

// ---------------------------------------------------------------------------
// Scenario [8]: Should select date into date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_datetime_from_date() {
    let date_source =
        eval_scalar_function("DATE", &[unival!({"year": 1984, "month": 10, "day": 11})]).unwrap();

    // datetime({date: other, hour: 10, minute: 10, second: 10}) → defaults to Z
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", date_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T10:10:10Z");

    // datetime({date: other, hour: 10, minute: 10, second: 10, timezone: '+05:00'})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", date_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T10:10:10+05:00");

    // datetime({date: other, day: 28, hour: 10, minute: 10, second: 10})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", date_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-28T10:10:10Z");

    // datetime({date: other, day: 28, hour: 10, minute: 10, second: 10, timezone: 'Pacific/Honolulu'})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", date_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-28T10:10:10-10:00[Pacific/Honolulu]"
    );

    // localdatetime source
    let ldt_source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", ldt_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-07T10:10:10Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", ldt_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-07T10:10:10+05:00");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", ldt_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-28T10:10:10Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", ldt_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-03-28T10:10:10-10:00[Pacific/Honolulu]"
    );

    // datetime source
    let dt_source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", dt_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T10:10:10Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", dt_source.clone()),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T10:10:10+05:00");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", dt_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-28T10:10:10Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("date", dt_source.clone()),
            ("day", Value::from(28)),
            ("hour", Value::from(10)),
            ("minute", Value::from(10)),
            ("second", Value::from(10)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-28T10:10:10-10:00[Pacific/Honolulu]"
    );
}

// ---------------------------------------------------------------------------
// Scenario [9]: Should select time into date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_datetime_from_time() {
    // localtime source
    let lt_source = eval_scalar_function(
        "LOCALTIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", lt_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645876123Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", lt_source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645876123+05:00");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", lt_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:42.645876123Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", lt_source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-11T12:31:42.645876123-10:00[Pacific/Honolulu]"
    );

    // time source (with timezone) - timezone CONVERSION when new tz specified
    let t_source = eval_scalar_function(
        "TIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "microsecond": 645876, "timezone": "+01:00"})],
    )
    .unwrap();

    // No tz override → keep source tz
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", t_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645876+01:00");

    // tz override → conversion: 12:31+01:00 → 16:31+05:00
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", t_source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T16:31:14.645876+05:00");

    // second override, no tz override
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", t_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:42.645876+01:00");

    // second + tz override → conversion: 12:31:42+01:00 → 01:31:42-10:00
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", t_source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-11T01:31:42.645876-10:00[Pacific/Honolulu]"
    );

    // localdatetime source
    let ldt_source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", ldt_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", ldt_source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:14.645+05:00");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", ldt_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:31:42.645Z");

    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", ldt_source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-11T12:31:42.645-10:00[Pacific/Honolulu]"
    );

    // datetime source with named tz (Europe/Stockholm in Oct 1984 = CET +01:00)
    let dt_source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "Europe/Stockholm"})],
    )
    .unwrap();

    // No tz override → keep named tz
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", dt_source.clone()),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00+01:00[Europe/Stockholm]");

    // tz override → conversion: 12:00+01:00 → 16:00+05:00
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", dt_source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T16:00+05:00");

    // second override, keep tz
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", dt_source.clone()),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-11T12:00:42+01:00[Europe/Stockholm]"
    );

    // second + tz override → conversion: 12:00:42+01:00 → 01:00:42-10:00
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("year", Value::from(1984)),
            ("month", Value::from(10)),
            ("day", Value::from(11)),
            ("time", dt_source.clone()),
            ("second", Value::from(42)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-11T01:00:42-10:00[Pacific/Honolulu]"
    );
}

// ---------------------------------------------------------------------------
// Scenario [10]: Should select date and time into date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_datetime_date_and_time() {
    // Date sources
    let date1 =
        eval_scalar_function("DATE", &[unival!({"year": 1984, "month": 10, "day": 11})]).unwrap();
    let date_ldt = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();
    let date_dt = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "+01:00"})],
    )
    .unwrap();

    // Time sources
    let time_lt = eval_scalar_function(
        "LOCALTIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "nanosecond": 645876123})],
    )
    .unwrap();
    let time_t = eval_scalar_function(
        "TIME",
        &[unival!({"hour": 12, "minute": 31, "second": 14, "microsecond": 645876, "timezone": "+01:00"})],
    )
    .unwrap();
    let time_ldt = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();
    let time_dt = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "Europe/Stockholm"})],
    )
    .unwrap();

    // Helper type for test cases
    struct Case {
        date: Value,
        time: Value,
        tz: Option<&'static str>,
        day_sec: Option<(i64, i64)>,
        expected: &'static str,
        label: &'static str,
    }

    let cases = vec![
        // date({year:1984,month:10,day:11}) + localtime
        Case {
            date: date1.clone(),
            time: time_lt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:31:14.645876123Z",
            label: "d1+lt",
        },
        Case {
            date: date1.clone(),
            time: time_lt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T12:31:14.645876123+05:00",
            label: "d1+lt+tz",
        },
        Case {
            date: date1.clone(),
            time: time_lt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645876123Z",
            label: "d1+lt+ds",
        },
        Case {
            date: date1.clone(),
            time: time_lt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645876123-10:00[Pacific/Honolulu]",
            label: "d1+lt+ds+tz",
        },
        // date + time (with tz conversion)
        Case {
            date: date1.clone(),
            time: time_t.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:31:14.645876+01:00",
            label: "d1+t",
        },
        Case {
            date: date1.clone(),
            time: time_t.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T16:31:14.645876+05:00",
            label: "d1+t+tz",
        },
        Case {
            date: date1.clone(),
            time: time_t.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645876+01:00",
            label: "d1+t+ds",
        },
        Case {
            date: date1.clone(),
            time: time_t.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T01:31:42.645876-10:00[Pacific/Honolulu]",
            label: "d1+t+ds+tz",
        },
        // date + localdatetime
        Case {
            date: date1.clone(),
            time: time_ldt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:31:14.645Z",
            label: "d1+ldt",
        },
        Case {
            date: date1.clone(),
            time: time_ldt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T12:31:14.645+05:00",
            label: "d1+ldt+tz",
        },
        Case {
            date: date1.clone(),
            time: time_ldt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645Z",
            label: "d1+ldt+ds",
        },
        Case {
            date: date1.clone(),
            time: time_ldt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645-10:00[Pacific/Honolulu]",
            label: "d1+ldt+ds+tz",
        },
        // date + datetime (Europe/Stockholm, CET in Oct = +01:00)
        Case {
            date: date1.clone(),
            time: time_dt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:00+01:00[Europe/Stockholm]",
            label: "d1+dt",
        },
        Case {
            date: date1.clone(),
            time: time_dt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T16:00+05:00",
            label: "d1+dt+tz",
        },
        Case {
            date: date1.clone(),
            time: time_dt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:00:42+01:00[Europe/Stockholm]",
            label: "d1+dt+ds",
        },
        Case {
            date: date1.clone(),
            time: time_dt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T01:00:42-10:00[Pacific/Honolulu]",
            label: "d1+dt+ds+tz",
        },
        // localdatetime as date source
        Case {
            date: date_ldt.clone(),
            time: time_lt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-03-07T12:31:14.645876123Z",
            label: "ldt+lt",
        },
        Case {
            date: date_ldt.clone(),
            time: time_lt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-03-07T12:31:14.645876123+05:00",
            label: "ldt+lt+tz",
        },
        Case {
            date: date_ldt.clone(),
            time: time_lt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-03-28T12:31:42.645876123Z",
            label: "ldt+lt+ds",
        },
        Case {
            date: date_ldt.clone(),
            time: time_lt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-03-28T12:31:42.645876123-10:00[Pacific/Honolulu]",
            label: "ldt+lt+ds+tz",
        },
        Case {
            date: date_ldt.clone(),
            time: time_t.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-03-07T12:31:14.645876+01:00",
            label: "ldt+t",
        },
        Case {
            date: date_ldt.clone(),
            time: time_t.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-03-07T16:31:14.645876+05:00",
            label: "ldt+t+tz",
        },
        Case {
            date: date_ldt.clone(),
            time: time_t.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-03-28T12:31:42.645876+01:00",
            label: "ldt+t+ds",
        },
        Case {
            date: date_ldt.clone(),
            time: time_t.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-03-28T01:31:42.645876-10:00[Pacific/Honolulu]",
            label: "ldt+t+ds+tz",
        },
        Case {
            date: date_ldt.clone(),
            time: time_ldt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-03-07T12:31:14.645Z",
            label: "ldt+ldt",
        },
        Case {
            date: date_ldt.clone(),
            time: time_ldt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-03-07T12:31:14.645+05:00",
            label: "ldt+ldt+tz",
        },
        Case {
            date: date_ldt.clone(),
            time: time_ldt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-03-28T12:31:42.645Z",
            label: "ldt+ldt+ds",
        },
        Case {
            date: date_ldt.clone(),
            time: time_ldt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-03-28T12:31:42.645-10:00[Pacific/Honolulu]",
            label: "ldt+ldt+ds+tz",
        },
        Case {
            date: date_ldt.clone(),
            time: time_dt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-03-07T12:00+01:00[Europe/Stockholm]",
            label: "ldt+dt",
        },
        Case {
            date: date_ldt.clone(),
            time: time_dt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-03-07T16:00+05:00",
            label: "ldt+dt+tz",
        },
        // Note: ldt date source -> 1984-03-28 in Europe/Stockholm = CET (+01:00) but March 28 = still winter time -> +01:00
        // Wait, let me check. CET -> CEST transition in 1984 was last Sunday of March = March 25.
        // So March 28 is CEST (+02:00).
        Case {
            date: date_ldt.clone(),
            time: time_dt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-03-28T12:00:42+02:00[Europe/Stockholm]",
            label: "ldt+dt+ds",
        },
        Case {
            date: date_ldt.clone(),
            time: time_dt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-03-28T00:00:42-10:00[Pacific/Honolulu]",
            label: "ldt+dt+ds+tz",
        },
        // datetime as date source
        Case {
            date: date_dt.clone(),
            time: time_lt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:31:14.645876123Z",
            label: "dt+lt",
        },
        Case {
            date: date_dt.clone(),
            time: time_lt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T12:31:14.645876123+05:00",
            label: "dt+lt+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_lt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645876123Z",
            label: "dt+lt+ds",
        },
        Case {
            date: date_dt.clone(),
            time: time_lt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645876123-10:00[Pacific/Honolulu]",
            label: "dt+lt+ds+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_t.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:31:14.645876+01:00",
            label: "dt+t",
        },
        Case {
            date: date_dt.clone(),
            time: time_t.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T16:31:14.645876+05:00",
            label: "dt+t+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_t.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645876+01:00",
            label: "dt+t+ds",
        },
        Case {
            date: date_dt.clone(),
            time: time_t.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T01:31:42.645876-10:00[Pacific/Honolulu]",
            label: "dt+t+ds+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_ldt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:31:14.645Z",
            label: "dt+ldt",
        },
        Case {
            date: date_dt.clone(),
            time: time_ldt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T12:31:14.645+05:00",
            label: "dt+ldt+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_ldt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645Z",
            label: "dt+ldt+ds",
        },
        Case {
            date: date_dt.clone(),
            time: time_ldt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:31:42.645-10:00[Pacific/Honolulu]",
            label: "dt+ldt+ds+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_dt.clone(),
            tz: None,
            day_sec: None,
            expected: "1984-10-11T12:00+01:00[Europe/Stockholm]",
            label: "dt+dt",
        },
        Case {
            date: date_dt.clone(),
            time: time_dt.clone(),
            tz: Some("+05:00"),
            day_sec: None,
            expected: "1984-10-11T16:00+05:00",
            label: "dt+dt+tz",
        },
        Case {
            date: date_dt.clone(),
            time: time_dt.clone(),
            tz: None,
            day_sec: Some((28, 42)),
            expected: "1984-10-28T12:00:42+01:00[Europe/Stockholm]",
            label: "dt+dt+ds",
        },
        Case {
            date: date_dt.clone(),
            time: time_dt.clone(),
            tz: Some("Pacific/Honolulu"),
            day_sec: Some((28, 42)),
            expected: "1984-10-28T01:00:42-10:00[Pacific/Honolulu]",
            label: "dt+dt+ds+tz",
        },
    ];

    for case in &cases {
        let mut entries: Vec<(&str, Value)> =
            vec![("date", case.date.clone()), ("time", case.time.clone())];
        if let Some((day, sec)) = case.day_sec {
            entries.push(("day", Value::from(day)));
            entries.push(("second", Value::from(sec)));
        }
        if let Some(tz) = case.tz {
            entries.push(("timezone", Value::from(tz)));
        }
        let res = eval_scalar_function("DATETIME", &[make_map(entries)])
            .unwrap_or_else(|e| panic!("datetime({}) failed: {}", case.label, e));
        assert_eq!(res.to_string(), case.expected, "case: {}", case.label);
    }
}

// ---------------------------------------------------------------------------
// Scenario [11]: Should datetime into date time
// ---------------------------------------------------------------------------

#[test]
fn test_project_datetime_from_datetime() {
    // Source: localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, minute: 31, second: 14, millisecond: 645})
    let ldt_source = eval_scalar_function(
        "LOCALDATETIME",
        &[unival!({"year": 1984, "week": 10, "dayOfWeek": 3, "hour": 12, "minute": 31, "second": 14, "millisecond": 645})],
    )
    .unwrap();

    // datetime(other) → add Z timezone
    let res = eval_scalar_function("DATETIME", std::slice::from_ref(&ldt_source)).unwrap();
    assert_eq!(res.to_string(), "1984-03-07T12:31:14.645Z");

    // datetime({datetime: other})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![("datetime", ldt_source.clone())])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-07T12:31:14.645Z");

    // datetime({datetime: other, timezone: '+05:00'}) → assign timezone (no conversion, source has no tz)
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("datetime", ldt_source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-07T12:31:14.645+05:00");

    // datetime({datetime: other, day: 28, second: 42})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("datetime", ldt_source.clone()),
            ("day", Value::from(28)),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-03-28T12:31:42.645Z");

    // datetime({datetime: other, day: 28, second: 42, timezone: 'Pacific/Honolulu'})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("datetime", ldt_source.clone()),
            ("day", Value::from(28)),
            ("second", Value::from(42)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-03-28T12:31:42.645-10:00[Pacific/Honolulu]"
    );

    // Source: datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'})
    let dt_source = eval_scalar_function(
        "DATETIME",
        &[unival!({"year": 1984, "month": 10, "day": 11, "hour": 12, "timezone": "Europe/Stockholm"})],
    )
    .unwrap();

    // datetime(other) → identity
    let res = eval_scalar_function("DATETIME", std::slice::from_ref(&dt_source)).unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00+01:00[Europe/Stockholm]");

    // datetime({datetime: other})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![("datetime", dt_source.clone())])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T12:00+01:00[Europe/Stockholm]");

    // datetime({datetime: other, timezone: '+05:00'}) → TIMEZONE CONVERSION: 12:00+01:00 → 16:00+05:00
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("datetime", dt_source.clone()),
            ("timezone", Value::from("+05:00")),
        ])],
    )
    .unwrap();
    assert_eq!(res.to_string(), "1984-10-11T16:00+05:00");

    // datetime({datetime: other, day: 28, second: 42})
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("datetime", dt_source.clone()),
            ("day", Value::from(28)),
            ("second", Value::from(42)),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-28T12:00:42+01:00[Europe/Stockholm]"
    );

    // datetime({datetime: other, day: 28, second: 42, timezone: 'Pacific/Honolulu'})
    // Conversion: 12:00:42 CET(+01:00) → UTC 11:00:42 → HST(-10:00) → 01:00:42
    let res = eval_scalar_function(
        "DATETIME",
        &[make_map(vec![
            ("datetime", dt_source.clone()),
            ("day", Value::from(28)),
            ("second", Value::from(42)),
            ("timezone", Value::from("Pacific/Honolulu")),
        ])],
    )
    .unwrap();
    assert_eq!(
        res.to_string(),
        "1984-10-28T01:00:42-10:00[Pacific/Honolulu]"
    );
}
