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
    // Cypher time() always includes timezone (defaults to +00:00 when unspecified).
    // TemporalValue::Time Display always includes seconds.
    let res = eval_scalar_function("TIME", &[unival!("10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "10:30:00+00:00");

    // Time with non-zero seconds includes seconds and timezone.
    let res = eval_scalar_function("TIME", &[unival!("10:30:45")]).unwrap();
    assert_eq!(res.to_string(), "10:30:45+00:00");

    // Time with explicit timezone preserves it.
    let res = eval_scalar_function("TIME", &[unival!("10:30:45+01:00")]).unwrap();
    assert_eq!(res.to_string(), "10:30:45+01:00");
}

#[test]
fn test_datetime_function() {
    // Standard Cypher datetime uses T separator (not space).
    // TemporalValue::DateTime Display always includes seconds.
    let res = eval_scalar_function("DATETIME", &[unival!("2023-01-15T10:30:00Z")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15T10:30:00Z");

    // Datetime with explicit timezone.
    let res = eval_scalar_function("DATETIME", &[unival!("2023-01-15T10:30:00+05:00")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15T10:30:00+05:00");
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
    assert!(s.len() >= 19, "Expected at least YYYY-MM-DDTHH:MM:SS");

    // Should work with string argument too
    // TemporalValue::LocalDateTime Display always includes seconds
    let res = eval_scalar_function("LOCALDATETIME", &[unival!("2023-01-15T10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "2023-01-15T10:30:00");
}

#[test]
fn test_localtime_function() {
    // localtime() returns current local time as TemporalValue::LocalTime
    let res = eval_scalar_function("LOCALTIME", &[]).unwrap();
    let s = res.to_string();
    // Should be in format HH:MM:SS (always includes seconds)
    assert!(s.contains(":"), "Expected time format with colons");
    assert!(s.len() >= 8, "Expected at least HH:MM:SS");

    // Should work with string argument too
    // TemporalValue::LocalTime Display always includes seconds
    let res = eval_scalar_function("LOCALTIME", &[unival!("10:30:00")]).unwrap();
    assert_eq!(res.to_string(), "10:30:00");
}
