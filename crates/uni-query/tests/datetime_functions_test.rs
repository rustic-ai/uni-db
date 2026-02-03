// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use serde_json::json;
use uni_query::query::expr_eval::eval_scalar_function;

#[test]
fn test_date_function() {
    let res = eval_scalar_function("DATE", &[json!("2023-01-15")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "2023-01-15");

    let res = eval_scalar_function("DATE", &[json!("2023-01-15 10:30:00")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "2023-01-15");

    // Current date (no args)
    let res = eval_scalar_function("DATE", &[]).unwrap();
    assert!(res.as_str().unwrap().len() == 10);
}

#[test]
fn test_time_function() {
    let res = eval_scalar_function("TIME", &[json!("10:30:00")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "10:30:00.000000");

    let res = eval_scalar_function("TIME", &[json!("2023-01-15 10:30:00")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "10:30:00.000000");
}

#[test]
fn test_datetime_function() {
    let res = eval_scalar_function("DATETIME", &[json!("2023-01-15 10:30:00")]).unwrap();
    // Output depends on how parser handles "space" separator - parse_datetime_utc uses DateTime::parse_from_str
    // Implementation uses .to_rfc3339() which produces "2023-01-15T10:30:00+00:00"
    assert_eq!(res.as_str().unwrap(), "2023-01-15T10:30:00+00:00");
}

#[test]
fn test_extract_functions() {
    let dt = json!("2023-01-15 10:30:45");

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
    // localdatetime() returns current local time as RFC3339 string
    let res = eval_scalar_function("LOCALDATETIME", &[]).unwrap();
    let s = res.as_str().unwrap();
    // Should be a valid RFC3339 datetime string
    assert!(s.contains("T"), "Expected RFC3339 format with T separator");
    assert!(s.len() >= 19, "Expected at least YYYY-MM-DDTHH:MM:SS");

    // Should fail with arguments
    assert!(eval_scalar_function("LOCALDATETIME", &[json!("arg")]).is_err());
}

#[test]
fn test_localtime_function() {
    // localtime() returns current local time as HH:MM:SS.microseconds
    let res = eval_scalar_function("LOCALTIME", &[]).unwrap();
    let s = res.as_str().unwrap();
    // Should be in format HH:MM:SS.microseconds
    assert!(s.contains(":"), "Expected time format with colons");
    assert!(s.len() >= 8, "Expected at least HH:MM:SS");

    // Should fail with arguments
    assert!(eval_scalar_function("LOCALTIME", &[json!("arg")]).is_err());
}
