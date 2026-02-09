// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_common::unival;
use uni_query::query::expr_eval::{eval_scalar_function, is_scalar_function};

#[test]
fn test_math_functions() {
    // LOG
    let res = eval_scalar_function("LOG", &[unival!(std::f64::consts::E)]).unwrap();
    assert!((res.as_f64().unwrap() - 1.0).abs() < 1e-10);

    // LOG10
    let res = eval_scalar_function("LOG10", &[unival!(100.0)]).unwrap();
    assert!((res.as_f64().unwrap() - 2.0).abs() < 1e-10);

    // EXP
    let res = eval_scalar_function("EXP", &[unival!(1.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::E).abs() < 1e-10);

    // POWER
    let res = eval_scalar_function("POWER", &[unival!(2.0), unival!(3.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 8.0);

    let res = eval_scalar_function("POW", &[unival!(3.0), unival!(2.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 9.0);

    // SIN
    let res = eval_scalar_function("SIN", &[unival!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);

    // COS
    let res = eval_scalar_function("COS", &[unival!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 1.0);

    // TAN
    let res = eval_scalar_function("TAN", &[unival!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);
}

#[test]
fn test_string_functions_pad() {
    // LPAD
    let res = eval_scalar_function("LPAD", &[unival!("abc"), unival!(5)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "  abc");

    let res = eval_scalar_function("LPAD", &[unival!("abc"), unival!(5), unival!("x")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "xxabc");

    let res = eval_scalar_function("LPAD", &[unival!("abc"), unival!(6), unival!("xy")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "xyxabc");

    // RPAD
    let res = eval_scalar_function("RPAD", &[unival!("abc"), unival!(5)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "abc  ");

    let res = eval_scalar_function("RPAD", &[unival!("abc"), unival!(5), unival!("x")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "abcxx");

    let res = eval_scalar_function("RPAD", &[unival!("abc"), unival!(6), unival!("xy")]).unwrap();
    assert_eq!(res.as_str().unwrap(), "abcxyx");

    // Truncation behavior
    let res = eval_scalar_function("LPAD", &[unival!("abc"), unival!(2)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "ab");

    let res = eval_scalar_function("RPAD", &[unival!("abc"), unival!(2)]).unwrap();
    assert_eq!(res.as_str().unwrap(), "ab");
}

#[test]
fn test_is_scalar_function() {
    assert!(is_scalar_function("log"));
    assert!(is_scalar_function("power"));
    assert!(is_scalar_function("lpad"));
    assert!(is_scalar_function("rpad"));
    assert!(is_scalar_function("id"));
    assert!(is_scalar_function("elementId"));
    assert!(is_scalar_function("type"));
    assert!(is_scalar_function("labels"));
    assert!(is_scalar_function("properties"));
    assert!(is_scalar_function("startNode"));
    assert!(is_scalar_function("endNode"));
    assert!(is_scalar_function("any"));
    assert!(is_scalar_function("all"));
    assert!(is_scalar_function("none"));
    assert!(is_scalar_function("single"));
    assert!(is_scalar_function("pi"));
    assert!(is_scalar_function("e"));
    assert!(is_scalar_function("rand"));
    assert!(is_scalar_function("asin"));
    assert!(is_scalar_function("acos"));
    assert!(is_scalar_function("atan"));
    assert!(is_scalar_function("atan2"));
    assert!(is_scalar_function("degrees"));
    assert!(is_scalar_function("radians"));
    assert!(is_scalar_function("haversin"));
    assert!(!is_scalar_function("non_existent"));
}

#[test]
fn test_trig_functions() {
    // ASIN
    let res = eval_scalar_function("ASIN", &[unival!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);

    let res = eval_scalar_function("ASIN", &[unival!(1.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::FRAC_PI_2).abs() < 1e-10);

    // ACOS
    let res = eval_scalar_function("ACOS", &[unival!(1.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);

    let res = eval_scalar_function("ACOS", &[unival!(0.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::FRAC_PI_2).abs() < 1e-10);

    // ATAN
    let res = eval_scalar_function("ATAN", &[unival!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);

    let res = eval_scalar_function("ATAN", &[unival!(1.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::FRAC_PI_4).abs() < 1e-10);

    // ATAN2
    let res = eval_scalar_function("ATAN2", &[unival!(1.0), unival!(1.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::FRAC_PI_4).abs() < 1e-10);

    // DEGREES
    let res = eval_scalar_function("DEGREES", &[unival!(std::f64::consts::PI)]).unwrap();
    assert!((res.as_f64().unwrap() - 180.0).abs() < 1e-10);

    // RADIANS
    let res = eval_scalar_function("RADIANS", &[unival!(180.0)]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::PI).abs() < 1e-10);

    // HAVERSIN
    let res = eval_scalar_function("HAVERSIN", &[unival!(0.0)]).unwrap();
    assert_eq!(res.as_f64().unwrap(), 0.0);

    // haversin(pi) = (1 - cos(pi)) / 2 = (1 - (-1)) / 2 = 1
    let res = eval_scalar_function("HAVERSIN", &[unival!(std::f64::consts::PI)]).unwrap();
    assert!((res.as_f64().unwrap() - 1.0).abs() < 1e-10);
}

#[test]
fn test_constant_functions() {
    // PI
    let res = eval_scalar_function("PI", &[]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::PI).abs() < 1e-10);

    // E
    let res = eval_scalar_function("E", &[]).unwrap();
    assert!((res.as_f64().unwrap() - std::f64::consts::E).abs() < 1e-10);

    // RAND returns a value between 0 and 1
    let res = eval_scalar_function("RAND", &[]).unwrap();
    let r = res.as_f64().unwrap();
    assert!((0.0..1.0).contains(&r));
}
