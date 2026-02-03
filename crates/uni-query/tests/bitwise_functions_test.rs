// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for uni.bitwise.* functions
//!
//! These tests verify that bitwise functions work correctly in the legacy executor.

use serde_json::json;
use uni_query::query::expr_eval::eval_scalar_function;

#[test]
fn test_bitwise_or() {
    let result = eval_scalar_function("uni_bitwise_or", &[json!(12), json!(10)]).unwrap();
    assert_eq!(result, json!(14)); // 12 | 10 = 14 (0b1100 | 0b1010 = 0b1110)
}

#[test]
fn test_bitwise_or_zero() {
    let result = eval_scalar_function("uni_bitwise_or", &[json!(5), json!(0)]).unwrap();
    assert_eq!(result, json!(5)); // 5 | 0 = 5
}

#[test]
fn test_bitwise_and() {
    let result = eval_scalar_function("uni_bitwise_and", &[json!(12), json!(10)]).unwrap();
    assert_eq!(result, json!(8)); // 12 & 10 = 8 (0b1100 & 0b1010 = 0b1000)
}

#[test]
fn test_bitwise_and_zero() {
    let result = eval_scalar_function("uni_bitwise_and", &[json!(5), json!(0)]).unwrap();
    assert_eq!(result, json!(0)); // 5 & 0 = 0
}

#[test]
fn test_bitwise_xor() {
    let result = eval_scalar_function("uni_bitwise_xor", &[json!(12), json!(10)]).unwrap();
    assert_eq!(result, json!(6)); // 12 ^ 10 = 6 (0b1100 ^ 0b1010 = 0b0110)
}

#[test]
fn test_bitwise_xor_same() {
    let result = eval_scalar_function("uni_bitwise_xor", &[json!(7), json!(7)]).unwrap();
    assert_eq!(result, json!(0)); // 7 ^ 7 = 0
}

#[test]
fn test_bitwise_not() {
    let result = eval_scalar_function("uni_bitwise_not", &[json!(5)]).unwrap();
    assert_eq!(result, json!(-6)); // !5 = -6 (two's complement)
}

#[test]
fn test_bitwise_not_zero() {
    let result = eval_scalar_function("uni_bitwise_not", &[json!(0)]).unwrap();
    assert_eq!(result, json!(-1)); // !0 = -1
}

#[test]
fn test_bitwise_not_negative() {
    let result = eval_scalar_function("uni_bitwise_not", &[json!(-1)]).unwrap();
    assert_eq!(result, json!(0)); // !(-1) = 0
}

#[test]
fn test_shift_left() {
    let result = eval_scalar_function("uni_bitwise_shiftLeft", &[json!(3), json!(2)]).unwrap();
    assert_eq!(result, json!(12)); // 3 << 2 = 12
}

#[test]
fn test_shift_left_zero() {
    let result = eval_scalar_function("uni_bitwise_shiftLeft", &[json!(5), json!(0)]).unwrap();
    assert_eq!(result, json!(5)); // 5 << 0 = 5
}

#[test]
fn test_shift_right() {
    let result = eval_scalar_function("uni_bitwise_shiftRight", &[json!(12), json!(2)]).unwrap();
    assert_eq!(result, json!(3)); // 12 >> 2 = 3
}

#[test]
fn test_shift_right_zero() {
    let result = eval_scalar_function("uni_bitwise_shiftRight", &[json!(5), json!(0)]).unwrap();
    assert_eq!(result, json!(5)); // 5 >> 0 = 5
}

// ============================================================================
// Error Cases
// ============================================================================

#[test]
fn test_bitwise_or_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_or", &[json!(5)]);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("exactly 2 arguments")
    );
}

#[test]
fn test_bitwise_or_non_integer() {
    let result = eval_scalar_function("uni_bitwise_or", &[json!("5"), json!(3)]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("integer"));
}

#[test]
fn test_bitwise_and_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_and", &[json!(5)]);
    assert!(result.is_err());
}

#[test]
fn test_bitwise_xor_non_integer() {
    let result = eval_scalar_function("uni_bitwise_xor", &[json!(5.5), json!(3)]);
    assert!(result.is_err());
}

#[test]
fn test_bitwise_not_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_not", &[json!(5), json!(3)]);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("exactly 1 argument")
    );
}

#[test]
fn test_shift_left_non_integer() {
    let result = eval_scalar_function("uni_bitwise_shiftLeft", &[json!(3.5), json!(2)]);
    assert!(result.is_err());
}

#[test]
fn test_shift_right_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_shiftRight", &[json!(12)]);
    assert!(result.is_err());
}

// ============================================================================
// Practical Use Cases
// ============================================================================

#[test]
fn test_flag_manipulation() {
    // Common use case: setting flags
    let base_flags = 0b0000;
    let read_flag = 0b0001;
    let write_flag = 0b0010;
    let execute_flag = 0b0100;

    // Set read and write flags
    let result =
        eval_scalar_function("uni_bitwise_or", &[json!(base_flags), json!(read_flag)]).unwrap();
    let result =
        eval_scalar_function("uni_bitwise_or", &[result.clone(), json!(write_flag)]).unwrap();
    assert_eq!(result, json!(0b0011)); // read + write = 3

    // Check if write flag is set
    let has_write =
        eval_scalar_function("uni_bitwise_and", &[result.clone(), json!(write_flag)]).unwrap();
    assert_eq!(has_write, json!(write_flag));

    // Check if execute flag is set
    let has_execute =
        eval_scalar_function("uni_bitwise_and", &[result, json!(execute_flag)]).unwrap();
    assert_eq!(has_execute, json!(0)); // Not set
}

#[test]
fn test_bitmask() {
    // Extract lower 4 bits
    let value = 0b11010110;
    let mask = 0b00001111;

    let result = eval_scalar_function("uni_bitwise_and", &[json!(value), json!(mask)]).unwrap();
    assert_eq!(result, json!(0b00000110)); // Lower 4 bits = 6
}

#[test]
fn test_power_of_two_check() {
    // Check if a number is a power of 2: (n & (n-1)) == 0 for powers of 2
    let n = 16;
    let n_minus_1 = n - 1;

    let result = eval_scalar_function("uni_bitwise_and", &[json!(n), json!(n_minus_1)]).unwrap();
    assert_eq!(result, json!(0)); // 16 is a power of 2

    let n = 15;
    let n_minus_1 = n - 1;
    let result = eval_scalar_function("uni_bitwise_and", &[json!(n), json!(n_minus_1)]).unwrap();
    assert!(result != json!(0)); // 15 is not a power of 2
}
