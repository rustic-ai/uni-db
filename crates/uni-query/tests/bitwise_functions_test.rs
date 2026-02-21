// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for uni.bitwise.* functions
//!
//! These tests verify that bitwise functions work correctly via the scalar function evaluator.

use uni_common::unival;
use uni_query::query::expr_eval::eval_scalar_function;

#[test]
fn test_bitwise_or() {
    let result = eval_scalar_function("uni_bitwise_or", &[unival!(12), unival!(10)]).unwrap();
    assert_eq!(result, unival!(14)); // 12 | 10 = 14 (0b1100 | 0b1010 = 0b1110)
}

#[test]
fn test_bitwise_or_zero() {
    let result = eval_scalar_function("uni_bitwise_or", &[unival!(5), unival!(0)]).unwrap();
    assert_eq!(result, unival!(5)); // 5 | 0 = 5
}

#[test]
fn test_bitwise_and() {
    let result = eval_scalar_function("uni_bitwise_and", &[unival!(12), unival!(10)]).unwrap();
    assert_eq!(result, unival!(8)); // 12 & 10 = 8 (0b1100 & 0b1010 = 0b1000)
}

#[test]
fn test_bitwise_and_zero() {
    let result = eval_scalar_function("uni_bitwise_and", &[unival!(5), unival!(0)]).unwrap();
    assert_eq!(result, unival!(0)); // 5 & 0 = 0
}

#[test]
fn test_bitwise_xor() {
    let result = eval_scalar_function("uni_bitwise_xor", &[unival!(12), unival!(10)]).unwrap();
    assert_eq!(result, unival!(6)); // 12 ^ 10 = 6 (0b1100 ^ 0b1010 = 0b0110)
}

#[test]
fn test_bitwise_xor_same() {
    let result = eval_scalar_function("uni_bitwise_xor", &[unival!(7), unival!(7)]).unwrap();
    assert_eq!(result, unival!(0)); // 7 ^ 7 = 0
}

#[test]
fn test_bitwise_not() {
    let result = eval_scalar_function("uni_bitwise_not", &[unival!(5)]).unwrap();
    assert_eq!(result, unival!(-6)); // !5 = -6 (two's complement)
}

#[test]
fn test_bitwise_not_zero() {
    let result = eval_scalar_function("uni_bitwise_not", &[unival!(0)]).unwrap();
    assert_eq!(result, unival!(-1)); // !0 = -1
}

#[test]
fn test_bitwise_not_negative() {
    let result = eval_scalar_function("uni_bitwise_not", &[unival!(-1)]).unwrap();
    assert_eq!(result, unival!(0)); // !(-1) = 0
}

#[test]
fn test_shift_left() {
    let result = eval_scalar_function("uni_bitwise_shiftLeft", &[unival!(3), unival!(2)]).unwrap();
    assert_eq!(result, unival!(12)); // 3 << 2 = 12
}

#[test]
fn test_shift_left_zero() {
    let result = eval_scalar_function("uni_bitwise_shiftLeft", &[unival!(5), unival!(0)]).unwrap();
    assert_eq!(result, unival!(5)); // 5 << 0 = 5
}

#[test]
fn test_shift_right() {
    let result =
        eval_scalar_function("uni_bitwise_shiftRight", &[unival!(12), unival!(2)]).unwrap();
    assert_eq!(result, unival!(3)); // 12 >> 2 = 3
}

#[test]
fn test_shift_right_zero() {
    let result = eval_scalar_function("uni_bitwise_shiftRight", &[unival!(5), unival!(0)]).unwrap();
    assert_eq!(result, unival!(5)); // 5 >> 0 = 5
}

// ============================================================================
// Error Cases
// ============================================================================

#[test]
fn test_bitwise_or_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_or", &[unival!(5)]);
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
    let result = eval_scalar_function("uni_bitwise_or", &[unival!("5"), unival!(3)]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("integer"));
}

#[test]
fn test_bitwise_and_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_and", &[unival!(5)]);
    assert!(result.is_err());
}

#[test]
fn test_bitwise_xor_non_integer() {
    let result = eval_scalar_function("uni_bitwise_xor", &[unival!(5.5), unival!(3)]);
    assert!(result.is_err());
}

#[test]
fn test_bitwise_not_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_not", &[unival!(5), unival!(3)]);
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
    let result = eval_scalar_function("uni_bitwise_shiftLeft", &[unival!(3.5), unival!(2)]);
    assert!(result.is_err());
}

#[test]
fn test_shift_right_wrong_arg_count() {
    let result = eval_scalar_function("uni_bitwise_shiftRight", &[unival!(12)]);
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
        eval_scalar_function("uni_bitwise_or", &[unival!(base_flags), unival!(read_flag)]).unwrap();
    let result =
        eval_scalar_function("uni_bitwise_or", &[result.clone(), unival!(write_flag)]).unwrap();
    assert_eq!(result, unival!(0b0011)); // read + write = 3

    // Check if write flag is set
    let has_write =
        eval_scalar_function("uni_bitwise_and", &[result.clone(), unival!(write_flag)]).unwrap();
    assert_eq!(has_write, unival!(write_flag));

    // Check if execute flag is set
    let has_execute =
        eval_scalar_function("uni_bitwise_and", &[result, unival!(execute_flag)]).unwrap();
    assert_eq!(has_execute, unival!(0)); // Not set
}

#[test]
fn test_bitmask() {
    // Extract lower 4 bits
    let value = 0b11010110;
    let mask = 0b00001111;

    let result = eval_scalar_function("uni_bitwise_and", &[unival!(value), unival!(mask)]).unwrap();
    assert_eq!(result, unival!(0b00000110)); // Lower 4 bits = 6
}

#[test]
fn test_power_of_two_check() {
    // Check if a number is a power of 2: (n & (n-1)) == 0 for powers of 2
    let n = 16;
    let n_minus_1 = n - 1;

    let result =
        eval_scalar_function("uni_bitwise_and", &[unival!(n), unival!(n_minus_1)]).unwrap();
    assert_eq!(result, unival!(0)); // 16 is a power of 2

    let n = 15;
    let n_minus_1 = n - 1;
    let result =
        eval_scalar_function("uni_bitwise_and", &[unival!(n), unival!(n_minus_1)]).unwrap();
    assert!(result != unival!(0)); // 15 is not a power of 2
}
