// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher Type Coercion Layer
//!
//! This module implements Cypher's type coercion semantics for all expression types.
//! It rewrites DataFusion logical expressions to handle cross-type operations correctly:
//! - Cross-type comparisons return `null` (not Arrow errors)
//! - Numeric types are widened automatically
//! - CASE expressions with cross-type operands use equality semantics
//! - Temporal types are normalized to UTC for comparison
//!
//! Design: `cypher_type_coercion_unified_design.md`

use anyhow::{Result, anyhow};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Case, Expr as DfExpr, ExprSchemable, Operator};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Type Classification
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Classifies the relationship between two Arrow types for Cypher semantics.
///
/// Used to determine how to handle comparisons, equality checks, and arithmetic
/// operations between values of potentially different types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TypeCompat {
    /// Same Arrow type, native comparison works.
    Same,
    /// Both numeric, widen to this common type before comparing.
    NumericWidening(DataType),
    /// Both DateTime structs, compare by nanos_since_epoch.
    DateTimeStruct,
    /// Both Time structs, compare by UTC-normalized nanos.
    TimeStruct,
    /// Both are string type (Utf8 / LargeUtf8).
    StringCompat,
    /// Both are boolean.
    BooleanCompat,
    /// One or both sides are null type.
    NullInvolved,
    /// Types belong to different compatibility classes. Comparison → null.
    Incomparable,
    /// At least one side has unknown/dynamic type. Need runtime UDF.
    Dynamic,
}

/// Determines type compatibility between two DataTypes for Cypher operations.
///
/// The order of rules matters and follows the design specification exactly.
pub(crate) fn type_compat(left: &DataType, right: &DataType) -> TypeCompat {
    use TypeCompat::*;

    // Rule 1: Same type → Same
    if left == right {
        return Same;
    }

    // Rule 2: Null on either side → NullInvolved
    if matches!(left, DataType::Null) || matches!(right, DataType::Null) {
        return NullInvolved;
    }

    // Rule 3: Both numeric → NumericWidening
    if is_numeric_type(left) && is_numeric_type(right) {
        let wider = super::df_expr::wider_numeric_type(left, right);
        return NumericWidening(wider);
    }

    // Rule 4: Both string → StringCompat
    if is_string_type(left) && is_string_type(right) {
        return StringCompat;
    }

    // Rule 5: Both Boolean → BooleanCompat
    if matches!(left, DataType::Boolean) && matches!(right, DataType::Boolean) {
        return BooleanCompat;
    }

    // Rule 6: Both DateTime struct → DateTimeStruct
    if super::df_expr::is_datetime_struct_type(left)
        && super::df_expr::is_datetime_struct_type(right)
    {
        return DateTimeStruct;
    }

    // Rule 7: Both Time struct → TimeStruct
    if super::df_expr::is_time_struct_type(left) && super::df_expr::is_time_struct_type(right) {
        return TimeStruct;
    }

    // Rule 8: LargeBinary on either side → Dynamic
    if matches!(left, DataType::LargeBinary) || matches!(right, DataType::LargeBinary) {
        return Dynamic;
    }

    // Rule 9: Both non-temporal structs → Dynamic
    // (Reviewer fix 2.4: require BOTH sides are structs)
    if let (DataType::Struct(_), DataType::Struct(_)) = (left, right) {
        // Both are structs, neither is DateTime nor Time (checked above)
        return Dynamic;
    }

    // Rule 10: Everything else (including one struct + one non-struct) → Incomparable
    Incomparable
}

/// Returns true if the DataType represents a numeric type in Cypher.
pub(crate) fn is_numeric_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Returns true if the DataType represents a string type in Cypher.
pub(crate) fn is_string_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Equality and Comparison Builders
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Builds a Cypher-correct comparison expression for any comparison operator.
///
/// Handles cross-type comparisons according to Cypher semantics:
/// - Compatible types: native Arrow comparison (with widening if needed)
/// - Temporal types: compare by UTC-normalized nanoseconds
/// - Null or incomparable types: return `null` (per openCypher CIP2016-06-14)
/// - Dynamic types: delegate to runtime UDF
///
/// Works for all comparison operators: Eq, NotEq, Lt, LtEq, Gt, GtEq.
pub(crate) fn build_cypher_comparison(
    left: DfExpr,
    left_type: &DataType,
    right: DfExpr,
    right_type: &DataType,
    op: Operator,
) -> DfExpr {
    use TypeCompat::*;

    match type_compat(left_type, right_type) {
        Same | StringCompat | BooleanCompat => binary_expr(left, op, right),
        NumericWidening(common) => {
            let left_cast = super::df_expr::cast_expr(left, common.clone());
            let right_cast = super::df_expr::cast_expr(right, common);
            binary_expr(left_cast, op, right_cast)
        }
        DateTimeStruct => {
            let left_nanos = super::df_expr::extract_datetime_nanos(left);
            let right_nanos = super::df_expr::extract_datetime_nanos(right);
            binary_expr(left_nanos, op, right_nanos)
        }
        TimeStruct => {
            let left_nanos = super::df_expr::extract_time_nanos(left);
            let right_nanos = super::df_expr::extract_time_nanos(right);
            binary_expr(left_nanos, op, right_nanos)
        }
        NullInvolved | Incomparable => lit(ScalarValue::Boolean(None)),
        Dynamic => {
            let udf_name = super::df_expr::comparison_udf_name(op)
                .expect("comparison operator should have UDF mapping");
            super::df_expr::dummy_udf_expr(udf_name, vec![left, right])
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Addition Operator (overloaded: numeric, string concat, list operations)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Builds a Cypher-correct Plus expression.
///
/// Handles:
/// - Null propagation (explicit, not accidental)
/// - LargeBinary routing to UDF
/// - Numeric widening and addition
/// - String concatenation (with toString coercion)
/// - List concatenation and append/prepend
///
/// Order of checks (reviewer fix 3.4):
/// 1. Null propagation first
/// 2. LargeBinary
/// 3. Both numeric
/// 4. Both string
/// 5. String + anything
/// 6. Anything + String
/// 7. List operations
/// 8. Error for incompatible types
pub(crate) fn build_cypher_plus(
    left: DfExpr,
    left_type: &DataType,
    right: DfExpr,
    right_type: &DataType,
) -> Result<DfExpr> {
    // 1. Null propagation first (reviewer fix 3.4: explicit, not accidental)
    if matches!(left_type, DataType::Null) || matches!(right_type, DataType::Null) {
        return Ok(lit(ScalarValue::Null));
    }

    // 2. LargeBinary: Either side LB → UDF
    if matches!(left_type, DataType::LargeBinary) || matches!(right_type, DataType::LargeBinary) {
        return Ok(super::df_expr::dummy_udf_expr(
            "_cypher_add",
            vec![left, right],
        ));
    }

    // 3. Both numeric: Widen and add
    if is_numeric_type(left_type) && is_numeric_type(right_type) {
        let common = super::df_expr::wider_numeric_type(left_type, right_type);
        let left_cast = super::df_expr::cast_expr(left, common.clone());
        let right_cast = super::df_expr::cast_expr(right, common);
        return Ok(binary_expr(left_cast, Operator::Plus, right_cast));
    }

    // 4. Both string: Concatenate
    if is_string_type(left_type) && is_string_type(right_type) {
        return Ok(datafusion::functions::string::expr_fn::concat(vec![
            left, right,
        ]));
    }

    // 5. String + anything: toString(right) and concat
    if is_string_type(left_type) {
        let right_str = to_string_expr(right, right_type)?;
        return Ok(datafusion::functions::string::expr_fn::concat(vec![
            left, right_str,
        ]));
    }

    // 6. Anything + String: toString(left) and concat
    if is_string_type(right_type) {
        let left_str = to_string_expr(left, left_type)?;
        return Ok(datafusion::functions::string::expr_fn::concat(vec![
            left_str, right,
        ]));
    }

    // 7. List operations
    if matches!(left_type, DataType::List(_)) && matches!(right_type, DataType::List(_)) {
        // Both lists → concatenate
        return Ok(super::df_expr::dummy_udf_expr(
            "_cypher_list_concat",
            vec![left, right],
        ));
    }

    if matches!(left_type, DataType::List(_)) {
        // List + scalar → append
        return Ok(super::df_expr::dummy_udf_expr(
            "_cypher_list_append",
            vec![left, right],
        ));
    }

    if matches!(right_type, DataType::List(_)) {
        // scalar + List → prepend (UDF handles direction per test at line 5554)
        return Ok(super::df_expr::dummy_udf_expr(
            "_cypher_list_append",
            vec![right, left],
        ));
    }

    // 8. Otherwise: Error
    Err(anyhow!(
        "Incompatible types for Plus operator: {:?} + {:?}",
        left_type,
        right_type
    ))
}

/// Converts a value to a string expression for concatenation.
///
/// Currently uses Utf8 cast for all types. When dedicated temporal toString UDFs
/// are added (e.g., `_cypher_datetime_tostring`), this should dispatch to them
/// for DateTime/Time structs.
fn to_string_expr(expr: DfExpr, _expr_type: &DataType) -> Result<DfExpr> {
    Ok(super::df_expr::cast_expr(expr, DataType::Utf8))
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CASE Expression Handling
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Rewrites a simple CASE (with operand) to a generic CASE (no operand).
///
/// Transforms:
///   `CASE operand WHEN val1 THEN res1 WHEN val2 THEN res2 ELSE res3 END`
/// Into:
///   `CASE WHEN operand=val1 THEN res1 WHEN operand=val2 THEN res2 ELSE res3 END`
///
/// Uses `build_cypher_comparison()` for each comparison to handle cross-type operands.
pub(crate) fn rewrite_simple_case_to_generic(
    operand: DfExpr,
    when_then_expr: Vec<(Box<DfExpr>, Box<DfExpr>)>,
    else_expr: Option<Box<DfExpr>>,
    schema: &datafusion::common::DFSchema,
) -> Result<Case> {
    let operand_type = operand
        .get_type(schema)
        .map_err(|e| anyhow!("Failed to get operand type: {}", e))?;

    let new_when_then = when_then_expr
        .into_iter()
        .map(|(when, then)| {
            let when_type = when
                .get_type(schema)
                .map_err(|e| anyhow!("Failed to get WHEN type: {}", e))?;
            let eq_expr = build_cypher_comparison(
                operand.clone(),
                &operand_type,
                *when,
                &when_type,
                Operator::Eq,
            );
            Ok((Box::new(eq_expr), then))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Case {
        expr: None, // Generic CASE has no operand
        when_then_expr: new_when_then,
        else_expr,
    })
}

/// Finds the common result type for CASE THEN/ELSE branches.
///
/// Priority rules (design spec):
/// 1. All same (excluding Null) → that type
/// 2. All numeric → widest
/// 3. Any string → Utf8
/// 4. All DateTime struct → DateTime struct type
/// 5. All Time struct → Time struct type
/// 6. Fallback → Utf8
pub(crate) fn find_common_result_type(
    types: &[DataType],
    _schema: &datafusion::common::DFSchema,
) -> DataType {
    if types.is_empty() {
        return DataType::Utf8; // Fallback
    }

    // Filter out Null types for consideration
    let non_null_types: Vec<&DataType> = types
        .iter()
        .filter(|t| !matches!(t, DataType::Null))
        .collect();

    if non_null_types.is_empty() {
        return DataType::Null; // All were null
    }

    // Rule 1: All same type → that type
    let first = non_null_types[0];
    if non_null_types.iter().all(|t| *t == first) {
        return first.clone();
    }

    // Rule 2: All numeric → widest
    if non_null_types.iter().all(|t| is_numeric_type(t)) {
        let mut widest = DataType::Int8;
        for t in non_null_types {
            widest = super::df_expr::wider_numeric_type(&widest, t);
        }
        return widest;
    }

    // Rule 3: Any string → Utf8
    if non_null_types.iter().any(|t| is_string_type(t)) {
        return DataType::Utf8;
    }

    // Rule 4: All DateTime struct → DateTime struct type
    if non_null_types
        .iter()
        .all(|t| super::df_expr::is_datetime_struct_type(t))
    {
        return first.clone(); // All are DateTime structs, return first
    }

    // Rule 5: All Time struct → Time struct type
    if non_null_types
        .iter()
        .all(|t| super::df_expr::is_time_struct_type(t))
    {
        return first.clone(); // All are Time structs, return first
    }

    // Rule 6: Fallback → Utf8
    DataType::Utf8
}

/// Coerces all CASE result types (THEN and ELSE branches) to a common type.
///
/// Note (reviewer fix 3.6): Cast wrapping returns correct post-coercion types
/// since sub-expressions were already coerced. This comment clarifies that
/// get_type() on the result will reflect the casted type.
pub(crate) fn coerce_case_results(
    case: &mut Case,
    schema: &datafusion::common::DFSchema,
) -> Result<()> {
    // Collect all result types
    let mut types = Vec::new();
    for (_, then_expr) in &case.when_then_expr {
        let then_type = then_expr
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get THEN type: {}", e))?;
        types.push(then_type);
    }
    if let Some(else_expr) = &case.else_expr {
        let else_type = else_expr
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get ELSE type: {}", e))?;
        types.push(else_type);
    }

    let common_type = find_common_result_type(&types, schema);

    // Cast all THEN branches to common type
    for (_, then_expr) in &mut case.when_then_expr {
        let then_type = then_expr
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get THEN type for cast: {}", e))?;
        if then_type != common_type {
            *then_expr = Box::new(super::df_expr::cast_expr(
                (**then_expr).clone(),
                common_type.clone(),
            ));
        }
    }

    // Cast ELSE branch to common type
    if let Some(else_expr) = &mut case.else_expr {
        let else_type = else_expr
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get ELSE type for cast: {}", e))?;
        if else_type != common_type {
            *else_expr = Box::new(super::df_expr::cast_expr(
                (**else_expr).clone(),
                common_type.clone(),
            ));
        }
    }

    Ok(())
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// IN Operator
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Builds a Cypher-correct IN list expression.
///
/// Two-pass approach (reviewer fix 2.2):
/// 1. First pass: Classify all items. If any require OR-chain (DateTime/Time/Dynamic),
///    immediately fall back to OR chain.
/// 2. Second pass: Partition into compatible/incomparable.
///
/// Null semantics:
/// - All compatible → native InList
/// - All incomparable → null
/// - Mixed → CASE WHEN expr IN [compatible...] THEN true/false ELSE null END
pub(crate) fn build_cypher_in_list(
    expr: DfExpr,
    expr_type: &DataType,
    list: Vec<DfExpr>,
    negated: bool,
    schema: &datafusion::common::DFSchema,
) -> Result<DfExpr> {
    // First pass: Check if we need OR chain
    for item in &list {
        let item_type = item
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get IN list item type: {}", e))?;

        match type_compat(expr_type, &item_type) {
            TypeCompat::DateTimeStruct | TypeCompat::TimeStruct | TypeCompat::Dynamic => {
                // Need OR chain for these types
                return build_in_as_or_chain(expr, expr_type, list, negated, schema);
            }
            _ => {} // Continue checking
        }
    }

    // Second pass: Partition into compatible and incomparable
    let mut compatible = Vec::new();
    let mut has_incomparable = false;

    for item in list {
        let item_type = item
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get IN list item type: {}", e))?;

        match type_compat(expr_type, &item_type) {
            TypeCompat::Same | TypeCompat::StringCompat | TypeCompat::BooleanCompat => {
                compatible.push(item);
            }
            TypeCompat::NumericWidening(common) => {
                let cast_item = super::df_expr::cast_expr(item, common);
                compatible.push(cast_item);
            }
            TypeCompat::NullInvolved => {
                has_incomparable = true;
                compatible.push(item); // Keep null — DataFusion handles it
            }
            TypeCompat::Incomparable => {
                has_incomparable = true;
                // Drop this item — it can never match
            }
            _ => {
                // Should have been caught in first pass
                return Err(anyhow!("Unexpected type compat in second pass"));
            }
        }
    }

    // Build result based on what we found
    if !has_incomparable {
        // Simple case: all compatible
        Ok(DfExpr::InList(InList {
            expr: Box::new(expr),
            list: compatible,
            negated,
        }))
    } else if compatible.is_empty() {
        // All incomparable → null
        Ok(lit(ScalarValue::Boolean(None)))
    } else {
        // Mixed case: CASE WHEN expr IN [compatible] THEN true/false ELSE null
        let in_expr = DfExpr::InList(InList {
            expr: Box::new(expr),
            list: compatible,
            negated,
        });

        let result_val = if negated {
            lit(ScalarValue::Boolean(Some(false)))
        } else {
            lit(ScalarValue::Boolean(Some(true)))
        };

        Ok(when(in_expr, result_val).otherwise(lit(ScalarValue::Boolean(None)))?)
    }
}

/// Builds an OR chain for IN list (used for DateTime/Time/Dynamic types).
///
/// `x IN [a, b, c]` → `x = a OR x = b OR x = c`
/// `x NOT IN [a, b, c]` → `NOT(x = a OR x = b OR x = c)`
///
/// Note (reviewer fix 3.5): NOT IN uses NOT(OR chain) which correctly propagates nulls.
/// If any comparison is null and none are true, the whole OR is null, and NOT(null) = null.
fn build_in_as_or_chain(
    expr: DfExpr,
    expr_type: &DataType,
    list: Vec<DfExpr>,
    negated: bool,
    schema: &datafusion::common::DFSchema,
) -> Result<DfExpr> {
    if list.is_empty() {
        // Empty list: IN → false, NOT IN → true
        return Ok(lit(ScalarValue::Boolean(Some(negated))));
    }

    let mut or_chain = None;

    for item in list {
        let item_type = item
            .get_type(schema)
            .map_err(|e| anyhow!("Failed to get item type in OR chain: {}", e))?;

        let eq_expr =
            build_cypher_comparison(expr.clone(), expr_type, item, &item_type, Operator::Eq);

        or_chain = Some(match or_chain {
            None => eq_expr,
            Some(chain) => binary_expr(chain, Operator::Or, eq_expr),
        });
    }

    let result = or_chain.unwrap(); // Safe because list is non-empty

    if negated { Ok(not(result)) } else { Ok(result) }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Unit Tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_compat_same() {
        let dt = DataType::Int64;
        assert_eq!(type_compat(&dt, &dt), TypeCompat::Same);
    }

    #[test]
    fn test_type_compat_numeric_widening() {
        let left = DataType::Int64;
        let right = DataType::Float64;
        match type_compat(&left, &right) {
            TypeCompat::NumericWidening(common) => assert_eq!(common, DataType::Float64),
            _ => panic!("Expected NumericWidening"),
        }
    }

    #[test]
    fn test_type_compat_incomparable() {
        let left = DataType::Utf8;
        let right = DataType::Int64;
        assert_eq!(type_compat(&left, &right), TypeCompat::Incomparable);
    }

    #[test]
    fn test_type_compat_null_involved() {
        let left = DataType::Null;
        let right = DataType::Int64;
        assert_eq!(type_compat(&left, &right), TypeCompat::NullInvolved);
    }

    #[test]
    fn test_type_compat_string_compat() {
        let left = DataType::Utf8;
        let right = DataType::Utf8;
        assert_eq!(type_compat(&left, &right), TypeCompat::Same);
    }

    #[test]
    fn test_build_cypher_comparison_incomparable_returns_null() {
        let left = lit(ScalarValue::Utf8(Some("hello".to_string())));
        let right = lit(ScalarValue::Int64(Some(42)));
        let result =
            build_cypher_comparison(left, &DataType::Utf8, right, &DataType::Int64, Operator::Eq);

        // Should return null literal
        match result {
            DfExpr::Literal(ScalarValue::Boolean(None), _) => {} // Success
            _ => panic!("Expected null literal for incomparable types"),
        }
    }

    #[test]
    fn test_build_cypher_comparison_null_involved() {
        let left = lit(ScalarValue::Null);
        let right = lit(ScalarValue::Int64(Some(42)));
        let result =
            build_cypher_comparison(left, &DataType::Null, right, &DataType::Int64, Operator::Eq);

        // Should return null literal
        match result {
            DfExpr::Literal(ScalarValue::Boolean(None), _) => {} // Success
            _ => panic!("Expected null literal for null involved"),
        }
    }

    #[test]
    fn test_build_cypher_plus_null_propagation() {
        let left = lit(ScalarValue::Null);
        let right = lit(ScalarValue::Int64(Some(42)));
        let result = build_cypher_plus(left, &DataType::Null, right, &DataType::Int64);

        // Should return null
        match result {
            Ok(DfExpr::Literal(ScalarValue::Null, _)) => {} // Success
            _ => panic!("Expected null for null propagation"),
        }
    }

    #[test]
    fn test_find_common_result_type_all_same() {
        let types = vec![DataType::Int64, DataType::Int64, DataType::Int64];
        let schema = datafusion::common::DFSchema::empty();
        let common = find_common_result_type(&types, &schema);
        assert_eq!(common, DataType::Int64);
    }

    #[test]
    fn test_find_common_result_type_numeric_widening() {
        let types = vec![DataType::Int64, DataType::Float64, DataType::Int32];
        let schema = datafusion::common::DFSchema::empty();
        let common = find_common_result_type(&types, &schema);
        assert_eq!(common, DataType::Float64);
    }

    #[test]
    fn test_find_common_result_type_any_string() {
        let types = vec![DataType::Int64, DataType::Utf8, DataType::Float64];
        let schema = datafusion::common::DFSchema::empty();
        let common = find_common_result_type(&types, &schema);
        assert_eq!(common, DataType::Utf8);
    }

    #[test]
    fn test_type_compat_one_struct_one_non_struct_incomparable() {
        use datafusion::arrow::datatypes::Field;
        let struct_type = DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into());
        let non_struct = DataType::Int64;

        // One struct + one non-struct → Incomparable (not Dynamic)
        assert_eq!(
            type_compat(&struct_type, &non_struct),
            TypeCompat::Incomparable
        );
        assert_eq!(
            type_compat(&non_struct, &struct_type),
            TypeCompat::Incomparable
        );
    }

    #[test]
    fn test_type_compat_both_non_temporal_structs_dynamic() {
        use datafusion::arrow::datatypes::Field;
        let struct1 = DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into());
        let struct2 = DataType::Struct(vec![Field::new("b", DataType::Utf8, true)].into());

        // Both non-temporal structs → Dynamic
        assert_eq!(type_compat(&struct1, &struct2), TypeCompat::Dynamic);
    }
}
