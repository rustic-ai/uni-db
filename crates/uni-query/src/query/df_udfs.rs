// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Cypher-specific User Defined Functions (UDFs) for DataFusion.
//!
//! This module provides UDFs for Cypher built-in functions that need to be
//! registered with the DataFusion SessionContext. These include:
//!
//! - `id(n)` - Returns the internal VID/EID of a node or relationship
//! - `type(r)` - Returns the type name of a relationship
//! - `keys(map)` - Returns the keys of a map or properties of a node/edge
//! - `properties(n)` - Returns all properties of a node or edge as a map
//! - `coalesce(...)` - Returns the first non-null argument
//! - `toInteger(x)` - Converts a value to an integer
//! - `toString(x)` - Converts a value to a string
//!
//! # Usage
//!
//! ```ignore
//! use uni_query::query::df_udfs::register_cypher_udfs;
//!
//! let ctx = SessionContext::new();
//! register_cypher_udfs(&ctx)?;
//! ```

use arrow::datatypes::DataType;
use arrow_array::{Array, ArrayRef, StringArray};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use lance_datafusion::udf::json::{
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_string_udf,
};
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Macro to implement common UDF trait boilerplate.
///
/// Implements PartialEq, Eq, and Hash based on the UDF name.
macro_rules! impl_udf_eq_hash {
    ($type:ty) => {
        impl PartialEq for $type {
            fn eq(&self, other: &Self) -> bool {
                self.signature == other.signature
            }
        }

        impl Eq for $type {}

        impl Hash for $type {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.name().hash(state);
            }
        }
    };
}

/// Register all Cypher UDFs with the given SessionContext.
///
/// Only registers UDFs that are graph-specific or not available in DataFusion.
/// Type conversions (toInteger, toFloat, etc.) use CAST expressions instead.
/// String functions (left, right, substring, split) use DataFusion's built-in functions.
///
/// # Errors
///
/// Returns an error if UDF registration fails.
pub fn register_cypher_udfs(ctx: &SessionContext) -> DFResult<()> {
    ctx.register_udf(create_id_udf());
    ctx.register_udf(create_type_udf());
    ctx.register_udf(create_keys_udf());
    ctx.register_udf(create_range_udf());

    // Bitwise UDFs
    ctx.register_udf(create_bitwise_or_udf());
    ctx.register_udf(create_bitwise_and_udf());
    ctx.register_udf(create_bitwise_xor_udf());
    ctx.register_udf(create_bitwise_not_udf());
    ctx.register_udf(create_shift_left_udf());
    ctx.register_udf(create_shift_right_udf());

    // JSON UDFs for overflow property access (Lance built-in, optimized for JSONB binary)
    ctx.register_udf(json_get_string_udf());
    ctx.register_udf(json_get_int_udf());
    ctx.register_udf(json_get_float_udf());
    ctx.register_udf(json_get_bool_udf());

    Ok(())
}

// ============================================================================
// id(node) -> UInt64
// ============================================================================

/// Create the `id` UDF for getting vertex/edge internal IDs.
///
/// Returns the internal VID or EID of a node or relationship.
pub fn create_id_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(IdUdf::new())
}

#[derive(Debug)]
struct IdUdf {
    signature: Signature,
}

impl IdUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::UInt64]),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(IdUdf);

impl ScalarUDFImpl for IdUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // id() is a pass-through - the VID/EID is already stored as UInt64
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "id() requires 1 argument".to_string(),
            ));
        }
        Ok(args.args[0].clone())
    }
}

// ============================================================================
// type(relationship) -> String
// ============================================================================

/// Create the `type` UDF for getting relationship type names.
///
/// Returns the type name of a relationship as a string.
pub fn create_type_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TypeUdf::new())
}

#[derive(Debug)]
struct TypeUdf {
    signature: Signature,
}

impl TypeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(TypeUdf);

impl ScalarUDFImpl for TypeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // type() is a pass-through - the edge type is already stored as a string column
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "type() requires 1 argument".to_string(),
            ));
        }
        Ok(args.args[0].clone())
    }
}

// ============================================================================
// keys(map) -> List<String>
// ============================================================================

/// Create the `keys` UDF for getting map keys.
///
/// Returns the keys of a map or the property names of a node/edge.
pub fn create_keys_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(KeysUdf::new())
}

#[derive(Debug)]
struct KeysUdf {
    signature: Signature,
}

impl KeysUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(KeysUdf);

impl ScalarUDFImpl for KeysUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(
            arrow::datatypes::Field::new_list_field(DataType::Utf8, true),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "keys() requires 1 argument".to_string(),
            ));
        }

        // For JSON string input, parse and extract keys
        match &args.args[0] {
            ColumnarValue::Array(arr) => {
                if let Some(string_arr) = arr.as_any().downcast_ref::<StringArray>() {
                    let mut list_builder = arrow_array::builder::ListBuilder::new(
                        arrow_array::builder::StringBuilder::new(),
                    );

                    for i in 0..string_arr.len() {
                        if string_arr.is_null(i) {
                            list_builder.append_null();
                            continue;
                        }

                        let json_str = string_arr.value(i);
                        if let Ok(serde_json::Value::Object(map)) =
                            serde_json::from_str::<serde_json::Value>(json_str)
                        {
                            let values = list_builder.values();
                            for key in map.keys() {
                                values.append_value(key);
                            }
                            list_builder.append(true);
                        } else {
                            // Not a JSON object, return empty list
                            list_builder.append(true);
                        }
                    }

                    let result: ArrayRef = Arc::new(list_builder.finish());
                    Ok(ColumnarValue::Array(result))
                } else {
                    Err(datafusion::error::DataFusionError::Execution(
                        "keys() expects a string (JSON) argument".to_string(),
                    ))
                }
            }
            ColumnarValue::Scalar(s) => {
                // Handle scalar case
                if let datafusion::common::ScalarValue::Utf8(Some(json_str)) = s {
                    if let Ok(serde_json::Value::Object(map)) =
                        serde_json::from_str::<serde_json::Value>(json_str)
                    {
                        // Build list of keys as ScalarValues
                        let key_scalars: Vec<datafusion::common::ScalarValue> = map
                            .keys()
                            .map(|k| datafusion::common::ScalarValue::Utf8(Some(k.clone())))
                            .collect();
                        let list = datafusion::common::ScalarValue::List(
                            datafusion::common::ScalarValue::new_list(
                                &key_scalars,
                                &DataType::Utf8,
                                true,
                            ),
                        );
                        Ok(ColumnarValue::Scalar(list))
                    } else {
                        // Not a JSON object, return empty list
                        let empty_list = datafusion::common::ScalarValue::List(
                            datafusion::common::ScalarValue::new_list_nullable(
                                &[],
                                &DataType::Utf8,
                            ),
                        );
                        Ok(ColumnarValue::Scalar(empty_list))
                    }
                } else {
                    Err(datafusion::error::DataFusionError::Execution(
                        "keys() expects a string (JSON) argument".to_string(),
                    ))
                }
            }
        }
    }
}

// ============================================================================
// range(start, end, [step]) -> List<Int64>
// ============================================================================

/// Create the `range` UDF for generating integer ranges.
pub fn create_range_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RangeUdf::new())
}

#[derive(Debug)]
struct RangeUdf {
    signature: Signature,
}

impl RangeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(RangeUdf);

impl ScalarUDFImpl for RangeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "range"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(
            arrow::datatypes::Field::new_list_field(DataType::Int64, true),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return Err(datafusion::error::DataFusionError::Execution(
                "range() requires 2 or 3 arguments".to_string(),
            ));
        }

        // Extract scalar values
        let start = match &args.args[0] {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(v))) => *v,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "range() start must be an integer".to_string(),
                ));
            }
        };

        let end = match &args.args[1] {
            ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(v))) => *v,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "range() end must be an integer".to_string(),
                ));
            }
        };

        let step = if args.args.len() == 3 {
            match &args.args[2] {
                ColumnarValue::Scalar(datafusion::common::ScalarValue::Int64(Some(v))) => *v,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "range() step must be an integer".to_string(),
                    ));
                }
            }
        } else {
            1
        };

        if step == 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "range() step cannot be zero".to_string(),
            ));
        }

        // Generate range
        let mut values = Vec::new();
        if step > 0 {
            let mut current = start;
            while current <= end {
                values.push(datafusion::common::ScalarValue::Int64(Some(current)));
                current += step;
            }
        } else {
            let mut current = start;
            while current >= end {
                values.push(datafusion::common::ScalarValue::Int64(Some(current)));
                current += step;
            }
        }

        let list = datafusion::common::ScalarValue::List(
            datafusion::common::ScalarValue::new_list(&values, &DataType::Int64, true),
        );
        Ok(ColumnarValue::Scalar(list))
    }
}

// ============================================================================
// Bitwise Functions (uni.bitwise.*)
// ============================================================================

/// Create the `uni.bitwise.or` UDF for bitwise OR operations.
pub fn create_bitwise_or_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseOrUdf::new())
}

#[derive(Debug)]
struct BitwiseOrUdf {
    signature: Signature,
}

impl BitwiseOrUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitwiseOrUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uni.bitwise.or"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow_array::Int64Array;
        use datafusion::common::ScalarValue;
        use datafusion::error::DataFusionError;

        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "uni.bitwise.or requires exactly 2 arguments".to_string(),
            ));
        }

        let left = &args.args[0];
        let right = &args.args[1];

        match (left, right) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(Some(l))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(r))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(l | r)))),
            (ColumnarValue::Array(l_arr), ColumnarValue::Array(r_arr)) => {
                let l_arr = l_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Left array must be Int64".to_string())
                })?;
                let r_arr = r_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Right array must be Int64".to_string())
                })?;

                let result: Int64Array = l_arr
                    .iter()
                    .zip(r_arr.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(l), Some(r)) => Some(l | r),
                        _ => None,
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "Mixed scalar/array not supported for uni.bitwise.or".to_string(),
            )),
        }
    }
}

impl_udf_eq_hash!(BitwiseOrUdf);

/// Create the `uni.bitwise.and` UDF for bitwise AND operations.
pub fn create_bitwise_and_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseAndUdf::new())
}

#[derive(Debug)]
struct BitwiseAndUdf {
    signature: Signature,
}

impl BitwiseAndUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitwiseAndUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uni.bitwise.and"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow_array::Int64Array;
        use datafusion::common::ScalarValue;
        use datafusion::error::DataFusionError;

        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "uni.bitwise.and requires exactly 2 arguments".to_string(),
            ));
        }

        let left = &args.args[0];
        let right = &args.args[1];

        match (left, right) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(Some(l))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(r))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(l & r)))),
            (ColumnarValue::Array(l_arr), ColumnarValue::Array(r_arr)) => {
                let l_arr = l_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Left array must be Int64".to_string())
                })?;
                let r_arr = r_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Right array must be Int64".to_string())
                })?;

                let result: Int64Array = l_arr
                    .iter()
                    .zip(r_arr.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(l), Some(r)) => Some(l & r),
                        _ => None,
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "Mixed scalar/array not supported for uni.bitwise.and".to_string(),
            )),
        }
    }
}

impl_udf_eq_hash!(BitwiseAndUdf);

/// Create the `uni.bitwise.xor` UDF for bitwise XOR operations.
pub fn create_bitwise_xor_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseXorUdf::new())
}

#[derive(Debug)]
struct BitwiseXorUdf {
    signature: Signature,
}

impl BitwiseXorUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitwiseXorUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uni.bitwise.xor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow_array::Int64Array;
        use datafusion::common::ScalarValue;
        use datafusion::error::DataFusionError;

        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "uni.bitwise.xor requires exactly 2 arguments".to_string(),
            ));
        }

        let left = &args.args[0];
        let right = &args.args[1];

        match (left, right) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(Some(l))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(r))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(l ^ r)))),
            (ColumnarValue::Array(l_arr), ColumnarValue::Array(r_arr)) => {
                let l_arr = l_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Left array must be Int64".to_string())
                })?;
                let r_arr = r_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Right array must be Int64".to_string())
                })?;

                let result: Int64Array = l_arr
                    .iter()
                    .zip(r_arr.iter())
                    .map(|(l, r)| match (l, r) {
                        (Some(l), Some(r)) => Some(l ^ r),
                        _ => None,
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "Mixed scalar/array not supported for uni.bitwise.xor".to_string(),
            )),
        }
    }
}

impl_udf_eq_hash!(BitwiseXorUdf);

/// Create the `uni.bitwise.not` UDF for bitwise NOT operations.
pub fn create_bitwise_not_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseNotUdf::new())
}

#[derive(Debug)]
struct BitwiseNotUdf {
    signature: Signature,
}

impl BitwiseNotUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitwiseNotUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uni.bitwise.not"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow_array::Int64Array;
        use datafusion::common::ScalarValue;
        use datafusion::error::DataFusionError;

        if args.args.len() != 1 {
            return Err(DataFusionError::Execution(
                "uni.bitwise.not requires exactly 1 argument".to_string(),
            ));
        }

        let operand = &args.args[0];

        match operand {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(!v))))
            }
            ColumnarValue::Array(arr) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Execution("Array must be Int64".to_string()))?;

                let result: Int64Array = arr.iter().map(|v| v.map(|val| !val)).collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid argument type for uni.bitwise.not".to_string(),
            )),
        }
    }
}

impl_udf_eq_hash!(BitwiseNotUdf);

/// Create the `uni.bitwise.shiftLeft` UDF for left shift operations.
pub fn create_shift_left_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ShiftLeftUdf::new())
}

#[derive(Debug)]
struct ShiftLeftUdf {
    signature: Signature,
}

impl ShiftLeftUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ShiftLeftUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uni.bitwise.shiftLeft"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow_array::Int64Array;
        use datafusion::common::ScalarValue;
        use datafusion::error::DataFusionError;

        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "uni.bitwise.shiftLeft requires exactly 2 arguments".to_string(),
            ));
        }

        let value = &args.args[0];
        let shift = &args.args[1];

        match (value, shift) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(s))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(v << s)))),
            (ColumnarValue::Array(v_arr), ColumnarValue::Array(s_arr)) => {
                let v_arr = v_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Value array must be Int64".to_string())
                })?;
                let s_arr = s_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Shift array must be Int64".to_string())
                })?;

                let result: Int64Array = v_arr
                    .iter()
                    .zip(s_arr.iter())
                    .map(|(v, s)| match (v, s) {
                        (Some(v), Some(s)) => Some(v << s),
                        _ => None,
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "Mixed scalar/array not supported for uni.bitwise.shiftLeft".to_string(),
            )),
        }
    }
}

impl_udf_eq_hash!(ShiftLeftUdf);

/// Create the `uni.bitwise.shiftRight` UDF for right shift operations.
pub fn create_shift_right_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ShiftRightUdf::new())
}

#[derive(Debug)]
struct ShiftRightUdf {
    signature: Signature,
}

impl ShiftRightUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ShiftRightUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uni.bitwise.shiftRight"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow_array::Int64Array;
        use datafusion::common::ScalarValue;
        use datafusion::error::DataFusionError;

        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "uni.bitwise.shiftRight requires exactly 2 arguments".to_string(),
            ));
        }

        let value = &args.args[0];
        let shift = &args.args[1];

        match (value, shift) {
            (
                ColumnarValue::Scalar(ScalarValue::Int64(Some(v))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(s))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(v >> s)))),
            (ColumnarValue::Array(v_arr), ColumnarValue::Array(s_arr)) => {
                let v_arr = v_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Value array must be Int64".to_string())
                })?;
                let s_arr = s_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Execution("Shift array must be Int64".to_string())
                })?;

                let result: Int64Array = v_arr
                    .iter()
                    .zip(s_arr.iter())
                    .map(|(v, s)| match (v, s) {
                        (Some(v), Some(s)) => Some(v >> s),
                        _ => None,
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => Err(DataFusionError::Execution(
                "Mixed scalar/array not supported for uni.bitwise.shiftRight".to_string(),
            )),
        }
    }
}

impl_udf_eq_hash!(ShiftRightUdf);

// ============================================================================
// JSON UDFs - Now using Lance's built-in implementations
// ============================================================================
// Lance provides optimized JSONB binary UDFs:
// - json_get_string(jsonb_binary, key) -> String
// - json_get_int(jsonb_binary, key) -> Int64
// - json_get_float(jsonb_binary, key) -> Float64
// - json_get_bool(jsonb_binary, key) -> Boolean
//
// These are registered in register_cypher_udfs() above.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::FunctionRegistry;

    #[test]
    fn test_register_udfs() {
        let ctx = SessionContext::new();
        register_cypher_udfs(&ctx).unwrap();

        // Verify only graph-specific and necessary UDFs are registered
        // Type conversions use CAST, string functions use DataFusion built-ins
        assert!(ctx.udf("id").is_ok());
        assert!(ctx.udf("type").is_ok());
        assert!(ctx.udf("keys").is_ok());
        assert!(ctx.udf("range").is_ok());
        assert!(
            ctx.udf("json_get_string").is_ok(),
            "json_get_string UDF should be registered"
        );
    }

    #[test]
    fn test_id_udf_signature() {
        let udf = create_id_udf();
        assert_eq!(udf.name(), "id");
    }

    #[test]
    fn test_type_udf_signature() {
        let udf = create_type_udf();
        assert_eq!(udf.name(), "type");
    }
}
