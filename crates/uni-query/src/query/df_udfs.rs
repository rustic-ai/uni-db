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
use arrow_array::Array;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
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
    ctx.register_udf(create_labels_udf());
    ctx.register_udf(create_nodes_udf());
    ctx.register_udf(create_relationships_udf());
    ctx.register_udf(create_range_udf());
    ctx.register_udf(create_index_udf());

    // Type conversion UDFs
    ctx.register_udf(create_to_integer_udf());
    ctx.register_udf(create_to_float_udf());
    ctx.register_udf(create_to_boolean_udf());

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

    // Temporal constructor UDFs
    for name in &[
        "date",
        "time",
        "localtime",
        "localdatetime",
        "datetime",
        "duration",
    ] {
        ctx.register_udf(create_temporal_udf(name));
    }

    // Temporal dotted function UDFs
    for name in &[
        "duration.between",
        "duration.inmonths",
        "duration.indays",
        "duration.inseconds",
        "datetime.fromepoch",
        "datetime.fromepochmillis",
        "date.truncate",
        "time.truncate",
        "datetime.truncate",
        "localdatetime.truncate",
        "localtime.truncate",
        "datetime.transaction",
        "datetime.statement",
        "datetime.realtime",
        "date.transaction",
        "date.statement",
        "date.realtime",
        "time.transaction",
        "time.statement",
        "time.realtime",
        "localtime.transaction",
        "localtime.statement",
        "localtime.realtime",
        "localdatetime.transaction",
        "localdatetime.statement",
        "localdatetime.realtime",
    ] {
        ctx.register_udf(create_temporal_udf(name));
    }

    // Duration property accessor UDF
    ctx.register_udf(create_duration_property_udf());

    // Temporal extraction UDFs (year, month, day, etc.)
    for name in &["year", "month", "day", "hour", "minute", "second"] {
        ctx.register_udf(create_temporal_udf(name));
    }

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
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "keys() requires 1 argument".to_string(),
            ));
        }

        let arg = &json_args[0];
        let keys = match arg {
            serde_json::Value::Object(map) => {
                let mut key_strings: Vec<String> = map
                    .iter()
                    .filter(|(k, v)| !v.is_null() && !k.starts_with('_'))
                    .map(|(k, _)| k.clone())
                    .collect();
                key_strings.sort();
                key_strings
                    .into_iter()
                    .map(|k| ScalarValue::Utf8(Some(k)))
                    .collect::<Vec<_>>()
            }
            serde_json::Value::Null => {
                return Ok(ColumnarValue::Scalar(ScalarValue::List(
                    ScalarValue::new_list_nullable(&[], &DataType::Utf8),
                )));
            }
            _ => {
                // Not a map/object, return empty list or error?
                // Cypher: keys(non-map) returns empty list or errors depending on type.
                vec![]
            }
        };

        let list = ScalarValue::List(ScalarValue::new_list(&keys, &DataType::Utf8, true));
        Ok(ColumnarValue::Scalar(list))
    }
}

// ============================================================================
// index(container, index) -> Any (JSONB)
// ============================================================================

pub fn create_index_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(IndexUdf::new())
}

#[derive(Debug)]
struct IndexUdf {
    signature: Signature,
}

impl IndexUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(IndexUdf);

impl ScalarUDFImpl for IndexUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "index"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Return LargeBinary (JSONB) so downstream result conversion can decode it.
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "index() requires 2 arguments".to_string(),
            ));
        }

        let container = &json_args[0];
        let index = &json_args[1];

        let result = match container {
            serde_json::Value::Array(arr) => {
                if let Some(i) = index.as_i64() {
                    let idx = if i < 0 {
                        let pos = arr.len() as i64 + i;
                        if pos < 0 { -1 } else { pos }
                    } else {
                        i
                    };
                    if idx >= 0 && (idx as usize) < arr.len() {
                        arr[idx as usize].clone()
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::Object(map) => {
                if let Some(key) = index.as_str() {
                    map.get(key).cloned().unwrap_or(serde_json::Value::Null)
                } else if !index.is_null() {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "InvalidArgumentValue: Map index must be a string".to_string(),
                    ));
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::Null => serde_json::Value::Null,
            _ => serde_json::Value::Null,
        };

        // Serialize to JSONB (LargeBinary) so result conversion can decode it.
        // We use standard JSON string here, record_batches_to_rows handles it.
        let json_str = serde_json::to_string(&result).unwrap_or_else(|_| "null".to_string());
        Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
            json_str.into_bytes(),
        ))))
    }
}

// ============================================================================
// labels(node) -> List<String>
// ============================================================================

pub fn create_labels_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(LabelsUdf::new())
}

#[derive(Debug)]
struct LabelsUdf {
    signature: Signature,
}

impl LabelsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(LabelsUdf);

impl ScalarUDFImpl for LabelsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "labels"
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
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "labels() requires 1 argument".to_string(),
            ));
        }

        let node = &json_args[0];
        if let serde_json::Value::Object(map) = node
            && let Some(serde_json::Value::Array(arr)) = map.get("_labels")
        {
            let scalars: Vec<ScalarValue> = arr
                .iter()
                .map(|v| ScalarValue::Utf8(v.as_str().map(|s| s.to_string())))
                .collect();
            let list = ScalarValue::List(ScalarValue::new_list(&scalars, &DataType::Utf8, true));
            return Ok(ColumnarValue::Scalar(list));
        }

        let empty_list = ScalarValue::List(ScalarValue::new_list_nullable(&[], &DataType::Utf8));
        Ok(ColumnarValue::Scalar(empty_list))
    }
}

// ============================================================================
// nodes(path) -> List<Node>
// ============================================================================

pub fn create_nodes_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(NodesUdf::new())
}

#[derive(Debug)]
struct NodesUdf {
    signature: Signature,
}

impl NodesUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(NodesUdf);

impl ScalarUDFImpl for NodesUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nodes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "nodes() requires 1 argument".to_string(),
            ));
        }

        let path = &json_args[0];
        let nodes = match path {
            serde_json::Value::Object(map) => {
                map.get("nodes").cloned().unwrap_or(serde_json::Value::Null)
            }
            _ => serde_json::Value::Null,
        };

        let json_str = serde_json::to_string(&nodes).unwrap_or_else(|_| "null".to_string());
        Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
            json_str.into_bytes(),
        ))))
    }
}

// ============================================================================
// relationships(path) -> List<Relationship>
// ============================================================================

pub fn create_relationships_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RelationshipsUdf::new())
}

#[derive(Debug)]
struct RelationshipsUdf {
    signature: Signature,
}

impl RelationshipsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(RelationshipsUdf);

impl ScalarUDFImpl for RelationshipsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "relationships"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "relationships() requires 1 argument".to_string(),
            ));
        }

        let path = &json_args[0];
        let rels = match path {
            serde_json::Value::Object(map) => map
                .get("relationships")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        };

        let json_str = serde_json::to_string(&rels).unwrap_or_else(|_| "null".to_string());
        Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(
            json_str.into_bytes(),
        ))))
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

/// Invoke a binary bitwise operation on two Int64 arguments.
///
/// Consolidates the matching logic for all binary bitwise UDFs.
fn invoke_binary_bitwise_op<F>(
    args: &ScalarFunctionArgs,
    name: &str,
    op: F,
) -> DFResult<ColumnarValue>
where
    F: Fn(i64, i64) -> i64,
{
    use arrow_array::Int64Array;
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError;

    if args.args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "{} requires exactly 2 arguments",
            name
        )));
    }

    let left = &args.args[0];
    let right = &args.args[1];

    match (left, right) {
        (
            ColumnarValue::Scalar(ScalarValue::Int64(Some(l))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(r))),
        ) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(op(*l, *r))))),
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
                    (Some(l), Some(r)) => Some(op(l, r)),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Mixed scalar/array not supported for {}",
            name
        ))),
    }
}

/// Invoke a unary bitwise operation on a single Int64 argument.
///
/// Consolidates the matching logic for unary bitwise UDFs.
fn invoke_unary_bitwise_op<F>(
    args: &ScalarFunctionArgs,
    name: &str,
    op: F,
) -> DFResult<ColumnarValue>
where
    F: Fn(i64) -> i64,
{
    use arrow_array::Int64Array;
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError;

    if args.args.len() != 1 {
        return Err(DataFusionError::Execution(format!(
            "{} requires exactly 1 argument",
            name
        )));
    }

    let operand = &args.args[0];

    match operand {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(op(*v)))))
        }
        ColumnarValue::Array(arr) => {
            let arr = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Execution("Array must be Int64".to_string()))?;

            let result: Int64Array = arr.iter().map(|v| v.map(&op)).collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "Invalid argument type for {}",
            name
        ))),
    }
}

/// Macro to define a binary bitwise UDF with minimal boilerplate.
///
/// Takes the struct name, UDF name string, and the bitwise operation as a closure.
macro_rules! define_binary_bitwise_udf {
    ($struct_name:ident, $udf_name:literal, $op:expr) => {
        #[derive(Debug)]
        struct $struct_name {
            signature: Signature,
        }

        impl $struct_name {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(
                        vec![DataType::Int64, DataType::Int64],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl_udf_eq_hash!($struct_name);

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Int64)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                invoke_binary_bitwise_op(&args, $udf_name, $op)
            }
        }
    };
}

/// Macro to define a unary bitwise UDF with minimal boilerplate.
///
/// Takes the struct name, UDF name string, and the bitwise operation as a closure.
macro_rules! define_unary_bitwise_udf {
    ($struct_name:ident, $udf_name:literal, $op:expr) => {
        #[derive(Debug)]
        struct $struct_name {
            signature: Signature,
        }

        impl $struct_name {
            fn new() -> Self {
                Self {
                    signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
                }
            }
        }

        impl_udf_eq_hash!($struct_name);

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(DataType::Int64)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                invoke_unary_bitwise_op(&args, $udf_name, $op)
            }
        }
    };
}

// Define all binary bitwise UDFs using the macro
define_binary_bitwise_udf!(BitwiseOrUdf, "uni.bitwise.or", |l, r| l | r);
define_binary_bitwise_udf!(BitwiseAndUdf, "uni.bitwise.and", |l, r| l & r);
define_binary_bitwise_udf!(BitwiseXorUdf, "uni.bitwise.xor", |l, r| l ^ r);
define_binary_bitwise_udf!(ShiftLeftUdf, "uni.bitwise.shiftLeft", |l, r| l << r);
define_binary_bitwise_udf!(ShiftRightUdf, "uni.bitwise.shiftRight", |l, r| l >> r);

// Define the unary bitwise NOT UDF using the macro
define_unary_bitwise_udf!(BitwiseNotUdf, "uni.bitwise.not", |v| !v);

/// Create the `uni.bitwise.or` UDF for bitwise OR operations.
pub fn create_bitwise_or_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseOrUdf::new())
}

/// Create the `uni.bitwise.and` UDF for bitwise AND operations.
pub fn create_bitwise_and_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseAndUdf::new())
}

/// Create the `uni.bitwise.xor` UDF for bitwise XOR operations.
pub fn create_bitwise_xor_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseXorUdf::new())
}

/// Create the `uni.bitwise.not` UDF for bitwise NOT operations.
pub fn create_bitwise_not_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(BitwiseNotUdf::new())
}

/// Create the `uni.bitwise.shiftLeft` UDF for left shift operations.
pub fn create_shift_left_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ShiftLeftUdf::new())
}

/// Create the `uni.bitwise.shiftRight` UDF for right shift operations.
pub fn create_shift_right_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ShiftRightUdf::new())
}

// ============================================================================
// Temporal UDFs — delegate to eval_datetime_function in datetime.rs
// ============================================================================

/// Create a temporal UDF that delegates to `eval_datetime_function`.
///
/// Accepts variadic Utf8 arguments and returns Utf8 (or Int64 for extraction
/// functions like year/month/day). Internally converts Arrow scalars to
/// `serde_json::Value`, calls the datetime module, and converts back.
fn create_temporal_udf(name: &str) -> ScalarUDF {
    ScalarUDF::new_from_impl(TemporalUdf::new(name.to_string()))
}

#[derive(Debug)]
struct TemporalUdf {
    name: String,
    signature: Signature,
}

impl TemporalUdf {
    fn new(name: String) -> Self {
        Self {
            name,
            // Accept zero or more args of any type — the datetime module validates.
            // OneOf is required because VariadicAny alone rejects zero-arg calls.
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![]),
                    TypeSignature::VariadicAny,
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(TemporalUdf);

impl ScalarUDFImpl for TemporalUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Temporal functions return Utf8 (ISO strings) or Int64 (extraction).
        // DataFusion doesn't dispatch on return type, so Utf8 is safe here;
        // the actual value written to the result column will be cast downstream.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        let func_name = self.name.to_uppercase();

        let result = crate::query::datetime::eval_datetime_function(&func_name, &json_args)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("{}(): {}", self.name, e))
            })?;

        json_value_to_columnar(&result)
    }
}

/// Create a UDF for accessing duration component properties.
///
/// Called as `_duration_property(duration_string, component_name)`.
/// Returns an Int64 value for the requested component.
fn create_duration_property_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(DurationPropertyUdf::new())
}

#[derive(Debug)]
struct DurationPropertyUdf {
    signature: Signature,
}

impl DurationPropertyUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(DurationPropertyUdf);

impl ScalarUDFImpl for DurationPropertyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_duration_property"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_duration_property requires 2 arguments (duration_string, component)".to_string(),
            ));
        }

        let dur_str = extract_scalar_utf8(&args.args[0])?;
        let component = extract_scalar_utf8(&args.args[1])?;

        let result =
            crate::query::datetime::eval_duration_accessor(&dur_str, &component).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "_duration_property(): {}",
                    e
                ))
            })?;

        // Duration accessors return integers.
        match result {
            serde_json::Value::Number(n) => {
                let i = n.as_i64().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Duration accessor returned non-integer".to_string(),
                    )
                })?;
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(i))))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Duration accessor returned unexpected type".to_string(),
            )),
        }
    }
}

/// Convert DataFusion `ColumnarValue` arguments to `serde_json::Value` for the datetime module.
fn columnar_args_to_json(args: &[ColumnarValue]) -> DFResult<Vec<serde_json::Value>> {
    args.iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(scalar) => scalar_to_json(scalar),
            ColumnarValue::Array(arr) if arr.len() == 1 => {
                // Single-row array — extract the scalar.
                let scalar = ScalarValue::try_from_array(arr, 0).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Cannot extract scalar from array: {e}"
                    ))
                })?;
                scalar_to_json(&scalar)
            }
            ColumnarValue::Array(_) => Err(datafusion::error::DataFusionError::Execution(
                "Temporal UDFs do not support batched array execution yet".to_string(),
            )),
        })
        .collect()
}

/// Convert a single `ScalarValue` to `serde_json::Value`.
fn scalar_to_json(scalar: &ScalarValue) -> DFResult<serde_json::Value> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            // Try to parse as JSON (for arguments serialized as JSON in UNWIND or Map literals).
            if (s.starts_with('{') || s.starts_with('[') || s.starts_with('"'))
                && let Ok(obj) = serde_json::from_str::<serde_json::Value>(s)
            {
                return Ok(obj);
            }
            Ok(serde_json::Value::String(s.clone()))
        }
        ScalarValue::Int64(Some(i)) => Ok(serde_json::json!(*i)),
        ScalarValue::Int32(Some(i)) => Ok(serde_json::json!(*i as i64)),
        ScalarValue::Float64(Some(f)) => Ok(serde_json::json!(*f)),
        ScalarValue::Boolean(Some(b)) => Ok(serde_json::json!(*b)),
        ScalarValue::Struct(arr) => {
            if arr.len() == 0 || arr.is_null(0) {
                Ok(serde_json::Value::Null)
            } else {
                Ok(uni_store::storage::arrow_convert::arrow_to_value(
                    arr.as_ref(),
                    0,
                ))
            }
        }
        ScalarValue::List(arr) => {
            if arr.len() == 0 || arr.is_null(0) {
                Ok(serde_json::Value::Null)
            } else {
                Ok(uni_store::storage::arrow_convert::arrow_to_value(
                    arr.as_ref(),
                    0,
                ))
            }
        }
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Float64(None) => Ok(serde_json::Value::Null),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unsupported scalar type for temporal function: {other:?}"
        ))),
    }
}

/// Convert a `serde_json::Value` result back to `ColumnarValue`.
fn json_value_to_columnar(val: &serde_json::Value) -> DFResult<ColumnarValue> {
    match val {
        serde_json::Value::String(s) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.clone()))))
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(i))))
            } else if let Some(f) = n.as_f64() {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(f))))
            } else {
                Err(datafusion::error::DataFusionError::Execution(
                    "Temporal function returned unsupported number type".to_string(),
                ))
            }
        }
        serde_json::Value::Bool(b) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(*b)))),
        serde_json::Value::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "Temporal function returned unsupported type: {other:?}"
        ))),
    }
}

/// Extract a scalar Utf8 string from a `ColumnarValue`.
fn extract_scalar_utf8(val: &ColumnarValue) -> DFResult<String> {
    match val {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(s.clone()),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Expected Utf8 scalar".to_string(),
        )),
    }
}

// ============================================================================
// toInteger(x) -> Int64
// ============================================================================

pub fn create_to_integer_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToIntegerUdf::new())
}

#[derive(Debug)]
struct ToIntegerUdf {
    signature: Signature,
}

impl ToIntegerUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(ToIntegerUdf);

impl ScalarUDFImpl for ToIntegerUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "toInteger"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "toInteger() requires 1 argument".to_string(),
            ));
        }

        let val = &json_args[0];
        let result = match val {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    serde_json::Value::Number(i.into())
                } else if let Some(f) = n.as_f64() {
                    serde_json::Value::Number((f as i64).into())
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::String(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    serde_json::Value::Number(i.into())
                } else if let Ok(f) = s.parse::<f64>() {
                    serde_json::Value::Number((f as i64).into())
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::Null => serde_json::Value::Null,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "InvalidArgumentValue: toInteger() cannot convert type".to_string(),
                ));
            }
        };

        json_value_to_columnar(&result)
    }
}

// ============================================================================
// toFloat(x) -> Float64
// ============================================================================

pub fn create_to_float_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToFloatUdf::new())
}

#[derive(Debug)]
struct ToFloatUdf {
    signature: Signature,
}

impl ToFloatUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(ToFloatUdf);

impl ScalarUDFImpl for ToFloatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "toFloat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "toFloat() requires 1 argument".to_string(),
            ));
        }

        let val = &json_args[0];
        let result = match val {
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap())
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::String(s) => {
                if let Ok(f) = s.parse::<f64>() {
                    serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap())
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::Null => serde_json::Value::Null,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "InvalidArgumentValue: toFloat() cannot convert type".to_string(),
                ));
            }
        };

        json_value_to_columnar(&result)
    }
}

// ============================================================================
// toBoolean(x) -> Boolean
// ============================================================================

pub fn create_to_boolean_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToBooleanUdf::new())
}

#[derive(Debug)]
struct ToBooleanUdf {
    signature: Signature,
}

impl ToBooleanUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(ToBooleanUdf);

impl ScalarUDFImpl for ToBooleanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "toBoolean"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let json_args = columnar_args_to_json(&args.args)?;
        if json_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "toBoolean() requires 1 argument".to_string(),
            ));
        }

        let val = &json_args[0];
        let result = match val {
            serde_json::Value::Bool(b) => serde_json::Value::Bool(*b),
            serde_json::Value::String(s) => {
                let s_lower = s.to_lowercase();
                if s_lower == "true" {
                    serde_json::Value::Bool(true)
                } else if s_lower == "false" {
                    serde_json::Value::Bool(false)
                } else {
                    serde_json::Value::Null
                }
            }
            serde_json::Value::Null => serde_json::Value::Null,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "InvalidArgumentValue: toBoolean() cannot convert type".to_string(),
                ));
            }
        };

        json_value_to_columnar(&result)
    }
}

// JSON UDFs - Now using Lance's built-in implementations
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
