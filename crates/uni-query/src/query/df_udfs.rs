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

use arrow::array::ArrayRef;
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
use uni_common::Value;
use uni_cypher::ast::BinaryOp;
use uni_store::storage::arrow_convert::values_to_array;

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
/// String functions (left, right, substring, split) use DataFusion's built-ins.
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
    ctx.register_udf(create_type_rank_udf());
    ctx.register_udf(create_has_null_udf());

    // String matching UDFs (used by CypherStringMatchExpr in expr_compiler)
    ctx.register_udf(create_cypher_starts_with_udf());
    ctx.register_udf(create_cypher_ends_with_udf());
    ctx.register_udf(create_cypher_contains_udf());

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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "keys() requires 1 argument".to_string(),
                ));
            }

            let arg = &val_args[0];
            let keys = match arg {
                Value::Map(map) => {
                    let mut key_strings: Vec<String> = map
                        .iter()
                        .filter(|(k, v)| !v.is_null() && !k.starts_with('_'))
                        .map(|(k, _)| k.clone())
                        .collect();
                    key_strings.sort();
                    key_strings
                        .into_iter()
                        .map(Value::String)
                        .collect::<Vec<_>>()
                }
                Value::Null => {
                    return Ok(Value::Null);
                }
                _ => {
                    // Not a map/object, return empty list or error?
                    // Cypher: keys(non-map) returns empty list or errors depending on type.
                    vec![]
                }
            };

            Ok(Value::List(keys))
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "index() requires 2 arguments".to_string(),
                ));
            }

            let container = &val_args[0];
            let index = &val_args[1];

            let result = match container {
                Value::List(arr) => {
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
                            Value::Null
                        }
                    } else {
                        Value::Null
                    }
                }
                Value::Map(map) => {
                    if let Some(key) = index.as_str() {
                        map.get(key).cloned().unwrap_or(Value::Null)
                    } else if !index.is_null() {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "InvalidArgumentValue: Map index must be a string".to_string(),
                        ));
                    } else {
                        Value::Null
                    }
                }
                Value::Node(node) => {
                    if let Some(key) = index.as_str() {
                        node.properties.get(key).cloned().unwrap_or(Value::Null)
                    } else if !index.is_null() {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "InvalidArgumentValue: Node index must be a string".to_string(),
                        ));
                    } else {
                        Value::Null
                    }
                }
                Value::Edge(edge) => {
                    if let Some(key) = index.as_str() {
                        edge.properties.get(key).cloned().unwrap_or(Value::Null)
                    } else if !index.is_null() {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "InvalidArgumentValue: Edge index must be a string".to_string(),
                        ));
                    } else {
                        Value::Null
                    }
                }
                Value::Null => Value::Null,
                _ => Value::Null,
            };

            Ok(result)
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "labels() requires 1 argument".to_string(),
                ));
            }

            let node = &val_args[0];
            if let Value::Map(map) = node
                && let Some(Value::List(arr)) = map.get("_labels")
            {
                return Ok(Value::List(arr.clone()));
            }

            Ok(Value::Null)
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "nodes() requires 1 argument".to_string(),
                ));
            }

            let path = &val_args[0];
            let nodes = match path {
                Value::Map(map) => map.get("nodes").cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };

            Ok(nodes)
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "relationships() requires 1 argument".to_string(),
                ));
            }

            let path = &val_args[0];
            let rels = match path {
                Value::Map(map) => {
                    map.get("relationships").cloned().unwrap_or(Value::Null)
                }
                _ => Value::Null,
            };

            Ok(rels)
        })
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

        // range() handles its own array extraction for now as it's a bit special
        // but we only support Scalar arguments for range() in practice for now.

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
/// `uni_common::Value`, calls the datetime module (which still uses
/// `serde_json::Value` internally), and converts back.
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
        let name = self.name.to_lowercase();
        // Extraction functions return Int64
        if matches!(
            name.as_str(),
            "year"
                | "month"
                | "day"
                | "hour"
                | "minute"
                | "second"
                | "duration.inmonths"
                | "duration.indays"
                | "duration.inseconds"
        ) {
            Ok(DataType::Int64)
        } else {
            Ok(DataType::Utf8)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let func_name = self.name.to_uppercase();
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            crate::query::datetime::eval_datetime_function(&func_name, val_args).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("{}(): {}", self.name, e))
            })
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_duration_property requires 2 arguments (duration_string, component)"
                        .to_string(),
                ));
            }

            let dur_str = match &val_args[0] {
                Value::String(s) => s,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Duration must be string".to_string(),
                    ));
                }
            };
            let component = match &val_args[1] {
                Value::String(s) => s,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Component must be string".to_string(),
                    ));
                }
            };

            crate::query::datetime::eval_duration_accessor(dur_str, component).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("_duration_property(): {}", e))
            })
        })
    }
}

/// Convert DataFusion `ColumnarValue` arguments to `uni_common::Value` for UDF evaluation.
fn get_value_args_for_row(args: &[ColumnarValue], row: usize) -> DFResult<Vec<Value>> {
    args.iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(scalar) => scalar_to_value(scalar),
            ColumnarValue::Array(arr) => {
                let scalar = ScalarValue::try_from_array(arr, row).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Cannot extract scalar from array at row {}: {}",
                        row, e
                    ))
                })?;
                scalar_to_value(&scalar)
            }
        })
        .collect()
}

/// Generic implementation for simple Cypher UDFs that process `uni_common::Value` arguments.
fn invoke_cypher_udf<F>(
    args: ScalarFunctionArgs,
    output_type: &DataType,
    f: F,
) -> DFResult<ColumnarValue>
where
    F: Fn(&[Value]) -> DFResult<Value>,
{
    let len = args
        .args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(arr) => Some(arr.len()),
            _ => None,
        })
        .unwrap_or(1);

    if len == 1
        && args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)))
    {
        let row_args = get_value_args_for_row(&args.args, 0)?;
        let res = f(&row_args)?;
        return value_to_columnar(&res);
    }

    let mut results = Vec::with_capacity(len);
    for i in 0..len {
        let row_args = get_value_args_for_row(&args.args, i)?;
        results.push(f(&row_args)?);
    }

    let arr = values_to_array(&results, output_type)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    Ok(ColumnarValue::Array(arr))
}

/// Convert a single `ScalarValue` to `uni_common::Value`.
fn scalar_to_value(scalar: &ScalarValue) -> DFResult<Value> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            // Try to parse as JSON ONLY if it looks like a JSON object, array or quoted string.
            // This avoids misinterpreting unquoted strings that happen to be numbers/bools.
            if (s.starts_with('{') || s.starts_with('[') || s.starts_with('"'))
                && let Ok(obj) = serde_json::from_str::<serde_json::Value>(s)
            {
                return Ok(Value::from(obj));
            }
            Ok(Value::String(s.clone()))
        }
        ScalarValue::LargeBinary(Some(b)) => {
            // LargeBinary may contain JSONB binary (from typed Json properties) or
            // plain JSON text bytes (from UNWIND or schemaless scans).
            // Try JSONB first (the common case for typed properties), then plain JSON.
            let raw = jsonb::RawJsonb::new(b);
            let jsonb_str = raw.to_string();
            if (jsonb_str != "null" || b.is_empty())
                && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&jsonb_str)
            {
                return Ok(Value::from(parsed));
            }
            // Fallback: try plain JSON text
            if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(b) {
                Ok(Value::from(obj))
            } else {
                Ok(Value::Null)
            }
        }
        ScalarValue::Int64(Some(i)) => Ok(Value::Int(*i)),
        ScalarValue::Int32(Some(i)) => Ok(Value::Int(*i as i64)),
        ScalarValue::Float64(Some(f)) => {
            // NaN and Infinity are natively supported by uni_common::Value::Float
            Ok(Value::Float(*f))
        }
        ScalarValue::Boolean(Some(b)) => Ok(Value::Bool(*b)),
        ScalarValue::Struct(arr) => {
            if arr.len() == 0 || arr.is_null(0) {
                Ok(Value::Null)
            } else {
                Ok(uni_store::storage::arrow_convert::arrow_to_value(arr.as_ref(), 0))
            }
        }
        ScalarValue::List(arr) => {
            if arr.len() == 0 || arr.is_null(0) {
                Ok(Value::Null)
            } else {
                Ok(uni_store::storage::arrow_convert::arrow_to_value(arr.as_ref(), 0))
            }
        }
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Float64(None) => Ok(Value::Null),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unsupported scalar type for UDF: {other:?}"
        ))),
    }
}

/// Convert a `uni_common::Value` result back to `ColumnarValue`.
fn value_to_columnar(val: &Value) -> DFResult<ColumnarValue> {
    match val {
        Value::String(s) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.clone())))),
        Value::Int(i) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(*i)))),
        Value::Float(f) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(*f)))),
        Value::Bool(b) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(*b)))),
        Value::Null => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "UDF returned unsupported type: {other:?}"
        ))),
    }
}

// ============================================================================
// _has_null(list) -> Boolean
// Internal UDF to check if a list contains any nulls
// ============================================================================

pub fn create_has_null_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(HasNullUdf::new())
}

#[derive(Debug)]
struct HasNullUdf {
    signature: Signature,
}

impl HasNullUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(HasNullUdf);

impl ScalarUDFImpl for HasNullUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_has_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_has_null requires 1 argument".to_string(),
            ));
        }

        let arg = &args.args[0];

        match arg {
            ColumnarValue::Scalar(scalar) => {
                match scalar {
                    ScalarValue::List(arr) => {
                        // Try downcasting to ListArray (GenericListArray<i32>)
                        if let Some(list_arr) =
                            arr.as_any().downcast_ref::<arrow::array::ListArray>()
                        {
                            if list_arr.is_empty() {
                                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))
                            } else {
                                let values = list_arr.value(0);
                                let null_count = values.null_count();
                                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                                    null_count > 0,
                                ))))
                            }
                        } else {
                            // Fallback: check array itself
                            let null_count = arr.null_count();
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                                null_count > 0,
                            ))))
                        }
                    }
                    ScalarValue::LargeList(arr) => {
                        if arr.len() == 0 {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))
                        } else {
                            let values = arr.value(0);
                            let null_count = values.null_count();
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                                null_count > 0,
                            ))))
                        }
                    }
                    ScalarValue::FixedSizeList(arr) => {
                        if arr.len() == 0 {
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))
                        } else {
                            let values = arr.value(0);
                            let null_count = values.null_count();
                            Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                                null_count > 0,
                            ))))
                        }
                    }
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false)))),
                }
            }
            ColumnarValue::Array(arr) => {
                use arrow_array::{LargeListArray, ListArray};

                if let Some(list_arr) = arr.as_any().downcast_ref::<ListArray>() {
                    let mut builder = arrow::array::BooleanBuilder::with_capacity(list_arr.len());
                    for i in 0..list_arr.len() {
                        if list_arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let value_slice = list_arr.value(i);
                            builder.append_value(value_slice.null_count() > 0);
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
                } else if let Some(large_list_arr) = arr.as_any().downcast_ref::<LargeListArray>() {
                    let mut builder =
                        arrow::array::BooleanBuilder::with_capacity(large_list_arr.len());
                    for i in 0..large_list_arr.len() {
                        if large_list_arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let value_slice = large_list_arr.value(i);
                            builder.append_value(value_slice.null_count() > 0);
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
                } else {
                    // Not a list array?
                    Err(datafusion::error::DataFusionError::Execution(
                        "_has_null requires List array".to_string(),
                    ))
                }
            }
        }
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "toInteger() requires 1 argument".to_string(),
                ));
            }

            let val = &val_args[0];
            let result = match val {
                Value::Int(i) => Value::Int(*i),
                Value::Float(f) => Value::Int(*f as i64),
                Value::String(s) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Value::Int(i)
                    } else if let Ok(f) = s.parse::<f64>() {
                        Value::Int(f as i64)
                    } else {
                        Value::Null
                    }
                }
                Value::Null => Value::Null,
                _ => {
                    // Cypher: return null if cannot convert
                    Value::Null
                }
            };
            Ok(result)
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "toFloat() requires 1 argument".to_string(),
                ));
            }

            let val = &val_args[0];
            let result = match val {
                Value::Int(i) => Value::Float(*i as f64),
                Value::Float(f) => Value::Float(*f),
                Value::String(s) => {
                    if let Ok(f) = s.parse::<f64>() {
                        Value::Float(f)
                    } else {
                        Value::Null
                    }
                }
                Value::Null => Value::Null,
                _ => {
                    // Cypher: return null if cannot convert
                    Value::Null
                }
            };
            Ok(result)
        })
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
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "toBoolean() requires 1 argument".to_string(),
                ));
            }

            let val = &val_args[0];
            let result = match val {
                Value::Bool(b) => Value::Bool(*b),
                Value::String(s) => {
                    let s_lower = s.to_lowercase();
                    if s_lower == "true" {
                        Value::Bool(true)
                    } else if s_lower == "false" {
                        Value::Bool(false)
                    } else {
                        Value::Null
                    }
                }
                Value::Null => Value::Null,
                _ => {
                    // Cypher: return null if cannot convert
                    Value::Null
                }
            };
            Ok(result)
        })
    }
}

// ============================================================================
// _cypher_type_rank(x) -> Int32
// Internal UDF for Cypher ORDER BY type ranking
// ============================================================================

pub fn create_type_rank_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TypeRankUdf::new())
}

#[derive(Debug)]
struct TypeRankUdf {
    signature: Signature,
}

impl TypeRankUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(TypeRankUdf);

impl ScalarUDFImpl for TypeRankUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_type_rank"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_type_rank requires 1 argument".to_string(),
            ));
        }

        let arg = &args.args[0];
        // println!("DEBUG: TypeRankUdf arg type: {:?}", arg.data_type());
        match arg {
            ColumnarValue::Scalar(s) => {
                let rank = get_type_rank_scalar(s);
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(rank))))
            }
            ColumnarValue::Array(arr) => {
                let ranks: arrow::array::Int32Array = (0..arr.len())
                    .map(|i| {
                        let scalar =
                            ScalarValue::try_from_array(arr, i).unwrap_or(ScalarValue::Null);
                        get_type_rank_scalar(&scalar)
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(ranks)))
            }
        }
    }
}

fn get_type_rank_scalar(val: &ScalarValue) -> i32 {
    if val.is_null() {
        return 9;
    }

    // println!("DEBUG: Rank {:?} -> {}", val, rank);
    match val {
        ScalarValue::Null => 9,
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Float16(_)
        | ScalarValue::Float32(_)
        | ScalarValue::Float64(_) => 1,

        ScalarValue::Boolean(_) => 2,

        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            // Try to infer type from string content to fix sorting of coerced values
            if s.parse::<f64>().is_ok() {
                1 // Number
            } else if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") {
                2 // Bool
            } else {
                3 // String
            }
        }
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) => 9,

        ScalarValue::List(_) | ScalarValue::LargeList(_) | ScalarValue::FixedSizeList(_) => 4,

        ScalarValue::Struct(arr) => {
            let fields = arr.fields();
            let field_names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();

            if field_names.contains(&"_vid") {
                7 // Node
            } else if field_names.contains(&"_eid") {
                6 // Rel
            } else if field_names.contains(&"nodes") && field_names.contains(&"relationships") {
                5 // Path
            } else {
                8 // Map
            }
        }

        ScalarValue::Dictionary(_, val) => get_type_rank_scalar(val),

        _ => 0,
    }
}

// ============================================================================
// String Matching UDFs (_cypher_starts_with, etc.)
// ============================================================================

pub fn invoke_cypher_string_op<F>(
    args: &ScalarFunctionArgs,
    name: &str,
    op: F,
) -> DFResult<ColumnarValue>
where
    F: Fn(&str, &str) -> bool,
{
    use arrow_array::{BooleanArray, LargeStringArray, StringArray};
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
        (ColumnarValue::Scalar(l_scalar), ColumnarValue::Scalar(r_scalar)) => {
            let l_str = match l_scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => None, // Null
                _ => None, // Non-string -> Null
            };
            let r_str = match r_scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => None,
                _ => None,
            };

            match (l_str, r_str) {
                (Some(l), Some(r)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(op(l, r)))))
                }
                _ => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
            }
        }
        (ColumnarValue::Array(l_arr), ColumnarValue::Scalar(r_scalar)) => {
            // Check right scalar first
            let r_val = match r_scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
                _ => None,
            };

            if r_val.is_none() {
                // If rhs is null or non-string, result is all null
                let nulls = arrow_array::new_null_array(&DataType::Boolean, l_arr.len());
                return Ok(ColumnarValue::Array(nulls));
            }
            let pattern = r_val.unwrap();

            // Handle left array
            let result_array = if let Some(arr) = l_arr.as_any().downcast_ref::<StringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(s, pattern)))
                    .collect::<BooleanArray>()
            } else if let Some(arr) = l_arr.as_any().downcast_ref::<LargeStringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(s, pattern)))
                    .collect::<BooleanArray>()
            } else {
                // Left array is not string -> return nulls
                arrow_array::new_null_array(&DataType::Boolean, l_arr.len())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone()
            };

            Ok(ColumnarValue::Array(Arc::new(result_array)))
        }
        (ColumnarValue::Scalar(l_scalar), ColumnarValue::Array(r_arr)) => {
            // Check left scalar first
            let l_val = match l_scalar {
                ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
                _ => None,
            };

            if l_val.is_none() {
                let nulls = arrow_array::new_null_array(&DataType::Boolean, r_arr.len());
                return Ok(ColumnarValue::Array(nulls));
            }
            let target = l_val.unwrap();

            let result_array = if let Some(arr) = r_arr.as_any().downcast_ref::<StringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(target, s)))
                    .collect::<BooleanArray>()
            } else if let Some(arr) = r_arr.as_any().downcast_ref::<LargeStringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(target, s)))
                    .collect::<BooleanArray>()
            } else {
                // Right array is not string -> return nulls
                arrow_array::new_null_array(&DataType::Boolean, r_arr.len())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone()
            };

            Ok(ColumnarValue::Array(Arc::new(result_array)))
        }
        (ColumnarValue::Array(l_arr), ColumnarValue::Array(r_arr)) => {
            // Both arrays.
            if l_arr.len() != r_arr.len() {
                return Err(DataFusionError::Execution(
                    "Array lengths must match".to_string(),
                ));
            }

            // We need to iterate both. Since we can't easily zip different types (String vs LargeString vs Other),
            // we handle common cases or fallback to row-by-row scalar extraction (slow but general)
            // Or just support StringArray/LargeStringArray combinations.

            // Helper to get iterator of Option<&str>
            // This is a bit tricky due to types.
            // Let's implement for StringArray vs StringArray for now, and Large vs Large.
            // Cross types (String vs Large) requires more boilerplate.
            // Fallback: use scalar extraction for simplicity if types differ?

            // For now, simplify: if either is not string array, return nulls.
            let l_is_string =
                l_arr.data_type() == &DataType::Utf8 || l_arr.data_type() == &DataType::LargeUtf8;
            let r_is_string =
                r_arr.data_type() == &DataType::Utf8 || r_arr.data_type() == &DataType::LargeUtf8;

            if !l_is_string || !r_is_string {
                let nulls = arrow_array::new_null_array(&DataType::Boolean, l_arr.len());
                return Ok(ColumnarValue::Array(nulls));
            }

            // Use arrow_cast to normalize to LargeUtf8?
            let l_cast = datafusion::arrow::compute::cast(l_arr, &DataType::LargeUtf8)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let r_cast = datafusion::arrow::compute::cast(r_arr, &DataType::LargeUtf8)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

            let l_strings = l_cast.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let r_strings = r_cast.as_any().downcast_ref::<LargeStringArray>().unwrap();

            let result: BooleanArray = l_strings
                .iter()
                .zip(r_strings.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(l_str), Some(r_str)) => Some(op(l_str, r_str)),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

macro_rules! define_string_op_udf {
    ($struct_name:ident, $udf_name:literal, $op:expr) => {
        #[derive(Debug)]
        struct $struct_name {
            signature: Signature,
        }

        impl $struct_name {
            fn new() -> Self {
                Self {
                    // Accepts any types, handles type checking at runtime
                    signature: Signature::any(2, Volatility::Immutable),
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
                Ok(DataType::Boolean)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
                invoke_cypher_string_op(&args, $udf_name, $op)
            }
        }
    };
}

define_string_op_udf!(CypherStartsWithUdf, "_cypher_starts_with", |s, p| s
    .starts_with(p));
define_string_op_udf!(CypherEndsWithUdf, "_cypher_ends_with", |s, p| s
    .ends_with(p));
define_string_op_udf!(CypherContainsUdf, "_cypher_contains", |s, p| s.contains(p));

pub fn create_cypher_starts_with_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherStartsWithUdf::new())
}
pub fn create_cypher_ends_with_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherEndsWithUdf::new())
}
pub fn create_cypher_contains_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherContainsUdf::new())
}

pub fn create_cypher_equal_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherCompareUdf::new("_cypher_equal", BinaryOp::Eq))
}
pub fn create_cypher_not_equal_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherCompareUdf::new("_cypher_not_equal", BinaryOp::NotEq))
}
pub fn create_cypher_lt_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherCompareUdf::new("_cypher_lt", BinaryOp::Lt))
}
pub fn create_cypher_lt_eq_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherCompareUdf::new("_cypher_lt_eq", BinaryOp::LtEq))
}
pub fn create_cypher_gt_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherCompareUdf::new("_cypher_gt", BinaryOp::Gt))
}
pub fn create_cypher_gt_eq_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherCompareUdf::new("_cypher_gt_eq", BinaryOp::GtEq))
}

#[derive(Debug)]
struct CypherCompareUdf {
    name: String,
    op: BinaryOp,
    signature: Signature,
}

impl CypherCompareUdf {
    fn new(name: &str, op: BinaryOp) -> Self {
        Self {
            name: name.to_string(),
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for CypherCompareUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for CypherCompareUdf {}

impl std::hash::Hash for CypherCompareUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for CypherCompareUdf {
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
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = DataType::Boolean;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "comparison UDF requires 2 arguments".to_string(),
                ));
            }
            crate::query::expr_eval::eval_binary_op(&val_args[0], &self.op, &val_args[1])
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }
}

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
    fn test_has_null_udf() {
        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::config::ConfigOptions;
        use datafusion::scalar::ScalarValue;
        use std::sync::Arc;

        let udf = create_has_null_udf();

        // Test [1, 2, null] (Int64)
        let values = vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(None),
        ];

        // Construct list manually
        let list_scalar = ScalarValue::List(ScalarValue::new_list(&values, &DataType::Int64, true));

        let list_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(list_scalar)],
            arg_fields: vec![list_field],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf.invoke_with_args(args).unwrap();

        if let ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) = result {
            assert!(b, "has_null should return true for list with null");
        } else {
            panic!("Unexpected result: {:?}", result);
        }
    }
}
