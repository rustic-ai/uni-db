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
use chrono::Offset;
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, UInt64Array,
};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use uni_common::Value;
use uni_cypher::ast::BinaryOp;
use uni_store::storage::arrow_convert::values_to_array;

use super::expr_eval::cypher_eq;

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
    ctx.register_udf(create_properties_udf());
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
    ctx.register_udf(create_cypher_size_udf());

    // String matching UDFs (used by CypherStringMatchExpr in expr_compiler)
    ctx.register_udf(create_cypher_starts_with_udf());
    ctx.register_udf(create_cypher_ends_with_udf());
    ctx.register_udf(create_cypher_contains_udf());

    // List comparison UDF for lexicographic ordering
    ctx.register_udf(create_cypher_list_compare_udf());

    // Boolean XOR UDF (3-valued logic with null propagation)
    ctx.register_udf(create_cypher_xor_udf());

    // CypherValue-aware comparison UDFs (decode LargeBinary values before comparing)
    ctx.register_udf(create_cypher_equal_udf());
    ctx.register_udf(create_cypher_not_equal_udf());
    ctx.register_udf(create_cypher_gt_udf());
    ctx.register_udf(create_cypher_gt_eq_udf());
    ctx.register_udf(create_cypher_lt_udf());
    ctx.register_udf(create_cypher_lt_eq_udf());

    // CypherValue to bool UDF (for boolean context: WHERE, CASE WHEN)
    ctx.register_udf(create_cv_to_bool_udf());

    // CypherValue arithmetic UDFs
    ctx.register_udf(create_cypher_add_udf());
    ctx.register_udf(create_cypher_sub_udf());
    ctx.register_udf(create_cypher_mul_udf());
    ctx.register_udf(create_cypher_div_udf());
    ctx.register_udf(create_cypher_mod_udf());

    // Map projection UDF
    ctx.register_udf(create_map_project_udf());

    // List assembly UDF (heterogeneous args → CypherValue array)
    ctx.register_udf(create_make_cypher_list_udf());

    // Cypher IN UDF (handles json-encoded and CypherValue list types)
    ctx.register_udf(create_cypher_in_udf());

    // List concatenation, append, slice, tail, and reverse UDFs
    ctx.register_udf(create_cypher_list_concat_udf());
    ctx.register_udf(create_cypher_list_append_udf());
    ctx.register_udf(create_cypher_list_slice_udf());
    ctx.register_udf(create_cypher_tail_udf());
    ctx.register_udf(create_cypher_reverse_udf());

    // Temporal extraction UDFs (year, month, day, etc.)
    for name in &["year", "month", "day", "hour", "minute", "second"] {
        ctx.register_udf(create_temporal_udf(name));
    }

    // CypherValue-to-Float64 conversion UDF (for sum/avg on LargeBinary columns)
    ctx.register_udf(create_cypher_to_float64_udf());

    // Cypher-aware aggregate UDAFs
    ctx.register_udaf(create_cypher_min_udaf());
    ctx.register_udaf(create_cypher_max_udaf());
    ctx.register_udaf(create_cypher_sum_udaf());
    ctx.register_udaf(create_cypher_collect_udaf());

    // Cypher percentileDisc/percentileCont UDAFs
    ctx.register_udaf(create_cypher_percentile_disc_udaf());
    ctx.register_udaf(create_cypher_percentile_cont_udaf());

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
                "id(): requires 1 argument".to_string(),
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
            // Accept any type: Utf8 for normal edge columns, LargeBinary for
            // CypherValue-encoded values (e.g. from heterogeneous list comprehensions),
            // and Null for null propagation.
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
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
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "type(): requires 1 argument".to_string(),
            ));
        }
        let output_type = DataType::Utf8;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "type(): requires 1 argument".to_string(),
                ));
            }
            let val = &val_args[0];
            match val {
                // Edge represented as a map (from CypherValue encoding)
                Value::Map(map) => {
                    if let Some(Value::String(t)) = map.get("_type") {
                        Ok(Value::String(t.clone()))
                    } else {
                        // Map without _type key is not a relationship
                        Err(datafusion::error::DataFusionError::Execution(
                            "TypeError: InvalidArgumentValue - type() requires a relationship argument".to_string(),
                        ))
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Err(datafusion::error::DataFusionError::Execution(
                    "TypeError: InvalidArgumentValue - type() requires a relationship argument"
                        .to_string(),
                )),
            }
        })
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
                    "keys(): requires 1 argument".to_string(),
                ));
            }

            let arg = &val_args[0];
            let keys = match arg {
                Value::Map(map) => {
                    // For schemaless entities, properties are stored in the
                    // _all_props CypherValue blob.  If the map contains an _all_props
                    // sub-map, extract property names from it instead of from
                    // the top-level map (which only has system fields).
                    let source = match map.get("_all_props") {
                        Some(Value::Map(all)) => all,
                        _ => map,
                    };
                    let mut key_strings: Vec<String> = source
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
// properties(entity) -> Map (all user-visible properties as a map)
// ============================================================================

pub fn create_properties_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(PropertiesUdf::new())
}

#[derive(Debug)]
struct PropertiesUdf {
    signature: Signature,
}

impl PropertiesUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(PropertiesUdf);

impl ScalarUDFImpl for PropertiesUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "properties"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        // Return as LargeBinary (CypherValue-encoded map)
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "properties(): requires 1 argument".to_string(),
                ));
            }

            let arg = &val_args[0];
            match arg {
                Value::Map(map) => {
                    // For schemaless entities, properties are in _all_props.
                    let source = match map.get("_all_props") {
                        Some(Value::Map(all)) => all,
                        _ => map,
                    };
                    // Filter out internal properties (those starting with '_')
                    let filtered: std::collections::HashMap<String, Value> = source
                        .iter()
                        .filter(|(k, _)| !k.starts_with('_'))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    Ok(Value::Map(filtered))
                }
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Null),
            }
        })
    }
}

// ============================================================================
// index(container, index) -> Any (CypherValue)
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
        // Return LargeBinary (CypherValue) so downstream result conversion can decode it.
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "index(): requires 2 arguments".to_string(),
                ));
            }

            let container = &val_args[0];
            let index = &val_args[1];

            // Strict integer-only index extraction — no coercion from string/float.
            // Integers from UNWIND now arrive as Value::Int via native Int64 columns
            // or CypherValue LargeBinary encoding.
            let index_as_int = index.as_i64();

            let result = match container {
                Value::List(arr) => {
                    if let Some(i) = index_as_int {
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
                    } else if index.is_null() {
                        Value::Null
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "TypeError: InvalidArgumentType - list index must be an integer, got: {:?}",
                            index
                        )));
                    }
                }
                Value::Map(map) => {
                    if let Some(key) = index.as_str() {
                        // Check top-level first
                        if let Some(val) = map.get(key) {
                            val.clone()
                        } else if let Some(Value::Map(props)) = map.get("properties") {
                            // Serialized Node/Edge: properties are nested under "properties"
                            props.get(key).cloned().unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        }
                    } else if !index.is_null() {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "index(): map index must be a string".to_string(),
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
                            "index(): node index must be a string".to_string(),
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
                            "index(): edge index must be a string".to_string(),
                        ));
                    } else {
                        Value::Null
                    }
                }
                Value::Null => Value::Null,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "TypeError: InvalidArgumentType - cannot index into {:?}",
                        container
                    )));
                }
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
                    "labels(): requires 1 argument".to_string(),
                ));
            }

            let node = &val_args[0];
            match node {
                Value::Map(map) => {
                    if let Some(Value::List(arr)) = map.get("_labels") {
                        Ok(Value::List(arr.clone()))
                    } else {
                        // Map without _labels key is not a node
                        Err(datafusion::error::DataFusionError::Execution(
                            "TypeError: InvalidArgumentValue - labels() requires a node argument"
                                .to_string(),
                        ))
                    }
                }
                Value::Null => Ok(Value::Null),
                _ => Err(datafusion::error::DataFusionError::Execution(
                    "TypeError: InvalidArgumentValue - labels() requires a node argument"
                        .to_string(),
                )),
            }
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
                    "nodes(): requires 1 argument".to_string(),
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
                    "relationships(): requires 1 argument".to_string(),
                ));
            }

            let path = &val_args[0];
            let rels = match path {
                Value::Map(map) => map.get("relationships").cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };

            Ok(rels)
        })
    }
}

// ============================================================================
// range(start, end, [step]) -> List<Int64>
// ============================================================================

/// Extract an i64 from a ColumnarValue, coercing from any integer type.
/// Rejects floats, booleans, strings, lists, and maps with `InvalidArgumentType`.
fn extract_i64_range_arg(arg: &ColumnarValue, name: &str) -> DFResult<i64> {
    match arg {
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Int8(Some(v)) => Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => Ok(*v as i64),
            ScalarValue::Int32(Some(v)) => Ok(*v as i64),
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => Ok(*v as i64),
            _ => Err(datafusion::error::DataFusionError::Execution(format!(
                "ArgumentError: InvalidArgumentType - range() {} must be an integer", name
            ))),
        },
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "ArgumentError: InvalidArgumentType - range() {} must be an integer", name
        ))),
    }
}

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
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
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
                "range(): requires 2 or 3 arguments".to_string(),
            ));
        }

        // range() handles its own array extraction for now as it's a bit special
        // but we only support Scalar arguments for range() in practice for now.

        // Extract scalar values with flexible integer coercion
        let start = extract_i64_range_arg(&args.args[0], "start")?;
        let end = extract_i64_range_arg(&args.args[1], "end")?;
        let step = if args.args.len() == 3 {
            extract_i64_range_arg(&args.args[2], "step")?
        } else {
            1
        };

        if step == 0 {
            return Err(datafusion::error::DataFusionError::Execution(
                "range(): step cannot be zero".to_string(),
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
            "{}(): requires exactly 2 arguments",
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
                DataFusionError::Execution(format!("{}(): left array must be Int64", name))
            })?;
            let r_arr = r_arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution(format!("{}(): right array must be Int64", name))
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
            "{}(): mixed scalar/array not supported",
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
            "{}(): requires exactly 1 argument",
            name
        )));
    }

    let operand = &args.args[0];

    match operand {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(op(*v)))))
        }
        ColumnarValue::Array(arr) => {
            let arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution(format!("{}(): array must be Int64", name))
            })?;

            let result: Int64Array = arr.iter().map(|v| v.map(&op)).collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => Err(DataFusionError::Execution(format!(
            "{}(): invalid argument type",
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
            match name.as_str() {
                // Temporal constructors use LargeBinary (CypherValue codec) to preserve
                // timezone names, Duration components, and nanosecond precision through
                // the DataFusion pipeline. Constant-folded calls bypass UDFs entirely.
                "datetime" | "localdatetime" | "date" | "time" | "localtime" | "duration"
                | "date.truncate" | "time.truncate" | "datetime.truncate"
                | "localdatetime.truncate" | "localtime.truncate"
                | "duration.between"
                | "datetime.fromepoch" | "datetime.fromepochmillis"
                | "datetime.transaction" | "datetime.statement" | "datetime.realtime"
                | "date.transaction" | "date.statement" | "date.realtime"
                | "time.transaction" | "time.statement" | "time.realtime"
                | "localtime.transaction" | "localtime.statement" | "localtime.realtime"
                | "localdatetime.transaction" | "localdatetime.statement"
                | "localdatetime.realtime" => Ok(DataType::LargeBinary),
                _ => Ok(DataType::Utf8),
            }
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
                    "_duration_property(): requires 2 arguments (duration_string, component)"
                        .to_string(),
                ));
            }

            let dur_str = match &val_args[0] {
                Value::String(s) => s,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "_duration_property(): duration must be a string".to_string(),
                    ));
                }
            };
            let component = match &val_args[1] {
                Value::String(s) => s,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "_duration_property(): component must be a string".to_string(),
                    ));
                }
            };

            crate::query::datetime::eval_duration_accessor(dur_str, component).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "_duration_property(): {}",
                    e
                ))
            })
        })
    }
}

/// Downcast an `ArrayRef` to a concrete Arrow array type, returning a
/// `DataFusionError::Execution` on failure.
macro_rules! downcast_arr {
    ($arr:expr, $array_type:ty) => {
        $arr.as_any().downcast_ref::<$array_type>().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to downcast to {}",
                stringify!($array_type)
            ))
        })?
    };
}

/// Convert a string slice to `Value`, attempting JSON parse for object/array/quoted-string prefixes.
fn string_to_value(s: &str) -> Value {
    if (s.starts_with('{') || s.starts_with('[') || s.starts_with('"'))
        && let Ok(obj) = serde_json::from_str::<serde_json::Value>(s)
    {
        return Value::from(obj);
    }
    Value::String(s.to_string())
}

/// Extract a `uni_common::Value` directly from an Arrow array at a given row.
///
/// This bypasses the `ScalarValue` intermediate allocation for common types,
/// significantly reducing overhead in UDF execution. Falls back to the
/// `ScalarValue::try_from_array` -> `scalar_to_value` path for complex types.
fn get_value_from_array(arr: &ArrayRef, row: usize) -> DFResult<Value> {
    if arr.is_null(row) {
        return Ok(Value::Null);
    }

    match arr.data_type() {
        DataType::LargeBinary => {
            let typed = downcast_arr!(arr, LargeBinaryArray);
            let bytes = typed.value(row);
            if let Ok(val) = uni_common::cypher_value_codec::decode(bytes) {
                return Ok(val);
            }
            // Fallback: try plain JSON text for UNWIND or legacy data
            Ok(serde_json::from_slice::<serde_json::Value>(bytes)
                .map(Value::from)
                .unwrap_or(Value::Null))
        }
        DataType::Int64 => Ok(Value::Int(downcast_arr!(arr, Int64Array).value(row))),
        DataType::Float64 => Ok(Value::Float(downcast_arr!(arr, Float64Array).value(row))),
        DataType::Utf8 => Ok(string_to_value(downcast_arr!(arr, StringArray).value(row))),
        DataType::LargeUtf8 => Ok(string_to_value(
            downcast_arr!(arr, LargeStringArray).value(row),
        )),
        DataType::Boolean => Ok(Value::Bool(downcast_arr!(arr, BooleanArray).value(row))),
        DataType::UInt64 => Ok(Value::Int(downcast_arr!(arr, UInt64Array).value(row) as i64)),
        DataType::Int32 => Ok(Value::Int(downcast_arr!(arr, Int32Array).value(row) as i64)),
        DataType::Float32 => Ok(Value::Float(
            downcast_arr!(arr, Float32Array).value(row) as f64
        )),
        // Fallback: use existing ScalarValue path for Struct, List, FixedSizeList,
        // Timestamp, Date32, and other complex types
        _ => {
            let scalar = ScalarValue::try_from_array(arr, row).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Cannot extract scalar from array at row {}: {}",
                    row, e
                ))
            })?;
            scalar_to_value(&scalar)
        }
    }
}

/// Convert DataFusion `ColumnarValue` arguments to `uni_common::Value` for UDF evaluation.
fn get_value_args_for_row(args: &[ColumnarValue], row: usize) -> DFResult<Vec<Value>> {
    args.iter()
        .map(|arg| match arg {
            ColumnarValue::Scalar(scalar) => scalar_to_value(scalar),
            ColumnarValue::Array(arr) => get_value_from_array(arr, row),
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
        if matches!(output_type, DataType::LargeBinary) {
            // Encode through array path to match UDF's declared LargeBinary return type
            let arr = values_to_array(&[res], output_type)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?));
        }
        // For null results, return a typed null matching the UDF's declared return type
        if res.is_null() {
            let typed_null = ScalarValue::try_from(output_type).unwrap_or(ScalarValue::Utf8(None));
            return Ok(ColumnarValue::Scalar(typed_null));
        }
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

/// Convert a scalar Arrow array (from Struct/List/LargeList/FixedSizeList) to `Value`.
/// Returns `Null` if the array is empty or the first element is null.
fn scalar_arr_to_value(arr: &dyn arrow::array::Array) -> DFResult<Value> {
    if arr.is_empty() || arr.is_null(0) {
        Ok(Value::Null)
    } else {
        Ok(uni_store::storage::arrow_convert::arrow_to_value(arr, 0))
    }
}

/// Resolve timezone offset from a timezone name at a given UTC nanosecond instant.
fn resolve_timezone_offset(tz_name: &str, nanos_utc: i64) -> i32 {
    if tz_name == "UTC" || tz_name == "Z" {
        return 0;
    }
    if let Ok(tz) = tz_name.parse::<chrono_tz::Tz>() {
        let dt = chrono::DateTime::from_timestamp_nanos(nanos_utc).with_timezone(&tz);
        dt.offset().fix().local_minus_utc()
    } else {
        0
    }
}

/// Convert a single `ScalarValue` to `uni_common::Value`.
pub(crate) fn scalar_to_value(scalar: &ScalarValue) -> DFResult<Value> {
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
            // LargeBinary contains CypherValue (MessagePack-tagged) binary encoding.
            // Try CypherValue decode first, then fall back to plain JSON text for legacy data.
            if let Ok(val) = uni_common::cypher_value_codec::decode(b) {
                return Ok(val);
            }
            // Fallback: try plain JSON text for UNWIND or legacy data
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
        ScalarValue::Struct(arr) => scalar_arr_to_value(arr.as_ref()),
        ScalarValue::List(arr) => scalar_arr_to_value(arr.as_ref()),
        ScalarValue::LargeList(arr) => scalar_arr_to_value(arr.as_ref()),
        ScalarValue::FixedSizeList(arr) => scalar_arr_to_value(arr.as_ref()),
        // Unsigned and smaller integer types
        ScalarValue::UInt64(Some(u)) => Ok(Value::Int(*u as i64)),
        ScalarValue::UInt32(Some(u)) => Ok(Value::Int(*u as i64)),
        ScalarValue::UInt16(Some(u)) => Ok(Value::Int(*u as i64)),
        ScalarValue::UInt8(Some(u)) => Ok(Value::Int(*u as i64)),
        ScalarValue::Int16(Some(i)) => Ok(Value::Int(*i as i64)),
        ScalarValue::Int8(Some(i)) => Ok(Value::Int(*i as i64)),

        // Temporal types — convert to Value::Temporal
        ScalarValue::Date32(Some(days)) => {
            Ok(Value::Temporal(uni_common::TemporalValue::Date {
                days_since_epoch: *days,
            }))
        }
        ScalarValue::Date64(Some(millis)) => {
            let days = (*millis / 86_400_000) as i32;
            Ok(Value::Temporal(uni_common::TemporalValue::Date {
                days_since_epoch: days,
            }))
        }
        ScalarValue::TimestampNanosecond(Some(nanos), tz) => {
            if let Some(tz_str) = tz {
                let offset = resolve_timezone_offset(tz_str.as_ref(), *nanos);
                let tz_name = if tz_str.as_ref() == "UTC" { None } else { Some(tz_str.to_string()) };
                Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                    nanos_since_epoch: *nanos,
                    offset_seconds: offset,
                    timezone_name: tz_name,
                }))
            } else {
                Ok(Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                    nanos_since_epoch: *nanos,
                }))
            }
        }
        ScalarValue::TimestampMicrosecond(Some(micros), tz) => {
            let nanos = *micros * 1_000;
            if let Some(tz_str) = tz {
                let offset = resolve_timezone_offset(tz_str.as_ref(), nanos);
                let tz_name = if tz_str.as_ref() == "UTC" { None } else { Some(tz_str.to_string()) };
                Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                    nanos_since_epoch: nanos,
                    offset_seconds: offset,
                    timezone_name: tz_name,
                }))
            } else {
                Ok(Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                    nanos_since_epoch: nanos,
                }))
            }
        }
        ScalarValue::TimestampMillisecond(Some(millis), tz) => {
            let nanos = *millis * 1_000_000;
            if let Some(tz_str) = tz {
                let offset = resolve_timezone_offset(tz_str.as_ref(), nanos);
                let tz_name = if tz_str.as_ref() == "UTC" { None } else { Some(tz_str.to_string()) };
                Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                    nanos_since_epoch: nanos,
                    offset_seconds: offset,
                    timezone_name: tz_name,
                }))
            } else {
                Ok(Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                    nanos_since_epoch: nanos,
                }))
            }
        }
        ScalarValue::TimestampSecond(Some(secs), tz) => {
            let nanos = *secs * 1_000_000_000;
            if let Some(tz_str) = tz {
                let offset = resolve_timezone_offset(tz_str.as_ref(), nanos);
                let tz_name = if tz_str.as_ref() == "UTC" { None } else { Some(tz_str.to_string()) };
                Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                    nanos_since_epoch: nanos,
                    offset_seconds: offset,
                    timezone_name: tz_name,
                }))
            } else {
                Ok(Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                    nanos_since_epoch: nanos,
                }))
            }
        }
        ScalarValue::Time64Nanosecond(Some(nanos)) => {
            Ok(Value::Temporal(uni_common::TemporalValue::LocalTime {
                nanos_since_midnight: *nanos,
            }))
        }
        ScalarValue::Time64Microsecond(Some(micros)) => {
            Ok(Value::Temporal(uni_common::TemporalValue::LocalTime {
                nanos_since_midnight: *micros * 1_000,
            }))
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            Ok(Value::Temporal(uni_common::TemporalValue::Duration {
                months: v.months as i64,
                days: v.days as i64,
                nanos: v.nanoseconds,
            }))
        }
        ScalarValue::DurationMicrosecond(Some(micros)) => {
            let dur = crate::query::datetime::CypherDuration::from_micros(*micros);
            Ok(Value::Temporal(uni_common::TemporalValue::Duration {
                months: dur.months,
                days: dur.days,
                nanos: dur.nanos,
            }))
        }
        ScalarValue::DurationMillisecond(Some(millis)) => {
            let dur = crate::query::datetime::CypherDuration::from_micros(*millis * 1_000);
            Ok(Value::Temporal(uni_common::TemporalValue::Duration {
                months: dur.months,
                days: dur.days,
                nanos: dur.nanos,
            }))
        }
        ScalarValue::DurationSecond(Some(secs)) => {
            let dur = crate::query::datetime::CypherDuration::from_micros(*secs * 1_000_000);
            Ok(Value::Temporal(uni_common::TemporalValue::Duration {
                months: dur.months,
                days: dur.days,
                nanos: dur.nanos,
            }))
        }
        ScalarValue::DurationNanosecond(Some(nanos)) => {
            Ok(Value::Temporal(uni_common::TemporalValue::Duration {
                months: 0,
                days: 0,
                nanos: *nanos,
            }))
        }
        ScalarValue::Float32(Some(f)) => Ok(Value::Float(*f as f64)),

        // All None variants for the above types
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int8(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Boolean(None)
        | ScalarValue::Date32(None)
        | ScalarValue::Date64(None)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::TimestampSecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::Time64Microsecond(None)
        | ScalarValue::Time64Nanosecond(None)
        | ScalarValue::DurationMicrosecond(None)
        | ScalarValue::DurationMillisecond(None)
        | ScalarValue::DurationSecond(None)
        | ScalarValue::DurationNanosecond(None)
        | ScalarValue::IntervalMonthDayNano(None) => Ok(Value::Null),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "scalar_to_value(): unsupported scalar type {other:?}"
        ))),
    }
}

/// Convert a `uni_common::Value` result back to `ColumnarValue`.
fn value_to_columnar(val: &Value) -> DFResult<ColumnarValue> {
    let scalar = match val {
        Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Value::Int(i) => ScalarValue::Int64(Some(*i)),
        Value::Float(f) => ScalarValue::Float64(Some(*f)),
        Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        Value::Null => ScalarValue::Utf8(None),
        Value::Temporal(tv) => {
            use uni_common::TemporalValue;
            match tv {
                TemporalValue::Date { days_since_epoch } => ScalarValue::Date32(Some(*days_since_epoch)),
                TemporalValue::LocalTime { nanos_since_midnight } => ScalarValue::Time64Nanosecond(Some(*nanos_since_midnight)),
                TemporalValue::Time { nanos_since_midnight, .. } => ScalarValue::Time64Nanosecond(Some(*nanos_since_midnight)),
                TemporalValue::LocalDateTime { nanos_since_epoch } => ScalarValue::TimestampNanosecond(Some(*nanos_since_epoch), None),
                TemporalValue::DateTime { nanos_since_epoch, timezone_name, .. } => {
                    let tz = timezone_name.as_deref().unwrap_or("UTC");
                    ScalarValue::TimestampNanosecond(Some(*nanos_since_epoch), Some(tz.into()))
                }
                TemporalValue::Duration { months, days, nanos } => {
                    ScalarValue::IntervalMonthDayNano(Some(
                        arrow::datatypes::IntervalMonthDayNano {
                            months: *months as i32,
                            days: *days as i32,
                            nanoseconds: *nanos,
                        }
                    ))
                }
            }
        }
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "value_to_columnar(): unsupported type {other:?}"
            )));
        }
    };
    Ok(ColumnarValue::Scalar(scalar))
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
                "_has_null(): requires 1 argument".to_string(),
            ));
        }

        /// Check if a list array element at index has any nulls
        fn check_list_nulls<T: arrow_array::OffsetSizeTrait>(
            arr: &arrow_array::GenericListArray<T>,
            idx: usize,
        ) -> bool {
            if arr.is_null(idx) || arr.is_empty() {
                false
            } else {
                arr.value(idx).null_count() > 0
            }
        }

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                let has_null = match scalar {
                    ScalarValue::List(arr) => arr
                        .as_any()
                        .downcast_ref::<arrow::array::ListArray>()
                        .map(|a| !a.is_empty() && a.value(0).null_count() > 0)
                        .unwrap_or(arr.null_count() > 0),
                    ScalarValue::LargeList(arr) => arr.len() > 0 && arr.value(0).null_count() > 0,
                    ScalarValue::FixedSizeList(arr) => {
                        arr.len() > 0 && arr.value(0).null_count() > 0
                    }
                    _ => false,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(has_null))))
            }
            ColumnarValue::Array(arr) => {
                use arrow_array::{LargeListArray, ListArray};

                let results: arrow::array::BooleanArray =
                    if let Some(list_arr) = arr.as_any().downcast_ref::<ListArray>() {
                        (0..list_arr.len())
                            .map(|i| {
                                if list_arr.is_null(i) {
                                    None
                                } else {
                                    Some(check_list_nulls(list_arr, i))
                                }
                            })
                            .collect()
                    } else if let Some(large) = arr.as_any().downcast_ref::<LargeListArray>() {
                        (0..large.len())
                            .map(|i| {
                                if large.is_null(i) {
                                    None
                                } else {
                                    Some(check_list_nulls(large, i))
                                }
                            })
                            .collect()
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "_has_null(): requires list array".to_string(),
                        ));
                    };
                Ok(ColumnarValue::Array(Arc::new(results)))
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
        "tointeger"
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
                    "tointeger(): requires 1 argument".to_string(),
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
        "tofloat"
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
                    "tofloat(): requires 1 argument".to_string(),
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
        "toboolean"
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
                    "toboolean(): requires 1 argument".to_string(),
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
                Value::Int(i) => Value::Bool(*i != 0),
                Value::Float(_) => Value::Null,
                Value::List(_) | Value::Map(_) => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "InvalidArgumentValue: toboolean(): cannot convert {:?} to boolean",
                        val
                    )));
                }
                _ => Value::Null,
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
                "_cypher_type_rank(): requires 1 argument".to_string(),
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
    use arrow_array::{BooleanArray, LargeBinaryArray, LargeStringArray, StringArray};
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError;

    if args.args.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "{}(): requires exactly 2 arguments",
            name
        )));
    }

    let left = &args.args[0];
    let right = &args.args[1];

    // Helper to extract string from scalar (including CypherValue-encoded)
    let extract_string = |scalar: &ScalarValue| -> Option<String> {
        match scalar {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
            ScalarValue::LargeBinary(Some(bytes)) => {
                // Decode CypherValue and extract string if present
                match uni_common::cypher_value_codec::decode(bytes) {
                    Ok(uni_common::Value::String(s)) => Some(s),
                    _ => None,
                }
            }
            ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::LargeBinary(None)
            | ScalarValue::Null => None,
            _ => None,
        }
    };

    match (left, right) {
        (ColumnarValue::Scalar(l_scalar), ColumnarValue::Scalar(r_scalar)) => {
            let l_str = extract_string(l_scalar);
            let r_str = extract_string(r_scalar);

            match (l_str, r_str) {
                (Some(l), Some(r)) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(op(
                    &l, &r,
                ))))),
                _ => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
            }
        }
        (ColumnarValue::Array(l_arr), ColumnarValue::Scalar(r_scalar)) => {
            // Check right scalar first (extract string, including from CypherValue)
            let r_val = extract_string(r_scalar);

            if r_val.is_none() {
                // If rhs is null or non-string, result is all null
                let nulls = arrow_array::new_null_array(&DataType::Boolean, l_arr.len());
                return Ok(ColumnarValue::Array(nulls));
            }
            let pattern = r_val.unwrap();

            // Handle left array
            let result_array = if let Some(arr) = l_arr.as_any().downcast_ref::<StringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(s, &pattern)))
                    .collect::<BooleanArray>()
            } else if let Some(arr) = l_arr.as_any().downcast_ref::<LargeStringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(s, &pattern)))
                    .collect::<BooleanArray>()
            } else if let Some(arr) = l_arr.as_any().downcast_ref::<LargeBinaryArray>() {
                // CypherValue-encoded array - decode each element
                arr.iter()
                    .map(|opt_bytes| {
                        opt_bytes.and_then(|bytes| {
                            match uni_common::cypher_value_codec::decode(bytes) {
                                Ok(uni_common::Value::String(s)) => Some(op(&s, &pattern)),
                                _ => None,
                            }
                        })
                    })
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
            // Check left scalar first (extract string, including from CypherValue)
            let l_val = extract_string(l_scalar);

            if l_val.is_none() {
                let nulls = arrow_array::new_null_array(&DataType::Boolean, r_arr.len());
                return Ok(ColumnarValue::Array(nulls));
            }
            let target = l_val.unwrap();

            let result_array = if let Some(arr) = r_arr.as_any().downcast_ref::<StringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(&target, s)))
                    .collect::<BooleanArray>()
            } else if let Some(arr) = r_arr.as_any().downcast_ref::<LargeStringArray>() {
                arr.iter()
                    .map(|opt_s| opt_s.map(|s| op(&target, s)))
                    .collect::<BooleanArray>()
            } else if let Some(arr) = r_arr.as_any().downcast_ref::<LargeBinaryArray>() {
                // CypherValue-encoded array - decode each element
                arr.iter()
                    .map(|opt_bytes| {
                        opt_bytes.and_then(|bytes| {
                            match uni_common::cypher_value_codec::decode(bytes) {
                                Ok(uni_common::Value::String(s)) => Some(op(&target, &s)),
                                _ => None,
                            }
                        })
                    })
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
                return Err(DataFusionError::Execution(format!(
                    "{}(): array lengths must match",
                    name
                )));
            }

            // Helper to extract string from each row (handles Utf8, LargeUtf8, and LargeBinary/CypherValue)
            let extract_string_at = |arr: &dyn Array, idx: usize| -> Option<String> {
                if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                    str_arr.value(idx).to_string().into()
                } else if let Some(str_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
                    str_arr.value(idx).to_string().into()
                } else if let Some(bin_arr) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
                    if bin_arr.is_null(idx) {
                        return None;
                    }
                    let bytes = bin_arr.value(idx);
                    match uni_common::cypher_value_codec::decode(bytes) {
                        Ok(uni_common::Value::String(s)) => Some(s),
                        _ => None,
                    }
                } else {
                    None
                }
            };

            let result: BooleanArray = (0..l_arr.len())
                .map(|idx| {
                    match (
                        extract_string_at(l_arr.as_ref(), idx),
                        extract_string_at(r_arr.as_ref(), idx),
                    ) {
                        (Some(l_str), Some(r_str)) => Some(op(&l_str, &r_str)),
                        _ => None,
                    }
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

/// Apply a comparison operator to an `Ordering` result.
#[allow(clippy::match_like_matches_macro)]
fn apply_comparison_op(ord: std::cmp::Ordering, op: &BinaryOp) -> bool {
    use std::cmp::Ordering;
    match (ord, op) {
        (Ordering::Less, BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::NotEq) => true,
        (Ordering::Equal, BinaryOp::Eq | BinaryOp::LtEq | BinaryOp::GtEq) => true,
        (Ordering::Greater, BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::NotEq) => true,
        _ => false,
    }
}

/// Compare two f64 values with NaN awareness and Cypher comparison semantics.
/// Returns `None` when partial_cmp fails (should not happen for non-NaN floats).
fn compare_f64(lhs: f64, rhs: f64, op: &BinaryOp) -> Option<bool> {
    if lhs.is_nan() || rhs.is_nan() {
        Some(matches!(op, BinaryOp::NotEq))
    } else {
        Some(apply_comparison_op(lhs.partial_cmp(&rhs)?, op))
    }
}

/// Decode CypherValue bytes as f64 (works for both TAG_INT and TAG_FLOAT).
fn cv_bytes_as_f64(bytes: &[u8]) -> Option<f64> {
    use uni_common::cypher_value_codec::{TAG_FLOAT, TAG_INT, decode_float, decode_int, peek_tag};
    match peek_tag(bytes)? {
        TAG_INT => decode_int(bytes).map(|i| i as f64),
        TAG_FLOAT => decode_float(bytes),
        _ => None,
    }
}

/// Compare CypherValue bytes against an f64, returning the boolean comparison result.
/// Returns `None` for null/incomparable types (caller should emit null).
fn compare_cv_numeric(bytes: &[u8], rhs: f64, op: &BinaryOp) -> Option<bool> {
    use uni_common::cypher_value_codec::{TAG_INT, TAG_NULL, decode_int, peek_tag};
    // Special case: int-vs-int comparison preserves exact integer semantics
    if peek_tag(bytes) == Some(TAG_INT)
        && let Some(lhs_int) = decode_int(bytes)
        // If rhs is exactly representable as i64, use integer comparison
        && rhs.fract() == 0.0
        && rhs >= i64::MIN as f64
        && rhs <= i64::MAX as f64
    {
        return Some(apply_comparison_op(lhs_int.cmp(&(rhs as i64)), op));
    }
    if peek_tag(bytes) == Some(TAG_NULL) {
        return None;
    }
    let lhs = cv_bytes_as_f64(bytes)?;
    compare_f64(lhs, rhs, op)
}

/// Fast-path comparison for LargeBinary (CypherValue) vs native Arrow types.
///
/// Returns `Some(ColumnarValue)` if fast path succeeded, `None` to fallback to slow path.
fn try_fast_compare(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    op: &BinaryOp,
) -> Option<ColumnarValue> {
    use arrow_array::builder::BooleanBuilder;
    use uni_common::cypher_value_codec::{
        TAG_INT, TAG_NULL, TAG_STRING, decode_int, decode_string, peek_tag,
    };

    let (lhs_arr, rhs_arr) = match (lhs, rhs) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (l, r),
        _ => return None,
    };

    // All fast paths require LHS to be LargeBinary
    if !matches!(lhs_arr.data_type(), DataType::LargeBinary) {
        return None;
    }

    let lb_arr = lhs_arr.as_any().downcast_ref::<LargeBinaryArray>()?;

    match rhs_arr.data_type() {
        // LargeBinary vs Int64
        DataType::Int64 => {
            let int_arr = rhs_arr.as_any().downcast_ref::<Int64Array>()?;
            let mut builder = BooleanBuilder::with_capacity(lb_arr.len());
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || int_arr.is_null(i) {
                    builder.append_null();
                } else {
                    match compare_cv_numeric(lb_arr.value(i), int_arr.value(i) as f64, op) {
                        Some(result) => builder.append_value(result),
                        None => builder.append_null(),
                    }
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        // LargeBinary vs Float64
        DataType::Float64 => {
            let float_arr = rhs_arr.as_any().downcast_ref::<Float64Array>()?;
            let mut builder = BooleanBuilder::with_capacity(lb_arr.len());
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || float_arr.is_null(i) {
                    builder.append_null();
                } else {
                    match compare_cv_numeric(lb_arr.value(i), float_arr.value(i), op) {
                        Some(result) => builder.append_value(result),
                        None => builder.append_null(),
                    }
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        // LargeBinary vs String (Utf8 or LargeUtf8)
        DataType::Utf8 | DataType::LargeUtf8 => {
            let mut builder = BooleanBuilder::with_capacity(lb_arr.len());
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || rhs_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let bytes = lb_arr.value(i);
                    let rhs_str = if matches!(rhs_arr.data_type(), DataType::Utf8) {
                        rhs_arr.as_any().downcast_ref::<StringArray>()?.value(i)
                    } else {
                        rhs_arr
                            .as_any()
                            .downcast_ref::<LargeStringArray>()?
                            .value(i)
                    };
                    match peek_tag(bytes) {
                        Some(TAG_STRING) => {
                            if let Some(lhs_str) = decode_string(bytes) {
                                builder.append_value(apply_comparison_op(
                                    lhs_str.as_str().cmp(rhs_str),
                                    op,
                                ));
                            } else {
                                builder.append_null();
                            }
                        }
                        _ => builder.append_null(),
                    }
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        // LargeBinary vs LargeBinary
        DataType::LargeBinary => {
            let rhs_lb = rhs_arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            let mut builder = BooleanBuilder::with_capacity(lb_arr.len());
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || rhs_lb.is_null(i) {
                    builder.append_null();
                } else {
                    let lhs_bytes = lb_arr.value(i);
                    let rhs_bytes = rhs_lb.value(i);
                    let lhs_tag = peek_tag(lhs_bytes);
                    let rhs_tag = peek_tag(rhs_bytes);

                    // Null propagation
                    if lhs_tag == Some(TAG_NULL) || rhs_tag == Some(TAG_NULL) {
                        builder.append_null();
                        continue;
                    }

                    // Int vs Int: exact integer comparison
                    if lhs_tag == Some(TAG_INT) && rhs_tag == Some(TAG_INT) {
                        if let (Some(l), Some(r)) = (decode_int(lhs_bytes), decode_int(rhs_bytes)) {
                            builder.append_value(apply_comparison_op(l.cmp(&r), op));
                        } else {
                            builder.append_null();
                        }
                        continue;
                    }

                    // String vs String
                    if lhs_tag == Some(TAG_STRING) && rhs_tag == Some(TAG_STRING) {
                        if let (Some(l), Some(r)) =
                            (decode_string(lhs_bytes), decode_string(rhs_bytes))
                        {
                            builder.append_value(apply_comparison_op(l.cmp(&r), op));
                        } else {
                            builder.append_null();
                        }
                        continue;
                    }

                    // Numeric (mixed int/float): promote both to f64
                    if let (Some(l), Some(r)) =
                        (cv_bytes_as_f64(lhs_bytes), cv_bytes_as_f64(rhs_bytes))
                    {
                        match compare_f64(l, r, op) {
                            Some(result) => builder.append_value(result),
                            None => builder.append_null(),
                        }
                    } else {
                        builder.append_null(); // Mismatched types or unsupported
                    }
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        _ => None, // Fallback to slow path
    }
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
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{}(): requires 2 arguments",
                self.name
            )));
        }

        // Try fast path first
        if let Some(result) = try_fast_compare(&args.args[0], &args.args[1], &self.op) {
            return Ok(result);
        }

        // Fallback to slow path
        let output_type = DataType::Boolean;
        invoke_cypher_udf(args, &output_type, |val_args| {
            crate::query::expr_eval::eval_binary_op(&val_args[0], &self.op, &val_args[1])
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }
}

// ============================================================================
// _cypher_add, _cypher_sub, _cypher_mul, _cypher_div, _cypher_mod:
// CypherValue-encoded arithmetic operators for mixed-type operations
// ============================================================================

pub fn create_cypher_add_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherArithmeticUdf::new("_cypher_add", BinaryOp::Add))
}
pub fn create_cypher_sub_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherArithmeticUdf::new("_cypher_sub", BinaryOp::Sub))
}
pub fn create_cypher_mul_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherArithmeticUdf::new("_cypher_mul", BinaryOp::Mul))
}
pub fn create_cypher_div_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherArithmeticUdf::new("_cypher_div", BinaryOp::Div))
}
pub fn create_cypher_mod_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherArithmeticUdf::new("_cypher_mod", BinaryOp::Mod))
}

/// Apply an integer arithmetic operator, returning CypherValue-encoded bytes.
/// Returns `None` on overflow or division by zero.
fn apply_int_arithmetic(lhs: i64, rhs: i64, op: &BinaryOp) -> Option<Vec<u8>> {
    use uni_common::cypher_value_codec::{encode_float, encode_int};
    match op {
        BinaryOp::Add => lhs.checked_add(rhs).map(encode_int),
        BinaryOp::Sub => lhs.checked_sub(rhs).map(encode_int),
        BinaryOp::Mul => lhs.checked_mul(rhs).map(encode_int),
        BinaryOp::Div => {
            // Division always produces float in Cypher
            if rhs == 0 {
                None
            } else {
                Some(encode_float(lhs as f64 / rhs as f64))
            }
        }
        BinaryOp::Mod => {
            if rhs == 0 {
                None
            } else {
                lhs.checked_rem(rhs).map(encode_int)
            }
        }
        _ => None,
    }
}

/// Apply a float arithmetic operator, returning CypherValue-encoded bytes.
fn apply_float_arithmetic(lhs: f64, rhs: f64, op: &BinaryOp) -> Option<Vec<u8>> {
    use uni_common::cypher_value_codec::encode_float;
    let result = match op {
        BinaryOp::Add => lhs + rhs,
        BinaryOp::Sub => lhs - rhs,
        BinaryOp::Mul => lhs * rhs,
        BinaryOp::Div => lhs / rhs, // Allows inf, -inf, NaN
        BinaryOp::Mod => lhs % rhs,
        _ => return None,
    };
    Some(encode_float(result))
}

/// Perform arithmetic on a CypherValue-encoded LHS against an i64 RHS.
/// Returns `None` for null/incompatible types.
fn cv_arithmetic_int(bytes: &[u8], rhs: i64, op: &BinaryOp) -> Option<Vec<u8>> {
    use uni_common::cypher_value_codec::{TAG_FLOAT, TAG_INT, decode_float, decode_int, peek_tag};
    match peek_tag(bytes)? {
        TAG_INT => apply_int_arithmetic(decode_int(bytes)?, rhs, op),
        TAG_FLOAT => apply_float_arithmetic(decode_float(bytes)?, rhs as f64, op),
        _ => None,
    }
}

/// Perform arithmetic on a CypherValue-encoded LHS against an f64 RHS.
/// Returns `None` for null/incompatible types.
fn cv_arithmetic_float(bytes: &[u8], rhs: f64, op: &BinaryOp) -> Option<Vec<u8>> {
    let lhs = cv_bytes_as_f64(bytes)?;
    apply_float_arithmetic(lhs, rhs, op)
}

/// Fast-path arithmetic for LargeBinary (CypherValue) vs native Arrow types.
///
/// Returns `Some(ColumnarValue)` if fast path succeeded, `None` to fallback to slow path.
fn try_fast_arithmetic(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    op: &BinaryOp,
) -> Option<ColumnarValue> {
    use arrow_array::builder::LargeBinaryBuilder;

    let (lhs_arr, rhs_arr) = match (lhs, rhs) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (l, r),
        _ => return None,
    };

    match (lhs_arr.data_type(), rhs_arr.data_type()) {
        // LargeBinary vs Int64
        (DataType::LargeBinary, DataType::Int64) => {
            let lb_arr = lhs_arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            let int_arr = rhs_arr.as_any().downcast_ref::<Int64Array>()?;
            let mut builder = LargeBinaryBuilder::new();
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || int_arr.is_null(i) {
                    builder.append_null();
                } else if let Some(bytes) = cv_arithmetic_int(lb_arr.value(i), int_arr.value(i), op)
                {
                    builder.append_value(&bytes);
                } else {
                    builder.append_null();
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        // LargeBinary vs Float64
        (DataType::LargeBinary, DataType::Float64) => {
            let lb_arr = lhs_arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            let float_arr = rhs_arr.as_any().downcast_ref::<Float64Array>()?;
            let mut builder = LargeBinaryBuilder::new();
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || float_arr.is_null(i) {
                    builder.append_null();
                } else if let Some(bytes) =
                    cv_arithmetic_float(lb_arr.value(i), float_arr.value(i), op)
                {
                    builder.append_value(&bytes);
                } else {
                    builder.append_null();
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        // Int64 vs Int64 (both native, routed here because other context forced UDF path)
        (DataType::Int64, DataType::Int64) => {
            let lhs_int = lhs_arr.as_any().downcast_ref::<Int64Array>()?;
            let rhs_int = rhs_arr.as_any().downcast_ref::<Int64Array>()?;
            let mut builder = LargeBinaryBuilder::new();
            for i in 0..lhs_int.len() {
                if lhs_int.is_null(i) || rhs_int.is_null(i) {
                    builder.append_null();
                } else if let Some(bytes) =
                    apply_int_arithmetic(lhs_int.value(i), rhs_int.value(i), op)
                {
                    builder.append_value(&bytes);
                } else {
                    builder.append_null();
                }
            }
            Some(ColumnarValue::Array(Arc::new(builder.finish())))
        }

        _ => None, // Fallback to slow path
    }
}

#[derive(Debug)]
struct CypherArithmeticUdf {
    name: String,
    op: BinaryOp,
    signature: Signature,
}

impl CypherArithmeticUdf {
    fn new(name: &str, op: BinaryOp) -> Self {
        Self {
            name: name.to_string(),
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for CypherArithmeticUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for CypherArithmeticUdf {}

impl std::hash::Hash for CypherArithmeticUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for CypherArithmeticUdf {
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
        Ok(DataType::LargeBinary) // result is CypherValue-encoded
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{}(): requires 2 arguments",
                self.name
            )));
        }

        // Try fast path first
        if let Some(result) = try_fast_arithmetic(&args.args[0], &args.args[1], &self.op) {
            return Ok(result);
        }

        // Fallback to slow path
        let output_type = DataType::LargeBinary;
        invoke_cypher_udf(args, &output_type, |val_args| {
            crate::query::expr_eval::eval_binary_op(&val_args[0], &self.op, &val_args[1])
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }
}

// ============================================================================
// _cypher_xor: 3-valued XOR with null propagation
// ============================================================================

pub fn create_cypher_xor_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherXorUdf::new())
}

#[derive(Debug)]
struct CypherXorUdf {
    signature: Signature,
}

impl CypherXorUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherXorUdf);

impl ScalarUDFImpl for CypherXorUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "_cypher_xor"
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
                    "_cypher_xor(): requires 2 arguments".to_string(),
                ));
            }
            // Coerce string-encoded booleans from UNWIND (Utf8 "true"/"false")
            let coerce_bool = |v: &Value| -> Value {
                match v {
                    Value::String(s) if s == "true" => Value::Bool(true),
                    Value::String(s) if s == "false" => Value::Bool(false),
                    other => other.clone(),
                }
            };
            let left = coerce_bool(&val_args[0]);
            let right = coerce_bool(&val_args[1]);
            crate::query::expr_eval::eval_binary_op(&left, &BinaryOp::Xor, &right)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }
}

// ============================================================================
// _cv_to_bool(value) -> Boolean
// Decode CypherValue (LargeBinary) to boolean for boolean context (WHERE, CASE WHEN).
// This is the ONLY extract UDF we keep - all other operations route through Cypher UDFs.
// ============================================================================

pub fn create_cv_to_bool_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CvToBoolUdf::new())
}

#[derive(Debug)]
struct CvToBoolUdf {
    signature: Signature,
}

impl CvToBoolUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::LargeBinary], Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CvToBoolUdf);

impl ScalarUDFImpl for CvToBoolUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "_cv_to_bool"
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
                "_cv_to_bool() requires exactly 1 argument".to_string(),
            ));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
                // Fast path: tag-only decode for boolean
                use uni_common::cypher_value_codec::{TAG_BOOL, TAG_NULL, decode_bool, peek_tag};
                let b = match peek_tag(bytes) {
                    Some(TAG_BOOL) => decode_bool(bytes).unwrap_or(false),
                    Some(TAG_NULL) => false,
                    _ => false, // Non-boolean in boolean context
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))))
            }
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None))),
            ColumnarValue::Array(arr) => {
                let lb_arr = arr
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "_cv_to_bool(): expected LargeBinary array, got {:?}",
                            arr.data_type()
                        ))
                    })?;

                let mut builder = arrow_array::builder::BooleanBuilder::with_capacity(lb_arr.len());

                // Fast path: tag-only decode for boolean
                use uni_common::cypher_value_codec::{TAG_BOOL, TAG_NULL, decode_bool, peek_tag};

                for i in 0..lb_arr.len() {
                    if lb_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        let bytes = lb_arr.value(i);
                        let b = match peek_tag(bytes) {
                            Some(TAG_BOOL) => decode_bool(bytes).unwrap_or(false),
                            Some(TAG_NULL) => false,
                            _ => false, // Non-boolean in boolean context
                        };
                        builder.append_value(b);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

// ============================================================================
// _cypher_size(value) -> Int64
// Polymorphic SIZE/LENGTH: dispatches on runtime type
// ============================================================================

pub fn create_cypher_size_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherSizeUdf::new())
}

#[derive(Debug)]
struct CypherSizeUdf {
    signature: Signature,
}

impl CypherSizeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherSizeUdf);

impl ScalarUDFImpl for CypherSizeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_size() requires exactly 1 argument".to_string(),
            ));
        }

        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                let result = cypher_size_scalar(scalar)?;
                Ok(ColumnarValue::Scalar(result))
            }
            ColumnarValue::Array(arr) => {
                let mut results: Vec<Option<i64>> = Vec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        results.push(None);
                    } else {
                        let scalar = ScalarValue::try_from_array(arr, i)?;
                        match cypher_size_scalar(&scalar)? {
                            ScalarValue::Int64(v) => results.push(v),
                            _ => results.push(None),
                        }
                    }
                }
                let arr: ArrayRef = Arc::new(arrow_array::Int64Array::from(results));
                Ok(ColumnarValue::Array(arr))
            }
        }
    }
}

fn cypher_size_scalar(scalar: &ScalarValue) -> DFResult<ScalarValue> {
    match scalar {
        // String types — return character count
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Ok(ScalarValue::Int64(Some(s.chars().count() as i64)))
        }
        // List types — return list length
        // ScalarValue::List wraps Arc<GenericListArray<i32>> with a single element
        ScalarValue::List(arr) => {
            if arr.is_empty() || arr.is_null(0) {
                Ok(ScalarValue::Int64(None))
            } else {
                Ok(ScalarValue::Int64(Some(arr.value(0).len() as i64)))
            }
        }
        ScalarValue::LargeList(arr) => {
            if arr.is_empty() || arr.is_null(0) {
                Ok(ScalarValue::Int64(None))
            } else {
                Ok(ScalarValue::Int64(Some(arr.value(0).len() as i64)))
            }
        }
        // LargeBinary (CypherValue) — decode and check type
        ScalarValue::LargeBinary(Some(b)) => {
            if let Ok(uni_val) = uni_common::cypher_value_codec::decode(b) {
                let json_val: serde_json::Value = uni_val.into();
                match json_val {
                    serde_json::Value::Array(arr) => Ok(ScalarValue::Int64(Some(arr.len() as i64))),
                    serde_json::Value::String(s) => {
                        Ok(ScalarValue::Int64(Some(s.chars().count() as i64)))
                    }
                    serde_json::Value::Object(m) => Ok(ScalarValue::Int64(Some(m.len() as i64))),
                    _ => Ok(ScalarValue::Int64(None)),
                }
            } else {
                Ok(ScalarValue::Int64(None))
            }
        }
        // Map type — return number of keys
        ScalarValue::Map(arr) => {
            if arr.is_empty() || arr.is_null(0) {
                Ok(ScalarValue::Int64(None))
            } else {
                // MapArray wraps a single map entry; value(0) returns the entries struct
                Ok(ScalarValue::Int64(Some(arr.value(0).len() as i64)))
            }
        }
        // Struct — for path structs (nodes + relationships), return edge count
        ScalarValue::Struct(arr) => {
            if arr.is_null(0) {
                Ok(ScalarValue::Int64(None))
            } else {
                // Check if this is a path struct (has "relationships" field)
                let schema = arr.fields();
                if let Some((rels_idx, _)) = schema
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name() == "relationships")
                {
                    // Path struct: length = number of relationships
                    let rels_col = arr.column(rels_idx);
                    if let Some(list_arr) =
                        rels_col.as_any().downcast_ref::<arrow_array::ListArray>()
                    {
                        if list_arr.is_null(0) {
                            Ok(ScalarValue::Int64(Some(0)))
                        } else {
                            Ok(ScalarValue::Int64(Some(list_arr.value(0).len() as i64)))
                        }
                    } else {
                        Ok(ScalarValue::Int64(Some(arr.num_columns() as i64)))
                    }
                } else {
                    Ok(ScalarValue::Int64(Some(arr.num_columns() as i64)))
                }
            }
        }
        // Null
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::LargeBinary(None) => Ok(ScalarValue::Int64(None)),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "_cypher_size(): unsupported type {other:?}"
        ))),
    }
}

// ============================================================================
// _cypher_list_compare(left_list, right_list, op_string) -> Boolean
// Lexicographic list ordering for Cypher comparison semantics
// ============================================================================

pub fn create_cypher_list_compare_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherListCompareUdf::new())
}

#[derive(Debug)]
struct CypherListCompareUdf {
    signature: Signature,
}

impl CypherListCompareUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherListCompareUdf);

impl ScalarUDFImpl for CypherListCompareUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_list_compare"
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
            if val_args.len() != 3 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_list_compare(): requires 3 arguments (left, right, op)".to_string(),
                ));
            }

            let left = &val_args[0];
            let right = &val_args[1];
            let op_str = match &val_args[2] {
                Value::String(s) => s.as_str(),
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "_cypher_list_compare(): op must be a string".to_string(),
                    ));
                }
            };

            let (left_items, right_items) = match (left, right) {
                (Value::List(l), Value::List(r)) => (l, r),
                (Value::Null, _) | (_, Value::Null) => return Ok(Value::Null),
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "_cypher_list_compare(): both arguments must be lists".to_string(),
                    ));
                }
            };

            // Element-wise comparison using Cypher ordering semantics
            let cmp = cypher_list_cmp(left_items, right_items);

            let result = match (op_str, cmp) {
                (_, None) => Value::Null,
                ("lt", Some(ord)) => Value::Bool(ord == std::cmp::Ordering::Less),
                ("lteq", Some(ord)) => Value::Bool(ord != std::cmp::Ordering::Greater),
                ("gt", Some(ord)) => Value::Bool(ord == std::cmp::Ordering::Greater),
                ("gteq", Some(ord)) => Value::Bool(ord != std::cmp::Ordering::Less),
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_list_compare(): unknown op '{}'",
                        op_str
                    )));
                }
            };

            Ok(result)
        })
    }
}

// ============================================================================
// _map_project(key1, val1, key2, val2, ...) -> LargeBinary (CypherValue)
// ============================================================================

pub fn create_map_project_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(MapProjectUdf::new())
}

#[derive(Debug)]
struct MapProjectUdf {
    signature: Signature,
}

impl MapProjectUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(MapProjectUdf);

impl ScalarUDFImpl for MapProjectUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_map_project"
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
            let mut result_map = std::collections::HashMap::new();
            let mut i = 0;
            while i + 1 < val_args.len() {
                let key = &val_args[i];
                let value = &val_args[i + 1];
                if let Some(k) = key.as_str() {
                    if k == "__all__" {
                        // AllProperties: expand entity map, skip _-prefixed keys
                        match value {
                            Value::Map(map) => {
                                for (mk, mv) in map {
                                    if !mk.starts_with('_') {
                                        result_map.insert(mk.clone(), mv.clone());
                                    }
                                }
                            }
                            Value::Node(node) => {
                                for (pk, pv) in &node.properties {
                                    result_map.insert(pk.clone(), pv.clone());
                                }
                            }
                            Value::Edge(edge) => {
                                for (pk, pv) in &edge.properties {
                                    result_map.insert(pk.clone(), pv.clone());
                                }
                            }
                            _ => {}
                        }
                    } else {
                        result_map.insert(k.to_string(), value.clone());
                    }
                }
                i += 2;
            }
            Ok(Value::Map(result_map))
        })
    }
}

// ============================================================================
// _make_cypher_list(arg0, arg1, ...) -> LargeBinary (CypherValue array)
// ============================================================================

pub fn create_make_cypher_list_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(MakeCypherListUdf::new())
}

#[derive(Debug)]
struct MakeCypherListUdf {
    signature: Signature,
}

impl MakeCypherListUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(MakeCypherListUdf);

impl ScalarUDFImpl for MakeCypherListUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_make_cypher_list"
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
            Ok(Value::List(val_args.to_vec()))
        })
    }
}

// ============================================================================
// _cypher_in(element, list) -> Boolean (nullable)
// ============================================================================

/// Create the `_cypher_in` UDF for Cypher's `x IN list` semantics.
///
/// Handles all list representations (native List, Utf8 json-encoded, LargeBinary CypherValue)
/// via `invoke_cypher_udf` which converts everything to `Value` first.
///
/// Cypher IN semantics (3-valued logic):
/// - list is null → null
/// - x found in list → true
/// - x not found, list contains null → null
/// - x not found, no nulls → false
/// - x is null, list empty → false
/// - x is null, list non-empty → null
pub fn create_cypher_in_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherInUdf::new())
}

#[derive(Debug)]
struct CypherInUdf {
    signature: Signature,
}

impl CypherInUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherInUdf);

impl ScalarUDFImpl for CypherInUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_in"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::Boolean, |vals| {
            if vals.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_in(): requires 2 arguments".to_string(),
                ));
            }
            let element = &vals[0];
            let list_val = &vals[1];

            // If list is null, result is null
            if list_val.is_null() {
                return Ok(Value::Null);
            }

            // Extract list items
            let items = match list_val {
                Value::List(items) => items.as_slice(),
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_in(): second argument must be a list, got {:?}",
                        list_val
                    )));
                }
            };

            // If element is null
            if element.is_null() {
                return if items.is_empty() {
                    Ok(Value::Bool(false))
                } else {
                    Ok(Value::Null) // null IN non-empty list → null
                };
            }

            // 3-valued comparison: cypher_eq returns Some(true/false) or None (indeterminate)
            let mut has_null = false;
            for item in items {
                match cypher_eq(element, item) {
                    Some(true) => return Ok(Value::Bool(true)),
                    None => has_null = true,
                    Some(false) => {}
                }
            }

            if has_null {
                Ok(Value::Null) // not found but comparison was indeterminate → null
            } else {
                Ok(Value::Bool(false))
            }
        })
    }
}

// ============================================================================
// _cypher_list_concat(left, right) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_list_concat` UDF for Cypher `list + list` concatenation.
pub fn create_cypher_list_concat_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherListConcatUdf::new())
}

#[derive(Debug)]
struct CypherListConcatUdf {
    signature: Signature,
}

impl CypherListConcatUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherListConcatUdf);

impl ScalarUDFImpl for CypherListConcatUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_list_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
            if vals.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_list_concat(): requires 2 arguments".to_string(),
                ));
            }
            // If either is null, result is null
            if vals[0].is_null() || vals[1].is_null() {
                return Ok(Value::Null);
            }
            match (&vals[0], &vals[1]) {
                (Value::List(left), Value::List(right)) => {
                    let mut result = left.clone();
                    result.extend(right.iter().cloned());
                    Ok(Value::List(result))
                }
                // When both sides are CypherValue we can't distinguish list+scalar
                // from list+list at compile time; handle append/prepend here too
                (Value::List(list), elem) => {
                    let mut result = list.clone();
                    result.push(elem.clone());
                    Ok(Value::List(result))
                }
                (elem, Value::List(list)) => {
                    let mut result = vec![elem.clone()];
                    result.extend(list.iter().cloned());
                    Ok(Value::List(result))
                }
                _ => {
                    // Neither is a list — fall back to regular addition
                    // (dispatch routes all CypherValue Plus here because LargeBinary matches)
                    crate::query::expr_eval::eval_binary_op(
                        &vals[0],
                        &uni_cypher::ast::BinaryOp::Add,
                        &vals[1],
                    )
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
                }
            }
        })
    }
}

// ============================================================================
// _cypher_list_append(left, right) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_list_append` UDF for Cypher `list + element` or `element + list`.
pub fn create_cypher_list_append_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherListAppendUdf::new())
}

#[derive(Debug)]
struct CypherListAppendUdf {
    signature: Signature,
}

impl CypherListAppendUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherListAppendUdf);

impl ScalarUDFImpl for CypherListAppendUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_list_append"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
            if vals.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_list_append(): requires 2 arguments".to_string(),
                ));
            }
            let left = &vals[0];
            let right = &vals[1];

            // If either is null, result is null
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }

            match (left, right) {
                // list + scalar → append
                (Value::List(list), elem) => {
                    let mut result = list.clone();
                    result.push(elem.clone());
                    Ok(Value::List(result))
                }
                // scalar + list → prepend
                (elem, Value::List(list)) => {
                    let mut result = vec![elem.clone()];
                    result.extend(list.iter().cloned());
                    Ok(Value::List(result))
                }
                _ => Err(datafusion::error::DataFusionError::Execution(format!(
                    "_cypher_list_append(): at least one argument must be a list, got {:?} and {:?}",
                    left, right
                ))),
            }
        })
    }
}

// ============================================================================
// _cypher_list_slice(list, start, end) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_list_slice` UDF for Cypher list slicing on CypherValue-encoded lists.
pub fn create_cypher_list_slice_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherListSliceUdf::new())
}

#[derive(Debug)]
struct CypherListSliceUdf {
    signature: Signature,
}

impl CypherListSliceUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherListSliceUdf);

impl ScalarUDFImpl for CypherListSliceUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_list_slice"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
            if vals.len() != 3 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_list_slice(): requires 3 arguments (list, start, end)".to_string(),
                ));
            }
            // Null list → null
            if vals[0].is_null() {
                return Ok(Value::Null);
            }
            let list = match &vals[0] {
                Value::List(l) => l,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_list_slice(): first argument must be a list, got {:?}",
                        vals[0]
                    )));
                }
            };
            // Null bounds → null result
            if vals[1].is_null() || vals[2].is_null() {
                return Ok(Value::Null);
            }

            let len = list.len() as i64;
            let raw_start = match &vals[1] {
                Value::Int(i) => *i,
                _ => 0,
            };
            let raw_end = match &vals[2] {
                Value::Int(i) => *i,
                _ => len,
            };

            // Resolve negative indices: if idx < 0 → len + idx (clamp to 0)
            let start = if raw_start < 0 {
                (len + raw_start).max(0) as usize
            } else {
                (raw_start).min(len) as usize
            };
            let end = if raw_end == i64::MAX {
                len as usize
            } else if raw_end < 0 {
                (len + raw_end).max(0) as usize
            } else {
                (raw_end).min(len) as usize
            };

            if start >= end {
                return Ok(Value::List(vec![]));
            }
            Ok(Value::List(list[start..end.min(list.len())].to_vec()))
        })
    }
}

// ============================================================================
// _cypher_reverse(val) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_reverse` UDF for Cypher `reverse()`.
///
/// Handles both strings and lists:
/// - `reverse("abc")` → `"cba"`
/// - `reverse([1,2,3])` → `[3,2,1]`
/// - `reverse(null)` → `null`
pub fn create_cypher_reverse_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherReverseUdf::new())
}

#[derive(Debug)]
struct CypherReverseUdf {
    signature: Signature,
}

impl CypherReverseUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherReverseUdf);

impl ScalarUDFImpl for CypherReverseUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
            if vals.len() != 1 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_reverse(): requires exactly 1 argument".to_string(),
                ));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                Value::String(s) => Ok(Value::String(s.chars().rev().collect())),
                Value::List(l) => {
                    let mut reversed = l.clone();
                    reversed.reverse();
                    Ok(Value::List(reversed))
                }
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "_cypher_reverse(): expected string or list, got {:?}",
                    other
                ))),
            }
        })
    }
}

// ============================================================================
// _cypher_tail(list) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_tail` UDF for Cypher `tail()`.
///
/// Returns all elements except the first element of a list.
/// - `tail([1,2,3])` → `[2,3]`
/// - `tail([1])` → `[]`
/// - `tail([])` → `[]`
/// - `tail(null)` → `null`
pub fn create_cypher_tail_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherTailUdf::new())
}

#[derive(Debug)]
struct CypherTailUdf {
    signature: Signature,
}

impl CypherTailUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherTailUdf);

impl ScalarUDFImpl for CypherTailUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_tail"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
            if vals.len() != 1 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_tail(): requires exactly 1 argument".to_string(),
                ));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                Value::List(l) => {
                    if l.is_empty() {
                        Ok(Value::List(vec![]))
                    } else {
                        Ok(Value::List(l[1..].to_vec()))
                    }
                }
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "_cypher_tail(): expected list, got {:?}",
                    other
                ))),
            }
        })
    }
}

/// Compare two lists element-wise using Cypher ordering semantics.
/// Returns None if comparison is undefined (incompatible types).
fn cypher_list_cmp(left: &[Value], right: &[Value]) -> Option<std::cmp::Ordering> {
    let min_len = left.len().min(right.len());
    for i in 0..min_len {
        let cmp = cypher_value_cmp(&left[i], &right[i])?;
        if cmp != std::cmp::Ordering::Equal {
            return Some(cmp);
        }
    }
    // All compared elements are equal; shorter list is "less"
    Some(left.len().cmp(&right.len()))
}

/// Compare two Cypher values for ordering.
/// Returns None if types are incomparable.
fn cypher_value_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Int(l), Value::Int(r)) => Some(l.cmp(r)),
        (Value::Float(l), Value::Float(r)) => l.partial_cmp(r),
        (Value::Int(l), Value::Float(r)) => (*l as f64).partial_cmp(r),
        (Value::Float(l), Value::Int(r)) => l.partial_cmp(&(*r as f64)),
        (Value::String(l), Value::String(r)) => Some(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Some(l.cmp(r)),
        (Value::List(l), Value::List(r)) => cypher_list_cmp(l, r),
        _ => None, // Incomparable types
    }
}

// ============================================================================
// CypherToFloat64 Scalar UDF
// ============================================================================

/// Scalar UDF that decodes LargeBinary CypherValue bytes to Float64.
/// Non-numeric or null inputs produce Arrow null.
/// Non-LargeBinary inputs (e.g., Int64, Float64) are passed through with a cast.
struct CypherToFloat64Udf {
    signature: Signature,
}

impl CypherToFloat64Udf {
    fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Any(1),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(CypherToFloat64Udf);

impl std::fmt::Debug for CypherToFloat64Udf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CypherToFloat64Udf").finish()
    }
}

impl ScalarUDFImpl for CypherToFloat64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "_cypher_to_float64"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_to_float64 requires exactly 1 argument".into(),
            ));
        }
        match &args.args[0] {
            ColumnarValue::Scalar(scalar) => {
                let f = match scalar {
                    ScalarValue::LargeBinary(Some(bytes)) => cv_bytes_as_f64(bytes),
                    ScalarValue::Int64(Some(i)) => Some(*i as f64),
                    ScalarValue::Int32(Some(i)) => Some(*i as f64),
                    ScalarValue::Float64(Some(f)) => Some(*f),
                    ScalarValue::Float32(Some(f)) => Some(*f as f64),
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(f)))
            }
            ColumnarValue::Array(arr) => {
                let len = arr.len();
                let mut builder = arrow::array::Float64Builder::with_capacity(len);
                match arr.data_type() {
                    DataType::LargeBinary => {
                        let lb = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                        for i in 0..len {
                            if lb.is_null(i) {
                                builder.append_null();
                            } else {
                                match cv_bytes_as_f64(lb.value(i)) {
                                    Some(f) => builder.append_value(f),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    DataType::Int64 => {
                        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                        for i in 0..len {
                            if int_arr.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(int_arr.value(i) as f64);
                            }
                        }
                    }
                    DataType::Float64 => {
                        let f_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                        for i in 0..len {
                            if f_arr.is_null(i) {
                                builder.append_null();
                            } else {
                                builder.append_value(f_arr.value(i));
                            }
                        }
                    }
                    _ => {
                        for _ in 0..len {
                            builder.append_null();
                        }
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
        }
    }
}

fn create_cypher_to_float64_udf() -> ScalarUDF {
    ScalarUDF::from(CypherToFloat64Udf::new())
}

/// Helper: wrap a DataFusion expression with `_cypher_to_float64()` UDF.
pub(crate) fn cypher_to_float64_expr(arg: datafusion::logical_expr::Expr) -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::ScalarFunction(
        datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(create_cypher_to_float64_udf()),
            vec![arg],
        ),
    )
}

// ============================================================================
// Cypher-aware Min/Max UDAFs
// ============================================================================

/// Cross-type ordering rank for Cypher min/max (lower rank = smaller).
/// In OpenCypher: MAP < NODE < REL < PATH < LIST < STRING < BOOLEAN < NUMBER
/// For min/max, we use: LIST(1) < STRING(2) < BOOLEAN(3) < NUMBER(4)
fn cypher_type_rank(val: &Value) -> u8 {
    match val {
        Value::Null => 0,
        Value::List(_) => 1,
        Value::String(_) => 2,
        Value::Bool(_) => 3,
        Value::Int(_) | Value::Float(_) => 4,
        _ => 5, // Map, Node, Edge, Path, etc.
    }
}

/// Compare two Cypher values for min/max with cross-type ordering.
/// Uses type rank for different types, within-type comparison for same type.
fn cypher_cross_type_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let ra = cypher_type_rank(a);
    let rb = cypher_type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }
    // Same type rank: compare within type
    match (a, b) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        (Value::Float(l), Value::Float(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Int(l), Value::Float(r)) => (*l as f64)
            .partial_cmp(r)
            .unwrap_or(Ordering::Equal),
        (Value::Float(l), Value::Int(r)) => l
            .partial_cmp(&(*r as f64))
            .unwrap_or(Ordering::Equal),
        (Value::String(l), Value::String(r)) => l.cmp(r),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (Value::List(l), Value::List(r)) => {
            cypher_list_cmp(l, r).unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

/// Decode a LargeBinary scalar into a Value.
fn scalar_binary_to_value(bytes: &[u8]) -> Value {
    uni_common::cypher_value_codec::decode(bytes).unwrap_or(Value::Null)
}

use datafusion::logical_expr::{
    Accumulator as DfAccumulator, AggregateUDF, AggregateUDFImpl,
};

/// Custom UDAF for Cypher-aware min/max on LargeBinary columns.
#[derive(Debug, Clone)]
struct CypherMinMaxUdaf {
    name: String,
    signature: Signature,
    is_max: bool,
}

impl CypherMinMaxUdaf {
    fn new(is_max: bool) -> Self {
        let name = if is_max {
            "_cypher_max"
        } else {
            "_cypher_min"
        };
        Self {
            name: name.to_string(),
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            is_max,
        }
    }
}

impl PartialEq for CypherMinMaxUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for CypherMinMaxUdaf {}

impl Hash for CypherMinMaxUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl AggregateUDFImpl for CypherMinMaxUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        // Return same type as input
        Ok(args.first().cloned().unwrap_or(DataType::LargeBinary))
    }
    fn accumulator(&self, _acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherMinMaxAccumulator {
            current: None,
            is_max: self.is_max,
        }))
    }
    fn state_fields(&self, args: datafusion::logical_expr::function::StateFieldsArgs) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![Arc::new(arrow::datatypes::Field::new(
            args.name,
            DataType::LargeBinary,
            true,
        ))])
    }
}

#[derive(Debug)]
struct CypherMinMaxAccumulator {
    current: Option<Value>,
    is_max: bool,
}

impl DfAccumulator for CypherMinMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        match arr.data_type() {
            DataType::LargeBinary => {
                let lb = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                for i in 0..lb.len() {
                    if lb.is_null(i) {
                        continue;
                    }
                    let val = scalar_binary_to_value(lb.value(i));
                    if val.is_null() {
                        continue;
                    }
                    self.current = Some(match self.current.take() {
                        None => val,
                        Some(cur) => {
                            let ord = cypher_cross_type_cmp(&val, &cur);
                            if (self.is_max && ord == std::cmp::Ordering::Greater)
                                || (!self.is_max && ord == std::cmp::Ordering::Less)
                            {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
            _ => {
                // For non-LargeBinary inputs, decode via ScalarValue
                for i in 0..arr.len() {
                    if arr.is_null(i) {
                        continue;
                    }
                    let sv = ScalarValue::try_from_array(arr, i)
                        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                    let val = scalar_to_value(&sv)?;
                    if val.is_null() {
                        continue;
                    }
                    self.current = Some(match self.current.take() {
                        None => val,
                        Some(cur) => {
                            let ord = cypher_cross_type_cmp(&val, &cur);
                            if (self.is_max && ord == std::cmp::Ordering::Greater)
                                || (!self.is_max && ord == std::cmp::Ordering::Less)
                            {
                                val
                            } else {
                                cur
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match &self.current {
            None => Ok(ScalarValue::LargeBinary(None)),
            Some(val) => {
                let bytes = uni_common::cypher_value_codec::encode(val);
                Ok(ScalarValue::LargeBinary(Some(bytes)))
            }
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.current.as_ref().map_or(0, |_| 64)
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.update_batch(states)
    }
}

pub(crate) fn create_cypher_min_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherMinMaxUdaf::new(false))
}

pub(crate) fn create_cypher_max_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherMinMaxUdaf::new(true))
}

// ============================================================================
// Cypher-aware SUM UDAF
// ============================================================================

/// Custom UDAF for Cypher sum that preserves integer type when all inputs are integers.
#[derive(Debug, Clone)]
struct CypherSumUdaf {
    signature: Signature,
}

impl CypherSumUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl PartialEq for CypherSumUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for CypherSumUdaf {}

impl Hash for CypherSumUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl AggregateUDFImpl for CypherSumUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "_cypher_sum"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        // We'll return LargeBinary to encode the result as a CypherValue,
        // which preserves Int vs Float distinction.
        Ok(DataType::LargeBinary)
    }
    fn accumulator(&self, _acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherSumAccumulator {
            sum: 0.0,
            all_ints: true,
            int_sum: 0i64,
            has_value: false,
        }))
    }
    fn state_fields(&self, args: datafusion::logical_expr::function::StateFieldsArgs) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_sum", args.name),
                DataType::Float64,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_int_sum", args.name),
                DataType::Int64,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_all_ints", args.name),
                DataType::Boolean,
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_has_value", args.name),
                DataType::Boolean,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct CypherSumAccumulator {
    sum: f64,
    all_ints: bool,
    int_sum: i64,
    has_value: bool,
}

impl DfAccumulator for CypherSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            match arr.data_type() {
                DataType::LargeBinary => {
                    let lb = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    let bytes = lb.value(i);
                    use uni_common::cypher_value_codec::{TAG_INT, TAG_FLOAT, peek_tag, decode_int, decode_float};
                    match peek_tag(bytes) {
                        Some(TAG_INT) => {
                            if let Some(v) = decode_int(bytes) {
                                self.sum += v as f64;
                                self.int_sum = self.int_sum.wrapping_add(v);
                                self.has_value = true;
                            }
                        }
                        Some(TAG_FLOAT) => {
                            if let Some(v) = decode_float(bytes) {
                                self.sum += v;
                                self.all_ints = false;
                                self.has_value = true;
                            }
                        }
                        _ => {} // skip non-numeric
                    }
                }
                DataType::Int64 => {
                    let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    let v = a.value(i);
                    self.sum += v as f64;
                    self.int_sum = self.int_sum.wrapping_add(v);
                    self.has_value = true;
                }
                DataType::Float64 => {
                    let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                    self.sum += a.value(i);
                    self.all_ints = false;
                    self.has_value = true;
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if !self.has_value {
            return Ok(ScalarValue::LargeBinary(None));
        }
        let val = if self.all_ints {
            Value::Int(self.int_sum)
        } else {
            Value::Float(self.sum)
        };
        let bytes = uni_common::cypher_value_codec::encode(&val);
        Ok(ScalarValue::LargeBinary(Some(bytes)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.sum)),
            ScalarValue::Int64(Some(self.int_sum)),
            ScalarValue::Boolean(Some(self.all_ints)),
            ScalarValue::Boolean(Some(self.has_value)),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let sum_arr = states[0].as_any().downcast_ref::<Float64Array>().unwrap();
        let int_sum_arr = states[1].as_any().downcast_ref::<Int64Array>().unwrap();
        let all_ints_arr = states[2].as_any().downcast_ref::<BooleanArray>().unwrap();
        let has_value_arr = states[3].as_any().downcast_ref::<BooleanArray>().unwrap();
        for i in 0..sum_arr.len() {
            if !has_value_arr.is_null(i) && has_value_arr.value(i) {
                self.sum += sum_arr.value(i);
                self.int_sum = self.int_sum.wrapping_add(int_sum_arr.value(i));
                if !all_ints_arr.value(i) {
                    self.all_ints = false;
                }
                self.has_value = true;
            }
        }
        Ok(())
    }
}

pub(crate) fn create_cypher_sum_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherSumUdaf::new())
}

// ============================================================================
// Cypher-aware COLLECT UDAF
// ============================================================================

/// Custom UDAF for Cypher collect() that filters nulls and returns [] (not null)
/// when all inputs are null.
#[derive(Debug, Clone)]
struct CypherCollectUdaf {
    signature: Signature,
}

impl CypherCollectUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl PartialEq for CypherCollectUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for CypherCollectUdaf {}

impl Hash for CypherCollectUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl AggregateUDFImpl for CypherCollectUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "_cypher_collect"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn accumulator(&self, acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherCollectAccumulator {
            values: Vec::new(),
            distinct: acc_args.is_distinct,
        }))
    }
    fn state_fields(&self, args: datafusion::logical_expr::function::StateFieldsArgs) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![Arc::new(arrow::datatypes::Field::new(
            args.name,
            DataType::LargeBinary,
            true,
        ))])
    }
}

#[derive(Debug)]
struct CypherCollectAccumulator {
    values: Vec<Value>,
    distinct: bool,
}

impl DfAccumulator for CypherCollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            // For struct columns (node/edge from OPTIONAL MATCH), the struct itself
            // may not be null, but the identity field (_vid/_eid) inside may be null.
            // Check the first child array of the struct to detect this case.
            if let Some(struct_arr) = arr.as_any().downcast_ref::<arrow::array::StructArray>()
                && struct_arr.num_columns() > 0
                && struct_arr.column(0).is_null(i)
            {
                continue;
            }
            let sv = ScalarValue::try_from_array(arr, i)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            let val = scalar_to_value(&sv)?;
            if val.is_null() {
                continue;
            }
            if self.distinct {
                // Use string repr for dedup (consistent with CountDistinct)
                let repr = val.to_string();
                if self.values.iter().any(|v| v.to_string() == repr) {
                    continue;
                }
            }
            self.values.push(val);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        // Always return a list (empty list, not null)
        let val = Value::List(self.values.clone());
        let bytes = uni_common::cypher_value_codec::encode(&val);
        Ok(ScalarValue::LargeBinary(Some(bytes)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.len() * 64
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // States are LargeBinary containing encoded list values
        let arr = &states[0];
        if let Some(lb) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
            for i in 0..lb.len() {
                if lb.is_null(i) {
                    continue;
                }
                let val = scalar_binary_to_value(lb.value(i));
                if let Value::List(items) = val {
                    for item in items {
                        if !item.is_null() {
                            if self.distinct {
                                let repr = item.to_string();
                                if self.values.iter().any(|v| v.to_string() == repr) {
                                    continue;
                                }
                            }
                            self.values.push(item);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn create_cypher_collect_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherCollectUdaf::new())
}

/// Create a Cypher collect() UDAF expression with optional distinct.
pub(crate) fn create_cypher_collect_expr(arg: datafusion::logical_expr::Expr, distinct: bool) -> datafusion::logical_expr::Expr {
    // We use the UDAF's call() but need to set distinct separately.
    // For now, always include arg directly - distinct is handled in the accumulator.
    let udaf = Arc::new(create_cypher_collect_udaf());
    if distinct {
        // Create with distinct flag set
        datafusion::logical_expr::Expr::AggregateFunction(
            datafusion::logical_expr::expr::AggregateFunction::new_udf(
                udaf,
                vec![arg],
                true,  // distinct
                None,
                vec![],
                None,
            ),
        )
    } else {
        udaf.call(vec![arg])
    }
}

// ============================================================================
// Cypher percentileDisc / percentileCont UDAFs
// ============================================================================

/// Custom UDAF for Cypher percentileDisc().
#[derive(Debug, Clone)]
struct CypherPercentileDiscUdaf {
    signature: Signature,
}

impl CypherPercentileDiscUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl PartialEq for CypherPercentileDiscUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for CypherPercentileDiscUdaf {}

impl Hash for CypherPercentileDiscUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl AggregateUDFImpl for CypherPercentileDiscUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "percentiledisc"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn accumulator(&self, _acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherPercentileDiscAccumulator {
            values: Vec::new(),
            percentile: None,
        }))
    }
    fn state_fields(&self, args: datafusion::logical_expr::function::StateFieldsArgs) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_values", args.name),
                DataType::List(Arc::new(arrow::datatypes::Field::new("item", DataType::Float64, true))),
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_percentile", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct CypherPercentileDiscAccumulator {
    values: Vec<f64>,
    percentile: Option<f64>,
}

impl CypherPercentileDiscAccumulator {
    fn extract_f64(arr: &ArrayRef, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            return None;
        }
        match arr.data_type() {
            DataType::LargeBinary => {
                let lb = arr.as_any().downcast_ref::<LargeBinaryArray>()?;
                cv_bytes_as_f64(lb.value(i))
            }
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<Int64Array>()?;
                Some(a.value(i) as f64)
            }
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<Float64Array>()?;
                Some(a.value(i))
            }
            DataType::Int32 => {
                let a = arr.as_any().downcast_ref::<Int32Array>()?;
                Some(a.value(i) as f64)
            }
            DataType::Float32 => {
                let a = arr.as_any().downcast_ref::<Float32Array>()?;
                Some(a.value(i) as f64)
            }
            _ => None,
        }
    }

    fn extract_percentile(arr: &ArrayRef, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            return None;
        }
        match arr.data_type() {
            DataType::Float64 => {
                let a = arr.as_any().downcast_ref::<Float64Array>()?;
                Some(a.value(i))
            }
            DataType::Int64 => {
                let a = arr.as_any().downcast_ref::<Int64Array>()?;
                Some(a.value(i) as f64)
            }
            DataType::LargeBinary => {
                let lb = arr.as_any().downcast_ref::<LargeBinaryArray>()?;
                cv_bytes_as_f64(lb.value(i))
            }
            _ => None,
        }
    }
}

impl DfAccumulator for CypherPercentileDiscAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let expr_arr = &values[0];
        let pct_arr = &values[1];
        for i in 0..expr_arr.len() {
            // Extract percentile from second arg (constant for all rows)
            if self.percentile.is_none() && let Some(p) = Self::extract_percentile(pct_arr, i) {
                if !(0.0..=1.0).contains(&p) {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "ArgumentError: NumberOutOfRange - percentileDisc(): percentile value must be between 0.0 and 1.0".to_string(),
                    ));
                }
                self.percentile = Some(p);
            }
            if let Some(f) = Self::extract_f64(expr_arr, i) {
                self.values.push(f);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let pct = match self.percentile {
            Some(p) if !(0.0..=1.0).contains(&p) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "ArgumentError: NumberOutOfRange - percentileDisc(): percentile value must be between 0.0 and 1.0".to_string(),
                ));
            }
            Some(p) => p,
            None => 0.0,
        };
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = self.values.len();
        let idx = (pct * (n as f64 - 1.0)).round() as usize;
        let idx = idx.min(n - 1);
        let result = self.values[idx];
        Ok(ScalarValue::Float64(Some(result)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * 8
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        // State: list of f64 values + percentile
        let list_values: Vec<ScalarValue> = self.values.iter().map(|f| ScalarValue::Float64(Some(*f))).collect();
        let list_scalar = ScalarValue::List(ScalarValue::new_list(&list_values, &DataType::Float64, true));
        Ok(vec![
            list_scalar,
            ScalarValue::Float64(self.percentile),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // Merge list arrays from state
        let list_arr = &states[0];
        let pct_arr = &states[1];
        // Extract percentile
        if self.percentile.is_none()
            && let Some(f64_arr) = pct_arr.as_any().downcast_ref::<Float64Array>()
        {
            for i in 0..f64_arr.len() {
                if !f64_arr.is_null(i) {
                    self.percentile = Some(f64_arr.value(i));
                    break;
                }
            }
        }
        // Extract values from list arrays
        if let Some(list_array) = list_arr.as_any().downcast_ref::<arrow_array::ListArray>() {
            for i in 0..list_array.len() {
                if list_array.is_null(i) {
                    continue;
                }
                let inner = list_array.value(i);
                if let Some(f64_arr) = inner.as_any().downcast_ref::<Float64Array>() {
                    for j in 0..f64_arr.len() {
                        if !f64_arr.is_null(j) {
                            self.values.push(f64_arr.value(j));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Custom UDAF for Cypher percentileCont().
#[derive(Debug, Clone)]
struct CypherPercentileContUdaf {
    signature: Signature,
}

impl CypherPercentileContUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl PartialEq for CypherPercentileContUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for CypherPercentileContUdaf {}

impl Hash for CypherPercentileContUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl AggregateUDFImpl for CypherPercentileContUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "percentilecont"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }
    fn accumulator(&self, _acc_args: datafusion::logical_expr::function::AccumulatorArgs) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherPercentileContAccumulator {
            values: Vec::new(),
            percentile: None,
        }))
    }
    fn state_fields(&self, args: datafusion::logical_expr::function::StateFieldsArgs) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_values", args.name),
                DataType::List(Arc::new(arrow::datatypes::Field::new("item", DataType::Float64, true))),
                true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_percentile", args.name),
                DataType::Float64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct CypherPercentileContAccumulator {
    values: Vec<f64>,
    percentile: Option<f64>,
}

impl DfAccumulator for CypherPercentileContAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let expr_arr = &values[0];
        let pct_arr = &values[1];
        for i in 0..expr_arr.len() {
            if self.percentile.is_none() && let Some(p) = CypherPercentileDiscAccumulator::extract_percentile(pct_arr, i) {
                if !(0.0..=1.0).contains(&p) {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "ArgumentError: NumberOutOfRange - percentileCont(): percentile value must be between 0.0 and 1.0".to_string(),
                    ));
                }
                self.percentile = Some(p);
            }
            if let Some(f) = CypherPercentileDiscAccumulator::extract_f64(expr_arr, i) {
                self.values.push(f);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let pct = match self.percentile {
            Some(p) if !(0.0..=1.0).contains(&p) => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "ArgumentError: NumberOutOfRange - percentileCont(): percentile value must be between 0.0 and 1.0".to_string(),
                ));
            }
            Some(p) => p,
            None => 0.0,
        };
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        self.values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = self.values.len();
        if n == 1 {
            return Ok(ScalarValue::Float64(Some(self.values[0])));
        }
        let pos = pct * (n as f64 - 1.0);
        let lower = pos.floor() as usize;
        let upper = pos.ceil() as usize;
        let lower = lower.min(n - 1);
        let upper = upper.min(n - 1);
        if lower == upper {
            Ok(ScalarValue::Float64(Some(self.values[lower])))
        } else {
            let frac = pos - lower as f64;
            let result = self.values[lower] + frac * (self.values[upper] - self.values[lower]);
            Ok(ScalarValue::Float64(Some(result)))
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * 8
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let list_values: Vec<ScalarValue> = self.values.iter().map(|f| ScalarValue::Float64(Some(*f))).collect();
        let list_scalar = ScalarValue::List(ScalarValue::new_list(&list_values, &DataType::Float64, true));
        Ok(vec![
            list_scalar,
            ScalarValue::Float64(self.percentile),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let list_arr = &states[0];
        let pct_arr = &states[1];
        if self.percentile.is_none()
            && let Some(f64_arr) = pct_arr.as_any().downcast_ref::<Float64Array>()
        {
            for i in 0..f64_arr.len() {
                if !f64_arr.is_null(i) {
                    self.percentile = Some(f64_arr.value(i));
                    break;
                }
            }
        }
        if let Some(list_array) = list_arr.as_any().downcast_ref::<arrow_array::ListArray>() {
            for i in 0..list_array.len() {
                if list_array.is_null(i) {
                    continue;
                }
                let inner = list_array.value(i);
                if let Some(f64_arr) = inner.as_any().downcast_ref::<Float64Array>() {
                    for j in 0..f64_arr.len() {
                        if !f64_arr.is_null(j) {
                            self.values.push(f64_arr.value(j));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn create_cypher_percentile_disc_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherPercentileDiscUdaf::new())
}

pub(crate) fn create_cypher_percentile_cont_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherPercentileContUdaf::new())
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
            ctx.udf("_make_cypher_list").is_ok(),
            "_make_cypher_list UDF should be registered"
        );
        assert!(
            ctx.udf("_cv_to_bool").is_ok(),
            "_cv_to_bool UDF should be registered"
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

    // ====================================================================
    // CypherValue Decode UDF Tests
    // ====================================================================

    /// Encode a JSON value to CypherValue binary bytes.
    fn json_to_cv_bytes(val: &serde_json::Value) -> Vec<u8> {
        let uni_val: uni_common::Value = val.clone().into();
        uni_common::cypher_value_codec::encode(&uni_val)
    }

    // Note: Old CypherValue decode UDF tests removed - those UDFs no longer exist.
    // CypherValue operations now route through Cypher-semantic UDFs instead.

    // ====================================================================
    // _make_cypher_list UDF Tests
    // ====================================================================

    // ====================================================================
    // _make_cypher_list UDF Tests
    // ====================================================================

    /// Helper to create ScalarFunctionArgs from multiple scalar values.
    fn make_multi_scalar_args(scalars: Vec<ScalarValue>) -> ScalarFunctionArgs {
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let arg_fields: Vec<_> = scalars
            .iter()
            .enumerate()
            .map(|(i, s)| Arc::new(Field::new(format!("arg{i}"), s.data_type(), true)))
            .collect();
        let args: Vec<_> = scalars.into_iter().map(ColumnarValue::Scalar).collect();
        ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::LargeBinary, true)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    /// Decode a CypherValue LargeBinary scalar to a serde_json::Value.
    fn decode_cv_scalar(cv: &ColumnarValue) -> serde_json::Value {
        match cv {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
                let val = uni_common::cypher_value_codec::decode(bytes)
                    .expect("failed to decode CypherValue output");
                val.into()
            }
            other => panic!("expected LargeBinary scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_make_cypher_list_scalars() {
        let udf = create_make_cypher_list_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Float64(Some(3.21)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Null,
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        let arr = json.as_array().expect("should be array");
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0], serde_json::json!(1));
        assert_eq!(arr[1], serde_json::json!(3.21));
        assert_eq!(arr[2], serde_json::json!("hello"));
        assert_eq!(arr[3], serde_json::json!(true));
        assert!(arr[4].is_null());
    }

    #[test]
    fn test_make_cypher_list_empty() {
        let udf = create_make_cypher_list_udf();
        let args = make_multi_scalar_args(vec![]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        let arr = json.as_array().expect("should be array");
        assert!(arr.is_empty());
    }

    #[test]
    fn test_make_cypher_list_single() {
        let udf = create_make_cypher_list_udf();
        let args = make_multi_scalar_args(vec![ScalarValue::Int64(Some(42))]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        let arr = json.as_array().expect("should be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0], serde_json::json!(42));
    }

    #[test]
    fn test_make_cypher_list_nested_cypher_value() {
        let udf = create_make_cypher_list_udf();
        // Create a CypherValue-encoded nested list [1, 2]
        let nested_bytes = json_to_cv_bytes(&serde_json::json!([1, 2]));
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(nested_bytes)),
            ScalarValue::Int64(Some(3)),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        let arr = json.as_array().expect("should be array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], serde_json::json!([1, 2]));
        assert_eq!(arr[1], serde_json::json!(3));
    }

    // ====================================================================
    // _cypher_in UDF Tests
    // ====================================================================

    /// Helper: make a 2-arg ScalarFunctionArgs with CypherValue scalars for _cypher_in.
    fn make_cypher_in_args(
        element: &serde_json::Value,
        list: &serde_json::Value,
    ) -> ScalarFunctionArgs {
        make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(element))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(list))),
        ])
    }

    #[test]
    fn test_cypher_in_found() {
        let udf = create_cypher_in_udf();
        let args = make_cypher_in_args(&serde_json::json!(3), &serde_json::json!([1, 2, 3]));
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => assert!(b),
            other => panic!("expected Boolean(true), got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_not_found() {
        let udf = create_cypher_in_udf();
        let args = make_cypher_in_args(&serde_json::json!(4), &serde_json::json!([1, 2, 3]));
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => assert!(!b),
            other => panic!("expected Boolean(false), got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_null_list() {
        let udf = create_cypher_in_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(1)))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {} // null
            other => panic!("expected Boolean(None) for null list, got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_null_element_nonempty() {
        let udf = create_cypher_in_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1, 2])))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {} // null
            other => panic!("expected Boolean(None) for null IN non-empty list, got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_null_element_empty() {
        let udf = create_cypher_in_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([])))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => assert!(!b),
            other => panic!("expected Boolean(false) for null IN [], got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_not_found_with_null() {
        let udf = create_cypher_in_udf();
        let args = make_cypher_in_args(&serde_json::json!(4), &serde_json::json!([1, null, 3]));
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {} // null
            other => panic!("expected Boolean(None) for 4 IN [1,null,3], got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_cross_type_int_float() {
        let udf = create_cypher_in_udf();
        let args = make_cypher_in_args(&serde_json::json!(1), &serde_json::json!([1.0, 2.0]));
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => assert!(b),
            other => panic!("expected Boolean(true) for 1 IN [1.0, 2.0], got {other:?}"),
        }
    }

    // ====================================================================
    // _cypher_list_concat UDF Tests
    // ====================================================================

    #[test]
    fn test_list_concat_basic() {
        let udf = create_cypher_list_concat_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1, 2])))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([3, 4])))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        assert_eq!(json, serde_json::json!([1, 2, 3, 4]));
    }

    #[test]
    fn test_list_concat_empty() {
        let udf = create_cypher_list_concat_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([])))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1])))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        assert_eq!(json, serde_json::json!([1]));
    }

    #[test]
    fn test_list_concat_null_left() {
        let udf = create_cypher_list_concat_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1])))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
                let uni_val = uni_common::cypher_value_codec::decode(&bytes).expect("decode");
                let json: serde_json::Value = uni_val.into();
                assert!(json.is_null(), "expected null, got {json}");
            }
            ColumnarValue::Scalar(ScalarValue::LargeBinary(None)) => {} // Arrow null is also acceptable
            other => panic!("expected null result, got {other:?}"),
        }
    }

    #[test]
    fn test_list_concat_null_right() {
        let udf = create_cypher_list_concat_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1])))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
                let uni_val = uni_common::cypher_value_codec::decode(&bytes).expect("decode");
                let json: serde_json::Value = uni_val.into();
                assert!(json.is_null(), "expected null, got {json}");
            }
            ColumnarValue::Scalar(ScalarValue::LargeBinary(None)) => {}
            other => panic!("expected null result, got {other:?}"),
        }
    }

    // ====================================================================
    // _cypher_list_append UDF Tests
    // ====================================================================

    #[test]
    fn test_list_append_scalar() {
        let udf = create_cypher_list_append_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1, 2])))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(3)))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        assert_eq!(json, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_list_prepend_scalar() {
        let udf = create_cypher_list_append_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(3)))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1, 2])))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        let json = decode_cv_scalar(&result);
        assert_eq!(json, serde_json::json!([3, 1, 2]));
    }

    #[test]
    fn test_list_append_null_list() {
        let udf = create_cypher_list_append_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(3)))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
                let uni_val = uni_common::cypher_value_codec::decode(&bytes).expect("decode");
                let json: serde_json::Value = uni_val.into();
                assert!(json.is_null(), "expected null, got {json}");
            }
            ColumnarValue::Scalar(ScalarValue::LargeBinary(None)) => {}
            other => panic!("expected null result, got {other:?}"),
        }
    }

    #[test]
    fn test_list_append_null_scalar() {
        let udf = create_cypher_list_append_udf();
        let args = make_multi_scalar_args(vec![
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1, 2])))),
            ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
        ]);
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes))) => {
                let uni_val = uni_common::cypher_value_codec::decode(&bytes).expect("decode");
                let json: serde_json::Value = uni_val.into();
                assert!(json.is_null(), "expected null, got {json}");
            }
            ColumnarValue::Scalar(ScalarValue::LargeBinary(None)) => {}
            other => panic!("expected null result, got {other:?}"),
        }
    }
}
