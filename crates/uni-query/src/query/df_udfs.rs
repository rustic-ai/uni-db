// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

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
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, UInt64Array,
};
use chrono::Offset;
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
    ctx.register_udf(create_startnode_udf());
    ctx.register_udf(create_endnode_udf());

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

    // Duration and temporal property accessor UDFs
    ctx.register_udf(create_duration_property_udf());
    ctx.register_udf(create_temporal_property_udf());
    ctx.register_udf(create_tostring_udf());
    ctx.register_udf(create_cypher_sort_key_udf());
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

    // List concatenation, append, slice, tail, reverse, and CV-wrapping UDFs
    ctx.register_udf(create_cypher_list_concat_udf());
    ctx.register_udf(create_cypher_list_append_udf());
    ctx.register_udf(create_cypher_list_slice_udf());
    ctx.register_udf(create_cypher_tail_udf());
    ctx.register_udf(create_cypher_head_udf());
    ctx.register_udf(create_cypher_last_udf());
    ctx.register_udf(create_cypher_reverse_udf());
    ctx.register_udf(create_cypher_substring_udf());
    ctx.register_udf(create_cypher_split_udf());
    ctx.register_udf(create_cypher_list_to_cv_udf());
    ctx.register_udf(create_cypher_scalar_to_cv_udf());

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
                    // When _all_props is present, the input is a schemaless
                    // entity (node/relationship).  Per the property graph model,
                    // a null-valued property does not exist on the entity, so we
                    // must filter it out.  For plain maps (literal or parameter),
                    // null-valued keys are valid and must be included.
                    let (source, is_entity) = match map.get("_all_props") {
                        Some(Value::Map(all)) => (all, true),
                        _ => (map, false),
                    };
                    let mut key_strings: Vec<String> = source
                        .iter()
                        .filter(|(k, v)| !k.starts_with('_') && (!is_entity || !v.is_null()))
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
                    // Detect null entities from OPTIONAL MATCH: when the entity's
                    // identity field (_vid for nodes, _eid for edges) is present
                    // but null, the entire entity is null. This happens because
                    // add_structural_projection builds a named_struct from
                    // individual columns, producing a valid struct with all-null
                    // fields rather than a null struct.
                    // Note: only check when the field EXISTS — regular maps passed
                    // to properties() (e.g. properties({name: 'foo'})) won't have
                    // these fields and should proceed normally.
                    let identity_null = map
                        .get("_vid")
                        .map(|v| v.is_null())
                        .or_else(|| map.get("_eid").map(|v| v.is_null()))
                        .unwrap_or(false);
                    if identity_null {
                        return Ok(Value::Null);
                    }

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
                        } else if let Some(Value::Map(all_props)) = map.get("_all_props") {
                            // Schemaless entities store user properties under _all_props.
                            all_props.get(key).cloned().unwrap_or(Value::Null)
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
// startNode(relationship) -> Node
// ============================================================================

/// Create the `startnode` UDF for getting the start node of a relationship.
///
/// At translation time, all known node variable columns are appended as extra arguments
/// so the UDF can find the matching node by VID at runtime.
pub fn create_startnode_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(StartNodeUdf::new())
}

#[derive(Debug)]
struct StartNodeUdf {
    signature: Signature,
}

impl StartNodeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(StartNodeUdf);

impl ScalarUDFImpl for StartNodeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "startnode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = DataType::LargeBinary;
        invoke_cypher_udf(args, &output_type, |val_args| {
            startnode_endnode_impl(val_args, true)
        })
    }
}

// ============================================================================
// endNode(relationship) -> Node
// ============================================================================

/// Create the `endnode` UDF for getting the end node of a relationship.
pub fn create_endnode_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(EndNodeUdf::new())
}

#[derive(Debug)]
struct EndNodeUdf {
    signature: Signature,
}

impl EndNodeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(EndNodeUdf);

impl ScalarUDFImpl for EndNodeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "endnode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = DataType::LargeBinary;
        invoke_cypher_udf(args, &output_type, |val_args| {
            startnode_endnode_impl(val_args, false)
        })
    }
}

/// Shared implementation for startNode/endNode UDFs.
///
/// `val_args[0]` is the edge (cv_encoded), `val_args[1..]` are node variables.
/// For `is_start=true`, finds the node matching `_src_vid`; for `false`, `_dst_vid`.
fn startnode_endnode_impl(val_args: &[Value], is_start: bool) -> DFResult<Value> {
    if val_args.is_empty() {
        let fn_name = if is_start { "startNode" } else { "endNode" };
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "{fn_name}(): requires at least 1 argument"
        )));
    }

    let edge_val = &val_args[0];
    let target_vid = extract_endpoint_vid(edge_val, is_start);

    let target_vid = match target_vid {
        Some(vid) => vid,
        None => return Ok(Value::Null),
    };

    // Search node arguments (args[1..]) for a matching _vid
    for node_val in val_args.iter().skip(1) {
        if let Some(vid) = extract_vid(node_val)
            && vid == target_vid
        {
            return Ok(node_val.clone());
        }
    }

    // Fallback: return minimal node map with just _vid
    let mut map = std::collections::HashMap::new();
    map.insert("_vid".to_string(), Value::Int(target_vid as i64));
    Ok(Value::Map(map))
}

/// Extract the src or dst VID from an edge value.
fn extract_endpoint_vid(val: &Value, is_start: bool) -> Option<u64> {
    match val {
        Value::Edge(edge) => {
            let vid = if is_start { edge.src } else { edge.dst };
            Some(vid.as_u64())
        }
        Value::Map(map) => {
            // Try _src_vid / _dst_vid first
            let key = if is_start { "_src_vid" } else { "_dst_vid" };
            if let Some(v) = map.get(key) {
                return v.as_u64();
            }
            // Try _src / _dst
            let key2 = if is_start { "_src" } else { "_dst" };
            if let Some(v) = map.get(key2) {
                return v.as_u64();
            }
            // Try _startNode / _endNode (return VID from nested node)
            let node_key = if is_start { "_startNode" } else { "_endNode" };
            if let Some(node_val) = map.get(node_key) {
                return extract_vid(node_val);
            }
            None
        }
        _ => None,
    }
}

/// Extract _vid from a node value.
fn extract_vid(val: &Value) -> Option<u64> {
    match val {
        Value::Map(map) => map.get("_vid").and_then(|v| v.as_u64()),
        _ => None,
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
                "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                name
            ))),
        },
        ColumnarValue::Array(arr) => {
            // Handle single-element arrays from columnar UDF context (e.g. size() output)
            if !arr.is_empty() && !arr.is_null(0) {
                use datafusion::arrow::array::{
                    Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array,
                    UInt32Array, UInt64Array,
                };
                match arr.data_type() {
                    DataType::Int8 => {
                        Ok(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(0) as i64)
                    }
                    DataType::Int16 => {
                        Ok(arr.as_any().downcast_ref::<Int16Array>().unwrap().value(0) as i64)
                    }
                    DataType::Int32 => {
                        Ok(arr.as_any().downcast_ref::<Int32Array>().unwrap().value(0) as i64)
                    }
                    DataType::Int64 => {
                        Ok(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(0))
                    }
                    DataType::UInt8 => {
                        Ok(arr.as_any().downcast_ref::<UInt8Array>().unwrap().value(0) as i64)
                    }
                    DataType::UInt16 => {
                        Ok(arr.as_any().downcast_ref::<UInt16Array>().unwrap().value(0) as i64)
                    }
                    DataType::UInt32 => {
                        Ok(arr.as_any().downcast_ref::<UInt32Array>().unwrap().value(0) as i64)
                    }
                    DataType::UInt64 => {
                        Ok(arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(0) as i64)
                    }
                    _ => Err(datafusion::error::DataFusionError::Execution(format!(
                        "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                        name
                    ))),
                }
            } else {
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                    name
                )))
            }
        }
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

        // Generate range — return empty list when direction and step are inconsistent
        let mut values = Vec::new();
        if step > 0 && start <= end {
            let mut current = start;
            while current <= end {
                values.push(datafusion::common::ScalarValue::Int64(Some(current)));
                current += step;
            }
        } else if step < 0 && start >= end {
            let mut current = start;
            while current >= end {
                values.push(datafusion::common::ScalarValue::Int64(Some(current)));
                current += step;
            }
        }
        // else: direction and step are inconsistent → return empty list

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
            "year" | "month" | "day" | "hour" | "minute" | "second"
        ) {
            Ok(DataType::Int64)
        } else {
            match name.as_str() {
                // Temporal constructors use LargeBinary (CypherValue codec) to preserve
                // timezone names, Duration components, and nanosecond precision through
                // the DataFusion pipeline. Constant-folded calls bypass UDFs entirely.
                // duration.inMonths/inDays/inSeconds compute durations between two temporal
                // values and return Duration compound types, not plain integers.
                "datetime"
                | "localdatetime"
                | "date"
                | "time"
                | "localtime"
                | "duration"
                | "date.truncate"
                | "time.truncate"
                | "datetime.truncate"
                | "localdatetime.truncate"
                | "localtime.truncate"
                | "duration.between"
                | "duration.inmonths"
                | "duration.indays"
                | "duration.inseconds"
                | "datetime.fromepoch"
                | "datetime.fromepochmillis"
                | "datetime.transaction"
                | "datetime.statement"
                | "datetime.realtime"
                | "date.transaction"
                | "date.statement"
                | "date.realtime"
                | "time.transaction"
                | "time.statement"
                | "time.realtime"
                | "localtime.transaction"
                | "localtime.statement"
                | "localtime.realtime"
                | "localdatetime.transaction"
                | "localdatetime.statement"
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

            let dur_string_owned;
            let dur_str = match &val_args[0] {
                Value::String(s) => s.as_str(),
                Value::Temporal(uni_common::TemporalValue::Duration { .. }) => {
                    dur_string_owned = val_args[0].to_string();
                    &dur_string_owned
                }
                Value::Null => return Ok(Value::Null),
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "_duration_property(): duration must be a string or temporal duration"
                            .to_string(),
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

/// Create a UDF for `toString()` that handles temporal types.
///
/// Converts any Value to its string representation. For temporals,
/// uses the canonical Display format. For other types, uses natural formatting.
fn create_tostring_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(ToStringUdf::new())
}

#[derive(Debug)]
struct ToStringUdf {
    signature: Signature,
}

impl ToStringUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(ToStringUdf);

impl ScalarUDFImpl for ToStringUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "tostring"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.is_empty() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "toString(): requires 1 argument".to_string(),
                ));
            }
            match &val_args[0] {
                Value::Null => Ok(Value::Null),
                Value::String(s) => Ok(Value::String(s.clone())),
                Value::Int(i) => Ok(Value::String(i.to_string())),
                Value::Float(f) => Ok(Value::String(f.to_string())),
                Value::Bool(b) => Ok(Value::String(b.to_string())),
                Value::Temporal(t) => Ok(Value::String(t.to_string())),
                other => {
                    let type_name = match other {
                        Value::List(_) => "List",
                        Value::Map(_) => "Map",
                        Value::Node { .. } => "Node",
                        Value::Edge { .. } => "Relationship",
                        Value::Path { .. } => "Path",
                        _ => "Unknown",
                    };
                    Err(datafusion::error::DataFusionError::Execution(format!(
                        "TypeError: InvalidArgumentValue - toString() does not accept {} values",
                        type_name
                    )))
                }
            }
        })
    }
}

/// Create a UDF for accessing temporal component properties.
///
/// Called as `_temporal_property(temporal_value, component_name)`.
/// Returns a LargeBinary-encoded value (some accessors return strings, most return integers).
fn create_temporal_property_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(TemporalPropertyUdf::new())
}

#[derive(Debug)]
struct TemporalPropertyUdf {
    signature: Signature,
}

impl TemporalPropertyUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(TemporalPropertyUdf);

impl ScalarUDFImpl for TemporalPropertyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_temporal_property"
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
            if val_args.len() != 2 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_temporal_property(): requires 2 arguments (temporal_value, component)"
                        .to_string(),
                ));
            }

            let component = match &val_args[1] {
                Value::String(s) => s.clone(),
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "_temporal_property(): component must be a string".to_string(),
                    ));
                }
            };

            crate::query::datetime::eval_temporal_accessor_value(&val_args[0], &component).map_err(
                |e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "_temporal_property(): {}",
                        e
                    ))
                },
            )
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

/// Return the Cypher type name for a `Value`, used in error messages.
fn cypher_type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "Null",
        Value::Bool(_) => "Boolean",
        Value::Int(_) => "Integer",
        Value::Float(_) => "Float",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Node(_) => "Node",
        Value::Edge(_) => "Relationship",
        Value::Path(_) => "Path",
        Value::Vector(_) => "Vector",
        Value::Temporal(_) => "Temporal",
        _ => "Unknown",
    }
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
            // UNWIND may produce JSON-encoded binary; try plain JSON decode
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
        if matches!(output_type, DataType::LargeBinary | DataType::List(_)) {
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
        // UDF outputs are CypherValue-encoded, no schema context needed
        Ok(uni_store::storage::arrow_convert::arrow_to_value(
            arr, 0, None,
        ))
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

/// Convert a duration in microseconds to a Value::Temporal(Duration).
fn duration_micros_to_value(micros: i64) -> Value {
    let dur = crate::query::datetime::CypherDuration::from_micros(micros);
    Value::Temporal(uni_common::TemporalValue::Duration {
        months: dur.months,
        days: dur.days,
        nanos: dur.nanos,
    })
}

/// Convert a timestamp (as nanoseconds since epoch) with optional timezone to Value.
fn timestamp_nanos_to_value(nanos: i64, tz: Option<&Arc<str>>) -> DFResult<Value> {
    if let Some(tz_str) = tz {
        let offset = resolve_timezone_offset(tz_str.as_ref(), nanos);
        let tz_name = if tz_str.as_ref() == "UTC" {
            None
        } else {
            Some(tz_str.to_string())
        };
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
            // Try CypherValue decode first; UNWIND may produce JSON-encoded binary.
            if let Ok(val) = uni_common::cypher_value_codec::decode(b) {
                return Ok(val);
            }
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
        ScalarValue::Date32(Some(days)) => Ok(Value::Temporal(uni_common::TemporalValue::Date {
            days_since_epoch: *days,
        })),
        ScalarValue::Date64(Some(millis)) => {
            let days = (*millis / 86_400_000) as i32;
            Ok(Value::Temporal(uni_common::TemporalValue::Date {
                days_since_epoch: days,
            }))
        }
        ScalarValue::TimestampNanosecond(Some(nanos), tz) => {
            timestamp_nanos_to_value(*nanos, tz.as_ref())
        }
        ScalarValue::TimestampMicrosecond(Some(micros), tz) => {
            timestamp_nanos_to_value(*micros * 1_000, tz.as_ref())
        }
        ScalarValue::TimestampMillisecond(Some(millis), tz) => {
            timestamp_nanos_to_value(*millis * 1_000_000, tz.as_ref())
        }
        ScalarValue::TimestampSecond(Some(secs), tz) => {
            timestamp_nanos_to_value(*secs * 1_000_000_000, tz.as_ref())
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
        ScalarValue::DurationMicrosecond(Some(micros)) => Ok(duration_micros_to_value(*micros)),
        ScalarValue::DurationMillisecond(Some(millis)) => {
            Ok(duration_micros_to_value(*millis * 1_000))
        }
        ScalarValue::DurationSecond(Some(secs)) => Ok(duration_micros_to_value(*secs * 1_000_000)),
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
                TemporalValue::Date { days_since_epoch } => {
                    ScalarValue::Date32(Some(*days_since_epoch))
                }
                TemporalValue::LocalTime {
                    nanos_since_midnight,
                } => ScalarValue::Time64Nanosecond(Some(*nanos_since_midnight)),
                TemporalValue::Time {
                    nanos_since_midnight,
                    ..
                } => ScalarValue::Time64Nanosecond(Some(*nanos_since_midnight)),
                TemporalValue::LocalDateTime { nanos_since_epoch } => {
                    ScalarValue::TimestampNanosecond(Some(*nanos_since_epoch), None)
                }
                TemporalValue::DateTime {
                    nanos_since_epoch,
                    timezone_name,
                    ..
                } => {
                    let tz = timezone_name.as_deref().unwrap_or("UTC");
                    ScalarValue::TimestampNanosecond(Some(*nanos_since_epoch), Some(tz.into()))
                }
                TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                } => ScalarValue::IntervalMonthDayNano(Some(
                    arrow::datatypes::IntervalMonthDayNano {
                        months: *months as i32,
                        days: *days as i32,
                        nanoseconds: *nanos,
                    },
                )),
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
            match val {
                Value::Int(i) => Ok(Value::Int(*i)),
                Value::Float(f) => Ok(Value::Int(*f as i64)),
                Value::String(s) => {
                    if let Ok(i) = s.parse::<i64>() {
                        Ok(Value::Int(i))
                    } else if let Ok(f) = s.parse::<f64>() {
                        Ok(Value::Int(f as i64))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Value::Null => Ok(Value::Null),
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "InvalidArgumentValue: tointeger(): cannot convert {} to integer",
                    cypher_type_name(other)
                ))),
            }
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
            match val {
                Value::Int(i) => Ok(Value::Float(*i as f64)),
                Value::Float(f) => Ok(Value::Float(*f)),
                Value::String(s) => {
                    if let Ok(f) = s.parse::<f64>() {
                        Ok(Value::Float(f))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Value::Null => Ok(Value::Null),
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "InvalidArgumentValue: tofloat(): cannot convert {} to float",
                    cypher_type_name(other)
                ))),
            }
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
            match val {
                Value::Bool(b) => Ok(Value::Bool(*b)),
                Value::String(s) => {
                    let s_lower = s.to_lowercase();
                    if s_lower == "true" {
                        Ok(Value::Bool(true))
                    } else if s_lower == "false" {
                        Ok(Value::Bool(false))
                    } else {
                        Ok(Value::Null)
                    }
                }
                Value::Null => Ok(Value::Null),
                Value::Int(i) => Ok(Value::Bool(*i != 0)),
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "InvalidArgumentValue: toboolean(): cannot convert {} to boolean",
                    cypher_type_name(other)
                ))),
            }
        })
    }
}

// ============================================================================
// _cypher_sort_key(x) -> LargeBinary
// Order-preserving binary encoding for Cypher ORDER BY.
// Produces byte sequences where memcmp matches Cypher's orderability rules.
// ============================================================================

pub fn create_cypher_sort_key_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherSortKeyUdf::new())
}

#[derive(Debug)]
struct CypherSortKeyUdf {
    signature: Signature,
}

impl CypherSortKeyUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherSortKeyUdf);

impl ScalarUDFImpl for CypherSortKeyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_sort_key"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_sort_key(): requires 1 argument".to_string(),
            ));
        }

        let arg = &args.args[0];
        match arg {
            ColumnarValue::Scalar(s) => {
                let val = if s.is_null() {
                    Value::Null
                } else {
                    scalar_to_value(s)?
                };
                let key = encode_cypher_sort_key(&val);
                Ok(ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(key))))
            }
            ColumnarValue::Array(arr) => {
                let mut keys: Vec<Option<Vec<u8>>> = Vec::with_capacity(arr.len());
                for i in 0..arr.len() {
                    let val = if arr.is_null(i) {
                        Value::Null
                    } else {
                        get_value_from_array(arr, i)?
                    };
                    keys.push(Some(encode_cypher_sort_key(&val)));
                }
                let array = LargeBinaryArray::from(
                    keys.iter()
                        .map(|k| k.as_deref())
                        .collect::<Vec<Option<&[u8]>>>(),
                );
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
        }
    }
}

/// Encode a Cypher value into an order-preserving binary sort key.
///
/// The resulting byte sequence has the property that lexicographic (memcmp)
/// comparison of two keys produces the same ordering as Cypher's ORDER BY
/// semantics, including cross-type ordering and within-type comparisons.
pub fn encode_cypher_sort_key(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    encode_sort_key_to_buf(value, &mut buf);
    buf
}

/// Recursive sort key encoder.
fn encode_sort_key_to_buf(value: &Value, buf: &mut Vec<u8>) {
    // Check for map-encoded temporals, nodes, edges, paths first
    if let Value::Map(map) = value {
        if let Some(tv) = sort_key_map_as_temporal(map) {
            buf.push(0x07); // Temporal rank
            encode_temporal_payload(&tv, buf);
            return;
        }
        let rank = sort_key_map_rank(map);
        if rank != 0 {
            // Node, Edge, or Path encoded as map
            buf.push(rank);
            match rank {
                0x01 => encode_map_as_node_payload(map, buf),
                0x02 => encode_map_as_edge_payload(map, buf),
                0x04 => encode_map_as_path_payload(map, buf),
                _ => {} // shouldn't happen
            }
            return;
        }
    }

    // Check for temporal strings
    if let Value::String(s) = value {
        if let Some(tv) = sort_key_string_as_temporal(s) {
            buf.push(0x07); // Temporal rank
            encode_temporal_payload(&tv, buf);
            return;
        }
        // Wide temporal: out-of-range dates that eval_datetime_function couldn't fit in i64 nanos.
        // Parse directly with chrono and encode with i128 nanos for correct ordering.
        if let Some(temporal_type) = crate::query::datetime::classify_temporal(s) {
            buf.push(0x07); // Temporal rank
            if encode_wide_temporal_sort_key(s, temporal_type, buf) {
                return;
            }
            // If wide parse failed, remove the temporal rank byte we just pushed
            buf.pop();
        }
    }

    let rank = sort_key_type_rank(value);
    buf.push(rank);

    match value {
        Value::Null => {}                   // rank byte 0x0A is sufficient
        Value::Float(f) if f.is_nan() => {} // rank byte 0x09 is sufficient
        Value::Bool(b) => buf.push(if *b { 0x01 } else { 0x00 }),
        Value::Int(i) => {
            let f = *i as f64;
            buf.extend_from_slice(&encode_order_preserving_f64(f));
        }
        Value::Float(f) => {
            buf.extend_from_slice(&encode_order_preserving_f64(*f));
        }
        Value::String(s) => {
            byte_stuff_terminate(s.as_bytes(), buf);
        }
        Value::Temporal(tv) => {
            encode_temporal_payload(tv, buf);
        }
        Value::List(items) => {
            encode_list_payload(items, buf);
        }
        Value::Map(map) => {
            encode_map_payload(map, buf);
        }
        Value::Node(node) => {
            encode_node_payload(node, buf);
        }
        Value::Edge(edge) => {
            encode_edge_payload(edge, buf);
        }
        Value::Path(path) => {
            encode_path_payload(path, buf);
        }
        // Bytes and Vector get rank 0x0B - just encode raw bytes
        Value::Bytes(b) => {
            byte_stuff_terminate(b, buf);
        }
        Value::Vector(v) => {
            for f in v {
                buf.extend_from_slice(&encode_order_preserving_f64(*f as f64));
            }
        }
        _ => {} // Future variants: rank byte is sufficient
    }
}

/// Type rank for sort key encoding.
///
/// Matches the fallback executor's `order_by_type_rank` at core.rs:401.
fn sort_key_type_rank(v: &Value) -> u8 {
    match v {
        Value::Map(map) => sort_key_map_rank(map),
        Value::Node(_) => 0x01,
        Value::Edge(_) => 0x02,
        Value::List(_) => 0x03,
        Value::Path(_) => 0x04,
        Value::String(_) => 0x05,
        Value::Bool(_) => 0x06,
        Value::Temporal(_) => 0x07,
        Value::Int(_) => 0x08,
        Value::Float(f) if f.is_nan() => 0x09,
        Value::Float(_) => 0x08,
        Value::Null => 0x0A,
        Value::Bytes(_) | Value::Vector(_) => 0x0B,
        _ => 0x0B, // Future variants
    }
}

/// Rank maps that represent other types (mirrors `map_order_rank` from core.rs:420).
fn sort_key_map_rank(map: &std::collections::HashMap<String, Value>) -> u8 {
    if sort_key_map_as_temporal(map).is_some() {
        0x07
    } else if map.contains_key("nodes")
        && (map.contains_key("relationships") || map.contains_key("edges"))
    {
        0x04 // Path
    } else if map.contains_key("_eid")
        || map.contains_key("_src")
        || map.contains_key("_dst")
        || map.contains_key("_type")
        || map.contains_key("_type_name")
    {
        0x02 // Edge
    } else if map.contains_key("_vid") || map.contains_key("_labels") || map.contains_key("_label")
    {
        0x01 // Node
    } else {
        0x00 // Regular map
    }
}

/// Try to interpret a map as a temporal value (mirrors `map_as_temporal` from core.rs:468).
fn sort_key_map_as_temporal(
    map: &std::collections::HashMap<String, Value>,
) -> Option<uni_common::TemporalValue> {
    if map.len() != 1 {
        return None;
    }

    let as_i32 = |v: &Value| v.as_i64().and_then(|n| i32::try_from(n).ok());
    let as_i64 = |v: &Value| v.as_i64();

    if let Some(Value::Map(inner)) = map.get("Date") {
        let days = inner.get("days_since_epoch").and_then(as_i32)?;
        return Some(uni_common::TemporalValue::Date {
            days_since_epoch: days,
        });
    }
    if let Some(Value::Map(inner)) = map.get("LocalTime") {
        let nanos = inner.get("nanos_since_midnight").and_then(as_i64)?;
        return Some(uni_common::TemporalValue::LocalTime {
            nanos_since_midnight: nanos,
        });
    }
    if let Some(Value::Map(inner)) = map.get("Time") {
        let nanos = inner.get("nanos_since_midnight").and_then(as_i64)?;
        let offset = inner.get("offset_seconds").and_then(as_i32)?;
        return Some(uni_common::TemporalValue::Time {
            nanos_since_midnight: nanos,
            offset_seconds: offset,
        });
    }
    if let Some(Value::Map(inner)) = map.get("LocalDateTime") {
        let nanos = inner.get("nanos_since_epoch").and_then(as_i64)?;
        return Some(uni_common::TemporalValue::LocalDateTime {
            nanos_since_epoch: nanos,
        });
    }
    if let Some(Value::Map(inner)) = map.get("DateTime") {
        let nanos = inner.get("nanos_since_epoch").and_then(as_i64)?;
        let offset = inner.get("offset_seconds").and_then(as_i32)?;
        let timezone_name = match inner.get("timezone_name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        };
        return Some(uni_common::TemporalValue::DateTime {
            nanos_since_epoch: nanos,
            offset_seconds: offset,
            timezone_name,
        });
    }
    if let Some(Value::Map(inner)) = map.get("Duration") {
        let months = inner.get("months").and_then(as_i64)?;
        let days = inner.get("days").and_then(as_i64)?;
        let nanos = inner.get("nanos").and_then(as_i64)?;
        return Some(uni_common::TemporalValue::Duration {
            months,
            days,
            nanos,
        });
    }
    None
}

/// Try to parse a string as a temporal value (mirrors `string_as_temporal` from core.rs:453).
fn sort_key_string_as_temporal(s: &str) -> Option<uni_common::TemporalValue> {
    use crate::query::datetime::{classify_temporal, eval_datetime_function};

    let fn_name = match classify_temporal(s)? {
        uni_common::TemporalType::Date => "DATE",
        uni_common::TemporalType::LocalTime => "LOCALTIME",
        uni_common::TemporalType::Time => "TIME",
        uni_common::TemporalType::LocalDateTime => "LOCALDATETIME",
        uni_common::TemporalType::DateTime => "DATETIME",
        uni_common::TemporalType::Duration => "DURATION",
    };
    match eval_datetime_function(fn_name, &[Value::String(s.to_string())]).ok()? {
        Value::Temporal(tv) => Some(tv),
        _ => None,
    }
}

/// Encode a wide (out-of-range) temporal sort key directly from a formatted string.
///
/// When `eval_datetime_function` returns `Value::String` because the nanos don't fit in i64,
/// we parse the formatted string directly with chrono and encode the sort key using i128 nanos.
/// This is called from `encode_sort_key_to_buf` as a fallback when `sort_key_string_as_temporal`
/// returns None but `classify_temporal` recognizes the string.
fn encode_wide_temporal_sort_key(
    s: &str,
    temporal_type: uni_common::TemporalType,
    buf: &mut Vec<u8>,
) -> bool {
    match temporal_type {
        uni_common::TemporalType::LocalDateTime => {
            if let Some(ndt) = parse_naive_datetime(s) {
                buf.push(0x03); // LocalDateTime variant
                let wide_nanos = naive_datetime_to_wide_nanos(&ndt);
                buf.extend_from_slice(&encode_order_preserving_i128(wide_nanos));
                return true;
            }
            false
        }
        uni_common::TemporalType::DateTime => {
            // Strip optional [timezone] suffix
            let base = if let Some(bracket_pos) = s.find('[') {
                &s[..bracket_pos]
            } else {
                s
            };
            if let Ok(dt) = chrono::DateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S%.f%:z") {
                buf.push(0x04); // DateTime variant
                let utc = dt.naive_utc();
                let wide_nanos = naive_datetime_to_wide_nanos(&utc);
                buf.extend_from_slice(&encode_order_preserving_i128(wide_nanos));
                return true;
            }
            if let Ok(dt) = chrono::DateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S%:z") {
                buf.push(0x04); // DateTime variant
                let utc = dt.naive_utc();
                let wide_nanos = naive_datetime_to_wide_nanos(&utc);
                buf.extend_from_slice(&encode_order_preserving_i128(wide_nanos));
                return true;
            }
            false
        }
        uni_common::TemporalType::Date => {
            if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                && let Some(epoch) = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            {
                buf.push(0x00); // Date variant
                let days = nd.signed_duration_since(epoch).num_days() as i32;
                buf.extend_from_slice(&encode_order_preserving_i32(days));
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Parse a naive datetime string in ISO format.
fn parse_naive_datetime(s: &str) -> Option<chrono::NaiveDateTime> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
}

/// Compute nanoseconds since Unix epoch as i128 for a NaiveDateTime.
/// This handles dates outside the i64 nanos range (~1677-2262).
fn naive_datetime_to_wide_nanos(ndt: &chrono::NaiveDateTime) -> i128 {
    let secs = ndt.and_utc().timestamp() as i128;
    let subsec_nanos = ndt.and_utc().timestamp_subsec_nanos() as i128;
    secs * 1_000_000_000 + subsec_nanos
}

/// Encode a map that looks like a node into the node sort key payload.
fn encode_map_as_node_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    // Extract labels
    let mut labels: Vec<String> = Vec::new();
    if let Some(Value::List(lbls)) = map.get("_labels") {
        for l in lbls {
            if let Value::String(s) = l {
                labels.push(s.clone());
            }
        }
    } else if let Some(Value::String(lbl)) = map.get("_label") {
        labels.push(lbl.clone());
    }
    labels.sort();

    // Extract vid
    let vid = map.get("_vid").and_then(|v| v.as_i64()).unwrap_or(0) as u64;

    // Labels
    let labels_joined = labels.join("\x01");
    byte_stuff_terminate(labels_joined.as_bytes(), buf);

    // VID
    buf.extend_from_slice(&vid.to_be_bytes());

    // Properties (all keys except internal ones)
    let mut props: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for (k, v) in map {
        if !k.starts_with('_') {
            props.insert(k.clone(), v.clone());
        }
    }
    encode_map_payload(&props, buf);
}

/// Encode a map that looks like an edge into the edge sort key payload.
fn encode_map_as_edge_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    let edge_type = map
        .get("_type")
        .or_else(|| map.get("_type_name"))
        .and_then(|v| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .unwrap_or("");

    byte_stuff_terminate(edge_type.as_bytes(), buf);

    let src = map.get("_src").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
    let dst = map.get("_dst").and_then(|v| v.as_i64()).unwrap_or(0) as u64;
    let eid = map.get("_eid").and_then(|v| v.as_i64()).unwrap_or(0) as u64;

    buf.extend_from_slice(&src.to_be_bytes());
    buf.extend_from_slice(&dst.to_be_bytes());
    buf.extend_from_slice(&eid.to_be_bytes());

    // Properties (all keys except internal ones)
    let mut props: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for (k, v) in map {
        if !k.starts_with('_') {
            props.insert(k.clone(), v.clone());
        }
    }
    encode_map_payload(&props, buf);
}

/// Encode a map that looks like a path into the path sort key payload.
fn encode_map_as_path_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    // Nodes
    if let Some(Value::List(nodes)) = map.get("nodes") {
        encode_list_payload(nodes, buf);
    } else {
        buf.push(0x00); // empty list terminator
    }
    // Edges/relationships
    let edges = map.get("relationships").or_else(|| map.get("edges"));
    if let Some(Value::List(edges)) = edges {
        encode_list_payload(edges, buf);
    } else {
        buf.push(0x00); // empty list terminator
    }
}

// ─── Encoding helpers ───────────────────────────────────────────────────

/// Order-preserving encoding of f64.
///
/// Transforms IEEE 754 bit pattern so that memcmp gives the correct
/// numeric order: -inf < negatives < -0 = +0 < positives < +inf < NaN.
fn encode_order_preserving_f64(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    let encoded = if bits >> 63 == 1 {
        // Negative: flip all bits
        !bits
    } else {
        // Non-negative: flip sign bit only
        bits ^ (1u64 << 63)
    };
    encoded.to_be_bytes()
}

/// Order-preserving encoding of i64.
fn encode_order_preserving_i64(i: i64) -> [u8; 8] {
    // XOR with sign bit to flip ordering
    ((i as u64) ^ (1u64 << 63)).to_be_bytes()
}

/// Order-preserving encoding of i32.
fn encode_order_preserving_i32(i: i32) -> [u8; 4] {
    ((i as u32) ^ (1u32 << 31)).to_be_bytes()
}

/// Order-preserving encoding of i128.
fn encode_order_preserving_i128(i: i128) -> [u8; 16] {
    ((i as u128) ^ (1u128 << 127)).to_be_bytes()
}

/// Byte-stuff and terminate: every 0x00 in data becomes 0x00 0xFF,
/// then append 0x00 0x00 as terminator.
///
/// This preserves lexicographic order because 0x00 0xFF > 0x00 0x00.
fn byte_stuff_terminate(data: &[u8], buf: &mut Vec<u8>) {
    byte_stuff(data, buf);
    buf.push(0x00);
    buf.push(0x00);
}

/// Byte-stuff without terminator.
fn byte_stuff(data: &[u8], buf: &mut Vec<u8>) {
    for &b in data {
        buf.push(b);
        if b == 0x00 {
            buf.push(0xFF);
        }
    }
}

/// Encode a list payload: each element wrapped, then end marker.
///
/// Format: `[0x01, stuffed(encode(elem)), 0x00, 0x00]...` then `0x00` end marker.
/// Shorter list < longer list because 0x00 (end) < 0x01 (more elements).
fn encode_list_payload(items: &[Value], buf: &mut Vec<u8>) {
    for item in items {
        buf.push(0x01); // element marker
        let elem_key = encode_cypher_sort_key(item);
        byte_stuff_terminate(&elem_key, buf);
    }
    buf.push(0x00); // end marker
}

/// Encode a map payload: entries sorted by key, then end marker.
fn encode_map_payload(map: &std::collections::HashMap<String, Value>, buf: &mut Vec<u8>) {
    let mut pairs: Vec<(&String, &Value)> = map.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);

    for (key, value) in pairs {
        buf.push(0x01); // entry marker
        byte_stuff_terminate(key.as_bytes(), buf);
        let val_key = encode_cypher_sort_key(value);
        byte_stuff_terminate(&val_key, buf);
    }
    buf.push(0x00); // end marker
}

/// Encode node sort key payload.
///
/// Format: `stuffed(sorted_labels_joined_by_\x01), 0x00 0x00, vid_be, map_payload`
fn encode_node_payload(node: &uni_common::Node, buf: &mut Vec<u8>) {
    let mut labels = node.labels.clone();
    labels.sort();
    let labels_joined = labels.join("\x01");
    byte_stuff_terminate(labels_joined.as_bytes(), buf);

    buf.extend_from_slice(&node.vid.as_u64().to_be_bytes());

    encode_map_payload(&node.properties, buf);
}

/// Encode edge sort key payload.
///
/// Format: `stuffed(edge_type), 0x00 0x00, src_be, dst_be, eid_be, map_payload`
fn encode_edge_payload(edge: &uni_common::Edge, buf: &mut Vec<u8>) {
    byte_stuff_terminate(edge.edge_type.as_bytes(), buf);

    buf.extend_from_slice(&edge.src.as_u64().to_be_bytes());
    buf.extend_from_slice(&edge.dst.as_u64().to_be_bytes());
    buf.extend_from_slice(&edge.eid.as_u64().to_be_bytes());

    encode_map_payload(&edge.properties, buf);
}

/// Encode path sort key payload.
///
/// Nodes encoded as list of node sort keys, edges encoded as list of edge sort keys.
fn encode_path_payload(path: &uni_common::Path, buf: &mut Vec<u8>) {
    // Nodes as list
    for node in &path.nodes {
        buf.push(0x01); // element marker
        let mut node_key = Vec::new();
        node_key.push(0x01); // Node rank
        encode_node_payload(node, &mut node_key);
        byte_stuff_terminate(&node_key, buf);
    }
    buf.push(0x00); // end nodes list

    // Edges as list
    for edge in &path.edges {
        buf.push(0x01); // element marker
        let mut edge_key = Vec::new();
        edge_key.push(0x02); // Edge rank
        encode_edge_payload(edge, &mut edge_key);
        byte_stuff_terminate(&edge_key, buf);
    }
    buf.push(0x00); // end edges list
}

/// Encode temporal value payload.
fn encode_temporal_payload(tv: &uni_common::TemporalValue, buf: &mut Vec<u8>) {
    match tv {
        uni_common::TemporalValue::Date { days_since_epoch } => {
            buf.push(0x00); // variant rank: Date
            buf.extend_from_slice(&encode_order_preserving_i32(*days_since_epoch));
        }
        uni_common::TemporalValue::LocalTime {
            nanos_since_midnight,
        } => {
            buf.push(0x01); // variant rank: LocalTime
            buf.extend_from_slice(&encode_order_preserving_i64(*nanos_since_midnight));
        }
        uni_common::TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        } => {
            buf.push(0x02); // variant rank: Time
            let utc_nanos =
                *nanos_since_midnight as i128 - (*offset_seconds as i128) * 1_000_000_000;
            buf.extend_from_slice(&encode_order_preserving_i128(utc_nanos));
        }
        uni_common::TemporalValue::LocalDateTime { nanos_since_epoch } => {
            buf.push(0x03); // variant rank: LocalDateTime
            // Use i128 for consistent width with wide (out-of-range) temporal sort keys
            buf.extend_from_slice(&encode_order_preserving_i128(*nanos_since_epoch as i128));
        }
        uni_common::TemporalValue::DateTime {
            nanos_since_epoch, ..
        } => {
            buf.push(0x04); // variant rank: DateTime
            // Use i128 for consistent width with wide (out-of-range) temporal sort keys
            buf.extend_from_slice(&encode_order_preserving_i128(*nanos_since_epoch as i128));
        }
        uni_common::TemporalValue::Duration {
            months,
            days,
            nanos,
        } => {
            buf.push(0x05); // variant rank: Duration
            buf.extend_from_slice(&encode_order_preserving_i64(*months));
            buf.extend_from_slice(&encode_order_preserving_i64(*days));
            buf.extend_from_slice(&encode_order_preserving_i64(*nanos));
        }
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
#[expect(clippy::match_like_matches_macro)]
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
                        // Complex types (lists, maps, temporals, nodes, edges, etc.)
                        // can't be compared in the fast path — fall back to slow path
                        // which fully decodes CypherValue to Value for comparison.
                        return None;
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
                match &uni_val {
                    uni_common::Value::Node(_) => {
                        Err(datafusion::error::DataFusionError::Execution(
                            "TypeError: InvalidArgumentValue - length() is not supported for Node values".to_string(),
                        ))
                    }
                    uni_common::Value::Edge(_) => {
                        Err(datafusion::error::DataFusionError::Execution(
                            "TypeError: InvalidArgumentValue - length() is not supported for Relationship values".to_string(),
                        ))
                    }
                    _ => {
                        let json_val: serde_json::Value = uni_val.into();
                        match json_val {
                            serde_json::Value::Array(arr) => Ok(ScalarValue::Int64(Some(arr.len() as i64))),
                            serde_json::Value::String(s) => {
                                Ok(ScalarValue::Int64(Some(s.chars().count() as i64)))
                            }
                            serde_json::Value::Object(m) => Ok(ScalarValue::Int64(Some(m.len() as i64))),
                            _ => Ok(ScalarValue::Int64(None)),
                        }
                    }
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
                let schema = arr.fields();
                let field_names: Vec<&str> = schema.iter().map(|f| f.name().as_str()).collect();
                // Check if this is a node struct (_vid field present, no "relationships" field)
                if field_names.contains(&"_vid") && !field_names.contains(&"relationships") {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "TypeError: InvalidArgumentValue - length() is not supported for Node values".to_string(),
                    ));
                }
                // Check if this is an edge struct (_eid or _src/_dst fields)
                if field_names.contains(&"_eid")
                    || (field_names.contains(&"_src") && field_names.contains(&"_dst"))
                {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "TypeError: InvalidArgumentValue - length() is not supported for Relationship values".to_string(),
                    ));
                }
                // Path struct: has "relationships" field
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
// _cypher_substring(str, start [, length]) -> Utf8
// ============================================================================

/// Create the `_cypher_substring` UDF for Cypher `substring()`.
///
/// Uses 0-based indexing (Cypher convention):
/// - `substring("hello", 1)` → `"ello"`
/// - `substring("hello", 1, 3)` → `"ell"`
/// - Any null argument → `null`
pub fn create_cypher_substring_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherSubstringUdf::new())
}

#[derive(Debug)]
struct CypherSubstringUdf {
    signature: Signature,
}

impl CypherSubstringUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherSubstringUdf);

impl ScalarUDFImpl for CypherSubstringUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_substring"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        invoke_cypher_udf(args, &DataType::Utf8, |vals| {
            if vals.len() < 2 || vals.len() > 3 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "_cypher_substring(): requires 2 or 3 arguments".to_string(),
                ));
            }
            // Null propagation
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let s = match &vals[0] {
                Value::String(s) => s.as_str(),
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_substring(): first argument must be a string, got {:?}",
                        other
                    )));
                }
            };
            let start = match &vals[1] {
                Value::Int(i) => *i,
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_substring(): second argument must be an integer, got {:?}",
                        other
                    )));
                }
            };

            // Cypher substring is 0-based, operates on characters (not bytes)
            let chars: Vec<char> = s.chars().collect();
            let len = chars.len() as i64;

            // Clamp start to valid range
            let start_idx = start.max(0).min(len) as usize;

            let end_idx = if vals.len() == 3 {
                let length = match &vals[2] {
                    Value::Int(i) => *i,
                    other => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "_cypher_substring(): third argument must be an integer, got {:?}",
                            other
                        )));
                    }
                };
                if length < 0 {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "ArgumentError: NegativeIntegerArgument - substring length must be non-negative".to_string(),
                    ));
                }
                (start_idx as i64 + length).min(len) as usize
            } else {
                len as usize
            };

            Ok(Value::String(chars[start_idx..end_idx].iter().collect()))
        })
    }
}

// ============================================================================
// _cypher_split(str, delimiter) -> LargeBinary (CypherValue list of strings)
// ============================================================================

/// Create the `_cypher_split` UDF for Cypher `split()`.
///
/// - `split("one,two", ",")` → `["one", "two"]`
/// - `split(null, ",")` → `null`
pub fn create_cypher_split_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherSplitUdf::new())
}

#[derive(Debug)]
struct CypherSplitUdf {
    signature: Signature,
}

impl CypherSplitUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherSplitUdf);

impl ScalarUDFImpl for CypherSplitUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_split"
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
                    "_cypher_split(): requires exactly 2 arguments".to_string(),
                ));
            }
            // Null propagation
            if vals.iter().any(|v| v.is_null()) {
                return Ok(Value::Null);
            }
            let s = match &vals[0] {
                Value::String(s) => s.clone(),
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_split(): first argument must be a string, got {:?}",
                        other
                    )));
                }
            };
            let delimiter = match &vals[1] {
                Value::String(d) => d.clone(),
                other => {
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "_cypher_split(): second argument must be a string, got {:?}",
                        other
                    )));
                }
            };
            let parts: Vec<Value> = s
                .split(&delimiter)
                .map(|p| Value::String(p.to_string()))
                .collect();
            Ok(Value::List(parts))
        })
    }
}

// ============================================================================
// _cypher_list_to_cv(list) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_list_to_cv` UDF.
///
/// Wraps a native Arrow `List<T>` or `LargeList<T>` column as a `LargeBinary`
/// CypherValue. Used by CASE/coalesce type coercion when branches have mixed
/// `LargeList<T>` and `LargeBinary` types — since Arrow cannot cast between
/// those types natively, we route through this UDF instead.
pub fn create_cypher_list_to_cv_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherListToCvUdf::new())
}

#[derive(Debug)]
struct CypherListToCvUdf {
    signature: Signature,
}

impl CypherListToCvUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherListToCvUdf);

impl ScalarUDFImpl for CypherListToCvUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_list_to_cv"
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
                    "_cypher_list_to_cv(): requires exactly 1 argument".to_string(),
                ));
            }
            Ok(vals[0].clone())
        })
    }
}

// ============================================================================
// _cypher_scalar_to_cv(scalar) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_scalar_to_cv` UDF.
///
/// Converts a native scalar column (Int64, Float64, Utf8, Boolean, etc.) to
/// CypherValue-encoded LargeBinary. Used when coalesce has mixed native +
/// LargeBinary args so all branches can be normalized to LargeBinary.
/// SQL NULLs are preserved as SQL NULLs (not encoded as CypherValue::Null).
pub fn create_cypher_scalar_to_cv_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherScalarToCvUdf::new())
}

#[derive(Debug)]
struct CypherScalarToCvUdf {
    signature: Signature,
}

impl CypherScalarToCvUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherScalarToCvUdf);

impl ScalarUDFImpl for CypherScalarToCvUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "_cypher_scalar_to_cv"
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
                    "_cypher_scalar_to_cv(): requires exactly 1 argument".to_string(),
                ));
            }
            Ok(vals[0].clone())
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

// ============================================================================
// _cypher_head(list) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_head` UDF for Cypher `head()`.
///
/// Returns the first element of a list. Handles LargeBinary-encoded lists.
/// - `head([1,2,3])` → `1`
/// - `head([])` → `null`
/// - `head(null)` → `null`
pub fn create_cypher_head_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherHeadUdf::new())
}

#[derive(Debug)]
struct CypherHeadUdf {
    signature: Signature,
}

impl CypherHeadUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherHeadUdf);

impl ScalarUDFImpl for CypherHeadUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "head"
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
                    "head(): requires exactly 1 argument".to_string(),
                ));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                Value::List(l) => Ok(l.first().cloned().unwrap_or(Value::Null)),
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "head(): expected list, got {:?}",
                    other
                ))),
            }
        })
    }
}

// ============================================================================
// _cypher_last(list) -> LargeBinary (CypherValue)
// ============================================================================

/// Create the `_cypher_last` UDF for Cypher `last()`.
///
/// Returns the last element of a list. Handles LargeBinary-encoded lists.
/// - `last([1,2,3])` → `3`
/// - `last([])` → `null`
/// - `last(null)` → `null`
pub fn create_cypher_last_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherLastUdf::new())
}

#[derive(Debug)]
struct CypherLastUdf {
    signature: Signature,
}

impl CypherLastUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherLastUdf);

impl ScalarUDFImpl for CypherLastUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last"
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
                    "last(): requires exactly 1 argument".to_string(),
                ));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                Value::List(l) => Ok(l.last().cloned().unwrap_or(Value::Null)),
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "last(): expected list, got {:?}",
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
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
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
pub(crate) fn cypher_to_float64_expr(
    arg: datafusion::logical_expr::Expr,
) -> datafusion::logical_expr::Expr {
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
        (Value::Int(l), Value::Float(r)) => (*l as f64).partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Float(l), Value::Int(r)) => l.partial_cmp(&(*r as f64)).unwrap_or(Ordering::Equal),
        (Value::String(l), Value::String(r)) => l.cmp(r),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        (Value::List(l), Value::List(r)) => cypher_list_cmp(l, r).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    }
}

/// Decode a LargeBinary scalar into a Value.
fn scalar_binary_to_value(bytes: &[u8]) -> Value {
    uni_common::cypher_value_codec::decode(bytes).unwrap_or(Value::Null)
}

use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, AggregateUDFImpl};

/// Custom UDAF for Cypher-aware min/max on LargeBinary columns.
#[derive(Debug, Clone)]
struct CypherMinMaxUdaf {
    name: String,
    signature: Signature,
    is_max: bool,
}

impl CypherMinMaxUdaf {
    fn new(is_max: bool) -> Self {
        let name = if is_max { "_cypher_max" } else { "_cypher_min" };
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
    fn accumulator(
        &self,
        acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherMinMaxAccumulator {
            current: None,
            is_max: self.is_max,
            return_type: acc_args.return_field.data_type().clone(),
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
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
    return_type: DataType,
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
                    let sv = ScalarValue::try_from_array(arr, i).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(e.to_string())
                    })?;
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
            None => {
                // Return null of the declared return type
                ScalarValue::try_from(&self.return_type).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(e.to_string())
                })
            }
            Some(val) => {
                // For LargeBinary return type, encode as CypherValue bytes
                if matches!(self.return_type, DataType::LargeBinary) {
                    let bytes = uni_common::cypher_value_codec::encode(val);
                    return Ok(ScalarValue::LargeBinary(Some(bytes)));
                }
                // For concrete types, convert the Value to the matching ScalarValue
                match val {
                    Value::Int(i) => match &self.return_type {
                        DataType::Int64 => Ok(ScalarValue::Int64(Some(*i))),
                        DataType::UInt64 => Ok(ScalarValue::UInt64(Some(*i as u64))),
                        _ => {
                            let bytes = uni_common::cypher_value_codec::encode(val);
                            Ok(ScalarValue::LargeBinary(Some(bytes)))
                        }
                    },
                    Value::Float(f) => match &self.return_type {
                        DataType::Float64 => Ok(ScalarValue::Float64(Some(*f))),
                        _ => {
                            let bytes = uni_common::cypher_value_codec::encode(val);
                            Ok(ScalarValue::LargeBinary(Some(bytes)))
                        }
                    },
                    Value::String(s) => match &self.return_type {
                        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(s.clone()))),
                        DataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(Some(s.clone()))),
                        _ => {
                            let bytes = uni_common::cypher_value_codec::encode(val);
                            Ok(ScalarValue::LargeBinary(Some(bytes)))
                        }
                    },
                    Value::Bool(b) => match &self.return_type {
                        DataType::Boolean => Ok(ScalarValue::Boolean(Some(*b))),
                        _ => {
                            let bytes = uni_common::cypher_value_codec::encode(val);
                            Ok(ScalarValue::LargeBinary(Some(bytes)))
                        }
                    },
                    _ => {
                        // For complex types (List, Map, etc.), always encode as CypherValue
                        let bytes = uni_common::cypher_value_codec::encode(val);
                        Ok(ScalarValue::LargeBinary(Some(bytes)))
                    }
                }
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
    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherSumAccumulator {
            sum: 0.0,
            all_ints: true,
            int_sum: 0i64,
            has_value: false,
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
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
                    use uni_common::cypher_value_codec::{
                        TAG_FLOAT, TAG_INT, decode_float, decode_int, peek_tag,
                    };
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
    fn accumulator(
        &self,
        acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherCollectAccumulator {
            values: Vec::new(),
            distinct: acc_args.is_distinct,
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
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
pub(crate) fn create_cypher_collect_expr(
    arg: datafusion::logical_expr::Expr,
    distinct: bool,
) -> datafusion::logical_expr::Expr {
    // We use the UDAF's call() but need to set distinct separately.
    // For now, always include arg directly - distinct is handled in the accumulator.
    let udaf = Arc::new(create_cypher_collect_udaf());
    if distinct {
        // Create with distinct flag set
        datafusion::logical_expr::Expr::AggregateFunction(
            datafusion::logical_expr::expr::AggregateFunction::new_udf(
                udaf,
                vec![arg],
                true, // distinct
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
    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherPercentileDiscAccumulator {
            values: Vec::new(),
            percentile: None,
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_values", args.name),
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                ))),
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
            if self.percentile.is_none()
                && let Some(p) = Self::extract_percentile(pct_arr, i)
            {
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
        self.values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
        let list_values: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|f| ScalarValue::Float64(Some(*f)))
            .collect();
        let list_scalar = ScalarValue::List(ScalarValue::new_list(
            &list_values,
            &DataType::Float64,
            true,
        ));
        Ok(vec![list_scalar, ScalarValue::Float64(self.percentile)])
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
    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(CypherPercentileContAccumulator {
            values: Vec::new(),
            percentile: None,
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![
            Arc::new(arrow::datatypes::Field::new(
                format!("{}_values", args.name),
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                ))),
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
            if self.percentile.is_none()
                && let Some(p) = CypherPercentileDiscAccumulator::extract_percentile(pct_arr, i)
            {
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
        self.values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
        let list_values: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|f| ScalarValue::Float64(Some(*f)))
            .collect();
        let list_scalar = ScalarValue::List(ScalarValue::new_list(
            &list_values,
            &DataType::Float64,
            true,
        ));
        Ok(vec![list_scalar, ScalarValue::Float64(self.percentile)])
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

    // ====================================================================
    // _cypher_sort_key UDF Tests
    // ====================================================================

    #[test]
    fn test_sort_key_cross_type_ordering() {
        // Cypher ORDER BY type precedence (ascending):
        // Map < Node < Edge < List < Path < String < Bool < Temporal < Number < NaN < Null
        use uni_common::core::id::{Eid, Vid};
        use uni_common::{Edge, Node, Path, TemporalValue, Value};

        let map_val = Value::Map([("a".to_string(), Value::String("map".to_string()))].into());
        let node_val = Value::Node(Node {
            vid: Vid::new(1),
            labels: vec!["L".to_string()],
            properties: Default::default(),
        });
        let edge_val = Value::Edge(Edge {
            eid: Eid::new(1),
            edge_type: "T".to_string(),
            src: Vid::new(1),
            dst: Vid::new(2),
            properties: Default::default(),
        });
        let list_val = Value::List(vec![Value::Int(1)]);
        let path_val = Value::Path(Path {
            nodes: vec![Node {
                vid: Vid::new(1),
                labels: vec!["L".to_string()],
                properties: Default::default(),
            }],
            edges: vec![],
        });
        let string_val = Value::String("hello".to_string());
        let bool_val = Value::Bool(false);
        let temporal_val = Value::Temporal(TemporalValue::Date {
            days_since_epoch: 1000,
        });
        let number_val = Value::Int(42);
        let nan_val = Value::Float(f64::NAN);
        let null_val = Value::Null;

        let values = vec![
            &map_val,
            &node_val,
            &edge_val,
            &list_val,
            &path_val,
            &string_val,
            &bool_val,
            &temporal_val,
            &number_val,
            &nan_val,
            &null_val,
        ];

        let keys: Vec<Vec<u8>> = values.iter().map(|v| encode_cypher_sort_key(v)).collect();

        // Each key must be strictly less than the next
        for i in 0..keys.len() - 1 {
            assert!(
                keys[i] < keys[i + 1],
                "Expected sort_key({:?}) < sort_key({:?}), but {:?} >= {:?}",
                values[i],
                values[i + 1],
                keys[i],
                keys[i + 1]
            );
        }
    }

    #[test]
    fn test_sort_key_numbers() {
        let neg_inf = encode_cypher_sort_key(&Value::Float(f64::NEG_INFINITY));
        let neg_100 = encode_cypher_sort_key(&Value::Float(-100.0));
        let neg_1 = encode_cypher_sort_key(&Value::Int(-1));
        let zero_int = encode_cypher_sort_key(&Value::Int(0));
        let zero_float = encode_cypher_sort_key(&Value::Float(0.0));
        let one_int = encode_cypher_sort_key(&Value::Int(1));
        let one_float = encode_cypher_sort_key(&Value::Float(1.0));
        let hundred = encode_cypher_sort_key(&Value::Int(100));
        let pos_inf = encode_cypher_sort_key(&Value::Float(f64::INFINITY));
        let nan = encode_cypher_sort_key(&Value::Float(f64::NAN));

        assert!(neg_inf < neg_100, "-inf < -100");
        assert!(neg_100 < neg_1, "-100 < -1");
        assert!(neg_1 < zero_int, "-1 < 0");
        assert_eq!(zero_int, zero_float, "0 int == 0.0 float");
        assert!(zero_int < one_int, "0 < 1");
        assert_eq!(one_int, one_float, "1 int == 1.0 float");
        assert!(one_int < hundred, "1 < 100");
        assert!(hundred < pos_inf, "100 < +inf");
        // NaN gets rank 0x09, numbers get rank 0x08, so NaN > any number
        assert!(pos_inf < nan, "+inf < NaN");
    }

    #[test]
    fn test_sort_key_booleans() {
        let f = encode_cypher_sort_key(&Value::Bool(false));
        let t = encode_cypher_sort_key(&Value::Bool(true));
        assert!(f < t, "false < true");
    }

    #[test]
    fn test_sort_key_strings() {
        let empty = encode_cypher_sort_key(&Value::String(String::new()));
        let a = encode_cypher_sort_key(&Value::String("a".to_string()));
        let ab = encode_cypher_sort_key(&Value::String("ab".to_string()));
        let b = encode_cypher_sort_key(&Value::String("b".to_string()));

        assert!(empty < a, "'' < 'a'");
        assert!(a < ab, "'a' < 'ab'");
        assert!(ab < b, "'ab' < 'b'");
    }

    #[test]
    fn test_sort_key_lists() {
        let empty = encode_cypher_sort_key(&Value::List(vec![]));
        let one = encode_cypher_sort_key(&Value::List(vec![Value::Int(1)]));
        let one_two = encode_cypher_sort_key(&Value::List(vec![Value::Int(1), Value::Int(2)]));
        let two = encode_cypher_sort_key(&Value::List(vec![Value::Int(2)]));

        assert!(empty < one, "[] < [1]");
        assert!(one < one_two, "[1] < [1,2]");
        assert!(one_two < two, "[1,2] < [2]");
    }

    #[test]
    fn test_sort_key_temporal() {
        use uni_common::TemporalValue;

        let date1 = encode_cypher_sort_key(&Value::Temporal(TemporalValue::Date {
            days_since_epoch: 100,
        }));
        let date2 = encode_cypher_sort_key(&Value::Temporal(TemporalValue::Date {
            days_since_epoch: 200,
        }));
        assert!(date1 < date2, "earlier date < later date");

        // Different temporal variants should sort by variant rank
        let date = encode_cypher_sort_key(&Value::Temporal(TemporalValue::Date {
            days_since_epoch: i32::MAX,
        }));
        let local_time = encode_cypher_sort_key(&Value::Temporal(TemporalValue::LocalTime {
            nanos_since_midnight: 0,
        }));
        assert!(date < local_time, "Date < LocalTime (by variant rank)");
    }

    #[test]
    fn test_sort_key_nested_lists() {
        let inner_a = Value::List(vec![Value::Int(1)]);
        let inner_b = Value::List(vec![Value::Int(2)]);

        let list_a = encode_cypher_sort_key(&Value::List(vec![inner_a.clone()]));
        let list_b = encode_cypher_sort_key(&Value::List(vec![inner_b.clone()]));

        assert!(list_a < list_b, "[[1]] < [[2]]");
    }

    #[test]
    fn test_sort_key_null_handling() {
        let null_key = encode_cypher_sort_key(&Value::Null);
        assert_eq!(null_key, vec![0x0A], "Null produces [0x0A]");

        // Null should sort after everything else
        let number_key = encode_cypher_sort_key(&Value::Int(42));
        assert!(number_key < null_key, "number < null");
    }

    #[test]
    fn test_byte_stuff_roundtrip() {
        // Verify byte-stuffing preserves ordering with 0x00 bytes in data
        let s1 = Value::String("a\x00b".to_string());
        let s2 = Value::String("a\x00c".to_string());
        let s3 = Value::String("a\x01".to_string());

        let k1 = encode_cypher_sort_key(&s1);
        let k2 = encode_cypher_sort_key(&s2);
        let k3 = encode_cypher_sort_key(&s3);

        assert!(k1 < k2, "a\\x00b < a\\x00c");
        // After stuffing: "a\x00\xFFb" vs "a\x01"
        // 0x00 0xFF < 0x01 => "a\x00b" < "a\x01"
        assert!(k1 < k3, "a\\x00b < a\\x01");
    }

    #[test]
    fn test_sort_key_order_preserving_f64() {
        // Verify the f64 encoding preserves order
        let vals = [f64::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY];
        let encoded: Vec<[u8; 8]> = vals
            .iter()
            .map(|f| encode_order_preserving_f64(*f))
            .collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "encode({}) should <= encode({}), got {:?} vs {:?}",
                vals[i],
                vals[i + 1],
                encoded[i],
                encoded[i + 1]
            );
        }
    }
}
