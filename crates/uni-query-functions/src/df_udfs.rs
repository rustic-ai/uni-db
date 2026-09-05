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
    Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, UInt64Array,
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
use uni_common::cypher_value_codec::handle::HandleScope;
use uni_common::value::EntityRef;
use uni_cypher::ast::BinaryOp;
use uni_store::storage::arrow_convert::values_to_array;

use super::expr_eval::{
    cypher_eq, cypher_type_name, eval_substring, eval_toboolean, eval_tointeger, eval_tostring,
    to_datafusion_err,
};

mod sort_key;
pub use sort_key::encode_cypher_sort_key;

/// Generate a Cypher scalar UDF whose shell is entirely mechanical.
///
/// Most UDFs in this module differ in only four things: the constructor and
/// struct names, the registered `name()` string, the `Signature`, and the
/// declared `return_type`. Everything else — the unit struct holding a single
/// `Signature`, `new`, `impl_udf_eq_hash!`, `as_any`, `name`, `signature` —
/// used to be hand-copied ~30 lines at a time per UDF. This macro emits that
/// shell so only `invoke_with_args` is written by hand.
///
/// The `name:` string is matched by `df_expr.rs` at planning time, so it must
/// stay byte-identical to what the planner emits.
macro_rules! cypher_scalar_udf {
    (
        $(#[$ctor_meta:meta])*
        $ctor:ident => $struct:ident,
        name: $name:literal,
        signature: $sig:expr,
        return_type: $ret:expr,
        invoke($args:ident) $body:block
    ) => {
        $(#[$ctor_meta])*
        pub fn $ctor() -> ScalarUDF {
            ScalarUDF::new_from_impl($struct::new())
        }

        #[derive(Debug)]
        struct $struct {
            signature: Signature,
        }

        impl $struct {
            fn new() -> Self {
                Self { signature: $sig }
            }
        }

        impl_udf_eq_hash!($struct);

        impl ScalarUDFImpl for $struct {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok($ret)
            }

            fn invoke_with_args(&self, $args: ScalarFunctionArgs) -> DFResult<ColumnarValue> $body
        }
    };
}

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
    ctx.register_udf(create_created_at_udf());
    ctx.register_udf(create_updated_at_udf());
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

    // Temporal UDFs: constructors, dotted functions, and clock functions
    for name in &[
        // Constructors
        "date",
        "time",
        "localtime",
        "localdatetime",
        "datetime",
        "duration",
        "btic",
        // Dotted functions
        "duration.between",
        "duration.inmonths",
        "duration.indays",
        "duration.inseconds",
        "datetime.fromepoch",
        "datetime.fromepochmillis",
        // Truncation
        "date.truncate",
        "time.truncate",
        "datetime.truncate",
        "localdatetime.truncate",
        "localtime.truncate",
        // Clock functions
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

    // Spatial UDFs: point constructor, distance, and bounding-box containment.
    for name in &["point", "distance", "point.withinbbox"] {
        ctx.register_udf(create_spatial_udf(name));
    }

    // Duration and temporal property accessor UDFs
    ctx.register_udf(create_duration_property_udf());
    ctx.register_udf(create_vector_distance_udf());
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

    // CypherValue-to-Float64 conversion UDF (for SUM/AVG on LargeBinary columns)
    ctx.register_udf(create_cypher_to_float64_udf());

    // Similarity scoring UDF
    ctx.register_udf(create_similar_to_udf());
    ctx.register_udf(create_vector_similarity_udf());
    ctx.register_udf(create_sparse_similar_to_udf());

    // Cypher-aware aggregate UDAFs
    ctx.register_udaf(create_cypher_min_udaf());
    ctx.register_udaf(create_cypher_max_udaf());
    ctx.register_udaf(create_cypher_sum_udaf());
    ctx.register_udaf(create_cypher_collect_udaf());

    // Cypher percentileDisc/percentileCont UDAFs
    ctx.register_udaf(create_cypher_percentile_disc_udaf());
    ctx.register_udaf(create_cypher_percentile_cont_udaf());

    // BTIC scalar UDFs
    register_btic_scalar_udfs(ctx)?;

    // BTIC aggregation UDAFs
    ctx.register_udaf(create_btic_min_udaf());
    ctx.register_udaf(create_btic_max_udaf());
    ctx.register_udaf(create_btic_span_agg_udaf());
    ctx.register_udaf(create_btic_count_at_udaf());

    Ok(())
}

/// Register user-defined custom scalar functions from a [`crate::custom_functions::CustomFunctionRegistry`]
/// as DataFusion UDFs on the given session context.
///
/// Legacy instance-scope path retained for the `CustomFunctionRegistry`
/// shadow registry; the plugin-registry path
/// (`register_plugin_scalar_udfs`, in the `uni-query` crate) is the
/// forward direction.
pub fn register_custom_udfs(
    ctx: &SessionContext,
    registry: &crate::custom_functions::CustomFunctionRegistry,
) -> DFResult<()> {
    for (name, func) in registry.iter() {
        // Register with both lowercase and uppercase so that resolve_udfs
        // finds the UDF regardless of the case the user writes in Cypher.
        let lower = name.to_lowercase();
        ctx.register_udf(ScalarUDF::new_from_impl(CustomScalarUdf::new(
            lower,
            func.clone(),
        )));
        // name is already UPPERCASE from the registry
        ctx.register_udf(ScalarUDF::new_from_impl(CustomScalarUdf::new(
            name.to_string(),
            func.clone(),
        )));
    }
    Ok(())
}

/// Adapter that wraps a [`CustomScalarFn`] as a DataFusion `ScalarUDFImpl`.
///
/// Uses `LargeBinary` (CypherValue encoding) as the return type, matching
/// the convention used by other Cypher UDFs in this module.
struct CustomScalarUdf {
    name: String,
    func: crate::custom_functions::CustomScalarFn,
    signature: Signature,
}

impl CustomScalarUdf {
    fn new(name: String, func: crate::custom_functions::CustomScalarFn) -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Volatile),
            name,
            func,
        }
    }
}

impl std::fmt::Debug for CustomScalarUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomScalarUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl_udf_eq_hash!(CustomScalarUdf);

impl ScalarUDFImpl for CustomScalarUdf {
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
        Ok(DataType::LargeBinary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let func = &self.func;
        invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
            func(vals).map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }
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
            // `Any`, not `Exact(UInt64)`: an entity that reached here as a
            // CypherValue blob — anything that has been through `collect()` and
            // `UNWIND` — is a `LargeBinary`, and an exact signature would reject
            // it before `invoke` could decode it.
            signature: Signature::any(1, Volatility::Immutable),
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
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "id(): requires 1 argument".to_string(),
            ));
        }
        // Usually a pass-through: the planner rewrites `id(n)` to the `n._vid`
        // column, which is already `UInt64`. But when `n` is an entity carried as
        // a single CypherValue column — what `UNWIND` over a collected list binds
        // — there is no `_vid` column to rewrite to, so the whole entity arrives
        // here and its id has to be read out of the encoding.
        // `return_type` promises `UInt64`, and DataFusion asserts that at
        // runtime, so every branch here has to produce one — passing an argument
        // straight through is only correct when it is already `UInt64`.
        let arg = args.args[0].clone();
        match arg.data_type() {
            DataType::UInt64 => Ok(arg),
            DataType::LargeBinary => {
                let array = arg.to_array(args.number_rows)?;
                let blobs = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "id(): expected a LargeBinary entity column".to_string(),
                        )
                    })?;
                let ids: UInt64Array = (0..blobs.len())
                    .map(|i| {
                        if blobs.is_null(i) {
                            return None;
                        }
                        let value = uni_common::cypher_value_codec::decode(blobs.value(i)).ok()?;
                        entity_identity(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(ids)))
            }
            other => {
                let array = arg.to_array(args.number_rows)?;
                arrow::compute::cast(&array, &DataType::UInt64)
                    .map(ColumnarValue::Array)
                    .map_err(|_| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "id(): expected an entity or an identity column, got {other}"
                        ))
                    })
            }
        }
    }
}

/// The id of an entity, whichever way it is encoded.
///
/// A typed `Value::Node` / `Value::Edge` and the `Value::Map` form carrying
/// `_vid` / `_eid` are both live representations of the same thing, and code that
/// handles only one of them silently does nothing for the other.
pub(crate) fn entity_identity(value: &Value) -> Option<u64> {
    // The map arm read `_vid`/`_eid` as integers, so an entity spelling its id
    // `_id` — the serde round-trip of a `Value::Node` — or carrying the string
    // form `"Vid(7)"` resolved to `None`, and `id(x)` returned NULL for an
    // entity sitting right there in the row. It also cast a negative id with
    // `as u64`, wrapping to `u64::MAX` rather than rejecting it (#234).
    value.entity_ref().map(|e| match e {
        EntityRef::Vertex(vid) => vid.as_u64(),
        EntityRef::Edge(eid) => eid.as_u64(),
    })
}

// ============================================================================
// created_at(node_or_rel) / updated_at(node_or_rel) -> Timestamp(ns, UTC)
// ============================================================================

/// Create the `created_at` UDF for getting the creation timestamp of a
/// node or relationship.
///
/// Returns the wall-clock time at which the row was first inserted, as a
/// UTC `Timestamp(Nanosecond)`. Passthrough fallback — the planner
/// normally rewrites `created_at(n)` to `n._created_at` before reaching
/// DataFusion, but the UDF is registered so any code path that skips the
/// rewrite yields a clean error rather than "unknown function".
pub fn create_created_at_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SystemTimestampUdf::new("created_at"))
}

/// Create the `updated_at` UDF for getting the most-recent write-touch
/// timestamp of a node or relationship.
///
/// Bumps on any successful CREATE/SET/MERGE that targets the row,
/// including same-value writes. Passthrough fallback (see
/// [`create_created_at_udf`]).
pub fn create_updated_at_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SystemTimestampUdf::new("updated_at"))
}

#[derive(Debug)]
struct SystemTimestampUdf {
    name: &'static str,
    signature: Signature,
}

impl SystemTimestampUdf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Timestamp(
                    arrow_schema::TimeUnit::Nanosecond,
                    Some("UTC".into()),
                )]),
                Volatility::Immutable,
            ),
        }
    }
}

impl_udf_eq_hash!(SystemTimestampUdf);

impl ScalarUDFImpl for SystemTimestampUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Nanosecond,
            Some("UTC".into()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        // Passthrough: the planner rewrites `created_at(n)` /
        // `updated_at(n)` to `n._created_at` / `n._updated_at` before
        // DataFusion sees the call. If we get here, the column is
        // already a Timestamp(ns, UTC).
        if args.args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{}(): requires 1 argument",
                self.name
            )));
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

cypher_scalar_udf! {
    create_nodes_udf => NodesUdf,
    name: "nodes",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
    let output_type = DataType::LargeBinary;
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

cypher_scalar_udf! {
    create_relationships_udf => RelationshipsUdf,
    name: "relationships",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
    let output_type = DataType::LargeBinary;
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

cypher_scalar_udf! {
    /// Create the `startnode` UDF for getting the start node of a relationship.
    ///
    /// At translation time, all known node variable columns are appended as extra arguments
    /// so the UDF can find the matching node by VID at runtime.
    create_startnode_udf => StartNodeUdf,
    name: "startnode",
    signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
    let output_type = DataType::LargeBinary;
    invoke_cypher_udf(args, &output_type, |val_args| {
        startnode_endnode_impl(val_args, true)
    })
    }
}

// ============================================================================
// endNode(relationship) -> Node
// ============================================================================

cypher_scalar_udf! {
    /// Create the `endnode` UDF for getting the end node of a relationship.
    create_endnode_udf => EndNodeUdf,
    name: "endnode",
    signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

    // No node argument matched the endpoint. This used to answer with a map
    // holding only `_vid`, which reads as a node and answers `id()` correctly
    // while every property on it is NULL — the silent trade #188 refused in
    // writing, and the shape #187's remaining case produced.
    //
    // The endpoint is materialised by `EndpointHydrateExec` wherever the
    // planner can reach the relationship. Somewhere it could not, so say so
    // rather than return a node that is not one.
    let fn_name = if is_start { "startNode" } else { "endNode" };
    Err(datafusion::error::DataFusionError::Execution(format!(
        "{fn_name}(): the relationship's endpoint (vid {target_vid}) is not \
         available in this scope, so its properties cannot be read. This is a \
         planner gap; please file an issue with the query."
    )))
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

/// Extract the VID of a node argument.
///
/// Delegates to [`uni_common::Value::entity_vid`]: the narrow reader is the
/// right one here, because this decides whether a *node argument* is the
/// endpoint being looked for. A lenient reader would let a plain integer column
/// match an endpoint by coincidence and be returned as the node.
///
/// Recognising only `Value::Map` — as this did — meant a node arriving in its
/// native `Value::Node` encoding never matched, and `startNode(r)` silently
/// returned the vid-only stand-in instead of the node, so `startNode(r).name`
/// came back NULL. That is not reachable through the node-argument path today
/// (a structural projection always yields the map form) but it costs nothing to
/// be correct for both, and the divergence is exactly what this consolidation
/// exists to remove.
fn extract_vid(val: &Value) -> Option<u64> {
    val.entity_vid().map(|vid| vid.as_u64())
}

// ============================================================================
// range(start, end, [step]) -> List<Int64>
// ============================================================================

/// Extract an i64 from a ColumnarValue, coercing from any integer type.
/// Rejects floats, booleans, strings, lists, and maps with `InvalidArgumentType`.
fn extract_i64_range_arg(arg: &ColumnarValue, row_idx: usize, name: &str) -> DFResult<i64> {
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
            ScalarValue::LargeBinary(Some(bytes)) => {
                scalar_binary_to_value(bytes).as_i64().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                        name
                    ))
                })
            }
            _ => Err(datafusion::error::DataFusionError::Execution(format!(
                "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                name
            ))),
        },
        ColumnarValue::Array(arr) => {
            if row_idx >= arr.len() || arr.is_null(row_idx) {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                    name
                )));
            }
            // Handle array inputs row-wise.
            if !arr.is_empty() {
                use datafusion::arrow::array::{
                    Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array,
                    UInt32Array, UInt64Array,
                };
                match arr.data_type() {
                    DataType::Int8 => Ok(arr
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::Int16 => Ok(arr
                        .as_any()
                        .downcast_ref::<Int16Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::Int32 => Ok(arr
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::Int64 => Ok(arr
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_idx)),
                    DataType::UInt8 => Ok(arr
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::UInt16 => Ok(arr
                        .as_any()
                        .downcast_ref::<UInt16Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::UInt32 => Ok(arr
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::UInt64 => Ok(arr
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(row_idx) as i64),
                    DataType::LargeBinary => {
                        let bytes = arr
                            .as_any()
                            .downcast_ref::<LargeBinaryArray>()
                            .unwrap()
                            .value(row_idx);
                        scalar_binary_to_value(bytes).as_i64().ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "ArgumentError: InvalidArgumentType - range() {} must be an integer",
                                name
                            ))
                        })
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

        let len = args
            .args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(arr) => Some(arr.len()),
                _ => None,
            })
            .unwrap_or(1);

        let mut list_builder =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::Int64Builder::new());

        for row_idx in 0..len {
            let start = extract_i64_range_arg(&args.args[0], row_idx, "start")?;
            let end = extract_i64_range_arg(&args.args[1], row_idx, "end")?;
            let step = if args.args.len() == 3 {
                extract_i64_range_arg(&args.args[2], row_idx, "step")?
            } else {
                1
            };

            if step == 0 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "range(): step cannot be zero".to_string(),
                ));
            }

            if step > 0 && start <= end {
                let mut current = start;
                while current <= end {
                    list_builder.values().append_value(current);
                    // checked_add: a range ending at/near i64::MAX must stop, not
                    // overflow (the interpreted path uses checked_add too).
                    match current.checked_add(step) {
                        Some(n) => current = n,
                        None => break,
                    }
                }
            } else if step < 0 && start >= end {
                let mut current = start;
                while current >= end {
                    list_builder.values().append_value(current);
                    match current.checked_add(step) {
                        Some(n) => current = n,
                        None => break,
                    }
                }
            }
            // Else: direction and step are inconsistent -> append an empty list row.
            list_builder.append(true);
        }

        let list_arr = Arc::new(list_builder.finish()) as ArrayRef;
        if len == 1
            && args
                .args
                .iter()
                .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &list_arr, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(list_arr))
        }
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
        match name.as_str() {
            // Extraction functions return Int64
            "year" | "month" | "day" | "hour" | "minute" | "second" => Ok(DataType::Int64),
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
            | "localdatetime.realtime"
            | "btic" => Ok(DataType::LargeBinary),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let func_name = self.name.to_uppercase();
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            crate::datetime::eval_datetime_function(&func_name, val_args).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("{}(): {}", self.name, e))
            })
        })
    }
}

/// Create a UDF for a Cypher spatial function, delegating to the interpreter.
///
/// Handles `point`, `distance`, and `point.withinBBox` by forwarding to
/// [`eval_spatial_function`](crate::spatial::eval_spatial_function). Registering
/// these as DataFusion UDFs lets spatial calls run inside DataFusion-planned
/// queries (e.g. `RETURN distance(a.loc, b.loc)`), not only the row-at-a-time
/// interpreter path.
fn create_spatial_udf(name: &str) -> ScalarUDF {
    ScalarUDF::new_from_impl(SpatialUdf::new(name.to_string()))
}

#[derive(Debug)]
struct SpatialUdf {
    name: String,
    signature: Signature,
}

impl SpatialUdf {
    fn new(name: String) -> Self {
        Self {
            name,
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(SpatialUdf);

impl ScalarUDFImpl for SpatialUdf {
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
        match self.name.to_lowercase().as_str() {
            // distance() returns meters (geographic) or Euclidean units (cartesian).
            "distance" => Ok(DataType::Float64),
            // point.withinBBox() is a containment predicate.
            "point.withinbbox" => Ok(DataType::Boolean),
            // point() yields a point map, carried through DataFusion via the
            // CypherValue codec exactly like the temporal constructors.
            _ => Ok(DataType::LargeBinary),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let func_name = self.name.to_uppercase();
        let output_type = self.return_type(&[])?;
        invoke_cypher_udf(args, &output_type, |val_args| {
            crate::spatial::eval_spatial_function(&func_name, val_args).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!("{}(): {}", self.name, e))
            })
        })
    }
}

/// Create the `vector_distance` UDF, routing to [`eval_vector_distance`].
///
/// Registering it as a DataFusion UDF lets `VECTOR_DISTANCE(a, b[, metric])`
/// run inside DataFusion-planned queries (e.g. `RETURN VECTOR_DISTANCE(...)`),
/// not only the row-at-a-time interpreter path. Supports every metric the
/// evaluator does — `cosine`/`l2`/`dot`/`l1` over dense vectors and
/// `hamming`/`jaccard` over binary vectors.
///
/// [`eval_vector_distance`]: crate::expr_eval::eval_vector_distance
fn create_vector_distance_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(VectorDistanceUdf::new())
}

#[derive(Debug)]
struct VectorDistanceUdf {
    signature: Signature,
}

impl VectorDistanceUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(VectorDistanceUdf);

impl ScalarUDFImpl for VectorDistanceUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "vector_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let output_type = DataType::Float64;
        invoke_cypher_udf(args, &output_type, |val_args| {
            if val_args.len() < 2 || val_args.len() > 3 {
                return Err(datafusion::error::DataFusionError::Execution(
                    "vector_distance() requires 2 or 3 arguments".to_string(),
                ));
            }
            let metric = if val_args.len() == 3 {
                val_args[2].as_str().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "vector_distance() metric must be a string".to_string(),
                    )
                })?
            } else {
                "cosine"
            };
            crate::expr_eval::eval_vector_distance(&val_args[0], &val_args[1], metric).map_err(
                |e| {
                    datafusion::error::DataFusionError::Execution(format!("vector_distance(): {e}"))
                },
            )
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

            crate::datetime::eval_duration_accessor(dur_str, component).map_err(|e| {
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
            eval_tostring(&val_args[0]).map_err(to_datafusion_err)
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

            crate::datetime::eval_temporal_accessor_value(&val_args[0], &component).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "_temporal_property(): {}",
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
fn get_value_from_array(
    arr: &ArrayRef,
    row: usize,
    field: Option<&arrow::datatypes::Field>,
) -> DFResult<Value> {
    if arr.is_null(row) {
        return Ok(Value::Null);
    }

    match arr.data_type() {
        DataType::LargeBinary => {
            let typed = downcast_arr!(arr, LargeBinaryArray);
            let bytes = typed.value(row);
            // A raw `DataType::Bytes` argument column carries the `uni_raw_bytes`
            // marker; read it verbatim instead of mis-decoding via the tagged
            // codec (which reads byte[0] as a type tag). (#93/#100 family)
            if field.is_some_and(|f| {
                f.metadata()
                    .get("uni_raw_bytes")
                    .is_some_and(|v| v == "true")
            }) {
                return Ok(Value::Bytes(bytes.to_vec()));
            }
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
        // Lists: decode via `arrow_to_value` directly so a `uni_raw_bytes`-marked child
        // (e.g. a raw-`Bytes` list literal) is honored from the array's own type. The
        // `ScalarValue::try_from_array` fallback strips child-field metadata, which
        // would mis-decode raw-`Bytes` elements through the tagged codec.
        DataType::List(_) | DataType::LargeList(_) => Ok(
            uni_store::storage::arrow_convert::arrow_to_value(arr.as_ref(), row, None),
        ),
        // Fallback: use existing ScalarValue path for Struct, FixedSizeList,
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

/// The raw bytes of `arr` at `row`, if it is a non-null `LargeBinary` value.
///
/// `LargeBinary` is the tagged-CypherValue encoding and the only arm of
/// [`get_value_from_array`] whose decode is expensive enough to be worth
/// avoiding; every other arm is a primitive read. `None` covers both "not that
/// type" and "null here", and either way the caller decodes.
fn large_binary_row(arr: &ArrayRef, row: usize) -> Option<&[u8]> {
    if !matches!(arr.data_type(), DataType::LargeBinary) {
        return None;
    }
    let typed = arr.as_any().downcast_ref::<LargeBinaryArray>()?;
    typed.is_valid(row).then(|| typed.value(row))
}

/// Convert DataFusion `ColumnarValue` arguments to `uni_common::Value` for UDF evaluation.
///
/// `arg_fields` is positional to `args` (DataFusion `ScalarFunctionArgs::arg_fields`) and
/// carries per-argument Arrow field metadata — notably the `uni_raw_bytes` marker that
/// distinguishes a raw `DataType::Bytes` column from a tagged CypherValue `LargeBinary`.
fn get_value_args_for_row(
    args: &[ColumnarValue],
    arg_fields: &[arrow::datatypes::FieldRef],
    row: usize,
) -> DFResult<Vec<Value>> {
    args.iter()
        .enumerate()
        .map(|(idx, arg)| match arg {
            ColumnarValue::Scalar(scalar) => scalar_to_value(scalar),
            ColumnarValue::Array(arr) => {
                get_value_from_array(arr, row, arg_fields.get(idx).map(|f| f.as_ref()))
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
        let row_args = get_value_args_for_row(&args.args, &args.arg_fields, 0)?;
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

    // An argument that does not vary across the batch is decoded once. A tagged
    // CypherValue is expensive to decode — a full msgpack walk, or a deep clone
    // of an interned list behind a process-global lock — and a constant list
    // argument, the shape `x IN <collected list>` produces, otherwise pays that
    // on every row. Measured at 96% of that predicate's cost (#229); the
    // membership scan it was attributed to is 4%.
    //
    // The decoded value stays in place in `row_args` across iterations rather
    // than being cloned back in, so this removes the per-row clone as well as
    // the per-row decode.
    // An argument is re-decoded only when its bytes differ from the row before.
    //
    // A tagged CypherValue is expensive to decode — a full msgpack walk, or a
    // deep clone of an interned list behind a process-global lock — and the
    // decoded value is a pure function of the bytes, so a run of equal rows
    // needs one decode. Measured on LDBC IC5, where `WHERE friend IN friends`
    // reads a list collected per forum: 1 808 724 per-row argument decodes cost
    // **218.2 s of the UDF's 220.5 s total** (#245), and rows arrive grouped by
    // forum, so the list changes about once every eleven rows.
    //
    // This subsumes the batch-constant case (a column constant across the batch
    // is a single run) without the O(rows) pre-pass that only that case could
    // use, and it catches the per-row-varying case that pre-pass could not.
    //
    // The decoded value stays in place in `row_args` rather than being cloned
    // back in, so a repeat costs one byte comparison and nothing else. Scalars
    // never vary and are decoded once, at row 0.
    let mut last_bytes: Vec<Option<Vec<u8>>> = vec![None; args.args.len()];

    let mut results = Vec::with_capacity(len);
    let mut row_args: Vec<Value> = Vec::new();
    for i in 0..len {
        if i == 0 {
            row_args = get_value_args_for_row(&args.args, &args.arg_fields, 0)?;
            for (idx, arg) in args.args.iter().enumerate() {
                if let ColumnarValue::Array(arr) = arg {
                    last_bytes[idx] = large_binary_row(arr, 0).map(<[u8]>::to_vec);
                }
            }
        } else {
            for (idx, arg) in args.args.iter().enumerate() {
                let ColumnarValue::Array(arr) = arg else {
                    // A scalar is the same on every row; row 0 already decoded it.
                    continue;
                };
                match large_binary_row(arr, i) {
                    Some(bytes) if last_bytes[idx].as_deref() == Some(bytes) => continue,
                    Some(bytes) => last_bytes[idx] = Some(bytes.to_vec()),
                    // A null row, or a column this memo does not cover, still
                    // overwrites `row_args[idx]` — so the remembered bytes must
                    // be cleared, or a later row repeating the pre-null value
                    // would skip the decode and inherit the `Null` left behind.
                    None => last_bytes[idx] = None,
                }
                row_args[idx] =
                    get_value_from_array(arr, i, args.arg_fields.get(idx).map(|f| f.as_ref()))?;
            }
        }
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
    let dur = crate::datetime::CypherDuration::from_micros(micros);
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
pub fn scalar_to_value(scalar: &ScalarValue) -> DFResult<Value> {
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

        // FixedSizeBinary(24) — BTIC temporal interval
        ScalarValue::FixedSizeBinary(24, Some(bytes)) => {
            match uni_btic::encode::decode_slice(bytes) {
                Ok(btic) => Ok(Value::Temporal(uni_common::TemporalValue::Btic {
                    lo: btic.lo(),
                    hi: btic.hi(),
                    meta: btic.meta(),
                })),
                Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                    "BTIC decode error: {e}"
                ))),
            }
        }

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
        | ScalarValue::IntervalMonthDayNano(None)
        | ScalarValue::FixedSizeBinary(_, None) => Ok(Value::Null),
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
                TemporalValue::Btic { lo, hi, meta } => {
                    let btic = uni_btic::Btic::new(*lo, *hi, *meta).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("invalid BTIC: {e}"))
                    })?;
                    let packed = uni_btic::encode::encode(&btic);
                    ScalarValue::FixedSizeBinary(24, Some(packed.to_vec()))
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

cypher_scalar_udf! {
    create_has_null_udf => HasNullUdf,
    name: "_has_null",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::Boolean,
    invoke(args) {
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

cypher_scalar_udf! {
    create_to_integer_udf => ToIntegerUdf,
    name: "tointeger",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::Int64,
    invoke(args) {
    let output_type = DataType::Int64;
    invoke_cypher_udf(args, &output_type, |val_args| {
        if val_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "tointeger(): requires 1 argument".to_string(),
            ));
        }

        eval_tointeger(&val_args[0]).map_err(to_datafusion_err)
    })
    }
}

// ============================================================================
// toFloat(x) -> Float64
// ============================================================================

cypher_scalar_udf! {
    create_to_float_udf => ToFloatUdf,
    name: "tofloat",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::Float64,
    invoke(args) {
    let output_type = DataType::Float64;
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

cypher_scalar_udf! {
    create_to_boolean_udf => ToBooleanUdf,
    name: "toboolean",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::Boolean,
    invoke(args) {
    let output_type = DataType::Boolean;
    invoke_cypher_udf(args, &output_type, |val_args| {
        if val_args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "toboolean(): requires 1 argument".to_string(),
            ));
        }

        eval_toboolean(&val_args[0]).map_err(to_datafusion_err)
    })
    }
}

// ============================================================================
// _cypher_sort_key(x) -> LargeBinary
// Order-preserving binary encoding for Cypher ORDER BY.
// Produces byte sequences where memcmp matches Cypher's orderability rules.
// ============================================================================

cypher_scalar_udf! {
    create_cypher_sort_key_udf => CypherSortKeyUdf,
    name: "_cypher_sort_key",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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
            let field = args.arg_fields.first().map(|f| f.as_ref());
            let mut keys: Vec<Option<Vec<u8>>> = Vec::with_capacity(arr.len());
            for i in 0..arr.len() {
                let val = if arr.is_null(i) {
                    Value::Null
                } else {
                    get_value_from_array(arr, i, field)?
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

// ============================================================================
// BTIC Scalar UDFs
// ============================================================================

/// A parameterized DataFusion scalar UDF for all BTIC functions.
///
/// Delegates to `eval_btic_function` from `expr_eval.rs` via `invoke_cypher_udf`,
/// supporting both scalar and columnar execution paths.
#[derive(Debug)]
struct BticScalarUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl BticScalarUdf {
    fn new(name: &str, num_args: usize, return_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(TypeSignature::Any(num_args), Volatility::Immutable),
            return_type,
        }
    }
}

impl_udf_eq_hash!(BticScalarUdf);

impl ScalarUDFImpl for BticScalarUdf {
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
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let fname = self.name.to_uppercase();
        let rt = self.return_type.clone();
        invoke_cypher_udf(args, &rt, |val_args| {
            crate::expr_eval::eval_btic_function(&fname, val_args)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }
}

/// Register all BTIC scalar UDFs with the session context.
fn register_btic_scalar_udfs(ctx: &SessionContext) -> DFResult<()> {
    // 1-arg accessors returning LargeBinary (temporal values encoded as CypherValue)
    for name in &["btic_lo", "btic_hi"] {
        ctx.register_udf(ScalarUDF::new_from_impl(BticScalarUdf::new(
            name,
            1,
            DataType::LargeBinary,
        )));
    }
    // 1-arg accessor returning Int64
    ctx.register_udf(ScalarUDF::new_from_impl(BticScalarUdf::new(
        "btic_duration",
        1,
        DataType::Int64,
    )));
    // 1-arg boolean accessors
    for name in &["btic_is_instant", "btic_is_unbounded", "btic_is_finite"] {
        ctx.register_udf(ScalarUDF::new_from_impl(BticScalarUdf::new(
            name,
            1,
            DataType::Boolean,
        )));
    }
    // 1-arg string accessors (granularity/certainty names)
    for name in &[
        "btic_granularity",
        "btic_lo_granularity",
        "btic_hi_granularity",
        "btic_certainty",
        "btic_lo_certainty",
        "btic_hi_certainty",
    ] {
        ctx.register_udf(ScalarUDF::new_from_impl(BticScalarUdf::new(
            name,
            1,
            DataType::Utf8,
        )));
    }
    // 2-arg boolean predicates
    for name in &[
        "btic_contains_point",
        "btic_overlaps",
        "btic_contains",
        "btic_before",
        "btic_after",
        "btic_meets",
        "btic_adjacent",
        "btic_disjoint",
        "btic_equals",
        "btic_starts",
        "btic_during",
        "btic_finishes",
    ] {
        ctx.register_udf(ScalarUDF::new_from_impl(BticScalarUdf::new(
            name,
            2,
            DataType::Boolean,
        )));
    }
    // 2-arg set operations returning LargeBinary (BTIC value as CypherValue)
    for name in &["btic_intersection", "btic_span", "btic_gap"] {
        ctx.register_udf(ScalarUDF::new_from_impl(BticScalarUdf::new(
            name,
            2,
            DataType::LargeBinary,
        )));
    }
    Ok(())
}

// ============================================================================
// BTIC Aggregation UDAFs
// ============================================================================

/// UDAF for `btic_min` and `btic_max` — returns earliest/latest interval in total order.
#[derive(Debug, Clone)]
struct BticMinMaxUdaf {
    name: String,
    signature: Signature,
    is_max: bool,
}

impl BticMinMaxUdaf {
    fn new(is_max: bool) -> Self {
        Self {
            name: (if is_max { "btic_max" } else { "btic_min" }).to_string(),
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            is_max,
        }
    }
}

impl_udf_eq_hash!(BticMinMaxUdaf);

impl AggregateUDFImpl for BticMinMaxUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(BticMinMaxAccumulator {
            current: None,
            is_max: self.is_max,
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
struct BticMinMaxAccumulator {
    current: Option<uni_btic::Btic>,
    is_max: bool,
}

impl DfAccumulator for BticMinMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let Some(btic) = decode_btic_from_array(arr, i)? else {
                continue;
            };
            self.current = Some(match self.current.take() {
                None => btic,
                Some(cur) => {
                    if (self.is_max && btic > cur) || (!self.is_max && btic < cur) {
                        btic
                    } else {
                        cur
                    }
                }
            });
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(btic_to_scalar_value(self.current.as_ref()))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.update_batch(states)
    }
}

/// UDAF for `btic_span_agg` — bounding interval across all values.
#[derive(Debug, Clone)]
struct BticSpanAggUdaf {
    signature: Signature,
}

impl BticSpanAggUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(BticSpanAggUdaf);

impl AggregateUDFImpl for BticSpanAggUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "btic_span_agg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(BticSpanAggAccumulator { current: None }))
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
struct BticSpanAggAccumulator {
    current: Option<uni_btic::Btic>,
}

impl DfAccumulator for BticSpanAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let Some(btic) = decode_btic_from_array(arr, i)? else {
                continue;
            };
            self.current = Some(match self.current.take() {
                None => btic,
                Some(cur) => uni_btic::set_ops::span(&cur, &btic),
            });
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(btic_to_scalar_value(self.current.as_ref()))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.update_batch(states)
    }
}

/// UDAF for `btic_count_at(collection, point)` — count intervals containing a point.
#[derive(Debug, Clone)]
struct BticCountAtUdaf {
    signature: Signature,
}

impl BticCountAtUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(BticCountAtUdaf);

impl AggregateUDFImpl for BticCountAtUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "btic_count_at"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> DFResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(BticCountAtAccumulator { count: 0 }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> DFResult<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![Arc::new(arrow::datatypes::Field::new(
            args.name,
            DataType::Int64,
            true,
        ))])
    }
}

#[derive(Debug)]
struct BticCountAtAccumulator {
    count: i64,
}

impl DfAccumulator for BticCountAtAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.len() < 2 {
            return Ok(());
        }
        let btic_arr = &values[0];
        let point_arr = &values[1];

        for i in 0..btic_arr.len() {
            if btic_arr.is_null(i) || point_arr.is_null(i) {
                continue;
            }
            let Some(btic) = decode_btic_from_array(btic_arr, i)? else {
                continue;
            };

            // Extract point as ms from the second column
            let point_ms = if let Some(int_arr) = point_arr.as_any().downcast_ref::<Int64Array>() {
                int_arr.value(i)
            } else if let Some(lb) = point_arr.as_any().downcast_ref::<LargeBinaryArray>() {
                let val = scalar_binary_to_value(lb.value(i));
                match &val {
                    Value::Int(ms) => *ms,
                    Value::Temporal(uni_common::TemporalValue::DateTime {
                        nanos_since_epoch,
                        ..
                    }) => nanos_since_epoch / 1_000_000,
                    _ => continue,
                }
            } else {
                continue;
            };

            if uni_btic::predicates::contains_point(&btic, point_ms) {
                self.count += 1;
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let arr = &states[0];
        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
            for i in 0..int_arr.len() {
                if !int_arr.is_null(i) {
                    self.count += int_arr.value(i);
                }
            }
        }
        Ok(())
    }
}

/// Encode an optional `Btic` as a `ScalarValue::LargeBinary` via CypherValue codec.
fn btic_to_scalar_value(btic: Option<&uni_btic::Btic>) -> ScalarValue {
    match btic {
        None => ScalarValue::LargeBinary(None),
        Some(b) => {
            let val = Value::Temporal(uni_common::TemporalValue::Btic {
                lo: b.lo(),
                hi: b.hi(),
                meta: b.meta(),
            });
            ScalarValue::LargeBinary(Some(uni_common::cypher_value_codec::encode(&val)))
        }
    }
}

/// Decode a BTIC value from an Arrow array at the given row.
fn decode_btic_from_array(arr: &ArrayRef, row: usize) -> DFResult<Option<uni_btic::Btic>> {
    // Try FixedSizeBinary(24) first (native BTIC storage)
    if let Some(fsb) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let bytes = fsb.value(row);
        return uni_btic::encode::decode_slice(bytes)
            .map(Some)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()));
    }
    // Try LargeBinary (CypherValue encoded)
    if let Some(lb) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
        let val = scalar_binary_to_value(lb.value(row));
        if let Value::Temporal(uni_common::TemporalValue::Btic { lo, hi, meta }) = val {
            return uni_btic::Btic::new(lo, hi, meta)
                .map(Some)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()));
        }
        return Ok(None);
    }
    Ok(None)
}

pub fn create_btic_min_udaf() -> AggregateUDF {
    AggregateUDF::from(BticMinMaxUdaf::new(false))
}

pub fn create_btic_max_udaf() -> AggregateUDF {
    AggregateUDF::from(BticMinMaxUdaf::new(true))
}

pub fn create_btic_span_agg_udaf() -> AggregateUDF {
    AggregateUDF::from(BticSpanAggUdaf::new())
}

pub fn create_btic_count_at_udaf() -> AggregateUDF {
    AggregateUDF::from(BticCountAtUdaf::new())
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
                    if str_arr.is_null(idx) {
                        return None;
                    }
                    str_arr.value(idx).to_string().into()
                } else if let Some(str_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
                    if str_arr.is_null(idx) {
                        return None;
                    }
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

/// Compare a CypherValue-encoded LHS against an exact `i64` RHS.
///
/// The `compare_cv_numeric` int fast-path is exact only if the caller preserves
/// the RHS integer; passing `rhs as f64` first rounds values above 2^53 before
/// the compare. When the LHS is itself an int, compare `i64` vs `i64` directly;
/// only a float LHS falls back to `f64` (where exact-int semantics don't apply).
fn compare_cv_vs_i64(bytes: &[u8], rhs: i64, op: &BinaryOp) -> Option<bool> {
    use uni_common::cypher_value_codec::{TAG_INT, TAG_NULL, decode_int, peek_tag};
    match peek_tag(bytes) {
        Some(TAG_NULL) => None,
        Some(TAG_INT) => decode_int(bytes).map(|lhs| apply_comparison_op(lhs.cmp(&rhs), op)),
        _ => compare_f64(cv_bytes_as_f64(bytes)?, rhs as f64, op),
    }
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
                    // Pass the exact i64 (not `as f64`) so an int-vs-int compare
                    // above 2^53 stays exact. (finding uni-query-functions[14])
                    match compare_cv_vs_i64(lb_arr.value(i), int_arr.value(i), op) {
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
            crate::expr_eval::eval_binary_op(&val_args[0], &self.op, &val_args[1])
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

/// Cypher-aware `abs()` that preserves integer/float type through cv_encoded values.
pub fn create_cypher_abs_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(CypherAbsUdf::new())
}

/// Wrap a DataFusion expression with `_cypher_abs()` UDF.
pub fn cypher_abs_expr(arg: datafusion::logical_expr::Expr) -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::ScalarFunction(
        datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(create_cypher_abs_udf()),
            vec![arg],
        ),
    )
}

#[derive(Debug)]
struct CypherAbsUdf {
    signature: Signature,
}

impl CypherAbsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl_udf_eq_hash!(CypherAbsUdf);

impl ScalarUDFImpl for CypherAbsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "_cypher_abs"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::LargeBinary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "_cypher_abs requires exactly 1 argument".into(),
            ));
        }
        invoke_cypher_udf(args, &DataType::LargeBinary, |val_args| {
            match &val_args[0] {
                Value::Int(i) => i.checked_abs().map(Value::Int).ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "integer overflow in abs()".into(),
                    )
                }),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                Value::Null => Ok(Value::Null),
                other => Err(datafusion::error::DataFusionError::Execution(format!(
                    "abs() requires a numeric argument, got {other:?}"
                ))),
            }
        })
    }
}

/// Apply a checked integer arithmetic operator, returning the `i64` result.
///
/// This is the i64-result core shared by both the DataFusion UDF path and the
/// interpreted expression evaluator, so checked-overflow and div/rem-by-zero
/// semantics stay identical across both paths. OpenCypher integer division and
/// modulo truncate toward zero.
///
/// Returns `None` on overflow, on division by zero, on modulo by zero, or for a
/// non-arithmetic operator.
// Rust guideline compliant
pub(crate) fn checked_int_op(lhs: i64, rhs: i64, op: &BinaryOp) -> Option<i64> {
    match op {
        BinaryOp::Add => lhs.checked_add(rhs),
        BinaryOp::Sub => lhs.checked_sub(rhs),
        BinaryOp::Mul => lhs.checked_mul(rhs),
        // OpenCypher: integer / integer = integer (truncated toward zero)
        BinaryOp::Div => {
            if rhs == 0 {
                None
            } else {
                lhs.checked_div(rhs)
            }
        }
        BinaryOp::Mod => {
            if rhs == 0 {
                None
            } else {
                lhs.checked_rem(rhs)
            }
        }
        _ => None,
    }
}

/// Outcome of a per-row CypherValue arithmetic operation.
///
/// Distinguishes a genuine null operand (preserve Cypher 3-valued logic, append
/// null) from a hard error (integer overflow or division/modulo by zero, which
/// must abort the query) and from a successful CypherValue-encoded result.
// Rust guideline compliant
enum CvArithOutcome {
    /// A successful result, as CypherValue-encoded bytes.
    Value(Vec<u8>),
    /// A genuine null operand or non-numeric input: append a null (3VL).
    Null,
    /// A hard error such as integer overflow or division by zero.
    Error(datafusion::error::DataFusionError),
}

/// Apply a checked integer arithmetic operator, encoding the CypherValue result.
///
/// Overflow and division/modulo by zero become a hard [`CvArithOutcome::Error`]
/// (matching the native integer path and the interpreted evaluator); a
/// non-arithmetic operator yields [`CvArithOutcome::Null`].
// Rust guideline compliant
fn apply_int_arithmetic(lhs: i64, rhs: i64, op: &BinaryOp) -> CvArithOutcome {
    use uni_common::cypher_value_codec::encode_int;
    if !matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod
    ) {
        return CvArithOutcome::Null;
    }
    match checked_int_op(lhs, rhs, op) {
        Some(v) => CvArithOutcome::Value(encode_int(v)),
        None => {
            let msg = if matches!(op, BinaryOp::Div | BinaryOp::Mod) && rhs == 0 {
                "division by zero"
            } else {
                "integer overflow"
            };
            CvArithOutcome::Error(datafusion::error::DataFusionError::Execution(msg.into()))
        }
    }
}

/// Apply a float arithmetic operator, encoding the CypherValue result.
///
/// Float arithmetic never errors (inf/NaN are valid Cypher); a non-arithmetic
/// operator yields [`CvArithOutcome::Null`].
// Rust guideline compliant
fn apply_float_arithmetic(lhs: f64, rhs: f64, op: &BinaryOp) -> CvArithOutcome {
    use uni_common::cypher_value_codec::encode_float;
    let result = match op {
        BinaryOp::Add => lhs + rhs,
        BinaryOp::Sub => lhs - rhs,
        BinaryOp::Mul => lhs * rhs,
        BinaryOp::Div => lhs / rhs, // Allows inf, -inf, NaN
        BinaryOp::Mod => lhs % rhs,
        _ => return CvArithOutcome::Null,
    };
    CvArithOutcome::Value(encode_float(result))
}

/// Perform arithmetic on a CypherValue-encoded LHS against an i64 RHS.
///
/// Returns [`CvArithOutcome::Null`] for null/incompatible operands and an error
/// for integer overflow or division by zero.
// Rust guideline compliant
fn cv_arithmetic_int(bytes: &[u8], rhs: i64, op: &BinaryOp) -> CvArithOutcome {
    use uni_common::cypher_value_codec::{TAG_FLOAT, TAG_INT, decode_float, decode_int, peek_tag};
    match peek_tag(bytes) {
        Some(TAG_INT) => match decode_int(bytes) {
            Some(lhs) => apply_int_arithmetic(lhs, rhs, op),
            None => CvArithOutcome::Null,
        },
        Some(TAG_FLOAT) => match decode_float(bytes) {
            Some(lhs) => apply_float_arithmetic(lhs, rhs as f64, op),
            None => CvArithOutcome::Null,
        },
        _ => CvArithOutcome::Null,
    }
}

/// Perform arithmetic on a CypherValue-encoded LHS against an f64 RHS.
///
/// Returns [`CvArithOutcome::Null`] for null/incompatible operands. Float
/// arithmetic itself never errors.
// Rust guideline compliant
fn cv_arithmetic_float(bytes: &[u8], rhs: f64, op: &BinaryOp) -> CvArithOutcome {
    match cv_bytes_as_f64(bytes) {
        Some(lhs) => apply_float_arithmetic(lhs, rhs, op),
        None => CvArithOutcome::Null,
    }
}

/// Resolve the dynamic return type of a Cypher arithmetic UDF from its arg types.
///
/// Keeps the result type type-preserving so the native overflow-checking Arrow
/// kernels can stay in their natural type: two `Int64` operands yield `Int64`,
/// any `Float64` operand (with no `LargeBinary`) yields `Float64`, and any
/// `LargeBinary` (CypherValue-encoded) operand falls back to `LargeBinary`. A
/// `Null` arg type is non-constraining: it matches the other operand, or
/// `LargeBinary` when nothing else constrains it. Every other combination keeps
/// the historical `LargeBinary` fallback so the CypherValue slow path applies.
///
/// This must agree with the array type each branch of [`try_fast_arithmetic`]
/// produces, otherwise DataFusion rejects the plan on a schema mismatch between
/// the declared `return_type` and the actual output column.
// Rust guideline compliant
pub(crate) fn cypher_arith_return_type(arg_types: &[DataType]) -> DataType {
    let any_large_binary = arg_types.iter().any(|t| matches!(t, DataType::LargeBinary));
    if any_large_binary {
        return DataType::LargeBinary;
    }
    let any_float = arg_types.iter().any(|t| matches!(t, DataType::Float64));
    if any_float {
        return DataType::Float64;
    }
    // No LargeBinary and no Float64: if every non-null arg is Int64, the result
    // is Int64. A lone Int64 alongside Null still resolves to Int64; all-Null or
    // anything else keeps the conservative LargeBinary fallback.
    let all_int_or_null = !arg_types.is_empty()
        && arg_types
            .iter()
            .all(|t| matches!(t, DataType::Int64 | DataType::Null));
    let any_int = arg_types.iter().any(|t| matches!(t, DataType::Int64));
    if all_int_or_null && any_int {
        DataType::Int64
    } else {
        DataType::LargeBinary
    }
}

/// Fast-path arithmetic for native and LargeBinary (CypherValue) operand types.
///
/// Returns `None` to fall through to the slow path, `Some(Ok(_))` when the fast
/// path produced a result, and `Some(Err(_))` when the fast path detected an
/// error condition (integer overflow or division/modulo by zero). The result
/// array type matches [`cypher_arith_return_type`] for the operand types:
/// `Int64` for `Int64 × Int64`, `Float64` for float/mixed numeric, and
/// `LargeBinary` for the CypherValue branches.
// Rust guideline compliant
fn try_fast_arithmetic(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    op: &BinaryOp,
) -> Option<DFResult<ColumnarValue>> {
    use arrow_array::builder::LargeBinaryBuilder;

    let (lhs_arr, rhs_arr) = match (lhs, rhs) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (l, r),
        _ => return None,
    };

    match (lhs_arr.data_type(), rhs_arr.data_type()) {
        // Int64 vs Int64: use the native overflow-checking Arrow kernels and
        // keep a native Int64 result. The kernels error on overflow and on
        // division/modulo by zero (and handle `i64::MIN / -1`).
        (DataType::Int64, DataType::Int64) => Some(int_kernel_arithmetic(lhs_arr, rhs_arr, op)),

        // Mixed Int×Float or Float×Float: compute in Float64. Float kernels
        // never error (inf/NaN are valid Cypher per IEEE 754).
        (DataType::Int64, DataType::Float64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Float64, DataType::Float64) => {
            Some(float_kernel_arithmetic(lhs_arr, rhs_arr, op))
        }

        // LargeBinary vs Int64
        (DataType::LargeBinary, DataType::Int64) => {
            let lb_arr = lhs_arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            let int_arr = rhs_arr.as_any().downcast_ref::<Int64Array>()?;
            let mut builder = LargeBinaryBuilder::new();
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || int_arr.is_null(i) {
                    builder.append_null();
                } else {
                    match cv_arithmetic_int(lb_arr.value(i), int_arr.value(i), op) {
                        CvArithOutcome::Value(bytes) => builder.append_value(&bytes),
                        CvArithOutcome::Null => builder.append_null(),
                        CvArithOutcome::Error(e) => return Some(Err(e)),
                    }
                }
            }
            Some(Ok(ColumnarValue::Array(Arc::new(builder.finish()))))
        }

        // LargeBinary vs Float64
        (DataType::LargeBinary, DataType::Float64) => {
            let lb_arr = lhs_arr.as_any().downcast_ref::<LargeBinaryArray>()?;
            let float_arr = rhs_arr.as_any().downcast_ref::<Float64Array>()?;
            let mut builder = LargeBinaryBuilder::new();
            for i in 0..lb_arr.len() {
                if lb_arr.is_null(i) || float_arr.is_null(i) {
                    builder.append_null();
                } else {
                    match cv_arithmetic_float(lb_arr.value(i), float_arr.value(i), op) {
                        CvArithOutcome::Value(bytes) => builder.append_value(&bytes),
                        CvArithOutcome::Null => builder.append_null(),
                        CvArithOutcome::Error(e) => return Some(Err(e)),
                    }
                }
            }
            Some(Ok(ColumnarValue::Array(Arc::new(builder.finish()))))
        }

        _ => None, // Fallback to slow path
    }
}

/// Run an overflow-checking integer arithmetic kernel over two `Int64` arrays.
///
/// Produces a native `Int64Array` wrapped in a `ColumnarValue::Array` and maps
/// any Arrow error (overflow, division/modulo by zero) to a
/// `DataFusionError::Execution`. A non-arithmetic operator surfaces as an
/// execution error.
// Rust guideline compliant
fn int_kernel_arithmetic(
    lhs_arr: &ArrayRef,
    rhs_arr: &ArrayRef,
    op: &BinaryOp,
) -> DFResult<ColumnarValue> {
    use arrow::compute::kernels::numeric::{add, div, mul, rem, sub};

    let result = match op {
        BinaryOp::Add => add(lhs_arr, rhs_arr),
        BinaryOp::Sub => sub(lhs_arr, rhs_arr),
        BinaryOp::Mul => mul(lhs_arr, rhs_arr),
        BinaryOp::Div => div(lhs_arr, rhs_arr),
        BinaryOp::Mod => rem(lhs_arr, rhs_arr),
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "unsupported integer arithmetic operator: {other:?}"
            )));
        }
    };
    result
        .map(ColumnarValue::Array)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
}

/// Run a float arithmetic kernel after casting both operands to `Float64`.
///
/// Float arithmetic never errors: overflow yields `inf` and division by zero
/// yields `inf`/`NaN` per IEEE 754, all of which are valid Cypher floats.
// Rust guideline compliant
fn float_kernel_arithmetic(
    lhs_arr: &ArrayRef,
    rhs_arr: &ArrayRef,
    op: &BinaryOp,
) -> DFResult<ColumnarValue> {
    use arrow::compute::cast;
    use arrow::compute::kernels::numeric::{add, div, mul, rem, sub};

    let lhs_f = cast(lhs_arr, &DataType::Float64)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let rhs_f = cast(rhs_arr, &DataType::Float64)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    let result = match op {
        BinaryOp::Add => add(&lhs_f, &rhs_f),
        BinaryOp::Sub => sub(&lhs_f, &rhs_f),
        BinaryOp::Mul => mul(&lhs_f, &rhs_f),
        BinaryOp::Div => div(&lhs_f, &rhs_f),
        BinaryOp::Mod => rem(&lhs_f, &rhs_f),
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "unsupported float arithmetic operator: {other:?}"
            )));
        }
    };
    result
        .map(ColumnarValue::Array)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
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
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // Type-preserving: Int64×Int64 → Int64, float/mixed → Float64, else
        // CypherValue-encoded LargeBinary. Must agree with the array type the
        // fast path produces for the same operand types.
        Ok(cypher_arith_return_type(arg_types))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{}(): requires 2 arguments",
                self.name
            )));
        }

        // Try fast path first. `Some(Ok)` = handled, `Some(Err)` = handled and
        // errored (overflow / div-by-zero), `None` = fall through to slow path.
        if let Some(result) = try_fast_arithmetic(&args.args[0], &args.args[1], &self.op) {
            return result;
        }

        // Fallback to slow path. The output type MUST match the type promised by
        // `return_type` (carried on `return_field`) — for native Int64/Float64
        // arg types DataFusion promised Int64/Float64, not LargeBinary, so the
        // materialized array must match or DataFusion rejects on a schema
        // mismatch. `eval_binary_op` itself still errors on integer overflow /
        // division by zero.
        let output_type = args.return_field.data_type().clone();
        invoke_cypher_udf(args, &output_type, |val_args| {
            crate::expr_eval::eval_binary_op(&val_args[0], &self.op, &val_args[1])
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
            crate::expr_eval::eval_binary_op(&left, &BinaryOp::Xor, &right)
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

cypher_scalar_udf! {
    create_cypher_size_udf => CypherSizeUdf,
    name: "_cypher_size",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::Int64,
    invoke(args) {
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

cypher_scalar_udf! {
    create_cypher_list_compare_udf => CypherListCompareUdf,
    name: "_cypher_list_compare",
    signature: Signature::any(3, Volatility::Immutable),
    return_type: DataType::Boolean,
    invoke(args) {
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

cypher_scalar_udf! {
    create_map_project_udf => MapProjectUdf,
    name: "_map_project",
    signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
    let output_type = DataType::LargeBinary;
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
                            // Prefer `_all_props` (the CypherValue-encoded
                            // property map, which round-trips raw `Bytes`
                            // losslessly) over the top-level struct columns,
                            // where a raw `Bytes` property decodes to Null
                            // because `named_struct` drops the `uni_raw_bytes`
                            // marker. Mirrors `properties()` for consistency.
                            let source = match map.get("_all_props") {
                                Some(Value::Map(all)) => all,
                                _ => map,
                            };
                            for (mk, mv) in source {
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

cypher_scalar_udf! {
    create_make_cypher_list_udf => MakeCypherListUdf,
    name: "_make_cypher_list",
    signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
    let output_type = DataType::LargeBinary;
    invoke_cypher_udf(args, &output_type, |val_args| {
        Ok(Value::List(val_args.to_vec()))
    })
    }
}

/// Equality for `IN`, which must reconcile the two ways an entity reaches this
/// UDF.
///
/// `translate_in_expression` rewrites a node/edge variable on the *left* of `IN`
/// down to its bare `_vid` / `_eid` `Int64` column, because a list injected from
/// a parameter holds ids rather than entities. It does not rewrite the right
/// side, so a list built in the query itself — `[countryX, countryY]`, or
/// anything from `collect()` — still holds whole entities. The comparison was
/// then `Int` against `Node`, which is unequal for every row: `n IN [n]`
/// returned false, silently and with no error.
///
/// Matching an entity against a bare id here restores the id-list contract the
/// left-hand rewrite assumes, in the one place that can see both sides. It adds
/// no new looseness to `IN`: because that rewrite already replaces the entity
/// with its id, `n IN [1, 2]` compares ids today regardless.
///
/// Entity-against-entity is left to `cypher_eq`, which compares by identity.
fn entity_aware_eq(left: &Value, right: &Value) -> Option<bool> {
    fn entity_id(v: &Value) -> Option<i64> {
        // An entity that came through a projection arrives as a map carrying
        // `_vid` / `_eid` alongside its properties, not as a typed `Node` or
        // `Edge`. Both encodings are live, and missing one is what made the
        // first attempt at this fix a no-op — so the question is asked once,
        // through the accessor, which also covers the `_id` spelling and the
        // serde string form that the hand-rolled map arm still missed (#234).
        v.entity_ref().map(|e| match e {
            EntityRef::Vertex(vid) => vid.as_u64() as i64,
            EntityRef::Edge(eid) => eid.as_u64() as i64,
        })
    }
    match (entity_id(left), entity_id(right)) {
        // Exactly one side is an entity and the other is a bare id.
        (Some(id), None) => match right.as_i64() {
            Some(other) => Some(id == other),
            None => cypher_eq(left, right),
        },
        (None, Some(id)) => match left.as_i64() {
            Some(other) => Some(id == other),
            None => cypher_eq(left, right),
        },
        _ => cypher_eq(left, right),
    }
}

// ============================================================================
// _cypher_in(element, list) -> Boolean (nullable)
// ============================================================================

cypher_scalar_udf! {
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
    create_cypher_in_udf => CypherInUdf,
    name: "_cypher_in",
    signature: Signature::any(2, Volatility::Immutable),
    return_type: DataType::Boolean,
    invoke(args) {
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
            match entity_aware_eq(element, item) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_list_concat` UDF for Cypher `list + list` concatenation.
    create_cypher_list_concat_udf => CypherListConcatUdf,
    name: "_cypher_list_concat",
    signature: Signature::any(2, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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
                crate::expr_eval::eval_binary_op(
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

cypher_scalar_udf! {
    /// Create the `_cypher_list_append` UDF for Cypher `list + element` or `element + list`.
    create_cypher_list_append_udf => CypherListAppendUdf,
    name: "_cypher_list_append",
    signature: Signature::any(2, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_list_slice` UDF for Cypher list slicing on CypherValue-encoded lists.
    create_cypher_list_slice_udf => CypherListSliceUdf,
    name: "_cypher_list_slice",
    signature: Signature::any(3, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_reverse` UDF for Cypher `reverse()`.
    ///
    /// Handles both strings and lists:
    /// - `reverse("abc")` → `"cba"`
    /// - `reverse([1,2,3])` → `[3,2,1]`
    /// - `reverse(null)` → `null`
    create_cypher_reverse_udf => CypherReverseUdf,
    name: "_cypher_reverse",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_substring` UDF for Cypher `substring()`.
    ///
    /// Uses 0-based indexing (Cypher convention):
    /// - `substring("hello", 1)` → `"ello"`
    /// - `substring("hello", 1, 3)` → `"ell"`
    /// - Any null argument → `null`
    create_cypher_substring_udf => CypherSubstringUdf,
    name: "_cypher_substring",
    signature: Signature::variadic_any(Volatility::Immutable),
    return_type: DataType::Utf8,
    invoke(args) {
    invoke_cypher_udf(args, &DataType::Utf8, |vals| {
        eval_substring(vals).map_err(to_datafusion_err)
    })
    }
}

// ============================================================================
// _cypher_split(str, delimiter) -> LargeBinary (CypherValue list of strings)
// ============================================================================

cypher_scalar_udf! {
    /// Create the `_cypher_split` UDF for Cypher `split()`.
    ///
    /// - `split("one,two", ",")` → `["one", "two"]`
    /// - `split(null, ",")` → `null`
    create_cypher_split_udf => CypherSplitUdf,
    name: "_cypher_split",
    signature: Signature::any(2, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
    invoke_cypher_udf(args, &DataType::LargeBinary, |vals| {
        crate::expr_eval::eval_split(vals).map_err(to_datafusion_err)
    })
    }
}

// ============================================================================
// _cypher_list_to_cv(list) -> LargeBinary (CypherValue)
// ============================================================================

cypher_scalar_udf! {
    /// Create the `_cypher_list_to_cv` UDF.
    ///
    /// Wraps a native Arrow `List<T>` or `LargeList<T>` column as a `LargeBinary`
    /// CypherValue. Used by CASE/coalesce type coercion when branches have mixed
    /// `LargeList<T>` and `LargeBinary` types — since Arrow cannot cast between
    /// those types natively, we route through this UDF instead.
    create_cypher_list_to_cv_udf => CypherListToCvUdf,
    name: "_cypher_list_to_cv",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_scalar_to_cv` UDF.
    ///
    /// Converts a native scalar column (Int64, Float64, Utf8, Boolean, etc.) to
    /// CypherValue-encoded LargeBinary. Used when coalesce has mixed native +
    /// LargeBinary args so all branches can be normalized to LargeBinary.
    /// SQL NULLs are preserved as SQL NULLs (not encoded as CypherValue::Null).
    create_cypher_scalar_to_cv_udf => CypherScalarToCvUdf,
    name: "_cypher_scalar_to_cv",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_tail` UDF for Cypher `tail()`.
    ///
    /// Returns all elements except the first element of a list.
    /// - `tail([1,2,3])` → `[2,3]`
    /// - `tail([1])` → `[]`
    /// - `tail([])` → `[]`
    /// - `tail(null)` → `null`
    create_cypher_tail_udf => CypherTailUdf,
    name: "_cypher_tail",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_head` UDF for Cypher `head()`.
    ///
    /// Returns the first element of a list. Handles LargeBinary-encoded lists.
    /// - `head([1,2,3])` → `1`
    /// - `head([])` → `null`
    /// - `head(null)` → `null`
    create_cypher_head_udf => CypherHeadUdf,
    name: "head",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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

cypher_scalar_udf! {
    /// Create the `_cypher_last` UDF for Cypher `last()`.
    ///
    /// Returns the last element of a list. Handles LargeBinary-encoded lists.
    /// - `last([1,2,3])` → `3`
    /// - `last([])` → `null`
    /// - `last(null)` → `null`
    create_cypher_last_udf => CypherLastUdf,
    name: "last",
    signature: Signature::any(1, Volatility::Immutable),
    return_type: DataType::LargeBinary,
    invoke(args) {
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
        // Temporals compare by their numeric representation, matching
        // `expr_eval::cypher_partial_cmp`. Without this arm they fell through to
        // `_ => None`, so a list of dates was incomparable on the UDF path only.
        // Mismatched temporal types and Durations still yield `None`.
        (Value::Temporal(l), Value::Temporal(r)) => crate::expr_eval::temporal_partial_cmp(l, r),
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
pub fn cypher_to_float64_expr(
    arg: datafusion::logical_expr::Expr,
) -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::ScalarFunction(
        datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(create_cypher_to_float64_udf()),
            vec![arg],
        ),
    )
}

/// Create the `_cypher_to_float64` ScalarUDF for use in physical planning.
pub fn cypher_to_float64_udf() -> datafusion::logical_expr::ScalarUDF {
    create_cypher_to_float64_udf()
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
        // A raw `DataType::Bytes` input column (`uni_raw_bytes=true`) holds verbatim
        // bytes, not tagged CypherValue payloads; honor the #93/#100 marker so
        // `update_batch` reads the input directly instead of mis-decoding via the
        // codec (which reads byte[0] as a type tag). State/merge stay in tagged
        // space (see `merge_batch`). (#100 follow-up)
        let raw_bytes = acc_args
            .expr_fields
            .first()
            .and_then(|field| field.metadata().get("uni_raw_bytes"))
            .is_some_and(|v| v == "true");
        Ok(Box::new(CypherMinMaxAccumulator {
            current: None,
            is_max: self.is_max,
            return_type: acc_args.return_field.data_type().clone(),
            raw_bytes,
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
    /// Input column is a raw `DataType::Bytes` column (`uni_raw_bytes=true`).
    raw_bytes: bool,
}

impl CypherMinMaxAccumulator {
    /// Folds one decoded value into the running min/max.
    fn accumulate(&mut self, val: Value) {
        if val.is_null() {
            return;
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
                    // Raw `Bytes` columns bypass the tagged codec; all other
                    // `LargeBinary` inputs are CypherValue-encoded.
                    let val = if self.raw_bytes {
                        Value::Bytes(lb.value(i).to_vec())
                    } else {
                        scalar_binary_to_value(lb.value(i))
                    };
                    self.accumulate(val);
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
                    self.accumulate(scalar_to_value(&sv)?);
                }
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        match &self.current {
            None => {
                // Return null of the declared return type
                ScalarValue::try_from(&self.return_type)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
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
        // Partial states are always CypherValue-tagged (produced by `evaluate`),
        // never raw `Bytes` — decode them with the codec regardless of the
        // `raw_bytes` input flag.
        let arr = &states[0];
        if let Some(lb) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
            for i in 0..lb.len() {
                if lb.is_null(i) {
                    continue;
                }
                self.accumulate(scalar_binary_to_value(lb.value(i)));
            }
        }
        Ok(())
    }
}

pub fn create_cypher_min_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherMinMaxUdaf::new(false))
}

pub fn create_cypher_max_udaf() -> AggregateUDF {
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
                                // On i64 overflow, drop the exact-int path so the
                                // result is the f64 sum, not a silent wraparound.
                                match self.int_sum.checked_add(v) {
                                    Some(s) => self.int_sum = s,
                                    None => self.all_ints = false,
                                }
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
                    match self.int_sum.checked_add(v) {
                        Some(s) => self.int_sum = s,
                        None => self.all_ints = false,
                    }
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
                match self.int_sum.checked_add(int_sum_arr.value(i)) {
                    Some(s) => self.int_sum = s,
                    None => self.all_ints = false,
                }
                if !all_ints_arr.value(i) {
                    self.all_ints = false;
                }
                self.has_value = true;
            }
        }
        Ok(())
    }
}

pub fn create_cypher_sum_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherSumUdaf::new())
}

// ============================================================================
// Cypher-aware COLLECT UDAF
// ============================================================================

/// Encoded size above which a collected list is interned rather than carried
/// inline.
///
/// The criterion is bytes, not element count, because bytes are what gets
/// copied: interning costs one registry insert per group and saves
/// `encoded_len - 9` bytes on every row the list is carried onto. An element
/// count is a poor proxy for that — twenty entities is ~6 KB (LDBC IC3, whose
/// list a 32-element threshold skipped entirely) while thirty-two small
/// integers is ~160 bytes. Below this the list stays on the path it has always
/// taken, which bounds what this change can regress.
const COLLECT_INTERN_MIN_BYTES: usize = 1024;

/// Custom UDAF for Cypher collect() that filters nulls and returns [] (not null)
/// when all inputs are null.
#[derive(Debug, Clone)]
struct CypherCollectUdaf {
    signature: Signature,
    /// Scope owning anything this aggregate interns; `None` disables interning.
    scope: Option<Arc<HandleScope>>,
}

impl CypherCollectUdaf {
    fn new(scope: Option<Arc<HandleScope>>) -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            scope,
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
        // `Bytes`, `CypherValue`, and `Duration` all map to Arrow `LargeBinary`.
        // A raw-`Bytes` column carries the `uni_raw_bytes=true` field-metadata
        // marker (stamped at scan time by `property_field`); honor it here so the
        // accumulator materializes raw bytes verbatim instead of feeding them to
        // the tagged CypherValue codec, which would read byte[0] as a type tag and
        // drop the value. This is the collect() analogue of the #93 scalar fix (#100).
        let raw_bytes = acc_args
            .expr_fields
            .first()
            .and_then(|field| field.metadata().get("uni_raw_bytes"))
            .is_some_and(|v| v == "true");
        Ok(Box::new(CypherCollectAccumulator {
            values: Vec::new(),
            distinct: acc_args.is_distinct,
            raw_bytes,
            scope: self.scope.clone(),
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
    /// Input column is a raw `DataType::Bytes` column (`uni_raw_bytes=true`); its
    /// `LargeBinary` elements are verbatim bytes, not tagged CypherValue payloads.
    raw_bytes: bool,
    /// Scope owning anything this accumulator interns; `None` disables it.
    scope: Option<Arc<HandleScope>>,
}

impl CypherCollectAccumulator {
    /// Pushes `val` into the accumulator, skipping duplicates when `distinct`.
    fn push_value(&mut self, val: Value) {
        if self.distinct {
            let key = Self::distinct_key(&val);
            if self.values.iter().any(|v| Self::distinct_key(v) == key) {
                return;
            }
        }
        self.values.push(val);
    }

    /// Deduplication key for `collect(DISTINCT ...)`.
    ///
    /// Entities dedup by identity (`_vid` / `_eid`) so two references to the
    /// same node/edge collapse to one regardless of property values — matching
    /// openCypher's identity-based DISTINCT (issue #134 family). Graph entities
    /// surface here as `Value::Map` (a node/edge struct carrying `_vid`/`_eid`),
    /// whose `to_string()` is order-nondeterministic (backed by a `HashMap`) and
    /// so cannot be used as a key. Non-entity values dedup by their string
    /// representation, as before. The `\0` prefixes keep entity keys from
    /// colliding with any scalar's string form.
    fn distinct_key(val: &Value) -> String {
        // The `\0n` / `\0e` prefixes carry the vertex/edge distinction, so a
        // vertex and an edge of the same number stay apart. What the map arm
        // this replaces did *not* handle: an `_id`-spelled entity fell through
        // to `val.to_string()` over a `HashMap`, whose rendering is
        // order-nondeterministic, and a `_vid` carrying the serde string
        // `"Vid(7)"` keyed as `"\0nVid(7)"` — neither matching the native
        // form's `"\0n7"`, so `collect(DISTINCT n)` emitted the same node
        // twice (#234).
        match val.entity_ref() {
            Some(EntityRef::Vertex(vid)) => format!("\0n{vid}"),
            Some(EntityRef::Edge(eid)) => format!("\0e{eid}"),
            None => val.to_string(),
        }
    }
}

impl DfAccumulator for CypherCollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let arr = &values[0];
        // Raw-`Bytes` columns must bypass the tagged CypherValue codec: read each
        // non-null `LargeBinary` element directly as `Value::Bytes` (#100).
        if self.raw_bytes
            && let Some(lb) = arr.as_any().downcast_ref::<LargeBinaryArray>()
        {
            for i in 0..lb.len() {
                if lb.is_null(i) {
                    continue;
                }
                self.push_value(Value::Bytes(lb.value(i).to_vec()));
            }
            return Ok(());
        }
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
            self.push_value(val);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        // Always return a list (empty list, not null)
        let val = Value::List(self.values.clone());
        let bytes = uni_common::cypher_value_codec::encode(&val);
        // Past the threshold the list travels as a handle, so every operator
        // above copies 9 bytes per row rather than the whole encoding.
        // `decode` resolves it transparently, so consumers are unchanged.
        //
        // The encoding above is measured and then dropped on this path. That is
        // one wasted encode per group, against a saving on every row the group
        // fans out to — and the inline path below needs those bytes anyway, so
        // nothing is spent when interning does not fire.
        if bytes.len() >= COLLECT_INTERN_MIN_BYTES
            && let Some(scope) = &self.scope
        {
            let id = scope.register(Arc::new(val));
            return Ok(ScalarValue::LargeBinary(Some(
                uni_common::cypher_value_codec::encode_handle(id),
            )));
        }
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
                            self.push_value(item);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// The name-registered `_cypher_collect`, reachable when an expression names
/// the UDAF rather than being built by the planner. It has no scope, so it
/// never interns — correct, just without the saving.
pub fn create_cypher_collect_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherCollectUdaf::new(None))
}

/// Create a Cypher collect() UDAF expression with optional distinct.
///
/// `scope` is the query's interning scope; pass `None` where no execution
/// context is available and the list will simply be carried inline.
pub fn create_cypher_collect_expr(
    arg: datafusion::logical_expr::Expr,
    distinct: bool,
    scope: Option<Arc<HandleScope>>,
) -> datafusion::logical_expr::Expr {
    // We use the UDAF's call() but need to set distinct separately.
    // For now, always include arg directly - distinct is handled in the accumulator.
    let udaf = Arc::new(AggregateUDF::from(CypherCollectUdaf::new(scope)));
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

pub fn create_cypher_percentile_disc_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherPercentileDiscUdaf::new())
}

pub fn create_cypher_percentile_cont_udaf() -> AggregateUDF {
    AggregateUDF::from(CypherPercentileContUdaf::new())
}

// ============================================================================
// similar_to / vector_similarity -> Float64
// ============================================================================

/// Shared invocation logic for similarity UDFs.
///
/// Both `similar_to` and `vector_similarity` compute pure vector-vector
/// cosine similarity in the DataFusion path. Storage-dependent cases
/// (auto-embed, FTS) are handled in the ReadQuery executor path.
fn invoke_similarity_udf(
    func_name: &str,
    min_args: usize,
    args: ScalarFunctionArgs,
) -> DFResult<ColumnarValue> {
    let output_type = DataType::Float64;
    invoke_cypher_udf(args, &output_type, |val_args| {
        if val_args.len() < min_args {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "{} requires at least {} arguments",
                func_name, min_args
            )));
        }
        crate::similar_to::eval_similar_to_pure(&val_args[0], &val_args[1])
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
    })
}

cypher_scalar_udf! {
    /// Create the `similar_to` UDF for unified similarity scoring.
    create_similar_to_udf => SimilarToUdf,
    name: "similar_to",
    signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
    return_type: DataType::Float64,
    invoke(args) {
    invoke_similarity_udf("similar_to", 2, args)
    }
}

cypher_scalar_udf! {
    /// Create the `vector_similarity` UDF (alias for similar_to with two vector args).
    create_vector_similarity_udf => VectorSimilarityUdf,
    name: "vector_similarity",
    signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
    return_type: DataType::Float64,
    invoke(args) {
    invoke_similarity_udf("vector_similarity", 2, args)
    }
}

/// Shared invocation logic for the `sparse_similar_to` UDF.
///
/// Computes the pure sparse-vector dot product between two `Value::SparseVector`
/// operands. Storage-backed retrieval lives in the `uni.sparse.query` procedure.
fn invoke_sparse_similarity_udf(args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let output_type = DataType::Float64;
    invoke_cypher_udf(args, &output_type, |val_args| {
        if val_args.len() != 2 {
            return Err(datafusion::error::DataFusionError::Execution(
                "sparse_similar_to takes 2 arguments".to_string(),
            ));
        }
        crate::similar_to::eval_sparse_similar_to_pure(&val_args[0], &val_args[1])
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
    })
}

cypher_scalar_udf! {
    /// Create the `sparse_similar_to` UDF for learned-sparse dot-product scoring.
    create_sparse_similar_to_udf => SparseSimilarToUdf,
    name: "sparse_similar_to",
    signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
    return_type: DataType::Float64,
    invoke(args) {
    invoke_sparse_similarity_udf(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::FunctionRegistry;

    /// `large_binary_row` gates the run-length memo that decides whether a UDF
    /// argument is re-decoded for a row (#245). A wrong `Some` there makes the
    /// previous row's decoded value stand in for this one, silently.
    #[test]
    fn large_binary_row_reports_only_real_bytes() {
        use arrow::array::LargeBinaryArray;

        let arr: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_ref()),
            None,
            Some(b"abd".as_ref()),
        ]));
        assert_eq!(large_binary_row(&arr, 0), Some(b"abc".as_ref()));
        // A null must not compare equal to the previous row's bytes, or the memo
        // would reuse a value where `Value::Null` is required.
        assert_eq!(large_binary_row(&arr, 1), None);
        // Equal length, different bytes: the memo compares bytes, not lengths.
        assert_eq!(large_binary_row(&arr, 2), Some(b"abd".as_ref()));
        assert_ne!(large_binary_row(&arr, 0), large_binary_row(&arr, 2));

        // A non-LargeBinary column is never memoised: its per-row read is cheap
        // and the comparison would not repay itself.
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![7i64, 7, 7]));
        assert_eq!(large_binary_row(&ints, 0), None);
    }

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
        make_multi_scalar_args_with_return(scalars, DataType::LargeBinary)
    }

    fn make_multi_scalar_args_with_return(
        scalars: Vec<ScalarValue>,
        return_type: DataType,
    ) -> ScalarFunctionArgs {
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
            return_field: Arc::new(Field::new("result", return_type, true)),
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
        make_multi_scalar_args_with_return(
            vec![
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(element))),
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(list))),
            ],
            DataType::Boolean,
        )
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
        let args = make_multi_scalar_args_with_return(
            vec![
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(1)))),
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
            ],
            DataType::Boolean,
        );
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {} // null
            other => panic!("expected Boolean(None) for null list, got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_null_element_nonempty() {
        let udf = create_cypher_in_udf();
        let args = make_multi_scalar_args_with_return(
            vec![
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([1, 2])))),
            ],
            DataType::Boolean,
        );
        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {} // null
            other => panic!("expected Boolean(None) for null IN non-empty list, got {other:?}"),
        }
    }

    #[test]
    fn test_cypher_in_null_element_empty() {
        let udf = create_cypher_in_udf();
        let args = make_multi_scalar_args_with_return(
            vec![
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!(null)))),
                ScalarValue::LargeBinary(Some(json_to_cv_bytes(&serde_json::json!([])))),
            ],
            DataType::Boolean,
        );
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
            .map(|f| sort_key::encode_order_preserving_f64(*f))
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

    // Regression tests: human-readable temporal strings must parse via sort_key_string_as_temporal.
    // These broke when the simplification commit removed the classify_temporal fallback.

    #[test]
    fn test_sort_key_string_as_temporal_time_with_offset() {
        let tv = sort_key::sort_key_string_as_temporal("12:35:15+05:00")
            .expect("should parse Time with positive offset");
        match tv {
            uni_common::TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                assert_eq!(offset_seconds, 5 * 3600, "offset should be +05:00 = 18000s");
                // 12h 35m 15s in nanos
                let expected_nanos = (12 * 3600 + 35 * 60 + 15) * 1_000_000_000i64;
                assert_eq!(nanos_since_midnight, expected_nanos);
            }
            other => panic!("expected TemporalValue::Time, got {other:?}"),
        }
    }

    #[test]
    fn test_sort_key_string_as_temporal_time_negative_offset() {
        let tv = sort_key::sort_key_string_as_temporal("10:35:00-08:00")
            .expect("should parse Time with negative offset");
        match tv {
            uni_common::TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                assert_eq!(
                    offset_seconds,
                    -8 * 3600,
                    "offset should be -08:00 = -28800s"
                );
                let expected_nanos = (10 * 3600 + 35 * 60) * 1_000_000_000i64;
                assert_eq!(nanos_since_midnight, expected_nanos);
            }
            other => panic!("expected TemporalValue::Time, got {other:?}"),
        }
    }

    #[test]
    fn test_sort_key_string_as_temporal_date() {
        use super::super::expr_eval::temporal_from_value;
        let tv = temporal_from_value(&Value::String("2024-01-15".into()))
            .expect("should parse Date string");
        match tv {
            uni_common::TemporalValue::Date { days_since_epoch } => {
                // 2024-01-15: verify it is a positive epoch offset (post-1970)
                assert!(days_since_epoch > 0, "2024-01-15 should be after epoch");
            }
            other => panic!("expected TemporalValue::Date, got {other:?}"),
        }
    }
}
