// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Columnar materialisation of vertex properties.
//!
//! Moved down from `uni-query`'s scan path so the storage layer can own the
//! columnar read pipeline, and so crates that depend on `uni-store` but not on
//! `uni-query` -- `uni-algo` in particular -- can hydrate properties without
//! routing every row through a per-vid `HashMap<String, Value>` (#209).
//!
//! Everything here is a pure relocation: the bodies are unchanged.

use chrono::{NaiveDate, NaiveTime, Timelike};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    Time64NanosecondBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit};
use uni_common::core::id::Vid;
use uni_common::core::schema::is_reserved_property_name;
use uni_common::{Properties, Value};

use crate::backend::types::{CmpOp, FilterExpr, Scalar};
use crate::runtime::l0_visibility::L0Context;
use crate::storage::arrow_convert;
use crate::storage::manager::StorageManager;
use crate::storage::property_builder::{
    EXT_ID_COLUMN, restore_ext_id_column, stored_in_ext_id_column,
};

/// Resolve the Arrow data type for a property, handling system columns like `overflow_json`.
///
/// Falls back to `LargeBinary` (CypherValue) if the property is not found in the schema,
/// preserving original value types for overflow/unknown properties.
pub fn resolve_property_type(
    prop: &str,
    schema_props: Option<
        &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
    >,
) -> DataType {
    if prop == "overflow_json" {
        DataType::LargeBinary
    } else if prop == "_created_at" || prop == "_updated_at" {
        // System-managed timestamps surfaced via `created_at(n)` /
        // `updated_at(n)`. Stored on every vertex/edge by the L0 buffer
        // and the on-disk Arrow tables as Timestamp(Nanosecond, UTC).
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    } else {
        schema_props
            .and_then(|props| props.get(prop))
            .map(|meta| meta.r#type.to_arrow())
            .unwrap_or(DataType::LargeBinary)
    }
}

/// Build a scan-output `Field`, tagging raw `Bytes` columns for the final read.
///
/// `DataType::Bytes`, `DataType::CypherValue`, and `DataType::Duration` all map to Arrow
/// `LargeBinary`, but only `Bytes` stores raw (un-codec'd) bytes. The projection read
/// (`record_batches_to_rows`) cannot tell them apart from the Arrow type alone, so it
/// would decode a raw `Bytes` column with the CypherValue MessagePack codec and corrupt
/// it. Stamping `uni_raw_bytes=true` lets the read route the column to the raw-bytes
/// branch of `arrow_to_value` instead.
pub fn property_field(
    col_name: &str,
    arrow_type: DataType,
    uni_type: Option<&uni_common::DataType>,
) -> Field {
    let field = Field::new(col_name, arrow_type, true);
    if matches!(uni_type, Some(uni_common::DataType::Bytes)) {
        field.with_metadata(std::collections::HashMap::from([(
            "uni_raw_bytes".to_string(),
            "true".to_string(),
        )]))
    } else {
        field
    }
}

/// Push `col_name` into `columns` if not already present.
///
/// Avoids the verbose `!columns.contains(&col_name.to_string())` pattern
/// that creates a temporary `String` allocation on every check.
pub fn push_column_if_absent(columns: &mut Vec<String>, col_name: &str) {
    if !columns.iter().any(|c| c == col_name) {
        columns.push(col_name.to_string());
    }
}

/// Extract a property value from an overflow_json CypherValue blob.
///
/// Returns the raw CypherValue bytes for `prop` if found in the blob, or
/// `Ok(None)` if there is no overflow column, the blob is null, or the key is
/// absent. A blob that will not deserialize is an error, not an absent key.
///
/// # Errors
///
/// Propagates a corrupt overflow payload.
pub fn extract_from_overflow_blob(
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    row: usize,
    prop: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    let Some(arr) = overflow_arr else {
        return Ok(None);
    };
    if arr.is_null(row) {
        return Ok(None);
    }
    Ok(uni_common::cypher_value_codec::extract_map_entry_raw(
        arr.value(row),
        prop,
    )?)
}

/// Build a `LargeBinary` column by extracting a property from overflow_json
/// blobs, with L0 buffer overlay.
///
/// For each row, checks L0 buffers first (later buffers take precedence).
/// If the property is not in L0, falls back to extracting from the
/// overflow_json CypherValue blob.
pub fn build_overflow_property_column(
    num_rows: usize,
    vid_arr: &UInt64Array,
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    prop: &str,
    l0_ctx: &L0Context,
) -> anyhow::Result<ArrayRef> {
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));

        // Check L0 buffers (later overwrites earlier)
        let l0_val = resolve_l0_property(&vid, prop, l0_ctx);

        if let Some(val_opt) = l0_val {
            append_value_as_cypher_binary(&mut builder, val_opt.as_ref());
        } else if let Some(bytes) = extract_from_overflow_blob(overflow_arr, i, prop)? {
            builder.append_value(&bytes);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Build the `ext_id` output column (`LargeBinary` CypherValue) for a labelled scan.
///
/// `ext_id` is never a declared property, so it is read like an overflow
/// property — L0 first, then storage — except that storage keeps a string
/// `ext_id` in the per-label `ext_id` column rather than in `overflow_json`
/// (see `property_builder::stored_in_ext_id_column`). Reading the blob alone
/// returned NULL for every flushed row.
///
/// # Errors
///
/// Propagates a corrupt overflow payload.
pub fn build_ext_id_property_column(
    num_rows: usize,
    vid_arr: &UInt64Array,
    stored: Option<&arrow_array::StringArray>,
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    l0_ctx: &L0Context,
) -> anyhow::Result<ArrayRef> {
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));
        if let Some(val_opt) = resolve_l0_property(&vid, EXT_ID_COLUMN, l0_ctx) {
            append_value_as_cypher_binary(&mut builder, val_opt.as_ref());
        } else if let Some(col) = stored
            && !col.is_null(i)
        {
            builder.append_value(uni_common::cypher_value_codec::encode(&Value::String(
                col.value(i).to_string(),
            )));
        } else if let Some(bytes) = extract_from_overflow_blob(overflow_arr, i, EXT_ID_COLUMN)? {
            builder.append_value(&bytes);
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Resolve a property value from the L0 visibility chain.
///
/// Returns `Some(Some(val))` when the property exists with a non-null value,
/// `Some(None)` when it exists but is null, and `None` when no L0 buffer
/// has the property.
pub fn resolve_l0_property(vid: &Vid, prop: &str, l0_ctx: &L0Context) -> Option<Option<Value>> {
    let mut result = None;
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        if let Some(props) = guard.vertex_properties.get(vid)
            && let Some(val) = props.get(prop)
        {
            result = Some(Some(val.clone()));
        }
    }
    result
}

/// Append a `Value` to a `LargeBinaryBuilder` as CypherValue bytes.
///
/// Encoded directly via the CypherValue codec so typed values (temporals,
/// nested lists/maps) round-trip losslessly. Null values produce null entries.
pub fn append_value_as_cypher_binary(
    builder: &mut arrow_array::builder::LargeBinaryBuilder,
    val: Option<&Value>,
) {
    match val {
        Some(v) if !v.is_null() => {
            builder.append_value(uni_common::cypher_value_codec::encode(v));
        }
        _ => builder.append_null(),
    }
}

/// Build `_all_props` for a schema-based scan by merging:
/// 1. Schema-defined columns from the batch
/// 2. Overflow_json properties
/// 3. L0 buffer properties
///
/// # Precondition
///
/// `projected_properties` must already contain every declared property the
/// caller expects to find in `_all_props`. This is a *projected* whole-entity
/// map, not an exhaustive one: step 1 reads only the columns named there, so a
/// declared property the caller did not project is silently absent -- which is
/// indistinguishable from the property being unset.
///
/// The planner satisfies this by not pushing the `_all_props` sentinel down
/// (`STRUCT_ONLY_SENTINEL`): a whole-node request arrives with every declared
/// property already in the projection, so the two sets coincide. The row-wise
/// `PropertyManager` path has no such precondition -- it reads every declared
/// property regardless -- so a caller that reaches this function with a
/// narrowed projection gets a quietly smaller map than the same request served
/// row-wise. Widening this to re-derive the column set from the batch would
/// undo the projection narrowing that keeps columnar hydration cheap, so the
/// precondition is the contract rather than a defect to fix here.
pub fn build_all_props_column_for_schema_scan(
    batch: &RecordBatch,
    vid_arr: &UInt64Array,
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    projected_properties: &[String],
    l0_ctx: &L0Context,
) -> anyhow::Result<ArrayRef> {
    // Collect schema-defined property column names (non-internal, non-overflow, non-_all_props)
    let schema_props: Vec<&str> = projected_properties
        .iter()
        .filter(|p| !is_reserved_property_name(p))
        .map(String::as_str)
        .collect();

    let num_rows = batch.num_rows();
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));
        // Build the merged map in `Value` space so typed values (temporals,
        // nested lists/maps) are preserved through to the CypherValue blob.
        let mut merged_props: HashMap<String, Value> = HashMap::new();

        // 1. Schema-defined columns
        for &prop in &schema_props {
            if let Some(col) = batch.column_by_name(prop) {
                let val = arrow_convert::arrow_to_value(col.as_ref(), i, None)?;
                if !val.is_null() {
                    merged_props.insert(prop.to_string(), val);
                }
            }
        }

        // 2. Overflow_json properties, and the `ext_id` column the flush
        //    moved a string `ext_id` into
        if let Some(arr) = overflow_arr
            && !arr.is_null(i)
            && let Ok(uni_common::Value::Map(map)) =
                uni_common::cypher_value_codec::decode(arr.value(i))
        {
            crate::storage::property_builder::merge_overflow_into(&mut merged_props, map);
        }
        restore_ext_id_column(batch, i, &mut merged_props);

        // 3. L0 buffer overlay (pending → current → transaction)
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if let Some(l0_props) = guard.vertex_properties.get(&vid) {
                for (k, v) in l0_props {
                    merged_props.insert(k.clone(), v.clone());
                }
            }
        }

        if merged_props.is_empty() {
            builder.append_null();
        } else {
            builder.append_value(uni_common::cypher_value_codec::encode(&Value::Map(
                merged_props,
            )));
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Get the property value for a VID, returning None if not found.
pub fn get_property_value(
    vid: &Vid,
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
) -> Option<Value> {
    if prop_name == "_all_props" {
        return props_map.get(vid).map(|p| {
            let map: HashMap<String, Value> =
                p.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            Value::Map(map)
        });
    }
    props_map
        .get(vid)
        .and_then(|props| props.get(prop_name))
        .cloned()
}

/// Convert a TemporalValue into a HashMap matching the Arrow struct field names,
/// so that `build_struct_property_column` can extract fields uniformly.
pub fn temporal_to_struct_map(tv: &uni_common::value::TemporalValue) -> HashMap<String, Value> {
    use uni_common::value::TemporalValue;
    let mut m = HashMap::new();
    match tv {
        TemporalValue::DateTime {
            nanos_since_epoch,
            offset_seconds,
            timezone_name,
        } => {
            m.insert("nanos_since_epoch".into(), Value::Int(*nanos_since_epoch));
            m.insert("offset_seconds".into(), Value::Int(*offset_seconds as i64));
            if let Some(tz) = timezone_name {
                m.insert("timezone_name".into(), Value::String(tz.clone()));
            }
        }
        TemporalValue::LocalDateTime { nanos_since_epoch } => {
            m.insert("nanos_since_epoch".into(), Value::Int(*nanos_since_epoch));
        }
        TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        } => {
            m.insert(
                "nanos_since_midnight".into(),
                Value::Int(*nanos_since_midnight),
            );
            m.insert("offset_seconds".into(), Value::Int(*offset_seconds as i64));
        }
        TemporalValue::LocalTime {
            nanos_since_midnight,
        } => {
            m.insert(
                "nanos_since_midnight".into(),
                Value::Int(*nanos_since_midnight),
            );
        }
        TemporalValue::Date { days_since_epoch } => {
            m.insert(
                "days_since_epoch".into(),
                Value::Int(*days_since_epoch as i64),
            );
        }
        TemporalValue::Duration {
            months,
            days,
            nanos,
        } => {
            m.insert("months".into(), Value::Int(*months));
            m.insert("days".into(), Value::Int(*days));
            m.insert("nanos".into(), Value::Int(*nanos));
        }
        TemporalValue::Btic { lo, hi, meta } => {
            m.insert("lo".into(), Value::Int(*lo));
            m.insert("hi".into(), Value::Int(*hi));
            m.insert("meta".into(), Value::Int(*meta as i64));
        }
    }
    m
}

/// Build a single Arrow column from L0 property values.
///
/// Operates on the `vid_data` map produced by `build_l0_vertex_batch`.
pub fn build_l0_property_column(
    vids: &[u64],
    vid_data: &HashMap<u64, (Properties, u64)>,
    prop_name: &str,
    data_type: &DataType,
) -> anyhow::Result<ArrayRef> {
    // Convert to Vid keys for reuse of existing build_property_column_static
    let vid_keys: Vec<Vid> = vids.iter().map(|v| Vid::from(*v)).collect();
    let props_map: HashMap<Vid, Properties> = vid_data
        .iter()
        .map(|(k, (props, _))| (Vid::from(*k), props.clone()))
        .collect();

    build_property_column_static(&vid_keys, &props_map, prop_name, data_type)
}

/// Build a numeric column from property values using the specified builder and extractor.
macro_rules! build_numeric_column {
    ($vids:expr, $props_map:expr, $prop_name:expr, $builder_ty:ty, $extractor:expr, $cast:expr) => {{
        let mut builder = <$builder_ty>::new();
        for vid in $vids {
            match get_property_value(vid, $props_map, $prop_name) {
                Some(ref v) => {
                    if let Some(val) = $extractor(v) {
                        builder.append_value($cast(val));
                    } else {
                        builder.append_null();
                    }
                }
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

/// Build an Arrow column from property values (static version).
pub fn build_property_column_static(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    data_type: &DataType,
) -> anyhow::Result<ArrayRef> {
    match data_type {
        DataType::LargeBinary => {
            // Handle CypherValue binary columns (overflow_json and Json-typed properties).
            use arrow_array::builder::LargeBinaryBuilder;
            let mut builder = LargeBinaryBuilder::new();

            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Bytes(bytes)) => {
                        builder.append_value(&bytes);
                    }
                    Some(Value::List(arr)) if arr.iter().all(|v| v.as_u64().is_some()) => {
                        // Potential raw CypherValue bytes stored as list<u8> from PropertyManager.
                        // Guard against misclassifying normal integer lists (e.g. [42, 43]) as bytes.
                        let bytes: Vec<u8> = arr
                            .iter()
                            .filter_map(|v| v.as_u64().map(|n| n as u8))
                            .collect();
                        if uni_common::cypher_value_codec::decode(&bytes).is_ok() {
                            builder.append_value(&bytes);
                        } else {
                            builder.append_value(uni_common::cypher_value_codec::encode(
                                &Value::List(arr),
                            ));
                        }
                    }
                    Some(val) => {
                        // Encode any other property value directly via the
                        // CypherValue codec so typed values (temporals, including
                        // BTIC, and nested lists/maps) round-trip losslessly.
                        builder.append_value(uni_common::cypher_value_codec::encode(&val));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            // CRDT binary properties: JSON-decoded CRDTs re-encoded to MessagePack.
            //
            // The string form has to be parsed as JSON text first. Cypher stores
            // a CRDT literal verbatim -- `DataType::accepts` passes a
            // `Value::String` through untouched, and the writer only parses when
            // a prior value exists to merge with -- so a freshly created,
            // unflushed CRDT sits in L0 as a string. Handing that to
            // `from_value` yields `Err` and the column came back null, so the
            // property was readable after a flush and null before one.
            //
            // The flush-side builder in `storage::arrow_convert` already splits
            // these two cases; this is the same split, kept in step with it.
            let mut builder = BinaryBuilder::new();
            for vid in vids {
                let bytes = get_property_value(vid, props_map, prop_name)
                    .filter(|v| !v.is_null())
                    .and_then(|v| {
                        if let Some(s) = v.as_str() {
                            serde_json::from_str::<uni_crdt::Crdt>(s).ok()
                        } else {
                            let json_val: serde_json::Value = v.into();
                            serde_json::from_value::<uni_crdt::Crdt>(json_val).ok()
                        }
                    })
                    .and_then(|crdt| crdt.to_msgpack().ok());
                match bytes {
                    Some(b) => builder.append_value(&b),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => builder.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Int64Builder,
                |v: &Value| v.as_i64(),
                |v| v
            )
        }
        DataType::Int32 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Int32Builder,
                |v: &Value| v.as_i64(),
                |v: i64| v as i32
            )
        }
        DataType::Float64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Float64Builder,
                |v: &Value| v.as_f64(),
                |v| v
            )
        }
        DataType::Float32 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Float32Builder,
                |v: &Value| v.as_f64(),
                |v: f64| v as f32
            )
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Bool(b)) => builder.append_value(b),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                UInt64Builder,
                |v: &Value| v.as_u64(),
                |v| v
            )
        }
        DataType::FixedSizeList(inner, dim) if *inner.data_type() == DataType::Float32 => {
            // Vector properties: FixedSizeList(Float32, N)
            let values_builder = Float32Builder::new();
            let mut list_builder = FixedSizeListBuilder::new(values_builder, *dim);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Vector(v)) => {
                        for val in v {
                            list_builder.values().append_value(val);
                        }
                        list_builder.append(true);
                    }
                    Some(Value::List(arr)) => {
                        for v in arr {
                            list_builder
                                .values()
                                .append_value(v.as_f64().unwrap_or(0.0) as f32);
                        }
                        list_builder.append(true);
                    }
                    _ => {
                        // Append dim nulls to inner values, then mark row as null
                        for _ in 0..*dim {
                            list_builder.values().append_null();
                        }
                        list_builder.append(false);
                    }
                }
            }
            Ok(Arc::new(list_builder.finish()))
        }
        DataType::FixedSizeList(inner, dim) if *inner.data_type() == DataType::UInt8 => {
            // Binary-vector properties: FixedSizeList(UInt8, N). Accepts a native
            // `BinaryVector` or a `List` of byte-ints (the literal input form).
            let values_builder = arrow_array::builder::UInt8Builder::new();
            let mut list_builder = FixedSizeListBuilder::new(values_builder, *dim);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::BinaryVector(b)) if b.len() == *dim as usize => {
                        for byte in b {
                            list_builder.values().append_value(byte);
                        }
                        list_builder.append(true);
                    }
                    Some(Value::List(arr)) if arr.len() == *dim as usize => {
                        let mut ok = true;
                        for v in &arr {
                            match v.as_i64() {
                                Some(n @ 0..=255) => list_builder.values().append_value(n as u8),
                                _ => {
                                    list_builder.values().append_value(0);
                                    ok = false;
                                }
                            }
                        }
                        list_builder.append(ok);
                    }
                    _ => {
                        for _ in 0..*dim {
                            list_builder.values().append_null();
                        }
                        list_builder.append(false);
                    }
                }
            }
            Ok(Arc::new(list_builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            // Timestamp properties stored as Value::Temporal, ISO 8601 strings, or i64 nanoseconds
            let mut builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(tv)) => match tv {
                        uni_common::TemporalValue::DateTime {
                            nanos_since_epoch, ..
                        }
                        | uni_common::TemporalValue::LocalDateTime {
                            nanos_since_epoch, ..
                        } => {
                            builder.append_value(nanos_since_epoch);
                        }
                        uni_common::TemporalValue::Date { days_since_epoch } => {
                            builder.append_value(days_since_epoch as i64 * 86_400_000_000_000);
                        }
                        _ => builder.append_null(),
                    },
                    Some(Value::String(s)) => match uni_common::datetime::parse_datetime_utc(&s) {
                        Ok(dt) => builder.append_value(dt.timestamp_nanos_opt().unwrap_or(0)),
                        Err(_) => builder.append_null(),
                    },
                    Some(Value::Int(n)) => {
                        builder.append_value(n);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::new();
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(uni_common::TemporalValue::Date { days_since_epoch })) => {
                        builder.append_value(days_since_epoch);
                    }
                    Some(Value::String(s)) => match NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                        Ok(d) => builder.append_value((d - epoch).num_days() as i32),
                        Err(_) => builder.append_null(),
                    },
                    Some(Value::Int(n)) => {
                        builder.append_value(n as i32);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let mut builder = Time64NanosecondBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(
                        uni_common::TemporalValue::LocalTime {
                            nanos_since_midnight,
                        }
                        | uni_common::TemporalValue::Time {
                            nanos_since_midnight,
                            ..
                        },
                    )) => {
                        builder.append_value(nanos_since_midnight);
                    }
                    Some(Value::Temporal(_)) => builder.append_null(),
                    Some(Value::String(s)) => {
                        match NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
                            .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M:%S"))
                        {
                            Ok(t) => {
                                let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                                    + t.nanosecond() as i64;
                                builder.append_value(nanos);
                            }
                            Err(_) => builder.append_null(),
                        }
                    }
                    Some(Value::Int(n)) => {
                        builder.append_value(n);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut values: Vec<Option<arrow::datatypes::IntervalMonthDayNano>> =
                Vec::with_capacity(vids.len());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(uni_common::TemporalValue::Duration {
                        months,
                        days,
                        nanos,
                    })) => {
                        values.push(Some(arrow::datatypes::IntervalMonthDayNano {
                            months: months as i32,
                            days: days as i32,
                            nanoseconds: nanos,
                        }));
                    }
                    Some(Value::Int(_n)) => {
                        values.push(None);
                    }
                    _ => values.push(None),
                }
            }
            let arr: arrow_array::IntervalMonthDayNanoArray = values.into_iter().collect();
            Ok(Arc::new(arr))
        }
        DataType::List(inner_field) => {
            build_list_property_column(vids, props_map, prop_name, inner_field)
        }
        // Sparse-vector struct must be matched BEFORE the generic struct arm
        // below (whose `build_struct_property_column` only knows scalar fields
        // and would emit Utf8 for the `List` children).
        DataType::Struct(_) if uni_common::core::schema::is_sparse_vector_struct(data_type) => {
            let values: Vec<Option<Value>> = vids
                .iter()
                .map(|vid| get_property_value(vid, props_map, prop_name))
                .collect();
            Ok(arrow_convert::build_sparse_vector_array(&values))
        }
        DataType::Struct(fields) => {
            build_struct_property_column(vids, props_map, prop_name, fields)
        }
        DataType::FixedSizeBinary(24) => {
            // BTIC temporal interval columns: encode as FixedSizeBinary(24)
            use arrow_array::builder::FixedSizeBinaryBuilder;
            const BTIC_LEN: i32 = 24;
            let mut builder = FixedSizeBinaryBuilder::with_capacity(vids.len(), BTIC_LEN);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(uni_common::TemporalValue::Btic { lo, hi, meta })) => {
                        match uni_btic::Btic::new(lo, hi, meta) {
                            Ok(b) => {
                                builder
                                    .append_value(uni_btic::encode::encode(&b))
                                    .map_err(anyhow::Error::from)?;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "BTIC coercion failed for property '{}': invalid value (lo={}, hi={}, meta={:#x}): {}",
                                    prop_name,
                                    lo,
                                    hi,
                                    meta,
                                    e
                                );
                                builder.append_null()
                            }
                        }
                    }
                    Some(Value::String(s)) => match uni_btic::parse::parse_btic_literal(&s) {
                        Ok(b) => {
                            builder
                                .append_value(uni_btic::encode::encode(&b))
                                .map_err(anyhow::Error::from)?;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "BTIC coercion failed for property '{}': '{}' is not a valid BTIC literal: {}",
                                prop_name,
                                s,
                                e
                            );
                            builder.append_null()
                        }
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Default: convert to string
        _ => {
            let mut builder = StringBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => builder.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Build a List-typed Arrow column from list property values.
pub fn build_list_property_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    inner_field: &Arc<Field>,
) -> anyhow::Result<ArrayRef> {
    match inner_field.data_type() {
        DataType::Utf8 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v {
                                Value::String(s) => builder.values().append_value(s),
                                Value::Null => builder.values().append_null(),
                                other => builder.values().append_value(format!("{other:?}")),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v.as_i64() {
                                Some(n) => builder.values().append_value(n),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v.as_f64() {
                                Some(n) => builder.values().append_value(n),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v.as_bool() {
                                Some(b) => builder.values().append_value(b),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Map types are List(Struct(key, value)) — build struct inner elements.
        // Any other struct element (a DateTime or Time) is a value of its own.
        DataType::Struct(fields)
            if fields.len() == 2 && fields[0].name() == "key" && fields[1].name() == "value" =>
        {
            build_list_of_structs_column(vids, props_map, prop_name, fields)
        }
        DataType::LargeBinary
            if inner_field
                .metadata()
                .get("uni_raw_bytes")
                .is_some_and(|v| v == "true") =>
        {
            // Typed `List(Bytes)`: store each buffer verbatim in a `LargeBinary`
            // child. The child field (reused from the schema) carries the
            // `uni_raw_bytes` marker so the read path decodes it as raw `Bytes`.
            // CV-encoded `LargeBinary` lists lack the marker and keep the string
            // fallback below — no pattern-comprehension/VLP regression.
            let mut builder = ListBuilder::new(arrow_array::builder::LargeBinaryBuilder::new())
                .with_field(inner_field.clone());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            if let Value::Bytes(b) = v {
                                builder.values().append_value(b);
                            } else {
                                builder.values().append_null();
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Multi-vector (`List<FixedSizeList<Float32>>`): build the typed
        // multi-vector column from the owned L0 values, mirroring the write path.
        // Without this arm the value falls to the string fallback below and yields
        // `List<Utf8>`, which mismatches the declared schema type when the result
        // batch is assembled (`RETURN d.tokens` on unflushed/L0 rows).
        DataType::FixedSizeList(child, dim) if matches!(child.data_type(), DataType::Float32) => {
            let values: Vec<Option<Value>> = vids
                .iter()
                .map(|vid| get_property_value(vid, props_map, prop_name))
                .collect();
            Ok(arrow_convert::build_multivector_array(
                &values,
                *dim as usize,
            ))
        }
        // Every other element type: built as one child column by the scalar
        // builder for that Arrow type, so an element is laid out exactly as a
        // top-level column of its type. The previous fallback wrote each
        // element's `Debug` text into a `Utf8` child, which did not match the
        // declared type: a `List<DateTime>` read from L0 failed with "expected
        // Timestamp(ns) got Utf8".
        _ => {
            let mut offsets: Vec<i32> = Vec::with_capacity(vids.len() + 1);
            let mut valid: Vec<bool> = Vec::with_capacity(vids.len());
            let mut element_vids: Vec<Vid> = Vec::new();
            let mut elements: HashMap<Vid, Properties> = HashMap::new();
            offsets.push(0);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            let key = Vid::new(element_vids.len() as u64);
                            elements.insert(key, HashMap::from([("item".to_string(), v)]));
                            element_vids.push(key);
                        }
                        valid.push(true);
                    }
                    _ => valid.push(false),
                }
                offsets.push(
                    i32::try_from(element_vids.len())
                        .map_err(|_| anyhow::anyhow!("list column '{prop_name}' is too large"))?,
                );
            }
            let child = build_property_column_static(
                &element_vids,
                &elements,
                "item",
                inner_field.data_type(),
            )?;
            Ok(Arc::new(arrow_array::ListArray::try_new(
                inner_field.clone(),
                arrow::buffer::OffsetBuffer::new(offsets.into()),
                child,
                Some(arrow::buffer::NullBuffer::from(valid)),
            )?))
        }
    }
}

/// Build a List(Struct(...)) column, used for Map-type properties.
///
/// Handles two value representations:
/// - `Value::List([Map{key: k, value: v}, ...])` — pre-converted kv pairs
/// - `Value::Map({k1: v1, k2: v2})` — raw map objects (converted to kv pairs)
pub fn build_list_of_structs_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    fields: &Fields,
) -> anyhow::Result<ArrayRef> {
    use arrow_array::StructArray;

    let values: Vec<Option<Value>> = vids
        .iter()
        .map(|vid| get_property_value(vid, props_map, prop_name))
        .collect();

    // Convert each row's value to an owned Vec of Maps (key-value pairs).
    // This normalizes both List-of-maps and Map representations.
    let rows: Vec<Option<Vec<HashMap<String, Value>>>> = values
        .iter()
        .map(|val| match val {
            Some(Value::List(arr)) => {
                let objs: Vec<HashMap<String, Value>> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Value::Map(m) = v {
                            Some(m.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if objs.is_empty() { None } else { Some(objs) }
            }
            Some(Value::Map(obj)) => {
                // Map property: convert {k1: v1, k2: v2} -> [{key: k1, value: v1}, ...]
                let kv_pairs: Vec<HashMap<String, Value>> = obj
                    .iter()
                    .map(|(k, v)| {
                        let mut m = HashMap::new();
                        m.insert("key".to_string(), Value::String(k.clone()));
                        m.insert("value".to_string(), v.clone());
                        m
                    })
                    .collect();
                Some(kv_pairs)
            }
            _ => None,
        })
        .collect();

    let total_items: usize = rows
        .iter()
        .filter_map(|r| r.as_ref())
        .map(|v| v.len())
        .sum();

    // Build child arrays for each field in the struct
    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| {
            let field_name = field.name();
            match field.data_type() {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(total_items, total_items * 16);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name) {
                            Some(Value::String(s)) => builder.append_value(s),
                            Some(Value::Null) | None => builder.append_null(),
                            Some(other) => builder.append_value(format!("{other:?}")),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(total_items);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name).and_then(|v| v.as_i64()) {
                            Some(n) => builder.append_value(n),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(total_items);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name).and_then(|v| v.as_f64()) {
                            Some(n) => builder.append_value(n),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                // Typed `Map(_, Bytes)` value child: the schema field carries the
                // `uni_raw_bytes` marker; store each buffer verbatim in a
                // `LargeBinary` so the read path (`try_reconstruct_map`) decodes it
                // as raw `Bytes`. CV-encoded values lack the marker → string fallback.
                DataType::LargeBinary
                    if field
                        .metadata()
                        .get("uni_raw_bytes")
                        .is_some_and(|v| v == "true") =>
                {
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name) {
                            Some(Value::Bytes(b)) => builder.append_value(b),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                // Fallback: serialize as string
                _ => {
                    let mut builder = StringBuilder::with_capacity(total_items, total_items * 16);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name) {
                            Some(Value::Null) | None => builder.append_null(),
                            Some(other) => builder.append_value(format!("{other:?}")),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            }
        })
        .collect();

    // Build struct array from children
    let struct_array = StructArray::try_new(fields.clone(), child_arrays, None)
        .map_err(|e| anyhow::Error::from(Box::new(e)))?;

    // Build list offsets
    let mut offsets = Vec::with_capacity(vids.len() + 1);
    let mut nulls = Vec::with_capacity(vids.len());
    let mut offset = 0i32;
    offsets.push(offset);
    for row in &rows {
        match row {
            Some(objs) => {
                offset += objs.len() as i32;
                offsets.push(offset);
                nulls.push(true);
            }
            None => {
                offsets.push(offset);
                nulls.push(false);
            }
        }
    }

    let list_field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));
    let list_array = arrow_array::ListArray::try_new(
        list_field,
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
        Arc::new(struct_array),
        Some(arrow::buffer::NullBuffer::from(nulls)),
    )
    .map_err(|e| anyhow::Error::from(Box::new(e)))?;

    Ok(Arc::new(list_array))
}

/// Build a Struct-typed Arrow column from Map property values (e.g. Point types).
pub fn build_struct_property_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    fields: &Fields,
) -> anyhow::Result<ArrayRef> {
    use arrow_array::StructArray;

    // Convert raw values, expanding Temporal values into Map representation
    // so the struct field extraction below works uniformly.
    let values: Vec<Option<Value>> = vids
        .iter()
        .map(|vid| {
            let val = get_property_value(vid, props_map, prop_name);
            match val {
                Some(Value::Temporal(ref tv)) => Some(Value::Map(temporal_to_struct_map(tv))),
                other => other,
            }
        })
        .collect();

    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| {
            let field_name = field.name();
            match field.data_type() {
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_f64()) {
                                    Some(n) => builder.append_value(n),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(vids.len(), vids.len() * 16);
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => match obj.get(field_name) {
                                Some(Value::String(s)) => builder.append_value(s),
                                Some(Value::Null) | None => builder.append_null(),
                                Some(other) => builder.append_value(format!("{other:?}")),
                            },
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_i64()) {
                                    Some(n) => builder.append_value(n),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Timestamp(_, _) => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_i64()) {
                                    Some(n) => builder.append_value(n),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_i64()) {
                                    Some(n) => builder.append_value(n as i32),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Time64(_) => {
                    let mut builder = Time64NanosecondBuilder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_i64()) {
                                    Some(n) => builder.append_value(n),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                // Fallback: serialize as string
                _ => {
                    let mut builder = StringBuilder::with_capacity(vids.len(), vids.len() * 16);
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => match obj.get(field_name) {
                                Some(Value::Null) | None => builder.append_null(),
                                Some(other) => builder.append_value(format!("{other:?}")),
                            },
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            }
        })
        .collect();

    // Build null bitmap — null when the value is null/missing
    let nulls: Vec<bool> = values
        .iter()
        .map(|v| matches!(v, Some(Value::Map(_))))
        .collect();

    let struct_array = StructArray::try_new(
        fields.clone(),
        child_arrays,
        Some(arrow::buffer::NullBuffer::from(nulls)),
    )
    .map_err(|e| anyhow::Error::from(Box::new(e)))?;

    Ok(Arc::new(struct_array))
}

/// MVCC deduplication: keep only the highest-version row for each `_vid`.
///
/// Sorts by (_vid ASC, _version DESC), then keeps the first occurrence of each
/// _vid (= the highest version). This is a pure Arrow-compute operation.
#[cfg(test)]
pub fn mvcc_dedup_batch(batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    mvcc_dedup_batch_by(batch, "_vid")
}

/// Dedup a Lance batch and return `Some` only when rows remain.
///
/// Wraps the common pattern of dedup + empty-check that appears in every
/// columnar scan path (vertex, edge, schemaless).
pub fn mvcc_dedup_to_option(
    batch: Option<RecordBatch>,
    id_column: &str,
) -> anyhow::Result<Option<RecordBatch>> {
    match batch {
        Some(b) => {
            let deduped = mvcc_dedup_batch_by(&b, id_column)?;
            Ok(if deduped.num_rows() > 0 {
                Some(deduped)
            } else {
                None
            })
        }
        None => Ok(None),
    }
}

/// Merge a deduped Lance batch with an L0 batch, re-deduplicating the combined
/// result. Returns an empty batch (against `output_schema`) when both inputs
/// are empty.
///
/// `counters`, when present, records how many rows each tier contributed. This
/// is the one place in the scan path that already knows the answer — the
/// `match` below exists precisely to distinguish storage-only, L0-only and
/// both — so counting here costs a pair of adds and needs no new branching.
/// Rows are counted **before** the combined dedup, so the numbers describe what
/// each tier *served*, not what survived MVCC resolution.
pub fn merge_lance_and_l0(
    lance_deduped: Option<RecordBatch>,
    l0_batch: RecordBatch,
    internal_schema: &SchemaRef,
    id_column: &str,
    counters: Option<&Arc<crate::QueryCounters>>,
) -> anyhow::Result<Option<RecordBatch>> {
    let has_l0 = l0_batch.num_rows() > 0;
    if let Some(c) = counters {
        let lance_rows = lance_deduped.as_ref().map_or(0, |b| b.num_rows());
        let l0_rows = l0_batch.num_rows();
        c.add_storage_rows(lance_rows);
        c.add_l0_rows(l0_rows);
        c.add_rows_scanned(lance_rows + l0_rows);
    }
    match (lance_deduped, has_l0) {
        (Some(lance), true) => {
            let combined = arrow::compute::concat_batches(internal_schema, &[lance, l0_batch])
                .map_err(anyhow::Error::from)?;
            Ok(Some(mvcc_dedup_batch_by(&combined, id_column)?))
        }
        (Some(lance), false) => Ok(Some(lance)),
        (None, true) => Ok(Some(l0_batch)),
        (None, false) => Ok(None),
    }
}

/// Drop rows superseded by a newer persisted version that the pushed
/// property predicate filtered out (issue #57 × MVCC-append tables).
///
/// Lance evaluates a pushed property predicate per ROW, before the per-vid
/// max-`_version` pick — so when a vid's property was rewritten and
/// re-flushed, its CURRENT row fails the predicate and never reaches the
/// dedup, while the stale still-matching row wins it by default. Re-reads
/// `_vid`/`_version` for the candidate vids WITHOUT the property predicate
/// (per-label table when `label_table` is `Some`, the main vertex table
/// otherwise) and keeps only rows carrying their vid's true maximum
/// persisted version. Must run on the RAW filtered batch, before
/// [`mvcc_dedup_to_option`].
pub async fn drop_superseded_pushdown_rows(
    storage: &Arc<crate::storage::manager::StorageManager>,
    label_table: Option<&str>,
    batch: RecordBatch,
) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    let (Some(vid_col), Some(ver_col)) = (
        batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
        batch
            .column_by_name("_version")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
    ) else {
        return Err(anyhow::anyhow!(
            "pushdown version verification: scan batch missing _vid/_version".to_string(),
        ));
    };

    let mut candidates: Vec<u64> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();
    for i in 0..vid_col.len() {
        let vid = vid_col.value(i);
        if seen.insert(vid) {
            candidates.push(vid);
        }
    }

    // True max persisted version per candidate vid — unfiltered apart from
    // the vid list, so rewritten-key rows and deletion tombstones are seen.
    // Chunked to bound the `_vid IN (…)` filter-string size.
    const VERIFY_CHUNK: usize = 1000;
    // A dense candidate set is read back in one `_vid` range instead: the
    // chunked lookups cost one storage query per 1000 candidates, which made
    // a non-selective indexed range (perf audit F1: `n.x IS NOT NULL` over
    // 32,000 rows, 30 chunks) 3x slower than not pushing it at all — measured
    // 126 ms against 37 ms unpushed, and 39 ms with the check skipped. Two
    // narrow `u64` columns over the candidates' vid span are cheaper than
    // that once the candidates fill at least 1/`DENSE_SPAN_RATIO` of the span;
    // a sparse set keeps the chunked lookups, so a few thousand matches in a
    // huge table never trigger a whole-table read.
    const DENSE_SPAN_RATIO: u64 = 16;
    let (lo, hi) = (
        candidates.iter().copied().min().unwrap_or(0),
        candidates.iter().copied().max().unwrap_or(0),
    );
    let span = hi - lo + 1;
    let dense = candidates.len() > VERIFY_CHUNK
        && (candidates.len() as u64).saturating_mul(DENSE_SPAN_RATIO) >= span;
    let filters: Vec<FilterExpr> = if dense {
        vec![FilterExpr::all([
            FilterExpr::compare("_vid", CmpOp::GtEq, Scalar::UInt(lo)),
            FilterExpr::compare("_vid", CmpOp::LtEq, Scalar::UInt(hi)),
        ])]
    } else {
        candidates
            .chunks(VERIFY_CHUNK)
            .map(|chunk| FilterExpr::one_of("_vid", chunk.iter().map(|v| Scalar::UInt(*v))))
            .collect()
    };
    let mut max_ver: HashMap<u64, u64> = HashMap::with_capacity(candidates.len());
    for filter in filters {
        let scanned = match label_table {
            Some(label) => {
                storage
                    .scan_vertex_table(label, &["_vid", "_version"], Some(&filter))
                    .await
            }
            None => {
                storage
                    .scan_main_vertex_table(&["_vid", "_version"], Some(&filter))
                    .await
            }
        }?;
        let Some(vbatch) = scanned else { continue };
        let (Some(v_vid), Some(v_ver)) = (
            vbatch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            vbatch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
        ) else {
            return Err(anyhow::anyhow!(
                "pushdown version verification: rescan missing _vid/_version".to_string(),
            ));
        };
        for i in 0..v_vid.len() {
            let entry = max_ver.entry(v_vid.value(i)).or_insert(0);
            *entry = (*entry).max(v_ver.value(i));
        }
    }

    let keep: arrow_array::BooleanArray = (0..batch.num_rows())
        .map(|i| {
            Some(
                max_ver
                    .get(&vid_col.value(i))
                    .is_none_or(|&max| ver_col.value(i) >= max),
            )
        })
        .collect();
    arrow::compute::filter_record_batch(&batch, &keep).map_err(anyhow::Error::from)
}

/// MVCC deduplication: keep only the highest-version row for each unique value
/// in the given `id_column`.
///
/// Sorts by (id_column ASC, _version DESC), then keeps the first occurrence of
/// each id (= the highest version). This is a pure Arrow-compute operation.
pub fn mvcc_dedup_batch_by(batch: &RecordBatch, id_column: &str) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let id_col = batch
        .column_by_name(id_column)
        .ok_or_else(|| anyhow::anyhow!(format!("Missing {} column", id_column)))?
        .clone();
    let version_col = batch
        .column_by_name("_version")
        .ok_or_else(|| anyhow::anyhow!("Missing _version column".to_string()))?
        .clone();

    // Sort by (id ASC, _version DESC, position DESC).
    //
    // The position key is what makes this deterministic.
    // `lexsort_to_indices` is an *unstable* sort — arrow-ord routes a
    // fixed-width lexsort to `sort_unstable_by` — so with only (id, version)
    // as keys the winner of a version tie was whichever row the sort happened
    // to leave first, and not reproducible between runs.
    //
    // Ties are reachable rather than theoretical: a transaction L0 forks
    // `current_version` from the main L0, so a tx write and a main write can
    // carry the same version, and `merge_lance_and_l0` concatenates both tiers
    // into one batch before this runs.
    //
    // Position descending resolves them as "the later row wins", which is the
    // rule the row-wise path already applies: the concatenation puts Lance
    // first and L0 second, so L0 takes a tie, and within one tier a
    // later-appended MVCC row takes it. Sorting on a synthetic array rather
    // than a batch column keeps the schema untouched.
    let position: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..batch.num_rows() as u64));
    // A delete is monotone, so a tombstone outranks a live row at the same
    // version. Without this a live row at equal version could win the tie and
    // resurrect a deleted id -- the same class of bug as review finding C2 on
    // the main tables, where an *older* live row could beat a tombstone.
    //
    // Ranked below `_version`, so a genuinely newer live row still un-deletes,
    // and above position, so it decides before "later row wins". `_deleted` is
    // optional: a table without the column is treated as having nothing
    // deleted, matching `filter_deleted_rows`.
    let deleted_rank = batch.column_by_name("_deleted").cloned();
    let mut sort_columns = vec![
        arrow::compute::SortColumn {
            values: id_col,
            options: Some(arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            }),
        },
        arrow::compute::SortColumn {
            values: version_col,
            options: Some(arrow::compute::SortOptions {
                descending: true,
                nulls_first: false,
            }),
        },
    ];
    if let Some(deleted) = deleted_rank {
        // `true` sorts above `false` under descending.
        sort_columns.push(arrow::compute::SortColumn {
            values: deleted,
            options: Some(arrow::compute::SortOptions {
                descending: true,
                nulls_first: false,
            }),
        });
    }
    sort_columns.push(arrow::compute::SortColumn {
        values: position,
        options: Some(arrow::compute::SortOptions {
            descending: true,
            nulls_first: false,
        }),
    });
    let indices =
        arrow::compute::lexsort_to_indices(&sort_columns, None).map_err(anyhow::Error::from)?;

    // Reorder all columns by sorted indices
    let sorted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<Result<_, _>>()
        .map_err(anyhow::Error::from)?;
    let sorted =
        RecordBatch::try_new(batch.schema(), sorted_columns).map_err(anyhow::Error::from)?;

    // Build dedup mask: keep first occurrence of each id
    let sorted_id = sorted
        .column_by_name(id_column)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let mut keep = vec![false; sorted.num_rows()];
    if !keep.is_empty() {
        keep[0] = true;
        for (i, flag) in keep.iter_mut().enumerate().skip(1) {
            if sorted_id.value(i) != sorted_id.value(i - 1) {
                *flag = true;
            }
        }
    }

    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(&sorted, &mask).map_err(anyhow::Error::from)
}

/// Filter out rows where `_deleted = true` after MVCC dedup.
pub fn filter_deleted_rows(batch: &RecordBatch) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let deleted_col = match batch.column_by_name("_deleted") {
        Some(col) => col
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap(),
        None => return Ok(batch.clone()),
    };
    let keep: Vec<bool> = (0..deleted_col.len())
        .map(|i| !deleted_col.value(i))
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask).map_err(anyhow::Error::from)
}

/// Filter out rows whose `_vid` appears in L0 tombstones.
pub fn filter_l0_tombstones(
    batch: &RecordBatch,
    l0_ctx: &L0Context,
) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut tombstones: HashSet<u64> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        for vid in guard.vertex_tombstones.iter() {
            tombstones.insert(vid.as_u64());
        }
    }

    if tombstones.is_empty() {
        return Ok(batch.clone());
    }

    let vid_col = batch
        .column_by_name("_vid")
        .ok_or_else(|| anyhow::anyhow!("Missing _vid column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let keep: Vec<bool> = (0..vid_col.len())
        .map(|i| !tombstones.contains(&vid_col.value(i)))
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask).map_err(anyhow::Error::from)
}

/// Drop rows for a known-label scan whose newest L0 label-overwrite no longer
/// includes the scanned label(s).
///
/// A flushed vertex's stored `labels` array still lists a label after a
/// `REMOVE n:Label` — the removal only updated L0. The label-scan candidate set
/// unions that stale flushed row, and neither the `_deleted` nor the vid-tombstone
/// filter drops it. When the newest L0 buffer carrying the vid flagged it in
/// `vertex_label_overwrites` (a `SET`/`REMOVE` that resolved its full label set),
/// that set is authoritative: keep the row only if it still contains every
/// requested label. Otherwise the label was resurrected in `MATCH (n:Label)`.
///
/// `label` may be `"A:B"` (all required) or empty (bare `MATCH (n)` — nothing to
/// filter). Mirrors the multi-label membership check in
/// `build_l0_schemaless_vertex_batch`.
///
/// # Errors
/// Returns an error if the `_vid` column is missing or the mask filter fails.
pub fn filter_l0_label_overwrites(
    batch: &RecordBatch,
    label: &str,
    l0_ctx: &L0Context,
) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 || label.is_empty() {
        return Ok(batch.clone());
    }
    let required: Vec<&str> = label.split(':').collect();

    // vid -> resolved label set from the NEWEST buffer that marked it as a full
    // label overwrite. `iter_l0_buffers` yields oldest -> newest, so later writes
    // win.
    let mut overwritten: HashMap<u64, Vec<String>> = HashMap::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        for vid in guard.vertex_label_overwrites.iter() {
            let labels = guard.vertex_labels.get(vid).cloned().unwrap_or_default();
            overwritten.insert(vid.as_u64(), labels);
        }
    }
    if overwritten.is_empty() {
        return Ok(batch.clone());
    }

    let vid_col = batch
        .column_by_name("_vid")
        .ok_or_else(|| anyhow::anyhow!("Missing _vid column".to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let keep: Vec<bool> = (0..vid_col.len())
        .map(|i| match overwritten.get(&vid_col.value(i)) {
            // The vid's newest overwrite resolved its full label set: keep only
            // if that set still contains every requested label.
            Some(resolved) => required.iter().all(|lf| resolved.iter().any(|l| l == lf)),
            // No overwrite for this vid: the stored (Lance or L0) labels stand.
            None => true,
        })
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask).map_err(anyhow::Error::from)
}

/// Flushed properties of the L0 vertices a label mutation alone put under
/// `label`, with each one's label-mutation version.
///
/// A `SET n:L` / `REMOVE n:L` on a flushed vertex leaves L0 holding its new
/// labels but no property row, and the vertex's flushed row sits under its
/// old labels, so a scan of a label it gained found neither: the L0 row came
/// back with every property NULL. These are the vids the L0 builders must
/// start from the flushed properties (with types intact, from the main
/// table's `props_json`) and stamp with the relabel's version, so the row
/// they emit is the vertex's complete, newest one.
///
/// `label` may be `"A:B"` (all required) or empty. `in_targets` mirrors the
/// scan's vid pushdown or range. A vid with a property row in any buffer is
/// skipped: L0 property rows are always complete.
///
/// # Errors
/// Propagates the main-table read's failures.
pub async fn label_only_l0_base_props(
    storage: &StorageManager,
    l0_ctx: &L0Context,
    label: &str,
    in_targets: impl Fn(u64) -> bool,
    counters: Option<&Arc<crate::QueryCounters>>,
) -> anyhow::Result<HashMap<u64, (Properties, u64)>> {
    let required: Vec<&str> = label.split(':').filter(|l| !l.is_empty()).collect();
    let mut tombstoned: HashSet<u64> = HashSet::new();
    // vid -> (newest resolved label set, newest label-mutation version)
    let mut relabeled: HashMap<u64, (Vec<String>, u64)> = HashMap::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        tombstoned.extend(guard.vertex_tombstones.iter().map(|v| v.as_u64()));
        for vid in &guard.vertex_label_overwrites {
            let labels = guard.vertex_labels.get(vid).cloned().unwrap_or_default();
            let version = guard.vertex_label_versions.get(vid).copied().unwrap_or(0);
            let entry = relabeled.entry(vid.as_u64()).or_insert((Vec::new(), 0));
            entry.0 = labels;
            entry.1 = entry.1.max(version);
        }
    }
    if relabeled.is_empty() {
        return Ok(HashMap::new());
    }
    // Probed per relabeled vid rather than collected from every buffer's
    // property map: that collection made each scan O(all L0 vertices) even
    // when the scan was a point lookup and nothing was relabeled (W3b).
    let mut has_props: HashSet<u64> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        has_props.extend(
            relabeled
                .keys()
                .filter(|vid| guard.vertex_properties.contains_key(&Vid::from(**vid))),
        );
    }
    let vids: Vec<Vid> = relabeled
        .iter()
        .filter(|(vid, (labels, _))| {
            !has_props.contains(vid)
                && !tombstoned.contains(vid)
                && in_targets(**vid)
                && required.iter().all(|r| labels.iter().any(|l| l == r))
        })
        .map(|(vid, _)| Vid::from(*vid))
        .collect();
    if vids.is_empty() {
        return Ok(HashMap::new());
    }
    let props = crate::storage::main_vertex::MainVertexDataset::find_batch_props_by_vids_counted(
        storage.backend(),
        &vids,
        storage.row_visibility(),
        counters,
    )
    .await?;
    Ok(props
        .into_iter()
        .map(|(vid, p)| {
            let raw = vid.as_u64();
            (raw, (p, relabeled[&raw].1))
        })
        .collect())
}

/// Which of a label's L0 rows one scan call wants.
///
/// Replaces an `Option<&[u64]>`, where `None` meant "the whole label". A range
/// has no enumeration source to hand in — the caller knows bounds, not members
/// — so it cannot be expressed as a vid list, and collapsing it to `None` would
/// make every range of a chunked scan re-emit the label's entire L0 set. Making
/// the three cases distinct is what keeps that unrepresentable.
#[derive(Debug, Clone, Copy)]
pub enum L0VertexTargets<'a> {
    /// Exactly these vids, from an `id(x) = ?` or `id(x) IN [...]` pushdown.
    Vids(&'a [u64]),
    /// Every vid of the label in the half-open range `[lo, hi)` (#214).
    Range(u64, u64),
    /// Every vid of the label.
    All,
    /// The label's vids — within `range` when one is set — that are also in
    /// `vids`.
    ///
    /// Unlike [`Self::Vids`], membership is decided exactly as [`Self::All`]
    /// and [`Self::Range`] decide it (the label set alone, with no property-row
    /// requirement), so a row this selects is built byte-for-byte as the
    /// unrestricted scan would have built it. Produced by
    /// [`l0_prefilter_candidates`] to skip L0 rows a pushed equality or range
    /// cannot match before they are materialized (W3b, F1).
    Filtered {
        /// Half-open `_vid` range the scan is walking, if any.
        range: Option<(u64, u64)>,
        /// The vids that may survive the scan's filters.
        vids: &'a HashSet<u64>,
    },
}

impl L0VertexTargets<'_> {
    /// Whether `vid` is one of the rows these targets select.
    pub fn contains(&self, vid: u64) -> bool {
        match self {
            Self::Vids(vids) => vids.contains(&vid),
            Self::Range(lo, hi) => vid >= *lo && vid < *hi,
            Self::All => true,
            Self::Filtered { range, vids } => {
                range.is_none_or(|(lo, hi)| vid >= lo && vid < hi) && vids.contains(&vid)
            }
        }
    }
}

/// Pushed constraints on one declared property, for pruning L0 rows early.
///
/// `property IN one_of` (one value for `=`) when `one_of` is set, and
/// `property op value` for every entry of `bounds`. It is a **pre-filter**: it
/// may keep a row the scan's runtime filter later rejects, but must never drop
/// a row that filter would accept, because the runtime filter — and the
/// residual `FilterExec` above the scan — stay authoritative for three-valued
/// logic and type coercion. [`l0_value_may_equal`] and
/// [`l0_value_may_satisfy`] are where that one-sided contract is kept.
///
/// Equality came first (W3b); range bounds were added with the F1 range
/// pushdown, so an indexed `n.x < 10` over unflushed rows reads the matching
/// L0 rows instead of the label's whole L0 set.
#[derive(Debug, Clone, PartialEq)]
pub struct L0PropertyPrefilter {
    /// Declared property the constraints apply to.
    pub property: String,
    /// Literals the property must equal one of, when the filter pins it.
    pub one_of: Option<Vec<Value>>,
    /// `(op, literal)` comparisons the property must all satisfy. `op` is one
    /// of `<`, `<=`, `>`, `>=`, with the property on the left.
    pub bounds: Vec<(CmpOp, Value)>,
}

impl L0PropertyPrefilter {
    /// Whether a stored L0 value of the property could pass every constraint.
    pub fn may_match(
        &self,
        column_type: &uni_common::core::schema::DataType,
        stored: Option<&Value>,
    ) -> bool {
        self.one_of.as_ref().is_none_or(|values| {
            values
                .iter()
                .any(|lit| l0_value_may_equal(column_type, stored, lit))
        }) && self
            .bounds
            .iter()
            .all(|(op, lit)| l0_value_may_satisfy(column_type, stored, *op, lit))
    }
}

/// Whether a stored L0 value could satisfy `column = literal` after the scan
/// converts it to the declared column type.
///
/// Returns `false` only when the conversion is the identity and the two values
/// are plainly unequal under the comparison the runtime filter performs, so the
/// answer is never `false` for a row that filter would keep:
///
/// * a missing or NULL value becomes a NULL cell, and `NULL = x` is never true;
/// * `String` in a `STRING` column against a string literal compares bytes;
/// * `Int` in an `INT64` column against an integer literal compares integers;
/// * `Int` or `Float` in a `FLOAT64` column against a numeric literal compares
///   as `f64` (the builder widens with `as_f64`), keeping any `NaN`;
/// * `Bool` in a `BOOL` column against a boolean literal compares booleans.
///
/// Every other pairing — a value of another type that the column builder would
/// coerce (`Int` into a `STRING` column renders `"5"`, `Float` into an `INT64`
/// column truncates), a cross-type literal, any other column type — answers
/// `true` and leaves the decision to the runtime filter.
pub fn l0_value_may_equal(
    column_type: &uni_common::core::schema::DataType,
    stored: Option<&Value>,
    literal: &Value,
) -> bool {
    use uni_common::core::schema::DataType as T;
    let Some(stored) = stored else {
        return false;
    };
    match (column_type, stored, literal) {
        (_, Value::Null, _) => false,
        (T::String, Value::String(a), Value::String(b)) => a == b,
        (T::Int64, Value::Int(a), Value::Int(b)) => a == b,
        (T::Float64, Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => {
            match (stored.as_f64(), literal.as_f64()) {
                (Some(a), Some(b)) if !a.is_nan() && !b.is_nan() => a == b,
                _ => true,
            }
        }
        (T::Bool, Value::Bool(a), Value::Bool(b)) => a == b,
        _ => true,
    }
}

/// Whether a stored L0 value could satisfy `column op literal` after the scan
/// converts it to the declared column type, for `op` one of `<`, `<=`, `>`,
/// `>=`.
///
/// The same one-sided contract as [`l0_value_may_equal`]: `false` only when
/// the conversion is the identity and the comparison is plainly false.
///
/// * a missing or NULL value is a NULL cell, and a comparison with NULL is
///   never true;
/// * `String` in a `STRING` column against a string literal compares bytes, as
///   the runtime filter does;
/// * `Int` in an `INT64` column against an integer literal compares integers;
/// * `Int` or `Float` in a `FLOAT64` column against a numeric literal compares
///   as `f64`, except that a NaN on either side, or a zero on both, answers
///   `true`: the runtime filter orders floats by IEEE total order (NaN above
///   everything, `-0.0` below `+0.0`), which disagrees with `f64`'s own
///   comparison exactly there.
///
/// Every other pairing answers `true`.
pub fn l0_value_may_satisfy(
    column_type: &uni_common::core::schema::DataType,
    stored: Option<&Value>,
    op: CmpOp,
    literal: &Value,
) -> bool {
    use std::cmp::Ordering;
    use uni_common::core::schema::DataType as T;
    let Some(stored) = stored else {
        return false;
    };
    let ordering = match (column_type, stored, literal) {
        (_, Value::Null, _) => return false,
        (T::String, Value::String(a), Value::String(b)) => a.as_bytes().cmp(b.as_bytes()),
        (T::Int64, Value::Int(a), Value::Int(b)) => a.cmp(b),
        (T::Float64, Value::Int(_) | Value::Float(_), Value::Int(_) | Value::Float(_)) => {
            match (stored.as_f64(), literal.as_f64()) {
                (Some(a), Some(b)) if !a.is_nan() && !b.is_nan() && !(a == 0.0 && b == 0.0) => {
                    match a.partial_cmp(&b) {
                        Some(o) => o,
                        None => return true,
                    }
                }
                _ => return true,
            }
        }
        _ => return true,
    };
    match op {
        CmpOp::Lt => ordering == Ordering::Less,
        CmpOp::LtEq => ordering != Ordering::Greater,
        CmpOp::Gt => ordering == Ordering::Greater,
        CmpOp::GtEq => ordering != Ordering::Less,
        CmpOp::Eq | CmpOp::NotEq => true,
    }
}

/// The L0 vids of `label` that the scan must still build under `prefilter`.
///
/// A vid is kept when its merged L0 value of the property may pass the
/// prefilter ([`L0PropertyPrefilter::may_match`]), **or** when `shadowed`
/// holds it. The
/// second arm is the shadowing contract: `shadowed` is the set of vids the
/// storage side of the same scan returned, and an L0 row for one of them must
/// reach the MVCC merge even when it no longer matches — it is the newer
/// version, and dropping it would let the stale flushed row win the dedup and
/// pass the filter. Every other non-matching L0 row is the newest version of a
/// vid the storage side did not return, so the runtime filter would reject it
/// anyway; skipping it changes cost, not results.
///
/// "Merged value" mirrors [`build_l0_vertex_batch`]: buffers in visibility
/// order, a buffer contributing only for vids in its own label set, the later
/// buffer's value for the key winning; a relabeled vid in `label_only` starts
/// from its flushed properties, which replace the L0 ones wholesale.
///
/// The pass touches one hash entry per L0 label member and clones nothing,
/// which is what replaces materializing every row (clone every property, build
/// every column, sort and dedup) for each lookup.
pub fn l0_prefilter_candidates(
    l0_ctx: &L0Context,
    label: &str,
    range: Option<(u64, u64)>,
    column_type: &uni_common::core::schema::DataType,
    prefilter: &L0PropertyPrefilter,
    label_only: &HashMap<u64, (Properties, u64)>,
    shadowed: impl IntoIterator<Item = u64>,
) -> HashSet<u64> {
    let in_range = |vid: u64| range.is_none_or(|(lo, hi)| vid >= lo && vid < hi);
    let may_equal = |stored: Option<&Value>| prefilter.may_match(column_type, stored);
    let prop = prefilter.property.as_str();
    // Buffers come oldest first, so the last verdict per vid is its merged
    // one. Recording it as membership -- insert on a possible match, remove
    // otherwise -- keeps that semantics while allocating only for the vids
    // that may match: the pass is still one probe per L0 label member, but
    // no longer one map insert per member as well.
    let mut keep: HashSet<u64> = HashSet::new();
    let mut record = |raw: u64, may: bool| {
        if may {
            keep.insert(raw);
        } else {
            keep.remove(&raw);
        }
    };
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        let Some(members) = guard.label_to_vids.get(label) else {
            continue;
        };
        for vid in members {
            let raw = vid.as_u64();
            if !in_range(raw) {
                continue;
            }
            if let Some(value) = guard.vertex_properties.get(vid).and_then(|p| p.get(prop)) {
                record(raw, may_equal(Some(value)));
            }
        }
    }
    for (vid, (base, _)) in label_only {
        if in_range(*vid) {
            record(*vid, may_equal(base.get(prop)));
        }
    }
    keep.extend(shadowed.into_iter().filter(|v| in_range(*v)));
    keep
}

/// Whether a stored schemaless L0 value could satisfy `property = literal`.
///
/// A schemaless property has no declared column type: the scan hands the
/// value to the Cypher comparison as it was written. The contract is
/// [`l0_value_may_equal`]'s, one-sided: `false` only when the comparison is
/// plainly false.
///
/// * a missing or NULL value never equals anything;
/// * two strings, two integers or two booleans compare by value;
/// * two floats compare as `f64`, keeping any `NaN`.
///
/// Every other pairing — an integer against a float (equal across types in
/// Cypher, and not transitively above 2^53), a list, a map, a temporal —
/// answers `true` and leaves the decision to the filter above the scan.
pub fn l0_value_may_equal_untyped(stored: Option<&Value>, literal: &Value) -> bool {
    let Some(stored) = stored else {
        return false;
    };
    match (stored, literal) {
        (Value::Null, _) => false,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Float(a), Value::Float(b)) if !a.is_nan() && !b.is_nan() => a == b,
        _ => true,
    }
}

/// The L0 vids a schemaless vertex scan of `label` must still build under an
/// equality prefilter.
///
/// The schemaless counterpart of [`l0_prefilter_candidates`], with the same
/// shadowing contract: a vid is kept when its merged L0 value of the property
/// may equal one of `prefilter.one_of` ([`l0_value_may_equal_untyped`]), **or**
/// when `shadowed` — the vids the flushed side of the same scan returned —
/// holds it, so a newer L0 version that no longer matches still reaches the
/// MVCC merge and hides the stale flushed row. Range bounds are not used: an
/// untyped stored value orders by Cypher's rules, which this pass does not
/// re-implement, so a prefilter without `one_of` keeps every vid.
///
/// Membership mirrors the schemaless L0 builder's unrestricted walk: with no
/// label every vid in a buffer's property map, else the vids carrying every
/// label of `label` (`"A:B"`) in that buffer; later buffers' values for the
/// key win; a relabeled vid in `label_only` starts from its flushed
/// properties.
///
/// Returns `None` when the prefilter cannot restrict (no `one_of`), meaning
/// "build every row".
pub fn schemaless_l0_prefilter_candidates(
    l0_ctx: &L0Context,
    label: &str,
    range: Option<(u64, u64)>,
    prefilter: &L0PropertyPrefilter,
    label_only: &HashMap<u64, (Properties, u64)>,
    shadowed: impl IntoIterator<Item = u64>,
) -> Option<HashSet<u64>> {
    let values = prefilter.one_of.as_ref()?;
    let in_range = |vid: u64| range.is_none_or(|(lo, hi)| vid >= lo && vid < hi);
    let may_equal = |stored: Option<&Value>| {
        values
            .iter()
            .any(|lit| l0_value_may_equal_untyped(stored, lit))
    };
    let labels: Vec<&str> = label.split(':').filter(|l| !l.is_empty()).collect();
    let prop = prefilter.property.as_str();
    // Membership as the merged verdict, as in `l0_prefilter_candidates`.
    let mut keep: HashSet<u64> = HashSet::new();
    let mut record = |raw: u64, may: bool| {
        if may {
            keep.insert(raw);
        } else {
            keep.remove(&raw);
        }
    };
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        let label_sets: Option<Vec<&HashSet<Vid>>> =
            labels.iter().map(|l| guard.label_to_vids.get(*l)).collect();
        let Some(label_sets) = label_sets else {
            // A label this buffer has never seen: no member here.
            continue;
        };
        // Walk the smallest label set rather than every vertex in the
        // buffer: with no label, every vertex is a member.
        let mut visit = |vid: &Vid, props: &Properties| {
            let raw = vid.as_u64();
            if !in_range(raw) || !label_sets.iter().all(|s| s.contains(vid)) {
                return;
            }
            if let Some(value) = props.get(prop) {
                record(raw, may_equal(Some(value)));
            }
        };
        match label_sets.iter().min_by_key(|s| s.len()) {
            Some(smallest) => {
                for vid in *smallest {
                    if let Some(props) = guard.vertex_properties.get(vid) {
                        visit(vid, props);
                    }
                }
            }
            None => {
                for (vid, props) in &guard.vertex_properties {
                    visit(vid, props);
                }
            }
        }
    }
    for (vid, (base, _)) in label_only {
        if in_range(*vid) {
            record(*vid, may_equal(base.get(prop)));
        }
    }
    keep.extend(shadowed.into_iter().filter(|v| in_range(*v)));
    Some(keep)
}

/// Build a RecordBatch from L0 buffer data for a given label, matching the
/// Lance query's column set.
///
/// Merges L0 buffers in visibility order (pending_flush → current → transaction),
/// with later buffers overwriting earlier ones for the same VID.
///
/// `targets` must mirror whatever restriction the Lance side of the same scan
/// applies — a vid pushdown or a `_vid` range — otherwise L0-only (unflushed)
/// rows bypass it and the scan emits the full label table. See issue #72 item 1
/// for the vid-list case and #214 for the range one.
pub fn build_l0_vertex_batch(
    l0_ctx: &L0Context,
    label: &str,
    lance_schema: &SchemaRef,
    label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
    targets: L0VertexTargets<'_>,
    label_only: &HashMap<u64, (Properties, u64)>,
) -> anyhow::Result<RecordBatch> {
    // Collect all L0 vertex data, merging in visibility order
    let mut vid_data: HashMap<u64, (Properties, u64)> = HashMap::new(); // vid -> (props, version)
    let mut tombstones: HashSet<u64> = HashSet::new();
    // System-managed timestamps: created_at takes the earliest seen
    // timestamp across L0 buffers (preserving the original creation
    // moment when a row has been touched in multiple buffers); updated_at
    // takes the latest (most recent write). Used by `created_at(n)` /
    // `updated_at(n)` Cypher functions.
    let mut vid_created_at: HashMap<u64, i64> = HashMap::new();
    let mut vid_updated_at: HashMap<u64, i64> = HashMap::new();

    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        // Collect tombstones
        for vid in guard.vertex_tombstones.iter() {
            tombstones.insert(vid.as_u64());
        }
        // Collect vertices — restrict to target_vids (single- or multi-VID
        // pushdown from id(x) = ? / id(x) IN [...]) when set, else all
        // vertices for the label. See issue #72 item 1: without this filter,
        // freshly-inserted L0 rows bypass the IN-list pushdown that Lance
        // already honors, defeating the optimization.
        let candidate_vids: Vec<Vid> = match targets {
            L0VertexTargets::Vids(tvs) => {
                let mut out = Vec::with_capacity(tvs.len());
                for &tv in tvs {
                    let vid = Vid::from(tv);
                    if (guard.vertex_properties.contains_key(&vid) || label_only.contains_key(&tv))
                        && (label.is_empty()
                            || guard
                                .label_to_vids
                                .get(label)
                                .is_some_and(|s| s.contains(&vid)))
                    {
                        out.push(vid);
                    }
                }
                out
            }
            // No list to probe against, so this filters the label's set rather
            // than doing point lookups. That is the same work the `All` arm
            // already does, plus a comparison per vid.
            L0VertexTargets::Range(lo, hi) => {
                let mut out = guard.vids_for_label(label);
                out.retain(|vid| {
                    let raw = vid.as_u64();
                    raw >= lo && raw < hi
                });
                out
            }
            L0VertexTargets::All => guard.vids_for_label(label),
            // Probe the (small) kept set against the label set rather than
            // walking the label: the membership rule is `All`'s / `Range`'s,
            // so each kept row is built exactly as those arms would build it.
            L0VertexTargets::Filtered { range, vids } => match guard.label_to_vids.get(label) {
                Some(members) => vids
                    .iter()
                    .filter(|raw| range.is_none_or(|(lo, hi)| **raw >= lo && **raw < hi))
                    .map(|raw| Vid::from(*raw))
                    .filter(|vid| members.contains(vid))
                    .collect(),
                None => Vec::new(),
            },
        };
        for vid in candidate_vids {
            let vid_u64 = vid.as_u64();
            if tombstones.contains(&vid_u64) {
                continue;
            }
            let version = guard.vertex_versions.get(&vid).copied().unwrap_or(0);
            let entry = vid_data
                .entry(vid_u64)
                .or_insert_with(|| (Properties::new(), 0));
            // Merge properties (later L0 overwrites)
            if let Some(props) = guard.vertex_properties.get(&vid) {
                for (k, v) in props {
                    entry.0.insert(k.clone(), v.clone());
                }
            }
            // Take the highest version
            if version > entry.1 {
                entry.1 = version;
            }
            // Merge system timestamps: earliest creation, latest update
            if let Some(&ts) = guard.vertex_created_at.get(&vid) {
                vid_created_at
                    .entry(vid_u64)
                    .and_modify(|cur| {
                        if ts < *cur {
                            *cur = ts;
                        }
                    })
                    .or_insert(ts);
            }
            if let Some(&ts) = guard.vertex_updated_at.get(&vid) {
                vid_updated_at
                    .entry(vid_u64)
                    .and_modify(|cur| {
                        if ts > *cur {
                            *cur = ts;
                        }
                    })
                    .or_insert(ts);
            }
        }
    }

    // A relabeled vertex with no L0 property row starts from its flushed
    // properties, at the relabel's version (see `label_only_l0_base_props`).
    for (vid, (base, version)) in label_only {
        if let Some(entry) = vid_data.get_mut(vid) {
            entry.0 = base.clone();
            entry.1 = entry.1.max(*version);
        }
    }

    // Remove tombstoned VIDs
    for t in &tombstones {
        vid_data.remove(t);
    }

    if vid_data.is_empty() {
        return Ok(RecordBatch::new_empty(lance_schema.clone()));
    }

    // Sort VIDs for deterministic output
    let mut vids: Vec<u64> = vid_data.keys().copied().collect();
    vids.sort_unstable();

    let num_rows = vids.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(lance_schema.fields().len());

    // Determine which schema property names exist
    let schema_prop_names: HashSet<&str> = label_props
        .map(|lp| lp.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    for field in lance_schema.fields() {
        let col_name = field.name().as_str();
        match col_name {
            "_vid" => {
                columns.push(Arc::new(UInt64Array::from(vids.clone())));
            }
            "_deleted" => {
                // L0 vertices are always live (tombstoned ones are already excluded)
                let vals = vec![false; num_rows];
                columns.push(Arc::new(arrow_array::BooleanArray::from(vals)));
            }
            "_version" => {
                let vals: Vec<u64> = vids.iter().map(|v| vid_data[v].1).collect();
                columns.push(Arc::new(UInt64Array::from(vals)));
            }
            "_created_at" => {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::new().with_timezone("UTC");
                for v in &vids {
                    match vid_created_at.get(v) {
                        Some(&ts) => builder.append_value(ts),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "_updated_at" => {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::new().with_timezone("UTC");
                for v in &vids {
                    match vid_updated_at.get(v) {
                        Some(&ts) => builder.append_value(ts),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "overflow_json" => {
                // Collect non-schema properties as CypherValue
                let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                for vid_u64 in &vids {
                    let (props, _) = &vid_data[vid_u64];
                    let mut overflow: HashMap<String, Value> = HashMap::new();
                    for (k, v) in props {
                        if stored_in_ext_id_column(k, v) || is_reserved_property_name(k) {
                            continue;
                        }
                        if !schema_prop_names.contains(k.as_str()) {
                            overflow.insert(k.clone(), v.clone());
                        }
                    }
                    if overflow.is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(uni_common::cypher_value_codec::encode(&Value::Map(
                            overflow,
                        )));
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "_labels" => {
                // Rows in this batch exist only in L0, so there is no stored
                // label set to carry. Emitting nulls is not a shortcut: a null
                // row makes `build_labels_column_for_known_label` fall back to
                // `[label]`, and its L0 overlay then resolves the true set from
                // `vertex_labels` — the same path these rows took before
                // `_labels` joined the projection, and the one that is already
                // correct for unflushed vertices.
                //
                // Without this arm the column falls through to
                // `build_l0_property_column`, which does not handle
                // `List<Utf8>`, and `RecordBatch::try_new` below fails.
                let mut builder = ListBuilder::new(StringBuilder::new())
                    .with_field(Arc::new(Field::new("item", DataType::Utf8, true)));
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
            EXT_ID_COLUMN => {
                // The flush-shaped split: a string `ext_id` here, anything
                // else in `overflow_json` above (`stored_in_ext_id_column`).
                let mut builder = StringBuilder::new();
                for vid_u64 in &vids {
                    match vid_data[vid_u64].0.get(EXT_ID_COLUMN) {
                        Some(Value::String(s)) => builder.append_value(s),
                        _ => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Schema property column: convert L0 Value → Arrow typed value
                let col = build_l0_property_column(&vids, &vid_data, col_name, field.data_type())?;
                columns.push(col);
            }
        }
    }

    RecordBatch::try_new(lance_schema.clone(), columns).map_err(anyhow::Error::from)
}

/// Build the `_labels` column for known-label vertices.
///
/// Reads `_labels` from the stored Lance batch if available. Falls back to
/// `[label]` when the column is absent (legacy data). Additional labels from
/// L0 buffers are merged in.
pub fn build_labels_column_for_known_label(
    vid_arr: &UInt64Array,
    label: &str,
    l0_ctx: &L0Context,
    batch_labels_col: Option<&arrow_array::ListArray>,
) -> anyhow::Result<ArrayRef> {
    use crate::storage::arrow_convert::labels_from_list_array;

    let mut labels_builder = ListBuilder::new(StringBuilder::new());

    for i in 0..vid_arr.len() {
        let vid = Vid::from(vid_arr.value(i));

        // Start with labels from the stored column, falling back to [label]
        let mut labels = match batch_labels_col {
            Some(list_arr) => {
                let stored = labels_from_list_array(list_arr, i);
                if stored.is_empty() {
                    vec![label.to_string()]
                } else {
                    stored
                }
            }
            None => vec![label.to_string()],
        };

        // Ensure the scanned label is present (defensive)
        if !labels.iter().any(|l| l == label) {
            labels.push(label.to_string());
        }

        // Merge additional labels from L0 buffers, honoring label-overwrite
        // markers: a vid flagged in `vertex_label_overwrites` has its full label
        // set resolved by a SET/REMOVE, which REPLACES the stored labels (newest
        // buffer wins) — so a REMOVE of the scanned label is respected rather
        // than resurrected by the union or the defensive push above.
        let mut overwrite_labels: Option<Vec<String>> = None;
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if guard.vertex_label_overwrites.contains(&vid) {
                overwrite_labels = guard.vertex_labels.get(&vid).cloned();
            } else if let Some(l0_labels) = guard.vertex_labels.get(&vid) {
                for lbl in l0_labels {
                    if !labels.contains(lbl) {
                        labels.push(lbl.clone());
                    }
                }
            }
        }
        if let Some(resolved) = overwrite_labels {
            labels = resolved;
        }

        let values = labels_builder.values();
        for lbl in &labels {
            values.append_value(lbl);
        }
        labels_builder.append(true);
    }

    Ok(Arc::new(labels_builder.finish()))
}

/// Map a Lance-schema batch to the DataFusion output schema.
///
/// The output schema has `{variable}.{property}` column names, while Lance
/// uses bare property names. This function performs the positional mapping,
/// adds the `_labels` column, and drops internal columns like `_deleted`/`_version`.
pub fn map_to_output_schema(
    batch: &RecordBatch,
    label: &str,
    _variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
    l0_ctx: &L0Context,
) -> anyhow::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // 1. {var}._vid
    let vid_col = batch
        .column_by_name("_vid")
        .ok_or_else(|| anyhow::anyhow!("Missing _vid column".to_string()))?
        .clone();
    let vid_arr = vid_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow::anyhow!("_vid not UInt64".to_string()))?;

    // 2. {var}._labels — read from stored column, overlay L0 additions
    let batch_labels_col = batch
        .column_by_name("_labels")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>());
    let labels_col = build_labels_column_for_known_label(vid_arr, label, l0_ctx, batch_labels_col)?;
    columns.push(vid_col.clone());
    columns.push(labels_col);

    // 3. Projected properties
    // Pre-load overflow_json column for extracting non-schema properties
    let overflow_arr = batch
        .column_by_name("overflow_json")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());

    for prop in projected_properties {
        if prop == "overflow_json" {
            match batch.column_by_name("overflow_json") {
                Some(col) => columns.push(col.clone()),
                None => {
                    // No overflow_json in Lance — return null column
                    columns.push(arrow_array::new_null_array(
                        &DataType::LargeBinary,
                        batch.num_rows(),
                    ));
                }
            }
        } else if prop == "_all_props" {
            // Build _all_props from overflow_json + L0 overlay.
            // Fast path: if no L0 buffer has vertex property mutations AND
            // there are no schema columns to merge, pass through overflow_json.
            let any_l0_has_vertex_props = l0_ctx.iter_l0_buffers().any(|l0| {
                let guard = l0.read();
                !guard.vertex_properties.is_empty()
            });
            // Check if this label has schema-defined columns (besides system columns)
            let has_schema_cols = projected_properties
                .iter()
                .any(|p| !is_reserved_property_name(p));

            let has_ext_id = batch
                .column_by_name(EXT_ID_COLUMN)
                .is_some_and(|c| c.null_count() < c.len());

            if !any_l0_has_vertex_props && !has_schema_cols && !has_ext_id {
                // No L0 mutations, no schema cols to merge: overflow_json IS _all_props
                match batch.column_by_name("overflow_json") {
                    Some(col) => columns.push(col.clone()),
                    None => {
                        columns.push(arrow_array::new_null_array(
                            &DataType::LargeBinary,
                            batch.num_rows(),
                        ));
                    }
                }
            } else {
                // Need to merge: schema columns + overflow_json + L0 overlay
                let col = build_all_props_column_for_schema_scan(
                    batch,
                    vid_arr,
                    overflow_arr,
                    projected_properties,
                    l0_ctx,
                )?;
                columns.push(col);
            }
        } else if prop == EXT_ID_COLUMN {
            let stored = batch
                .column_by_name(EXT_ID_COLUMN)
                .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>());
            columns.push(build_ext_id_property_column(
                batch.num_rows(),
                vid_arr,
                stored,
                overflow_arr,
                l0_ctx,
            )?);
        } else {
            match batch.column_by_name(prop) {
                Some(col) => columns.push(col.clone()),
                None => {
                    // Column missing in Lance -- extract from overflow_json
                    // CypherValue blob with L0 overlay
                    let col = build_overflow_property_column(
                        batch.num_rows(),
                        vid_arr,
                        overflow_arr,
                        prop,
                        l0_ctx,
                    )?;
                    columns.push(col);
                }
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns).map_err(anyhow::Error::from)
}

/// Arrow type of the `_labels` column.
///
/// `uni-query`'s `common.rs` keeps its own copy for six other callers; one
/// line, and duplicating it beats a cross-crate dependency in the wrong
/// direction.
fn labels_data_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

/// What to read in one columnar vertex scan.
pub struct ColumnarVertexScanRequest<'a> {
    /// Label whose table is scanned.
    pub label: &'a str,
    /// Properties to materialise, in output order.
    pub projected_properties: &'a [String],
    /// Positional target for the result; the caller owns column naming.
    pub output_schema: &'a SchemaRef,
    /// Single-vid short circuit, already resolved from the caller's filter.
    pub target_vid: Option<u64>,
    /// Multi-vid restriction (`_vid IN (…)`).
    pub vid_list_filter: Option<&'a [u64]>,
    /// Half-open `_vid` range `[lo, hi)` this call is restricted to.
    ///
    /// Set when a caller is walking a label in bounded vid ranges rather than
    /// reading it whole (#214). `_vid` is the MVCC dedup key, so a range
    /// partitions the row space along the same axis the dedup groups on: every
    /// version of a vid falls in exactly one range, and the highest-`_version`
    /// choice still sees all its candidates. No other partitioning of this scan
    /// has that property.
    pub vid_range: Option<(u64, u64)>,
    /// Rendered SQL from the planner's hash-index pushdown; passed through
    /// verbatim, nothing here parses it.
    pub extra_lance_filter: Option<&'a str>,
    /// Structured form of the equality / range the caller filters this scan's
    /// rows by, used to skip L0 rows that cannot match before they are
    /// materialized.
    ///
    /// Only a cost lever: the caller still applies its own filter to every
    /// returned row. Without it a point lookup on unflushed rows built the
    /// label's whole L0 set and filtered afterwards — `rows_scanned` equal to
    /// the L0 population per lookup (W3b). See [`L0PropertyPrefilter`].
    pub l0_prefilter: Option<&'a L0PropertyPrefilter>,
}

/// Read a batch of vertices columnarly: Lance + L0, MVCC-resolved.
///
/// The storage half of what used to be `columnar_scan_vertex_batch_static` in
/// `uni-query`. Everything DataFusion-shaped -- resolving a `PhysicalExpr` down
/// to a target vid, the runtime filter, the `Count` metric -- stays with the
/// caller, so this function has no DataFusion types and `uni-store` needs no
/// dependency on it (#209). That is what lets crates below the query layer read
/// properties columnarly instead of through a per-vid map.
///
/// Rows come back positionally against `req.output_schema`; the caller owns the
/// `{var}.{prop}` naming.
///
/// # Errors
///
/// Propagates storage, plugin and Arrow failures.
pub async fn columnar_scan_vertex_batch(
    storage: &Arc<StorageManager>,
    l0_ctx: &L0Context,
    plugin_registry: Option<&Arc<uni_plugin::PluginRegistry>>,
    counters: Option<&Arc<crate::QueryCounters>>,
    req: ColumnarVertexScanRequest<'_>,
    index_consulted: Option<&mut usize>,
) -> anyhow::Result<RecordBatch> {
    let ColumnarVertexScanRequest {
        label,
        projected_properties,
        output_schema,
        target_vid,
        vid_list_filter,
        vid_range,
        extra_lance_filter,
        l0_prefilter,
    } = req;
    let uni_schema = storage.schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    // `build_all_props_column_for_schema_scan` assembles the whole-entity map
    // from `projected_properties` alone, so a narrowed projection yields a map
    // that is quietly missing declared properties -- indistinguishable from
    // those properties being unset. Today the planner emits `_all_props` only
    // from its `"*"` branches, which widen the projection to the full declared
    // set in the same step, and both traversal hydration paths route
    // `_all_props` to the row-wise `PropertyManager` instead of here. That is
    // four sites holding one invariant by convention, with nothing checking it.
    debug_assert!(
        !projected_properties.iter().any(|p| p == "_all_props")
            || label_props.is_none_or(|declared| {
                declared
                    .keys()
                    .all(|d| projected_properties.iter().any(|p| p == d))
            }),
        "columnar `_all_props` for label `{label}` was requested with a narrowed \
         projection, so the whole-entity map would silently omit declared \
         properties. Either widen the projection to the full declared set or \
         route this caller through `PropertyManager`. Projected: \
         {projected_properties:?}"
    );

    // Build the list of columns to request from Lance
    let mut lance_columns: Vec<String> = vec![
        "_vid".to_string(),
        "_deleted".to_string(),
        "_version".to_string(),
    ];
    // `_labels` is REQUIRED, not a projection nicety. Without it
    // `build_labels_column_for_known_label` fabricates `[label]`, and that
    // fabricated set is what the executor writes back — truncating a
    // multi-label vertex on DELETE, SET/REMOVE label, and even a plain
    // `SET n.prop`, as well as returning a wrong `labels(n)`.
    //
    // Requesting it is safe on legacy tables that predate the column:
    // `StorageManager::scan_vertex_table_counted` narrows the projection to
    // physically-present columns, and the builder's `[label]` fallback covers
    // the absence.
    push_column_if_absent(&mut lance_columns, "_labels");
    for prop in projected_properties {
        if prop == "overflow_json" {
            push_column_if_absent(&mut lance_columns, "overflow_json");
        } else if prop == "_created_at" || prop == "_updated_at" {
            // System-managed timestamps live on every vertex table regardless
            // of label schema. Request them directly from Lance.
            push_column_if_absent(&mut lance_columns, prop);
        } else {
            let exists_in_schema = label_props.is_some_and(|lp| lp.contains_key(prop));
            if exists_in_schema {
                push_column_if_absent(&mut lance_columns, prop);
            }
        }
    }

    // `ext_id` is undeclared but has its own column: a flush moves a string
    // `ext_id` there and out of `overflow_json`, so the blob alone cannot
    // answer `n.ext_id` or a whole-entity map.
    if projected_properties
        .iter()
        .any(|p| p == EXT_ID_COLUMN || p == "_all_props")
    {
        push_column_if_absent(&mut lance_columns, EXT_ID_COLUMN);
    }

    // Ensure overflow_json is present when any projected property is not in the schema
    // (excluding system-managed columns like `_created_at` / `_updated_at`).
    let needs_overflow = projected_properties.iter().any(|p| {
        p == "overflow_json"
            || (!matches!(p.as_str(), "_created_at" | "_updated_at")
                && !label_props.is_some_and(|lp| lp.contains_key(p)))
    });
    if needs_overflow {
        push_column_if_absent(&mut lance_columns, "overflow_json");
    }

    // Push _vid filter to Lance for O(log N) BTree index lookup instead of full scan.
    // Prefer the multi-VID list (formats as `_vid IN (...)`); fall back to
    // single-VID `_vid = N` from the WHERE-clause path. AND-combined with
    // any indexed-property pushdown (issue #57).
    let vid_part = match (vid_list_filter, target_vid) {
        (Some(vs), _) if !vs.is_empty() => Some(FilterExpr::one_of(
            "_vid",
            vs.iter().map(|v| Scalar::UInt(*v)),
        )),
        (_, Some(v)) => Some(FilterExpr::equals("_vid", Scalar::UInt(v))),
        // A range is a `_vid >= lo AND _vid < hi` pair rather than an IN list:
        // the whole point of walking a label in ranges is that the predicate
        // stays two comparisons however wide the range is, where an IN list
        // would grow with it.
        _ => vid_range.map(|(lo, hi)| {
            FilterExpr::all([
                FilterExpr::compare("_vid", CmpOp::GtEq, Scalar::UInt(lo)),
                FilterExpr::compare("_vid", CmpOp::Lt, Scalar::UInt(hi)),
            ])
        }),
    };
    // `extra_lance_filter` arrives as rendered SQL from the planner's
    // hash-index pushdown, so it stays `Raw` — nothing in the engine parses it.
    let combined_filter = match (vid_part, extra_lance_filter) {
        (Some(v), Some(e)) => Some(FilterExpr::all([v, FilterExpr::Raw(e.to_string())])),
        (Some(v), None) => Some(v),
        (None, Some(e)) => Some(FilterExpr::Raw(e.to_string())),
        (None, None) => None,
    };
    let lance_columns_refs: Vec<&str> = lance_columns.iter().map(|s| s.as_str()).collect();

    // M5h.2: route through plugin Storage if one is registered for
    // this label. v1 ships reads only — writes still go to native
    // backend. v1 ignores `combined_filter` when delegating (the
    // planner re-filters via the surrounding Filter node); per-plugin
    // filter pushdown is a v1.1 follow-up (`TODO(M5h.2-filter)`).
    let plugin_batch: Option<arrow::record_batch::RecordBatch> = match plugin_registry {
        Some(reg) => match reg.lookup_label_storage(label) {
            Some(plugin_storage) => {
                let mut stream = plugin_storage.read_batch(label, None).await.map_err(|e| {
                    anyhow::anyhow!(format!(
                        "plugin Storage::read_batch({label}) failed: {} (code 0x{:x})",
                        e.message, e.code
                    ))
                })?;
                use futures::StreamExt;
                let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
                let mut schema_ref: Option<SchemaRef> = None;
                while let Some(b) = stream.next().await {
                    let b = b.map_err(|e| {
                        anyhow::anyhow!(format!("plugin Storage stream({label}) errored: {e}"))
                    })?;
                    if schema_ref.is_none() {
                        schema_ref = Some(b.schema());
                    }
                    batches.push(b);
                }
                if let Some(s) = schema_ref {
                    Some(arrow::compute::concat_batches(&s, &batches).map_err(|e| {
                        anyhow::anyhow!(format!("plugin Storage concat({label}) failed: {e}"))
                    })?)
                } else {
                    None
                }
            }
            None => None,
        },
        None => None,
    };

    // Track whether the batch came through the property-filtered native scan:
    // plugin batches ignore `combined_filter` (re-filtered by the planner), so
    // they need no stale-version verification.
    let (lance_batch, pushdown_filtered) = match plugin_batch {
        Some(b) => (Some(b), false),
        None => (
            {
                // A scan-local counter set, merged into the query's afterwards.
                // Taking a delta on the shared set instead would misattribute
                // whenever two scans of the same query overlap; `merge_from`
                // exists for exactly this fan-out.
                let scan_local = Arc::new(crate::QueryCounters::new());
                let batch = storage
                    .scan_vertex_table_counted(
                        label,
                        &lance_columns_refs,
                        combined_filter.as_ref(),
                        Some(&scan_local),
                    )
                    .await?;
                if let Some(q) = counters {
                    q.merge_from(&scan_local);
                }
                if let Some(m) = index_consulted {
                    *m += scan_local.index_scans() as usize;
                }
                batch
            },
            extra_lance_filter.is_some(),
        ),
    };

    // A pushed property predicate hides a vid's CURRENT row from the scan when
    // that row no longer matches (MVCC-append: the stale still-matching row
    // would win the dedup by default) — drop superseded rows first.
    let lance_batch = match (lance_batch, pushdown_filtered) {
        (Some(b), true) => Some(drop_superseded_pushdown_rows(storage, Some(label), b).await?),
        (b, _) => b,
    };

    // MVCC dedup the Lance batch
    let lance_deduped = mvcc_dedup_to_option(lance_batch, "_vid")?;

    // Build the internal Lance schema for L0 batch construction.
    // Use the Lance batch schema if available, otherwise build from scratch.
    let internal_schema = match &lance_deduped {
        Some(batch) => batch.schema(),
        None => {
            let mut fields = vec![
                Field::new("_vid", DataType::UInt64, false),
                Field::new("_deleted", DataType::Boolean, false),
                Field::new("_version", DataType::UInt64, false),
            ];
            for col in &lance_columns {
                if matches!(col.as_str(), "_vid" | "_deleted" | "_version") {
                    continue;
                }
                if col == "overflow_json" {
                    fields.push(Field::new("overflow_json", DataType::LargeBinary, true));
                } else if col == EXT_ID_COLUMN {
                    fields.push(Field::new(EXT_ID_COLUMN, DataType::Utf8, true));
                } else if col == "_labels" {
                    // Typed explicitly: falling through to the `label_props`
                    // lookup below would default it to LargeBinary, since
                    // `_labels` is never a declared user property.
                    fields.push(Field::new("_labels", labels_data_type(), true));
                } else if col == "_created_at" || col == "_updated_at" {
                    fields.push(Field::new(
                        col,
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        true,
                    ));
                } else {
                    let arrow_type = label_props
                        .and_then(|lp| lp.get(col.as_str()))
                        .map(|meta| meta.r#type.to_arrow())
                        .unwrap_or(DataType::LargeBinary);
                    fields.push(Field::new(col, arrow_type, true));
                }
            }
            Arc::new(Schema::new(fields))
        }
    };

    // Build L0 batch. Prefer the multi-VID list when present (IN-list pushdown
    // from issue #55 PR #4 — must restrict L0 to the same VID set Lance was
    // filtered against, see issue #72 item 1). Fall back to single-VID
    // (`id(x) = $literal` short-circuit). One-element buffer keeps the
    // borrowed slice alive for the single-VID case.
    let single_vid_buf: [u64; 1];
    let l0_targets: L0VertexTargets<'_> = match (vid_list_filter, target_vid) {
        (Some(vs), _) if !vs.is_empty() => L0VertexTargets::Vids(vs),
        (_, Some(v)) => {
            single_vid_buf = [v];
            L0VertexTargets::Vids(&single_vid_buf)
        }
        // Same restriction Lance was given. Without it every range would
        // re-emit the label's whole L0 set and the chunks would duplicate
        // rather than partition.
        _ => match vid_range {
            Some((lo, hi)) => L0VertexTargets::Range(lo, hi),
            None => L0VertexTargets::All,
        },
    };
    let label_only = label_only_l0_base_props(
        storage,
        l0_ctx,
        label,
        |vid| l0_targets.contains(vid),
        counters,
    )
    .await?;
    // Narrow a whole-label (or range) L0 read to the rows the pushed equality
    // or range can match, plus every vid the storage side returned — those L0 rows
    // must reach the merge to shadow their stale flushed versions. A vid list
    // is already narrow; an undeclared property has no column type to reason
    // about, so both keep the unrestricted read. `ext_id` is the exception:
    // undeclared, but every per-label table carries it as a string column,
    // and `l0_value_may_equal` answers `true` for any non-string L0 value, so
    // reasoning about it as a `STRING` column keeps every row that could match.
    let ext_id_type = uni_common::core::schema::DataType::String;
    let l0_kept: HashSet<u64>;
    let l0_targets = match (l0_prefilter, l0_targets) {
        (Some(pf), L0VertexTargets::All | L0VertexTargets::Range(..)) => {
            let column_type = label_props
                .and_then(|lp| lp.get(pf.property.as_str()))
                .map(|meta| &meta.r#type)
                .or_else(|| (pf.property == EXT_ID_COLUMN).then_some(&ext_id_type));
            match column_type {
                Some(column_type) => {
                    let range = match l0_targets {
                        L0VertexTargets::Range(lo, hi) => Some((lo, hi)),
                        _ => None,
                    };
                    let shadowed = lance_deduped
                        .as_ref()
                        .and_then(|b| b.column_by_name("_vid"))
                        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                        .map(|a| a.values().to_vec())
                        .unwrap_or_default();
                    l0_kept = l0_prefilter_candidates(
                        l0_ctx,
                        label,
                        range,
                        column_type,
                        pf,
                        &label_only,
                        shadowed,
                    );
                    L0VertexTargets::Filtered {
                        range,
                        vids: &l0_kept,
                    }
                }
                None => l0_targets,
            }
        }
        (_, t) => t,
    };
    let l0_batch = build_l0_vertex_batch(
        l0_ctx,
        label,
        &internal_schema,
        label_props,
        l0_targets,
        &label_only,
    )?;

    // Merge Lance + L0
    let Some(merged) =
        merge_lance_and_l0(lance_deduped, l0_batch, &internal_schema, "_vid", counters)?
    else {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    };

    // Filter out MVCC deletion tombstones (_deleted = true)
    let merged = filter_deleted_rows(&merged)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 tombstones
    let filtered = filter_l0_tombstones(&merged, l0_ctx)?;

    // Symmetric with the schemaless path: drop a flushed row whose scanned label
    // was REMOVE'd in L0 (a no-op unless a vid carries a label-overwrite marker
    // that no longer includes `label`).
    let filtered = filter_l0_label_overwrites(&filtered, label, l0_ctx)?;

    if filtered.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Map to output schema
    let mapped = map_to_output_schema(
        &filtered,
        label,
        // `map_to_output_schema` ignores this; the caller owns column naming.
        "",
        projected_properties,
        output_schema,
        l0_ctx,
    )?;

    Ok(mapped)
}

#[cfg(test)]
mod tests {
    //! Relocation validators. These moved with the pipeline they cover, so a
    //! failure here points at the move rather than at a caller.

    use super::*;
    use arrow_array::UInt64Array;
    use arrow_schema::Schema;
    use std::sync::Arc;

    /// A declared column beats `overflow_json` residue for the same key.
    ///
    /// The blob only ever holds schemaless properties -- every writer excludes
    /// declared keys via `build_overflow_json_column` -- so a collision means
    /// pre-declaration residue on a row whose typed column is the later write.
    /// This pinned nothing before: the merge was `extend`, so the blob won,
    /// disagreeing with the row-wise reader on the same data.
    ///
    /// No writer can currently produce a colliding row, so the batch is built
    /// by hand. That is the point of the test -- it fixes the contract while
    /// the state is still unreachable, rather than after something reaches it.
    #[test]
    fn all_props_prefers_the_declared_column_over_overflow_residue() {
        use uni_common::Value;

        let schema = Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("score", DataType::Int64, true),
            Field::new("overflow_json", DataType::LargeBinary, true),
        ]));

        // The same key in both places, plus a schemaless-only key as a control
        // that the blob is genuinely being read.
        let residue = Value::Map(HashMap::from([
            ("score".to_string(), Value::Int(1)),
            ("nickname".to_string(), Value::String("only-in-blob".into())),
        ]));
        let blob = uni_common::cypher_value_codec::encode(&residue);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![7u64])),
                Arc::new(arrow_array::Int64Array::from(vec![Some(2i64)])),
                Arc::new(arrow_array::LargeBinaryArray::from(vec![Some(
                    blob.as_slice(),
                )])),
            ],
        )
        .unwrap();

        let vid_arr = UInt64Array::from(vec![7u64]);
        let overflow_arr = batch
            .column_by_name("overflow_json")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap()
            .clone();

        let out = build_all_props_column_for_schema_scan(
            &batch,
            &vid_arr,
            Some(&overflow_arr),
            &["score".to_string()],
            &L0Context::empty(),
        )
        .expect("schema-scan props column must build");

        let out = out
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        let Ok(Value::Map(merged)) = uni_common::cypher_value_codec::decode(out.value(0)) else {
            panic!("_all_props did not decode to a map");
        };

        assert_eq!(
            merged.get("score"),
            Some(&Value::Int(2)),
            "the declared column is the later write and must win: {merged:?}"
        );
        assert_eq!(
            merged.get("nickname"),
            Some(&Value::String("only-in-blob".into())),
            "schemaless keys must still come through from the blob"
        );
    }

    /// Helper to build a RecordBatch with _vid, _deleted, _version columns for testing.
    fn make_mvcc_batch(vids: &[u64], versions: &[u64], deleted: &[bool]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        // Generate name values like "v{vid}_ver{version}" for tracking which row wins
        let names: Vec<String> = vids
            .iter()
            .zip(versions.iter())
            .map(|(v, ver)| format!("v{}_ver{}", v, ver))
            .collect();
        let name_arr: arrow_array::StringArray = names.iter().map(|s| Some(s.as_str())).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vids.to_vec())),
                Arc::new(arrow_array::BooleanArray::from(deleted.to_vec())),
                Arc::new(UInt64Array::from(versions.to_vec())),
                Arc::new(name_arr),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_mvcc_dedup_multiple_versions() {
        // VID 1 at versions 3, 1, 5 — should keep version 5
        // VID 2 at versions 2, 4 — should keep version 4
        let batch = make_mvcc_batch(
            &[1, 1, 1, 2, 2],
            &[3, 1, 5, 2, 4],
            &[false, false, false, false, false],
        );

        let result = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vid_col = result
            .column_by_name("_vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ver_col = result
            .column_by_name("_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let name_col = result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();

        // VID 1 → version 5, VID 2 → version 4
        assert_eq!(vid_col.value(0), 1);
        assert_eq!(ver_col.value(0), 5);
        assert_eq!(name_col.value(0), "v1_ver5");

        assert_eq!(vid_col.value(1), 2);
        assert_eq!(ver_col.value(1), 4);
        assert_eq!(name_col.value(1), "v2_ver4");
    }

    #[test]
    fn test_mvcc_dedup_single_rows() {
        // Each VID appears once — nothing should change
        let batch = make_mvcc_batch(&[1, 2, 3], &[1, 1, 1], &[false, false, false]);
        let result = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    /// A version tie must resolve the same way every time.
    ///
    /// `lexsort_to_indices` is unstable, so before the position tiebreak the
    /// winner of a tie was whichever row the sort left first — arbitrary, and
    /// not reproducible. Enough tied rows are used that an unstable sort
    /// actually reorders them; two would often survive by luck.
    ///
    /// The rule asserted is the one the row-wise path already applies: the
    /// later row wins, so an L0 row concatenated after a Lance row takes the
    /// tie.
    #[test]
    fn test_mvcc_dedup_tie_resolves_to_the_later_row() {
        const TIED: usize = 64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        // One vid, every row at the same version, `name` recording the
        // original position so the winner is identifiable.
        let names: Vec<String> = (0..TIED).map(|i| format!("pos{i}")).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64; TIED])),
                Arc::new(arrow_array::BooleanArray::from(vec![false; TIED])),
                Arc::new(UInt64Array::from(vec![7u64; TIED])),
                Arc::new(
                    names
                        .iter()
                        .map(|s| Some(s.as_str()))
                        .collect::<arrow_array::StringArray>(),
                ),
            ],
        )
        .unwrap();

        let out = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(out.num_rows(), 1, "one row survives per vid");
        let got = out
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("name column is Utf8")
            .value(0)
            .to_string();
        assert_eq!(
            got,
            format!("pos{}", TIED - 1),
            "the last tied row must win, deterministically"
        );
    }

    /// A tombstone must win a version tie: a delete is monotone, so a live row
    /// at the *same* version must not resurrect a deleted id.
    ///
    /// This is the C2 pattern -- the main tables were fixed for the case where
    /// an older live row beat a tombstone; the equal-version case is the same
    /// bug one step in. Note the existing dedup tests all pass `deleted`
    /// all-false, so this is also the first coverage of a tombstone surviving
    /// the dedup at all.
    #[test]
    fn test_mvcc_dedup_tombstone_wins_a_version_tie() {
        // Live row first, tombstone second, then the reverse: neither
        // ordering may resurrect the vid.
        for (deleted, label) in [
            (vec![false, true], "live first"),
            (vec![true, false], "tombstone first"),
        ] {
            let batch = make_mvcc_batch(&[1, 1], &[7, 7], &deleted);
            let out = mvcc_dedup_batch(&batch).unwrap();
            assert_eq!(out.num_rows(), 1, "{label}: one row survives");
            let del = out
                .column_by_name("_deleted")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .expect("_deleted is Boolean");
            assert!(del.value(0), "{label}: the tombstone must win the tie");
        }
    }

    /// A genuinely newer live row still un-deletes, so the tombstone rank must
    /// sit below `_version` rather than above it.
    #[test]
    fn test_mvcc_dedup_newer_live_row_beats_an_older_tombstone() {
        let batch = make_mvcc_batch(&[1, 1], &[8, 7], &[false, true]);
        let out = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(out.num_rows(), 1);
        let del = out
            .column_by_name("_deleted")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .expect("_deleted is Boolean");
        assert!(!del.value(0), "v8 live must beat the v7 tombstone");
    }

    #[test]
    fn test_mvcc_dedup_empty() {
        let batch = make_mvcc_batch(&[], &[], &[]);
        let result = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_filter_l0_tombstones_removes_tombstoned() {
        use L0Context;

        // Create a batch with VIDs 1, 2, 3
        let batch = make_mvcc_batch(&[1, 2, 3], &[1, 1, 1], &[false, false, false]);

        // Create L0 context with VID 2 tombstoned
        let l0 = crate::runtime::l0::L0Buffer::new(1, None);
        {
            // We need to insert a tombstone — L0Buffer has pub vertex_tombstones
            // But we can't easily create one with tombstones through the constructor.
            // Use a direct approach.
        }
        let l0_buf = std::sync::Arc::new(parking_lot::RwLock::new(l0));
        l0_buf.write().vertex_tombstones.insert(Vid::from(2u64));

        let l0_ctx = L0Context {
            current_l0: Some(l0_buf),
            transaction_l0: None,
            pending_flush_l0s: vec![],
        };

        let result = filter_l0_tombstones(&batch, &l0_ctx).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vid_col = result
            .column_by_name("_vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(vid_col.value(0), 1);
        assert_eq!(vid_col.value(1), 3);
    }

    #[test]
    fn test_filter_l0_tombstones_none() {
        use L0Context;

        let batch = make_mvcc_batch(&[1, 2, 3], &[1, 1, 1], &[false, false, false]);
        let l0_ctx = L0Context::default();

        let result = filter_l0_tombstones(&batch, &l0_ctx).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_map_to_output_schema_basic() {
        use L0Context;

        // Input: Lance-schema batch with _vid, _deleted, _version, name columns
        let lance_schema = Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let name_arr: arrow_array::StringArray =
            vec![Some("Alice"), Some("Bob")].into_iter().collect();
        let batch = RecordBatch::try_new(
            lance_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2])),
                Arc::new(arrow_array::BooleanArray::from(vec![false, false])),
                Arc::new(UInt64Array::from(vec![1u64, 1])),
                Arc::new(name_arr),
            ],
        )
        .unwrap();

        // Output schema: n._vid, n._labels, n.name
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("n._vid", DataType::UInt64, false),
            Field::new("n._labels", labels_data_type(), true),
            Field::new("n.name", DataType::Utf8, true),
        ]));

        let l0_ctx = L0Context::default();
        let result = map_to_output_schema(
            &batch,
            "Person",
            "n",
            &["name".to_string()],
            &output_schema,
            &l0_ctx,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "n._vid");
        assert_eq!(result.schema().field(1).name(), "n._labels");
        assert_eq!(result.schema().field(2).name(), "n.name");

        // Check name values carried through
        let name_col = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
    }

    /// `l0_value_may_equal` answers `false` only where the column builder's
    /// conversion is the identity and the values plainly differ; every
    /// coercing pairing is kept for the runtime filter to decide (W3b).
    #[test]
    fn l0_value_may_equal_drops_only_identity_converted_mismatches() {
        use uni_common::core::schema::DataType as T;
        let s = |v: &str| Value::String(v.into());

        // Missing or NULL is a NULL cell: never equal.
        assert!(!l0_value_may_equal(&T::String, None, &s("a")));
        assert!(!l0_value_may_equal(&T::String, Some(&Value::Null), &s("a")));

        // Identity conversions compare.
        assert!(l0_value_may_equal(&T::String, Some(&s("a")), &s("a")));
        assert!(!l0_value_may_equal(&T::String, Some(&s("a")), &s("b")));
        assert!(!l0_value_may_equal(
            &T::Int64,
            Some(&Value::Int(1)),
            &Value::Int(2)
        ));
        assert!(l0_value_may_equal(
            &T::Float64,
            Some(&Value::Int(1)),
            &Value::Float(1.0)
        ));
        assert!(!l0_value_may_equal(
            &T::Float64,
            Some(&Value::Float(1.5)),
            &Value::Int(1)
        ));
        assert!(!l0_value_may_equal(
            &T::Bool,
            Some(&Value::Bool(true)),
            &Value::Bool(false)
        ));

        // Coercing conversions are kept: `Int` renders into a STRING column as
        // "5", `Float` truncates into an INT64 column, NaN is left to Arrow.
        assert!(l0_value_may_equal(
            &T::String,
            Some(&Value::Int(5)),
            &s("5")
        ));
        assert!(l0_value_may_equal(
            &T::Int64,
            Some(&Value::Float(5.5)),
            &Value::Int(5)
        ));
        assert!(l0_value_may_equal(
            &T::Int64,
            Some(&Value::Int(5)),
            &Value::Float(5.5)
        ));
        assert!(l0_value_may_equal(
            &T::Float64,
            Some(&Value::Float(f64::NAN)),
            &Value::Float(f64::NAN)
        ));
        assert!(l0_value_may_equal(&T::CypherValue, Some(&s("a")), &s("b")));
    }

    /// The untyped (schemaless) variant drops only same-type scalar
    /// mismatches and missing/NULL values; a cross-type numeric pair, which
    /// Cypher may call equal, and every non-scalar are kept.
    #[test]
    fn l0_value_may_equal_untyped_drops_only_same_type_mismatches() {
        let s = |v: &str| Value::String(v.into());
        assert!(!l0_value_may_equal_untyped(None, &s("a")));
        assert!(!l0_value_may_equal_untyped(Some(&Value::Null), &s("a")));
        assert!(l0_value_may_equal_untyped(Some(&s("a")), &s("a")));
        assert!(!l0_value_may_equal_untyped(Some(&s("a")), &s("b")));
        assert!(!l0_value_may_equal_untyped(
            Some(&Value::Int(1)),
            &Value::Int(2)
        ));
        assert!(!l0_value_may_equal_untyped(
            Some(&Value::Bool(true)),
            &Value::Bool(false)
        ));
        assert!(!l0_value_may_equal_untyped(
            Some(&Value::Float(1.5)),
            &Value::Float(2.5)
        ));
        // Kept: `1 = 1.0` in Cypher; NaN; a string against an integer is left
        // to the filter; a list.
        assert!(l0_value_may_equal_untyped(
            Some(&Value::Int(1)),
            &Value::Float(1.0)
        ));
        assert!(l0_value_may_equal_untyped(
            Some(&Value::Float(1.0)),
            &Value::Int(2)
        ));
        assert!(l0_value_may_equal_untyped(
            Some(&Value::Float(f64::NAN)),
            &Value::Float(f64::NAN)
        ));
        assert!(l0_value_may_equal_untyped(Some(&s("1")), &Value::Int(1)));
        assert!(l0_value_may_equal_untyped(
            Some(&Value::List(vec![Value::Int(1)])),
            &Value::List(vec![Value::Int(2)])
        ));
    }

    /// The L0 range prefilter never drops a row the runtime filter keeps.
    ///
    /// The runtime filter compares with Arrow's kernels, which order floats by
    /// IEEE total order (NaN above everything, `-0.0` below `+0.0`). Over every
    /// pairing of stored value and literal from domains that straddle those
    /// edges, a `false` from [`l0_value_may_satisfy`] must coincide with the
    /// kernel's `false` (or NULL).
    #[test]
    fn l0_range_prefilter_is_a_superset_of_the_arrow_comparison() {
        use arrow::compute::kernels::cmp;
        use arrow_array::{Datum, Float64Array, Int64Array, Scalar as ArrowScalar, StringArray};
        use uni_common::core::schema::DataType as T;

        let floats = [
            f64::NAN,
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            1.0,
            2.5,
            f64::INFINITY,
        ];
        let ints = [i64::MIN, -3, -1, 0, 1, 2, 10, i64::MAX];
        let strs = [
            "",
            "a",
            "a\u{10FFFF}",
            "b",
            "u\u{D7FF}",
            "u\u{E000}",
            "é",
            "it's",
        ];
        let ops = [CmpOp::Lt, CmpOp::LtEq, CmpOp::Gt, CmpOp::GtEq];
        let kernel = |op: CmpOp, l: &dyn Datum, r: &dyn Datum| -> bool {
            let out = match op {
                CmpOp::Lt => cmp::lt(l, r),
                CmpOp::LtEq => cmp::lt_eq(l, r),
                CmpOp::Gt => cmp::gt(l, r),
                _ => cmp::gt_eq(l, r),
            }
            .expect("kernel");
            out.is_valid(0) && out.value(0)
        };
        let mut pruned = 0usize;
        for op in ops {
            // FLOAT64 column, stored Int or Float, literal Int or Float.
            for stored in floats {
                for lit in floats {
                    let want = kernel(
                        op,
                        &Float64Array::from(vec![stored]),
                        &ArrowScalar::new(Float64Array::from(vec![lit])),
                    );
                    let may = l0_value_may_satisfy(
                        &T::Float64,
                        Some(&Value::Float(stored)),
                        op,
                        &Value::Float(lit),
                    );
                    assert!(may || !want, "{stored} {op:?} {lit}");
                    pruned += usize::from(!may);
                }
            }
            for stored in ints {
                for lit in ints {
                    let want = kernel(
                        op,
                        &Int64Array::from(vec![stored]),
                        &ArrowScalar::new(Int64Array::from(vec![lit])),
                    );
                    let may = l0_value_may_satisfy(
                        &T::Int64,
                        Some(&Value::Int(stored)),
                        op,
                        &Value::Int(lit),
                    );
                    assert_eq!(may, want, "{stored} {op:?} {lit}");
                }
            }
            for stored in strs {
                for lit in strs {
                    let want = kernel(
                        op,
                        &StringArray::from(vec![stored]),
                        &ArrowScalar::new(StringArray::from(vec![lit])),
                    );
                    let may = l0_value_may_satisfy(
                        &T::String,
                        Some(&Value::String(stored.into())),
                        op,
                        &Value::String(lit.into()),
                    );
                    assert_eq!(may, want, "{stored:?} {op:?} {lit:?}");
                }
            }
            // NULL and missing values never satisfy a comparison.
            for stored in [None, Some(&Value::Null)] {
                assert!(!l0_value_may_satisfy(&T::Int64, stored, op, &Value::Int(1)));
            }
            // Cross-type pairings are left to the runtime filter.
            assert!(l0_value_may_satisfy(
                &T::String,
                Some(&Value::Int(5)),
                op,
                &Value::String("a".into())
            ));
            assert!(l0_value_may_satisfy(
                &T::Int64,
                Some(&Value::Int(5)),
                op,
                &Value::Float(1.5)
            ));
        }
        assert!(pruned > 0, "the float domain never pruned anything");
    }
}
