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
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    Time64NanosecondBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use uni_common::core::id::Vid;
use uni_common::{Properties, Value};

use crate::runtime::l0_visibility::L0Context;
use crate::storage::arrow_convert;

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
/// Returns the raw CypherValue bytes for `prop` if found in the blob,
/// or `None` if the blob is null or the key is absent.
pub fn extract_from_overflow_blob(
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    row: usize,
    prop: &str,
) -> Option<Vec<u8>> {
    let arr = overflow_arr?;
    if arr.is_null(row) {
        return None;
    }
    uni_common::cypher_value_codec::extract_map_entry_raw(arr.value(row), prop)
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
) -> ArrayRef {
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));

        // Check L0 buffers (later overwrites earlier)
        let l0_val = resolve_l0_property(&vid, prop, l0_ctx);

        if let Some(val_opt) = l0_val {
            append_value_as_cypher_binary(&mut builder, val_opt.as_ref());
        } else if let Some(bytes) = extract_from_overflow_blob(overflow_arr, i, prop) {
            builder.append_value(&bytes);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
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
pub fn build_all_props_column_for_schema_scan(
    batch: &RecordBatch,
    vid_arr: &UInt64Array,
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    projected_properties: &[String],
    l0_ctx: &L0Context,
) -> ArrayRef {
    // Collect schema-defined property column names (non-internal, non-overflow, non-_all_props)
    let schema_props: Vec<&str> = projected_properties
        .iter()
        .filter(|p| *p != "overflow_json" && *p != "_all_props" && !p.starts_with('_'))
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
                let val = arrow_convert::arrow_to_value(col.as_ref(), i, None);
                if !val.is_null() {
                    merged_props.insert(prop.to_string(), val);
                }
            }
        }

        // 2. Overflow_json properties
        if let Some(arr) = overflow_arr
            && !arr.is_null(i)
            && let Ok(uni_common::Value::Map(map)) =
                uni_common::cypher_value_codec::decode(arr.value(i))
        {
            merged_props.extend(map);
        }

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
    Arc::new(builder.finish())
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
            // CRDT binary properties: JSON-decoded CRDTs re-encoded to MessagePack
            let mut builder = BinaryBuilder::new();
            for vid in vids {
                let bytes = get_property_value(vid, props_map, prop_name)
                    .filter(|v| !v.is_null())
                    .and_then(|v| {
                        let json_val: serde_json::Value = v.into();
                        serde_json::from_value::<uni_crdt::Crdt>(json_val).ok()
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
        DataType::Struct(fields) => {
            // Map types are List(Struct(key, value)) — build struct inner elements
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
        // Fallback: serialize inner elements as strings
        _ => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v {
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
