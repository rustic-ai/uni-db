// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Arrow type conversion utilities for reducing cognitive complexity.
//!
//! This module provides shared helper functions and macros for converting
//! between Arrow arrays and JSON Values, reducing code duplication across
//! vertex.rs, delta.rs, and executor.rs.

use anyhow::{Result, anyhow};
use arrow_array::builder::{
    BinaryBuilder, BooleanBufferBuilder, BooleanBuilder, Date32Builder, DurationMicrosecondBuilder,
    FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, IntervalMonthDayNanoBuilder, LargeBinaryBuilder, ListBuilder, PrimitiveBuilder,
    StringBuilder, StructBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder,
    TimestampNanosecondBuilder, UInt8Builder, UInt32Builder,
};
use arrow_array::types::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int32Type, Int64Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, ListArray, StringArray,
    StringViewArray, StructArray, Time64NanosecondArray, TimestampNanosecondArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType as ArrowDataType, Field};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::DataType;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema;
use uni_common::core::schema::PointType;
use uni_crdt::Crdt;

/// Build a timestamp column from a map of ID -> timestamp (nanoseconds).
///
/// Shared utility for building `_created_at` and `_updated_at` columns
/// in vertex and edge tables. Works with any hashable ID type (Vid, Eid, etc.).
fn build_timestamp_column_from_id_map<K, I>(
    ids: I,
    timestamps: Option<&HashMap<K, i64>>,
) -> ArrayRef
where
    K: Eq + std::hash::Hash,
    I: IntoIterator<Item = K>,
{
    let mut builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
    for id in ids {
        match timestamps.and_then(|m| m.get(&id)) {
            Some(&ts) => builder.append_value(ts),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

pub fn build_timestamp_column_from_vid_map<I>(
    ids: I,
    timestamps: Option<&HashMap<Vid, i64>>,
) -> ArrayRef
where
    I: IntoIterator<Item = Vid>,
{
    build_timestamp_column_from_id_map(ids, timestamps)
}

pub fn build_timestamp_column_from_eid_map<I>(
    ids: I,
    timestamps: Option<&HashMap<Eid, i64>>,
) -> ArrayRef
where
    I: IntoIterator<Item = Eid>,
{
    build_timestamp_column_from_id_map(ids, timestamps)
}

/// Build a timestamp column from an iterator of optional timestamps.
///
/// This is useful for building timestamp columns directly from entry structs.
pub fn build_timestamp_column<I>(timestamps: I) -> ArrayRef
where
    I: IntoIterator<Item = Option<i64>>,
{
    let mut builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
    for ts in timestamps {
        builder.append_option(ts);
    }
    Arc::new(builder.finish())
}

/// Extract a `Vec<String>` from a single row of a `List<Utf8>` column.
///
/// Returns an empty vec when the row is null, the inner array is not a
/// `StringArray`, or the list is empty.  Null entries inside the list are
/// silently skipped.
pub fn labels_from_list_array(list_arr: &ListArray, row: usize) -> Vec<String> {
    if list_arr.is_null(row) {
        return Vec::new();
    }
    let values = list_arr.value(row);
    let Some(str_arr) = values.as_any().downcast_ref::<StringArray>() else {
        return Vec::new();
    };
    (0..str_arr.len())
        .filter(|&j| !str_arr.is_null(j))
        .map(|j| str_arr.value(j).to_string())
        .collect()
}

/// Parse a datetime string into nanoseconds since Unix epoch.
///
/// Tries RFC3339, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M%:z",
/// and "%Y-%m-%dT%H:%MZ" formats.
fn parse_datetime_to_nanos(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| {
            dt.with_timezone(&chrono::Utc)
                .timestamp_nanos_opt()
                .unwrap_or(0)
        })
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| ndt.and_utc().timestamp_nanos_opt().unwrap_or(0))
        })
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                .map(|ndt| ndt.and_utc().timestamp_nanos_opt().unwrap_or(0))
        })
        .or_else(|_| {
            chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M%:z").map(|dt| {
                dt.with_timezone(&chrono::Utc)
                    .timestamp_nanos_opt()
                    .unwrap_or(0)
            })
        })
        .ok()
        .or_else(|| {
            s.strip_suffix('Z')
                .and_then(|base| chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M").ok())
                .map(|ndt| ndt.and_utc().timestamp_nanos_opt().unwrap_or(0))
        })
}

/// Detect the Arrow Map-as-List(Struct(key, value)) pattern and reconstruct a map.
///
/// Arrow represents Map columns as `List(Struct { key, value })`. This helper
/// checks whether the given array matches that layout and, if so, converts the
/// key/value pairs back into a `HashMap<String, Value>`.
pub(crate) fn try_reconstruct_map(arr: &ArrayRef) -> Option<HashMap<String, Value>> {
    let structs = arr.as_any().downcast_ref::<StructArray>()?;
    let fields = structs.fields();
    if fields.len() != 2 || fields[0].name() != "key" || fields[1].name() != "value" {
        return None;
    }
    // A typed `Map(_, Bytes)` value child carries the `uni_raw_bytes` marker; decode
    // each value verbatim. CV-encoded map values carry no marker → codec path.
    let value_hint = raw_bytes_hint(fields[1].metadata());
    let key_col = structs.column(0);
    let val_col = structs.column(1);
    let mut map = HashMap::new();
    for i in 0..structs.len() {
        // A key or value that fails to decode abandons the reconstruction
        // rather than silently dropping an entry: a map missing one pair is
        // a wrong answer, and the caller's fallback path can still try.
        let Ok(Value::String(k)) = arrow_to_value(key_col.as_ref(), i, None) else {
            return None;
        };
        let v = arrow_to_value(val_col.as_ref(), i, value_hint).ok()?;
        map.insert(k, v);
    }
    Some(map)
}

/// Convert all elements of an Arrow array into a `Vec<Value>`.
///
/// `elem_type` is the schema hint for each element — `Some(DataType::Bytes)` when the
/// list child field is marked `uni_raw_bytes`, so raw `Bytes` elements decode verbatim.
fn array_to_value_list(arr: &ArrayRef, elem_type: Option<&DataType>) -> anyhow::Result<Vec<Value>> {
    (0..arr.len())
        .map(|i| arrow_to_value(arr.as_ref(), i, elem_type))
        .collect()
}

/// The decode hint carried by an Arrow field's own metadata.
///
/// A `LargeBinary` column is how both a CypherValue blob and a raw `Bytes`
/// value are transported, and only the field's `uni_raw_bytes` marker tells
/// them apart. A caller that has the `Field` and passes `None` throws that
/// away, leaving [`arrow_to_value`] to sniff — which it can only do by trying
/// the codec and degrading when it fails.
///
/// Pass this wherever a `Field` is in scope, so the ambiguity is confined to
/// callers that genuinely have only an `ArrayRef` (#233 class).
#[must_use]
pub fn type_hint_for_field(field: &arrow_schema::Field) -> Option<&'static DataType> {
    raw_bytes_hint(field.metadata())
}

/// Returns `Some(&DataType::Bytes)` when Arrow field metadata marks the field as a
/// raw `Bytes` value (`uni_raw_bytes=true`), else `None`. Used to discriminate raw
/// `Bytes` container children from CV-encoded `LargeBinary` without array sniffing.
fn raw_bytes_hint(metadata: &HashMap<String, String>) -> Option<&'static DataType> {
    if metadata.get("uni_raw_bytes").is_some_and(|v| v == "true") {
        Some(&DataType::Bytes)
    } else {
        None
    }
}

/// Extracts the raw-`Bytes` element hint from a list-like Arrow type's child field.
fn list_child_bytes_hint(dt: &ArrowDataType) -> Option<&'static DataType> {
    match dt {
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _) => raw_bytes_hint(f.metadata()),
        _ => None,
    }
}

/// Convert an Arrow array value at a given row index to a Uni Value.
///
/// Handles all common Arrow types and recursively processes nested structures
/// like Lists and Structs. The optional `data_type` parameter provides schema
/// context for decoding DateTime and Time struct arrays; when provided, it
/// takes precedence over runtime type detection.
pub fn arrow_to_value(
    col: &dyn Array,
    row: usize,
    data_type: Option<&DataType>,
) -> anyhow::Result<Value> {
    if col.is_null(row) {
        return Ok(Value::Null);
    }

    // Schema-driven decode for DateTime and Time structs
    if let Some(dt) = data_type {
        match dt {
            DataType::DateTime => {
                // Expect StructArray with three fields
                if let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>()
                    && let (Some(nanos_col), Some(offset_col), Some(tz_col)) = (
                        struct_arr.column_by_name("nanos_since_epoch"),
                        struct_arr.column_by_name("offset_seconds"),
                        struct_arr.column_by_name("timezone_name"),
                    )
                    && let (Some(nanos_arr), Some(offset_arr), Some(tz_arr)) = (
                        nanos_col
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>(),
                        offset_col.as_any().downcast_ref::<Int32Array>(),
                        tz_col.as_any().downcast_ref::<StringArray>(),
                    )
                {
                    if nanos_arr.is_null(row) {
                        return Ok(Value::Null);
                    }
                    let nanos = nanos_arr.value(row);
                    if offset_arr.is_null(row) {
                        // No offset → LocalDateTime
                        return Ok(Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                            nanos_since_epoch: nanos,
                        }));
                    }
                    let offset = offset_arr.value(row);
                    let tz_name = (!tz_arr.is_null(row)).then(|| tz_arr.value(row).to_string());
                    return Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                        nanos_since_epoch: nanos,
                        offset_seconds: offset,
                        timezone_name: tz_name,
                    }));
                }
                // Fall back to old schema migration: TimestampNanosecond → DateTime with offset=0
                if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    let nanos = ts.value(row);
                    let tz_name = ts.timezone().map(|s| s.to_string());
                    return Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                        nanos_since_epoch: nanos,
                        offset_seconds: 0,
                        timezone_name: tz_name,
                    }));
                }
            }
            DataType::Time => {
                // Expect StructArray with two fields
                if let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>()
                    && let (Some(nanos_col), Some(offset_col)) = (
                        struct_arr.column_by_name("nanos_since_midnight"),
                        struct_arr.column_by_name("offset_seconds"),
                    )
                    && let (Some(nanos_arr), Some(offset_arr)) = (
                        nanos_col.as_any().downcast_ref::<Time64NanosecondArray>(),
                        offset_col.as_any().downcast_ref::<Int32Array>(),
                    )
                {
                    // Check field-level nulls before calling .value()
                    if nanos_arr.is_null(row) || offset_arr.is_null(row) {
                        return Ok(Value::Null);
                    }
                    let nanos = nanos_arr.value(row);
                    let offset = offset_arr.value(row);
                    return Ok(Value::Temporal(uni_common::TemporalValue::Time {
                        nanos_since_midnight: nanos,
                        offset_seconds: offset,
                    }));
                }
                // Fall back to old schema: Time64Nanosecond → Time with offset=0
                if let Some(t) = col.as_any().downcast_ref::<Time64NanosecondArray>() {
                    let nanos = t.value(row);
                    return Ok(Value::Temporal(uni_common::TemporalValue::Time {
                        nanos_since_midnight: nanos,
                        offset_seconds: 0,
                    }));
                }
            }
            DataType::Point(pt) => {
                // Reconstruct the `Value::Map` shape produced by `point(...)`
                // (`spatial.rs`) from the declared struct layout. A Point-typed
                // column can only hold points, so anything not reconstructable
                // (non-struct, missing fields) is `Null` — never a bare scalar and
                // never a panic. Returns unconditionally rather than falling
                // through to the generic handler.
                if let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() {
                    let f64_at = |name: &str| -> Option<f64> {
                        struct_arr
                            .column_by_name(name)
                            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                            .filter(|a| !a.is_null(row))
                            .map(|a| a.value(row))
                    };
                    let crs = struct_arr
                        .column_by_name("crs")
                        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                        .filter(|a| !a.is_null(row))
                        .map(|a| a.value(row).to_string());

                    match pt {
                        PointType::Geographic => {
                            if let (Some(lat), Some(lon)) =
                                (f64_at("latitude"), f64_at("longitude"))
                            {
                                return Ok(Value::Map(HashMap::from([
                                    ("type".to_string(), Value::String("Point".into())),
                                    (
                                        "crs".to_string(),
                                        Value::String(crs.unwrap_or_else(|| "WGS84".into())),
                                    ),
                                    ("latitude".to_string(), Value::Float(lat)),
                                    ("longitude".to_string(), Value::Float(lon)),
                                ])));
                            }
                        }
                        PointType::Cartesian2D => {
                            if let (Some(x), Some(y)) = (f64_at("x"), f64_at("y")) {
                                return Ok(Value::Map(HashMap::from([
                                    ("type".to_string(), Value::String("Point".into())),
                                    (
                                        "crs".to_string(),
                                        Value::String(crs.unwrap_or_else(|| "cartesian".into())),
                                    ),
                                    ("x".to_string(), Value::Float(x)),
                                    ("y".to_string(), Value::Float(y)),
                                ])));
                            }
                        }
                        PointType::Cartesian3D => {
                            if let (Some(x), Some(y), Some(z)) =
                                (f64_at("x"), f64_at("y"), f64_at("z"))
                            {
                                return Ok(Value::Map(HashMap::from([
                                    ("type".to_string(), Value::String("Point".into())),
                                    (
                                        "crs".to_string(),
                                        Value::String(crs.unwrap_or_else(|| "cartesian-3d".into())),
                                    ),
                                    ("x".to_string(), Value::Float(x)),
                                    ("y".to_string(), Value::Float(y)),
                                    ("z".to_string(), Value::Float(z)),
                                ])));
                            }
                        }
                    }
                }
                // Point-typed but not a reconstructable point → Null.
                return Ok(Value::Null);
            }
            DataType::Bytes => {
                let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() else {
                    return Err(anyhow::anyhow!("Bytes column is not LargeBinaryArray"));
                };
                if arr.is_null(row) {
                    return Ok(Value::Null);
                }
                return Ok(Value::Bytes(arr.value(row).to_vec()));
            }
            DataType::Btic => {
                let Some(fsb) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
                    return Err(anyhow::anyhow!("BTIC column is not FixedSizeBinaryArray"));
                };
                let bytes = fsb.value(row);
                let btic = uni_btic::encode::decode_slice(bytes)
                    .map_err(|e| anyhow::anyhow!("BTIC decode failed: {e}"))?;
                return Ok(Value::Temporal(uni_common::TemporalValue::Btic {
                    lo: btic.lo(),
                    hi: btic.hi(),
                    meta: btic.meta(),
                }));
            }
            DataType::SparseVector { .. } => {
                let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() else {
                    return Err(anyhow::anyhow!("SparseVector column is not StructArray"));
                };
                if struct_arr.is_null(row) {
                    return Ok(Value::Null);
                }
                let (Some(indices_list), Some(values_list)) = (
                    struct_arr
                        .column_by_name("indices")
                        .and_then(|c| c.as_any().downcast_ref::<ListArray>()),
                    struct_arr
                        .column_by_name("values")
                        .and_then(|c| c.as_any().downcast_ref::<ListArray>()),
                ) else {
                    return Err(anyhow::anyhow!(
                        "SparseVector struct missing indices/values list columns"
                    ));
                };
                let idx_vals = indices_list.value(row);
                let Some(idx_arr) = idx_vals.as_any().downcast_ref::<UInt32Array>() else {
                    return Err(anyhow::anyhow!("SparseVector 'indices' inner not UInt32"));
                };
                let w_vals = values_list.value(row);
                let Some(w_arr) = w_vals.as_any().downcast_ref::<Float32Array>() else {
                    return Err(anyhow::anyhow!("SparseVector 'values' inner not Float32"));
                };
                let indices: Vec<u32> = (0..idx_arr.len()).map(|i| idx_arr.value(i)).collect();
                let values: Vec<f32> = (0..w_arr.len()).map(|i| w_arr.value(i)).collect();
                return Ok(Value::SparseVector { indices, values });
            }
            _ => {}
        }
    }

    // String types
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::String(s.value(row).to_string()));
    }
    // `LargeUtf8`. Distinct from `Utf8` at the Arrow level, so it needs its own
    // downcast — a `StringArray` downcast does not match a `LargeStringArray`.
    // Locy reaches this constantly: `infer_expr_type` types string literals and
    // every string-returning function (`toUpper`, `substring`, `toString`, ...)
    // as `LargeUtf8`, and `plan_locy_project` casts the column to match. Without
    // this arm all of those fell through to the `Value::Null` fallback below,
    // so `derived` reported NULL while `QUERY` (which evaluates natively, never
    // touching Arrow) reported the real value.
    if let Some(s) = col.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Value::String(s.value(row).to_string()));
    }
    // `Utf8View`. DataFusion 54 returns `Utf8View` from a growing set of string functions (`left`, `right`, `split_part` joined `btrim`, `initcap`, `md5`, `substr` in 54). It is a distinct Arrow type, so a `StringArray` / `LargeStringArray` downcast does not match it, which is how `left()`/`right()` started
    // erroring here on the DataFusion 54 upgrade. Expect this set to keep
    // growing as DataFusion migrates more functions to StringView.
    if let Some(s) = col.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Value::String(s.value(row).to_string()));
    }

    // Integer types
    // An engine `UInt64` is an identity: its bits, not a value (see `Value::from_id_bits`).
    if let Some(u) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Value::from_id_bits(u.value(row)));
    }
    if let Some(i) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::Int(i.value(row)));
    }
    if let Some(i) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(Value::Int(i.value(row) as i64));
    }

    // Float types
    if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(Value::Float(f.value(row)));
    }
    if let Some(f) = col.as_any().downcast_ref::<Float32Array>() {
        return Ok(Value::Float(f.value(row) as f64));
    }

    // Boolean type
    if let Some(b) = col.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Value::Bool(b.value(row)));
    }

    // Fixed-size list: a `Float32` child is a dense vector and round-trips as
    // `Value::Vector`, preserving type identity (parity with `SparseVector` /
    // `Btic`); any other element type decodes as a generic list. The recursive
    // inner-element decode for multivector tokens (via `array_to_value_list`)
    // reaches this same arm, so a `List<FixedSizeList<Float32>>` multivector
    // decodes as a `Value::List` of `Value::Vector` tokens.
    if let Some(list) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        let inner = list.value(row);
        if let Some(floats) = inner.as_any().downcast_ref::<Float32Array>() {
            return Ok(Value::Vector(
                (0..floats.len()).map(|i| floats.value(i)).collect(),
            ));
        }
        // A `UInt8` child is a binary vector — round-trip as `Value::BinaryVector`
        // to preserve type identity (parity with the `Float32` dense-vector arm).
        if let Some(bytes) = inner.as_any().downcast_ref::<UInt8Array>() {
            return Ok(Value::BinaryVector(
                (0..bytes.len()).map(|i| bytes.value(i)).collect(),
            ));
        }
        let elem_hint = list_child_bytes_hint(list.data_type());
        return Ok(Value::List(array_to_value_list(&inner, elem_hint)?));
    }

    // Variable-size list
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        let arr = list.value(row);

        // Map types are stored as List(Struct(key, value)); reconstruct as map
        if let Some(obj) = try_reconstruct_map(&arr) {
            return Ok(Value::Map(obj));
        }

        let elem_hint = list_child_bytes_hint(list.data_type());
        return Ok(Value::List(array_to_value_list(&arr, elem_hint)?));
    }

    // Large list (variable-size list with i64 offsets)
    if let Some(list) = col.as_any().downcast_ref::<arrow_array::LargeListArray>() {
        let elem_hint = list_child_bytes_hint(list.data_type());
        return Ok(Value::List(array_to_value_list(
            &list.value(row),
            elem_hint,
        )?));
    }

    // Struct type — detect temporal structs by field names before generic handler
    if let Some(s) = col.as_any().downcast_ref::<StructArray>() {
        // Sparse-vector struct: the result-row path calls with `data_type = None`,
        // so detect it by Arrow shape here (otherwise the generic struct→Map
        // handler below reads the `List<UInt32>` indices child as null).
        if schema::is_sparse_vector_struct(col.data_type()) {
            if s.is_null(row) {
                return Ok(Value::Null);
            }
            if let (Some(idx_list), Some(val_list)) = (
                s.column_by_name("indices")
                    .and_then(|c| c.as_any().downcast_ref::<ListArray>()),
                s.column_by_name("values")
                    .and_then(|c| c.as_any().downcast_ref::<ListArray>()),
            ) {
                let idx_vals = idx_list.value(row);
                let val_vals = val_list.value(row);
                if let (Some(ia), Some(va)) = (
                    idx_vals.as_any().downcast_ref::<UInt32Array>(),
                    val_vals.as_any().downcast_ref::<Float32Array>(),
                ) {
                    let indices = (0..ia.len()).map(|i| ia.value(i)).collect();
                    let values = (0..va.len()).map(|i| va.value(i)).collect();
                    return Ok(Value::SparseVector { indices, values });
                }
            }
        }

        let field_names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();

        // DateTime struct: {nanos_since_epoch, offset_seconds, timezone_name}
        if field_names.contains(&"nanos_since_epoch")
            && field_names.contains(&"offset_seconds")
            && field_names.contains(&"timezone_name")
            && let (Some(nanos_col), Some(offset_col), Some(tz_col)) = (
                s.column_by_name("nanos_since_epoch"),
                s.column_by_name("offset_seconds"),
                s.column_by_name("timezone_name"),
            )
        {
            // Try TimestampNanosecond first (standard schema), then Int64 fallback
            let nanos_opt = nanos_col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .map(|a| {
                    if a.is_null(row) {
                        None
                    } else {
                        Some(a.value(row))
                    }
                })
                .or_else(|| {
                    nanos_col.as_any().downcast_ref::<Int64Array>().map(|a| {
                        if a.is_null(row) {
                            None
                        } else {
                            Some(a.value(row))
                        }
                    })
                });
            let offset_opt = offset_col.as_any().downcast_ref::<Int32Array>().map(|a| {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            });

            if let Some(Some(nanos)) = nanos_opt {
                match offset_opt {
                    Some(Some(offset)) => {
                        let tz_name = tz_col.as_any().downcast_ref::<StringArray>().and_then(|a| {
                            if a.is_null(row) {
                                None
                            } else {
                                Some(a.value(row).to_string())
                            }
                        });
                        return Ok(Value::Temporal(uni_common::TemporalValue::DateTime {
                            nanos_since_epoch: nanos,
                            offset_seconds: offset,
                            timezone_name: tz_name,
                        }));
                    }
                    _ => {
                        // No offset → LocalDateTime
                        return Ok(Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                            nanos_since_epoch: nanos,
                        }));
                    }
                }
            }
        }

        // Time struct: {nanos_since_midnight, offset_seconds}
        if field_names.contains(&"nanos_since_midnight")
            && field_names.contains(&"offset_seconds")
            && let (Some(nanos_col), Some(offset_col)) = (
                s.column_by_name("nanos_since_midnight"),
                s.column_by_name("offset_seconds"),
            )
        {
            // Try Time64Nanosecond first (standard schema), then Int64 fallback
            let nanos_opt = nanos_col
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .map(|a| {
                    if a.is_null(row) {
                        None
                    } else {
                        Some(a.value(row))
                    }
                })
                .or_else(|| {
                    nanos_col.as_any().downcast_ref::<Int64Array>().map(|a| {
                        if a.is_null(row) {
                            None
                        } else {
                            Some(a.value(row))
                        }
                    })
                });
            let offset_opt = offset_col.as_any().downcast_ref::<Int32Array>().map(|a| {
                if a.is_null(row) {
                    None
                } else {
                    Some(a.value(row))
                }
            });

            if let (Some(Some(nanos)), Some(Some(offset))) = (nanos_opt, offset_opt) {
                return Ok(Value::Temporal(uni_common::TemporalValue::Time {
                    nanos_since_midnight: nanos,
                    offset_seconds: offset,
                }));
            }
        }

        // Generic struct → Map
        let mut map = HashMap::new();
        for (field, child) in s.fields().iter().zip(s.columns()) {
            map.insert(
                field.name().clone(),
                arrow_to_value(child.as_ref(), row, None)?,
            );
        }
        return Ok(Value::Map(map));
    }

    // Date32 type (days since epoch) - return as Value::Temporal
    if let Some(d) = col.as_any().downcast_ref::<Date32Array>() {
        let days = d.value(row);
        return Ok(Value::Temporal(uni_common::TemporalValue::Date {
            days_since_epoch: days,
        }));
    }

    // Timestamp (nanoseconds since epoch) - timezone presence determines DateTime vs LocalDateTime
    if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let nanos = ts.value(row);
        return Ok(match ts.timezone() {
            Some(tz) => Value::Temporal(uni_common::TemporalValue::DateTime {
                nanos_since_epoch: nanos,
                offset_seconds: 0,
                timezone_name: Some(tz.to_string()),
            }),
            None => Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                nanos_since_epoch: nanos,
            }),
        });
    }

    // Time64 (nanoseconds since midnight) - return as Value::Temporal
    if let Some(t) = col.as_any().downcast_ref::<Time64NanosecondArray>() {
        let nanos = t.value(row);
        return Ok(Value::Temporal(uni_common::TemporalValue::LocalTime {
            nanos_since_midnight: nanos,
        }));
    }

    // Time64 (microseconds since midnight) - convert to nanoseconds
    if let Some(t) = col
        .as_any()
        .downcast_ref::<arrow_array::Time64MicrosecondArray>()
    {
        let micros = t.value(row);
        return Ok(Value::Temporal(uni_common::TemporalValue::LocalTime {
            nanos_since_midnight: micros * 1000,
        }));
    }

    // DurationMicrosecond - convert to Duration with nanoseconds
    if let Some(d) = col
        .as_any()
        .downcast_ref::<arrow_array::DurationMicrosecondArray>()
    {
        let micros = d.value(row);
        let total_nanos = micros * 1000;
        let seconds = total_nanos / 1_000_000_000;
        let remaining_nanos = total_nanos % 1_000_000_000;
        return Ok(Value::Temporal(uni_common::TemporalValue::Duration {
            months: 0,
            days: 0,
            nanos: seconds * 1_000_000_000 + remaining_nanos,
        }));
    }

    // IntervalMonthDayNano - return as Value::Temporal(Duration)
    if let Some(interval) = col.as_any().downcast_ref::<IntervalMonthDayNanoArray>() {
        let val = interval.value(row);
        return Ok(Value::Temporal(uni_common::TemporalValue::Duration {
            months: val.months as i64,
            days: val.days as i64,
            nanos: val.nanoseconds,
        }));
    }

    // LargeBinary (CypherValue MessagePack-tagged encoding)
    if let Some(b) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        let bytes = b.value(row);
        if bytes.is_empty() {
            return Ok(Value::Null);
        }
        // No `data_type` hint means nothing has said this column holds a
        // CypherValue rather than opaque bytes, and both are legitimate here:
        // no writer stamps `uni_raw_bytes` on every such column. So an
        // undecodable payload degrades to the bytes themselves, which is the
        // contract `procedure.rs` already documents and pins
        // (`undecodable_bytes_fall_back_instead_of_erroring`).
        //
        // This is *not* the #233 shape it replaced: `Value::Bytes` carries the
        // payload, where the previous `Value::Null` was a legal value that made
        // an unreadable property indistinguishable from an absent one. The
        // typed path below — where `data_type` does say CypherValue — errors.
        return Ok(uni_common::cypher_value_codec::decode(bytes)
            .unwrap_or_else(|_| Value::Bytes(bytes.to_vec())));
    }

    // FixedSizeBinary(24) — BTIC temporal interval
    if let Some(fsb) = col.as_any().downcast_ref::<FixedSizeBinaryArray>()
        && fsb.value_length() == 24
    {
        let bytes = fsb.value(row);
        let btic = uni_btic::encode::decode_slice(bytes)
            .map_err(|e| anyhow::anyhow!("BTIC decode failed: {e}"))?;
        return Ok(Value::Temporal(uni_common::TemporalValue::Btic {
            lo: btic.lo(),
            hi: btic.hi(),
            meta: btic.meta(),
        }));
    }

    // Binary (CRDT MessagePack) - decode to Value via serde_json boundary
    if let Some(b) = col.as_any().downcast_ref::<BinaryArray>() {
        let bytes = b.value(row);
        // A corrupt CRDT blob is unreadable, not null. `Value::Null` is a legal
        // property value, so collapsing to it made the two indistinguishable
        // (#233 class). Unlike the untyped `LargeBinary` arm below — where only
        // the field's `uni_raw_bytes` marker separates a CypherValue from raw
        // bytes, so the arm can merely sniff — a `BinaryArray` reaching here is
        // unambiguously CRDT MessagePack, so the failure is a real error.
        let crdt = Crdt::from_msgpack(bytes)
            .map_err(|e| anyhow::anyhow!("CRDT MessagePack decode failed: {e}"))?;
        return Ok(Value::from(serde_json::to_value(&crdt)?));
    }

    // Arrow's Null type carries no values by construction, so `Value::Null` is
    // the correct decode, not a gap. Listed explicitly so it does not reach the
    // diagnostic below.
    if *col.data_type() == ArrowDataType::Null {
        return Ok(Value::Null);
    }

    // `_uid` — the 32-byte content hash on vertex/index datasets
    // (`vertex.rs`, `main_vertex.rs`, `index.rs`). Internal, never user-facing,
    // and by far the most common visitor here (~750 calls in one integration
    // run), which is why it is matched explicitly rather than left to warn on
    // every cell. The `FixedSizeBinary(24)` arm above claims BTIC; every other
    // width is this.
    if matches!(col.data_type(), ArrowDataType::FixedSizeBinary(_)) {
        return Ok(Value::Null);
    }

    // Fallback: an Arrow type no arm above handles. Decoding to `Null` silently
    // is how the missing `LargeUtf8` arm survived long enough to be written down
    // as a Locy language constraint — every string literal and string-returning
    // function in a `YIELD` came back NULL with no diagnostic anywhere. Warn so
    // the next gap is visible.
    //
    // Deliberately not an error: this decoder is shared with the Cypher read
    // path and not every caller has been audited for types that legitimately
    // land here. The two high-volume benign cases are handled above, so
    // reaching this point is genuinely unexpected and worth a line in the log.
    Err(anyhow::anyhow!(
        "arrow_to_value: no decoder for Arrow type {:?}. This used to return \
         Null, which is a legal value, so a column this decoder cannot read \
         was indistinguishable from one genuinely absent — the shape that hid \
         a missing LargeUtf8 arm. Add a downcast arm for it.",
        col.data_type()
    ))
}

/// Shared body of the primitive `values_to_*_array` helpers: build a nullable
/// Arrow primitive array, appending a null wherever `extract` yields `None`.
fn values_to_primitive<T: ArrowPrimitiveType>(
    values: &[Value],
    extract: impl Fn(&Value) -> Option<T::Native>,
) -> ArrayRef {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for v in values {
        match extract(v) {
            Some(n) => builder.append_value(n),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn values_to_uint64_array(values: &[Value]) -> ArrayRef {
    values_to_primitive::<UInt64Type>(values, Value::as_u64)
}

fn values_to_int64_array(values: &[Value]) -> ArrayRef {
    values_to_primitive::<Int64Type>(values, Value::as_i64)
}

fn values_to_int32_array(values: &[Value]) -> ArrayRef {
    // Wrapping `as i32` is this path's long-standing narrowing behavior; do not
    // swap it for `i32::try_from` (which would null out-of-range values).
    values_to_primitive::<Int32Type>(values, |v| v.as_i64().map(|n| n as i32))
}

fn values_to_string_array(values: &[Value]) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 10);
    for v in values {
        if let Some(s) = v.as_str() {
            builder.append_value(s);
        } else if v.is_null() {
            builder.append_null();
        } else {
            builder.append_value(v.to_string());
        }
    }
    Arc::new(builder.finish())
}

fn values_to_bool_array(values: &[Value]) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for v in values {
        if let Some(b) = v.as_bool() {
            builder.append_value(b);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_float32_array(values: &[Value]) -> ArrayRef {
    values_to_primitive::<Float32Type>(values, |v| v.as_f64().map(|n| n as f32))
}

fn values_to_float64_array(values: &[Value]) -> ArrayRef {
    values_to_primitive::<Float64Type>(values, Value::as_f64)
}

fn values_to_fixed_size_binary_array(values: &[Value], size: i32) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), size);
    for v in values {
        match v {
            Value::Temporal(uni_common::TemporalValue::Btic { lo, hi, meta }) if size == 24 => {
                let btic = uni_btic::Btic::new(*lo, *hi, *meta)
                    .map_err(|e| anyhow!("invalid BTIC value: {}", e))?;
                builder.append_value(uni_btic::encode::encode(&btic))?;
            }
            Value::String(s) if size == 24 => match uni_btic::parse::parse_btic_literal(s) {
                Ok(b) => builder.append_value(uni_btic::encode::encode(&b))?,
                Err(_) => builder.append_null(),
            },
            Value::List(bytes) => {
                let b: Vec<u8> = bytes
                    .iter()
                    .map(|bv| bv.as_u64().unwrap_or(0) as u8)
                    .collect();
                if b.len() as i32 == size {
                    builder.append_value(&b)?;
                } else {
                    builder.append_null();
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Extract f32 vector values from a Value, ensuring correct Arrow FixedSizeList invariants.
///
/// Always returns exactly `dimensions` f32 values (zeros for null/invalid), plus a validity flag.
/// This guarantees `child_array.len() == parent_array.len() × dimensions`.
///
/// # Arguments
/// - `val`: Optional property value to extract from
/// - `is_deleted`: Whether the containing entity is deleted (affects validity)
/// - `dimensions`: Expected vector dimensions
///
/// # Returns
/// - Tuple of (vector values, validity flag)
///   - Vector always has exactly `dimensions` elements
///   - Validity is `true` for valid vectors or deleted entries, `false` for null/invalid
pub fn extract_vector_f32_values(
    val: Option<&Value>,
    is_deleted: bool,
    dimensions: usize,
) -> (Vec<f32>, bool) {
    let zeros = || vec![0.0_f32; dimensions];

    // Deleted entries always return zeros with valid=true
    if is_deleted {
        return (zeros(), true);
    }

    match val {
        // Native f32 vector (Value::Vector)
        Some(Value::Vector(v)) if v.len() == dimensions => (v.clone(), true),
        Some(Value::Vector(_)) => (zeros(), false), // Wrong dimensions
        // List of values (Value::List) - convert to f32
        Some(Value::List(arr)) if arr.len() == dimensions => {
            let values: Vec<f32> = arr
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                .collect();
            (values, true)
        }
        Some(Value::List(_)) => (zeros(), false), // Wrong dimensions
        _ => (zeros(), false),                    // Missing or unsupported value
    }
}

/// Fail-closed sibling of [`extract_vector_f32_values`] for declared-schema columns.
///
/// The lenient extractor silently nulls a wrong-dimension or wrong-shape value —
/// which is how issue #137's corruption reached storage. On the declared-schema
/// flush path (`PropertyExtractor::build_vector_column` and the multi-vector list
/// builder) that is always a bug or a guard bypass, so this variant errors instead.
/// Deleted rows and genuine nulls (`None` / `Value::Null`) keep the lenient
/// behavior — they are legal null rows, not data loss.
///
/// # Errors
/// Returns the underlying [`VectorDimError`](uni_common::core::schema::VectorDimError)
/// when a present, non-null value has the wrong dimensions, a non-numeric element,
/// or is not a vector at all.
pub fn extract_vector_f32_values_strict(
    val: Option<&Value>,
    is_deleted: bool,
    dimensions: usize,
) -> Result<(Vec<f32>, bool)> {
    if !is_deleted && let Some(v) = val {
        uni_common::core::schema::check_dense_vector_value(v, dimensions)?;
    }
    Ok(extract_vector_f32_values(val, is_deleted, dimensions))
}

/// Extract `u8` lane values from a Value for a `BinaryVector(dimensions)` column.
///
/// The binary-vector analogue of [`extract_vector_f32_values`]: always returns
/// exactly `dimensions` bytes (zeros for null/invalid), plus a validity flag, so
/// the Arrow `FixedSizeList<UInt8>` stride invariant holds. Accepts a
/// [`Value::BinaryVector`] or a [`Value::List`] of byte-valued integers.
///
/// # Returns
/// `(lane bytes, validity)` — validity is `true` for a valid vector or a deleted
/// entry, `false` for a null/wrong-shape value.
pub fn extract_binary_vector_values(
    val: Option<&Value>,
    is_deleted: bool,
    dimensions: usize,
) -> (Vec<u8>, bool) {
    let zeros = || vec![0_u8; dimensions];

    if is_deleted {
        return (zeros(), true);
    }

    match val {
        Some(Value::BinaryVector(b)) if b.len() == dimensions => (b.clone(), true),
        Some(Value::BinaryVector(_)) => (zeros(), false), // Wrong lane count
        Some(Value::List(arr)) if arr.len() == dimensions => {
            let mut bytes = Vec::with_capacity(dimensions);
            for v in arr {
                match v.as_i64() {
                    Some(n @ 0..=255) => bytes.push(n as u8),
                    _ => return (zeros(), false), // Non-byte element
                }
            }
            (bytes, true)
        }
        Some(Value::List(_)) => (zeros(), false), // Wrong lane count
        _ => (zeros(), false),                    // Missing or unsupported value
    }
}

/// Fail-closed sibling of [`extract_binary_vector_values`] for declared-schema
/// columns, mirroring [`extract_vector_f32_values_strict`].
///
/// # Errors
/// Returns the underlying [`VectorDimError`](uni_common::core::schema::VectorDimError)
/// when a present, non-null value has the wrong lane count, a non-byte element,
/// or is not a binary vector at all.
pub fn extract_binary_vector_values_strict(
    val: Option<&Value>,
    is_deleted: bool,
    dimensions: usize,
) -> Result<(Vec<u8>, bool)> {
    if !is_deleted && let Some(v) = val {
        uni_common::core::schema::check_binary_vector_value(v, dimensions)?;
    }
    Ok(extract_binary_vector_values(val, is_deleted, dimensions))
}

fn values_to_fixed_size_list_f32_array(values: &[Value], size: i32) -> ArrayRef {
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), size);
    for v in values {
        let (vals, valid) = extract_vector_f32_values(Some(v), false, size as usize);
        for val in vals {
            builder.values().append_value(val);
        }
        builder.append(valid);
    }
    Arc::new(builder.finish())
}

fn values_to_timestamp_array(values: &[Value], tz: Option<&Arc<str>>) -> ArrayRef {
    let mut builder = TimestampNanosecondBuilder::with_capacity(values.len());
    for v in values {
        if v.is_null() {
            builder.append_null();
        } else if let Value::Temporal(tv) = v {
            match tv {
                uni_common::TemporalValue::DateTime {
                    nanos_since_epoch, ..
                }
                | uni_common::TemporalValue::LocalDateTime {
                    nanos_since_epoch, ..
                } => builder.append_value(*nanos_since_epoch),
                _ => builder.append_null(),
            }
        } else if let Some(n) = v.as_i64() {
            builder.append_value(n);
        } else if let Some(s) = v.as_str() {
            match parse_datetime_to_nanos(s) {
                Some(nanos) => builder.append_value(nanos),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    }

    let arr = builder.finish();
    if let Some(tz) = tz {
        Arc::new(arr.with_timezone(tz.as_ref()))
    } else {
        Arc::new(arr)
    }
}

/// Build a DateTime struct array from values.
///
/// Encodes DateTime as a 3-field struct: (nanos_since_epoch, offset_seconds, timezone_name).
/// This preserves timezone offset information that was previously lost with TimestampNanosecond encoding.
fn values_to_datetime_struct_array(values: &[Value]) -> ArrayRef {
    let mut nanos_builder = TimestampNanosecondBuilder::with_capacity(values.len());
    let mut offset_builder = Int32Builder::with_capacity(values.len());
    let mut tz_builder = StringBuilder::with_capacity(values.len(), values.len() * 20);
    let mut null_buffer = BooleanBufferBuilder::new(values.len());

    for v in values {
        match v {
            Value::Temporal(uni_common::TemporalValue::DateTime {
                nanos_since_epoch,
                offset_seconds,
                timezone_name,
            }) => {
                nanos_builder.append_value(*nanos_since_epoch);
                offset_builder.append_value(*offset_seconds);
                tz_builder.append_option(timezone_name.as_deref());
                null_buffer.append(true);
            }
            Value::Temporal(uni_common::TemporalValue::LocalDateTime { nanos_since_epoch }) => {
                nanos_builder.append_value(*nanos_since_epoch);
                offset_builder.append_null();
                tz_builder.append_null();
                null_buffer.append(true);
            }
            _ => {
                nanos_builder.append_null();
                offset_builder.append_null();
                tz_builder.append_null();
                null_buffer.append(false);
            }
        }
    }

    let struct_arr = StructArray::new(
        schema::datetime_struct_fields(),
        vec![
            Arc::new(nanos_builder.finish()) as ArrayRef,
            Arc::new(offset_builder.finish()) as ArrayRef,
            Arc::new(tz_builder.finish()) as ArrayRef,
        ],
        Some(null_buffer.finish().into()),
    );
    Arc::new(struct_arr)
}

/// Build a Time struct array from values.
///
/// Encodes Time as a 2-field struct: (nanos_since_midnight, offset_seconds).
/// This preserves timezone offset information that was previously lost with Time64Nanosecond encoding.
fn values_to_time_struct_array(values: &[Value]) -> ArrayRef {
    let mut nanos_builder = Time64NanosecondBuilder::with_capacity(values.len());
    let mut offset_builder = Int32Builder::with_capacity(values.len());
    let mut null_buffer = BooleanBufferBuilder::new(values.len());

    for v in values {
        match v {
            Value::Temporal(uni_common::TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            }) => {
                nanos_builder.append_value(*nanos_since_midnight);
                offset_builder.append_value(*offset_seconds);
                null_buffer.append(true);
            }
            Value::Temporal(uni_common::TemporalValue::LocalTime {
                nanos_since_midnight,
            }) => {
                nanos_builder.append_value(*nanos_since_midnight);
                offset_builder.append_null();
                null_buffer.append(true);
            }
            _ => {
                nanos_builder.append_null();
                offset_builder.append_null();
                null_buffer.append(false);
            }
        }
    }

    let struct_arr = StructArray::new(
        schema::time_struct_fields(),
        vec![
            Arc::new(nanos_builder.finish()) as ArrayRef,
            Arc::new(offset_builder.finish()) as ArrayRef,
        ],
        Some(null_buffer.finish().into()),
    );
    Arc::new(struct_arr)
}

/// Build a Point struct array from `Value::Map` points.
///
/// The Arrow layout is selected by the column's declared [`PointType`], matching
/// [`schema::DataType::to_arrow`]: Geographic → `{latitude, longitude, crs}`,
/// Cartesian2D → `{x, y, crs}`, Cartesian3D → `{x, y, z, crs}`. All child fields
/// are non-nullable, so a missing/mismatched row marks the struct slot null while
/// writing placeholder `0.0`/`""` into the children (a null struct slot's child
/// values are never observed).
pub(crate) fn values_to_point_struct_array(values: &[Value], point_type: PointType) -> ArrayRef {
    use arrow_array::builder::Float64Builder;

    let n = values.len();
    let fields = match schema::DataType::Point(point_type).to_arrow() {
        ArrowDataType::Struct(f) => f,
        // `DataType::Point` always maps to a struct; anything else is a bug.
        _ => unreachable!("Point maps to an Arrow struct"),
    };

    // Read an f64 coordinate from a point map, or None if absent/non-numeric.
    let coord = |v: &Value, key: &str| -> Option<f64> {
        v.as_object()
            .and_then(|m| m.get(key))
            .and_then(Value::as_f64)
    };
    let crs_of = |v: &Value, default: &str| -> String {
        v.as_object()
            .and_then(|m| m.get("crs"))
            .and_then(|c| c.as_str())
            .unwrap_or(default)
            .to_string()
    };

    let mut null_buffer = BooleanBufferBuilder::new(n);
    let mut crs_b = StringBuilder::with_capacity(n, n * 8);

    // Ordered coordinate builders + the key/default-crs metadata for this layout.
    let (keys, default_crs): (&[&str], &str) = match point_type {
        PointType::Geographic => (&["latitude", "longitude"], "WGS84"),
        PointType::Cartesian2D => (&["x", "y"], "cartesian"),
        PointType::Cartesian3D => (&["x", "y", "z"], "cartesian-3d"),
    };
    let mut coord_builders: Vec<Float64Builder> = keys
        .iter()
        .map(|_| Float64Builder::with_capacity(n))
        .collect();

    for v in values {
        let coords: Option<Vec<f64>> = keys.iter().map(|k| coord(v, k)).collect();
        match coords {
            Some(cs) => {
                for (b, c) in coord_builders.iter_mut().zip(cs) {
                    b.append_value(c);
                }
                crs_b.append_value(crs_of(v, default_crs));
                null_buffer.append(true);
            }
            None => {
                for b in &mut coord_builders {
                    b.append_value(0.0);
                }
                crs_b.append_value(default_crs);
                null_buffer.append(false);
            }
        }
    }

    let mut columns: Vec<ArrayRef> = coord_builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    columns.push(Arc::new(crs_b.finish()) as ArrayRef);

    Arc::new(StructArray::new(
        fields,
        columns,
        Some(null_buffer.finish().into()),
    ))
}

fn values_to_large_binary_array(values: &[Value]) -> ArrayRef {
    let mut builder =
        arrow_array::builder::LargeBinaryBuilder::with_capacity(values.len(), values.len() * 64);
    for v in values {
        if v.is_null() {
            builder.append_null();
        } else {
            // Encode as CypherValue (MessagePack-tagged)
            let cv_bytes = uni_common::cypher_value_codec::encode(v);
            builder.append_value(&cv_bytes);
        }
    }
    Arc::new(builder.finish())
}

/// Convert a slice of JSON Values to an Arrow array based on the target Arrow DataType.
///
/// Note: there is deliberately no `LargeUtf8` arm, unlike the read direction in
/// [`arrow_to_value`]. Checked when the missing `LargeUtf8` *read* arm was
/// fixed: no Uni `DataType` maps to `LargeUtf8` (`DataType::String` →
/// `Utf8`), and no UDF declares it as a return type, so nothing reaches here
/// with it. Should that change, the fallthrough is an `Err` — loud, and the
/// right direction — not a silent null.
pub fn values_to_array(values: &[Value], dt: &ArrowDataType) -> Result<ArrayRef> {
    match dt {
        ArrowDataType::UInt64 => Ok(values_to_uint64_array(values)),
        ArrowDataType::Int64 => Ok(values_to_int64_array(values)),
        ArrowDataType::Int32 => Ok(values_to_int32_array(values)),
        ArrowDataType::Utf8 => Ok(values_to_string_array(values)),
        ArrowDataType::Boolean => Ok(values_to_bool_array(values)),
        ArrowDataType::Float32 => Ok(values_to_float32_array(values)),
        ArrowDataType::Float64 => Ok(values_to_float64_array(values)),
        ArrowDataType::FixedSizeBinary(size) => values_to_fixed_size_binary_array(values, *size),
        ArrowDataType::FixedSizeList(inner, size) => {
            if inner.data_type() == &ArrowDataType::Float32 {
                Ok(values_to_fixed_size_list_f32_array(values, *size))
            } else {
                Err(anyhow!("Unsupported FixedSizeList inner type"))
            }
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, tz) => {
            Ok(values_to_timestamp_array(values, tz.as_ref()))
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => {
            Ok(values_to_timestamp_array(values, tz.as_ref()))
        }
        ArrowDataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(values.len());
            for v in values {
                if v.is_null() {
                    builder.append_null();
                } else if let Value::Temporal(uni_common::TemporalValue::Date {
                    days_since_epoch,
                }) = v
                {
                    builder.append_value(*days_since_epoch);
                } else if let Some(n) = v.as_i64() {
                    builder.append_value(n as i32);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            let mut builder = Time64NanosecondBuilder::with_capacity(values.len());
            for v in values {
                if v.is_null() {
                    builder.append_null();
                } else if let Value::Temporal(tv) = v {
                    match tv {
                        uni_common::TemporalValue::LocalTime {
                            nanos_since_midnight,
                        }
                        | uni_common::TemporalValue::Time {
                            nanos_since_midnight,
                            ..
                        } => builder.append_value(*nanos_since_midnight),
                        _ => builder.append_null(),
                    }
                } else if let Some(n) = v.as_i64() {
                    builder.append_value(n);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            let mut builder = Time64MicrosecondBuilder::with_capacity(values.len());
            for v in values {
                if v.is_null() {
                    builder.append_null();
                } else if let Value::Temporal(tv) = v {
                    match tv {
                        uni_common::TemporalValue::LocalTime {
                            nanos_since_midnight,
                        }
                        | uni_common::TemporalValue::Time {
                            nanos_since_midnight,
                            ..
                        } => builder.append_value(*nanos_since_midnight / 1_000), // nanos→micros for legacy
                        _ => builder.append_null(),
                    }
                } else if let Some(n) = v.as_i64() {
                    builder.append_value(n);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => {
            let mut builder = IntervalMonthDayNanoBuilder::with_capacity(values.len());
            for v in values {
                if v.is_null() {
                    builder.append_null();
                } else if let Value::Temporal(uni_common::TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                }) = v
                {
                    builder.append_value(arrow::datatypes::IntervalMonthDayNano {
                        months: *months as i32,
                        days: *days as i32,
                        nanoseconds: *nanos,
                    });
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Duration(arrow_schema::TimeUnit::Microsecond) => {
            let mut builder = DurationMicrosecondBuilder::with_capacity(values.len());
            for v in values {
                if v.is_null() {
                    builder.append_null();
                } else if let Value::Temporal(uni_common::TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                }) = v
                {
                    let total_micros =
                        months * 30 * 86_400_000_000i64 + days * 86_400_000_000i64 + nanos / 1_000;
                    builder.append_value(total_micros);
                } else if let Some(n) = v.as_i64() {
                    builder.append_value(n);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::LargeBinary => Ok(values_to_large_binary_array(values)),
        ArrowDataType::List(field) => {
            if field.data_type() == &ArrowDataType::Utf8 {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for v in values {
                    if let Value::List(arr) = v {
                        for item in arr {
                            if let Some(s) = item.as_str() {
                                builder.values().append_value(s);
                            } else {
                                builder.values().append_null();
                            }
                        }
                        builder.append(true);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            } else {
                Err(anyhow!(
                    "Unsupported List inner type: {:?}",
                    field.data_type()
                ))
            }
        }
        ArrowDataType::Struct(_) if schema::is_datetime_struct(dt) => {
            Ok(values_to_datetime_struct_array(values))
        }
        ArrowDataType::Struct(_) if schema::is_time_struct(dt) => {
            Ok(values_to_time_struct_array(values))
        }
        _ => Err(anyhow!("Unsupported type for conversion: {:?}", dt)),
    }
}

/// Property value extractor for building Arrow columns from entity properties.
pub struct PropertyExtractor<'a> {
    /// Property name, carried for actionable flush-error messages (issue #137).
    name: &'a str,
    data_type: &'a DataType,
}

/// Extract sparse `(indices, values)` from a property value.
///
/// Accepts the native [`Value::SparseVector`] **and** the degraded
/// `Value::Map { "indices": [..], "values": [..] }` form that a `SparseVector`
/// collapses into when round-tripped through `#[serde(untagged)]` persistence —
/// notably the WAL, which serializes mutations via `serde_json`. This mirrors
/// how the dense `Vector` column tolerates a `Value::List` from the same hazard.
/// Returns `None` for any other value (→ a null struct row).
fn sparse_pair_from_value(v: &Value) -> Option<(Vec<u32>, Vec<f32>)> {
    match v {
        Value::SparseVector { indices, values } => {
            // Guard the native arm the same way the `Value::Map` arm below does: a
            // length-mismatched value would otherwise emit an Arrow struct whose
            // `indices`/`values` child lists desync, which the reader silently
            // truncates via `.zip()` — pairing weights with the wrong term ids
            // (issue #95). Reject it to a null struct row instead.
            if indices.len() != values.len() {
                return None;
            }
            Some((indices.clone(), values.clone()))
        }
        Value::Map(m) => {
            let idx = match m.get("indices") {
                Some(Value::List(l)) => l,
                _ => return None,
            };
            let vals = match m.get("values") {
                Some(Value::List(l)) => l,
                _ => return None,
            };
            let indices: Vec<u32> = idx
                .iter()
                .map(|x| x.as_u64().map(|n| n as u32))
                .collect::<Option<_>>()?;
            let values: Vec<f32> = vals
                .iter()
                .map(|x| x.as_f64().map(|n| n as f32))
                .collect::<Option<_>>()?;
            if indices.len() != values.len() {
                return None;
            }
            Some((indices, values))
        }
        _ => None,
    }
}

/// Build a sparse-vector `Struct { indices: List<UInt32>, values: List<Float32> }`
/// Arrow column from a sequence of property values. Used by the query result
/// projection path (`RETURN d.sparse_col`). A `None` / non-sparse / map-degraded
/// value that can't be extracted becomes a null struct row. Mirrors
/// `PropertyExtractor::build_sparse_vector_column` but operates on owned values.
pub fn build_sparse_vector_array(values: &[Option<Value>]) -> ArrayRef {
    let mut indices_builder = ListBuilder::new(UInt32Builder::new());
    let mut values_builder = ListBuilder::new(Float32Builder::new());
    let mut null_buffer = BooleanBufferBuilder::new(values.len());
    for v in values {
        match v.as_ref().and_then(sparse_pair_from_value) {
            Some((indices, vals)) => {
                for ix in indices {
                    indices_builder.values().append_value(ix);
                }
                indices_builder.append(true);
                for w in vals {
                    values_builder.values().append_value(w);
                }
                values_builder.append(true);
                null_buffer.append(true);
            }
            None => {
                indices_builder.append(true);
                values_builder.append(true);
                null_buffer.append(false);
            }
        }
    }
    let struct_arr = StructArray::new(
        schema::sparse_vector_struct_fields(),
        vec![
            Arc::new(indices_builder.finish()) as ArrayRef,
            Arc::new(values_builder.finish()) as ArrayRef,
        ],
        Some(null_buffer.finish().into()),
    );
    Arc::new(struct_arr)
}

/// Build a multi-vector `List<FixedSizeList<Float32, dimensions>>` Arrow column.
///
/// Used by the query result projection path (`RETURN d.multivector_col`) on the
/// L0 (unflushed) read path. Mirrors the `DataType::Vector { dimensions }` arm of
/// `PropertyExtractor::build_list_column` but operates on owned values: a `None`
/// or non-list value becomes a null row, and each token is validated through
/// `extract_vector_f32_values` (a wrong-dimension or non-numeric token becomes a
/// null inner vector), so failure modes match the write path.
///
/// # Examples
/// ```ignore
/// let col = build_multivector_array(&[Some(Value::List(vec![Value::Vector(vec![1.0, 2.0])]))], 2);
/// ```
pub fn build_multivector_array(values: &[Option<Value>], dimensions: usize) -> ArrayRef {
    let dim = dimensions as i32;
    let mut builder = ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), dim));
    for v in values {
        match v.as_ref().and_then(|v| v.as_array()) {
            Some(arr) => {
                // Variable token count per row: one fixed-size inner vector per
                // token. `extract_vector_f32_values` always yields exactly
                // `dimensions` values, satisfying the FixedSizeList stride.
                for tok in arr {
                    let (vals, valid) = extract_vector_f32_values(Some(tok), false, dimensions);
                    for f in vals {
                        builder.values().values().append_value(f);
                    }
                    builder.values().append(valid);
                }
                builder.append(true);
            }
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Collect one nullable scalar per row for a `PropertyExtractor` column build.
///
/// A row whose property is absent yields `None`, except on a *deleted* row,
/// where `deleted_placeholder` stands in — deleted rows carry a type-specific
/// zero so the column stays non-null for tombstones.
fn collect_scalar<T: Copy>(
    len: usize,
    deleted: &[bool],
    extract: impl Fn(usize) -> Option<T>,
    deleted_placeholder: T,
) -> Vec<Option<T>> {
    let mut values = Vec::with_capacity(len);
    for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
        values.push(extract(i).or(is_deleted.then_some(deleted_placeholder)));
    }
    values
}

impl<'a> PropertyExtractor<'a> {
    pub fn new(name: &'a str, data_type: &'a DataType) -> Self {
        Self { name, data_type }
    }

    /// Build an Arrow column from a slice of property maps.
    /// The `deleted` slice indicates which entries are deleted (use default values).
    pub fn build_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        match self.data_type {
            DataType::String => self.build_string_column(len, deleted, get_props),
            DataType::Int32 => self.build_int32_column(len, deleted, get_props),
            DataType::Int64 => self.build_int64_column(len, deleted, get_props),
            DataType::Float32 => self.build_float32_column(len, deleted, get_props),
            DataType::Float64 => self.build_float64_column(len, deleted, get_props),
            DataType::Bool => self.build_bool_column(len, deleted, get_props),
            DataType::Vector { dimensions } => {
                self.build_vector_column(len, deleted, get_props, *dimensions)
            }
            DataType::SparseVector { .. } => {
                self.build_sparse_vector_column(len, deleted, get_props)
            }
            DataType::BinaryVector { dimensions } => {
                self.build_binary_vector_column(len, deleted, get_props, *dimensions)
            }
            DataType::CypherValue => self.build_json_column(len, deleted, get_props),
            DataType::Bytes => self.build_bytes_column(len, deleted, get_props),
            DataType::List(inner) => self.build_list_column(len, deleted, get_props, inner),
            DataType::Map(key, value) => self.build_map_column(len, deleted, get_props, key, value),
            DataType::Crdt(_) => self.build_crdt_column(len, deleted, get_props),
            DataType::DateTime => self.build_datetime_struct_column(len, deleted, get_props),
            DataType::Timestamp => self.build_timestamp_column(len, deleted, get_props),
            DataType::Date => self.build_date32_column(len, deleted, get_props),
            DataType::Time => self.build_time_struct_column(len, deleted, get_props),
            DataType::Point(pt) => self.build_point_struct_column(len, deleted, get_props, *pt),
            DataType::Duration => self.build_duration_column(len, deleted, get_props),
            DataType::Btic => self.build_btic_column(len, deleted, get_props),
            _ => Err(anyhow!(
                "Unsupported data type for arrow conversion: {:?}",
                self.data_type
            )),
        }
    }

    fn build_string_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = arrow_array::builder::StringBuilder::with_capacity(len, len * 32);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let prop = get_props(i);
            if let Some(s) = prop.and_then(|v| v.as_str()) {
                builder.append_value(s);
            } else if let Some(Value::Temporal(tv)) = prop {
                builder.append_value(tv.to_string());
            } else if is_deleted {
                builder.append_value("");
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_int32_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = collect_scalar(
            len,
            deleted,
            // i64 -> i32 via try_from: an out-of-range value becomes NULL rather
            // than silently wrapping to a different number. (review H13)
            |i| {
                get_props(i)
                    .and_then(|v| v.as_i64())
                    .and_then(|v| i32::try_from(v).ok())
            },
            0,
        );
        Ok(Arc::new(Int32Array::from(values)))
    }

    fn build_int64_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = collect_scalar(len, deleted, |i| get_props(i).and_then(|v| v.as_i64()), 0);
        Ok(Arc::new(Int64Array::from(values)))
    }

    fn build_timestamp_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i);
            // A missing property on a live row must be NULL, not epoch 0 — every
            // sibling builder (int/string/datetime-struct) appends null here.
            // Storing Some(0) rendered absent timestamps as 1970-01-01 and broke
            // `IS NULL`. Only deleted rows get the 0 placeholder (below).
            let ts = if let Some(Value::Temporal(tv)) = val {
                match tv {
                    uni_common::TemporalValue::DateTime {
                        nanos_since_epoch, ..
                    }
                    | uni_common::TemporalValue::LocalDateTime {
                        nanos_since_epoch, ..
                    } => Some(*nanos_since_epoch),
                    _ => None,
                }
            } else if let Some(v) = val.and_then(|v| v.as_i64()) {
                Some(v)
            } else if let Some(s) = val.and_then(|v| v.as_str()) {
                parse_datetime_to_nanos(s)
            } else {
                None
            };

            if is_deleted {
                values.push(Some(0));
            } else {
                values.push(ts);
            }
        }
        let arr = TimestampNanosecondArray::from(values).with_timezone("UTC");
        Ok(Arc::new(arr))
    }

    fn build_datetime_struct_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = self.collect_values_or_null(len, deleted, &get_props);
        Ok(values_to_datetime_struct_array(&values))
    }

    fn build_time_struct_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = self.collect_values_or_null(len, deleted, &get_props);
        Ok(values_to_time_struct_array(&values))
    }

    fn build_point_struct_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        point_type: PointType,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = self.collect_values_or_null(len, deleted, &get_props);
        Ok(values_to_point_struct_array(&values, point_type))
    }

    /// Collect property values into a Vec, substituting `Value::Null` for deleted or missing entries.
    fn collect_values_or_null<F>(&self, len: usize, deleted: &[bool], get_props: &F) -> Vec<Value>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        deleted
            .iter()
            .enumerate()
            .take(len)
            .map(|(i, &is_deleted)| {
                if is_deleted {
                    Value::Null
                } else {
                    get_props(i).cloned().unwrap_or(Value::Null)
                }
            })
            .collect()
    }

    fn build_date32_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = Date32Builder::with_capacity(len);
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i);
            // Missing property on a live row -> NULL (append_null below), not
            // 1970-01-01. Only deleted rows get the 0 placeholder.
            let days = if let Some(Value::Temporal(uni_common::TemporalValue::Date {
                days_since_epoch,
            })) = val
            {
                Some(*days_since_epoch)
            } else if let Some(v) = val.and_then(|v| v.as_i64()) {
                // i64 day count -> i32: an out-of-range value becomes NULL
                // rather than silently wrapping to a different date. (review H13)
                i32::try_from(v).ok()
            } else if let Some(s) = val.and_then(|v| v.as_str()) {
                match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Ok(date) => Some(date.signed_duration_since(epoch).num_days() as i32),
                    Err(_) => None,
                }
            } else {
                None
            };

            if is_deleted {
                builder.append_value(0);
            } else if let Some(v) = days {
                builder.append_value(v);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_duration_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        // Duration stored as LargeBinary via CypherValue codec (Lance doesn't support Interval(MonthDayNano))
        let mut builder = LargeBinaryBuilder::with_capacity(len, len * 32);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let raw_val = get_props(i);
            if let Some(val @ Value::Temporal(uni_common::TemporalValue::Duration { .. })) = raw_val
            {
                let encoded = uni_common::cypher_value_codec::encode(val);
                builder.append_value(&encoded);
            } else if is_deleted {
                let zero = Value::Temporal(uni_common::TemporalValue::Duration {
                    months: 0,
                    days: 0,
                    nanos: 0,
                });
                let encoded = uni_common::cypher_value_codec::encode(&zero);
                builder.append_value(&encoded);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_btic_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        const ENCODED_LEN: i32 = 24;
        let mut builder = FixedSizeBinaryBuilder::with_capacity(len, ENCODED_LEN);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let raw_val = get_props(i);
            let btic = match raw_val {
                Some(Value::Temporal(uni_common::TemporalValue::Btic { lo, hi, meta })) => Some(
                    uni_btic::Btic::new(*lo, *hi, *meta)
                        .map_err(|e| anyhow!("invalid BTIC value: {}", e))?,
                ),
                Some(Value::String(s)) => Some(
                    uni_btic::parse::parse_btic_literal(s)
                        .map_err(|e| anyhow!("BTIC parse error for '{}': {}", s, e))?,
                ),
                _ => None,
            };

            if let Some(b) = btic {
                builder.append_value(uni_btic::encode::encode(&b))?;
            } else if is_deleted {
                builder.append_value([0u8; ENCODED_LEN as usize])?;
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_float32_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = collect_scalar(
            len,
            deleted,
            |i| get_props(i).and_then(|v| v.as_f64()).map(|v| v as f32),
            0.0,
        );
        Ok(Arc::new(Float32Array::from(values)))
    }

    fn build_float64_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = collect_scalar(len, deleted, |i| get_props(i).and_then(|v| v.as_f64()), 0.0);
        Ok(Arc::new(Float64Array::from(values)))
    }

    fn build_bool_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let values = collect_scalar(
            len,
            deleted,
            |i| get_props(i).and_then(|v| v.as_bool()),
            false,
        );
        Ok(Arc::new(BooleanArray::from(values)))
    }

    fn build_vector_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        dimensions: usize,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dimensions as i32);

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i);
            // Fail closed on a wrong-dimension value instead of nulling it: the write
            // paths reject these (issue #137), so reaching one here means an older
            // WAL/version wrote it or a write path bypassed validation.
            let (values, valid) = extract_vector_f32_values_strict(val, is_deleted, dimensions)
                .map_err(|e| {
                    anyhow!(
                        "flush: property '{}' row {}: {}; wrong-dimension vectors are rejected \
                         at write time as of issue #137 — this value was likely written by an \
                         older version",
                        self.name,
                        i,
                        e
                    )
                })?;
            for v in values {
                builder.values().append_value(v);
            }
            builder.append(valid);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Build a binary-vector column as `FixedSizeList<UInt8, dimensions>`.
    ///
    /// The `u8`-lane analogue of [`Self::build_vector_column`]: each row yields
    /// exactly `dimensions` bytes (via [`extract_binary_vector_values_strict`]),
    /// preserving the FixedSizeList stride. Fails closed on a present, wrong-shape
    /// value — the write path validates lane counts (`check_vector_dims`), so
    /// reaching one here means a bypass or an older-version WAL value.
    fn build_binary_vector_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        dimensions: usize,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = FixedSizeListBuilder::new(UInt8Builder::new(), dimensions as i32);

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i);
            let (values, valid) = extract_binary_vector_values_strict(val, is_deleted, dimensions)
                .map_err(|e| {
                    anyhow!(
                        "flush: property '{}' row {}: {}; wrong-shape binary vectors are \
                         rejected at write time — this value was likely written by an older \
                         version",
                        self.name,
                        i,
                        e
                    )
                })?;
            for v in values {
                builder.values().append_value(v);
            }
            builder.append(valid);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Build a sparse-vector column as `Struct { indices: List<UInt32>,
    /// values: List<Float32> }` (see `schema::sparse_vector_struct_fields`).
    /// Deleted, missing, or non-sparse rows become null struct rows carrying
    /// empty child lists — mirroring how `build_vector_column` nulls deleted
    /// rows. An empty sparse vector is stored as two empty (non-null) lists.
    fn build_sparse_vector_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut indices_builder = ListBuilder::new(UInt32Builder::new());
        let mut values_builder = ListBuilder::new(Float32Builder::new());
        let mut null_buffer = BooleanBufferBuilder::new(len);

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let pair = if is_deleted {
                None
            } else {
                get_props(i).and_then(sparse_pair_from_value)
            };
            match pair {
                Some((indices, values)) => {
                    for ix in indices {
                        indices_builder.values().append_value(ix);
                    }
                    indices_builder.append(true);
                    for w in values {
                        values_builder.values().append_value(w);
                    }
                    values_builder.append(true);
                    null_buffer.append(true);
                }
                None => {
                    indices_builder.append(true);
                    values_builder.append(true);
                    null_buffer.append(false);
                }
            }
        }

        let struct_arr = StructArray::new(
            schema::sparse_vector_struct_fields(),
            vec![
                Arc::new(indices_builder.finish()) as ArrayRef,
                Arc::new(values_builder.finish()) as ArrayRef,
            ],
            Some(null_buffer.finish().into()),
        );
        Ok(Arc::new(struct_arr))
    }

    fn build_json_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let null_val = Value::Null;
        let mut builder = arrow_array::builder::LargeBinaryBuilder::with_capacity(len, len * 64);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i);
            let uni_val = if val.is_none() && is_deleted {
                &null_val
            } else {
                val.unwrap_or(&null_val)
            };
            // Encode to CypherValue (MessagePack-tagged)
            let cv_bytes = uni_common::cypher_value_codec::encode(uni_val);
            builder.append_value(&cv_bytes);
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_bytes_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = LargeBinaryBuilder::with_capacity(len, len * 64);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i);
            if let Some(Value::Bytes(b)) = val {
                builder.append_value(b);
            } else if is_deleted {
                builder.append_value(&[][..]);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_list_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        inner: &'a DataType,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        match inner {
            DataType::String => {
                self.build_typed_list(len, deleted, &get_props, StringBuilder::new(), |v, b| {
                    if let Some(s) = v.as_str() {
                        b.append_value(s);
                    } else {
                        b.append_null();
                    }
                })
            }
            DataType::Int64 => {
                self.build_typed_list(len, deleted, &get_props, Int64Builder::new(), |v, b| {
                    if let Some(n) = v.as_i64() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                })
            }
            DataType::Float64 => {
                self.build_typed_list(len, deleted, &get_props, Float64Builder::new(), |v, b| {
                    if let Some(f) = v.as_f64() {
                        b.append_value(f);
                    } else {
                        b.append_null();
                    }
                })
            }
            DataType::Bytes => {
                // Raw `Bytes` elements: store each buffer verbatim in a `LargeBinary`
                // child and mark the child field `uni_raw_bytes` so the read path
                // decodes it verbatim instead of through the tagged codec.
                let item_field = Arc::new(
                    Field::new("item", ArrowDataType::LargeBinary, true)
                        .with_metadata(schema::raw_bytes_field_metadata()),
                );
                let mut builder =
                    ListBuilder::new(LargeBinaryBuilder::new()).with_field(item_field);
                for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
                    let val_array = get_props(i).and_then(|v| v.as_array());
                    if val_array.is_none() && is_deleted {
                        builder.append_null();
                    } else if let Some(arr) = val_array {
                        for v in arr {
                            if let Value::Bytes(b) = v {
                                builder.values().append_value(b);
                            } else {
                                builder.values().append_null();
                            }
                        }
                        builder.append(true);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Vector { dimensions } => {
                // Multi-vector / late-interaction (ColBERT) property: a per-row
                // variable-count set of fixed-`dimensions` token vectors, stored as
                // `List<FixedSizeList<Float32, dimensions>>`. Each inner token is
                // validated through the same `extract_vector_f32_values` path used for
                // single dense vectors, so dimension/type failure modes match.
                //
                // NOTE: only the declared-schema write path (this builder) supports
                // multi-vector today. The schemaless, Arrow-type-driven `values_to_array`
                // path does not yet handle `List<FixedSizeList<Float32>>`; schemaless
                // multi-vector writes are a deferred follow-up (issue #96, Phase 1.5).
                let dim = *dimensions as i32;
                let mut builder =
                    ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), dim));
                for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
                    let raw = get_props(i);
                    // Fail closed on a wrong-dimension / wrong-shape token set instead
                    // of nulling it: the write paths reject these (issue #137), so
                    // reaching one here means an older WAL/version wrote it or a write
                    // path bypassed validation. `check_vector_dims` reports the
                    // offending token index; nulls stay legal null rows.
                    if !is_deleted && let Some(v) = raw {
                        self.data_type.check_vector_dims(v).map_err(|e| {
                            anyhow!(
                                "flush: property '{}' row {}: {}; wrong-dimension vectors \
                                 are rejected at write time as of issue #137 — this value \
                                 was likely written by an older version",
                                self.name,
                                i,
                                e
                            )
                        })?;
                    }
                    let val_array = raw.and_then(|v| v.as_array());
                    if val_array.is_none() && is_deleted {
                        builder.append_null();
                    } else if let Some(arr) = val_array {
                        // Variable token count per row: append one fixed-size inner
                        // vector per token. `extract_vector_f32_values` always yields
                        // exactly `dimensions` values, satisfying the FixedSizeList stride
                        // (tokens are pre-validated above, so nothing is silently nulled).
                        for tok in arr {
                            let (vals, valid) =
                                extract_vector_f32_values(Some(tok), false, *dimensions);
                            for v in vals {
                                builder.values().values().append_value(v);
                            }
                            builder.values().append(valid);
                        }
                        builder.append(true);
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            // Every other element type. The schema accepts a list of any type,
            // and a `List<DateTime>` (or Date, Duration, Bool, Int32, ...) was
            // written and read in memory but failed the flush, which left the
            // data unflushable. The elements are built as one child column by the
            // scalar builder for `inner`, so each type is stored exactly as a
            // top-level column of that type is.
            _ => {
                let mut offsets: Vec<i32> = Vec::with_capacity(len + 1);
                let mut valid: Vec<bool> = Vec::with_capacity(len);
                let mut elements: Vec<&'a Value> = Vec::new();
                offsets.push(0);
                for i in 0..len {
                    match get_props(i).and_then(|v| v.as_array()) {
                        Some(arr) => {
                            elements.extend(arr.iter());
                            valid.push(true);
                        }
                        None => valid.push(false),
                    }
                    offsets.push(
                        i32::try_from(elements.len())
                            .map_err(|_| anyhow!("list column '{}' is too large", self.name))?,
                    );
                }
                // A `dyn` accessor, so a nested list instantiates `build_column`
                // once rather than once per nesting level, without bound.
                let element: &dyn Fn(usize) -> Option<&'a Value> = &|j| Some(elements[j]);
                let child = PropertyExtractor::new(self.name, inner).build_column(
                    elements.len(),
                    &vec![false; elements.len()],
                    element,
                )?;
                let item = Arc::new(Field::new("item", inner.to_arrow(), true));
                Ok(Arc::new(
                    arrow_array::ListArray::try_new(
                        item,
                        arrow::buffer::OffsetBuffer::new(offsets.into()),
                        child,
                        Some(arrow::buffer::NullBuffer::from(valid)),
                    )
                    .map_err(|e| anyhow!("list column '{}': {}", self.name, e))?,
                ))
            }
        }
    }

    /// Generic helper to build a list column with any inner builder type.
    fn build_typed_list<F, B, A>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: &F,
        inner_builder: B,
        mut append_value: A,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let mut builder = ListBuilder::new(inner_builder);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val_array = get_props(i).and_then(|v| v.as_array());
            if val_array.is_none() && is_deleted {
                builder.append_null();
            } else if let Some(arr) = val_array {
                for v in arr {
                    append_value(v, builder.values());
                }
                builder.append(true);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_map_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        key: &DataType,
        value: &DataType,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        if !matches!(key, DataType::String) {
            return Err(anyhow!("Map keys must be String (JSON limitation)"));
        }

        match value {
            DataType::String => self.build_typed_map(
                len,
                deleted,
                &get_props,
                StringBuilder::new(),
                arrow_schema::DataType::Utf8,
                None,
                |v, b: &mut StringBuilder| {
                    if let Some(s) = v.as_str() {
                        b.append_value(s);
                    } else {
                        b.append_null();
                    }
                },
            ),
            DataType::Int64 => self.build_typed_map(
                len,
                deleted,
                &get_props,
                Int64Builder::new(),
                arrow_schema::DataType::Int64,
                None,
                |v, b: &mut Int64Builder| {
                    if let Some(n) = v.as_i64() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                },
            ),
            DataType::Int32 => self.build_typed_map(
                len,
                deleted,
                &get_props,
                Int32Builder::new(),
                arrow_schema::DataType::Int32,
                None,
                |v, b: &mut Int32Builder| match v.as_i64().and_then(|n| i32::try_from(n).ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                },
            ),
            DataType::Float64 => self.build_typed_map(
                len,
                deleted,
                &get_props,
                Float64Builder::new(),
                arrow_schema::DataType::Float64,
                None,
                |v, b: &mut Float64Builder| match v.as_f64() {
                    Some(f) => b.append_value(f),
                    None => b.append_null(),
                },
            ),
            DataType::Float32 => self.build_typed_map(
                len,
                deleted,
                &get_props,
                Float32Builder::new(),
                arrow_schema::DataType::Float32,
                None,
                |v, b: &mut Float32Builder| match v.as_f64() {
                    Some(f) => b.append_value(f as f32),
                    None => b.append_null(),
                },
            ),
            DataType::Bool => self.build_typed_map(
                len,
                deleted,
                &get_props,
                BooleanBuilder::new(),
                arrow_schema::DataType::Boolean,
                None,
                |v, b: &mut BooleanBuilder| match v.as_bool() {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                },
            ),
            DataType::Bytes => self.build_typed_map(
                len,
                deleted,
                &get_props,
                LargeBinaryBuilder::new(),
                arrow_schema::DataType::LargeBinary,
                // Mark the value child `uni_raw_bytes` so the read path decodes each
                // raw `Bytes` value verbatim rather than through the tagged codec.
                Some(schema::raw_bytes_field_metadata()),
                |v, b: &mut LargeBinaryBuilder| {
                    if let Value::Bytes(bytes) = v {
                        b.append_value(bytes);
                    } else {
                        b.append_null();
                    }
                },
            ),
            // Nested / non-scalar value types (Vector, List, Map, temporal, …): no typed
            // Arrow builder — CypherValue-encode each value into an UNMARKED LargeBinary
            // child (no `uni_raw_bytes`), so the read path (`try_reconstruct_map` →
            // `arrow_to_value` LargeBinary arm) decodes it through the tagged codec. This
            // mirrors how schemaless/overflow maps are stored and handles arbitrary nesting.
            _ => self.build_typed_map(
                len,
                deleted,
                &get_props,
                LargeBinaryBuilder::new(),
                arrow_schema::DataType::LargeBinary,
                None,
                |v, b: &mut LargeBinaryBuilder| {
                    if v.is_null() {
                        b.append_null();
                    } else {
                        b.append_value(uni_common::cypher_value_codec::encode(v));
                    }
                },
            ),
        }
    }

    /// Generic helper to build a map column with any value builder type.
    #[expect(
        clippy::too_many_arguments,
        reason = "builder plumbing: value type + optional child metadata are distinct knobs"
    )]
    fn build_typed_map<F, B, A>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: &F,
        value_builder: B,
        value_arrow_type: arrow_schema::DataType,
        value_metadata: Option<HashMap<String, String>>,
        mut append_value: A,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let key_builder = Box::new(StringBuilder::new());
        let value_builder = Box::new(value_builder);
        let value_field = match value_metadata {
            Some(meta) => Field::new("value", value_arrow_type, true).with_metadata(meta),
            None => Field::new("value", value_arrow_type, true),
        };
        let struct_builder = StructBuilder::new(
            vec![
                Field::new("key", arrow_schema::DataType::Utf8, false),
                value_field,
            ],
            vec![key_builder, value_builder],
        );
        let mut builder = ListBuilder::new(struct_builder);

        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            self.append_map_entry(&mut builder, get_props(i), is_deleted, &mut append_value);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Append a single map entry to the list builder.
    fn append_map_entry<B, A>(
        &self,
        builder: &mut ListBuilder<StructBuilder>,
        val: Option<&'a Value>,
        is_deleted: bool,
        append_value: &mut A,
    ) where
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let val_obj = val.and_then(|v| v.as_object());
        if val_obj.is_none() && is_deleted {
            builder.append(false);
        } else if let Some(obj) = val_obj {
            let struct_b = builder.values();
            for (k, v) in obj {
                struct_b
                    .field_builder::<StringBuilder>(0)
                    .unwrap()
                    .append_value(k);
                // Safety: We know the value builder type matches B
                let value_b = struct_b.field_builder::<B>(1).unwrap();
                append_value(v, value_b);
                struct_b.append(true);
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }

    fn build_crdt_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = BinaryBuilder::new();
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            if is_deleted {
                builder.append_null();
                continue;
            }
            if let Some(val) = get_props(i) {
                // Try to parse CRDT from the value
                // If it's a string, first parse it as JSON, then as CRDT
                let crdt_result = if let Some(s) = val.as_str() {
                    serde_json::from_str::<Crdt>(s)
                } else {
                    // Convert uni_common::Value to serde_json::Value at the CRDT boundary
                    let json_val: serde_json::Value = val.clone().into();
                    serde_json::from_value::<Crdt>(json_val)
                };

                if let Ok(crdt) = crdt_result {
                    if let Ok(bytes) = crdt.to_msgpack() {
                        builder.append_value(&bytes);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// Build a column for edge entries (no deleted flag handling needed).
pub fn build_edge_column<'a>(
    name: &'a str,
    data_type: &'a DataType,
    len: usize,
    get_props: impl Fn(usize) -> Option<&'a Value>,
) -> Result<ArrayRef> {
    // For edges, use empty deleted array
    let deleted = vec![false; len];
    let extractor = PropertyExtractor::new(name, data_type);
    extractor.build_column(len, &deleted, get_props)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, DurationMicrosecondArray,
        builder::{BinaryBuilder, Time64MicrosecondBuilder, TimestampNanosecondBuilder},
    };
    use std::collections::HashMap;
    use uni_common::TemporalValue;
    use uni_crdt::{Crdt, GCounter};

    #[test]
    fn test_sparse_vector_columnar_roundtrip_and_no_silent_null() {
        use crate::storage::value_codec::{CrdtDecodeMode, decode_column_value, value_from_column};

        let dt = DataType::SparseVector { dimensions: 100 };
        let v0 = Value::SparseVector {
            indices: vec![1, 5, 9],
            values: vec![0.5, -1.0, 2.0],
        };
        // An empty sparse vector must round-trip as empty lists, not null.
        let v1 = Value::SparseVector {
            indices: vec![],
            values: vec![],
        };
        // row 2 is a tombstone → decodes back to Null.
        let props = [Some(v0.clone()), Some(v1.clone()), None];
        let deleted = [false, false, true];

        let extractor = PropertyExtractor::new("emb", &dt);
        let col = extractor
            .build_column(3, &deleted, |i| props[i].as_ref())
            .unwrap();

        // Full-fidelity decode → uni_common::Value::SparseVector.
        assert_eq!(
            decode_column_value(&col, &dt, 0, CrdtDecodeMode::Strict).unwrap(),
            v0
        );
        assert_eq!(
            decode_column_value(&col, &dt, 1, CrdtDecodeMode::Strict).unwrap(),
            v1
        );
        assert_eq!(
            decode_column_value(&col, &dt, 2, CrdtDecodeMode::Strict).unwrap(),
            Value::Null
        );

        // Regression for the `_ => Ok(Value::Null)` fallback: the serde_json
        // read path must surface the data, not silently null it.
        let json0 = value_from_column(&col, &dt, 0, CrdtDecodeMode::Strict).unwrap();
        assert!(
            json0.is_object(),
            "sparse column was silently nulled by value_from_column: {json0:?}"
        );
        assert_eq!(json0["indices"], serde_json::json!([1u32, 5u32, 9u32]));
        assert_eq!(
            json0["values"],
            serde_json::json!([0.5f32, -1.0f32, 2.0f32])
        );
    }

    #[test]
    fn test_arrow_to_value_string() {
        let arr = StringArray::from(vec![Some("hello"), None, Some("world")]);
        assert_eq!(
            arrow_to_value(&arr, 0, None).unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap(), Value::Null);
        assert_eq!(
            arrow_to_value(&arr, 2, None).unwrap(),
            Value::String("world".to_string())
        );
    }

    /// `LargeUtf8` is a distinct Arrow type from `Utf8` and needs its own
    /// downcast — this decoder had a `StringArray` arm but no
    /// `LargeStringArray` one, so every `LargeUtf8` column fell through to the
    /// trailing `Value::Null` fallback.
    ///
    /// Locy hit this on every string literal and every string-returning
    /// function in a `YIELD` (all typed `LargeUtf8` by `infer_expr_type`),
    /// reporting NULL from `derived` while `QUERY` — which never touches Arrow
    /// — reported the real value.
    #[test]
    fn test_arrow_to_value_large_string() {
        let arr = LargeStringArray::from(vec![Some("hello"), None, Some("world")]);
        assert_eq!(
            arrow_to_value(&arr, 0, None).unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap(), Value::Null);
        assert_eq!(
            arrow_to_value(&arr, 2, None).unwrap(),
            Value::String("world".to_string())
        );
    }

    #[test]
    fn test_arrow_to_value_int64() {
        let arr = Int64Array::from(vec![Some(42), None, Some(-10)]);
        assert_eq!(arrow_to_value(&arr, 0, None).unwrap(), Value::Int(42));
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap(), Value::Null);
        assert_eq!(arrow_to_value(&arr, 2, None).unwrap(), Value::Int(-10));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_arrow_to_value_float64() {
        let arr = Float64Array::from(vec![Some(3.14), None]);
        assert_eq!(arrow_to_value(&arr, 0, None).unwrap(), Value::Float(3.14));
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap(), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_bool() {
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(arrow_to_value(&arr, 0, None).unwrap(), Value::Bool(true));
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap(), Value::Bool(false));
        assert_eq!(arrow_to_value(&arr, 2, None).unwrap(), Value::Null);
    }

    #[test]
    fn test_values_to_array_int64() {
        let values = vec![Value::Int(1), Value::Int(2), Value::Null, Value::Int(4)];
        let arr = values_to_array(&values, &ArrowDataType::Int64).unwrap();
        assert_eq!(arr.len(), 4);

        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 1);
        assert_eq!(int_arr.value(1), 2);
        assert!(int_arr.is_null(2));
        assert_eq!(int_arr.value(3), 4);
    }

    #[test]
    fn test_values_to_array_string() {
        let values = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
            Value::Null,
        ];
        let arr = values_to_array(&values, &ArrowDataType::Utf8).unwrap();
        assert_eq!(arr.len(), 3);

        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "a");
        assert_eq!(str_arr.value(1), "b");
        assert!(str_arr.is_null(2));
    }

    #[test]
    fn test_property_extractor_string() {
        let props: Vec<HashMap<String, Value>> = vec![
            [("name".to_string(), Value::String("Alice".to_string()))]
                .into_iter()
                .collect(),
            [("name".to_string(), Value::String("Bob".to_string()))]
                .into_iter()
                .collect(),
            HashMap::new(),
        ];
        let deleted = vec![false, false, true];

        let extractor = PropertyExtractor::new("name", &DataType::String);
        let arr = extractor
            .build_column(3, &deleted, |i| props[i].get("name"))
            .unwrap();

        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "Alice");
        assert_eq!(str_arr.value(1), "Bob");
        assert_eq!(str_arr.value(2), ""); // Deleted entries get default
    }

    #[test]
    fn test_property_extractor_int64() {
        let props: Vec<HashMap<String, Value>> = vec![
            [("age".to_string(), Value::Int(25))].into_iter().collect(),
            [("age".to_string(), Value::Int(30))].into_iter().collect(),
            HashMap::new(),
        ];
        let deleted = vec![false, false, true];

        let extractor = PropertyExtractor::new("age", &DataType::Int64);
        let arr = extractor
            .build_column(3, &deleted, |i| props[i].get("age"))
            .unwrap();

        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 25);
        assert_eq!(int_arr.value(1), 30);
        assert_eq!(int_arr.value(2), 0); // Deleted entries get default
    }

    #[test]
    fn test_property_extractor_bytes_roundtrip() {
        let blob = vec![0u8, 1, 2, 255];
        let props: Vec<HashMap<String, Value>> = vec![
            [("blob".to_string(), Value::Bytes(blob.clone()))]
                .into_iter()
                .collect(),
            [("blob".to_string(), Value::Bytes(Vec::new()))]
                .into_iter()
                .collect(),
            HashMap::new(),
        ];
        let deleted = vec![false, false, false];

        let extractor = PropertyExtractor::new("blob", &DataType::Bytes);
        let arr = extractor
            .build_column(3, &deleted, |i| props[i].get("blob"))
            .unwrap();

        // Decode each row through arrow_to_value with Bytes hint.
        assert_eq!(
            arrow_to_value(arr.as_ref(), 0, Some(&DataType::Bytes)).unwrap(),
            Value::Bytes(blob)
        );
        assert_eq!(
            arrow_to_value(arr.as_ref(), 1, Some(&DataType::Bytes)).unwrap(),
            Value::Bytes(Vec::new())
        );
        // Missing property → null in the Arrow array.
        assert_eq!(
            arrow_to_value(arr.as_ref(), 2, Some(&DataType::Bytes)).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_bytes_vs_cypher_value_disambiguation() {
        // A LargeBinary column tagged as DataType::Bytes must NOT be decoded
        // through the CypherValue MessagePack codec, even though both share
        // the same Arrow physical type.
        let raw = vec![0xDEu8, 0xAD, 0xBE, 0xEF];
        let props: Vec<HashMap<String, Value>> = vec![
            [("blob".to_string(), Value::Bytes(raw.clone()))]
                .into_iter()
                .collect(),
        ];
        let extractor = PropertyExtractor::new("blob", &DataType::Bytes);
        let arr = extractor
            .build_column(1, &[false], |i| props[i].get("blob"))
            .unwrap();
        // With schema hint: raw bytes returned.
        assert_eq!(
            arrow_to_value(arr.as_ref(), 0, Some(&DataType::Bytes)).unwrap(),
            Value::Bytes(raw)
        );
    }

    #[test]
    fn test_data_type_bytes_to_arrow() {
        assert_eq!(DataType::Bytes.to_arrow(), ArrowDataType::LargeBinary);
    }

    #[test]
    fn test_arrow_to_value_time64() {
        // Test Time64MicrosecondArray legacy fallback (micros→nanos conversion)
        let mut builder = Time64MicrosecondBuilder::new();
        // 10:30:45 = 10*3600 + 30*60 + 45 = 37845 seconds = 37845000000 microseconds
        builder.append_value(37_845_000_000);
        // 00:00:00 = 0 microseconds
        builder.append_value(0);
        // 23:59:59.123456 = 86399.123456 seconds
        builder.append_value(86_399_123_456);
        builder.append_null();

        let arr = builder.finish();
        // Arrow→Value returns Value::Temporal(LocalTime) with nanos (micros * 1000)
        assert_eq!(
            arrow_to_value(&arr, 0, None).unwrap().to_string(),
            "10:30:45"
        );
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap().to_string(), "00:00");
        assert_eq!(
            arrow_to_value(&arr, 2, None).unwrap().to_string(),
            "23:59:59.123456"
        );
        assert_eq!(arrow_to_value(&arr, 3, None).unwrap(), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_duration() {
        // Test DurationMicrosecondArray conversion
        // Arrow→Value now returns Value::Temporal(Duration)
        let arr = DurationMicrosecondArray::from(vec![
            Some(1_000_000),      // 1 second in microseconds
            Some(3_600_000_000),  // 1 hour
            Some(86_400_000_000), // 1 day
            None,
        ]);

        assert_eq!(arrow_to_value(&arr, 0, None).unwrap().to_string(), "PT1S");
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap().to_string(), "PT1H");
        assert_eq!(arrow_to_value(&arr, 2, None).unwrap().to_string(), "PT24H");
        assert_eq!(arrow_to_value(&arr, 3, None).unwrap(), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_binary_crdt() {
        // Test BinaryArray (CRDT) conversion - round-trip test
        let mut builder = BinaryBuilder::new();

        // Create a GCounter CRDT and serialize it
        let mut counter = GCounter::new();
        counter.increment("actor1", 5);
        let crdt = Crdt::GCounter(counter);
        let bytes = crdt.to_msgpack().unwrap();
        builder.append_value(&bytes);

        // Add a null value
        builder.append_null();

        let arr = builder.finish();

        // The first value should deserialize back to a map
        let result = arrow_to_value(&arr, 0, None).unwrap();
        assert!(result.as_object().is_some());
        let obj = result.as_object().unwrap();
        // GCounter serializes with tag "t": "gc"
        assert_eq!(obj.get("t"), Some(&Value::String("gc".to_string())));

        // Null value should return null
        assert_eq!(arrow_to_value(&arr, 1, None).unwrap(), Value::Null);
    }

    #[test]
    fn test_datetime_struct_encode_decode_roundtrip() {
        // Test DateTime struct encoding with offset and timezone preservation
        let values = vec![
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 441763200000000000, // 1984-01-01T00:00:00Z
                offset_seconds: 3600,                  // +01:00
                timezone_name: Some("Europe/Paris".to_string()),
            }),
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 1704067200000000000, // 2024-01-01T00:00:00Z
                offset_seconds: -18000,                 // -05:00
                timezone_name: None,
            }),
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 0, // Unix epoch
                offset_seconds: 0,
                timezone_name: Some("UTC".to_string()),
            }),
        ];

        // Encode to Arrow struct
        let arr_ref = values_to_datetime_struct_array(&values);
        let arr = arr_ref.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(arr.len(), 3);

        // Decode back to Value
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::DateTime)).unwrap();
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::DateTime)).unwrap();
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::DateTime)).unwrap();

        // Verify round-trip preserves all fields
        assert_eq!(decoded_0, values[0]);
        assert_eq!(decoded_1, values[1]);
        assert_eq!(decoded_2, values[2]);

        // Verify struct field extraction
        if let Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch,
            offset_seconds,
            timezone_name,
        }) = decoded_0
        {
            assert_eq!(nanos_since_epoch, 441763200000000000);
            assert_eq!(offset_seconds, 3600);
            assert_eq!(timezone_name, Some("Europe/Paris".to_string()));
        } else {
            panic!("Expected DateTime value");
        }
    }

    #[test]
    fn test_datetime_struct_null_handling() {
        // Test DateTime struct with null values
        let values = vec![
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 441763200000000000,
                offset_seconds: 3600,
                timezone_name: Some("Europe/Paris".to_string()),
            }),
            Value::Null,
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 0,
                offset_seconds: 0,
                timezone_name: None,
            }),
        ];

        let arr_ref = values_to_datetime_struct_array(&values);
        let arr = arr_ref.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(arr.len(), 3);

        // Check first value is valid
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::DateTime)).unwrap();
        assert_eq!(decoded_0, values[0]);

        // Check second value is null
        assert!(arr.is_null(1));
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::DateTime)).unwrap();
        assert_eq!(decoded_1, Value::Null);

        // Check third value is valid
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::DateTime)).unwrap();
        assert_eq!(decoded_2, values[2]);
    }

    #[test]
    fn test_datetime_struct_boundary_values() {
        // Test boundary values: offset=0, large positive/negative offsets
        let values = vec![
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 441763200000000000,
                offset_seconds: 0, // UTC
                timezone_name: None,
            }),
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 441763200000000000,
                offset_seconds: 43200, // +12:00 (max typical offset)
                timezone_name: None,
            }),
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: 441763200000000000,
                offset_seconds: -43200, // -12:00 (min typical offset)
                timezone_name: None,
            }),
        ];

        let arr_ref = values_to_datetime_struct_array(&values);
        let arr = arr_ref.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(arr.len(), 3);

        // Verify round-trip for all boundary values
        for (i, expected) in values.iter().enumerate() {
            let decoded = arrow_to_value(arr_ref.as_ref(), i, Some(&DataType::DateTime)).unwrap();
            assert_eq!(&decoded, expected);
        }
    }

    #[test]
    fn test_datetime_old_schema_migration() {
        // Test backward compatibility: TimestampNanosecondArray → DateTime with offset=0
        let mut builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
        builder.append_value(441763200000000000); // 1984-01-01T00:00:00Z
        builder.append_value(1704067200000000000); // 2024-01-01T00:00:00Z
        builder.append_null();

        let arr = builder.finish();

        // Decode with DataType::DateTime hint should migrate old schema
        let decoded_0 = arrow_to_value(&arr, 0, Some(&DataType::DateTime)).unwrap();
        let _decoded_1 = arrow_to_value(&arr, 1, Some(&DataType::DateTime)).unwrap();
        let decoded_2 = arrow_to_value(&arr, 2, Some(&DataType::DateTime)).unwrap();

        // Old schema should default to offset=0, preserve timezone
        if let Value::Temporal(TemporalValue::DateTime {
            nanos_since_epoch,
            offset_seconds,
            timezone_name,
        }) = decoded_0
        {
            assert_eq!(nanos_since_epoch, 441763200000000000);
            assert_eq!(offset_seconds, 0);
            assert_eq!(timezone_name, Some("UTC".to_string()));
        } else {
            panic!("Expected DateTime value");
        }

        // Verify null handling
        assert_eq!(decoded_2, Value::Null);
    }

    #[test]
    fn test_time_struct_encode_decode_roundtrip() {
        // Test Time struct encoding with offset preservation
        let values = vec![
            Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: 37845000000000, // 10:30:45
                offset_seconds: 3600,                 // +01:00
            }),
            Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: 0, // 00:00:00
                offset_seconds: 0,
            }),
            Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: 86399999999999, // 23:59:59.999999999
                offset_seconds: -18000,               // -05:00
            }),
        ];

        // Encode to Arrow struct
        let arr_ref = values_to_time_struct_array(&values);
        let arr = arr_ref.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(arr.len(), 3);

        // Decode back to Value
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::Time)).unwrap();
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::Time)).unwrap();
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::Time)).unwrap();

        // Verify round-trip preserves all fields
        assert_eq!(decoded_0, values[0]);
        assert_eq!(decoded_1, values[1]);
        assert_eq!(decoded_2, values[2]);

        // Verify struct field extraction
        if let Value::Temporal(TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        }) = decoded_0
        {
            assert_eq!(nanos_since_midnight, 37845000000000);
            assert_eq!(offset_seconds, 3600);
        } else {
            panic!("Expected Time value");
        }
    }

    #[test]
    fn test_time_struct_null_handling() {
        // Test Time struct with null values
        let values = vec![
            Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: 37845000000000,
                offset_seconds: 3600,
            }),
            Value::Null,
            Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: 0,
                offset_seconds: 0,
            }),
        ];

        let arr_ref = values_to_time_struct_array(&values);
        let arr = arr_ref.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(arr.len(), 3);

        // Check first value is valid
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::Time)).unwrap();
        assert_eq!(decoded_0, values[0]);

        // Check second value is null
        assert!(arr.is_null(1));
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::Time)).unwrap();
        assert_eq!(decoded_1, Value::Null);

        // Check third value is valid
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::Time)).unwrap();
        assert_eq!(decoded_2, values[2]);
    }

    // Tests for extract_vector_f32_values

    #[test]
    fn test_extract_vector_f32_values_valid_vector() {
        let v = vec![1.0, 2.0, 3.0];
        let val = Value::Vector(v.clone());
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, v);
        assert!(valid);
    }

    #[test]
    fn test_extract_vector_f32_values_vector_wrong_dims() {
        // Pins the LENIENT read-path helper: wrong dims null out. The declared-schema
        // flush path uses `extract_vector_f32_values_strict`, which errors instead
        // (issue #137).
        let v = vec![1.0, 2.0];
        let val = Value::Vector(v);
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(!valid);
    }

    #[test]
    fn test_extract_vector_f32_values_valid_list() {
        let v = vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)];
        let val = Value::List(v);
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
        assert!(valid);
    }

    #[test]
    fn test_extract_vector_f32_values_list_wrong_dims() {
        // Pins the LENIENT read-path helper (see the strict variant for the flush path).
        let v = vec![Value::Float(1.0), Value::Float(2.0)];
        let val = Value::List(v);
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(!valid);
    }

    #[test]
    fn test_extract_vector_f32_values_list_int_coercion() {
        let v = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let val = Value::List(v);
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, vec![1.0, 2.0, 3.0]);
        assert!(valid);
    }

    #[test]
    fn test_extract_vector_f32_values_none() {
        let (result, valid) = extract_vector_f32_values(None, false, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(!valid);
    }

    #[test]
    fn test_extract_vector_f32_values_null() {
        let val = Value::Null;
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(!valid);
    }

    #[test]
    fn test_extract_vector_f32_values_unsupported_type() {
        let val = Value::String("not a vector".to_string());
        let (result, valid) = extract_vector_f32_values(Some(&val), false, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(!valid);
    }

    #[test]
    fn test_extract_vector_f32_values_deleted_with_none() {
        let (result, valid) = extract_vector_f32_values(None, true, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(valid); // Deleted entries are marked as valid with zeros
    }

    #[test]
    fn test_extract_vector_f32_values_deleted_with_null() {
        let val = Value::Null;
        let (result, valid) = extract_vector_f32_values(Some(&val), true, 3);
        assert_eq!(result, vec![0.0, 0.0, 0.0]);
        assert!(valid); // Deleted entries are marked as valid with zeros
    }

    // Tests for values_to_array with FixedSizeList

    #[test]
    fn test_values_to_fixed_size_list_vector_with_nulls() {
        let values = vec![
            Value::Vector(vec![1.0, 2.0]),
            Value::Null,
            Value::Vector(vec![3.0, 4.0]),
            Value::String("invalid".to_string()),
        ];
        let arr_ref = values_to_array(
            &values,
            &ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                2,
            ),
        )
        .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 4);
        assert!(arr.is_valid(0));
        assert!(!arr.is_valid(1)); // Null value
        assert!(arr.is_valid(2));
        assert!(!arr.is_valid(3)); // Invalid type
    }

    #[test]
    fn test_values_to_fixed_size_list_from_list() {
        let values = vec![
            Value::List(vec![Value::Float(1.0), Value::Float(2.0)]),
            Value::List(vec![Value::Int(3), Value::Int(4)]),
        ];
        let arr_ref = values_to_array(
            &values,
            &ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                2,
            ),
        )
        .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 2);
        assert!(arr.is_valid(0));
        assert!(arr.is_valid(1));

        // Check values
        let child = arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.value(0), 1.0);
        assert_eq!(child.value(1), 2.0);
        assert_eq!(child.value(2), 3.0);
        assert_eq!(child.value(3), 4.0);
    }

    #[test]
    fn test_values_to_fixed_size_list_wrong_dimensions() {
        let values = vec![
            Value::Vector(vec![1.0, 2.0, 3.0]),   // 3 dims, expecting 2
            Value::List(vec![Value::Float(4.0)]), // 1 dim, expecting 2
        ];
        let arr_ref = values_to_array(
            &values,
            &ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                2,
            ),
        )
        .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 2);
        assert!(!arr.is_valid(0)); // Wrong dimensions
        assert!(!arr.is_valid(1)); // Wrong dimensions

        // Check that child array has zeros for invalid entries
        let child = arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.value(0), 0.0);
        assert_eq!(child.value(1), 0.0);
        assert_eq!(child.value(2), 0.0);
        assert_eq!(child.value(3), 0.0);
    }

    #[test]
    fn test_values_to_fixed_size_list_all_nulls() {
        let values = vec![Value::Null, Value::Null, Value::Null];
        let arr_ref = values_to_array(
            &values,
            &ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                3,
            ),
        )
        .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 3);
        assert!(!arr.is_valid(0));
        assert!(!arr.is_valid(1));
        assert!(!arr.is_valid(2));

        // Verify child array length is correct (3 rows × 3 dims = 9)
        let child = arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.len(), 9);
    }

    #[test]
    fn test_values_to_fixed_size_list_mixed_types() {
        let values = vec![
            Value::Vector(vec![1.0, 2.0]),
            Value::List(vec![Value::Float(3.0), Value::Float(4.0)]),
            Value::Null,
            Value::String("invalid".to_string()),
        ];
        let arr_ref = values_to_array(
            &values,
            &ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, false)),
                2,
            ),
        )
        .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 4);
        assert!(arr.is_valid(0)); // Value::Vector
        assert!(arr.is_valid(1)); // Value::List
        assert!(!arr.is_valid(2)); // Value::Null
        assert!(!arr.is_valid(3)); // Value::String

        // Check values for valid entries
        let child = arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.value(0), 1.0);
        assert_eq!(child.value(1), 2.0);
        assert_eq!(child.value(2), 3.0);
        assert_eq!(child.value(3), 4.0);
    }

    // Tests for PropertyExtractor::build_vector_column

    #[test]
    fn test_build_vector_column_with_nulls_and_deleted() {
        let data_type = DataType::Vector { dimensions: 3 };
        let extractor = PropertyExtractor::new("test_vec", &data_type);

        let props = [
            Some(Value::Vector(vec![1.0, 2.0, 3.0])),
            None,              // Missing property
            Some(Value::Null), // Null value
            Some(Value::Vector(vec![4.0, 5.0, 6.0])),
        ];
        let deleted = [false, false, false, true]; // Last one is deleted

        let arr_ref = extractor
            .build_vector_column(4, &deleted, |i| props[i].as_ref(), 3)
            .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 4);
        assert!(arr.is_valid(0)); // Valid vector
        assert!(!arr.is_valid(1)); // Missing property
        assert!(!arr.is_valid(2)); // Null value
        assert!(arr.is_valid(3)); // Deleted entry (valid with zeros)

        // Check values
        let child = arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.value(0), 1.0);
        assert_eq!(child.value(1), 2.0);
        assert_eq!(child.value(2), 3.0);
        // Indices 3-5: zeros for missing
        // Indices 6-8: zeros for null
        // Indices 9-11: zeros for deleted (but marked as valid)
        assert_eq!(child.value(9), 0.0);
        assert_eq!(child.value(10), 0.0);
        assert_eq!(child.value(11), 0.0);
    }

    #[test]
    fn test_build_vector_column_with_list_input() {
        let data_type = DataType::Vector { dimensions: 2 };
        let extractor = PropertyExtractor::new("test_vec", &data_type);

        let props = [
            Some(Value::List(vec![Value::Float(1.0), Value::Float(2.0)])),
            Some(Value::List(vec![Value::Int(3), Value::Int(4)])),
            Some(Value::Vector(vec![5.0, 6.0])),
        ];
        let deleted = [false, false, false];

        let arr_ref = extractor
            .build_vector_column(3, &deleted, |i| props[i].as_ref(), 2)
            .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();

        assert_eq!(arr.len(), 3);
        assert!(arr.is_valid(0));
        assert!(arr.is_valid(1));
        assert!(arr.is_valid(2));

        // Check values
        let child = arr
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(child.value(0), 1.0);
        assert_eq!(child.value(1), 2.0);
        assert_eq!(child.value(2), 3.0);
        assert_eq!(child.value(3), 4.0);
        assert_eq!(child.value(4), 5.0);
        assert_eq!(child.value(5), 6.0);
    }

    #[test]
    fn test_build_binary_vector_column_roundtrip() {
        // A `BinaryVector(dim)` column is `FixedSizeList<UInt8, dim>`. Accepts a
        // native `BinaryVector` or a `List` of byte-ints, decodes back with type
        // fidelity as `Value::BinaryVector` (not a generic int list).
        let data_type = DataType::BinaryVector { dimensions: 3 };
        let extractor = PropertyExtractor::new("bits", &data_type);

        let props = [
            Some(Value::BinaryVector(vec![0x00, 0xFF, 0xA5])),
            Some(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(255),
            ])),
            None, // deleted
            None, // missing/null
        ];
        let deleted = [false, false, true, false];

        let arr_ref = extractor
            .build_binary_vector_column(4, &deleted, |i| props[i].as_ref(), 3)
            .unwrap();

        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(arr.len(), 4);
        assert!(arr.is_valid(0));
        assert!(arr.is_valid(1));
        assert!(arr.is_valid(2)); // deleted rows are valid zeros
        assert!(!arr.is_valid(3)); // null row

        let child = arr.values().as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!(child.value(0), 0x00);
        assert_eq!(child.value(1), 0xFF);
        assert_eq!(child.value(2), 0xA5);
        assert_eq!(child.value(3), 1);
        assert_eq!(child.value(5), 255);

        // Read-back preserves type identity.
        assert_eq!(
            arrow_to_value(&arr_ref, 0, Some(&data_type)).unwrap(),
            Value::BinaryVector(vec![0x00, 0xFF, 0xA5])
        );
        assert_eq!(
            arrow_to_value(&arr_ref, 1, Some(&data_type)).unwrap(),
            Value::BinaryVector(vec![1, 2, 255])
        );
        assert_eq!(
            arrow_to_value(&arr_ref, 3, Some(&data_type)).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_build_binary_vector_column_wrong_length_fails_closed() {
        // A present, wrong-lane-count value must error at flush, not silently null.
        let data_type = DataType::BinaryVector { dimensions: 3 };
        let extractor = PropertyExtractor::new("bits", &data_type);
        let props = [Some(Value::BinaryVector(vec![1, 2]))]; // only 2 lanes
        let deleted = [false];
        let err = extractor
            .build_binary_vector_column(1, &deleted, |i| props[i].as_ref(), 3)
            .unwrap_err();
        assert!(err.to_string().contains("bits"), "got: {err}");
    }

    // Tests for multi-vector (ColBERT) `List<Vector>` columns (issue #96)

    #[test]
    fn test_build_multivector_list_column_roundtrip() {
        // A multi-vector property is `List<FixedSizeList<Float32, dim>>` with a
        // VARIABLE token count per row. Each token is a dense vector, so it reads
        // back with type fidelity as a `Value::List` of `Value::Vector` tokens
        // (parity with `SparseVector`/`Btic` — `FixedSizeList<Float32>` decodes to
        // `Value::Vector`, not a generic float list).
        let data_type = DataType::List(Box::new(DataType::Vector { dimensions: 3 }));
        let extractor = PropertyExtractor::new("tokens", &data_type);

        let props = [
            // row 0: two tokens
            Some(Value::List(vec![
                Value::Vector(vec![1.0, 2.0, 3.0]),
                Value::Vector(vec![4.0, 5.0, 6.0]),
            ])),
            // row 1: three tokens (different count -> exercises variable length)
            Some(Value::List(vec![
                Value::Vector(vec![7.0, 8.0, 9.0]),
                Value::Vector(vec![10.0, 11.0, 12.0]),
                Value::Vector(vec![13.0, 14.0, 15.0]),
            ])),
            // row 2: empty token set (present but zero tokens)
            Some(Value::List(vec![])),
            // row 3: deleted, missing property
            None,
        ];
        let deleted = [false, false, false, true];

        let arr_ref = extractor
            .build_column(4, &deleted, |i| props[i].as_ref())
            .unwrap();

        // Outer column is a variable-length list of fixed-size vectors.
        let outer = arr_ref.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(outer.len(), 4);
        assert!(outer.is_valid(0));
        assert!(outer.is_valid(1));
        assert!(outer.is_valid(2)); // empty-but-present row
        assert!(!outer.is_valid(3)); // deleted/missing -> null

        // Variable token counts survive.
        assert_eq!(outer.value(0).len(), 2);
        assert_eq!(outer.value(1).len(), 3);
        assert_eq!(outer.value(2).len(), 0);

        // Read back through the generic decoder; each token round-trips with type
        // fidelity as a `Value::Vector`.
        let row0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&data_type)).unwrap();
        assert_eq!(
            row0,
            Value::List(vec![
                Value::Vector(vec![1.0, 2.0, 3.0]),
                Value::Vector(vec![4.0, 5.0, 6.0]),
            ])
        );

        let row1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&data_type)).unwrap();
        let Value::List(tokens) = row1 else {
            panic!("row1 should decode to a list of tokens");
        };
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[2], Value::Vector(vec![13.0, 14.0, 15.0]));
    }

    #[test]
    fn test_build_multivector_invalid_inner_tokens() {
        // A row with a wrong-dimension token fails the build (fail-closed, issue
        // #137) instead of silently nulling the token, and the error names the
        // property and the offending token index.
        let data_type = DataType::List(Box::new(DataType::Vector { dimensions: 2 }));
        let extractor = PropertyExtractor::new("tokens", &data_type);

        let props = [Some(Value::List(vec![
            Value::Vector(vec![1.0, 2.0]),      // valid
            Value::Vector(vec![9.0, 9.0, 9.0]), // wrong dim -> error
        ]))];
        let deleted = [false];

        let err = extractor
            .build_column(1, &deleted, |i| props[i].as_ref())
            .unwrap_err()
            .to_string();
        assert!(err.contains("'tokens'"), "message: {err}");
        assert!(err.contains("token 1"), "message: {err}");

        // A non-vector token fails too.
        let props = [Some(Value::List(vec![Value::String("nope".to_string())]))];
        let err = extractor
            .build_column(1, &deleted, |i| props[i].as_ref())
            .unwrap_err()
            .to_string();
        assert!(err.contains("token 0"), "message: {err}");
    }

    #[test]
    fn test_build_multivector_valid_tokens_and_null_rows() {
        // Happy path: valid tokens (both Vector and numeric-List form) build, a
        // missing/null value stays a legal null row, and an empty token list is a
        // legal empty multi-vector.
        let data_type = DataType::List(Box::new(DataType::Vector { dimensions: 2 }));
        let extractor = PropertyExtractor::new("tokens", &data_type);

        let props = [
            Some(Value::List(vec![
                Value::Vector(vec![1.0, 2.0]),
                Value::List(vec![Value::Float(3.0), Value::Float(4.0)]),
            ])),
            None,
            Some(Value::Null),
            Some(Value::List(vec![])),
        ];
        let deleted = [false; 4];

        let arr_ref = extractor
            .build_column(4, &deleted, |i| props[i].as_ref())
            .unwrap();
        let outer = arr_ref.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(outer.is_valid(0));
        assert!(!outer.is_valid(1)); // absent -> null row
        assert!(!outer.is_valid(2)); // explicit Null -> null row
        assert!(outer.is_valid(3)); // empty multi-vector row
        let inner_row = outer.value(0);
        let inner = inner_row
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(inner.len(), 2);
        assert!(inner.is_valid(0));
        assert!(inner.is_valid(1));
        assert_eq!(outer.value(3).len(), 0);
    }

    #[test]
    fn test_build_vector_column_wrong_dims_is_error() {
        // Fail-closed flush for single dense vectors (issue #137): a wrong-dimension
        // value errors with the property name and row instead of becoming NULL.
        let data_type = DataType::Vector { dimensions: 3 };
        let extractor = PropertyExtractor::new("embedding", &data_type);

        let props = [
            Some(Value::Vector(vec![1.0, 2.0, 3.0])),
            Some(Value::Vector(vec![1.0, 2.0])),
        ];
        let deleted = [false; 2];

        let err = extractor
            .build_column(2, &deleted, |i| props[i].as_ref())
            .unwrap_err()
            .to_string();
        assert!(err.contains("'embedding'"), "message: {err}");
        assert!(err.contains("row 1"), "message: {err}");

        // Null rows and deleted rows still build fine.
        let props = [
            Some(Value::Vector(vec![1.0, 2.0, 3.0])),
            None,
            Some(Value::Null),
        ];
        let deleted = [false, false, true];
        let arr_ref = extractor
            .build_column(3, &deleted, |i| props[i].as_ref())
            .unwrap();
        let arr = arr_ref
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert!(arr.is_valid(0));
        assert!(!arr.is_valid(1));
    }

    #[test]
    fn test_extract_vector_f32_values_strict() {
        // Wrong dim -> Err; null/none -> Ok null row; deleted -> Ok valid zeros.
        assert!(
            extract_vector_f32_values_strict(Some(&Value::Vector(vec![1.0, 2.0])), false, 3)
                .is_err()
        );
        assert!(
            extract_vector_f32_values_strict(Some(&Value::String("x".into())), false, 3).is_err()
        );
        let (vals, valid) = extract_vector_f32_values_strict(None, false, 3).unwrap();
        assert_eq!((vals, valid), (vec![0.0, 0.0, 0.0], false));
        let (vals, valid) = extract_vector_f32_values_strict(Some(&Value::Null), false, 3).unwrap();
        assert_eq!((vals, valid), (vec![0.0, 0.0, 0.0], false));
        let (vals, valid) =
            extract_vector_f32_values_strict(Some(&Value::Vector(vec![1.0, 2.0])), true, 3)
                .unwrap();
        assert_eq!((vals, valid), (vec![0.0, 0.0, 0.0], true));
        let (vals, valid) =
            extract_vector_f32_values_strict(Some(&Value::Vector(vec![1.0, 2.0, 3.0])), false, 3)
                .unwrap();
        assert_eq!((vals, valid), (vec![1.0, 2.0, 3.0], true));
    }

    #[test]
    fn test_values_to_array_multivector_schemaless_deferred() {
        // Schemaless (Arrow-type-driven) multi-vector writes are intentionally NOT
        // supported yet (issue #96, Phase 1.5). Declared-schema writes go through
        // `build_list_column` instead. This test pins the deferral contract so a
        // future change that enables it updates this expectation deliberately.
        let values = vec![Value::List(vec![Value::Vector(vec![1.0, 2.0])])];
        let dt = ArrowDataType::List(Arc::new(Field::new(
            "item",
            ArrowDataType::FixedSizeList(
                Arc::new(Field::new("item", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        )));
        assert!(values_to_array(&values, &dt).is_err());
    }

    /// H13: out-of-range i64 values must become NULL in i32/date32 columns
    /// instead of silently wrapping to a different number/date.
    #[test]
    fn test_int32_and_date32_columns_null_out_of_range() {
        let dt = DataType::Int64;
        let extractor = PropertyExtractor::new("x", &dt);

        let over = Value::Int(i64::from(i32::MAX) + 1);
        let ok = Value::Int(42);
        let vals = [over, ok];
        let deleted = [false, false];

        let arr = extractor
            .build_int32_column(2, &deleted, |i| Some(&vals[i]))
            .unwrap();
        let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(
            arr.is_null(0),
            "out-of-range i64 must be NULL in an int32 column, not wrapped"
        );
        assert_eq!(arr.value(1), 42);

        let arr = extractor
            .build_date32_column(2, &deleted, |i| Some(&vals[i]))
            .unwrap();
        let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert!(
            arr.is_null(0),
            "out-of-range day count must be NULL in a date32 column, not wrapped"
        );
        assert_eq!(arr.value(1), 42);
    }
}

#[cfg(test)]
mod decode_failure_is_not_null {
    //! A blob that cannot be decoded must not read as `Value::Null`.
    //!
    //! `Value::Null` is a legal property value, so substituting it for a
    //! failed decode told the caller "this property is absent" when the truth
    //! was "this property could not be read". Every arm that can fail now
    //! returns `Err`; these pin that, because the failure mode is a wrong
    //! answer rather than a loud one and nothing else would catch a
    //! regression (#233 class, #215's sibling).

    use super::*;
    use arrow_array::{FixedSizeBinaryArray, LargeBinaryArray};

    /// An undecodable LargeBinary payload keeps its bytes; it does not become Null.
    ///
    /// There is no `DataType` that says "this column holds a CypherValue", so
    /// an unhinted LargeBinary column may legitimately hold opaque bytes — the
    /// contract `procedure.rs` documents and pins. Erroring would break every
    /// plugin yielding raw bytes.
    ///
    /// The fix at this arm is therefore `Null` → `Bytes`, not `Null` → `Err`:
    /// `Value::Null` is a legal value that made an unreadable property
    /// indistinguishable from an absent one, while `Value::Bytes` carries the
    /// payload the caller can still inspect. The arms that *do* know their type
    /// — BTIC, the shape mismatches, the terminal fallback — return `Err`.
    #[test]
    fn an_undecodable_blob_keeps_its_bytes_rather_than_becoming_null() {
        // 0xFF is not a valid CypherValue tag.
        let junk = vec![0xFFu8, 0x00, 0x01];
        let arr = LargeBinaryArray::from(vec![Some(junk.as_slice())]);
        let got = arrow_to_value(&arr, 0, None).expect("an opaque payload must not fail the read");
        assert_eq!(
            got,
            Value::Bytes(junk),
            "an undecodable payload must keep its bytes, not read as a legal Null"
        );
    }

    /// An empty blob is still a genuine null — the control.
    ///
    /// Without this the test above would pass for a decoder that errored on
    /// everything, which would be a different defect.
    #[test]
    fn an_empty_blob_is_still_null() {
        let arr = LargeBinaryArray::from(vec![Some(&[][..])]);
        assert_eq!(arrow_to_value(&arr, 0, None).unwrap(), Value::Null);
    }

    /// A real value still decodes — the second control.
    #[test]
    fn a_valid_blob_still_decodes() {
        let encoded = uni_common::cypher_value_codec::encode(&Value::Int(7));
        let arr = LargeBinaryArray::from(vec![Some(encoded.as_slice())]);
        assert_eq!(arrow_to_value(&arr, 0, None).unwrap(), Value::Int(7));
    }

    /// A 24-byte BTIC interval that is not a valid encoding is an error.
    #[test]
    fn a_corrupt_btic_interval_is_an_error() {
        let arr = FixedSizeBinaryArray::try_from_iter([[0xFFu8; 24]].into_iter())
            .expect("fixed size binary");
        let err = arrow_to_value(&arr, 0, None)
            .expect_err("a corrupt BTIC interval must not decode to Null");
        assert!(
            err.to_string().contains("BTIC decode failed"),
            "unexpected error: {err}"
        );
    }

    /// An Arrow type with no decoder arm is an error, not a null.
    ///
    /// This is the arm whose own comment recorded that it had hidden a missing
    /// `LargeUtf8` decoder — "a silent wrong answer if the column holds real
    /// data" — and shipped anyway.
    #[test]
    fn an_undecodable_arrow_type_is_an_error() {
        let arr = arrow_array::DurationSecondArray::from(vec![Some(5i64)]);
        let err = arrow_to_value(&arr, 0, None)
            .expect_err("an Arrow type with no decoder arm must not read as Null");
        assert!(err.to_string().contains("no decoder for Arrow type"));
    }
}
