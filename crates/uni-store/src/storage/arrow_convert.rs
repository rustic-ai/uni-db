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
    Int64Builder, IntervalMonthDayNanoBuilder, LargeBinaryBuilder, ListBuilder, StringBuilder,
    StructBuilder, Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampNanosecondBuilder,
    UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray, ListArray,
    StringArray, StructArray, Time64NanosecondArray, TimestampNanosecondArray, UInt64Array,
};
use arrow_schema::{DataType as ArrowDataType, Field};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::DataType;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema;
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
fn try_reconstruct_map(arr: &ArrayRef) -> Option<HashMap<String, Value>> {
    let structs = arr.as_any().downcast_ref::<StructArray>()?;
    let fields = structs.fields();
    if fields.len() != 2 || fields[0].name() != "key" || fields[1].name() != "value" {
        return None;
    }
    let key_col = structs.column(0);
    let val_col = structs.column(1);
    let mut map = HashMap::new();
    for i in 0..structs.len() {
        if let Value::String(k) = arrow_to_value(key_col.as_ref(), i, None) {
            map.insert(k, arrow_to_value(val_col.as_ref(), i, None));
        }
    }
    Some(map)
}

/// Convert all elements of an Arrow array into a `Vec<Value>`.
fn array_to_value_list(arr: &ArrayRef) -> Vec<Value> {
    (0..arr.len())
        .map(|i| arrow_to_value(arr.as_ref(), i, None))
        .collect()
}

/// Convert an Arrow array value at a given row index to a Uni Value.
///
/// Handles all common Arrow types and recursively processes nested structures
/// like Lists and Structs. The optional `data_type` parameter provides schema
/// context for decoding DateTime and Time struct arrays; when provided, it
/// takes precedence over runtime type detection.
pub fn arrow_to_value(col: &dyn Array, row: usize, data_type: Option<&DataType>) -> Value {
    if col.is_null(row) {
        return Value::Null;
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
                    if nanos_arr.is_null(row) || offset_arr.is_null(row) {
                        return Value::Null;
                    }
                    let nanos = nanos_arr.value(row);
                    let offset = offset_arr.value(row);
                    let tz_name = (!tz_arr.is_null(row)).then(|| tz_arr.value(row).to_string());
                    return Value::Temporal(uni_common::TemporalValue::DateTime {
                        nanos_since_epoch: nanos,
                        offset_seconds: offset,
                        timezone_name: tz_name,
                    });
                }
                // Fall back to old schema migration: TimestampNanosecond → DateTime with offset=0
                if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                    let nanos = ts.value(row);
                    let tz_name = ts.timezone().map(|s| s.to_string());
                    return Value::Temporal(uni_common::TemporalValue::DateTime {
                        nanos_since_epoch: nanos,
                        offset_seconds: 0,
                        timezone_name: tz_name,
                    });
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
                        return Value::Null;
                    }
                    let nanos = nanos_arr.value(row);
                    let offset = offset_arr.value(row);
                    return Value::Temporal(uni_common::TemporalValue::Time {
                        nanos_since_midnight: nanos,
                        offset_seconds: offset,
                    });
                }
                // Fall back to old schema: Time64Nanosecond → Time with offset=0
                if let Some(t) = col.as_any().downcast_ref::<Time64NanosecondArray>() {
                    let nanos = t.value(row);
                    return Value::Temporal(uni_common::TemporalValue::Time {
                        nanos_since_midnight: nanos,
                        offset_seconds: 0,
                    });
                }
            }
            _ => {}
        }
    }

    // String types
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Value::String(s.value(row).to_string());
    }

    // Integer types
    if let Some(u) = col.as_any().downcast_ref::<UInt64Array>() {
        return Value::Int(u.value(row) as i64);
    }
    if let Some(i) = col.as_any().downcast_ref::<Int64Array>() {
        return Value::Int(i.value(row));
    }
    if let Some(i) = col.as_any().downcast_ref::<Int32Array>() {
        return Value::Int(i.value(row) as i64);
    }

    // Float types
    if let Some(f) = col.as_any().downcast_ref::<Float64Array>() {
        return Value::Float(f.value(row));
    }
    if let Some(f) = col.as_any().downcast_ref::<Float32Array>() {
        return Value::Float(f.value(row) as f64);
    }

    // Boolean type
    if let Some(b) = col.as_any().downcast_ref::<BooleanArray>() {
        return Value::Bool(b.value(row));
    }

    // Fixed-size list (vectors)
    if let Some(list) = col.as_any().downcast_ref::<FixedSizeListArray>() {
        return Value::List(array_to_value_list(&list.value(row)));
    }

    // Variable-size list
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        let arr = list.value(row);

        // Map types are stored as List(Struct(key, value)); reconstruct as map
        if let Some(obj) = try_reconstruct_map(&arr) {
            return Value::Map(obj);
        }

        return Value::List(array_to_value_list(&arr));
    }

    // Large list (variable-size list with i64 offsets)
    if let Some(list) = col.as_any().downcast_ref::<arrow_array::LargeListArray>() {
        return Value::List(array_to_value_list(&list.value(row)));
    }

    // Struct type — detect temporal structs by field names before generic handler
    if let Some(s) = col.as_any().downcast_ref::<StructArray>() {
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

            if let (Some(Some(nanos)), Some(Some(offset))) = (nanos_opt, offset_opt) {
                let tz_name = tz_col.as_any().downcast_ref::<StringArray>().and_then(|a| {
                    if a.is_null(row) {
                        None
                    } else {
                        Some(a.value(row).to_string())
                    }
                });
                return Value::Temporal(uni_common::TemporalValue::DateTime {
                    nanos_since_epoch: nanos,
                    offset_seconds: offset,
                    timezone_name: tz_name,
                });
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
                return Value::Temporal(uni_common::TemporalValue::Time {
                    nanos_since_midnight: nanos,
                    offset_seconds: offset,
                });
            }
        }

        // Generic struct → Map
        let mut map = HashMap::new();
        for (field, child) in s.fields().iter().zip(s.columns()) {
            map.insert(
                field.name().clone(),
                arrow_to_value(child.as_ref(), row, None),
            );
        }
        return Value::Map(map);
    }

    // Date32 type (days since epoch) - return as Value::Temporal
    if let Some(d) = col.as_any().downcast_ref::<Date32Array>() {
        let days = d.value(row);
        return Value::Temporal(uni_common::TemporalValue::Date {
            days_since_epoch: days,
        });
    }

    // Timestamp (nanoseconds since epoch) - timezone presence determines DateTime vs LocalDateTime
    if let Some(ts) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let nanos = ts.value(row);
        return match ts.timezone() {
            Some(tz) => Value::Temporal(uni_common::TemporalValue::DateTime {
                nanos_since_epoch: nanos,
                offset_seconds: 0,
                timezone_name: Some(tz.to_string()),
            }),
            None => Value::Temporal(uni_common::TemporalValue::LocalDateTime {
                nanos_since_epoch: nanos,
            }),
        };
    }

    // Time64 (nanoseconds since midnight) - return as Value::Temporal
    if let Some(t) = col.as_any().downcast_ref::<Time64NanosecondArray>() {
        let nanos = t.value(row);
        return Value::Temporal(uni_common::TemporalValue::LocalTime {
            nanos_since_midnight: nanos,
        });
    }

    // IntervalMonthDayNano - return as Value::Temporal(Duration)
    if let Some(interval) = col.as_any().downcast_ref::<IntervalMonthDayNanoArray>() {
        let val = interval.value(row);
        return Value::Temporal(uni_common::TemporalValue::Duration {
            months: val.months as i64,
            days: val.days as i64,
            nanos: val.nanoseconds,
        });
    }

    // LargeBinary (CypherValue MessagePack-tagged encoding)
    if let Some(b) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        let bytes = b.value(row);
        if bytes.is_empty() {
            return Value::Null;
        }
        return uni_common::cypher_value_codec::decode(bytes).unwrap_or_else(|e| {
            eprintln!("CypherValue decode error: {}", e);
            Value::Null
        });
    }

    // Binary (CRDT MessagePack) - decode to Value via serde_json boundary
    if let Some(b) = col.as_any().downcast_ref::<BinaryArray>() {
        let bytes = b.value(row);
        return Crdt::from_msgpack(bytes)
            .ok()
            .and_then(|crdt| serde_json::to_value(&crdt).ok())
            .map(Value::from)
            .unwrap_or(Value::Null);
    }

    // Fallback
    Value::Null
}

fn values_to_uint64_array(values: &[Value]) -> ArrayRef {
    let mut builder = UInt64Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_u64() {
            builder.append_value(n);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_int64_array(values: &[Value]) -> ArrayRef {
    let mut builder = Int64Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_i64() {
            builder.append_value(n);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_int32_array(values: &[Value]) -> ArrayRef {
    let mut builder = Int32Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_i64() {
            builder.append_value(n as i32);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
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
    let mut builder = Float32Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_f64() {
            builder.append_value(n as f32);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_float64_array(values: &[Value]) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(values.len());
    for v in values {
        if let Some(n) = v.as_f64() {
            builder.append_value(n);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

fn values_to_fixed_size_binary_array(values: &[Value], size: i32) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), size);
    for v in values {
        if let Value::List(bytes) = v {
            let b: Vec<u8> = bytes
                .iter()
                .map(|bv| bv.as_u64().unwrap_or(0) as u8)
                .collect();
            if b.len() as i32 == size {
                builder.append_value(&b)?;
            } else {
                builder.append_null();
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn values_to_fixed_size_list_f32_array(values: &[Value], size: i32) -> ArrayRef {
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), size);
    for v in values {
        if let Value::List(arr) = v {
            if arr.len() as i32 == size {
                for item in arr {
                    builder
                        .values()
                        .append_value(item.as_f64().unwrap_or(0.0) as f32);
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        } else {
            builder.append(false);
        }
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
        if let Value::Temporal(uni_common::TemporalValue::DateTime {
            nanos_since_epoch,
            offset_seconds,
            timezone_name,
        }) = v
        {
            nanos_builder.append_value(*nanos_since_epoch);
            offset_builder.append_value(*offset_seconds);
            tz_builder.append_option(timezone_name.as_deref());
            null_buffer.append(true);
        } else {
            nanos_builder.append_null();
            offset_builder.append_null();
            tz_builder.append_null();
            null_buffer.append(false);
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
        if let Value::Temporal(uni_common::TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        }) = v
        {
            nanos_builder.append_value(*nanos_since_midnight);
            offset_builder.append_value(*offset_seconds);
            null_buffer.append(true);
        } else {
            nanos_builder.append_null();
            offset_builder.append_null();
            null_buffer.append(false);
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
    data_type: &'a DataType,
}

impl<'a> PropertyExtractor<'a> {
    pub fn new(_name: &'a str, data_type: &'a DataType) -> Self {
        Self { data_type }
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
            DataType::CypherValue => self.build_json_column(len, deleted, get_props),
            DataType::List(inner) => self.build_list_column(len, deleted, get_props, inner),
            DataType::Map(key, value) => self.build_map_column(len, deleted, get_props, key, value),
            DataType::Crdt(_) => self.build_crdt_column(len, deleted, get_props),
            DataType::DateTime => self.build_datetime_struct_column(len, deleted, get_props),
            DataType::Timestamp => self.build_timestamp_column(len, deleted, get_props),
            DataType::Date => self.build_date32_column(len, deleted, get_props),
            DataType::Time => self.build_time_struct_column(len, deleted, get_props),
            DataType::Duration => self.build_duration_column(len, deleted, get_props),
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
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_i64()).map(|v| v as i32);
            if val.is_none() && is_deleted {
                values.push(Some(0));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(Int32Array::from(values)))
    }

    fn build_int64_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_i64());
            if val.is_none() && is_deleted {
                values.push(Some(0));
            } else {
                values.push(val);
            }
        }
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
            let ts = if is_deleted || val.is_none() {
                Some(0i64)
            } else if let Some(Value::Temporal(tv)) = val {
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
            let days = if is_deleted || val.is_none() {
                Some(0)
            } else if let Some(Value::Temporal(uni_common::TemporalValue::Date {
                days_since_epoch,
            })) = val
            {
                Some(*days_since_epoch)
            } else if let Some(v) = val.and_then(|v| v.as_i64()) {
                Some(v as i32)
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

    fn build_float32_column<F>(
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
            let val = get_props(i).and_then(|v| v.as_f64()).map(|v| v as f32);
            if val.is_none() && is_deleted {
                values.push(Some(0.0));
            } else {
                values.push(val);
            }
        }
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
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_f64());
            if val.is_none() && is_deleted {
                values.push(Some(0.0));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(Float64Array::from(values)))
    }

    fn build_bool_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_bool());
            if val.is_none() && is_deleted {
                values.push(Some(false));
            } else {
                values.push(val);
            }
        }
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
            let (values, valid) = self.extract_vector_values(val, is_deleted, dimensions);
            for v in values {
                builder.values().append_value(v);
            }
            builder.append(valid);
        }
        Ok(Arc::new(builder.finish()))
    }

    /// Extract vector values from a property value, handling defaults and validation.
    ///
    /// Supports both `Value::Vector(Vec<f32>)` and `Value::List(Vec<Value>)` inputs.
    fn extract_vector_values(
        &self,
        val: Option<&Value>,
        is_deleted: bool,
        dimensions: usize,
    ) -> (Vec<f32>, bool) {
        let zeros = || vec![0.0_f32; dimensions];

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
            _ if is_deleted => (zeros(), true),       // Deleted entry gets default
            _ => (zeros(), false),                    // Missing or unsupported value
        }
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

    fn build_list_column<F>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: F,
        inner: &DataType,
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
            _ => Err(anyhow!("Unsupported inner type for List: {:?}", inner)),
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
                |v, b: &mut Int64Builder| {
                    if let Some(n) = v.as_i64() {
                        b.append_value(n);
                    } else {
                        b.append_null();
                    }
                },
            ),
            _ => Err(anyhow!("Unsupported value type for Map: {:?}", value)),
        }
    }

    /// Generic helper to build a map column with any value builder type.
    fn build_typed_map<F, B, A>(
        &self,
        len: usize,
        deleted: &[bool],
        get_props: &F,
        value_builder: B,
        value_arrow_type: arrow_schema::DataType,
        mut append_value: A,
    ) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
        B: arrow_array::builder::ArrayBuilder,
        A: FnMut(&Value, &mut B),
    {
        let key_builder = Box::new(StringBuilder::new());
        let value_builder = Box::new(value_builder);
        let struct_builder = StructBuilder::new(
            vec![
                Field::new("key", arrow_schema::DataType::Utf8, false),
                Field::new("value", value_arrow_type, true),
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
    fn test_arrow_to_value_string() {
        let arr = StringArray::from(vec![Some("hello"), None, Some("world")]);
        assert_eq!(
            arrow_to_value(&arr, 0, None),
            Value::String("hello".to_string())
        );
        assert_eq!(arrow_to_value(&arr, 1, None), Value::Null);
        assert_eq!(
            arrow_to_value(&arr, 2, None),
            Value::String("world".to_string())
        );
    }

    #[test]
    fn test_arrow_to_value_int64() {
        let arr = Int64Array::from(vec![Some(42), None, Some(-10)]);
        assert_eq!(arrow_to_value(&arr, 0, None), Value::Int(42));
        assert_eq!(arrow_to_value(&arr, 1, None), Value::Null);
        assert_eq!(arrow_to_value(&arr, 2, None), Value::Int(-10));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_arrow_to_value_float64() {
        let arr = Float64Array::from(vec![Some(3.14), None]);
        assert_eq!(arrow_to_value(&arr, 0, None), Value::Float(3.14));
        assert_eq!(arrow_to_value(&arr, 1, None), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_bool() {
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(arrow_to_value(&arr, 0, None), Value::Bool(true));
        assert_eq!(arrow_to_value(&arr, 1, None), Value::Bool(false));
        assert_eq!(arrow_to_value(&arr, 2, None), Value::Null);
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
        assert_eq!(arrow_to_value(&arr, 0, None).to_string(), "10:30:45");
        assert_eq!(arrow_to_value(&arr, 1, None).to_string(), "00:00");
        assert_eq!(arrow_to_value(&arr, 2, None).to_string(), "23:59:59.123456");
        assert_eq!(arrow_to_value(&arr, 3, None), Value::Null);
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

        assert_eq!(arrow_to_value(&arr, 0, None).to_string(), "PT1S");
        assert_eq!(arrow_to_value(&arr, 1, None).to_string(), "PT1H");
        assert_eq!(arrow_to_value(&arr, 2, None).to_string(), "PT24H");
        assert_eq!(arrow_to_value(&arr, 3, None), Value::Null);
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
        let result = arrow_to_value(&arr, 0, None);
        assert!(result.as_object().is_some());
        let obj = result.as_object().unwrap();
        // GCounter serializes with tag "t": "gc"
        assert_eq!(obj.get("t"), Some(&Value::String("gc".to_string())));

        // Null value should return null
        assert_eq!(arrow_to_value(&arr, 1, None), Value::Null);
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
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::DateTime));
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::DateTime));
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::DateTime));

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
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::DateTime));
        assert_eq!(decoded_0, values[0]);

        // Check second value is null
        assert!(arr.is_null(1));
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::DateTime));
        assert_eq!(decoded_1, Value::Null);

        // Check third value is valid
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::DateTime));
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
            let decoded = arrow_to_value(arr_ref.as_ref(), i, Some(&DataType::DateTime));
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
        let decoded_0 = arrow_to_value(&arr, 0, Some(&DataType::DateTime));
        let _decoded_1 = arrow_to_value(&arr, 1, Some(&DataType::DateTime));
        let decoded_2 = arrow_to_value(&arr, 2, Some(&DataType::DateTime));

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
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::Time));
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::Time));
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::Time));

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
        let decoded_0 = arrow_to_value(arr_ref.as_ref(), 0, Some(&DataType::Time));
        assert_eq!(decoded_0, values[0]);

        // Check second value is null
        assert!(arr.is_null(1));
        let decoded_1 = arrow_to_value(arr_ref.as_ref(), 1, Some(&DataType::Time));
        assert_eq!(decoded_1, Value::Null);

        // Check third value is valid
        let decoded_2 = arrow_to_value(arr_ref.as_ref(), 2, Some(&DataType::Time));
        assert_eq!(decoded_2, values[2]);
    }
}
