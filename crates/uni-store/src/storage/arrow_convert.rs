// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Arrow type conversion utilities for reducing cognitive complexity.
//!
//! This module provides shared helper functions and macros for converting
//! between Arrow arrays and JSON Values, reducing code duplication across
//! vertex.rs, delta.rs, and executor.rs.

use anyhow::{Result, anyhow};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, DurationMicrosecondBuilder,
    FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder, StructBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, DurationMicrosecondArray,
    FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
    ListArray, StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt64Array,
};
use arrow_schema::{DataType as ArrowDataType, Field};
use std::sync::Arc;
use uni_common::DataType;
use uni_common::Value;
use uni_crdt::Crdt;

// ============================================================================
// Timestamp Column Builders
// ============================================================================

use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};

/// Build a timestamp column from a map of ID -> timestamp (microseconds).
///
/// This is a shared utility for building `_created_at` and `_updated_at` columns
/// in vertex and edge tables. Works with any hashable ID type (Vid, Eid, etc.).
fn build_timestamp_column_from_id_map<K, I>(
    ids: I,
    timestamps: Option<&HashMap<K, i64>>,
) -> ArrayRef
where
    K: Eq + std::hash::Hash,
    I: IntoIterator<Item = K>,
{
    let mut builder = TimestampMicrosecondBuilder::new().with_timezone("UTC");
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
    let mut builder = TimestampMicrosecondBuilder::new().with_timezone("UTC");
    for ts in timestamps {
        builder.append_option(ts);
    }
    Arc::new(builder.finish())
}

/// Parse a datetime string into microseconds since Unix epoch.
///
/// Tries RFC3339, "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M%:z",
/// and "%Y-%m-%dT%H:%MZ" formats.
fn parse_datetime_to_micros(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| ndt.and_utc().timestamp_micros())
        })
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                .map(|ndt| ndt.and_utc().timestamp_micros())
        })
        .or_else(|_| {
            chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M%:z")
                .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
        })
        .ok()
        .or_else(|| {
            s.strip_suffix('Z')
                .and_then(|base| chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M").ok())
                .map(|ndt| ndt.and_utc().timestamp_micros())
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
        if let Value::String(k) = arrow_to_value(key_col.as_ref(), i) {
            map.insert(k, arrow_to_value(val_col.as_ref(), i));
        }
    }
    Some(map)
}

/// Convert an Arrow array element at a given row index to a JSON Value.
///
/// This function handles all common Arrow types and recursively processes
/// nested structures like Lists and Structs.
pub fn arrow_to_value(col: &dyn Array, row: usize) -> Value {
    if col.is_null(row) {
        return Value::Null;
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
        let arr = list.value(row);
        let mut vals = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            vals.push(arrow_to_value(arr.as_ref(), i));
        }
        return Value::List(vals);
    }

    // Variable-size list
    if let Some(list) = col.as_any().downcast_ref::<ListArray>() {
        let arr = list.value(row);

        // Map types are stored as List(Struct(key, value)); reconstruct as map
        if let Some(obj) = try_reconstruct_map(&arr) {
            return Value::Map(obj);
        }

        let mut vals = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            vals.push(arrow_to_value(arr.as_ref(), i));
        }
        return Value::List(vals);
    }

    // Large list (variable-size list with i64 offsets)
    if let Some(list) = col.as_any().downcast_ref::<arrow_array::LargeListArray>() {
        let arr = list.value(row);
        let mut vals = Vec::with_capacity(arr.len());
        for i in 0..arr.len() {
            vals.push(arrow_to_value(arr.as_ref(), i));
        }
        return Value::List(vals);
    }

    // Struct type
    if let Some(s) = col.as_any().downcast_ref::<StructArray>() {
        let mut map = HashMap::new();
        for (field, child) in s.fields().iter().zip(s.columns()) {
            map.insert(field.name().clone(), arrow_to_value(child.as_ref(), row));
        }
        return Value::Map(map);
    }

    // Date32 type (days since epoch) - convert to ISO date string
    if let Some(d) = col.as_any().downcast_ref::<Date32Array>() {
        let days = d.value(row);
        // Convert days since Unix epoch to date string
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
            return Value::String(date.format("%Y-%m-%d").to_string());
        }
        return Value::Null;
    }

    // Timestamp (microseconds since epoch) - convert to ISO datetime string
    if let Some(ts) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let micros = ts.value(row);
        if let Some(dt) = chrono::DateTime::from_timestamp_micros(micros) {
            use chrono::Timelike;
            if dt.nanosecond() > 0 {
                let s = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
                let micros_part = dt.nanosecond() / 1000;
                return Value::String(format!("{}.{:06}Z", s, micros_part));
            } else if dt.second() == 0 {
                return Value::String(dt.format("%Y-%m-%dT%H:%MZ").to_string());
            } else {
                return Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string());
            }
        }
        return Value::Null;
    }

    // Time64 (microseconds since midnight) - convert to ISO time string
    if let Some(t) = col.as_any().downcast_ref::<Time64MicrosecondArray>() {
        let micros = t.value(row);
        let total_secs = micros / 1_000_000;
        let hours = total_secs / 3600;
        let minutes = (total_secs % 3600) / 60;
        let seconds = total_secs % 60;
        let micro_part = micros % 1_000_000;
        if micro_part > 0 {
            return Value::String(format!(
                "{:02}:{:02}:{:02}.{:06}",
                hours, minutes, seconds, micro_part
            ));
        } else {
            return Value::String(format!("{:02}:{:02}:{:02}", hours, minutes, seconds));
        }
    }

    // Duration (microseconds) - return as numeric for arithmetic
    if let Some(d) = col.as_any().downcast_ref::<DurationMicrosecondArray>() {
        return Value::Int(d.value(row));
    }

    // LargeBinary (JSONB-encoded JSON values)
    if let Some(b) = col.as_any().downcast_ref::<LargeBinaryArray>() {
        let bytes = b.value(row);
        if bytes.is_empty() {
            return Value::Null;
        }
        let raw = jsonb::RawJsonb::new(bytes);
        let json_str = raw.to_string();
        if json_str == "null" {
            return Value::Null;
        }
        let json_val: serde_json::Value =
            serde_json::from_str(&json_str).unwrap_or_else(|_| serde_json::Value::String(json_str));
        return Value::from(json_val);
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

// ============================================================================
// Helper functions for values_to_array to reduce CC
// ============================================================================

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
    let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
    for v in values {
        if v.is_null() {
            builder.append_null();
        } else if let Some(n) = v.as_i64() {
            builder.append_value(n);
        } else if let Some(s) = v.as_str() {
            match parse_datetime_to_micros(s) {
                Some(micros) => builder.append_value(micros),
                None => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    }

    let arr = builder.finish();
    let tz_str = tz.map(|t| t.as_ref()).unwrap_or("UTC");
    Arc::new(arr.with_timezone(tz_str))
}

fn values_to_large_binary_array(values: &[Value]) -> ArrayRef {
    let mut builder =
        arrow_array::builder::LargeBinaryBuilder::with_capacity(values.len(), values.len() * 64);
    for v in values {
        if v.is_null() {
            builder.append_null();
        } else {
            // Encode as JSONB — convert to serde_json::Value at the serialization boundary
            let json_val: serde_json::Value = v.clone().into();
            let jsonb_bytes = jsonb::to_owned_jsonb(&json_val)
                .map(|b| b.to_vec())
                .unwrap_or_else(|_| vec![]);
            builder.append_value(&jsonb_bytes);
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
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, tz) => {
            Ok(values_to_timestamp_array(values, tz.as_ref()))
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
        _ => Err(anyhow!("Unsupported type for conversion: {:?}", dt)),
    }
}

/// Property value extractor for building Arrow columns from entity properties.
pub struct PropertyExtractor<'a> {
    _name: &'a str,
    data_type: &'a DataType,
}

impl<'a> PropertyExtractor<'a> {
    pub fn new(name: &'a str, data_type: &'a DataType) -> Self {
        Self {
            _name: name,
            data_type,
        }
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
            DataType::Json => self.build_json_column(len, deleted, get_props),
            DataType::List(inner) => self.build_list_column(len, deleted, get_props, inner),
            DataType::Map(key, value) => self.build_map_column(len, deleted, get_props, key, value),
            DataType::Crdt(_) => self.build_crdt_column(len, deleted, get_props),
            DataType::DateTime | DataType::Timestamp => {
                self.build_timestamp_column(len, deleted, get_props)
            }
            DataType::Date => self.build_date32_column(len, deleted, get_props),
            DataType::Time => self.build_time64_column(len, deleted, get_props),
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
        let mut values = Vec::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let val = get_props(i).and_then(|v| v.as_str());
            if val.is_none() && is_deleted {
                values.push(Some(""));
            } else {
                values.push(val);
            }
        }
        Ok(Arc::new(StringArray::from(values)))
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
            } else if let Some(v) = val.and_then(|v| v.as_i64()) {
                Some(v)
            } else if let Some(s) = val.and_then(|v| v.as_str()) {
                parse_datetime_to_micros(s)
            } else {
                None
            };

            if is_deleted {
                values.push(Some(0));
            } else {
                values.push(ts);
            }
        }
        let arr = TimestampMicrosecondArray::from(values).with_timezone("UTC");
        Ok(Arc::new(arr))
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

    fn build_time64_column<F>(&self, len: usize, deleted: &[bool], get_props: F) -> Result<ArrayRef>
    where
        F: Fn(usize) -> Option<&'a Value>,
    {
        let mut builder = Time64MicrosecondBuilder::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            if is_deleted {
                builder.append_value(0);
                continue;
            }

            let val = get_props(i);
            let micros = if let Some(v) = val.and_then(|v| v.as_i64()) {
                Some(v)
            } else if let Some(s) = val.and_then(|v| v.as_str()) {
                // Try parsing time string in various formats
                // Supported: "HH:MM", "HH:MM:SS", "HH:MM:SS.fff", etc.
                parse_time_string_to_micros(s)
            } else {
                None
            };

            if let Some(v) = micros {
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
        let mut builder = DurationMicrosecondBuilder::with_capacity(len);
        for (i, &is_deleted) in deleted.iter().enumerate().take(len) {
            let raw_val = get_props(i);
            // Try to get microseconds from i64, or parse from ISO 8601 string
            let val = raw_val.and_then(|v| {
                v.as_i64().or_else(|| {
                    v.as_str().and_then(|s| {
                        // Parse ISO 8601 duration string (e.g., "PT1H30M")
                        if s.starts_with('P') || s.starts_with('p') {
                            parse_iso8601_duration_to_micros(s).ok()
                        } else {
                            None
                        }
                    })
                })
            });
            if val.is_none() && is_deleted {
                builder.append_value(0);
            } else if let Some(v) = val {
                builder.append_value(v);
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
            // Convert to serde_json::Value at the JSONB serialization boundary
            let json_val: serde_json::Value = uni_val.clone().into();
            let jsonb_bytes = jsonb::to_owned_jsonb(&json_val)
                .map_err(|e| anyhow!("JSONB encode error: {}", e))?
                .to_vec();
            builder.append_value(&jsonb_bytes);
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

/// Strip a timezone suffix ("Z", "+HH:MM", "-HH:MM") from a time string.
///
/// Arrow Time64 stores offset-naive microseconds since midnight, so timezone
/// information must be removed before parsing.
fn strip_timezone_suffix(s: &str) -> &str {
    if let Some(bare) = s.strip_suffix('Z') {
        return bare;
    }
    let bytes = s.as_bytes();
    if bytes.len() >= 6 {
        let sign_pos = bytes.len() - 6;
        if (bytes[sign_pos] == b'+' || bytes[sign_pos] == b'-') && bytes[sign_pos + 3] == b':' {
            return &s[..sign_pos];
        }
    }
    s
}

/// Parse a time string to microseconds since midnight.
///
/// Supports formats: "HH:MM", "HH:MM:SS", "HH:MM:SS.fff", etc.
/// Timezone suffixes ("Z", "+HH:MM", "-HH:MM") are stripped before parsing.
fn parse_time_string_to_micros(s: &str) -> Option<i64> {
    use chrono::Timelike;

    let bare = strip_timezone_suffix(s);

    let time = chrono::NaiveTime::parse_from_str(bare, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(bare, "%H:%M:%S"))
        .or_else(|_| chrono::NaiveTime::parse_from_str(bare, "%H:%M"))
        .ok()?;

    Some(time.num_seconds_from_midnight() as i64 * 1_000_000 + time.nanosecond() as i64 / 1000)
}

/// Parse ISO 8601 duration string to microseconds.
///
/// Supports formats like "PT1H30M", "P1D", "PT90S", etc.
/// This is a simplified parser for duration storage conversion.
fn parse_iso8601_duration_to_micros(s: &str) -> Result<i64> {
    let s = s.trim();
    if !s.starts_with('P') && !s.starts_with('p') {
        return Err(anyhow!("Duration must start with 'P'"));
    }

    const MICROS_PER_SECOND: i64 = 1_000_000;
    const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
    const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
    const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

    let mut total_micros: i64 = 0;
    let mut in_time_part = false;
    let mut num_str = String::new();

    for c in s[1..].chars() {
        if c == 'T' || c == 't' {
            in_time_part = true;
            continue;
        }

        if c.is_ascii_digit() || c == '.' {
            num_str.push(c);
        } else {
            if num_str.is_empty() {
                continue;
            }

            let value: f64 = num_str.parse().map_err(|_| anyhow!("Invalid number"))?;
            num_str.clear();

            let micros = match c.to_ascii_uppercase() {
                'Y' => (value * 365.25 * MICROS_PER_DAY as f64) as i64,
                'M' if !in_time_part => (value * 30.0 * MICROS_PER_DAY as f64) as i64, // months
                'W' => (value * 7.0 * MICROS_PER_DAY as f64) as i64,
                'D' => (value * MICROS_PER_DAY as f64) as i64,
                'H' => (value * MICROS_PER_HOUR as f64) as i64,
                'M' if in_time_part => (value * MICROS_PER_MINUTE as f64) as i64, // minutes
                'S' => (value * MICROS_PER_SECOND as f64) as i64,
                _ => return Err(anyhow!("Unknown duration unit: {}", c)),
            };
            total_micros += micros;
        }
    }

    Ok(total_micros)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        DurationMicrosecondArray,
        builder::{BinaryBuilder, Time64MicrosecondBuilder},
    };
    use std::collections::HashMap;
    use uni_crdt::{Crdt, GCounter};

    #[test]
    fn test_arrow_to_value_string() {
        let arr = StringArray::from(vec![Some("hello"), None, Some("world")]);
        assert_eq!(arrow_to_value(&arr, 0), Value::String("hello".to_string()));
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
        assert_eq!(arrow_to_value(&arr, 2), Value::String("world".to_string()));
    }

    #[test]
    fn test_arrow_to_value_int64() {
        let arr = Int64Array::from(vec![Some(42), None, Some(-10)]);
        assert_eq!(arrow_to_value(&arr, 0), Value::Int(42));
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
        assert_eq!(arrow_to_value(&arr, 2), Value::Int(-10));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_arrow_to_value_float64() {
        let arr = Float64Array::from(vec![Some(3.14), None]);
        assert_eq!(arrow_to_value(&arr, 0), Value::Float(3.14));
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_bool() {
        let arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(arrow_to_value(&arr, 0), Value::Bool(true));
        assert_eq!(arrow_to_value(&arr, 1), Value::Bool(false));
        assert_eq!(arrow_to_value(&arr, 2), Value::Null);
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
        // Test Time64MicrosecondArray conversion
        let mut builder = Time64MicrosecondBuilder::new();
        // 10:30:45 = 10*3600 + 30*60 + 45 = 37845 seconds = 37845000000 microseconds
        builder.append_value(37_845_000_000);
        // 00:00:00 = 0 microseconds
        builder.append_value(0);
        // 23:59:59.123456 = 86399.123456 seconds
        builder.append_value(86_399_123_456);
        builder.append_null();

        let arr = builder.finish();
        assert_eq!(
            arrow_to_value(&arr, 0),
            Value::String("10:30:45".to_string())
        );
        assert_eq!(
            arrow_to_value(&arr, 1),
            Value::String("00:00:00".to_string())
        );
        assert_eq!(
            arrow_to_value(&arr, 2),
            Value::String("23:59:59.123456".to_string())
        );
        assert_eq!(arrow_to_value(&arr, 3), Value::Null);
    }

    #[test]
    fn test_arrow_to_value_duration() {
        // Test DurationMicrosecondArray conversion
        let arr = DurationMicrosecondArray::from(vec![
            Some(1_000_000),      // 1 second in microseconds
            Some(3_600_000_000),  // 1 hour
            Some(86_400_000_000), // 1 day
            None,
        ]);

        assert_eq!(arrow_to_value(&arr, 0), Value::Int(1_000_000));
        assert_eq!(arrow_to_value(&arr, 1), Value::Int(3_600_000_000));
        assert_eq!(arrow_to_value(&arr, 2), Value::Int(86_400_000_000));
        assert_eq!(arrow_to_value(&arr, 3), Value::Null);
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
        let result = arrow_to_value(&arr, 0);
        assert!(result.as_object().is_some());
        let obj = result.as_object().unwrap();
        // GCounter serializes with tag "t": "gc"
        assert_eq!(obj.get("t"), Some(&Value::String("gc".to_string())));

        // Null value should return null
        assert_eq!(arrow_to_value(&arr, 1), Value::Null);
    }
}
