// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Arrow column value decoding utilities.
//!
//! Provides a unified `value_from_column` function for decoding Arrow column
//! values to `serde_json::Value`, used by both PropertyManager and DeltaDataset.

use anyhow::{Result, anyhow};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, ListArray, StringArray, StructArray,
    Time64NanosecondArray, TimestampNanosecondArray,
};
use serde_json::Value;
use uni_common::{DataType, TemporalValue};
use uni_crdt::Crdt;

/// Controls how CRDT decode errors are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CrdtDecodeMode {
    /// Return an error on CRDT decode failure (strict validation).
    #[default]
    Strict,
    /// Log a warning and return a default GCounter on failure (lenient).
    Lenient,
}

/// Decode an Arrow column value to a serde_json::Value.
///
/// # Arguments
/// * `col` - The Arrow array to read from
/// * `data_type` - The uni_common::DataType describing the column's logical type
/// * `row` - The row index to read
/// * `crdt_mode` - How to handle CRDT decode errors
///
/// # Returns
/// The decoded JSON value, or an error if decoding fails.
pub fn value_from_column(
    col: &dyn Array,
    data_type: &DataType,
    row: usize,
    crdt_mode: CrdtDecodeMode,
) -> Result<Value> {
    match data_type {
        DataType::String => {
            let s = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Invalid string col"))?
                .value(row);
            Ok(Value::String(s.to_string()))
        }
        DataType::Int32 => {
            let v = col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow!("Invalid int32 col"))?
                .value(row);
            Ok(serde_json::json!(v))
        }
        DataType::Int64 => {
            let v = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("Invalid int64 col"))?
                .value(row);
            Ok(serde_json::json!(v))
        }
        DataType::Float32 => {
            let v = col
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("Invalid float32 col"))?
                .value(row);
            Ok(serde_json::json!(v))
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("Invalid float64 col"))?
                .value(row);
            Ok(serde_json::json!(v))
        }
        DataType::Bool => {
            let v = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("Invalid bool col"))?
                .value(row);
            Ok(serde_json::json!(v))
        }
        DataType::Vector { .. } => {
            let list_arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| anyhow!("Invalid fixed list col for vector"))?;
            let values = list_arr.value(row);
            let float_values = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("Invalid float32 inner col for vector"))?;

            let vec: Vec<f32> = (0..float_values.len())
                .map(|i| float_values.value(i))
                .collect();
            Ok(serde_json::json!(vec))
        }
        DataType::CypherValue => {
            let bytes = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| anyhow!("Invalid large binary col for CypherValue"))?
                .value(row);
            if bytes.is_empty() {
                return Ok(Value::Null);
            }
            let uni_val = uni_common::cypher_value_codec::decode(bytes)
                .map_err(|e| anyhow!("CypherValue decode error: {}", e))?;
            // Convert uni_common::Value to serde_json::Value
            Ok(uni_val.into())
        }
        DataType::Crdt(_) => {
            let bytes = col
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("Invalid binary col for CRDT"))?
                .value(row);

            match crdt_mode {
                CrdtDecodeMode::Strict => {
                    let crdt = Crdt::from_msgpack(bytes)
                        .map_err(|e| anyhow!("CRDT decode error: {}", e))?;
                    Ok(serde_json::to_value(crdt)?)
                }
                CrdtDecodeMode::Lenient => {
                    let crdt = Crdt::from_msgpack(bytes).unwrap_or_else(|e| {
                        log::warn!("Failed to deserialize CRDT: {}", e);
                        Crdt::GCounter(uni_crdt::GCounter::new())
                    });
                    Ok(serde_json::to_value(crdt).unwrap_or(Value::Null))
                }
            }
        }
        DataType::List(inner) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid list col"))?;
            if list_arr.is_null(row) {
                return Ok(Value::Null);
            }
            let values = list_arr.value(row);
            let mut vec = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                vec.push(value_from_column(values.as_ref(), inner, i, crdt_mode)?);
            }
            Ok(Value::Array(vec))
        }
        DataType::Map(key_type, value_type) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid map (list) col"))?;
            if list_arr.is_null(row) {
                return Ok(Value::Null);
            }
            let struct_arr = list_arr.value(row);
            let struct_arr_ref = struct_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("Invalid struct array inner for map"))?;

            let keys = struct_arr_ref.column(0);
            let values = struct_arr_ref.column(1);

            let mut map = serde_json::Map::with_capacity(struct_arr_ref.len());

            for i in 0..struct_arr_ref.len() {
                let k_val = value_from_column(keys.as_ref(), key_type, i, crdt_mode)?;
                let v_val = value_from_column(values.as_ref(), value_type, i, crdt_mode)?;

                // Convert key to string for JSON object
                if let Some(k_str) = k_val.as_str() {
                    map.insert(k_str.to_string(), v_val);
                } else if let Some(k_int) = k_val.as_i64() {
                    map.insert(k_int.to_string(), v_val);
                } else {
                    map.insert(k_val.to_string(), v_val);
                }
            }
            Ok(Value::Object(map))
        }
        DataType::Date => {
            let arr = col
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow!("Invalid date32 col"))?;
            if arr.is_null(row) {
                return Ok(Value::Null);
            }
            let days = arr.value(row);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            if let Some(date) = epoch.checked_add_signed(chrono::Duration::days(days as i64)) {
                Ok(Value::String(date.format("%Y-%m-%d").to_string()))
            } else {
                Ok(Value::Null)
            }
        }
        DataType::Time => {
            // Preferred schema: struct{nanos_since_midnight, offset_seconds}
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
                if nanos_arr.is_null(row) || offset_arr.is_null(row) {
                    return Ok(Value::Null);
                }
                let tv = TemporalValue::Time {
                    nanos_since_midnight: nanos_arr.value(row),
                    offset_seconds: offset_arr.value(row),
                };
                return Ok(Value::String(tv.to_string()));
            }

            // Legacy schema: plain time64 nanos, assume UTC offset=0
            let arr = col
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| anyhow!("Invalid time64 col"))?;
            if arr.is_null(row) {
                return Ok(Value::Null);
            }
            let tv = TemporalValue::Time {
                nanos_since_midnight: arr.value(row),
                offset_seconds: 0,
            };
            Ok(Value::String(tv.to_string()))
        }
        DataType::Duration => {
            // Duration is stored as LargeBinary via CypherValue codec
            let arr = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| anyhow!("Invalid duration col (expected LargeBinary)"))?;
            if arr.is_null(row) {
                return Ok(Value::Null);
            }
            let bytes = arr.value(row);
            let uni_val = uni_common::cypher_value_codec::decode(bytes)
                .map_err(|e| anyhow!("Failed to decode duration: {}", e))?;
            // Return canonical ISO-8601 text for compatibility.
            if let uni_common::Value::Temporal(uni_common::TemporalValue::Duration {
                months,
                days,
                nanos,
            }) = &uni_val
            {
                let tv = TemporalValue::Duration {
                    months: *months,
                    days: *days,
                    nanos: *nanos,
                };
                Ok(Value::String(tv.to_string()))
            } else {
                Ok(serde_json::json!(uni_val.to_string()))
            }
        }
        DataType::DateTime | DataType::Timestamp => {
            // Preferred schema: struct{nanos_since_epoch, offset_seconds, timezone_name}
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
                    return Ok(Value::Null);
                }
                let timezone_name = (!tz_arr.is_null(row)).then(|| tz_arr.value(row).to_string());
                let tv = TemporalValue::DateTime {
                    nanos_since_epoch: nanos_arr.value(row),
                    offset_seconds: offset_arr.value(row),
                    timezone_name,
                };
                return Ok(Value::String(tv.to_string()));
            }

            // Legacy schema: plain timestamp nanos, assume UTC offset=0
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| anyhow!("Invalid timestamp col"))?;
            if arr.is_null(row) {
                return Ok(Value::Null);
            }
            let tv = TemporalValue::DateTime {
                nanos_since_epoch: arr.value(row),
                offset_seconds: 0,
                timezone_name: arr.timezone().map(|s| s.to_string()),
            };
            Ok(Value::String(tv.to_string()))
        }
        _ => Ok(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int64Builder, StringBuilder};

    #[test]
    fn test_decode_string() {
        let mut builder = StringBuilder::new();
        builder.append_value("hello");
        builder.append_value("world");
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::String, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, Value::String("hello".to_string()));

        let val = value_from_column(&array, &DataType::String, 1, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, Value::String("world".to_string()));
    }

    #[test]
    fn test_decode_int64() {
        let mut builder = Int64Builder::new();
        builder.append_value(42);
        builder.append_value(-100);
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Int64, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(42));

        let val = value_from_column(&array, &DataType::Int64, 1, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(-100));
    }

    #[test]
    fn test_decode_json() {
        use arrow_array::builder::LargeBinaryBuilder;

        // Encode JSON values as JSONB binary (matching the LargeBinary storage format)
        let mut builder = LargeBinaryBuilder::new();

        let obj_cv = {
            let val: uni_common::Value = serde_json::json!({"key": "value"}).into();
            uni_common::cypher_value_codec::encode(&val)
        };
        builder.append_value(&obj_cv);

        let null_cv = uni_common::cypher_value_codec::encode(&uni_common::Value::Null);
        builder.append_value(&null_cv);

        let text_cv = uni_common::cypher_value_codec::encode(&uni_common::Value::String(
            "plain text".to_string(),
        ));
        builder.append_value(&text_cv);

        let array = builder.finish();

        let val =
            value_from_column(&array, &DataType::CypherValue, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!({"key": "value"}));

        let val =
            value_from_column(&array, &DataType::CypherValue, 1, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, Value::Null);

        let val =
            value_from_column(&array, &DataType::CypherValue, 2, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, Value::String("plain text".to_string()));
    }
}
