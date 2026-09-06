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
    Time64NanosecondArray, TimestampNanosecondArray, UInt32Array,
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
    /// Log a warning and read the value as `Null` on failure (lenient).
    ///
    /// Lenient exists so that one unreadable row does not fail an entire
    /// scan. It does not fabricate a value: substituting a default
    /// `GCounter` made an unreadable CRDT indistinguishable from a counter
    /// standing at 0 (#233).
    Lenient,
}

/// Maximum recursion depth for nested List/Map decoding to prevent stack overflow.
/// Issue #62: Added to prevent stack overflow from deeply nested structures.
pub const MAX_DECODE_DEPTH: usize = 32;

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
    value_from_column_inner(col, data_type, row, crdt_mode, 0)
}

/// Internal implementation of value_from_column with depth tracking.
fn value_from_column_inner(
    col: &dyn Array,
    data_type: &DataType,
    row: usize,
    crdt_mode: CrdtDecodeMode,
    depth: usize,
) -> Result<Value> {
    if depth > MAX_DECODE_DEPTH {
        return Err(anyhow!("decode depth exceeded (max {})", MAX_DECODE_DEPTH));
    }
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
        DataType::SparseVector { .. } => {
            // Explicit arm so a sparse column is never silently nulled by the
            // `_ => Ok(Value::Null)` fallback below. This serde_json shape
            // (`{indices, values}`) is the lossy JSON view; full-fidelity
            // `uni_common::Value::SparseVector` is produced by `arrow_to_value`
            // via `decode_column_value`.
            let struct_arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("Invalid struct col for sparse vector"))?;
            if struct_arr.is_null(row) {
                return Ok(Value::Null);
            }
            let indices_list = struct_arr
                .column_by_name("indices")
                .and_then(|c| c.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| anyhow!("sparse vector missing list column 'indices'"))?;
            let values_list = struct_arr
                .column_by_name("values")
                .and_then(|c| c.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| anyhow!("sparse vector missing list column 'values'"))?;
            let idx_vals = indices_list.value(row);
            let idx_arr = idx_vals
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("sparse 'indices' inner not UInt32"))?;
            let w_vals = values_list.value(row);
            let w_arr = w_vals
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("sparse 'values' inner not Float32"))?;
            let indices: Vec<Value> = (0..idx_arr.len())
                .map(|i| serde_json::json!(idx_arr.value(i)))
                .collect();
            let values: Vec<Value> = (0..w_arr.len())
                .map(|i| serde_json::json!(w_arr.value(i)))
                .collect();
            let mut map = serde_json::Map::new();
            map.insert("indices".to_string(), Value::Array(indices));
            map.insert("values".to_string(), Value::Array(values));
            Ok(Value::Object(map))
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
        DataType::Bytes => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| anyhow!("Invalid large binary col for Bytes"))?;
            if arr.is_null(row) {
                return Ok(Value::Null);
            }
            // serde_json::Value has no native bytes variant; encode as JSON array of u8.
            let bytes = arr.value(row);
            Ok(Value::Array(
                bytes.iter().map(|b| serde_json::json!(*b)).collect(),
            ))
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
                CrdtDecodeMode::Lenient => match Crdt::from_msgpack(bytes) {
                    Ok(crdt) => Ok(serde_json::to_value(crdt)?),
                    Err(e) => {
                        // #233 Tier 1: this used to substitute
                        // `Crdt::GCounter(GCounter::new())`, so an unreadable
                        // CRDT of *any* variant silently read as a zero-valued
                        // GCounter — the wrong value and the wrong type, with
                        // nothing downstream able to tell it apart from a
                        // counter that genuinely stands at 0. Null is the
                        // honest answer: the value could not be read. The
                        // `to_value` failure was swallowed the same way, by
                        // `unwrap_or(Value::Null)`, and now propagates.
                        log::warn!("Failed to deserialize CRDT, reading as null: {e}");
                        Ok(Value::Null)
                    }
                },
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
                vec.push(value_from_column_inner(
                    values.as_ref(),
                    inner,
                    i,
                    crdt_mode,
                    depth + 1,
                )?);
            }
            Ok(Value::Array(vec))
        }
        DataType::Map(_, _) => {
            let list_arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("Invalid map (list) col"))?;
            if list_arr.is_null(row) {
                return Ok(Value::Null);
            }
            // Decode through the unified map reconstructor (single source of truth with
            // `arrow_to_value`): it handles typed scalar value children, raw-`Bytes`
            // (`uni_raw_bytes`-marked) children, and CV-encoded nested-value fallback
            // children uniformly by runtime Arrow type — so this is correct regardless of
            // the declared value type (a nested value is stored as a CV LargeBinary, which
            // a declared-type recursion would otherwise fail to downcast).
            let struct_arr = list_arr.value(row);
            let uni_map = super::arrow_convert::try_reconstruct_map(&struct_arr)
                .ok_or_else(|| anyhow!("Invalid struct array inner for map"))?;
            let mut map = serde_json::Map::with_capacity(uni_map.len());
            for (k, v) in uni_map {
                map.insert(
                    k,
                    serde_json::to_value(&v).unwrap_or(serde_json::Value::Null),
                );
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
                if nanos_arr.is_null(row) {
                    return Ok(Value::Null);
                }
                let tv = if offset_arr.is_null(row) {
                    TemporalValue::LocalTime {
                        nanos_since_midnight: nanos_arr.value(row),
                    }
                } else {
                    TemporalValue::Time {
                        nanos_since_midnight: nanos_arr.value(row),
                        offset_seconds: offset_arr.value(row),
                    }
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
                if nanos_arr.is_null(row) {
                    return Ok(Value::Null);
                }
                let tv = if offset_arr.is_null(row) {
                    TemporalValue::LocalDateTime {
                        nanos_since_epoch: nanos_arr.value(row),
                    }
                } else {
                    let timezone_name =
                        (!tz_arr.is_null(row)).then(|| tz_arr.value(row).to_string());
                    TemporalValue::DateTime {
                        nanos_since_epoch: nanos_arr.value(row),
                        offset_seconds: offset_arr.value(row),
                        timezone_name,
                    }
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
        // Point decodes as a `serde_json` object mirroring the `Value::Map` shape
        // `point(...)` produces, so the legacy `value_from_column` path (e.g.
        // `delta.rs`) does not silently read `Null`. The rich
        // `decode_column_value` path reconstructs a native `Value::Map` instead.
        DataType::Point(_) => {
            let v = super::arrow_convert::arrow_to_value(col, row, Some(data_type));
            Ok(serde_json::to_value(&v).unwrap_or(Value::Null))
        }
        _ => Ok(Value::Null),
    }
}

/// Decode an Arrow column value to a [`uni_common::Value`], preserving
/// `Value::Temporal` variants for round-trip fidelity.
///
/// For DateTime/Timestamp/Date/Time, delegates to [`super::arrow_convert::arrow_to_value`].
/// For all other types, decodes via [`value_from_column`] and converts.
pub fn decode_column_value(
    col: &dyn Array,
    data_type: &DataType,
    row: usize,
    crdt_mode: CrdtDecodeMode,
) -> anyhow::Result<uni_common::Value> {
    match data_type {
        DataType::DateTime
        | DataType::Timestamp
        | DataType::Date
        | DataType::Time
        | DataType::Btic
        | DataType::Bytes
        // Sparse vectors decode to a full-fidelity `Value::SparseVector` via
        // `arrow_to_value`; the `value_from_column` serde_json path would lose
        // the type (an object would round-trip back as a `Map`).
        | DataType::SparseVector { .. }
        // Binary vectors likewise decode to a full-fidelity `Value::BinaryVector`
        // via `arrow_to_value` (`FixedSizeList<UInt8>` → `BinaryVector`); the
        // serde_json path has no arm and would return `Value::Null`.
        | DataType::BinaryVector { .. }
        // Point columns decode to the `Value::Map` shape `point(...)` produces via
        // the struct reconstruction in `arrow_to_value`; the `value_from_column`
        // scalar path has no Point arm and would return `Value::Null`.
        | DataType::Point(_)
        // Maps decode natively (full fidelity, CV-aware) via the unified
        // `try_reconstruct_map` path inside `arrow_to_value`, which handles typed scalar
        // value children, raw-`Bytes` (uni_raw_bytes-marked) children, and CV-encoded
        // nested-value fallback children uniformly by runtime Arrow type.
        | DataType::Map(_, _) => Ok(super::arrow_convert::arrow_to_value(
            col,
            row,
            Some(data_type),
        )),
        _ => value_from_column(col, data_type, row, crdt_mode).map(uni_common::Value::from),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int64Builder, StringBuilder};

    /// A CRDT that cannot be decoded must not read as a zero-valued counter.
    ///
    /// #233 Tier 1. `Lenient` substituted `Crdt::GCounter(GCounter::new())`,
    /// which serializes to a counter standing at 0. A caller could not tell
    /// that apart from a genuine 0, so an unreadable ORSet, LWWRegister or
    /// PNCounter silently became "count = 0" — a wrong value of the wrong
    /// type, on the only path that uses `Lenient` (`delta.rs`).
    #[test]
    fn lenient_crdt_decode_failure_reads_as_null_not_a_zero_counter() {
        use uni_common::core::schema::CrdtType;

        let array = BinaryArray::from(vec![&b"not-valid-msgpack"[..]]);
        let dt = DataType::Crdt(CrdtType::GCounter);

        let val = value_from_column(&array, &dt, 0, CrdtDecodeMode::Lenient)
            .expect("lenient decode still yields a value rather than an error");

        assert_eq!(
            val,
            Value::Null,
            "an undecodable CRDT must read as Null, not as a fabricated default"
        );

        // The specific regression: the old fallback produced a GCounter whose
        // rendering carries a zero count. Assert the shape is gone, so this
        // test fails if the substitution is ever restored.
        let rendered = val.to_string();
        assert!(
            !rendered.contains("GCounter"),
            "must not fabricate a GCounter, got {rendered}"
        );
    }

    /// Strict mode keeps propagating the same failure as an error.
    #[test]
    fn strict_crdt_decode_failure_is_an_error() {
        use uni_common::core::schema::CrdtType;

        let array = BinaryArray::from(vec![&b"not-valid-msgpack"[..]]);
        let dt = DataType::Crdt(CrdtType::GCounter);

        assert!(
            value_from_column(&array, &dt, 0, CrdtDecodeMode::Strict).is_err(),
            "strict mode must surface an undecodable CRDT"
        );
    }

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

    #[test]
    fn test_decode_bool() {
        use arrow_array::builder::BooleanBuilder;
        let mut builder = BooleanBuilder::new();
        builder.append_value(true);
        builder.append_value(false);
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Bool, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(true));

        let val = value_from_column(&array, &DataType::Bool, 1, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(false));
    }

    #[test]
    fn test_decode_float64() {
        use arrow_array::builder::Float64Builder;
        let mut builder = Float64Builder::new();
        builder.append_value(3.25);
        builder.append_value(-0.5);
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Float64, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(3.25));

        let val = value_from_column(&array, &DataType::Float64, 1, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(-0.5));
    }

    #[test]
    fn test_decode_int32() {
        use arrow_array::builder::Int32Builder;
        let mut builder = Int32Builder::new();
        builder.append_value(42);
        builder.append_value(-1);
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Int32, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(42));

        let val = value_from_column(&array, &DataType::Int32, 1, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, serde_json::json!(-1));
    }

    #[test]
    fn test_decode_float32() {
        use arrow_array::builder::Float32Builder;
        let mut builder = Float32Builder::new();
        builder.append_value(1.5);
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Float32, 0, CrdtDecodeMode::Strict).unwrap();
        // Float32 has limited precision so compare approximately
        let f = val.as_f64().unwrap();
        assert!((f - 1.5).abs() < 0.001);
    }

    #[test]
    fn test_decode_vector() {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
        let values_builder = Float32Builder::new();
        let mut builder = FixedSizeListBuilder::new(values_builder, 3);
        builder.values().append_value(1.0);
        builder.values().append_value(2.0);
        builder.values().append_value(3.0);
        builder.append(true);
        let array = builder.finish();

        let val = value_from_column(
            &array,
            &DataType::Vector { dimensions: 3 },
            0,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(val, serde_json::json!([1.0, 2.0, 3.0]));
    }

    #[test]
    fn test_decode_date() {
        use arrow_array::builder::Date32Builder;
        let mut builder = Date32Builder::new();
        // 2021-01-01 = 18628 days since epoch
        builder.append_value(18628);
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Date, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, Value::String("2021-01-01".to_string()));
    }

    #[test]
    fn test_decode_date_null() {
        use arrow_array::builder::Date32Builder;
        let mut builder = Date32Builder::new();
        builder.append_null();
        let array = builder.finish();

        let val = value_from_column(&array, &DataType::Date, 0, CrdtDecodeMode::Strict).unwrap();
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn test_decode_list_of_strings() {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.values().append_value("c");
        builder.append(true);
        let array = builder.finish();

        let val = value_from_column(
            &array,
            &DataType::List(Box::new(DataType::String)),
            0,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(val, serde_json::json!(["a", "b", "c"]));
    }

    #[test]
    fn test_decode_list_of_ints() {
        use arrow_array::builder::{Int64Builder, ListBuilder};
        let values_builder = Int64Builder::new();
        let mut builder = ListBuilder::new(values_builder);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);
        let array = builder.finish();

        let val = value_from_column(
            &array,
            &DataType::List(Box::new(DataType::Int64)),
            0,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(val, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_decode_list_null() {
        use arrow_array::builder::{Int64Builder, ListBuilder};
        let values_builder = Int64Builder::new();
        let mut builder = ListBuilder::new(values_builder);
        builder.append_null();
        let array = builder.finish();

        let val = value_from_column(
            &array,
            &DataType::List(Box::new(DataType::Int64)),
            0,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn test_decode_column_value_non_struct_point_is_null() {
        // A physically-wrong column (String) declared as Point must reconstruct to
        // Null rather than panic — the struct downcast fails and falls through.
        use uni_common::core::schema::PointType;
        let mut builder = StringBuilder::new();
        builder.append_value("test");
        let array = builder.finish();

        let val = decode_column_value(
            &array,
            &DataType::Point(PointType::Geographic),
            0,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(val, uni_common::Value::Null);
    }

    #[test]
    fn test_point_struct_roundtrip() {
        // A geographic Point encoded via the storage builder must decode back to
        // the `Value::Map` shape `point(...)` produces (previously it errored on
        // write and decoded to Null).
        use std::collections::HashMap;
        use uni_common::core::schema::PointType;

        let point = uni_common::Value::Map(HashMap::from([
            (
                "type".to_string(),
                uni_common::Value::String("Point".into()),
            ),
            ("crs".to_string(), uni_common::Value::String("WGS84".into())),
            ("latitude".to_string(), uni_common::Value::Float(51.5)),
            ("longitude".to_string(), uni_common::Value::Float(-0.12)),
        ]));

        let arr = crate::storage::arrow_convert::values_to_point_struct_array(
            &[point.clone(), uni_common::Value::Null],
            PointType::Geographic,
        );

        let decoded = decode_column_value(
            &arr,
            &DataType::Point(PointType::Geographic),
            0,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(decoded, point);

        // Row 1 was Null → the struct slot is null → decodes back to Null.
        let decoded_null = decode_column_value(
            &arr,
            &DataType::Point(PointType::Geographic),
            1,
            CrdtDecodeMode::Strict,
        )
        .unwrap();
        assert_eq!(decoded_null, uni_common::Value::Null);
    }
}
