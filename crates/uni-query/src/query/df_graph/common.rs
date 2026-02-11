// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Common helpers shared across graph execution plan implementations.
//!
//! This module provides shared utilities to reduce code duplication across
//! the df_graph module's execution plan implementations.

use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::arrow::array::Array;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::PlanProperties;
use std::sync::Arc;

/// Compute standard plan properties for graph operators.
///
/// All graph operators use the same plan properties:
/// - Unknown partitioning with 1 partition
/// - Incremental emission type
/// - Bounded execution
pub fn compute_plan_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        datafusion::physical_plan::execution_plan::EmissionType::Incremental,
        datafusion::physical_plan::execution_plan::Boundedness::Bounded,
    )
}

/// Build the standard node struct fields for path structures.
///
/// Used when materializing path objects containing nodes.
/// Fields: `_vid`, `_label`, `properties`
pub fn node_struct_fields() -> arrow_schema::Fields {
    arrow_schema::Fields::from(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("_label", DataType::Utf8, true),
        Field::new("properties", DataType::LargeBinary, true),
    ])
}

/// Build the standard edge struct fields for path structures.
///
/// Used when materializing path objects containing edges.
/// Fields: `_eid`, `_type_name`, `_src`, `_dst`, `properties`
pub fn edge_struct_fields() -> arrow_schema::Fields {
    arrow_schema::Fields::from(vec![
        Field::new("_eid", DataType::UInt64, false),
        Field::new("_type_name", DataType::Utf8, false),
        Field::new("_src", DataType::UInt64, false),
        Field::new("_dst", DataType::UInt64, false),
        Field::new("properties", DataType::LargeBinary, true),
    ])
}

/// Build edge list field for schema with given step variable name.
///
/// Creates a list of edge structs for the relationship variable in VLP patterns.
/// For example, `r` in `MATCH (a)-[r*1..3]->(b)` gets a `List<EdgeStruct>`.
pub fn build_edge_list_field(step_var: &str) -> Field {
    let edge_item = Field::new("item", DataType::Struct(edge_struct_fields()), true);
    Field::new(step_var, DataType::List(Arc::new(edge_item)), false)
}

/// Build path struct field for schema with given path variable name.
///
/// Creates a struct field with `nodes` and `relationships` lists.
pub fn build_path_struct_field(path_var: &str) -> Field {
    let node_item = Field::new("item", DataType::Struct(node_struct_fields()), true);
    let nodes_field = Field::new("nodes", DataType::List(Arc::new(node_item)), false);

    let edge_item = Field::new("item", DataType::Struct(edge_struct_fields()), true);
    let relationships_field =
        Field::new("relationships", DataType::List(Arc::new(edge_item)), false);

    Field::new(
        path_var,
        DataType::Struct(arrow_schema::Fields::from(vec![
            nodes_field,
            relationships_field,
        ])),
        false,
    )
}

/// Convert a `LargeBinaryArray` of JSONB-encoded arrays into a `LargeListArray`.
///
/// Each element in the input array is a JSONB blob encoding a JSON array (e.g. `[1,2,3]`).
/// Elements are converted to the specified `element_type`. For example, if `element_type`
/// is `Int64`, JSONB numbers are parsed as i64 values.
///
/// Non-array JSONB values and nulls produce empty lists.
pub fn jsonb_array_to_large_list(
    array: &dyn datafusion::arrow::array::Array,
    element_type: &DataType,
) -> datafusion::error::Result<Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::LargeBinaryArray;
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};

    let binary_arr = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "jsonb_array_to_large_list: expected LargeBinaryArray".to_string(),
            )
        })?;

    // Collect all JSON elements across all rows
    let num_rows = binary_arr.len();
    let mut all_elements: Vec<Vec<serde_json::Value>> = Vec::with_capacity(num_rows);
    let mut nulls = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        if binary_arr.is_null(i) {
            all_elements.push(Vec::new());
            nulls.push(false);
            continue;
        }

        let blob = binary_arr.value(i);
        let raw = jsonb::RawJsonb::new(blob);
        let json_str = raw.to_string();

        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(serde_json::Value::Array(elements)) => {
                all_elements.push(elements);
                nulls.push(true);
            }
            _ => {
                all_elements.push(Vec::new());
                nulls.push(true);
            }
        }
    }

    // Build typed values array and offsets
    let mut offsets: Vec<i64> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);

    let values_array: Arc<dyn datafusion::arrow::array::Array> = match element_type {
        DataType::Int64 => {
            let mut builder = datafusion::arrow::array::builder::Int64Builder::new();
            for elems in &all_elements {
                for elem in elems {
                    match elem {
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                builder.append_value(i);
                            } else if let Some(f) = n.as_f64() {
                                builder.append_value(f as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        serde_json::Value::Null => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                offsets.push(offsets.last().unwrap() + elems.len() as i64);
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = datafusion::arrow::array::builder::Float64Builder::new();
            for elems in &all_elements {
                for elem in elems {
                    match elem {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        serde_json::Value::Null => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                offsets.push(offsets.last().unwrap() + elems.len() as i64);
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let mut builder = datafusion::arrow::array::builder::StringBuilder::new();
            for elems in &all_elements {
                for elem in elems {
                    match elem {
                        serde_json::Value::String(s) => builder.append_value(s),
                        serde_json::Value::Null => builder.append_null(),
                        other => builder.append_value(other.to_string()),
                    }
                }
                offsets.push(offsets.last().unwrap() + elems.len() as i64);
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = datafusion::arrow::array::builder::BooleanBuilder::new();
            for elems in &all_elements {
                for elem in elems {
                    match elem {
                        serde_json::Value::Bool(b) => builder.append_value(*b),
                        serde_json::Value::Null => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                offsets.push(offsets.last().unwrap() + elems.len() as i64);
            }
            Arc::new(builder.finish())
        }
        // Fallback: keep as JSONB LargeBinary blobs
        _ => {
            let mut builder = datafusion::arrow::array::builder::LargeBinaryBuilder::new();
            for elems in &all_elements {
                for elem in elems {
                    let elem_str = serde_json::to_string(elem).unwrap_or_default();
                    match jsonb::parse_value(elem_str.as_bytes()) {
                        Ok(owned) => builder.append_value(owned.to_vec()),
                        Err(_) => builder.append_null(),
                    }
                }
                offsets.push(offsets.last().unwrap() + elems.len() as i64);
            }
            Arc::new(builder.finish())
        }
    };

    let field = Arc::new(Field::new("item", element_type.clone(), true));
    let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let null_buffer = datafusion::arrow::buffer::NullBuffer::from(nulls);

    let large_list = datafusion::arrow::array::LargeListArray::new(
        field,
        offset_buffer,
        values_array,
        Some(null_buffer),
    );

    Ok(Arc::new(large_list))
}
