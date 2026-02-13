// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Common helpers shared across graph execution plan implementations.
//!
//! This module provides shared utilities to reduce code duplication across
//! the df_graph module's execution plan implementations.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::array::Array;
use datafusion::common::Result as DFResult;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::PlanProperties;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr};
use uni_store::storage::manager::StorageManager;

use super::GraphExecutionContext;
use super::unwind::arrow_to_json_value;
use crate::query::df_planner::HybridPhysicalPlanner;
use crate::query::planner::LogicalPlan;

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

/// Extract a `UInt64Array` of vertex/edge IDs from an Arrow column.
///
/// Accepts both `UInt64` (native VID type) and `Int64` (from parameter
/// injection where `arrow_to_json_value` round-trips through `Value::Int`).
/// For `Int64` columns the values are cast to `UInt64`.
///
/// # Errors
///
/// Returns a `DataFusionError::Execution` if the column is neither `UInt64`
/// nor `Int64`.
pub fn column_as_vid_array(
    col: &dyn arrow_array::Array,
) -> datafusion::error::Result<std::borrow::Cow<'_, arrow_array::UInt64Array>> {
    use arrow_array::{Int64Array, UInt64Array};

    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(std::borrow::Cow::Borrowed(arr));
    }

    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        let cast: UInt64Array = arr.iter().map(|v| v.map(|i| i as u64)).collect();
        return Ok(std::borrow::Cow::Owned(cast));
    }

    Err(datafusion::error::DataFusionError::Execution(format!(
        "VID column has type {:?}, expected UInt64 or Int64",
        col.data_type()
    )))
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

/// Encode a properties HashMap to CypherValue bytes for LargeBinary columns.
///
/// Used when materializing path properties that need to be stored in LargeBinary
/// columns. Converts the HashMap into a `Value::Map` and encodes it using the
/// CypherValue codec.
pub fn encode_props_to_cv(props: &std::collections::HashMap<String, uni_common::Value>) -> Vec<u8> {
    let val = uni_common::Value::Map(props.clone());
    uni_common::cypher_value_codec::encode(&val)
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

/// Re-encode a `LargeListArray` of CypherValue elements into a `LargeBinaryArray` of CypherValue arrays.
///
/// Each row in the input `LargeListArray` contains zero or more `LargeBinary`
/// elements that are individually CypherValue-encoded values. This function decodes
/// each element, wraps them into a `serde_json::Value::Array`, and re-encodes
/// the whole array as a single CypherValue blob in the output `LargeBinaryArray`.
///
/// Null rows in the input produce null entries in the output.
///
/// # Errors
///
/// Returns a `DataFusionError::Execution` if the input is not a
/// `LargeListArray` or if CypherValue decoding fails.
pub fn large_list_of_cv_to_cv_array(
    list: &datafusion::arrow::array::LargeListArray,
) -> datafusion::error::Result<Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::{LargeBinaryArray, LargeBinaryBuilder};

    let values = list.values();
    let binary_values = values
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "large_list_of_cv_to_cv_array: inner values must be LargeBinaryArray".to_string(),
            )
        })?;

    let mut builder = LargeBinaryBuilder::new();

    for row_idx in 0..list.len() {
        if list.is_null(row_idx) {
            builder.append_null();
            continue;
        }

        let start = list.offsets()[row_idx] as usize;
        let end = list.offsets()[row_idx + 1] as usize;

        let mut json_elements = Vec::with_capacity(end - start);
        for elem_idx in start..end {
            if binary_values.is_null(elem_idx) {
                json_elements.push(serde_json::Value::Null);
            } else {
                let blob = binary_values.value(elem_idx);
                match uni_common::cypher_value_codec::decode(blob) {
                    Ok(uni_val) => {
                        let json_val: serde_json::Value = uni_val.into();
                        json_elements.push(json_val);
                    }
                    Err(_) => json_elements.push(serde_json::Value::Null),
                }
            }
        }

        let uni_val: uni_common::Value = serde_json::Value::Array(json_elements).into();
        let bytes = uni_common::cypher_value_codec::encode(&uni_val);
        builder.append_value(&bytes);
    }

    Ok(Arc::new(builder.finish()))
}

/// Convert a typed `LargeListArray` to a `LargeBinaryArray` of CypherValue arrays.
///
/// Each row in the input `LargeListArray` contains zero or more elements of a
/// specific type (Int64, Float64, Utf8, Boolean, or nested LargeBinary). This
/// function converts each row into a JSON array and encodes it as a CypherValue blob.
///
/// If the inner type is already `LargeBinary` (CypherValue), delegates to
/// `large_list_of_cv_to_cv_array()`.
///
/// Null rows in the input produce null entries in the output.
///
/// # Errors
///
/// Returns a `DataFusionError::Execution` if CypherValue encoding fails.
pub fn typed_large_list_to_cv_array(
    list: &datafusion::arrow::array::LargeListArray,
) -> datafusion::error::Result<Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::{
        BooleanArray, Float64Array, Int64Array, LargeBinaryBuilder, StringArray,
    };

    let values = list.values();

    // If inner type is LargeBinary, delegate to existing function
    if values.data_type() == &DataType::LargeBinary {
        return large_list_of_cv_to_cv_array(list);
    }

    // Downcast the values array once before iterating over rows.
    // The converter closure maps (values_array, element_index) -> serde_json::Value.
    let elem_to_json: Box<dyn Fn(usize) -> serde_json::Value> = match values.data_type() {
        DataType::Int64 => {
            let typed = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Expected Int64Array".to_string())
                })?;
            Box::new(move |idx| {
                if typed.is_null(idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::Number(serde_json::Number::from(typed.value(idx)))
                }
            })
        }
        DataType::Float64 => {
            let typed = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Float64Array".to_string(),
                    )
                })?;
            Box::new(move |idx| {
                if typed.is_null(idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Number::from_f64(typed.value(idx))
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                }
            })
        }
        DataType::Utf8 => {
            let typed = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected StringArray".to_string(),
                    )
                })?;
            Box::new(move |idx| {
                if typed.is_null(idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(typed.value(idx).to_string())
                }
            })
        }
        DataType::Boolean => {
            let typed = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected BooleanArray".to_string(),
                    )
                })?;
            Box::new(move |idx| {
                if typed.is_null(idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::Bool(typed.value(idx))
                }
            })
        }
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Unsupported element type for typed_large_list_to_cv_array: {:?}",
                other
            )));
        }
    };

    let mut builder = LargeBinaryBuilder::new();

    for row_idx in 0..list.len() {
        if list.is_null(row_idx) {
            builder.append_null();
            continue;
        }

        let start = list.offsets()[row_idx] as usize;
        let end = list.offsets()[row_idx + 1] as usize;
        let json_elements: Vec<serde_json::Value> = (start..end).map(&elem_to_json).collect();

        let uni_val: uni_common::Value = serde_json::Value::Array(json_elements).into();
        let bytes = uni_common::cypher_value_codec::encode(&uni_val);
        builder.append_value(&bytes);
    }

    Ok(Arc::new(builder.finish()))
}

/// Convert a `LargeBinaryArray` of CypherValue-encoded arrays into a `LargeListArray`.
///
/// Each element in the input array is a CypherValue blob encoding a JSON array (e.g. `[1,2,3]`).
/// Elements are converted to the specified `element_type`. For example, if `element_type`
/// is `Int64`, CypherValue numbers are parsed as i64 values.
///
/// Non-array CypherValue values and nulls produce empty lists.
pub fn cv_array_to_large_list(
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
                "cv_array_to_large_list: expected LargeBinaryArray".to_string(),
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
        let uni_val = match uni_common::cypher_value_codec::decode(blob) {
            Ok(v) => v,
            Err(_) => {
                all_elements.push(Vec::new());
                nulls.push(false);
                continue;
            }
        };
        let json_val_decoded: serde_json::Value = uni_val.into();

        match json_val_decoded {
            serde_json::Value::Array(elements) => {
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
        // Fallback: keep as CypherValue LargeBinary blobs
        _ => {
            let mut builder = datafusion::arrow::array::builder::LargeBinaryBuilder::new();
            for elems in &all_elements {
                for elem in elems {
                    let elem_str = serde_json::to_string(elem).unwrap_or_default();
                    match serde_json::from_str::<serde_json::Value>(&elem_str) {
                        Ok(json_val) => {
                            let uni_val: uni_common::Value = json_val.into();
                            let bytes = uni_common::cypher_value_codec::encode(&uni_val);
                            builder.append_value(&bytes);
                        }
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

/// Execute a logical plan using a fresh HybridPhysicalPlanner with the given params.
///
/// Shared by `RecursiveCTEExec`, `GraphApplyExec`, and `ExistsExecExpr`.
pub async fn execute_subplan(
    plan: &LogicalPlan,
    params: &HashMap<String, Value>,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<SessionContext>>,
    storage: &Arc<StorageManager>,
    schema_info: &Arc<UniSchema>,
) -> DFResult<Vec<RecordBatch>> {
    let l0_context = graph_ctx.l0_context().clone();
    let prop_manager = graph_ctx.property_manager().clone();

    let planner = HybridPhysicalPlanner::with_l0_context(
        session_ctx.clone(),
        storage.clone(),
        l0_context,
        prop_manager,
        schema_info.clone(),
        params.clone(),
    );

    let execution_plan = planner.plan(plan).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Sub-plan error: {}", e))
    })?;

    let task_ctx = session_ctx.read().task_ctx();
    let partition_count = execution_plan
        .properties()
        .output_partitioning()
        .partition_count();

    let mut all_batches = Vec::new();
    for partition in 0..partition_count {
        let stream = execution_plan.execute(partition, task_ctx.clone())?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        all_batches.extend(batches);
    }

    Ok(all_batches)
}

/// Extract a single row from a RecordBatch as a HashMap of column name → Value.
///
/// Used to build parameters for correlated subqueries (Apply, EXISTS).
pub fn extract_row_params(batch: &RecordBatch, row_idx: usize) -> HashMap<String, Value> {
    let schema = batch.schema();
    let mut row = HashMap::new();
    for col_idx in 0..batch.num_columns() {
        let col_name = schema.field(col_idx).name().clone();
        let val = arrow_to_json_value(batch.column(col_idx).as_ref(), row_idx);
        row.insert(col_name, val);
    }
    row
}

/// Infer the output schema of a logical plan using UniSchema property metadata.
///
/// This is needed because correlated subqueries reference outer variables that
/// don't exist as physical columns at planning time, so we can't dry-run plan
/// the subquery to get its schema. Instead we walk the logical plan and use
/// `UniSchema` property metadata to infer types.
pub fn infer_logical_plan_schema(plan: &LogicalPlan, schema_info: &UniSchema) -> SchemaRef {
    // Walk to outermost Project
    if let LogicalPlan::Project { projections, .. } = plan {
        let fields: Vec<Field> = projections
            .iter()
            .map(|(expr, alias)| {
                let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
                let dt = infer_expr_type(expr, schema_info);
                Field::new(name, dt, true)
            })
            .collect();
        return Arc::new(Schema::new(fields));
    }

    // For non-Project plans, walk through wrapping nodes
    match plan {
        LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Distinct { input } => infer_logical_plan_schema(input, schema_info),
        _ => {
            // Fallback: empty schema
            Arc::new(Schema::empty())
        }
    }
}

/// Infer Arrow DataType for a Cypher expression using schema metadata.
fn infer_expr_type(expr: &Expr, schema_info: &UniSchema) -> DataType {
    match expr {
        Expr::Property(base, key) => {
            if let Expr::Variable(_) = base.as_ref() {
                // Look up key across all labels/edge types in schema
                for props in schema_info.properties.values() {
                    if let Some(meta) = props.get(key.as_str()) {
                        return meta.r#type.to_arrow();
                    }
                }
                DataType::LargeBinary
            } else {
                DataType::LargeBinary
            }
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                let lt = infer_expr_type(left, schema_info);
                let rt = infer_expr_type(right, schema_info);
                numeric_promotion(&lt, &rt)
            }
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
            | BinaryOp::And
            | BinaryOp::Or => DataType::Boolean,
            _ => DataType::LargeBinary,
        },
        Expr::Literal(lit) => match lit {
            CypherLiteral::Integer(_) => DataType::Int64,
            CypherLiteral::Float(_) => DataType::Float64,
            CypherLiteral::String(_) => DataType::Utf8,
            CypherLiteral::Bool(_) => DataType::Boolean,
            CypherLiteral::Null => DataType::Null,
        },
        Expr::Variable(_) => DataType::LargeBinary,
        Expr::FunctionCall { name, args, .. } => match name.to_lowercase().as_str() {
            "count" => DataType::Int64,
            "sum" | "avg" => {
                if let Some(arg) = args.first() {
                    let arg_type = infer_expr_type(arg, schema_info);
                    if matches!(arg_type, DataType::Float32 | DataType::Float64) {
                        DataType::Float64
                    } else {
                        DataType::Int64
                    }
                } else {
                    DataType::Int64
                }
            }
            "min" | "max" => {
                if let Some(arg) = args.first() {
                    infer_expr_type(arg, schema_info)
                } else {
                    DataType::LargeBinary
                }
            }
            "tostring" | "trim" | "ltrim" | "rtrim" | "tolower" | "toupper" | "left" | "right"
            | "substring" | "replace" | "reverse" | "type" => DataType::Utf8,
            "tointeger" | "toint" | "size" | "length" | "id" => DataType::Int64,
            "tofloat" => DataType::Float64,
            "toboolean" => DataType::Boolean,
            _ => DataType::LargeBinary,
        },
        _ => DataType::LargeBinary,
    }
}

/// Numeric type promotion for binary arithmetic.
fn numeric_promotion(left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        _ => DataType::Int64,
    }
}

/// Evaluate a simple expression to get a `uni_common::Value`.
///
/// Supports:
/// - Literal values
/// - Parameter references ($param)
/// - Literal lists
pub(crate) fn evaluate_simple_expr(
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> DFResult<Value> {
    match expr {
        Expr::Literal(lit) => Ok(lit.to_value()),

        Expr::Parameter(name) => params.get(name).cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("Parameter '{}' not found", name))
        }),

        Expr::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(evaluate_simple_expr(item, params)?);
            }
            Ok(Value::List(values))
        }

        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "Unsupported expression type for procedure argument: {:?}",
            expr
        ))),
    }
}
