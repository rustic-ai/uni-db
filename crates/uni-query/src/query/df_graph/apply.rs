// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Apply (correlated subquery) execution plan for DataFusion.
//!
//! Implements `CALL { ... }` subqueries by executing the subquery once per
//! input row, injecting the input row's columns as parameters, and cross-joining
//! the results.
//!
//! # Semantics
//!
//! For each row from the input plan:
//! 1. Optionally filter via `input_filter`
//! 2. Inject the input row's columns as parameters
//! 3. Re-plan and execute the subquery with those parameters
//! 4. Cross-join: merge each subquery result row with the input row
//!
//! If input produces zero rows (after filtering), execute the subquery once
//! with the base parameters (standalone CALL support).

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    compute_plan_properties, execute_subplan, extract_row_params,
};
use crate::query::df_graph::unwind::arrow_to_json_value;
use crate::query::planner::LogicalPlan;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{Expr, UnaryOp};
use uni_store::storage::manager::StorageManager;

/// Apply (correlated subquery) execution plan.
///
/// The input is pre-planned as a physical plan (executed directly).
/// The subquery is stored as a **logical** plan and re-planned per row at runtime
/// with correlated parameters injected.
/// Handles both `SubqueryCall` (no input_filter) and `Apply` (with input_filter).
pub struct GraphApplyExec {
    /// Physical plan for the driving input (e.g., MATCH scan).
    /// Pre-planned at construction time to preserve property context.
    input_exec: Arc<dyn ExecutionPlan>,

    /// Logical plan for the correlated subquery (re-planned per row).
    subquery_plan: LogicalPlan,

    /// Optional pre-filter applied to input rows before subquery execution.
    input_filter: Option<Expr>,

    /// Graph execution context shared with sub-planners.
    graph_ctx: Arc<GraphExecutionContext>,

    /// DataFusion session context.
    session_ctx: Arc<RwLock<SessionContext>>,

    /// Storage manager for creating sub-planners.
    storage: Arc<StorageManager>,

    /// Schema for label/edge type lookups.
    schema_info: Arc<UniSchema>,

    /// Query parameters.
    params: HashMap<String, Value>,

    /// Output schema (merged: input columns + subquery columns).
    output_schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphApplyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphApplyExec")
            .field("has_input_filter", &self.input_filter.is_some())
            .finish()
    }
}

impl GraphApplyExec {
    /// Create a new Apply execution plan.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        subquery_plan: LogicalPlan,
        input_filter: Option<Expr>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = compute_plan_properties(output_schema.clone());

        Self {
            input_exec,
            subquery_plan,
            input_filter,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for GraphApplyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphApplyExec: filter={}",
            if self.input_filter.is_some() {
                "yes"
            } else {
                "none"
            }
        )
    }
}

impl ExecutionPlan for GraphApplyExec {
    fn name(&self) -> &str {
        "GraphApplyExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No physical children — sub-plans are re-planned at execution time
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "GraphApplyExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let input_exec = self.input_exec.clone();
        let subquery_plan = self.subquery_plan.clone();
        let input_filter = self.input_filter.clone();
        let graph_ctx = self.graph_ctx.clone();
        let session_ctx = self.session_ctx.clone();
        let storage = self.storage.clone();
        let schema_info = self.schema_info.clone();
        let params = self.params.clone();
        let output_schema = self.output_schema.clone();

        let fut = async move {
            run_apply(
                input_exec,
                &subquery_plan,
                input_filter.as_ref(),
                &graph_ctx,
                &session_ctx,
                &storage,
                &schema_info,
                &params,
                &output_schema,
            )
            .await
        };

        Ok(Box::pin(ApplyStream {
            state: ApplyStreamState::Running(Box::pin(fut)),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Core apply logic
// ---------------------------------------------------------------------------

/// Convert record batches into row-oriented `HashMap<String, Value>` representation.
fn batches_to_row_maps(batches: &[RecordBatch]) -> Vec<HashMap<String, Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row = HashMap::new();
            for col_idx in 0..batch.num_columns() {
                let col_name = schema.field(col_idx).name().clone();
                let val = arrow_to_json_value(batch.column(col_idx).as_ref(), row_idx);
                row.insert(col_name, val);
            }
            rows.push(row);
        }
    }
    rows
}

/// Evaluate a Cypher filter expression against a row.
///
/// Supports simple binary comparisons and boolean operations needed for
/// input_filter pushdown (e.g., `p.age > 30`, `p.status = 'active'`).
fn evaluate_filter(filter: &Expr, row: &HashMap<String, Value>) -> bool {
    match filter {
        Expr::BinaryOp { left, op, right } => {
            use uni_cypher::ast::BinaryOp;
            match op {
                BinaryOp::And => evaluate_filter(left, row) && evaluate_filter(right, row),
                BinaryOp::Or => evaluate_filter(left, row) || evaluate_filter(right, row),
                _ => {
                    let left_val = resolve_expr_value(left, row);
                    let right_val = resolve_expr_value(right, row);
                    match op {
                        BinaryOp::Eq => left_val == right_val,
                        BinaryOp::NotEq => left_val != right_val,
                        BinaryOp::Lt => {
                            compare_values(&left_val, &right_val) == Some(std::cmp::Ordering::Less)
                        }
                        BinaryOp::LtEq => matches!(
                            compare_values(&left_val, &right_val),
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                        ),
                        BinaryOp::Gt => {
                            compare_values(&left_val, &right_val)
                                == Some(std::cmp::Ordering::Greater)
                        }
                        BinaryOp::GtEq => matches!(
                            compare_values(&left_val, &right_val),
                            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                        ),
                        _ => false,
                    }
                }
            }
        }
        Expr::UnaryOp {
            op: UnaryOp::Not,
            expr,
        } => !evaluate_filter(expr, row),
        _ => {
            // Treat any other expression as a truth test on its resolved value
            let val = resolve_expr_value(filter, row);
            val.as_bool().unwrap_or(false)
        }
    }
}

/// Resolve a simple expression to a Value using the row context.
fn resolve_expr_value(expr: &Expr, row: &HashMap<String, Value>) -> Value {
    match expr {
        Expr::Literal(lit) => lit.to_value(),
        Expr::Variable(name) => row.get(name).cloned().unwrap_or(Value::Null),
        Expr::Property(base_expr, key) => {
            if let Expr::Variable(var) = base_expr.as_ref() {
                // Look up "var.key" in the row map
                let col_name = format!("{}.{}", var, key);
                row.get(&col_name).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

/// Compare two Values for ordering.
fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Build a RecordBatch from merged row maps using the output schema.
fn rows_to_batch(rows: &[HashMap<String, Value>], schema: &SchemaRef) -> DFResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name();
        match field.data_type() {
            DataType::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        if let Some(v) = val.as_u64() {
                            builder.append_value(v);
                        } else if let Some(v) = val.as_i64() {
                            builder.append_value(v as u64);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        if let Some(v) = val.as_i64() {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        if let Some(v) = val.as_i64() {
                            builder.append_value(v as i32);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        if let Some(v) = val.as_f64() {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        if let Some(v) = val.as_bool() {
                            builder.append_value(v);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::LargeBinary => {
                let mut builder = arrow_array::builder::LargeBinaryBuilder::with_capacity(
                    num_rows,
                    num_rows * 64,
                );
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        if val.is_null() {
                            builder.append_null();
                        } else {
                            // Serialize to CypherValue
                            let json_val: serde_json::Value = val.clone().into();
                            let json_str = serde_json::to_string(&json_val).unwrap_or_default();
                            match serde_json::from_str::<serde_json::Value>(&json_str) {
                                Ok(parsed) => {
                                    let uni_val: uni_common::Value = parsed.into();
                                    let cv_bytes = uni_common::cypher_value_codec::encode(&uni_val);
                                    builder.append_value(&cv_bytes);
                                }
                                Err(_) => builder.append_null(),
                            }
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::List(inner_field) if inner_field.data_type() == &DataType::Utf8 => {
                let inner = Arc::new(arrow_schema::Field::new("item", DataType::Utf8, true));
                let mut builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        match val {
                            Value::List(items) => {
                                for item in items {
                                    match item {
                                        Value::String(s) => builder.values().append_value(s),
                                        Value::Null => builder.values().append_null(),
                                        other => {
                                            builder.values().append_value(format!("{}", other))
                                        }
                                    }
                                }
                                builder.append(true);
                            }
                            Value::Null => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                let _ = inner;
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Null => {
                columns.push(Arc::new(arrow_array::NullArray::new(num_rows)));
            }
            // Default: Utf8 for everything else
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    if let Some(val) = row.get(col_name) {
                        match val {
                            Value::Null => builder.append_null(),
                            Value::String(s) => builder.append_value(s),
                            other => builder.append_value(format!("{}", other)),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Slice a single row from a RecordBatch, preserving Arrow types.
fn slice_row(batch: &RecordBatch, row_idx: usize) -> Vec<ArrayRef> {
    batch
        .columns()
        .iter()
        .map(|col| col.slice(row_idx, 1))
        .collect()
}

/// Run the apply operation: execute input, filter, correlate subquery, merge results.
///
/// Uses Arrow-native row slicing for input columns to preserve complex types
/// (Struct, List, etc.), and only converts to Value for parameter injection.
#[expect(clippy::too_many_arguments)]
async fn run_apply(
    input_exec: Arc<dyn ExecutionPlan>,
    subquery_plan: &LogicalPlan,
    input_filter: Option<&Expr>,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<SessionContext>>,
    storage: &Arc<StorageManager>,
    schema_info: &Arc<UniSchema>,
    params: &HashMap<String, Value>,
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    // 1. Execute pre-planned input physical plan directly
    let task_ctx = session_ctx.read().task_ctx();
    let partition_count = input_exec
        .properties()
        .output_partitioning()
        .partition_count();
    let mut input_batches = Vec::new();
    for partition in 0..partition_count {
        let stream = input_exec.execute(partition, task_ctx.clone())?;
        let batches: Vec<RecordBatch> = futures::TryStreamExt::try_collect(stream).await?;
        input_batches.extend(batches);
    }

    // 2. Collect (batch_ref, row_idx) for rows that pass the input filter,
    //    along with their Value-based params for subquery injection.
    let mut filtered_entries: Vec<(&RecordBatch, usize, HashMap<String, Value>)> = Vec::new();
    for batch in &input_batches {
        for row_idx in 0..batch.num_rows() {
            let row_params = extract_row_params(batch, row_idx);
            if let Some(filter) = input_filter
                && !evaluate_filter(filter, &row_params)
            {
                continue;
            }
            filtered_entries.push((batch, row_idx, row_params));
        }
    }

    // 3. Handle empty input: execute subquery once with base params
    if filtered_entries.is_empty() {
        let sub_batches = execute_subplan(
            subquery_plan,
            params,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
        )
        .await?;
        let sub_rows = batches_to_row_maps(&sub_batches);
        return rows_to_batch(&sub_rows, output_schema);
    }

    // 4. For each input row, execute subquery and collect output column arrays.
    //    Each output row is: input columns (sliced) + subquery columns (sliced).
    let input_schema = input_batches[0].schema();
    let num_input_cols = input_schema.fields().len();
    let num_output_cols = output_schema.fields().len();
    // Accumulate per-column arrays for all output rows
    let mut column_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_output_cols];

    for (batch, row_idx, row_params) in &filtered_entries {
        let mut sub_params = params.clone();
        sub_params.extend(row_params.clone());

        let sub_batches = execute_subplan(
            subquery_plan,
            &sub_params,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
        )
        .await?;

        let input_row_arrays = slice_row(batch, *row_idx);
        let sub_rows = batches_to_row_maps(&sub_batches);

        if sub_rows.is_empty() {
            // No subquery results — skip this input row (inner join semantics)
            continue;
        }

        for sub_row in &sub_rows {
            // Add input columns (Arrow-native, preserves types)
            for (col_idx, arr) in input_row_arrays.iter().enumerate() {
                column_arrays[col_idx].push(arr.clone());
            }

            // Add subquery columns using Value→Arrow conversion
            for (col_arr, field) in column_arrays[num_input_cols..num_output_cols]
                .iter_mut()
                .zip(output_schema.fields()[num_input_cols..num_output_cols].iter())
            {
                let col_name = field.name();
                let val = sub_row.get(col_name).cloned().unwrap_or(Value::Null);
                let arr = value_to_single_row_array(&val, field.data_type())?;
                col_arr.push(arr);
            }
        }
    }

    // 5. Concatenate all accumulated arrays per column
    if column_arrays[0].is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut final_columns: Vec<ArrayRef> = Vec::with_capacity(num_output_cols);
    for arrays in &column_arrays {
        let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow::compute::concat(&refs)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        final_columns.push(concatenated);
    }

    RecordBatch::try_new(output_schema.clone(), final_columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Convert a single Value to a single-row Arrow array of the given type.
fn value_to_single_row_array(val: &Value, data_type: &DataType) -> DFResult<ArrayRef> {
    match data_type {
        DataType::UInt64 => {
            let mut b = UInt64Builder::with_capacity(1);
            match val.as_u64().or_else(|| val.as_i64().map(|v| v as u64)) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(1);
            match val.as_i64() {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(1);
            match val.as_i64() {
                Some(v) => b.append_value(v as i32),
                None => b.append_null(),
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(1);
            match val.as_f64() {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(1);
            match val.as_bool() {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Null => Ok(Arc::new(arrow_array::NullArray::new(1)) as ArrayRef),
        _ => {
            // Default: Utf8
            let mut b = StringBuilder::with_capacity(1, 64);
            match val {
                Value::Null => b.append_null(),
                Value::String(s) => b.append_value(s),
                other => b.append_value(format!("{}", other)),
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

/// Stream state for the apply operation.
enum ApplyStreamState {
    /// The apply computation is running.
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Computation completed.
    Done,
}

/// Stream that runs the apply operation and emits the result.
struct ApplyStream {
    state: ApplyStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for ApplyStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            ApplyStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = ApplyStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = ApplyStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            ApplyStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ApplyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
