// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! UNWIND execution plan for DataFusion.
//!
//! This module provides [`GraphUnwindExec`], a DataFusion [`ExecutionPlan`] that
//! expands list values into multiple rows (similar to SQL `UNNEST`).
//!
//! # Supported Expressions
//!
//! Currently supports:
//! - Literal lists: `UNWIND [1, 2, 3] AS x`
//! - Variable references: `UNWIND list AS item` (where `list` is a column)
//! - Property access: `UNWIND n.items AS item`
//!
//! # Example
//!
//! ```text
//! Input:   [{"list": [1, 2, 3]}]
//! UNWIND:  list AS item
//! Output:  [{"list": [1,2,3], "item": 1},
//!           {"list": [1,2,3], "item": 2},
//!           {"list": [1,2,3], "item": 3}]
//! ```

use crate::query::df_graph::common::compute_plan_properties;
use arrow::compute::take;
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_cypher::ast::Expr;

/// UNWIND execution plan that expands list values into multiple rows.
///
/// Takes an input plan and an expression that evaluates to a list. For each
/// row in the input, if the expression evaluates to a list, produces multiple
/// output rows (one per list element) with the list element bound to a new
/// variable.
pub struct GraphUnwindExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Expression to evaluate (should produce a list).
    expr: Expr,

    /// Variable name to bind list elements to.
    variable: String,

    /// Query parameters for expression evaluation.
    params: HashMap<String, Value>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphUnwindExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphUnwindExec")
            .field("expr", &self.expr)
            .field("variable", &self.variable)
            .finish()
    }
}

impl GraphUnwindExec {
    /// Create a new UNWIND execution plan.
    ///
    /// # Arguments
    ///
    /// * `input` - Input plan providing rows to expand
    /// * `expr` - Expression that evaluates to a list
    /// * `variable` - Variable name for list elements
    /// * `params` - Query parameters for expression evaluation
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        expr: Expr,
        variable: impl Into<String>,
        params: HashMap<String, Value>,
    ) -> Self {
        let variable = variable.into();

        // Build output schema: input schema + new variable column
        let schema = Self::build_schema(input.schema(), &variable);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            expr,
            variable,
            params,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema by adding the unwind variable column.
    ///
    /// The UNWIND variable column stores JSON-encoded values and is marked
    /// with metadata `{"json_encoded": "true"}` so that result conversion
    /// can properly parse them back to their original types.
    fn build_schema(input_schema: SchemaRef, variable: &str) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add variable column as Utf8 with JSON-encoded metadata
        // This signals to result conversion that values should be parsed as JSON
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("json_encoded".to_string(), "true".to_string());
        let field = Field::new(variable, DataType::Utf8, true).with_metadata(metadata);
        fields.push(field);

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphUnwindExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "GraphUnwindExec: {} AS {}",
                    self.expr.to_string_repr(),
                    self.variable
                )
            }
        }
    }
}

impl ExecutionPlan for GraphUnwindExec {
    fn name(&self) -> &str {
        "GraphUnwindExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(
                "GraphUnwindExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.expr.clone(),
            self.variable.clone(),
            self.params.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(GraphUnwindStream {
            input: input_stream,
            expr: self.expr.clone(),
            _variable: self.variable.clone(),
            params: self.params.clone(),
            schema: self.schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream that performs the UNWIND operation.
struct GraphUnwindStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Expression to evaluate.
    expr: Expr,

    /// Variable name for list elements (used in schema).
    _variable: String,

    /// Query parameters.
    params: HashMap<String, Value>,

    /// Output schema.
    schema: SchemaRef,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl GraphUnwindStream {
    /// Process a single input batch.
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        // For each row, evaluate the expression and expand if it's a list
        let mut expansions: Vec<(usize, Value)> = Vec::new(); // (input_row_idx, list_element)

        for row_idx in 0..batch.num_rows() {
            // Evaluate expression for this row
            let list_value = self.evaluate_expr_for_row(&batch, row_idx)?;

            match list_value {
                Value::List(items) => {
                    for item in items {
                        expansions.push((row_idx, item));
                    }
                }
                Value::Null => {
                    // UNWIND on null produces no rows (Cypher semantics)
                }
                other => {
                    // Non-list values: treat as single-element list
                    expansions.push((row_idx, other));
                }
            }
        }

        self.build_output_batch(&batch, &expansions)
    }

    /// Evaluate the expression for a specific row.
    fn evaluate_expr_for_row(&self, batch: &RecordBatch, row_idx: usize) -> DFResult<Value> {
        self.evaluate_expr_impl(&self.expr, batch, row_idx)
    }

    /// Evaluate an expression recursively.
    fn evaluate_expr_impl(
        &self,
        expr: &Expr,
        batch: &RecordBatch,
        row_idx: usize,
    ) -> DFResult<Value> {
        match expr {
            // Literal list: [1, 2, 3]
            Expr::List(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(self.evaluate_expr_impl(item, batch, row_idx)?);
                }
                Ok(Value::List(values))
            }

            // Literal value
            Expr::Literal(lit) => Ok(lit.to_value()),

            // Parameter reference: $param
            Expr::Parameter(name) => self.params.get(name).cloned().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Parameter '{}' not found",
                    name
                ))
            }),

            // Variable reference: look up column
            Expr::Variable(var_name) => self.get_column_value(batch, var_name, row_idx),

            // Property access: n.prop
            Expr::Property(base_expr, prop_name) => {
                // Get the base value (should be a map)
                let base_value = self.evaluate_expr_impl(base_expr, batch, row_idx)?;

                match base_value {
                    Value::Map(map) => Ok(map.get(prop_name).cloned().unwrap_or(Value::Null)),
                    _ => {
                        // Try looking up as column name: var.prop
                        if let Expr::Variable(var_name) = base_expr.as_ref() {
                            let col_name = format!("{}.{}", var_name, prop_name);
                            if batch.schema().column_with_name(&col_name).is_some() {
                                return self.get_column_value(batch, &col_name, row_idx);
                            }
                        }
                        Ok(Value::Null)
                    }
                }
            }

            // Function call: range(1, 10)
            Expr::FunctionCall { name, args, .. } => {
                let name_lower = name.to_lowercase();
                match name_lower.as_str() {
                    "range" => {
                        if args.len() >= 2 {
                            let start = self.evaluate_expr_impl(&args[0], batch, row_idx)?;
                            let end = self.evaluate_expr_impl(&args[1], batch, row_idx)?;
                            let step = if args.len() >= 3 {
                                self.evaluate_expr_impl(&args[2], batch, row_idx)?
                            } else {
                                Value::Int(1)
                            };

                            if let (Some(s), Some(e), Some(st)) =
                                (start.as_i64(), end.as_i64(), step.as_i64())
                            {
                                let mut result = Vec::new();
                                let mut i = s;
                                while (st > 0 && i <= e) || (st < 0 && i >= e) {
                                    result.push(Value::Int(i));
                                    i += st;
                                }
                                return Ok(Value::List(result));
                            }
                        }
                        Ok(Value::List(vec![]))
                    }
                    "keys" => {
                        if args.len() == 1 {
                            let val = self.evaluate_expr_impl(&args[0], batch, row_idx)?;
                            if let Value::Map(map) = val {
                                let keys: Vec<Value> =
                                    map.keys().map(|k| Value::String(k.clone())).collect();
                                return Ok(Value::List(keys));
                            }
                        }
                        Ok(Value::List(vec![]))
                    }
                    _ => {
                        // Unsupported function - return empty list
                        Ok(Value::List(vec![]))
                    }
                }
            }

            // Unsupported expressions return null
            _ => Ok(Value::Null),
        }
    }

    /// Get a column value as JSON for a specific row.
    fn get_column_value(
        &self,
        batch: &RecordBatch,
        col_name: &str,
        row_idx: usize,
    ) -> DFResult<Value> {
        let col = batch.column_by_name(col_name).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Column '{}' not found for UNWIND",
                col_name
            ))
        })?;

        Ok(arrow_to_json_value(col.as_ref(), row_idx))
    }

    /// Build output batch from expansions.
    fn build_output_batch(
        &self,
        input: &RecordBatch,
        expansions: &[(usize, Value)],
    ) -> DFResult<RecordBatch> {
        if expansions.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_rows = expansions.len();

        // Build index array for take operation
        let indices: Vec<u64> = expansions.iter().map(|(idx, _)| *idx as u64).collect();
        let indices_array = UInt64Array::from(indices);

        // Expand input columns
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col in input.columns() {
            let expanded = take(col.as_ref(), &indices_array, None)?;
            columns.push(expanded);
        }

        // Add the unwind variable column
        // Values are JSON-encoded to preserve type information (numbers, booleans, etc.)
        let mut builder = StringBuilder::new();
        for (_, value) in expansions {
            if value.is_null() {
                builder.append_null();
            } else {
                // Serialize as JSON to preserve type (numbers stay as "1", strings as "\"hello\"")
                let json_val: serde_json::Value = value.clone().into();
                let json_str =
                    serde_json::to_string(&json_val).unwrap_or_else(|_| "null".to_string());
                builder.append_value(&json_str);
            }
        }
        columns.push(Arc::new(builder.finish()));

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for GraphUnwindStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = self.process_batch(batch);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for GraphUnwindStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Convert an Arrow array value at a specific row to `uni_common::Value`.
pub(crate) fn arrow_to_json_value(array: &dyn Array, row: usize) -> Value {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, ListArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };

    if array.is_null(row) {
        return Value::Null;
    }

    let any = array.as_any();

    // String types
    if let Some(arr) = any.downcast_ref::<StringArray>() {
        return Value::String(arr.value(row).to_string());
    }
    if let Some(arr) = any.downcast_ref::<LargeStringArray>() {
        return Value::String(arr.value(row).to_string());
    }

    // Integer types - use a macro to reduce repetition
    macro_rules! try_int {
        ($arr_type:ty) => {
            if let Some(arr) = any.downcast_ref::<$arr_type>() {
                return Value::Int(arr.value(row) as i64);
            }
        };
    }
    try_int!(Int64Array);
    try_int!(Int32Array);
    try_int!(Int16Array);
    try_int!(Int8Array);
    try_int!(UInt32Array);
    try_int!(UInt16Array);
    try_int!(UInt8Array);

    // UInt64 needs special handling to avoid overflow
    if let Some(arr) = any.downcast_ref::<UInt64Array>() {
        return Value::Int(arr.value(row) as i64);
    }

    // Float types
    if let Some(arr) = any.downcast_ref::<Float64Array>() {
        return Value::Float(arr.value(row));
    }
    if let Some(arr) = any.downcast_ref::<Float32Array>() {
        return Value::Float(arr.value(row) as f64);
    }

    // Boolean
    if let Some(arr) = any.downcast_ref::<BooleanArray>() {
        return Value::Bool(arr.value(row));
    }

    // List (recursive)
    if let Some(arr) = any.downcast_ref::<ListArray>() {
        let values = arr.value(row);
        let result: Vec<Value> = (0..values.len())
            .map(|i| arrow_to_json_value(values.as_ref(), i))
            .collect();
        return Value::List(result);
    }

    // Fallback
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_build_schema() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("n._vid", DataType::UInt64, false),
            Field::new("n.name", DataType::Utf8, true),
        ]));

        let output_schema = GraphUnwindExec::build_schema(input_schema, "item");

        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "n._vid");
        assert_eq!(output_schema.field(1).name(), "n.name");
        assert_eq!(output_schema.field(2).name(), "item");
    }

    #[test]
    fn test_evaluate_literal_list() {
        use arrow_array::builder::UInt64Builder;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        // Create a simple batch
        let mut vid_builder = UInt64Builder::new();
        vid_builder.append_value(1);

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "n._vid",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(vid_builder.finish())],
        )
        .unwrap();

        // Create a schema for the empty input stream
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        // Create empty input stream using RecordBatchStreamAdapter
        let empty_stream = RecordBatchStreamAdapter::new(input_schema, futures::stream::empty());

        // Create stream with literal list expression
        let stream = GraphUnwindStream {
            input: Box::pin(empty_stream),
            expr: Expr::List(vec![
                Expr::Literal(CypherLiteral::Integer(1)),
                Expr::Literal(CypherLiteral::Integer(2)),
                Expr::Literal(CypherLiteral::Integer(3)),
            ]),
            _variable: "x".to_string(),
            params: HashMap::new(),
            schema: Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("x", DataType::Utf8, true),
            ])),
            metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        };

        let result = stream.evaluate_expr_for_row(&batch, 0).unwrap();
        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], Value::Int(1));
                assert_eq!(items[1], Value::Int(2));
                assert_eq!(items[2], Value::Int(3));
            }
            _ => panic!("Expected list"),
        }
    }
}
