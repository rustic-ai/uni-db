// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

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
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
};
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
use uni_cypher::ast::{CypherLiteral, Expr};

/// Result of UNWIND element type inference.
struct ElementTypeInfo {
    /// Arrow data type for the unwind variable column.
    data_type: DataType,
    /// Whether values need JSON encoding metadata.
    is_cv_encoded: bool,
}

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
        let schema = Self::build_schema(input.schema(), &variable, &expr);
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

    /// Infer the native Arrow `DataType` for the elements of an UNWIND expression.
    ///
    /// For literal lists with homogeneous element types (ignoring nulls), returns
    /// the native type. For heterogeneous or non-inferrable expressions, falls back
    /// to JSON-encoded Utf8.
    fn infer_element_type(expr: &Expr) -> ElementTypeInfo {
        let json_fallback = || ElementTypeInfo {
            data_type: DataType::LargeBinary,
            is_cv_encoded: true,
        };

        let Expr::List(items) = expr else {
            return json_fallback();
        };

        // Infer type from first non-null literal
        let first_type = items.iter().find_map(|item| match item {
            Expr::Literal(CypherLiteral::Null) => None,
            Expr::Literal(CypherLiteral::Bool(_)) => Some(DataType::Boolean),
            Expr::Literal(CypherLiteral::Integer(_)) => Some(DataType::Int64),
            Expr::Literal(CypherLiteral::Float(_)) => Some(DataType::Float64),
            Expr::Literal(CypherLiteral::String(_)) => Some(DataType::Utf8),
            _ => Some(DataType::Utf8), // Sentinel for non-literal: forces fallback
        });

        let Some(expected) = first_type else {
            return json_fallback(); // All nulls or empty
        };

        // Verify all remaining non-null items match the expected type
        let all_match = items.iter().all(|item| match item {
            Expr::Literal(CypherLiteral::Null) => true,
            Expr::Literal(CypherLiteral::Bool(_)) => expected == DataType::Boolean,
            Expr::Literal(CypherLiteral::Integer(_)) => expected == DataType::Int64,
            Expr::Literal(CypherLiteral::Float(_)) => expected == DataType::Float64,
            Expr::Literal(CypherLiteral::String(_)) => expected == DataType::Utf8,
            _ => false, // Non-literal
        });

        if all_match {
            ElementTypeInfo {
                data_type: expected,
                is_cv_encoded: false,
            }
        } else {
            json_fallback()
        }
    }

    /// Build output schema by adding the unwind variable column.
    ///
    /// Uses type inference on the UNWIND expression to emit natively-typed
    /// columns when possible. Falls back to JSON-encoded `Utf8` for
    /// heterogeneous or non-inferrable expressions.
    fn build_schema(input_schema: SchemaRef, variable: &str, expr: &Expr) -> SchemaRef {
        let mut fields: Vec<Arc<Field>> = input_schema.fields().to_vec();

        let type_info = Self::infer_element_type(expr);

        let mut field = Field::new(variable, type_info.data_type, true);
        if type_info.is_cv_encoded {
            field = field.with_metadata(HashMap::from([("cv_encoded".into(), "true".into())]));
        }
        fields.push(Arc::new(field));

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphUnwindExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphUnwindExec: {} AS {}",
            self.expr.to_string_repr(),
            self.variable
        )
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
        Arc::clone(&self.schema)
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
            Arc::clone(&children[0]),
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
            params: self.params.clone(),
            schema: Arc::clone(&self.schema),
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
                // Try looking up as column name first: var.prop
                if let Expr::Variable(var_name) = base_expr.as_ref() {
                    let col_name = format!("{}.{}", var_name, prop_name);
                    if batch.schema().column_with_name(&col_name).is_some() {
                        return self.get_column_value(batch, &col_name, row_idx);
                    }
                }

                // Fall back to evaluating base as a map
                let base_value = self.evaluate_expr_impl(base_expr, batch, row_idx)?;
                if let Value::Map(map) = base_value {
                    Ok(map.get(prop_name).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Null)
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
                                // Use _all_props sub-map for schemaless entities
                                // when present; otherwise use the top-level map.
                                let source = match map.get("_all_props") {
                                    Some(Value::Map(all)) => all,
                                    _ => &map,
                                };
                                let mut key_strings: Vec<String> = source
                                    .iter()
                                    .filter(|(k, v)| !v.is_null() && !k.starts_with('_'))
                                    .map(|(k, _)| k.clone())
                                    .collect();
                                key_strings.sort();
                                let keys: Vec<Value> =
                                    key_strings.into_iter().map(Value::String).collect();
                                return Ok(Value::List(keys));
                            }
                            if val.is_null() {
                                return Ok(Value::Null);
                            }
                        }
                        Ok(Value::List(vec![]))
                    }
                    "size" | "length" => {
                        if args.len() == 1 {
                            let val = self.evaluate_expr_impl(&args[0], batch, row_idx)?;
                            let sz = match &val {
                                Value::List(arr) => arr.len() as i64,
                                Value::String(s) => s.len() as i64,
                                Value::Map(m) => m.len() as i64,
                                _ => 0,
                            };
                            return Ok(Value::Int(sz));
                        }
                        Ok(Value::Null)
                    }
                    // Temporal constructors: date(), time(), localtime(), datetime(), localdatetime(), duration()
                    "date" | "time" | "localtime" | "datetime" | "localdatetime" | "duration" => {
                        let mut eval_args = Vec::with_capacity(args.len());
                        for arg in args {
                            eval_args.push(self.evaluate_expr_impl(arg, batch, row_idx)?);
                        }
                        crate::query::datetime::eval_datetime_function(
                            &name.to_uppercase(),
                            &eval_args,
                        )
                        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
                    }
                    "split" => {
                        let mut eval_args = Vec::with_capacity(args.len());
                        for arg in args {
                            eval_args.push(self.evaluate_expr_impl(arg, batch, row_idx)?);
                        }
                        crate::query::expr_eval::eval_split(&eval_args).map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })
                    }
                    _ => {
                        // Unsupported function - return empty list
                        Ok(Value::List(vec![]))
                    }
                }
            }

            // Binary operations: e.g. size(types) - 1
            Expr::BinaryOp { left, op, right } => {
                let l = self.evaluate_expr_impl(left, batch, row_idx)?;
                let r = self.evaluate_expr_impl(right, batch, row_idx)?;
                crate::query::expr_eval::eval_binary_op(&l, op, &r)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
            }

            // Map literal: {a: 1, b: 'x'}
            Expr::Map(entries) => {
                let mut map = HashMap::new();
                for (key, val_expr) in entries {
                    let val = self.evaluate_expr_impl(val_expr, batch, row_idx)?;
                    map.insert(key.clone(), val);
                }
                Ok(Value::Map(map))
            }

            // Array index: qrows[p]
            Expr::ArrayIndex { array, index } => {
                let arr_val = self.evaluate_expr_impl(array, batch, row_idx)?;
                let idx_val = self.evaluate_expr_impl(index, batch, row_idx)?;
                match (&arr_val, idx_val.as_i64()) {
                    (Value::List(list), Some(i)) => {
                        // Cypher uses 0-based indexing; negative indices count from end
                        let len = list.len() as i64;
                        let resolved = if i < 0 { len + i } else { i };
                        if resolved >= 0 && (resolved as usize) < list.len() {
                            Ok(list[resolved as usize].clone())
                        } else {
                            Ok(Value::Null)
                        }
                    }
                    _ => Ok(Value::Null),
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
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
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

        // Add the unwind variable column using the appropriate typed builder
        let unwind_field = self.schema.field(self.schema.fields().len() - 1);
        let is_cv_encoded = unwind_field
            .metadata()
            .get("cv_encoded")
            .is_some_and(|v| v == "true");

        let unwind_col: ArrayRef = match (unwind_field.data_type(), is_cv_encoded) {
            (DataType::Boolean, false) => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for (_, value) in expansions {
                    if let Value::Bool(b) = value {
                        builder.append_value(*b);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::Int64, false) => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for (_, value) in expansions {
                    if let Value::Int(i) = value {
                        builder.append_value(*i);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::Float64, false) => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for (_, value) in expansions {
                    if let Value::Float(f) = value {
                        builder.append_value(*f);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::Utf8, false) => {
                let mut builder = StringBuilder::new();
                for (_, value) in expansions {
                    if let Value::String(s) = value {
                        builder.append_value(s);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::LargeBinary, _) => {
                // CypherValue-encoded: preserves exact types through UNWIND
                let mut builder = LargeBinaryBuilder::with_capacity(num_rows, num_rows * 16);
                for (_, value) in expansions {
                    if value.is_null() {
                        builder.append_null();
                    } else {
                        let encoded = uni_common::cypher_value_codec::encode(value);
                        builder.append_value(&encoded);
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Fallback: JSON-encoded Utf8 (heterogeneous or non-inferrable types)
                let mut builder = StringBuilder::new();
                for (_, value) in expansions {
                    if value.is_null() {
                        builder.append_null();
                    } else {
                        let json_val: serde_json::Value = value.clone().into();
                        let json_str =
                            serde_json::to_string(&json_val).unwrap_or_else(|_| "null".to_string());
                        builder.append_value(&json_str);
                    }
                }
                Arc::new(builder.finish())
            }
        };
        columns.push(unwind_col);

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
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
            other => other,
        }
    }
}

impl RecordBatchStream for GraphUnwindStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
    try_int!(UInt64Array);
    try_int!(UInt32Array);
    try_int!(UInt16Array);
    try_int!(UInt8Array);

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

    // LargeBinary (CypherValue) — decode to Value
    if let Some(arr) = any.downcast_ref::<arrow_array::LargeBinaryArray>() {
        let bytes = arr.value(row);
        if let Ok(uni_val) = uni_common::cypher_value_codec::decode(bytes) {
            return uni_val;
        }
        // Fallback: try plain JSON text
        if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(bytes) {
            return Value::from(parsed);
        }
        return Value::Null;
    }

    // Struct — convert fields to a Map so keys()/properties() UDFs work
    if let Some(s) = any.downcast_ref::<arrow_array::StructArray>() {
        let mut map = HashMap::new();
        for (field, child) in s.fields().iter().zip(s.columns()) {
            map.insert(
                field.name().clone(),
                arrow_to_json_value(child.as_ref(), row),
            );
        }
        return Value::Map(map);
    }

    // Fallback
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{LargeBinaryArray, UInt64Array};
    use uni_cypher::ast::CypherLiteral;

    #[test]
    fn test_build_schema() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("n._vid", DataType::UInt64, false),
            Field::new("n.name", DataType::Utf8, true),
        ]));

        // Variable reference -> falls back to JSON-encoded Utf8
        let expr = Expr::Variable("some_list".to_string());
        let output_schema = GraphUnwindExec::build_schema(input_schema, "item", &expr);

        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "n._vid");
        assert_eq!(output_schema.field(1).name(), "n.name");
        assert_eq!(output_schema.field(2).name(), "item");
        assert_eq!(output_schema.field(2).data_type(), &DataType::LargeBinary);
        assert_eq!(
            output_schema
                .field(2)
                .metadata()
                .get("cv_encoded")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn test_build_schema_boolean_list() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Bool(true)),
            Expr::Literal(CypherLiteral::Bool(false)),
            Expr::Literal(CypherLiteral::Null),
        ]);
        let output_schema = GraphUnwindExec::build_schema(input_schema, "a", &expr);

        let field = output_schema.field(1);
        assert_eq!(field.name(), "a");
        assert_eq!(field.data_type(), &DataType::Boolean);
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn test_build_schema_integer_list() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Integer(1)),
            Expr::Literal(CypherLiteral::Integer(2)),
            Expr::Literal(CypherLiteral::Integer(3)),
        ]);
        let output_schema = GraphUnwindExec::build_schema(input_schema, "x", &expr);

        let field = output_schema.field(1);
        assert_eq!(field.name(), "x");
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn test_build_schema_float_list() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Float(1.5)),
            Expr::Literal(CypherLiteral::Float(2.5)),
        ]);
        let output_schema = GraphUnwindExec::build_schema(input_schema, "x", &expr);

        let field = output_schema.field(1);
        assert_eq!(field.name(), "x");
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn test_build_schema_string_list() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::String("hello".to_string())),
            Expr::Literal(CypherLiteral::String("world".to_string())),
        ]);
        let output_schema = GraphUnwindExec::build_schema(input_schema, "x", &expr);

        let field = output_schema.field(1);
        assert_eq!(field.name(), "x");
        assert_eq!(field.data_type(), &DataType::Utf8);
        // Plain string, no cv_encoded metadata
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn test_build_schema_mixed_list() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Integer(1)),
            Expr::Literal(CypherLiteral::String("hello".to_string())),
        ]);
        let output_schema = GraphUnwindExec::build_schema(input_schema, "x", &expr);

        let field = output_schema.field(1);
        assert_eq!(field.name(), "x");
        assert_eq!(field.data_type(), &DataType::LargeBinary);
        assert_eq!(
            field.metadata().get("cv_encoded").map(String::as_str),
            Some("true")
        );
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

    #[test]
    fn test_evaluate_map_literal() {
        use arrow_array::builder::UInt64Builder;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

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

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let empty_stream = RecordBatchStreamAdapter::new(input_schema, futures::stream::empty());

        let stream = GraphUnwindStream {
            input: Box::pin(empty_stream),
            expr: Expr::Map(vec![
                ("a".to_string(), Expr::Literal(CypherLiteral::Integer(1))),
                (
                    "b".to_string(),
                    Expr::Literal(CypherLiteral::String("hello".to_string())),
                ),
            ]),
            params: HashMap::new(),
            schema: Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("x", DataType::LargeBinary, true),
            ])),
            metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        };

        let result = stream.evaluate_expr_for_row(&batch, 0).unwrap();
        match result {
            Value::Map(map) => {
                assert_eq!(map.get("a"), Some(&Value::Int(1)));
                assert_eq!(map.get("b"), Some(&Value::String("hello".to_string())));
            }
            _ => panic!("Expected Map, got {:?}", result),
        }
    }

    #[test]
    fn test_evaluate_map_property_access() {
        use arrow_array::builder::UInt64Builder;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

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

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));

        let empty_stream = RecordBatchStreamAdapter::new(input_schema, futures::stream::empty());

        // Test: {a: 1, b: 'x'}.a should return 1
        let map_expr = Expr::Map(vec![
            ("a".to_string(), Expr::Literal(CypherLiteral::Integer(1))),
            (
                "b".to_string(),
                Expr::Literal(CypherLiteral::String("x".to_string())),
            ),
        ]);
        let prop_expr = Expr::Property(Box::new(map_expr), "a".to_string());

        let stream = GraphUnwindStream {
            input: Box::pin(empty_stream),
            expr: prop_expr.clone(),
            params: HashMap::new(),
            schema: Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("x", DataType::LargeBinary, true),
            ])),
            metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        };

        let result = stream.evaluate_expr_impl(&prop_expr, &batch, 0).unwrap();
        assert_eq!(result, Value::Int(1));
    }

    #[test]
    fn test_arrow_to_json_value_uint64_is_coerced_to_int() {
        let arr = UInt64Array::from(vec![Some(42u64)]);
        let value = arrow_to_json_value(&arr, 0);
        assert_eq!(value, Value::Int(42));
    }

    #[test]
    fn test_arrow_to_json_value_largebinary_decodes_cypher_map() {
        let encoded = uni_common::cypher_value_codec::encode(&Value::Map(HashMap::new()));
        let arr = LargeBinaryArray::from(vec![Some(encoded.as_slice())]);
        let value = arrow_to_json_value(&arr, 0);
        assert_eq!(value, Value::Map(HashMap::new()));
    }
}
