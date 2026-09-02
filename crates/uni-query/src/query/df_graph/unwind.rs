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

use crate::query::df_graph::common::{arrow_err, compute_plan_properties, exec_err};
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

    /// Input column indices carried into the output, in order. Anything not
    /// listed belongs to a consumed UNWIND source and is dropped.
    kept: Vec<usize>,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

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
        Self::new_dropping_source(input, expr, variable, params, None)
    }

    /// [`Self::new`], additionally dropping the source variable's columns from
    /// the output when the planner has proven nothing above reads them.
    ///
    /// `UNWIND xs AS x` consumes `xs`, but every input column is copied
    /// forward — and copied again *per fan-out row* by any traversal above. A
    /// collected list of *n* entities is therefore re-materialised `rows × n`
    /// times in the stream's `build_output_batch` `take` over the input columns,
    /// which is the allocation that aborts the process on LDBC IC6/IC9 at SF1
    /// (#184).
    /// Dropping the column here is the whole fix: the list has already been
    /// expanded into rows, so nothing downstream can want it back.
    ///
    /// Liveness is decided in the planner, not here — see
    /// `mark_dead_unwind_sources`, which refuses the rewrite for `RETURN *`,
    /// for a list unwound more than once, and for a non-variable source.
    pub fn new_dropping_source(
        input: Arc<dyn ExecutionPlan>,
        expr: Expr,
        variable: impl Into<String>,
        params: HashMap<String, Value>,
        drop_source: Option<&str>,
    ) -> Self {
        let variable = variable.into();
        let input_schema = input.schema();

        // Input column indices that survive into the output. The bare column
        // and any dotted columns the variable owns both go, so an entity list
        // does not leave `xs._vid` behind.
        let kept: Vec<usize> = (0..input_schema.fields().len())
            .filter(|&i| {
                let name = input_schema.field(i).name();
                drop_source.is_none_or(|src| name != src && !name.starts_with(&format!("{src}.")))
            })
            .collect();

        let schema = Self::build_schema(&input_schema, &kept, &variable, &expr);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            expr,
            variable,
            params,
            schema,
            kept,
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
    fn build_schema(
        input_schema: &SchemaRef,
        kept: &[usize],
        variable: &str,
        expr: &Expr,
    ) -> SchemaRef {
        let mut fields: Vec<Arc<Field>> = kept
            .iter()
            .map(|&i| Arc::clone(&input_schema.fields()[i]))
            .collect();

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

    fn properties(&self) -> &Arc<PlanProperties> {
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
        // Read the configured batch size before the context is handed to the
        // input: it is what bounds the output chunks below.
        let chunk_size = context.session_config().batch_size().max(1);
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(GraphUnwindStream {
            input: input_stream,
            expr: self.expr.clone(),
            params: self.params.clone(),
            schema: Arc::clone(&self.schema),
            kept: self.kept.clone(),
            chunk_size,
            pending: None,
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

    /// Input column indices carried into the output; see `GraphUnwindExec`.
    kept: Vec<usize>,

    /// Rows per output batch. One input batch is emitted as however many
    /// batches this takes, which is what bounds the peak allocation — see
    /// [`GraphUnwindStream::fill_chunk`].
    chunk_size: usize,

    /// An input batch part way through its expansion, carried across polls.
    pending: Option<Pending>,

    /// Metrics.
    metrics: BaselineMetrics,
}

/// An input batch mid-expansion.
struct Pending {
    /// The batch being expanded; its columns are `take`n per output chunk.
    batch: RecordBatch,

    /// Index of the next input row to evaluate.
    row: usize,

    /// Elements of `row - 1`'s list not yet emitted, and that row's index.
    /// A list longer than `chunk_size` spans several chunks, so the position
    /// within it has to survive the poll that emitted the last one.
    rest: Option<(usize, std::vec::IntoIter<Value>)>,
}

impl GraphUnwindStream {
    /// Take up to `chunk_size` expansions from `pending`, in input order.
    ///
    /// Expansion is driven row by row rather than materialised up front: the
    /// whole-batch form accumulated `input_rows × list_size` owned values and
    /// then `take`-replicated every carried column over all of them in one
    /// allocation, which is what reached tens of gigabytes against a 1 GiB
    /// query-memory pool at LDBC SF1 (issue #198). Peak is now
    /// `chunk_size × columns` plus the one list currently being expanded.
    ///
    /// Row order is unchanged — rows are still visited in index order and
    /// elements in list order; only the batch boundary moves.
    ///
    /// What this does *not* bound: a single row's list is still materialised
    /// whole, because expression evaluation is row-at-a-time and hands back an
    /// owned [`Value::List`]. The shape that reached those figures is
    /// `rows × list_size`; one row is bounded by what that row itself holds.
    /// Whether these allocations should reserve through the memory pool at all
    /// is the separate question #198 leaves open.
    ///
    /// Returns the expansions and whether the batch is now exhausted. An empty
    /// return with `true` means the batch produced no rows at all.
    fn fill_chunk(&self, pending: &mut Pending) -> DFResult<(Vec<(usize, Value)>, bool)> {
        let mut expansions: Vec<(usize, Value)> = Vec::new(); // (input_row_idx, list_element)

        loop {
            if let Some((row_idx, items)) = pending.rest.as_mut() {
                let row_idx = *row_idx;
                for item in items.by_ref() {
                    expansions.push((row_idx, item));
                    if expansions.len() >= self.chunk_size {
                        return Ok((expansions, false));
                    }
                }
                pending.rest = None;
            }

            if pending.row >= pending.batch.num_rows() {
                return Ok((expansions, true));
            }

            let row_idx = pending.row;
            pending.row += 1;
            match self.evaluate_expr_for_row(&pending.batch, row_idx)? {
                Value::List(items) => {
                    pending.rest = Some((row_idx, items.into_iter()));
                }
                Value::Null => {
                    // UNWIND on null produces no rows (Cypher semantics)
                }
                other => {
                    // Non-list values: treat as single-element list
                    expansions.push((row_idx, other));
                    if expansions.len() >= self.chunk_size {
                        return Ok((expansions, false));
                    }
                }
            }
        }
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

                            // Cypher null-propagation: any null bound yields null.
                            if start.is_null() || end.is_null() || step.is_null() {
                                return Ok(Value::Null);
                            }

                            // range() requires integer arguments. `as_i64` returns
                            // None for floats, so a non-integer numeric (or any
                            // non-numeric) is a type error — NOT a silent empty list
                            // (openCypher requires an error). (review H5)
                            let (s, e, st) = match (start.as_i64(), end.as_i64(), step.as_i64()) {
                                (Some(s), Some(e), Some(st)) => (s, e, st),
                                _ => {
                                    return Err(datafusion::error::DataFusionError::Execution(
                                        format!(
                                            "range() requires integer arguments, got start={start:?}, end={end:?}, step={step:?}"
                                        ),
                                    ));
                                }
                            };
                            if st == 0 {
                                return Err(datafusion::error::DataFusionError::Execution(
                                    "range() step argument cannot be 0".to_string(),
                                ));
                            }
                            let mut result = Vec::new();
                            let mut i = s;
                            while (st > 0 && i <= e) || (st < 0 && i >= e) {
                                result.push(Value::Int(i));
                                // Checked step: stop at the i64 boundary instead of
                                // panicking (debug) or wrapping into an infinite loop
                                // (release). (review H5)
                                match i.checked_add(st) {
                                    Some(next) => i = next,
                                    None => break,
                                }
                            }
                            return Ok(Value::List(result));
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
                                // Unicode character count, not byte length:
                                // openCypher size('héllo') == 5, not 6. (review H6)
                                Value::String(s) => s.chars().count() as i64,
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
                        .map_err(exec_err)
                    }
                    "split" => {
                        let mut eval_args = Vec::with_capacity(args.len());
                        for arg in args {
                            eval_args.push(self.evaluate_expr_impl(arg, batch, row_idx)?);
                        }
                        crate::query::expr_eval::eval_split(&eval_args).map_err(exec_err)
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
                crate::query::expr_eval::eval_binary_op(&l, op, &r).map_err(exec_err)
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
        // Expand only the surviving input columns. Skipping a dropped column
        // skips its `take`, which is the point: that `take` is what replicated
        // a whole collected list onto every fan-out row.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.kept.len() + 1);
        for &i in &self.kept {
            let expanded = take(input.column(i).as_ref(), &indices_array, None)?;
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

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)
    }
}

impl Stream for GraphUnwindStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(mut pending) = self.pending.take() {
                let metrics = self.metrics.clone();
                let _timer = metrics.elapsed_compute().timer();
                let (expansions, exhausted) = match self.fill_chunk(&mut pending) {
                    Ok(v) => v,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                if expansions.is_empty() {
                    // Nothing survived this batch; ask the input for the next
                    // one rather than emitting an empty batch. (`fill_chunk`
                    // only stops early with a full chunk, so this implies
                    // `exhausted`, but do not rely on that here.)
                    if !exhausted {
                        self.pending = Some(pending);
                    }
                    continue;
                }
                let result = self.build_output_batch(&pending.batch, &expansions);
                if !exhausted {
                    self.pending = Some(pending);
                }
                return Poll::Ready(Some(result));
            }

            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.pending = Some(Pending {
                        batch,
                        row: 0,
                        rest: None,
                    });
                }
                other => return other,
            }
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
        let output_schema = GraphUnwindExec::build_schema(
            &input_schema,
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            "item",
            &expr,
        );

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
        let output_schema = GraphUnwindExec::build_schema(
            &input_schema,
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            "a",
            &expr,
        );

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
        let output_schema = GraphUnwindExec::build_schema(
            &input_schema,
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            "x",
            &expr,
        );

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
        let output_schema = GraphUnwindExec::build_schema(
            &input_schema,
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            "x",
            &expr,
        );

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
        let output_schema = GraphUnwindExec::build_schema(
            &input_schema,
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            "x",
            &expr,
        );

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
        let output_schema = GraphUnwindExec::build_schema(
            &input_schema,
            &(0..input_schema.fields().len()).collect::<Vec<_>>(),
            "x",
            &expr,
        );

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
            // These fixtures feed a one-column input and keep it.
            kept: vec![0],
            schema: Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("x", DataType::Utf8, true),
            ])),
            chunk_size: 8192,
            pending: None,
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
            // These fixtures feed a one-column input and keep it.
            kept: vec![0],
            schema: Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("x", DataType::LargeBinary, true),
            ])),
            chunk_size: 8192,
            pending: None,
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
            // These fixtures feed a one-column input and keep it.
            kept: vec![0],
            schema: Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("x", DataType::LargeBinary, true),
            ])),
            chunk_size: 8192,
            pending: None,
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

    /// Evaluate a scalar function-call expression (e.g. `range(...)`, `size(...)`)
    /// over a one-row batch — the harness for the H5/H6 regression tests.
    fn eval_scalar_fn(name: &str, args: Vec<Expr>) -> DFResult<Value> {
        use arrow_array::builder::UInt64Builder;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let mut vid_builder = UInt64Builder::new();
        vid_builder.append_value(1);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "n._vid",
            DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(vid_builder.finish())]).unwrap();
        let empty_stream = RecordBatchStreamAdapter::new(schema.clone(), futures::stream::empty());

        let expr = Expr::FunctionCall {
            name: name.to_string(),
            args,
            distinct: false,
            window_spec: None,
        };
        let stream = GraphUnwindStream {
            input: Box::pin(empty_stream),
            expr: expr.clone(),
            params: HashMap::new(),
            // This harness only evaluates an expression; no column is dropped.
            kept: (0..schema.fields().len()).collect(),
            schema,
            chunk_size: 8192,
            pending: None,
            metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        };
        stream.evaluate_expr_impl(&expr, &batch, 0)
    }

    fn int_lit(v: i64) -> Expr {
        Expr::Literal(CypherLiteral::Integer(v))
    }

    /// H5: `i += st` near i64::MAX must terminate at the boundary, not panic
    /// (debug) or wrap into an infinite loop (release).
    #[test]
    fn test_range_overflow_terminates() {
        let result = eval_scalar_fn(
            "range",
            vec![int_lit(i64::MAX - 1), int_lit(i64::MAX), int_lit(2)],
        )
        .unwrap();
        // Only i64::MAX-1 fits; the next step would overflow and is not emitted.
        assert_eq!(result, Value::List(vec![Value::Int(i64::MAX - 1)]));
    }

    /// H5: a zero step is an error, not an infinite loop.
    #[test]
    fn test_range_zero_step_errors() {
        let err = eval_scalar_fn("range", vec![int_lit(1), int_lit(5), int_lit(0)]);
        assert!(err.is_err(), "range(1, 5, 0) must error, got {err:?}");
    }

    /// H5: float bounds are a type error (openCypher), NOT a silent empty list.
    #[test]
    fn test_range_float_args_error() {
        let err = eval_scalar_fn(
            "range",
            vec![
                Expr::Literal(CypherLiteral::Float(1.0)),
                Expr::Literal(CypherLiteral::Float(3.0)),
            ],
        );
        assert!(err.is_err(), "range(1.0, 3.0) must error, got {err:?}");
    }

    /// H6: size()/length() of a multi-byte string counts characters, not bytes.
    #[test]
    fn test_size_string_counts_chars_not_bytes() {
        let result = eval_scalar_fn(
            "size",
            vec![Expr::Literal(CypherLiteral::String("héllo".to_string()))],
        )
        .unwrap();
        // 5 chars, but 6 UTF-8 bytes — must be 5.
        assert_eq!(result, Value::Int(5));
    }

    /// Drive the stream to completion, returning `(batch sizes, rows)` where a
    /// row is `(carried vid, unwound value)`.
    async fn drive(
        input: Vec<RecordBatch>,
        expr: Expr,
        out_field: Field,
        chunk_size: usize,
    ) -> (Vec<usize>, Vec<(u64, Value)>) {
        use arrow_array::UInt64Array;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let input_schema = input[0].schema();
        let schema = Arc::new(Schema::new(vec![
            input_schema.field(0).clone(),
            out_field.clone(),
        ]));
        let adapter = RecordBatchStreamAdapter::new(
            Arc::clone(&input_schema),
            futures::stream::iter(input.into_iter().map(Ok)),
        );
        let stream = GraphUnwindStream {
            input: Box::pin(adapter),
            expr,
            params: HashMap::new(),
            // Only the vid column is carried; the last output field is the
            // unwound variable, which the operator appends itself.
            kept: vec![0],
            schema,
            chunk_size,
            pending: None,
            metrics: BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
        };

        let batches: Vec<RecordBatch> = stream.map(|b| b.unwrap()).collect().await;
        let sizes = batches.iter().map(|b| b.num_rows()).collect();
        let mut rows = Vec::new();
        for b in &batches {
            assert!(b.num_rows() > 0, "an empty batch must not be emitted");
            let vids = b
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("vid column");
            for i in 0..b.num_rows() {
                rows.push((vids.value(i), arrow_to_json_value(b.column(1).as_ref(), i)));
            }
        }
        (sizes, rows)
    }

    fn vid_batch(vids: &[u64]) -> RecordBatch {
        use arrow_array::UInt64Array;
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "n._vid",
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(vids.to_vec()))],
        )
        .unwrap()
    }

    /// Chunking changes where the batch boundaries fall and nothing else: the
    /// same rows, in the same order, carrying the same input columns (#198).
    #[tokio::test]
    async fn expansion_is_chunked_without_changing_rows_or_order() {
        let expr = Expr::List(vec![
            Expr::Literal(CypherLiteral::Integer(1)),
            Expr::Literal(CypherLiteral::Integer(2)),
            Expr::Literal(CypherLiteral::Integer(3)),
        ]);
        let field = Field::new("x", DataType::Int64, true);

        // Three input rows × a three-element list = nine output rows.
        let expected: Vec<(u64, Value)> = [10u64, 20, 30]
            .iter()
            .flat_map(|&v| (1..=3).map(move |i| (v, Value::Int(i))))
            .collect();

        let (whole, rows) = drive(
            vec![vid_batch(&[10, 20, 30])],
            expr.clone(),
            field.clone(),
            8192,
        )
        .await;
        assert_eq!(whole, vec![9], "one chunk when the budget fits the batch");
        assert_eq!(rows, expected);

        // A chunk boundary that lands mid-list is the case the cursor exists
        // for: 2 does not divide 3.
        let (sizes, rows) = drive(vec![vid_batch(&[10, 20, 30])], expr, field, 2).await;
        assert_eq!(sizes, vec![2, 2, 2, 2, 1]);
        assert_eq!(rows, expected, "chunking must not reorder or drop rows");
    }

    /// A row that expands to nothing must not desynchronise the cursor, and
    /// must not cause an empty batch to be emitted.
    #[tokio::test]
    async fn rows_that_expand_to_nothing_are_skipped() {
        use arrow_array::builder::LargeBinaryBuilder;
        use uni_common::cypher_value_codec::encode;

        // `xs` per row: empty list, two elements, null, one element.
        let sources = [
            Value::List(vec![]),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
            Value::Null,
            Value::List(vec![Value::Int(3)]),
        ];
        let mut xs = LargeBinaryBuilder::new();
        for v in &sources {
            if v.is_null() {
                xs.append_null();
            } else {
                xs.append_value(encode(v));
            }
        }
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n._vid", DataType::UInt64, false),
                Field::new("xs", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(arrow_array::UInt64Array::from(vec![10u64, 20, 30, 40])),
                Arc::new(xs.finish()),
            ],
        )
        .unwrap();

        let field = Field::new("x", DataType::Int64, true);
        let (sizes, rows) = drive(vec![batch], Expr::Variable("xs".to_string()), field, 8192).await;
        assert_eq!(sizes, vec![3]);
        assert_eq!(
            rows,
            vec![
                (20, Value::Int(1)),
                (20, Value::Int(2)),
                (40, Value::Int(3)),
            ]
        );

        // Every row expanding to nothing must end the stream, not emit an
        // empty batch — `drive` asserts that for each batch it sees.
        let (sizes, rows) = drive(
            vec![vid_batch(&[10, 20])],
            Expr::Literal(CypherLiteral::Null),
            Field::new("x", DataType::Int64, true),
            8192,
        )
        .await;
        assert!(sizes.is_empty(), "got batches: {sizes:?}");
        assert!(rows.is_empty());
    }
}
