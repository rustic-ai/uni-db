// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! OPTIONAL MATCH filter with NULL row preservation.
//!
//! This module provides [`OptionalFilterExec`], a DataFusion [`ExecutionPlan`]
//! that applies a filter predicate while preserving OPTIONAL MATCH semantics:
//! when all matched rows for a source group are filtered out, the source row
//! is preserved with NULL values for all optional columns.
//!
//! # Problem
//!
//! Standard `FilterExec` removes rows that fail the predicate. For OPTIONAL
//! MATCH queries like:
//!
//! ```text
//! MATCH (n:Single) OPTIONAL MATCH (n)-[r]-(m) WHERE m:NonExistent RETURN r
//! ```
//!
//! The traverse finds matches (m=A, m=B), but WHERE filters all of them out.
//! A standard filter yields 0 rows. Cypher requires 1 row with r=NULL.
//!
//! # Algorithm
//!
//! 1. Consume each input batch
//! 2. Group rows by source VID columns (non-optional columns ending in `._vid`)
//! 3. Evaluate the filter predicate on the batch
//! 4. For each source group:
//!    - If at least one row passes the filter, emit those rows
//!    - If ALL rows fail the filter, emit one row with source columns preserved
//!      and optional columns set to NULL

use crate::query::df_graph::common::compute_plan_properties;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, new_null_array};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Filter with OPTIONAL MATCH NULL row preservation.
///
/// Applies a filter predicate but ensures that for each distinct source
/// row group, at least one output row is produced. When all matched rows
/// for a source group are filtered out, a single row with NULL optional
/// columns is emitted instead.
pub struct OptionalFilterExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Filter predicate to evaluate.
    predicate: Arc<dyn PhysicalExpr>,

    /// Variable names from the OPTIONAL MATCH pattern.
    ///
    /// Columns matching these variable prefixes (e.g., `m._vid`, `r._eid`)
    /// are set to NULL when the filter removes all matched rows for a
    /// source group.
    optional_variables: HashSet<String>,

    /// Output schema (same as input).
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for OptionalFilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OptionalFilterExec")
            .field("predicate", &self.predicate)
            .field("optional_variables", &self.optional_variables)
            .finish()
    }
}

impl OptionalFilterExec {
    /// Create a new optional filter execution plan.
    ///
    /// The `optional_variables` set determines which columns are nulled out
    /// when all rows for a source group are filtered. Columns whose name
    /// starts with `{var}.` for any var in `optional_variables` are treated
    /// as optional.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
        optional_variables: HashSet<String>,
    ) -> Self {
        let schema = input.schema();
        let properties = compute_plan_properties(Arc::clone(&schema));

        Self {
            input,
            predicate,
            optional_variables,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Check whether a column belongs to an optional variable.
    fn is_optional_column(&self, col_name: &str) -> bool {
        for var in &self.optional_variables {
            // Match columns like "m._vid", "m.name", "r._eid", "r._type"
            if col_name.starts_with(var.as_str()) && col_name[var.len()..].starts_with('.') {
                return true;
            }
            // Also match the bare variable column itself (struct column)
            if col_name == var.as_str() {
                return true;
            }
            // Match internal EID tracking columns like "__eid_to_m"
            if col_name.starts_with("__eid_to_") && col_name.ends_with(var.as_str()) {
                return true;
            }
        }
        false
    }
}

impl DisplayAs for OptionalFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "OptionalFilterExec: predicate={}, optional_vars=[{}]",
                    self.predicate,
                    self.optional_variables
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    }
}

impl ExecutionPlan for OptionalFilterExec {
    fn name(&self) -> &str {
        "OptionalFilterExec"
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
                "OptionalFilterExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.predicate),
            self.optional_variables.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Pre-compute which column indices are optional vs source.
        let mut source_vid_indices = Vec::new();
        let mut optional_col_indices = Vec::new();
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if self.is_optional_column(field.name()) {
                optional_col_indices.push(idx);
            } else if field.name().ends_with("._vid") {
                source_vid_indices.push(idx);
            }
        }

        Ok(Box::pin(OptionalFilterStream {
            input: input_stream,
            predicate: Arc::clone(&self.predicate),
            schema: Arc::clone(&self.schema),
            source_vid_indices,
            optional_col_indices,
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream implementing the optional filter logic.
struct OptionalFilterStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Filter predicate.
    predicate: Arc<dyn PhysicalExpr>,

    /// Output schema.
    schema: SchemaRef,

    /// Indices of source VID columns (used for grouping).
    source_vid_indices: Vec<usize>,

    /// Indices of optional columns (nulled for filtered-out groups).
    optional_col_indices: Vec<usize>,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl OptionalFilterStream {
    /// Process a single input batch with optional filter semantics.
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        // Evaluate the filter predicate.
        let filter_result = self.predicate.evaluate(&batch)?;
        let filter_array = filter_result.into_array(batch.num_rows())?;
        let filter_bools = filter_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Filter predicate did not return BooleanArray".to_string(),
                )
            })?;

        // If all rows pass, return the batch as-is.
        if filter_bools.true_count() == batch.num_rows() {
            self.metrics.record_output(batch.num_rows());
            return Ok(batch);
        }

        // Group rows by source VID values.
        // Key = serialized source VID values, Value = list of row indices.
        let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        let mut group_order: Vec<Vec<u8>> = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let key = self.compute_source_key(&batch, row_idx);
            let entry = groups.entry(key.clone());
            if matches!(entry, std::collections::hash_map::Entry::Vacant(_)) {
                group_order.push(key.clone());
            }
            entry.or_default().push(row_idx);
        }

        // For each group, check if any row passes the filter.
        let mut passed_indices: Vec<usize> = Vec::new();
        let mut null_row_indices: Vec<usize> = Vec::new(); // Row index to use for source cols

        for key in &group_order {
            let row_indices = &groups[key];
            let mut any_passed = false;

            for &row_idx in row_indices {
                if filter_bools.is_valid(row_idx) && filter_bools.value(row_idx) {
                    passed_indices.push(row_idx);
                    any_passed = true;
                }
            }

            if !any_passed {
                // No rows passed for this source group — emit a null row.
                // Use the first row of the group for source column values.
                null_row_indices.push(row_indices[0]);
            }
        }

        // Build output batch.
        let total_rows = passed_indices.len() + null_row_indices.len();
        if total_rows == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        let optional_set: HashSet<usize> = self.optional_col_indices.iter().copied().collect();

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);

            if optional_set.contains(&col_idx) {
                // Optional column: passed rows get real values, null rows get NULL.
                let passed_array = take_indices(col, &passed_indices)?;
                let null_array = new_null_array(field.data_type(), null_row_indices.len());
                let combined =
                    arrow::compute::concat(&[&*passed_array, &*null_array]).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                columns.push(combined);
            } else {
                // Source column: take values from both passed and null row indices.
                let all_indices: Vec<usize> = passed_indices
                    .iter()
                    .chain(null_row_indices.iter())
                    .copied()
                    .collect();
                let taken = take_indices(col, &all_indices)?;
                columns.push(taken);
            }
        }

        self.metrics.record_output(total_rows);

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Compute a grouping key from source VID column values for a row.
    fn compute_source_key(&self, batch: &RecordBatch, row_idx: usize) -> Vec<u8> {
        use arrow_array::UInt64Array;

        let mut key = Vec::with_capacity(self.source_vid_indices.len() * 8);
        for &col_idx in &self.source_vid_indices {
            let col = batch.column(col_idx);
            if let Some(vid_array) = col.as_any().downcast_ref::<UInt64Array>() {
                if vid_array.is_valid(row_idx) {
                    key.extend_from_slice(&vid_array.value(row_idx).to_le_bytes());
                } else {
                    // NULL VID — use sentinel.
                    key.extend_from_slice(&u64::MAX.to_le_bytes());
                }
            } else {
                // Non-UInt64 VID column (shouldn't happen) — use null sentinel.
                key.extend_from_slice(&u64::MAX.to_le_bytes());
            }
        }
        key
    }
}

/// Take elements from an array at the given indices.
fn take_indices(array: &ArrayRef, indices: &[usize]) -> DFResult<ArrayRef> {
    let idx_array =
        arrow_array::UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
    arrow::compute::take(array.as_ref(), &idx_array, None)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

impl Stream for OptionalFilterStream {
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

impl RecordBatchStream for OptionalFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_optional_column() {
        let optional_vars: HashSet<String> =
            ["m".to_string(), "r".to_string()].into_iter().collect();
        let exec = OptionalFilterExec::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                arrow_schema::Schema::empty(),
            ))),
            Arc::new(datafusion::physical_expr::expressions::Literal::new(
                datafusion::common::ScalarValue::Boolean(Some(true)),
            )),
            optional_vars,
        );

        assert!(exec.is_optional_column("m._vid"));
        assert!(exec.is_optional_column("m.name"));
        assert!(exec.is_optional_column("r._eid"));
        assert!(exec.is_optional_column("r._type"));
        assert!(exec.is_optional_column("r"));
        assert!(exec.is_optional_column("m"));
        assert!(exec.is_optional_column("__eid_to_m"));

        assert!(!exec.is_optional_column("n._vid"));
        assert!(!exec.is_optional_column("n.name"));
        assert!(!exec.is_optional_column("x._vid"));
    }
}
