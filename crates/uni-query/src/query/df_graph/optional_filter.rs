// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

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
//! 2. Group rows by source VID columns (non-optional columns with `._vid`
//!    suffix OR struct columns containing a `_vid` field)
//! 3. Evaluate the filter predicate on the batch
//! 4. For each source group:
//!    - If at least one row passes the filter, emit those rows
//!    - If ALL rows fail the filter, emit one row with source columns preserved
//!      and optional columns set to NULL

use crate::query::df_graph::common::compute_plan_properties;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, new_null_array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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

/// Describes how to extract a grouping value from a column.
#[derive(Debug, Clone)]
enum SourceKeyColumn {
    /// Direct `._vid` column at the given index (e.g., column named `a._vid`).
    FlatVid(usize),
    /// Struct column at index, with `_vid` at the given field index within the struct.
    /// This handles `WITH *` projections that bundle variables into struct columns
    /// (e.g., column `a` containing fields `_vid`, `_labels`, etc.).
    StructVid(usize, usize),
    /// LargeBinary CypherValue blob column representing a node/edge variable.
    /// Used when variables are produced as CypherValue blobs (e.g., from MERGE)
    /// without flat `._vid` columns.
    CypherValueBlob(usize),
}

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
    /// Check whether a column belongs to an optional variable.
    fn is_optional_column_name(optional_variables: &HashSet<String>, col_name: &str) -> bool {
        optional_variables.iter().any(|var| {
            let var = var.as_str();
            // Match the bare variable column itself (struct column)
            col_name == var
            // Match "m._vid", "m.name", etc.
            || col_name.strip_prefix(var).is_some_and(|rest| rest.starts_with('.'))
            // Match internal EID tracking columns like "__eid_to_m"
            || (col_name.starts_with("__eid_to_") && col_name.ends_with(var))
        })
    }

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
        let input_schema = input.schema();
        // OptionalFilter can synthesize NULLs for optional columns even when upstream
        // declared them non-nullable (e.g., reused bound variables in OPTIONAL MATCH).
        // Ensure these columns are nullable in this operator's output schema.
        let fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| {
                if Self::is_optional_column_name(&optional_variables, f.name()) && !f.is_nullable()
                {
                    Field::new(f.name(), f.data_type().clone(), true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
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
        Self::is_optional_column_name(&self.optional_variables, col_name)
    }

    /// Compute source key columns for grouping.
    ///
    /// Finds columns suitable for source-row grouping:
    /// 1. Flat `._vid` columns (e.g., `a._vid`) that are not optional
    /// 2. Struct columns (e.g., `a`) that contain a `_vid` field and are not optional
    /// 3. LargeBinary CypherValue blob columns for variables without a flat `._vid`
    ///    column (e.g., `b` from MERGE output)
    ///
    /// The struct case handles `WITH *` projections that bundle node variables
    /// into struct columns. The blob case handles MERGE output where variables
    /// are serialized as CypherValue blobs without separate `._vid` columns.
    fn compute_source_key_columns(&self) -> Vec<SourceKeyColumn> {
        let mut result = Vec::new();
        let mut covered_vars: HashSet<String> = HashSet::new();

        // First pass: find FlatVid and StructVid columns
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if self.is_optional_column(field.name()) {
                continue;
            }
            if field.name().ends_with("._vid") {
                result.push(SourceKeyColumn::FlatVid(idx));
                if let Some(var_name) = field.name().strip_suffix("._vid") {
                    covered_vars.insert(var_name.to_string());
                }
            } else if let DataType::Struct(struct_fields) = field.data_type() {
                // Look for `_vid` field within struct (e.g., variable `a` has field `_vid`)
                for (fi, sf) in struct_fields.iter().enumerate() {
                    if sf.name() == "_vid" {
                        result.push(SourceKeyColumn::StructVid(idx, fi));
                        covered_vars.insert(field.name().to_string());
                        break;
                    }
                }
            }
        }

        // Second pass: find LargeBinary variable blob columns not yet covered.
        // These are bare variable names (no dots, no `__` prefix) of type LargeBinary
        // that don't have a corresponding `._vid` FlatVid column.
        for (idx, field) in self.schema.fields().iter().enumerate() {
            if self.is_optional_column(field.name()) {
                continue;
            }
            if *field.data_type() == DataType::LargeBinary
                && !field.name().contains('.')
                && !field.name().starts_with("__")
                && !covered_vars.contains(field.name())
            {
                result.push(SourceKeyColumn::CypherValueBlob(idx));
            }
        }

        result
    }
}

impl DisplayAs for OptionalFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let vars: Vec<&str> = self.optional_variables.iter().map(|s| s.as_str()).collect();
        write!(
            f,
            "OptionalFilterExec: predicate={}, optional_vars=[{}]",
            self.predicate,
            vars.join(", ")
        )
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
        let source_key_columns = self.compute_source_key_columns();
        let optional_col_indices: Vec<usize> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| self.is_optional_column(field.name()))
            .map(|(idx, _)| idx)
            .collect();

        // Debug: log schema and source key columns
        tracing::debug!(
            "OptionalFilterExec schema: {:?}",
            self.schema
                .fields()
                .iter()
                .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        );
        tracing::debug!(
            "OptionalFilterExec source_key_columns: {:?}, optional_cols: {:?}",
            source_key_columns,
            optional_col_indices
        );

        Ok(Box::pin(OptionalFilterStream {
            input: input_stream,
            predicate: Arc::clone(&self.predicate),
            schema: Arc::clone(&self.schema),
            source_key_columns,
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

    /// Source key columns for grouping (flat VID or struct VID).
    source_key_columns: Vec<SourceKeyColumn>,

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
            if !groups.contains_key(&key) {
                group_order.push(key.clone());
            }
            groups.entry(key).or_default().push(row_idx);
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

    /// Compute a grouping key from source column values for a row.
    fn compute_source_key(&self, batch: &RecordBatch, row_idx: usize) -> Vec<u8> {
        use arrow_array::{LargeBinaryArray, StructArray};

        /// Append a u64 value (or NULL sentinel) to the key buffer.
        fn append_u64_key(key: &mut Vec<u8>, val: Option<u64>) {
            key.extend_from_slice(&val.unwrap_or(u64::MAX).to_le_bytes());
        }

        let mut key = Vec::with_capacity(self.source_key_columns.len() * 8);
        for skc in &self.source_key_columns {
            match skc {
                SourceKeyColumn::FlatVid(col_idx) => {
                    append_u64_key(&mut key, extract_u64_value(batch.column(*col_idx), row_idx));
                }
                SourceKeyColumn::StructVid(col_idx, field_idx) => {
                    let vid = batch
                        .column(*col_idx)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .and_then(|sa| extract_u64_value(sa.column(*field_idx), row_idx));
                    append_u64_key(&mut key, vid);
                }
                SourceKeyColumn::CypherValueBlob(col_idx) => {
                    let col = batch.column(*col_idx);
                    if let Some(ba) = col.as_any().downcast_ref::<LargeBinaryArray>()
                        && ba.is_valid(row_idx)
                    {
                        let bytes = ba.value(row_idx);
                        // Length-prefix for unambiguous concatenation
                        key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        key.extend_from_slice(bytes);
                    } else {
                        append_u64_key(&mut key, None);
                    }
                }
            }
        }
        key
    }
}

/// Extract a u64 value from a column at a given row index.
fn extract_u64_value(col: &dyn Array, row_idx: usize) -> Option<u64> {
    use arrow_array::UInt64Array;
    let vid_array = col.as_any().downcast_ref::<UInt64Array>()?;
    vid_array
        .is_valid(row_idx)
        .then(|| vid_array.value(row_idx))
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
            other => other,
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
