// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! BEST BY operator for Locy.
//!
//! `BestByExec` selects the "best" row per group of KEY columns, using ordered
//! criteria (ASC/DESC) to rank rows and keeping only the top-ranked row per group.

use crate::query::df_graph::common::{
    ScalarKey, arrow_err, collect_accounted, compute_plan_properties, concat_accounted,
    extract_scalar_key, operator_reservation,
};
use arrow::compute::take;
use arrow_array::{ArrayRef, LargeBinaryArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// `column` in a form whose Arrow order is Cypher's `ORDER BY` order.
///
/// A CypherValue column (`LargeBinary`) holds tagged encodings, and Arrow
/// orders it by those bytes: by type tag first, so every integer ranked below
/// every float (`BEST BY v` over `5` and `1.5` chose `5`), and then by the
/// payload's byte order, which is not the numbers' order. It becomes the
/// column of its values' Cypher sort keys ([`encode_cypher_sort_key`]),
/// which order bytewise as `ORDER BY` does, with numbers by exact value. A
/// NULL stays NULL, so the criterion's own NULL placement still applies.
/// Any other column, including raw `Bytes`, is returned as it is.
///
/// # Errors
///
/// When a value does not decode as a CypherValue.
///
/// [`encode_cypher_sort_key`]: uni_query_functions::df_udfs::encode_cypher_sort_key
pub(crate) fn order_column(column: &ArrayRef, field: &Field) -> DFResult<ArrayRef> {
    use arrow::array::AsArray;
    let raw_bytes = field
        .metadata()
        .get("uni_raw_bytes")
        .is_some_and(|v| v == "true");
    if column.data_type() != &DataType::LargeBinary || raw_bytes {
        return Ok(Arc::clone(column));
    }
    let keys = column
        .as_binary::<i64>()
        .iter()
        .map(|bytes| {
            bytes
                .map(|b| {
                    uni_common::cypher_value_codec::decode(b)
                        .map(|v| uni_query_functions::df_udfs::encode_cypher_sort_key(&v))
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "BEST BY: undecodable value in column '{}': {e}",
                                field.name()
                            ))
                        })
                })
                .transpose()
        })
        .collect::<DFResult<LargeBinaryArray>>()?;
    Ok(Arc::new(keys))
}

/// Sort criterion for BEST BY ordering.
#[derive(Debug, Clone)]
pub struct SortCriterion {
    /// The criterion column's position in the schema it was resolved against.
    pub col_index: usize,
    /// The criterion column's name. The fixpoint's BEST BY merge resolves it
    /// by name in its facts' schema, and refuses one it does not hold.
    pub column: String,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// DataFusion `ExecutionPlan` that applies BEST BY selection.
///
/// For each group of rows sharing the same KEY columns, sorts by the given
/// criteria and keeps only the first (best) row per group.
#[derive(Debug)]
pub struct BestByExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Vec<usize>,
    sort_criteria: Vec<SortCriterion>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// When true, apply a secondary sort on remaining columns for deterministic
    /// tie-breaking. When false, tied rows are selected non-deterministically.
    deterministic: bool,
}

impl BestByExec {
    /// Create a new `BestByExec`.
    ///
    /// # Arguments
    /// * `input` - Child execution plan
    /// * `key_indices` - Indices of KEY columns for grouping
    /// * `sort_criteria` - Ordering criteria for selecting the "best" row
    /// * `deterministic` - Whether to apply secondary sort for tie-breaking
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        sort_criteria: Vec<SortCriterion>,
        deterministic: bool,
    ) -> Self {
        let schema = input.schema();
        let properties = compute_plan_properties(Arc::clone(&schema));
        Self {
            input,
            key_indices,
            sort_criteria,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            deterministic,
        }
    }
}

impl DisplayAs for BestByExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BestByExec: key_indices={:?}, criteria={:?}",
            self.key_indices, self.sort_criteria
        )
    }
}

impl ExecutionPlan for BestByExec {
    fn name(&self) -> &str {
        "BestByExec"
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
                "BestByExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.key_indices.clone(),
            self.sort_criteria.clone(),
            self.deterministic,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let key_indices = self.key_indices.clone();
        let sort_criteria = self.sort_criteria.clone();
        let schema = Arc::clone(&self.schema);
        let input_schema = self.input.schema();
        let deterministic = self.deterministic;

        // #261. This is an eager barrier: it holds the whole input, and
        // then the input and its concatenation at once. Nothing counted
        // either, so the pool could neither refuse nor report it.
        let mut reservation = operator_reservation("BestByExec", partition, &context);
        let fut = async move {
            // #261: charged as the barrier fills, not after it is full.
            let batches = collect_accounted(input_stream, &mut reservation).await?;

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let batch = concat_accounted(&input_schema, batches, &mut reservation)?;

            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            // Build sort columns: key columns ASC first (for grouping contiguity),
            // then criteria columns, then remaining columns ASC (deterministic tie-breaking).
            let num_cols = batch.num_columns();
            let mut sort_columns = Vec::new();

            // 1. Key columns ASC, nulls last
            for &ki in &key_indices {
                sort_columns.push(arrow::compute::SortColumn {
                    values: Arc::clone(batch.column(ki)),
                    options: Some(arrow::compute::SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                });
            }

            // 2. Criteria columns, in Cypher's order
            for criterion in &sort_criteria {
                sort_columns.push(arrow::compute::SortColumn {
                    values: order_column(
                        batch.column(criterion.col_index),
                        input_schema.field(criterion.col_index),
                    )?,
                    options: Some(arrow::compute::SortOptions {
                        descending: !criterion.ascending,
                        nulls_first: criterion.nulls_first,
                    }),
                });
            }

            // 3. Remaining columns ASC for deterministic tie-breaking (optional)
            if deterministic {
                let used_cols: std::collections::HashSet<usize> = key_indices
                    .iter()
                    .copied()
                    .chain(sort_criteria.iter().map(|c| c.col_index))
                    .collect();
                for col_idx in 0..num_cols {
                    if !used_cols.contains(&col_idx) {
                        sort_columns.push(arrow::compute::SortColumn {
                            values: Arc::clone(batch.column(col_idx)),
                            options: Some(arrow::compute::SortOptions {
                                descending: false,
                                nulls_first: false,
                            }),
                        });
                    }
                }
            }

            // Sort to get indices
            let sorted_indices =
                arrow::compute::lexsort_to_indices(&sort_columns, None).map_err(arrow_err)?;

            // Reorder batch by sorted indices
            let sorted_columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), &sorted_indices, None))
                .collect::<Result<Vec<_>, _>>()?;
            let sorted_batch =
                RecordBatch::try_new(Arc::clone(&schema), sorted_columns).map_err(arrow_err)?;

            // Dedup: keep first row per key group (linear scan)
            let mut keep_indices: Vec<u32> = Vec::new();
            let mut prev_key: Option<Vec<ScalarKey>> = None;

            for row_idx in 0..sorted_batch.num_rows() {
                let key = extract_scalar_key(&sorted_batch, &key_indices, row_idx);
                let is_new_group = match &prev_key {
                    None => true,
                    Some(prev) => *prev != key,
                };
                if is_new_group {
                    keep_indices.push(row_idx as u32);
                    prev_key = Some(key);
                }
            }

            let keep_array = UInt32Array::from(keep_indices);
            let output_columns: Vec<_> = sorted_batch
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), &keep_array, None))
                .collect::<Result<Vec<_>, _>>()?;

            RecordBatch::try_new(schema, output_columns).map_err(arrow_err)
        };

        Ok(Box::pin(BestByStream {
            state: BestByStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

enum BestByStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    Done,
}

struct BestByStream {
    state: BestByStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for BestByStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        match &mut self.state {
            BestByStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = BestByStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = BestByStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            BestByStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for BestByStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;

    fn make_test_batch(names: Vec<&str>, scores: Vec<f64>, ages: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("age", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(scores)),
                Arc::new(Int64Array::from(ages)),
            ],
        )
        .unwrap()
    }

    fn make_memory_exec(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    async fn execute_best_by(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        sort_criteria: Vec<SortCriterion>,
    ) -> RecordBatch {
        let exec = BestByExec::new(input, key_indices, sort_criteria, true);
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        if batches.is_empty() {
            RecordBatch::new_empty(exec.schema())
        } else {
            arrow::compute::concat_batches(&exec.schema(), &batches).unwrap()
        }
    }

    #[tokio::test]
    async fn test_best_ascending() {
        // Group "a" with scores 3.0, 1.0, 2.0 → best ascending = 1.0
        let batch = make_test_batch(vec!["a", "a", "a"], vec![3.0, 1.0, 2.0], vec![20, 30, 25]);
        let input = make_memory_exec(batch);
        let result = execute_best_by(
            input,
            vec![0], // key: name
            vec![SortCriterion {
                col_index: 1,
                column: String::new(), // sort by score
                ascending: true,
                nulls_first: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 1.0);
    }

    #[tokio::test]
    async fn test_best_descending() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![3.0, 1.0, 2.0], vec![20, 30, 25]);
        let input = make_memory_exec(batch);
        let result = execute_best_by(
            input,
            vec![0],
            vec![SortCriterion {
                col_index: 1,
                column: String::new(),
                ascending: false,
                nulls_first: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 3.0);
    }

    #[tokio::test]
    async fn test_multiple_groups() {
        let batch = make_test_batch(
            vec!["a", "a", "b", "b"],
            vec![3.0, 1.0, 5.0, 2.0],
            vec![20, 30, 40, 50],
        );
        let input = make_memory_exec(batch);
        let result = execute_best_by(
            input,
            vec![0],
            vec![SortCriterion {
                col_index: 1,
                column: String::new(),
                ascending: true,
                nulls_first: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..2 {
            match names.value(i) {
                "a" => assert_eq!(scores.value(i), 1.0),
                "b" => assert_eq!(scores.value(i), 2.0),
                _ => panic!("unexpected name"),
            }
        }
    }

    #[tokio::test]
    async fn test_multi_column_criteria() {
        // Two rows with same score, different age → second criterion breaks tie
        let batch = make_test_batch(vec!["a", "a"], vec![1.0, 1.0], vec![30, 20]);
        let input = make_memory_exec(batch);
        let result = execute_best_by(
            input,
            vec![0],
            vec![
                SortCriterion {
                    col_index: 1,
                    column: String::new(),
                    ascending: true,
                    nulls_first: false,
                },
                SortCriterion {
                    col_index: 2,
                    column: String::new(),
                    ascending: true,
                    nulls_first: false,
                },
            ],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let ages = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ages.value(0), 20); // younger wins with ascending age
    }

    #[tokio::test]
    async fn test_empty_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        let input = make_memory_exec(batch);
        let result = execute_best_by(input, vec![0], vec![]).await;
        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_single_row_passthrough() {
        let batch = make_test_batch(vec!["x"], vec![42.0], vec![10]);
        let input = make_memory_exec(batch);
        let result = execute_best_by(
            input,
            vec![0],
            vec![SortCriterion {
                col_index: 1,
                column: String::new(),
                ascending: true,
                nulls_first: false,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let scores = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(scores.value(0), 42.0);
    }
}
