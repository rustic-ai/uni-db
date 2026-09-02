// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Recursive CTE execution plan for DataFusion.
//!
//! Implements `WITH RECURSIVE` by iteratively executing the recursive part
//! with an updated working table until a fixed point is reached (no new rows).
//!
//! # Algorithm
//!
//! 1. Execute the anchor (initial) query → working table
//! 2. Loop:
//!    a. Bind working table as a parameter under the CTE name
//!    b. Re-plan and execute the recursive query with updated params
//!    c. Deduplicate against previously seen rows (cycle detection)
//!    d. If no new rows, terminate
//!    e. Accumulate new rows and repeat
//! 3. Output all accumulated rows as a single-column list

use crate::query::df_graph::common::{arrow_err, compute_plan_properties, execute_subplan};
use crate::query::df_graph::unwind::arrow_to_json_value;
use crate::query::df_graph::{GraphExecutionContext, MutationContext};
use crate::query::planner::LogicalPlan;
use arrow_array::RecordBatch;
use arrow_array::builder::{Int64Builder, LargeListBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_store::storage::manager::StorageManager;

/// Maximum number of CTE iterations before forced termination.
const MAX_ITERATIONS: usize = 1000;

/// Recursive CTE execution plan.
///
/// Stores **logical** plans (not physical) and re-plans + executes on each
/// iteration with updated parameters. The CTE name is injected as a parameter
/// containing the current working table.
pub struct RecursiveCTEExec {
    /// Name of the CTE (e.g., `hierarchy`).
    cte_name: String,

    /// Logical plan for the anchor query.
    initial_plan: LogicalPlan,

    /// Logical plan for the recursive query.
    recursive_plan: LogicalPlan,

    /// Graph execution context shared with sub-planners.
    graph_ctx: Arc<GraphExecutionContext>,

    /// DataFusion session context.
    session_ctx: Arc<RwLock<SessionContext>>,

    /// Storage manager for creating sub-planners.
    storage: Arc<StorageManager>,

    /// Schema for label/edge type lookups.
    schema_info: Arc<UniSchema>,

    /// Query parameters (will be extended with CTE working table).
    params: HashMap<String, Value>,

    /// Output schema (single column: the CTE name containing JSON-encoded values).
    output_schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Outer mutation context, threaded into each iteration's sub-planner
    /// so that writes inside the recursive body (rare but supported by the
    /// general API) route through the same transaction's L0 buffer.
    mutation_ctx: Option<Arc<MutationContext>>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for RecursiveCTEExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecursiveCTEExec")
            .field("cte_name", &self.cte_name)
            .finish()
    }
}

impl RecursiveCTEExec {
    /// Create a new recursive CTE execution plan.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        cte_name: String,
        initial_plan: LogicalPlan,
        recursive_plan: LogicalPlan,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        mutation_ctx: Option<Arc<MutationContext>>,
    ) -> Self {
        // Output schema: single column named after CTE, containing a LargeList<Int64>.
        // Each element is a VID (cast to Int64) from the CTE results. The `n IN hierarchy`
        // expression is rewritten to `CAST(n._vid AS Int64) IN hierarchy` by the expression
        // translator, so the types match.
        let inner_field = Arc::new(Field::new("item", DataType::Int64, true));
        let field = Field::new(&cte_name, DataType::LargeList(inner_field), false);
        let output_schema = Arc::new(Schema::new(vec![field]));
        let properties = compute_plan_properties(output_schema.clone());

        Self {
            cte_name,
            initial_plan,
            recursive_plan,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            properties,
            mutation_ctx,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for RecursiveCTEExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecursiveCTEExec: {}", self.cte_name)
    }
}

impl ExecutionPlan for RecursiveCTEExec {
    fn name(&self) -> &str {
        "RecursiveCTEExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
                "RecursiveCTEExec has no children".to_string(),
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

        // Clone all fields needed for the async computation
        let cte_name = self.cte_name.clone();
        let initial_plan = self.initial_plan.clone();
        let recursive_plan = self.recursive_plan.clone();
        let graph_ctx = self.graph_ctx.clone();
        let session_ctx = self.session_ctx.clone();
        let storage = self.storage.clone();
        let schema_info = self.schema_info.clone();
        let params = self.params.clone();
        let output_schema = self.output_schema.clone();
        let mutation_ctx = self.mutation_ctx.clone();

        let fut = async move {
            run_cte_loop(
                &cte_name,
                &initial_plan,
                &recursive_plan,
                &graph_ctx,
                &session_ctx,
                &storage,
                &schema_info,
                &params,
                &output_schema,
                mutation_ctx.as_ref(),
            )
            .await
        };

        Ok(Box::pin(RecursiveCTEStream {
            state: RecursiveCTEStreamState::Running(Box::pin(fut)),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Free functions for the CTE iteration loop
// ---------------------------------------------------------------------------

/// Extract values from record batches into a flat list of `Value`.
///
/// Each row becomes a single `Value`. If the row has one column, the column
/// value is used directly. If multiple columns, they are combined into a `Value::Map`.
fn batches_to_values(batches: &[RecordBatch]) -> Vec<Value> {
    let mut values = Vec::new();
    for batch in batches {
        let num_cols = batch.num_columns();
        let schema = batch.schema();

        for row_idx in 0..batch.num_rows() {
            if num_cols == 1 {
                values.push(arrow_to_json_value(batch.column(0).as_ref(), row_idx));
            } else {
                let mut map = Vec::new();
                for col_idx in 0..num_cols {
                    let col_name = schema.field(col_idx).name().clone();
                    let val = arrow_to_json_value(batch.column(col_idx).as_ref(), row_idx);
                    map.push((col_name, val));
                }
                values.push(Value::Map(map.into_iter().collect()));
            }
        }
    }
    values
}

/// Extract the VID from a CTE result value.
///
/// A CTE working-set row legitimately carries a raw id as well as an entity —
/// a single-column `RETURN id(n)` recursion produces integers — so this uses
/// the lenient [`uni_common::Value::coerce_vid`].
///
/// The one thing it adds over the shared reader is the qualified-column form:
/// a multi-column scan result arrives as a map keyed `n._vid` rather than
/// `_vid`. Picking that key by iterating the map chose arbitrarily between
/// several candidates, since `HashMap` iteration order is not stable — two
/// entity columns in the working set meant a per-run coin flip over which
/// entity the recursion followed. The smallest key by name is picked instead,
/// so the choice is at least the same on every run.
fn extract_vid(val: &Value) -> Option<u64> {
    if let Some(vid) = val.coerce_vid() {
        return Some(vid.as_u64());
    }
    let Value::Map(map) = val else {
        return None;
    };
    map.iter()
        .filter(|(k, _)| k.ends_with("._vid"))
        .min_by(|(a, _), (b, _)| a.cmp(b))
        .and_then(|(_, v)| v.as_u64())
}

/// Run the full recursive CTE iteration loop and produce the output batch.
#[expect(clippy::too_many_arguments)]
async fn run_cte_loop(
    cte_name: &str,
    initial_plan: &LogicalPlan,
    recursive_plan: &LogicalPlan,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<SessionContext>>,
    storage: &Arc<StorageManager>,
    schema_info: &Arc<UniSchema>,
    params: &HashMap<String, Value>,
    output_schema: &SchemaRef,
    mutation_ctx: Option<&Arc<MutationContext>>,
) -> DFResult<RecordBatch> {
    // 1. Execute anchor
    let anchor_batches = execute_subplan(
        initial_plan,
        params,
        &HashMap::new(), // No outer values for anchor
        graph_ctx,
        session_ctx,
        storage,
        schema_info,
        mutation_ctx,
    )
    .await?;
    let mut working_values = batches_to_values(&anchor_batches);
    let mut result_values = working_values.clone();

    // Track seen values for cycle detection. Key on `Value` directly rather
    // than `format!("{val:?}")`: multi-column rows are `Value::Map(HashMap)`,
    // whose `Debug` order is instance-dependent, so the same visited row could
    // hash to different strings and defeat cycle detection (non-termination).
    // `Value` has a canonical `Hash`/`Eq`, so content-equal rows dedup.
    let mut seen: HashSet<Value> = working_values.iter().cloned().collect();

    // 2. Iterate
    for _iteration in 0..MAX_ITERATIONS {
        if working_values.is_empty() {
            break;
        }

        // Bind working table VIDs to CTE name in params.
        // Extract VIDs so the expression translator resolves `hierarchy` as List<Int64>,
        // matching the VID column type used by `n._vid IN hierarchy`.
        let vid_list = Value::List(
            working_values
                .iter()
                .filter_map(|v| extract_vid(v).map(|vid| Value::Int(vid as i64)))
                .collect(),
        );
        let mut next_params = params.clone();
        next_params.insert(cte_name.to_string(), vid_list);

        // Execute recursive part
        let recursive_batches = execute_subplan(
            recursive_plan,
            &next_params,
            &HashMap::new(), // No outer values for recursive part
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            mutation_ctx,
        )
        .await?;
        let next_values = batches_to_values(&recursive_batches);

        if next_values.is_empty() {
            break;
        }

        // Filter out already-seen values (cycle detection)
        let new_values: Vec<Value> = next_values
            .into_iter()
            .filter(|val| {
                seen.insert(val.clone()) // returns false if already present
            })
            .collect();

        if new_values.is_empty() {
            break;
        }

        result_values.extend(new_values.clone());
        working_values = new_values;
    }

    // 3. Build output: single row with a LargeList<Int64> column of VIDs.
    // Each element is a VID (as Int64) extracted from the CTE results.
    let mut list_builder = LargeListBuilder::new(Int64Builder::new());
    for val in &result_values {
        if let Some(vid) = extract_vid(val) {
            list_builder.values().append_value(vid as i64);
        }
    }
    list_builder.append(true);
    let array = Arc::new(list_builder.finish());

    RecordBatch::try_new(output_schema.clone(), vec![array]).map_err(arrow_err)
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

/// Stream state for the recursive CTE.
enum RecursiveCTEStreamState {
    /// The CTE computation is running.
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Computation completed, batch ready to emit.
    Done,
}

/// Stream that runs the recursive CTE and emits the result.
struct RecursiveCTEStream {
    state: RecursiveCTEStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for RecursiveCTEStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        match &mut self.state {
            RecursiveCTEStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = RecursiveCTEStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = RecursiveCTEStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            RecursiveCTEStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for RecursiveCTEStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
