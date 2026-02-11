// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

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

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::compute_plan_properties;
use crate::query::df_graph::unwind::arrow_to_json_value;
use crate::query::df_planner::HybridPhysicalPlanner;
use crate::query::planner::LogicalPlan;
use arrow_array::RecordBatch;
use arrow_array::builder::{Int64Builder, LargeListBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
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
    properties: PlanProperties,

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
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for RecursiveCTEExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "RecursiveCTEExec: {}", self.cte_name)
            }
        }
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

/// Execute a logical plan using a fresh HybridPhysicalPlanner with the given params.
async fn execute_subplan(
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
        datafusion::error::DataFusionError::Execution(format!("RecursiveCTE sub-plan error: {}", e))
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

/// Create a stable string key for a Value, used for cycle detection.
fn value_key(val: &Value) -> String {
    format!("{:?}", val)
}

/// Extract the VID from a CTE result value.
///
/// CTE result values can be:
/// - A Map with a `*._vid` key (from multi-column scan results)
/// - A raw integer (from single-column VID returns)
/// - A Map with a `_vid` key
fn extract_vid(val: &Value) -> Option<u64> {
    match val {
        Value::Int(v) => Some(*v as u64),
        Value::Map(map) => {
            // Look for any key ending in `._vid` or exactly `_vid`
            for (k, v) in map {
                if k == "_vid" || k.ends_with("._vid") {
                    return v.as_u64();
                }
            }
            None
        }
        _ => val.as_u64(),
    }
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
) -> DFResult<RecordBatch> {
    // 1. Execute anchor
    let anchor_batches = execute_subplan(
        initial_plan,
        params,
        graph_ctx,
        session_ctx,
        storage,
        schema_info,
    )
    .await?;
    let mut working_values = batches_to_values(&anchor_batches);
    let mut result_values = working_values.clone();

    // Track seen values for cycle detection
    let mut seen: HashSet<String> = working_values.iter().map(value_key).collect();

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
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
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
                let key = value_key(val);
                seen.insert(key) // returns false if already present
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

    RecordBatch::try_new(output_schema.clone(), vec![array])
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
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
