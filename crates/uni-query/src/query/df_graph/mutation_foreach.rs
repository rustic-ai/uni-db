// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher FOREACH clauses.
//!
//! FOREACH executes side-effect mutations (CREATE, SET, REMOVE, DELETE, MERGE)
//! for each item in a list expression, per input row. The output rows are
//! passed through unchanged (FOREACH does not modify the caller-visible result).

use super::common::compute_plan_properties;
use super::mutation_common::{MutationContext, batches_to_rows, rows_to_batches};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use uni_common::Value;
use uni_cypher::ast::Expr;

use crate::query::planner::LogicalPlan;

/// DataFusion `ExecutionPlan` for Cypher FOREACH clauses.
///
/// FOREACH is a side-effect-only operator: it iterates over a list expression
/// per input row and executes body plans (mutations) for each item. The input
/// rows are passed through unchanged to downstream operators.
///
/// Implements the "eager barrier" pattern: collects all input batches, then
/// processes FOREACH side effects, then yields original batches.
#[derive(Debug)]
pub struct ForeachExec {
    /// Child plan producing input rows.
    input: Arc<dyn ExecutionPlan>,

    /// Iteration variable name (bound per list item).
    variable: String,

    /// AST expression for the list to iterate over.
    list_expr: Expr,

    /// Body plans to execute per list item.
    body: Vec<LogicalPlan>,

    /// Shared mutation context with executor and writer.
    mutation_ctx: Arc<MutationContext>,

    /// Output schema (same as input — FOREACH is pass-through).
    schema: SchemaRef,

    /// Plan properties for DataFusion optimizer.
    properties: PlanProperties,

    /// Metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl ForeachExec {
    /// Create a new `ForeachExec`.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        variable: String,
        list_expr: Expr,
        body: Vec<LogicalPlan>,
        mutation_ctx: Arc<MutationContext>,
    ) -> Self {
        let schema = input.schema();
        let properties = compute_plan_properties(schema.clone());
        Self {
            input,
            variable,
            list_expr,
            body,
            mutation_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for ForeachExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ForeachExec [var={}]", self.variable)
    }
}

impl ExecutionPlan for ForeachExec {
    fn name(&self) -> &str {
        "ForeachExec"
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
                "ForeachExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(ForeachExec::new(
            children[0].clone(),
            self.variable.clone(),
            self.list_expr.clone(),
            self.body.clone(),
            self.mutation_ctx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.clone();
        let schema = self.schema.clone();
        let variable = self.variable.clone();
        let list_expr = self.list_expr.clone();
        let body = self.body.clone();
        let mutation_ctx = self.mutation_ctx.clone();

        let stream = futures::stream::once(execute_foreach_inner(
            input,
            schema.clone(),
            variable,
            list_expr,
            body,
            mutation_ctx,
            partition,
            context,
        ))
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Inner async function for FOREACH execution.
#[expect(clippy::too_many_arguments)]
async fn execute_foreach_inner(
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    variable: String,
    list_expr: Expr,
    body: Vec<LogicalPlan>,
    mutation_ctx: Arc<MutationContext>,
    partition: usize,
    task_ctx: Arc<TaskContext>,
) -> DFResult<futures::stream::Iter<std::vec::IntoIter<DFResult<RecordBatch>>>> {
    // 1. Collect all input batches (eager barrier)
    let input_stream = input.execute(partition, task_ctx)?;
    let input_batches: Vec<RecordBatch> = input_stream.try_collect().await?;

    let input_row_count: usize = input_batches.iter().map(|b| b.num_rows()).sum();
    tracing::debug!(
        variable = variable.as_str(),
        rows = input_row_count,
        "Executing FOREACH"
    );

    let df_err = |msg: &str, e: &dyn std::fmt::Display| {
        datafusion::error::DataFusionError::Execution(format!("FOREACH: {msg}: {e}"))
    };

    // 2. Convert to rows for expression evaluation
    let rows = batches_to_rows(&input_batches)
        .map_err(|e| df_err("failed to convert batches to rows", &e))?;

    // 3. Execute FOREACH body per row, per list item
    let exec = &mutation_ctx.executor;
    let pm = &mutation_ctx.prop_manager;
    let params = &mutation_ctx.params;
    let ctx = mutation_ctx.query_ctx.as_ref();

    let writer_lock = &mutation_ctx.writer;
    let mut writer = writer_lock.write().await;

    for row in &rows {
        // Evaluate the list expression
        let list_val = exec
            .evaluate_expr(&list_expr, row, pm, params, ctx)
            .await
            .map_err(|e| df_err("list evaluation failed", &e))?;

        let items = match list_val {
            Value::List(arr) => arr,
            Value::Null => continue,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "FOREACH requires a list expression".to_string(),
                ));
            }
        };

        // Execute body plans for each item
        for item in items {
            let mut scope = row.clone();
            scope.insert(variable.clone(), item);

            for plan in &body {
                exec.execute_foreach_body_plan(
                    plan.clone(),
                    &mut scope,
                    &mut writer,
                    pm,
                    params,
                    ctx,
                )
                .await
                .map_err(|e| df_err("body execution failed", &e))?;
            }
        }
    }

    drop(writer);

    tracing::debug!(
        variable = variable.as_str(),
        rows = input_row_count,
        "FOREACH complete"
    );

    // 4. Pass through original rows (FOREACH is side-effect only)
    // Reconstruct from rows in case the schema needs normalization
    let result_batches =
        rows_to_batches(&rows, &schema).map_err(|e| df_err("failed to reconstruct batches", &e))?;
    let results: Vec<DFResult<RecordBatch>> = result_batches.into_iter().map(Ok).collect();
    Ok(futures::stream::iter(results))
}
