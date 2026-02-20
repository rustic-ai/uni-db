// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Zero-length path binding execution plan for DataFusion.
//!
//! This module provides [`BindZeroLengthPathExec`], a DataFusion [`ExecutionPlan`] that
//! converts a single-node pattern `p = (a)` into a Path with one node and zero edges.
//!
//! # Example
//!
//! ```text
//! Input:   [{"a._vid": 1, "a._label": "Person", ...}]
//! Bind:    p = (a)
//! Output:  [{"a._vid": 1, "a._label": "Person", ..., "p": Path{nodes: [node1], edges: []}}]
//! ```

use super::GraphExecutionContext;
use super::common::compute_plan_properties;
use arrow_array::builder::{
    LargeBinaryBuilder, ListBuilder, StringBuilder, StructBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
/// Execution plan that binds a zero-length path for single-node patterns.
///
/// For patterns like `p = (a)`, this creates a Path struct with one node
/// (from the bound node variable) and zero edges.
pub struct BindZeroLengthPathExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Node variable name (e.g., "a" in `p = (a)`).
    node_variable: String,

    /// Path variable name (e.g., "p" in `p = (a)`).
    path_variable: String,

    /// Graph execution context for property/label lookup.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for BindZeroLengthPathExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindZeroLengthPathExec")
            .field("node_variable", &self.node_variable)
            .field("path_variable", &self.path_variable)
            .finish()
    }
}

impl BindZeroLengthPathExec {
    /// Create a new zero-length path binding execution plan.
    ///
    /// # Arguments
    ///
    /// * `input` - Input plan providing rows with the node variable
    /// * `node_variable` - Variable name of the bound node
    /// * `path_variable` - Variable name for the path
    /// * `graph_ctx` - Graph context for property/label lookups
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        node_variable: String,
        path_variable: String,
        graph_ctx: Arc<GraphExecutionContext>,
    ) -> Self {
        let schema = super::common::extend_schema_with_path(input.schema(), &path_variable);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            node_variable,
            path_variable,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for BindZeroLengthPathExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BindZeroLengthPathExec: {} = ({})",
            self.path_variable, self.node_variable
        )
    }
}

impl ExecutionPlan for BindZeroLengthPathExec {
    fn name(&self) -> &str {
        "BindZeroLengthPathExec"
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
                "BindZeroLengthPathExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.node_variable.clone(),
            self.path_variable.clone(),
            self.graph_ctx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(BindZeroLengthPathStream {
            input: input_stream,
            node_variable: self.node_variable.clone(),
            schema: self.schema.clone(),
            graph_ctx: self.graph_ctx.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream that performs the zero-length path binding.
struct BindZeroLengthPathStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Node variable name.
    node_variable: String,

    /// Output schema.
    schema: SchemaRef,

    /// Graph context for lookups.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Metrics.
    metrics: BaselineMetrics,
}

use super::common::extract_column_value;

impl BindZeroLengthPathStream {
    /// Process a single input batch.
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let query_ctx = self.graph_ctx.query_context();

        let vid_col_name = format!("{}._vid", self.node_variable);

        // Create builders for nodes and empty edges
        let node_struct_fields = super::common::node_struct_fields();
        let edge_struct_fields = super::common::edge_struct_fields();

        let mut nodes_builder = ListBuilder::new(StructBuilder::new(
            node_struct_fields,
            vec![
                Box::new(UInt64Builder::new()),
                Box::new(ListBuilder::new(StringBuilder::new())),
                Box::new(LargeBinaryBuilder::new()),
            ],
        ));
        let mut rels_builder = ListBuilder::new(StructBuilder::from_fields(edge_struct_fields, 0));
        let mut path_validity = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let vid: Option<uni_common::core::id::Vid> = extract_column_value(
                &batch,
                &vid_col_name,
                row_idx,
                |arr: &arrow_array::UInt64Array, i| uni_common::core::id::Vid::from(arr.value(i)),
            );

            if vid.is_none() {
                nodes_builder.append(false);
                rels_builder.append(false);
                path_validity.push(false);
                continue;
            }

            super::common::append_node_to_struct_optional(nodes_builder.values(), vid, &query_ctx);
            nodes_builder.append(true);
            rels_builder.append(true);
            path_validity.push(true);
        }

        let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
        let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

        let path_array =
            super::common::build_path_struct_array(nodes_array, rels_array, path_validity)?;

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(path_array));

        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl Stream for BindZeroLengthPathStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let _timer = self.metrics.elapsed_compute().timer();
                let result = self.process_batch(batch);
                Poll::Ready(Some(result))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for BindZeroLengthPathStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
