// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixed-length path binding execution plan for DataFusion.
//!
//! This module provides [`BindFixedPathExec`], a DataFusion [`ExecutionPlan`] that
//! synthesizes a path struct from existing node and edge columns in the batch.
//!
//! Used for patterns like `p = (a)-[r]->(b)` or `p = (a)-[r1]->(b)-[r2]->(c)`
//! where the traversals are single-hop and the path variable needs to be materialized.

use super::GraphExecutionContext;
use super::common::{compute_plan_properties, extract_column_value};
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
use uni_common::core::id::{Eid, Vid};

/// Execution plan that binds a fixed-length path from existing node/edge columns.
///
/// For patterns like `p = (a)-[r]->(b)` or `p = (a)-[r1]->(b)-[r2]->(c)`,
/// this creates a Path struct with nodes and relationships from the already-computed
/// columns in the input batch.
pub struct BindFixedPathExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Node variable names in path order (e.g., ["a", "b"] or ["a", "b", "c"]).
    node_variables: Vec<String>,

    /// Edge variable names in path order (e.g., ["r"] or ["r1", "r2"]).
    edge_variables: Vec<String>,

    /// Path variable name (e.g., "p" in `p = (a)-[r]->(b)`).
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

impl fmt::Debug for BindFixedPathExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindFixedPathExec")
            .field("node_variables", &self.node_variables)
            .field("edge_variables", &self.edge_variables)
            .field("path_variable", &self.path_variable)
            .finish()
    }
}

impl BindFixedPathExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        node_variables: Vec<String>,
        edge_variables: Vec<String>,
        path_variable: String,
        graph_ctx: Arc<GraphExecutionContext>,
    ) -> Self {
        let schema = super::common::extend_schema_with_path(input.schema(), &path_variable);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            node_variables,
            edge_variables,
            path_variable,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for BindFixedPathExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BindFixedPathExec: {} = ({}) via [{}]",
            self.path_variable,
            self.node_variables.join(", "),
            self.edge_variables.join(", "),
        )
    }
}

impl ExecutionPlan for BindFixedPathExec {
    fn name(&self) -> &str {
        "BindFixedPathExec"
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
                "BindFixedPathExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.node_variables.clone(),
            self.edge_variables.clone(),
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

        Ok(Box::pin(BindFixedPathStream {
            input: input_stream,
            node_variables: self.node_variables.clone(),
            edge_variables: self.edge_variables.clone(),
            schema: self.schema.clone(),
            graph_ctx: self.graph_ctx.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Stream that synthesizes path structs from existing node/edge columns.
struct BindFixedPathStream {
    input: SendableRecordBatchStream,
    node_variables: Vec<String>,
    edge_variables: Vec<String>,
    schema: SchemaRef,
    graph_ctx: Arc<GraphExecutionContext>,
    metrics: BaselineMetrics,
}

impl BindFixedPathStream {
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let query_ctx = self.graph_ctx.query_context();

        // Build node and edge struct fields
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
        let mut rels_builder = ListBuilder::new(StructBuilder::from_fields(
            edge_struct_fields,
            num_rows * self.edge_variables.len(),
        ));
        let mut path_validity = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            // A fixed path is NULL if any required node or edge binding is missing.
            let row_has_missing_node = self.node_variables.iter().any(|node_var| {
                let vid_col_name = format!("{}._vid", node_var);
                extract_column_value::<arrow_array::UInt64Array, u64>(
                    &batch,
                    &vid_col_name,
                    row_idx,
                    |arr, i| arr.value(i),
                )
                .is_none()
            });
            let row_has_missing_edge = self.edge_variables.iter().any(|edge_var| {
                let eid_col_name = if edge_var.starts_with("__eid_to_") {
                    edge_var.clone()
                } else {
                    format!("{}._eid", edge_var)
                };
                extract_column_value::<arrow_array::UInt64Array, u64>(
                    &batch,
                    &eid_col_name,
                    row_idx,
                    |arr, i| arr.value(i),
                )
                .is_none()
            });

            if row_has_missing_node || row_has_missing_edge {
                nodes_builder.append(false);
                rels_builder.append(false);
                path_validity.push(false);
                continue;
            }

            // Add all nodes in path order
            for node_var in &self.node_variables {
                let vid_col_name = format!("{}._vid", node_var);

                let vid: Option<Vid> = extract_column_value(
                    &batch,
                    &vid_col_name,
                    row_idx,
                    |arr: &arrow_array::UInt64Array, i| Vid::from(arr.value(i)),
                );

                super::common::append_node_to_struct_optional(
                    nodes_builder.values(),
                    vid,
                    &query_ctx,
                );
            }
            nodes_builder.append(true);

            // Add all edges in path order
            // Edge i connects node_variables[i] to node_variables[i+1]
            for (edge_idx, edge_var) in self.edge_variables.iter().enumerate() {
                // Internal tracking columns like __eid_to_b are the column name directly;
                // named edge variables use {var}._eid format
                let eid_col_name = if edge_var.starts_with("__eid_to_") {
                    edge_var.clone()
                } else {
                    format!("{}._eid", edge_var)
                };

                let eid: Option<Eid> = extract_column_value(
                    &batch,
                    &eid_col_name,
                    row_idx,
                    |arr: &arrow_array::UInt64Array, i| Eid::from(arr.value(i)),
                );

                // Try to get the edge type name from the batch column (populated by
                // GraphTraverseExec from the schema). This is the primary source;
                // L0 lookup is only a fallback for in-memory mutations.
                let batch_type_name: Option<String> = if !edge_var.starts_with("__eid_to_") {
                    let type_col_name = format!("{}._type", edge_var);
                    extract_column_value(
                        &batch,
                        &type_col_name,
                        row_idx,
                        |arr: &arrow_array::StringArray, i| arr.value(i).to_string(),
                    )
                } else {
                    None
                };

                // Get src/dst VIDs from adjacent node variables
                let src_vid = self
                    .node_variables
                    .get(edge_idx)
                    .and_then(|nv| {
                        let col = format!("{}._vid", nv);
                        extract_column_value::<arrow_array::UInt64Array, u64>(
                            &batch,
                            &col,
                            row_idx,
                            |arr, i| arr.value(i),
                        )
                    })
                    .unwrap_or(0);
                let dst_vid = self
                    .node_variables
                    .get(edge_idx + 1)
                    .and_then(|nv| {
                        let col = format!("{}._vid", nv);
                        extract_column_value::<arrow_array::UInt64Array, u64>(
                            &batch,
                            &col,
                            row_idx,
                            |arr, i| arr.value(i),
                        )
                    })
                    .unwrap_or(0);

                super::common::append_edge_to_struct_optional(
                    rels_builder.values(),
                    eid,
                    src_vid,
                    dst_vid,
                    batch_type_name,
                    &query_ctx,
                );
            }
            rels_builder.append(true);
            path_validity.push(true);
        }

        let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
        let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

        let path_array =
            super::common::build_path_struct_array(nodes_array, rels_array, path_validity)?;

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(path_array));

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for BindFixedPathStream {
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

impl RecordBatchStream for BindFixedPathStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
