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
use super::common::{
    EntityPropertyCache, arrow_err, compute_plan_properties, extract_column_value,
};
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
    properties: Arc<PlanProperties>,

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
            pending: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// An in-flight [`EntityPropertyCache`] fetch and the batch waiting on it.
type PendingPrefetch = (
    Pin<Box<dyn std::future::Future<Output = DFResult<EntityPropertyCache>> + Send>>,
    RecordBatch,
);

/// Stream that synthesizes path structs from existing node/edge columns.
struct BindFixedPathStream {
    input: SendableRecordBatchStream,
    node_variables: Vec<String>,
    edge_variables: Vec<String>,
    schema: SchemaRef,
    graph_ctx: Arc<GraphExecutionContext>,
    metrics: BaselineMetrics,

    /// In-flight property pre-fetch for the batch being materialized. Path
    /// element structs carry user properties in a `properties` blob whose only
    /// synchronous accessor reads the L0 write buffers alone, so a flushed
    /// entity would come back with no properties. See
    /// [`super::common::EntityPropertyCache`].
    pending: Option<PendingPrefetch>,
}

impl BindFixedPathStream {
    /// The column holding `edge_var`'s eid. Internal tracking columns are named
    /// directly; user-named edge variables use the `{var}._eid` form.
    fn eid_column_name(edge_var: &str) -> String {
        if edge_var.starts_with("__eid_to_") {
            edge_var.to_string()
        } else {
            format!("{}._eid", edge_var)
        }
    }

    /// The entities this batch will materialize into path element structs.
    fn batch_entities(&self, batch: &RecordBatch) -> (Vec<Vid>, Vec<Eid>) {
        let mut vids = Vec::new();
        let mut eids = Vec::new();
        for row_idx in 0..batch.num_rows() {
            for node_var in &self.node_variables {
                if let Some(vid) = extract_column_value(
                    batch,
                    &format!("{}._vid", node_var),
                    row_idx,
                    |arr: &arrow_array::UInt64Array, i| Vid::from(arr.value(i)),
                ) {
                    vids.push(vid);
                }
            }
            for edge_var in &self.edge_variables {
                if let Some(eid) = extract_column_value(
                    batch,
                    &Self::eid_column_name(edge_var),
                    row_idx,
                    |arr: &arrow_array::UInt64Array, i| Eid::from(arr.value(i)),
                ) {
                    eids.push(eid);
                }
            }
        }
        (vids, eids)
    }

    fn process_batch(
        &self,
        batch: RecordBatch,
        prop_cache: Option<&EntityPropertyCache>,
    ) -> DFResult<RecordBatch> {
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
                let eid_col_name = Self::eid_column_name(edge_var);
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

                super::common::append_node_to_struct_optional_with(
                    nodes_builder.values(),
                    vid,
                    &query_ctx,
                    prop_cache,
                );
            }
            nodes_builder.append(true);

            // Add all edges in path order
            // Edge i connects node_variables[i] to node_variables[i+1]
            for (edge_idx, edge_var) in self.edge_variables.iter().enumerate() {
                // Internal tracking columns like __eid_to_b are the column name directly;
                // named edge variables use {var}._eid format
                let eid_col_name = Self::eid_column_name(edge_var);

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

                // A relationship in a path must report its STORED (start -> end)
                // direction, even when the path traversed it backward (undirected
                // `-[r]-` or incoming `<-[r]-`). The L0 chain holds the stored
                // endpoints for in-memory edges; for flushed (L1-resident) edges
                // `resolve_stored_edge_endpoints` recovers the orientation via a
                // directed-outgoing adjacency probe, keyed by the edge type id
                // resolved from the batch `_type` column (falling back to the L0
                // type). The adjacent path-node columns supply the traversal
                // order, which is also the final fallback when the probe is
                // inconclusive.
                //
                // Deliberately not routed through `common::append_traversed_edge`
                // like the traversal operators are: this is a fixed-length path,
                // so the eid is an `Option` (an unmatched OPTIONAL binding), the
                // candidate type ids are derived from a *name* rather than given,
                // and the traversal endpoints may legitimately be 0 when a
                // `_vid` column is absent. Nothing but the shape is shared. The
                // type name here comes from the batch `_type` column, which is
                // storage-backed, so it has none of the flushed-edge problem the
                // shared helper exists to solve.
                let adjacent = |idx: usize| {
                    self.node_variables
                        .get(idx)
                        .and_then(|nv| {
                            let col = format!("{}._vid", nv);
                            extract_column_value::<arrow_array::UInt64Array, u64>(
                                &batch,
                                &col,
                                row_idx,
                                |arr, i| arr.value(i),
                            )
                        })
                        .unwrap_or(0)
                };
                let traversal_src = adjacent(edge_idx);
                let traversal_dst = adjacent(edge_idx + 1);

                let (src_vid, dst_vid) = match eid {
                    Some(e) => {
                        let edge_type_ids: Vec<u32> = batch_type_name
                            .as_deref()
                            .and_then(|name| {
                                self.graph_ctx
                                    .storage()
                                    .schema_manager()
                                    .schema()
                                    .edge_type_id_by_name(name)
                            })
                            .or_else(|| {
                                uni_store::runtime::l0_visibility::get_edge_type(e, &query_ctx)
                                    .and_then(|name| {
                                        self.graph_ctx
                                            .storage()
                                            .schema_manager()
                                            .schema()
                                            .edge_type_id_by_name(&name)
                                    })
                            })
                            .map(|id| vec![id])
                            .unwrap_or_default();
                        self.graph_ctx.resolve_stored_edge_endpoints(
                            e,
                            uni_common::core::id::Vid::from(traversal_src),
                            uni_common::core::id::Vid::from(traversal_dst),
                            &edge_type_ids,
                        )
                    }
                    None => (traversal_src, traversal_dst),
                };

                super::common::append_edge_to_struct_optional_with(
                    rels_builder.values(),
                    eid,
                    src_vid,
                    dst_vid,
                    batch_type_name,
                    &query_ctx,
                    prop_cache,
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

        RecordBatch::try_new(self.schema.clone(), columns).map_err(arrow_err)
    }
}

impl Stream for BindFixedPathStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some((mut fut, batch)) = self.pending.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(cache)) => {
                        let _timer = self.metrics.elapsed_compute().timer();
                        let result = self.process_batch(batch, Some(&cache));
                        if let Ok(ref b) = result {
                            self.metrics.record_output(b.num_rows());
                        }
                        return Poll::Ready(Some(result));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => {
                        self.pending = Some((fut, batch));
                        return Poll::Pending;
                    }
                }
            }

            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let (vids, eids) = self.batch_entities(&batch);
                    let graph_ctx = self.graph_ctx.clone();
                    let fut = Box::pin(async move {
                        let query_ctx = graph_ctx.query_context();
                        EntityPropertyCache::prefetch(&graph_ctx, &query_ctx, &vids, &eids).await
                    });
                    self.pending = Some((fut, batch));
                    // Continue the loop to poll the freshly created future.
                }
                other => return other,
            }
        }
    }
}

impl RecordBatchStream for BindFixedPathStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
