// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shortest path execution plan for DataFusion.
//!
//! This module provides [`GraphShortestPathExec`], a DataFusion [`ExecutionPlan`] that
//! computes shortest paths between source and target vertices using BFS.
//!
//! # Algorithm
//!
//! Uses bidirectional BFS for efficiency:
//! 1. Expand from source (forward direction)
//! 2. Expand from target (backward direction)
//! 3. Return path when frontiers meet
//!
//! Falls back to single-direction BFS when bidirectional is not applicable.

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    column_as_vid_array, compute_plan_properties, edge_struct_fields, new_node_list_builder,
};
use arrow::compute::take;
use arrow_array::builder::{ListBuilder, StructBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use fxhash::FxHashMap;
use std::any::Any;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::core::id::Vid;
use uni_store::runtime::l0_visibility;
use uni_store::storage::direction::Direction;

/// Shortest path execution plan.
///
/// Computes shortest paths between source and target vertices using BFS.
/// Returns the path as a list of VIDs.
///
/// # Example
///
/// ```ignore
/// // Find shortest path from source to target via KNOWS edges
/// let shortest_path = GraphShortestPathExec::new(
///     input_plan,
///     "_source_vid",
///     "_target_vid",
///     vec![knows_type_id],
///     Direction::Both,
///     "p",
///     graph_ctx,
/// );
///
/// // Output: input columns + p._path (List<UInt64>)
/// ```
pub struct GraphShortestPathExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Column name containing source VIDs.
    source_column: String,

    /// Column name containing target VIDs.
    target_column: String,

    /// Edge type IDs to traverse.
    edge_type_ids: Vec<u32>,

    /// Traversal direction.
    direction: Direction,

    /// Variable name for the path.
    path_variable: String,

    /// Whether this is allShortestPaths (true) or shortestPath (false).
    all_shortest: bool,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphShortestPathExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphShortestPathExec")
            .field("source_column", &self.source_column)
            .field("target_column", &self.target_column)
            .field("edge_type_ids", &self.edge_type_ids)
            .field("direction", &self.direction)
            .field("path_variable", &self.path_variable)
            .field("all_shortest", &self.all_shortest)
            .finish()
    }
}

impl GraphShortestPathExec {
    /// Create a new shortest path execution plan.
    ///
    /// # Arguments
    ///
    /// * `input` - Input plan providing source and target vertices
    /// * `source_column` - Column name containing source VIDs
    /// * `target_column` - Column name containing target VIDs
    /// * `edge_type_ids` - Edge types to traverse
    /// * `direction` - Traversal direction
    /// * `path_variable` - Variable name for the path
    /// * `graph_ctx` - Graph execution context
    #[expect(
        clippy::too_many_arguments,
        reason = "Shortest path requires many parameters"
    )]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        target_column: impl Into<String>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
        path_variable: impl Into<String>,
        graph_ctx: Arc<GraphExecutionContext>,
        all_shortest: bool,
    ) -> Self {
        let source_column = source_column.into();
        let target_column = target_column.into();
        let path_variable = path_variable.into();

        let schema = Self::build_schema(input.schema(), &path_variable);
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            source_column,
            target_column,
            edge_type_ids,
            direction,
            path_variable,
            all_shortest,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema.
    fn build_schema(input_schema: SchemaRef, path_variable: &str) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add the proper path struct column (nodes + relationships)
        fields.push(crate::query::df_graph::common::build_path_struct_field(
            path_variable,
        ));

        // Add path column (raw VID list for internal use)
        let path_col_name = format!("{}._path", path_variable);
        fields.push(Field::new(
            &path_col_name,
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            true, // Nullable - null when no path exists
        ));

        // Add path length column
        let len_col_name = format!("{}._length", path_variable);
        fields.push(Field::new(&len_col_name, DataType::UInt64, true));

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphShortestPathExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = if self.all_shortest { "all" } else { "any" };
        write!(
            f,
            "GraphShortestPathExec: {} -> {} via {:?} ({})",
            self.source_column, self.target_column, self.edge_type_ids, mode
        )
    }
}

impl ExecutionPlan for GraphShortestPathExec {
    fn name(&self) -> &str {
        "GraphShortestPathExec"
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
                "GraphShortestPathExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.source_column.clone(),
            self.target_column.clone(),
            self.edge_type_ids.clone(),
            self.direction,
            self.path_variable.clone(),
            Arc::clone(&self.graph_ctx),
            self.all_shortest,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let warm_fut = self
            .graph_ctx
            .warming_future(self.edge_type_ids.clone(), self.direction);

        Ok(Box::pin(GraphShortestPathStream {
            input: input_stream,
            source_column: self.source_column.clone(),
            target_column: self.target_column.clone(),
            edge_type_ids: self.edge_type_ids.clone(),
            direction: self.direction,
            all_shortest: self.all_shortest,
            graph_ctx: Arc::clone(&self.graph_ctx),
            schema: Arc::clone(&self.schema),
            state: ShortestPathStreamState::Warming(warm_fut),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// State machine for shortest path stream execution.
enum ShortestPathStreamState {
    /// Warming adjacency CSRs before first batch.
    Warming(Pin<Box<dyn std::future::Future<Output = DFResult<()>> + Send>>),
    /// Processing input batches.
    Reading,
    /// Stream is done.
    Done,
}

/// Stream that computes shortest paths.
struct GraphShortestPathStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Column name containing source VIDs.
    source_column: String,

    /// Column name containing target VIDs.
    target_column: String,

    /// Edge type IDs to traverse.
    edge_type_ids: Vec<u32>,

    /// Traversal direction.
    direction: Direction,

    /// Whether this is allShortestPaths mode.
    all_shortest: bool,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: ShortestPathStreamState,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl GraphShortestPathStream {
    /// Compute shortest path between two vertices using BFS.
    fn compute_shortest_path(&self, source: Vid, target: Vid) -> Option<Vec<Vid>> {
        if source == target {
            return Some(vec![source]);
        }

        let mut visited: HashSet<Vid> = HashSet::new();
        let mut queue: VecDeque<(Vid, Vec<Vid>)> = VecDeque::new();

        visited.insert(source);
        queue.push_back((source, vec![source]));

        while let Some((current, path)) = queue.pop_front() {
            // Get neighbors for all edge types
            for &edge_type in &self.edge_type_ids {
                let neighbors = self
                    .graph_ctx
                    .get_neighbors(current, edge_type, self.direction);

                for (neighbor, _eid) in neighbors {
                    if neighbor == target {
                        // Found the target
                        let mut result = path.clone();
                        result.push(target);
                        return Some(result);
                    }

                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        let mut new_path = path.clone();
                        new_path.push(neighbor);
                        queue.push_back((neighbor, new_path));
                    }
                }
            }
        }

        None // No path found
    }

    /// Compute all shortest paths between two vertices using layer-by-layer BFS
    /// with predecessor tracking.
    ///
    /// Returns all paths of minimum length from source to target.
    fn compute_all_shortest_paths(&self, source: Vid, target: Vid) -> Vec<Vec<Vid>> {
        if source == target {
            return vec![vec![source]];
        }

        // Layer-by-layer BFS recording ALL predecessors at shortest depth
        let mut depth: FxHashMap<Vid, u32> = FxHashMap::default();
        let mut predecessors: FxHashMap<Vid, Vec<Vid>> = FxHashMap::default();
        depth.insert(source, 0);

        let mut current_layer: Vec<Vid> = vec![source];
        let mut current_depth = 0u32;
        let mut target_found = false;

        while !current_layer.is_empty() && !target_found {
            current_depth += 1;
            let mut next_layer_set: HashSet<Vid> = HashSet::new();

            for &current in &current_layer {
                for &edge_type in &self.edge_type_ids {
                    let neighbors =
                        self.graph_ctx
                            .get_neighbors(current, edge_type, self.direction);

                    for (neighbor, _eid) in neighbors {
                        if let Some(&d) = depth.get(&neighbor) {
                            // Already discovered: only add predecessor if same depth
                            if d == current_depth {
                                predecessors.entry(neighbor).or_default().push(current);
                            }
                            continue;
                        }

                        // First time seeing this vertex at current_depth
                        depth.insert(neighbor, current_depth);
                        predecessors.entry(neighbor).or_default().push(current);

                        if neighbor == target {
                            target_found = true;
                        } else {
                            next_layer_set.insert(neighbor);
                        }
                    }
                }
            }

            current_layer = next_layer_set.into_iter().collect();
        }

        if !target_found {
            return vec![];
        }

        // Enumerate all shortest paths via backward DFS from target to source
        let mut result: Vec<Vec<Vid>> = Vec::new();
        let mut stack: Vec<(Vid, Vec<Vid>)> = vec![(target, vec![target])];

        while let Some((node, path)) = stack.pop() {
            if node == source {
                let mut full_path = path;
                full_path.reverse();
                result.push(full_path);
                continue;
            }
            if let Some(preds) = predecessors.get(&node) {
                for &pred in preds {
                    let mut new_path = path.clone();
                    new_path.push(pred);
                    stack.push((pred, new_path));
                }
            }
        }

        result
    }

    /// Process a single input batch.
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        // Extract source and target VIDs
        let source_col = batch.column_by_name(&self.source_column).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Source column '{}' not found",
                self.source_column
            ))
        })?;

        let target_col = batch.column_by_name(&self.target_column).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Target column '{}' not found",
                self.target_column
            ))
        })?;

        let source_vid_cow = column_as_vid_array(source_col.as_ref())?;
        let source_vids: &UInt64Array = &source_vid_cow;

        let target_vid_cow = column_as_vid_array(target_col.as_ref())?;
        let target_vids: &UInt64Array = &target_vid_cow;

        if self.all_shortest {
            // allShortestPaths: each input row can produce multiple output rows
            let mut row_indices: Vec<u32> = Vec::new();
            let mut all_paths: Vec<Option<Vec<Vid>>> = Vec::new();

            for i in 0..batch.num_rows() {
                if source_vids.is_null(i) || target_vids.is_null(i) {
                    row_indices.push(i as u32);
                    all_paths.push(None);
                } else {
                    let source = Vid::from(source_vids.value(i));
                    let target = Vid::from(target_vids.value(i));
                    let paths = self.compute_all_shortest_paths(source, target);
                    if paths.is_empty() {
                        row_indices.push(i as u32);
                        all_paths.push(None);
                    } else {
                        for path in paths {
                            row_indices.push(i as u32);
                            all_paths.push(Some(path));
                        }
                    }
                }
            }

            // Expand input batch rows according to row_indices
            let indices = UInt32Array::from(row_indices);
            let expanded_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| {
                    take(col.as_ref(), &indices, None).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;
            let expanded_batch = RecordBatch::try_new(batch.schema(), expanded_columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            self.build_output_batch(&expanded_batch, &all_paths)
        } else {
            // shortestPath: one path per input row
            let mut paths: Vec<Option<Vec<Vid>>> = Vec::with_capacity(batch.num_rows());

            for i in 0..batch.num_rows() {
                let path = if source_vids.is_null(i) || target_vids.is_null(i) {
                    None
                } else {
                    let source = Vid::from(source_vids.value(i));
                    let target = Vid::from(target_vids.value(i));
                    self.compute_shortest_path(source, target)
                };
                paths.push(path);
            }

            self.build_output_batch(&batch, &paths)
        }
    }

    /// Build output batch with path columns.
    fn build_output_batch(
        &self,
        input: &RecordBatch,
        paths: &[Option<Vec<Vid>>],
    ) -> DFResult<RecordBatch> {
        let num_rows = paths.len();
        let query_ctx = self.graph_ctx.query_context();

        // Copy input columns
        let mut columns: Vec<ArrayRef> = input.columns().to_vec();

        // Build the path struct column (nodes + relationships)
        let mut nodes_builder = new_node_list_builder();
        let mut rels_builder =
            ListBuilder::new(StructBuilder::from_fields(edge_struct_fields(), num_rows));
        let mut path_validity = Vec::with_capacity(num_rows);

        for path in paths {
            match path {
                Some(vids) => {
                    // Add all nodes
                    for &vid in vids {
                        super::common::append_node_to_struct(
                            nodes_builder.values(),
                            vid,
                            &query_ctx,
                        );
                    }
                    nodes_builder.append(true);

                    // Add edges between consecutive nodes
                    // BFS returns node VIDs; edges are between consecutive pairs
                    for window in vids.windows(2) {
                        let src = window[0];
                        let dst = window[1];
                        let (eid, type_name) = self.find_edge(src, dst);
                        super::common::append_edge_to_struct(
                            rels_builder.values(),
                            eid,
                            &type_name,
                            src.as_u64(),
                            dst.as_u64(),
                            &query_ctx,
                        );
                    }
                    rels_builder.append(true);
                    path_validity.push(true);
                }
                None => {
                    // Null path
                    nodes_builder.append(false);
                    rels_builder.append(false);
                    path_validity.push(false);
                }
            }
        }

        let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
        let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

        let path_struct =
            super::common::build_path_struct_array(nodes_array, rels_array, path_validity)?;
        columns.push(Arc::new(path_struct));

        // Build raw path list column (VID list for internal use)
        let mut list_builder = ListBuilder::new(UInt64Builder::new());
        for path in paths {
            match path {
                Some(p) => {
                    let values: Vec<u64> = p.iter().map(|v| v.as_u64()).collect();
                    list_builder.values().append_slice(&values);
                    list_builder.append(true);
                }
                None => {
                    list_builder.append(false); // Null for no path
                }
            }
        }
        columns.push(Arc::new(list_builder.finish()));

        // Build path length column
        let lengths: Vec<Option<u64>> = paths
            .iter()
            .map(|p| p.as_ref().map(|path| (path.len() - 1) as u64))
            .collect();
        columns.push(Arc::new(UInt64Array::from(lengths)));

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Find an edge connecting src to dst.
    /// Returns (eid, type_name). Property lookup is handled by `append_edge_to_struct`.
    fn find_edge(&self, src: Vid, dst: Vid) -> (uni_common::core::id::Eid, String) {
        let query_ctx = self.graph_ctx.query_context();
        for &edge_type in &self.edge_type_ids {
            let neighbors = self.graph_ctx.get_neighbors(src, edge_type, self.direction);
            for (neighbor, eid) in neighbors {
                if neighbor == dst {
                    let type_name =
                        l0_visibility::get_edge_type(eid, &query_ctx).unwrap_or_default();
                    return (eid, type_name);
                }
            }
        }
        (uni_common::core::id::Eid::from(0u64), String::new())
    }
}

impl Stream for GraphShortestPathStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, ShortestPathStreamState::Done);

            match state {
                ShortestPathStreamState::Warming(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.state = ShortestPathStreamState::Reading;
                        // Continue loop to start reading
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = ShortestPathStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = ShortestPathStreamState::Warming(fut);
                        return Poll::Pending;
                    }
                },
                ShortestPathStreamState::Reading => {
                    // Check timeout
                    if let Err(e) = self.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::Execution(e.to_string()),
                        )));
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let result = self.process_batch(batch);
                            self.state = ShortestPathStreamState::Reading;
                            return Poll::Ready(Some(result));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.state = ShortestPathStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            self.state = ShortestPathStreamState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            self.state = ShortestPathStreamState::Reading;
                            return Poll::Pending;
                        }
                    }
                }
                ShortestPathStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for GraphShortestPathStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shortest_path_schema() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("_source_vid", DataType::UInt64, false),
            Field::new("_target_vid", DataType::UInt64, false),
        ]));

        let output_schema = GraphShortestPathExec::build_schema(input_schema, "p");

        assert_eq!(output_schema.fields().len(), 5);
        assert_eq!(output_schema.field(0).name(), "_source_vid");
        assert_eq!(output_schema.field(1).name(), "_target_vid");
        assert_eq!(output_schema.field(2).name(), "p");
        assert_eq!(output_schema.field(3).name(), "p._path");
        assert_eq!(output_schema.field(4).name(), "p._length");
    }
}
