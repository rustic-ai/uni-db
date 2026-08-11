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
use crate::query::df_graph::bitmap::EidFilter;
use crate::query::df_graph::common::{
    EdgeAppendCtx, EntityPropertyCache, append_traversed_edge, arrow_err, column_as_vid_array,
    compute_plan_properties, edge_struct_fields, exec_err, new_node_list_builder,
};
use crate::query::df_graph::traverse::build_edge_property_filter;
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
use uni_common::Value as UniValue;
use uni_common::core::id::{Eid, Vid};
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

    /// Relationship variable bound by the pattern, e.g. `r` in
    /// `shortestPath((a)-[r:E*]->(b))`. Emitted as a `List<Struct(edge)>`
    /// column holding the path's relationships in path order.
    step_variable: Option<String>,

    /// Whether this is allShortestPaths (true) or shortestPath (false).
    all_shortest: bool,

    /// Minimum hops. Only 0 and 1 are reachable here — the planner refuses
    /// anything higher, which would need the search to continue past the first
    /// sighting of the target. 0 is what admits the zero-length self path.
    min_hops: u32,

    /// Maximum hops. `u32::MAX` when the pattern set no upper bound.
    max_hops: u32,

    /// Equality conditions from the relationship's inline property map, used to
    /// precompute an `EidFilter` during warming. Empty when there is no map.
    edge_property_conditions: Vec<(String, UniValue)>,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

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
            .field("step_variable", &self.step_variable)
            .field("all_shortest", &self.all_shortest)
            .field("min_hops", &self.min_hops)
            .field("max_hops", &self.max_hops)
            .field("edge_property_conditions", &self.edge_property_conditions)
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
        step_variable: Option<String>,
        graph_ctx: Arc<GraphExecutionContext>,
        all_shortest: bool,
        min_hops: u32,
        max_hops: u32,
        edge_property_conditions: Vec<(String, UniValue)>,
    ) -> Self {
        let source_column = source_column.into();
        let target_column = target_column.into();
        let path_variable = path_variable.into();

        let schema = Self::build_schema(input.schema(), &path_variable, step_variable.as_deref());
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            source_column,
            target_column,
            edge_type_ids,
            direction,
            path_variable,
            step_variable,
            all_shortest,
            min_hops,
            max_hops,
            edge_property_conditions,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema.
    fn build_schema(
        input_schema: SchemaRef,
        path_variable: &str,
        step_variable: Option<&str>,
    ) -> SchemaRef {
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

        // Appended last, so the existing column positions are untouched.
        if let Some(sv) = step_variable {
            fields.push(crate::query::df_graph::common::build_edge_list_field(sv));
        }

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
            self.step_variable.clone(),
            Arc::clone(&self.graph_ctx),
            self.all_shortest,
            self.min_hops,
            self.max_hops,
            self.edge_property_conditions.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Warm the CSR and precompute the edge filter in one future. The two
        // are independent scans — `build_edge_property_filter` reads edge
        // properties from storage, not the CSR — but both must finish before
        // the first batch is read. Mirrors the variable-length path.
        let warm_ctx = Arc::clone(&self.graph_ctx);
        let warm_types = self.edge_type_ids.clone();
        let warm_direction = self.direction;
        let warm_conditions = self.edge_property_conditions.clone();
        let warm_fut: Pin<Box<dyn std::future::Future<Output = DFResult<EidFilter>> + Send>> =
            Box::pin(async move {
                warm_ctx
                    .ensure_adjacency_warmed(&warm_types, warm_direction)
                    .await
                    .map_err(exec_err)?;
                build_edge_property_filter(&warm_ctx, &warm_types, warm_direction, &warm_conditions)
                    .await
            });

        Ok(Box::pin(GraphShortestPathStream {
            input: input_stream,
            source_column: self.source_column.clone(),
            target_column: self.target_column.clone(),
            edge_type_ids: self.edge_type_ids.clone(),
            direction: self.direction,
            all_shortest: self.all_shortest,
            step_variable: self.step_variable.clone(),
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            // Replaced by the warming future's result before any batch is read;
            // `AllAllowed` admits everything, so an absent map costs nothing.
            edge_property_filter: EidFilter::AllAllowed,
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
    /// Warming adjacency CSRs and building the edge filter before first batch.
    Warming(Pin<Box<dyn std::future::Future<Output = DFResult<EidFilter>> + Send>>),
    /// Processing input batches.
    Reading,
    /// Fetching storage-backed properties for this batch's path entities before
    /// the synchronous column builders run. Path element structs carry user
    /// properties in a `properties` blob whose only synchronous accessor reads
    /// the L0 write buffers alone, so a flushed entity would come back with no
    /// properties. See [`EntityPropertyCache`].
    PrefetchingProperties {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<EntityPropertyCache>> + Send>>,
        input: RecordBatch,
        paths: Vec<Option<Vec<Vid>>>,
    },
    /// Stream is done.
    Done,
}

/// Stream that computes shortest paths.
struct GraphShortestPathStream {
    /// Input stream.
    input: SendableRecordBatchStream,

    /// Relationship variable bound by the pattern, if any. See the exec's field.
    step_variable: Option<String>,

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

    /// Minimum hops (0 or 1; higher is refused by the planner).
    min_hops: u32,

    /// Maximum hops; `u32::MAX` when unbounded.
    max_hops: u32,

    /// Edges permitted by the relationship's inline property map. Precomputed
    /// during warming; `AllAllowed` when the pattern carried no map.
    edge_property_filter: EidFilter,

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
        // A zero-length path is only a match when the pattern allows zero hops.
        // `min_hops` defaults to 1, so `[:T]` and `[:T*1..n]` must not report
        // the source as reaching itself for free.
        if source == target {
            return (self.min_hops == 0).then(|| vec![source]);
        }
        if self.max_hops == 0 {
            return None;
        }

        let mut visited: HashSet<Vid> = HashSet::new();
        let mut queue: VecDeque<(Vid, Vec<Vid>)> = VecDeque::new();

        visited.insert(source);
        queue.push_back((source, vec![source]));

        while let Some((current, path)) = queue.pop_front() {
            // `path` holds vertices, so hops taken is one less than its length.
            let hops_taken = (path.len() - 1) as u32;
            if hops_taken >= self.max_hops {
                continue;
            }
            // Get neighbors for all edge types
            for &edge_type in &self.edge_type_ids {
                let neighbors = self
                    .graph_ctx
                    .get_neighbors(current, edge_type, self.direction);

                for (neighbor, eid) in neighbors {
                    // The property map gates expansion, not the result set: the
                    // shortest path over permitted edges is not the shortest
                    // path filtered afterwards.
                    if !self.edge_property_filter.contains(eid) {
                        continue;
                    }
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
        // See `compute_shortest_path`: zero hops is a match only under `*0..`.
        if source == target {
            return if self.min_hops == 0 {
                vec![vec![source]]
            } else {
                vec![]
            };
        }
        if self.max_hops == 0 {
            return vec![];
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
            if current_depth > self.max_hops {
                break;
            }
            let mut next_layer_set: HashSet<Vid> = HashSet::new();

            for &current in &current_layer {
                for &edge_type in &self.edge_type_ids {
                    let neighbors =
                        self.graph_ctx
                            .get_neighbors(current, edge_type, self.direction);

                    for (neighbor, eid) in neighbors {
                        // Gating the forward pass is sufficient for the backward
                        // reconstruction too: it walks `predecessors`, which is
                        // built only from edges admitted here.
                        if !self.edge_property_filter.contains(eid) {
                            continue;
                        }
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

    /// The entities the batch's paths will materialize into path element
    /// structs, for [`EntityPropertyCache::prefetch`]. Edges are resolved the
    /// same way [`Self::build_output_batch`] resolves them.
    fn path_entities(&self, paths: &[Option<Vec<Vid>>]) -> (Vec<Vid>, Vec<Eid>) {
        let mut vids = Vec::new();
        let mut eids = Vec::new();
        for path in paths.iter().flatten() {
            vids.extend_from_slice(path);
            for window in path.windows(2) {
                let eid = self.find_edge(window[0], window[1]);
                eids.push(eid);
            }
        }
        (vids, eids)
    }

    /// Compute the shortest path(s) for a batch, returning the (possibly
    /// row-expanded) input alongside them.
    ///
    /// Split from column materialization so path element properties can be
    /// pre-fetched from storage first — see [`EntityPropertyCache`].
    fn compute_paths(&self, batch: RecordBatch) -> DFResult<(RecordBatch, Vec<Option<Vec<Vid>>>)> {
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
            let expanded_batch =
                RecordBatch::try_new(batch.schema(), expanded_columns).map_err(arrow_err)?;

            Ok((expanded_batch, all_paths))
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

            Ok((batch, paths))
        }
    }

    /// Build output batch with path columns.
    fn build_output_batch(
        &self,
        input: &RecordBatch,
        paths: &[Option<Vec<Vid>>],
        prop_cache: Option<&EntityPropertyCache>,
    ) -> DFResult<RecordBatch> {
        let num_rows = paths.len();
        let query_ctx = self.graph_ctx.query_context();
        let edge_ctx = EdgeAppendCtx {
            graph_ctx: &self.graph_ctx,
            query_ctx: &query_ctx,
            edge_type_ids: &self.edge_type_ids,
            prop_cache,
            fixed_type_name: None,
        };

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
                        super::common::append_node_to_struct_with(
                            nodes_builder.values(),
                            vid,
                            &query_ctx,
                            prop_cache,
                        );
                    }
                    nodes_builder.append(true);

                    // Add edges between consecutive nodes
                    // BFS returns node VIDs; edges are between consecutive pairs
                    for window in vids.windows(2) {
                        let src = window[0];
                        let dst = window[1];
                        let eid = self.find_edge(src, dst);
                        append_traversed_edge(rels_builder.values(), &edge_ctx, eid, src, dst);
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

        // The bound relationship variable is the path's relationship list, so
        // it is the very array about to become the path struct's child — take a
        // second reference rather than rebuilding it. It already carries a null
        // for each unmatched row, matching the path column's validity.
        let step_array = self
            .step_variable
            .is_some()
            .then(|| Arc::clone(&rels_array));

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

        // Appended last, matching `build_schema`.
        if let Some(step_array) = step_array {
            columns.push(step_array);
        }

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)
    }

    /// Find an edge connecting src to dst.
    ///
    /// Returns the eid only; the type name and stored orientation are resolved
    /// by `append_traversed_edge`, which can recover both for a flushed edge.
    fn find_edge(&self, src: Vid, dst: Vid) -> Eid {
        for &edge_type in &self.edge_type_ids {
            let neighbors = self.graph_ctx.get_neighbors(src, edge_type, self.direction);
            for (neighbor, eid) in neighbors {
                // Must re-apply the filter. The BFS admitted this *pair* via a
                // permitted edge, but with parallel edges the first neighbour
                // match can be a different edge that fails the map — the
                // returned path would then carry a relationship contradicting
                // the query that produced it.
                if neighbor == dst && self.edge_property_filter.contains(eid) {
                    return eid;
                }
            }
        }
        Eid::from(0u64)
    }
}

impl Stream for GraphShortestPathStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            let state = std::mem::replace(&mut self.state, ShortestPathStreamState::Done);

            match state {
                ShortestPathStreamState::Warming(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(filter)) => {
                        self.edge_property_filter = filter;
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
                        return Poll::Ready(Some(Err(exec_err(e))));
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let (input, paths) = match self.compute_paths(batch) {
                                Ok(pair) => pair,
                                Err(e) => {
                                    self.state = ShortestPathStreamState::Reading;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };
                            let (vids, eids) = self.path_entities(&paths);
                            let graph_ctx = self.graph_ctx.clone();
                            let fut = Box::pin(async move {
                                let query_ctx = graph_ctx.query_context();
                                EntityPropertyCache::prefetch(&graph_ctx, &query_ctx, &vids, &eids)
                                    .await
                            });
                            self.state = ShortestPathStreamState::PrefetchingProperties {
                                fut,
                                input,
                                paths,
                            };
                            // Continue loop to poll the freshly created future.
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
                ShortestPathStreamState::PrefetchingProperties {
                    mut fut,
                    input,
                    paths,
                } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(cache)) => {
                        self.state = ShortestPathStreamState::Reading;
                        return Poll::Ready(Some(self.build_output_batch(
                            &input,
                            &paths,
                            Some(&cache),
                        )));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = ShortestPathStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state =
                            ShortestPathStreamState::PrefetchingProperties { fut, input, paths };
                        return Poll::Pending;
                    }
                },
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

        let output_schema = GraphShortestPathExec::build_schema(input_schema, "p", None);

        assert_eq!(output_schema.fields().len(), 5);
        assert_eq!(output_schema.field(0).name(), "_source_vid");
        assert_eq!(output_schema.field(1).name(), "_target_vid");
        assert_eq!(output_schema.field(2).name(), "p");
        assert_eq!(output_schema.field(3).name(), "p._path");
        assert_eq!(output_schema.field(4).name(), "p._length");
    }

    #[test]
    fn test_shortest_path_schema_with_extra_input_fields() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("_source_vid", DataType::UInt64, false),
            Field::new("_target_vid", DataType::UInt64, false),
            Field::new("extra_col", DataType::Utf8, true),
        ]));

        let output_schema = GraphShortestPathExec::build_schema(input_schema, "route", None);
        // Extra input fields should be preserved in output
        assert!(
            output_schema.field_with_name("extra_col").is_ok(),
            "Extra input columns should pass through"
        );
        assert!(
            output_schema.field_with_name("route").is_ok(),
            "Path variable should be in output"
        );
        assert!(
            output_schema.field_with_name("route._length").is_ok(),
            "Path length should be in output"
        );
    }

    #[test]
    fn test_shortest_path_schema_empty_path_var() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("_source_vid", DataType::UInt64, false),
            Field::new("_target_vid", DataType::UInt64, false),
        ]));

        // Empty string path variable name should still work
        let output_schema = GraphShortestPathExec::build_schema(input_schema, "", None);
        assert!(output_schema.fields().len() >= 4);
    }

    #[test]
    fn test_shortest_path_schema_appends_step_variable_last() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("_source_vid", DataType::UInt64, false),
            Field::new("_target_vid", DataType::UInt64, false),
        ]));

        let output_schema = GraphShortestPathExec::build_schema(input_schema, "p", Some("r"));

        // The bound relationship column is appended after the existing three,
        // so their positions — which `build_output_batch` pushes to by index —
        // are unchanged.
        assert_eq!(output_schema.fields().len(), 6);
        assert_eq!(output_schema.field(2).name(), "p");
        assert_eq!(output_schema.field(3).name(), "p._path");
        assert_eq!(output_schema.field(4).name(), "p._length");
        assert_eq!(output_schema.field(5).name(), "r");
        assert!(
            matches!(output_schema.field(5).data_type(), DataType::List(_)),
            "the relationship variable is a list of edge structs, got {:?}",
            output_schema.field(5).data_type()
        );
    }
}
