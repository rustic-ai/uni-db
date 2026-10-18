// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shortest path execution plan for DataFusion.
//!
//! This module provides [`GraphShortestPathExec`], a DataFusion [`ExecutionPlan`] that
//! computes shortest paths between source and target vertices using BFS.
//!
//! # Algorithm
//!
//! A single-direction breadth-first search from the source. Input rows are
//! grouped by source, and each group shares **one** search, grown only until
//! every target in the group is settled; the search is retained across batches
//! while the source stays the same. An unbound target — the source cross-joined
//! with every vertex of the target label — therefore costs one search, not one
//! per target (A8).
//!
//! The search is not bidirectional. Which of several equal-length paths
//! `shortestPath` returns is decided by the forward expansion order (the first
//! vertex, in BFS order, that reaches the target is its parent), and a
//! bidirectional meet would choose differently. A target filter such as
//! `WHERE b.x < 20` is applied by the planner to the target's scan, before the
//! cross join, so the operator sees only the qualifying targets.
//!
//! # A pair with no path
//!
//! Under a mandatory MATCH the operator drops it, as openCypher does. Under an
//! OPTIONAL MATCH it keeps the row with a NULL path: whether that row stands
//! (the pattern is one bound pair) or gives way to the entering row's matches
//! or to a single NULL row (the target was scanned inside the clause) is
//! decided per entering row by the `OptionalFilterExec` closing the clause.

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
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use rustc_hash::FxHashMap;
use std::collections::VecDeque;
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

    /// Whether the pattern belongs to an `OPTIONAL MATCH`: keep a pair with
    /// no path as a NULL-path row (true) or drop it (false).
    optional: bool,

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
            .field("optional", &self.optional)
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
    /// * `optional` - Keep a pair with no path as a NULL-path row (OPTIONAL
    ///   MATCH) instead of dropping it (mandatory MATCH)
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
        optional: bool,
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
            optional,
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
        )?;
        if self.optional {
            write!(f, " optional")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for GraphShortestPathExec {
    fn name(&self) -> &str {
        "GraphShortestPathExec"
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
            self.optional,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Taken before `context` is moved into the child's `execute`.
        let pool = Arc::clone(context.memory_pool());
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
            // #261. The paths this operator enumerates are combinatorial in the
            // graph, not linear in it: `allShortestPaths` between two vertices
            // yields the product of the predecessor counts along the layers.
            // Nothing counted them, so the pool could not refuse, and LDBC IC14
            // — an `allShortestPaths` query — is on record as "killed by hand
            // after 111 min" at "19.2 GB and climbing".
            batch_path_bytes: 0,
            source_bfs: None,
            source_layers: None,
            reservation: MemoryConsumer::new(format!("GraphShortestPathExec[{partition}]"))
                .register(&pool),
            source_column: self.source_column.clone(),
            target_column: self.target_column.clone(),
            edge_type_ids: self.edge_type_ids.clone(),
            direction: self.direction,
            all_shortest: self.all_shortest,
            step_variable: self.step_variable.clone(),
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            optional: self.optional,
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

    /// Keep a pair with no path as a NULL-path row; see the exec's field.
    optional: bool,

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

    /// Bytes of `reservation` currently charged to the paths of the batch in
    /// flight, released when the next batch arrives.
    batch_path_bytes: usize,

    /// The `shortestPath` search retained from the last source searched, reused
    /// while the next rows share its source.
    source_bfs: Option<SourceBfs>,

    /// The `allShortestPaths` search retained likewise.
    source_layers: Option<SourceLayers>,

    /// Pool reservation for the BFS state and the enumerated paths (#261).
    ///
    /// Sized to what this operator holds for the batch it is working on, and
    /// resized back down when the next batch arrives — by then the previous
    /// batch's paths have been handed downstream and are no longer ours to
    /// account for.
    reservation: MemoryReservation,
}

/// Growth charged to the reservation at a time by a search (64 KiB).
///
/// `try_grow` takes the pool's lock; charging per vertex would cost more than
/// the allocation it guards. A search smaller than one block is never charged,
/// which bounds the unaccounted state at one block per retained search.
const GROW_BLOCK: usize = 64 * 1024;

/// Charged per vertex a [`SourceBfs`] discovers: its `parent` and `depth`
/// entries and its queue slot.
const SOURCE_BFS_BYTES_PER_VERTEX: usize =
    2 * std::mem::size_of::<(Vid, Vid)>() + std::mem::size_of::<Vid>();

/// Charged per vertex a [`SourceLayers`] discovers: its `depth` entry, its
/// `predecessors` entry with the first predecessor, and its layer slot. A
/// further predecessor is charged one `Vid`.
const SOURCE_LAYERS_BYTES_PER_VERTEX: usize = std::mem::size_of::<(Vid, u32)>()
    + std::mem::size_of::<(Vid, Vec<Vid>)>()
    + 2 * std::mem::size_of::<Vid>();

/// A breadth-first search from one source, grown on demand for `shortestPath`.
///
/// Shared by every row with this source: each target settles it a little
/// further, and a target it has already passed costs one map lookup.
struct SourceBfs {
    /// Where the search starts.
    source: Vid,
    /// `parent[v]` is the vertex `v` was first reached from. The source has no
    /// entry, which is what terminates [`rebuild_path`].
    parent: FxHashMap<Vid, Vid>,
    /// Hops from the source for every vertex discovered, the source included.
    depth: FxHashMap<Vid, u32>,
    /// Discovered vertices not yet expanded, in discovery order.
    queue: VecDeque<Vid>,
    /// Bytes this search holds against the stream's reservation.
    charged: usize,
    /// Growth not yet charged, below one [`GROW_BLOCK`].
    uncharged: usize,
}

impl SourceBfs {
    /// A search from `source`, counted on `graph_ctx`'s per-query counters
    /// (`QueryMetrics::shortest_path_searches`).
    fn new(source: Vid, graph_ctx: &GraphExecutionContext) -> Self {
        if let Some(counters) = graph_ctx.counters() {
            counters.add_shortest_path_search();
        }
        let mut depth = FxHashMap::default();
        depth.insert(source, 0);
        Self {
            source,
            parent: FxHashMap::default(),
            depth,
            queue: VecDeque::from([source]),
            charged: 0,
            uncharged: 0,
        }
    }
}

/// A layer-by-layer search from one source, grown on demand for
/// `allShortestPaths`, recording every predecessor at shortest depth.
struct SourceLayers {
    /// Where the search starts.
    source: Vid,
    /// Hops from the source for every vertex discovered, the source included.
    depth: FxHashMap<Vid, u32>,
    /// Every vertex one layer closer to the source with an admitted edge into
    /// the key, once per such edge.
    predecessors: FxHashMap<Vid, Vec<Vid>>,
    /// The deepest complete layer, not yet expanded.
    current_layer: Vec<Vid>,
    /// Depth of `current_layer`.
    current_depth: u32,
    /// No further layer exists within the hop bound.
    exhausted: bool,
    /// Bytes this search holds against the stream's reservation.
    charged: usize,
    /// Growth not yet charged, below one [`GROW_BLOCK`].
    uncharged: usize,
}

impl SourceLayers {
    /// A search from `source`, counted on `graph_ctx`'s per-query counters
    /// (`QueryMetrics::shortest_path_searches`).
    fn new(source: Vid, graph_ctx: &GraphExecutionContext) -> Self {
        if let Some(counters) = graph_ctx.counters() {
            counters.add_shortest_path_search();
        }
        let mut depth = FxHashMap::default();
        depth.insert(source, 0);
        Self {
            source,
            depth,
            predecessors: FxHashMap::default(),
            current_layer: vec![source],
            current_depth: 0,
            exhausted: false,
            charged: 0,
            uncharged: 0,
        }
    }
}

/// Walk `parent` back from `from` to the root, returning the path root-first.
fn rebuild_path(parent: &FxHashMap<Vid, Vid>, from: Vid) -> Vec<Vid> {
    let mut path = vec![from];
    let mut cur = from;
    while let Some(&p) = parent.get(&cur) {
        path.push(p);
        cur = p;
    }
    path.reverse();
    path
}

/// Charged per path held in `result`: the `Vec<Vid>` header plus its elements.
///
/// The elements dominate for anything but a one-hop path, but the header is not
/// negligible at the scale that matters — 64 000 four-hop paths are half header.
fn path_bytes(len: usize) -> usize {
    std::mem::size_of::<Vec<Vid>>() + len * std::mem::size_of::<Vid>()
}

/// Move `reservation` to `want` bytes for the caller whose current charge is
/// `have`, in whichever direction is needed, and update `have`.
///
/// Growth is the only direction that can fail. On failure `have` is left at the
/// last successful figure, so the caller can still give back exactly what it
/// took.
fn charge(reservation: &mut MemoryReservation, have: &mut usize, want: usize) -> DFResult<()> {
    if want > *have {
        reservation.try_grow(want - *have)?;
    } else {
        reservation.shrink(*have - want);
    }
    *have = want;
    Ok(())
}

impl GraphShortestPathStream {
    /// Charge `bytes` more of search state to the reservation, in blocks.
    ///
    /// `try_grow` takes the pool's lock, and a per-vertex call on a
    /// million-vertex search would cost more than the allocation it guards, so
    /// growth accumulates in `uncharged` and is charged a [`GROW_BLOCK`] at a
    /// time. `charged` is what this search holds against the reservation.
    fn charge_search(
        reservation: &mut MemoryReservation,
        charged: &mut usize,
        uncharged: &mut usize,
        bytes: usize,
    ) -> DFResult<()> {
        *uncharged += bytes;
        if *uncharged >= GROW_BLOCK {
            reservation.try_grow(*uncharged)?;
            *charged += *uncharged;
            *uncharged = 0;
        }
        Ok(())
    }

    /// The single-source search for `source`, reusing the retained one when it
    /// already starts there.
    ///
    /// A different source's search is released first: one is retained at a time,
    /// so the reservation holds at most one search's state beyond the batch.
    fn take_source_bfs(&mut self, source: Vid) -> SourceBfs {
        match self.source_bfs.take() {
            Some(bfs) if bfs.source == source => bfs,
            other => {
                if let Some(old) = other {
                    self.reservation.shrink(old.charged);
                }
                SourceBfs::new(source, &self.graph_ctx)
            }
        }
    }

    /// The shortest path from the search's source to `target`, growing the
    /// search only as far as `target` needs.
    ///
    /// Equivalent, path for path, to a fresh breadth-first search from the
    /// source that stops when it first sees `target`: the vertices are expanded
    /// in the same order, and `parent[v]` is the vertex `v` was first reached
    /// from, which does not depend on where an earlier call stopped. Stopping
    /// is at whole-vertex granularity, so finishing an expansion past the point
    /// the target appeared only discovers further vertices, never re-parents
    /// one.
    ///
    /// The caller has already handled `source == target` and `max_hops == 0`.
    ///
    /// # Memory
    ///
    /// The search carries a **predecessor per vertex**, not a path per vertex:
    /// `rebuild` walks `parent` back to the root for `O(1)` per vertex. Its state
    /// is charged to the reservation as it grows and stays charged while the
    /// search is retained (#261).
    ///
    /// # Errors
    ///
    /// Returns the pool's refusal when the search state does not fit.
    fn settle_shortest_path(
        &mut self,
        bfs: &mut SourceBfs,
        target: Vid,
    ) -> DFResult<Option<Vec<Vid>>> {
        loop {
            if bfs.parent.contains_key(&target) {
                return Ok(Some(rebuild_path(&bfs.parent, target)));
            }
            let Some(current) = bfs.queue.pop_front() else {
                return Ok(None);
            };
            let hops_taken = bfs.depth[&current];
            if hops_taken >= self.max_hops {
                continue;
            }
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
                    if let std::collections::hash_map::Entry::Vacant(slot) =
                        bfs.depth.entry(neighbor)
                    {
                        slot.insert(hops_taken + 1);
                        bfs.parent.insert(neighbor, current);
                        bfs.queue.push_back(neighbor);
                        Self::charge_search(
                            &mut self.reservation,
                            &mut bfs.charged,
                            &mut bfs.uncharged,
                            SOURCE_BFS_BYTES_PER_VERTEX,
                        )?;
                    }
                }
            }
        }
    }

    /// The layered search for `source`, reusing the retained one when it
    /// already starts there. See [`Self::take_source_bfs`].
    fn take_source_layers(&mut self, source: Vid) -> SourceLayers {
        match self.source_layers.take() {
            Some(layers) if layers.source == source => layers,
            other => {
                if let Some(old) = other {
                    self.reservation.shrink(old.charged);
                }
                SourceLayers::new(source, &self.graph_ctx)
            }
        }
    }

    /// Expand the layered search until `target` has a complete layer, or no
    /// further layer exists within `max_hops`.
    ///
    /// A layer is always expanded in full before `target` is checked, so once
    /// `target` has a depth, every predecessor one layer closer to the source is
    /// recorded — the same set a search dedicated to `target` would record.
    /// Unlike such a search, targets are not held back from the next layer:
    /// one target is an intermediate vertex on the way to another, and expanding
    /// it adds only predecessors of deeper vertices.
    ///
    /// # Errors
    ///
    /// Returns the pool's refusal when the search state does not fit.
    fn settle_layers(&mut self, layers: &mut SourceLayers, target: Vid) -> DFResult<()> {
        while !layers.depth.contains_key(&target) && !layers.exhausted {
            if layers.current_layer.is_empty() || layers.current_depth >= self.max_hops {
                layers.exhausted = true;
                break;
            }
            let next_depth = layers.current_depth + 1;
            let mut next_layer: Vec<Vid> = Vec::new();
            for &current in &layers.current_layer {
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
                        let bytes = match layers.depth.get(&neighbor) {
                            // Already discovered: a predecessor only at the same
                            // depth, i.e. another shortest route into it.
                            Some(&d) if d == next_depth => {
                                layers
                                    .predecessors
                                    .entry(neighbor)
                                    .or_default()
                                    .push(current);
                                std::mem::size_of::<Vid>()
                            }
                            Some(_) => continue,
                            None => {
                                layers.depth.insert(neighbor, next_depth);
                                layers
                                    .predecessors
                                    .entry(neighbor)
                                    .or_default()
                                    .push(current);
                                next_layer.push(neighbor);
                                SOURCE_LAYERS_BYTES_PER_VERTEX
                            }
                        };
                        Self::charge_search(
                            &mut self.reservation,
                            &mut layers.charged,
                            &mut layers.uncharged,
                            bytes,
                        )?;
                    }
                }
            }
            layers.current_layer = next_layer;
            layers.current_depth = next_depth;
        }
        Ok(())
    }

    /// All shortest paths from the layered search's source to `target`.
    ///
    /// The caller has already handled `source == target` and `max_hops == 0`.
    ///
    /// # Errors
    ///
    /// Returns the pool's refusal when the search state or the enumerated paths
    /// do not fit.
    fn all_shortest_paths_to(
        &mut self,
        layers: &mut SourceLayers,
        target: Vid,
    ) -> DFResult<Vec<Vec<Vid>>> {
        self.settle_layers(layers, target)?;
        if !layers.depth.contains_key(&target) {
            return Ok(vec![]);
        }
        let source = layers.source;

        // Enumerate all shortest paths via backward DFS from target to source.
        //
        // # Memory (#261)
        //
        // This is the unbounded step. The number of shortest paths between two
        // vertices is the *product* of the predecessor counts along the layers,
        // so it is combinatorial in the graph rather than linear in it: a graph
        // of 122 vertices and 3 280 edges yields 64 000 paths. Both `result` and
        // the stack's per-branch path clones grow with that count.
        //
        // **Not attributed to any recorded LDBC peak.** The obvious candidate
        // was IC14 -- an `allShortestPaths` query the remediation doc records as
        // "killed by hand after 111 min" at "19.2 GB and climbing". It was
        // tested: under a 1 GiB per-query ceiling IC14 against SF1 does refuse,
        // but on `GraphTraverseExec` asking 4.2 GB, which is upstream of this
        // operator and was already accounted. The enumeration here is never
        // reached. The evidence for this reservation is therefore its own
        // fixture in
        // `a_shortest_path_search_accounts_for_the_paths_it_enumerates`, and
        // nothing else.
        //
        // It cannot be bounded without changing the answer -- every shortest
        // path is a row the query asked for -- so accounting is the whole of the
        // remedy here, and what it buys is precisely what #261 claims for this
        // case: a refusal naming the operator instead of a process that grows
        // until the OS ends it.
        //
        // One running figure for what this call holds, reconciled against the
        // reservation by difference. `try_resize` is not usable here: the
        // reservation is shared with the retained search and the batch-level
        // charge in `compute_paths`, and resizing it to a per-call figure would
        // silently drop those.
        let mut local = 0usize;
        let mut result: Vec<Vec<Vid>> = Vec::new();
        let mut result_bytes = 0usize;
        let mut stack: Vec<(Vid, Vec<Vid>)> = vec![(target, vec![target])];
        let mut stack_bytes = path_bytes(1);

        while let Some((node, path)) = stack.pop() {
            stack_bytes -= path_bytes(path.len());
            if node == source {
                let mut full_path = path;
                full_path.reverse();
                result_bytes += path_bytes(full_path.len());
                result.push(full_path);
            } else if let Some(preds) = layers.predecessors.get(&node) {
                for &pred in preds {
                    let mut new_path = path.clone();
                    new_path.push(pred);
                    stack_bytes += path_bytes(new_path.len());
                    stack.push((pred, new_path));
                }
            }
            // Reconciled every iteration, in both directions: the stack shrinks
            // as well as grows, and a grow-only account of a structure that
            // shrinks reports a peak the operator never held.
            if let Err(e) = charge(
                &mut self.reservation,
                &mut local,
                result_bytes + stack_bytes,
            ) {
                self.reservation.shrink(local);
                return Err(e);
            }
        }

        // `result` is handed to the caller, which charges what it keeps; the
        // stack is gone.
        self.reservation.shrink(local);
        Ok(result)
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
    ///
    /// # Cost
    ///
    /// Rows are grouped by source, and each group is answered from **one**
    /// search from that source, grown only until every target in the group is
    /// settled (A8). An unbound target — `shortestPath((a {id: 0})-[*]->(b:N))`
    /// — arrives as the source cross-joined with every `N` node; answering each
    /// row with its own search cost `O(|B| x search)`, quadratic in the label
    /// size for one source. The search is retained across batches while the
    /// source stays the same, since a cross join emits a source's pairs over
    /// several batches. Output rows keep their input order.
    ///
    /// A pair with no path — a NULL endpoint included — is dropped unless the
    /// pattern is optional, in which case it is kept with a `None` path.
    fn compute_paths(
        &mut self,
        batch: RecordBatch,
    ) -> DFResult<(RecordBatch, Vec<Option<Vec<Vid>>>)> {
        // Give back the previous batch's paths. By the time this is called
        // again they have been through `build_output_batch` and belong to the
        // consumer; holding a reservation against them would double-count them
        // downstream.
        self.reservation.shrink(self.batch_path_bytes);
        self.batch_path_bytes = 0;
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

        // Rows that need a search, grouped by source in order of first
        // appearance. A NULL endpoint, a zero-length match and a zero hop bound
        // are answered without one.
        let mut group_of: FxHashMap<Vid, usize> = FxHashMap::default();
        let mut groups: Vec<(Vid, Vec<usize>)> = Vec::new();
        for i in 0..batch.num_rows() {
            if source_vids.is_null(i) || target_vids.is_null(i) {
                continue;
            }
            let source = Vid::from(source_vids.value(i));
            if source == Vid::from(target_vids.value(i)) || self.max_hops == 0 {
                continue;
            }
            let g = *group_of.entry(source).or_insert_with(|| {
                groups.push((source, Vec::new()));
                groups.len() - 1
            });
            groups[g].1.push(i);
        }

        // The answer for a row that needs no search. A zero-length path is only
        // a match when the pattern allows zero hops: `min_hops` defaults to 1,
        // so `[:T]` and `[:T*1..n]` must not report the source as reaching
        // itself for free.
        let min_hops = self.min_hops;
        let trivial = |i: usize| -> Option<Vec<Vid>> {
            if source_vids.is_null(i) || target_vids.is_null(i) {
                return None;
            }
            let source = Vid::from(source_vids.value(i));
            (source == Vid::from(target_vids.value(i)) && min_hops == 0).then(|| vec![source])
        };

        if self.all_shortest {
            // allShortestPaths: each input row can produce multiple output rows
            let mut per_row: Vec<Vec<Vec<Vid>>> = (0..batch.num_rows())
                .map(|i| trivial(i).into_iter().collect())
                .collect();
            for (source, rows) in &groups {
                let mut layers = self.take_source_layers(*source);
                for &i in rows {
                    let target = Vid::from(target_vids.value(i));
                    match self.all_shortest_paths_to(&mut layers, target) {
                        Ok(paths) => per_row[i] = paths,
                        Err(e) => {
                            // A refused search may be half a layer in; it is
                            // not reusable, so give its state back with it.
                            self.reservation.shrink(layers.charged);
                            return Err(e);
                        }
                    }
                }
                self.source_layers = Some(layers);
            }

            let mut row_indices: Vec<u32> = Vec::new();
            let mut all_paths: Vec<Option<Vec<Vid>>> = Vec::new();
            for (i, paths) in per_row.into_iter().enumerate() {
                if paths.is_empty() && self.optional {
                    row_indices.push(i as u32);
                    all_paths.push(None);
                }
                for path in paths {
                    row_indices.push(i as u32);
                    // Charged as it accumulates, not after: the point of the
                    // reservation is to stop a batch whose paths do not fit,
                    // and a charge levied once the vector is already resident
                    // records the peak instead of refusing it.
                    let grew = path_bytes(path.len());
                    self.reservation.try_grow(grew)?;
                    self.batch_path_bytes += grew;
                    all_paths.push(Some(path));
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
            let mut paths: Vec<Option<Vec<Vid>>> = (0..batch.num_rows()).map(trivial).collect();
            for p in paths.iter().flatten() {
                let grew = path_bytes(p.len());
                self.reservation.try_grow(grew)?;
                self.batch_path_bytes += grew;
            }
            for (source, rows) in &groups {
                let mut bfs = self.take_source_bfs(*source);
                for &i in rows {
                    let target = Vid::from(target_vids.value(i));
                    let found = self
                        .settle_shortest_path(&mut bfs, target)
                        .and_then(|path| {
                            if let Some(p) = &path {
                                let grew = path_bytes(p.len());
                                self.reservation.try_grow(grew)?;
                                self.batch_path_bytes += grew;
                            }
                            Ok(path)
                        });
                    match found {
                        Ok(path) => paths[i] = path,
                        Err(e) => {
                            // The reservation outlives the search -- it is a
                            // field on the stream -- so a refusal gives back
                            // what the search charged before it propagates, or
                            // the budget stays spent for the rest of the query.
                            self.reservation.shrink(bfs.charged);
                            return Err(e);
                        }
                    }
                }
                self.source_bfs = Some(bfs);
            }

            if self.optional || paths.iter().all(Option::is_some) {
                return Ok((batch, paths));
            }
            let kept: Vec<u32> = paths
                .iter()
                .enumerate()
                .filter(|(_, p)| p.is_some())
                .map(|(i, _)| i as u32)
                .collect();
            let indices = UInt32Array::from(kept);
            let kept_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), &indices, None).map_err(arrow_err))
                .collect::<DFResult<Vec<_>>>()?;
            let kept_batch =
                RecordBatch::try_new(batch.schema(), kept_columns).map_err(arrow_err)?;
            paths.retain(Option::is_some);
            Ok((kept_batch, paths))
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
                            if paths.is_empty() {
                                // Every pair was dropped: nothing to prefetch
                                // or emit, so read on.
                                self.state = ShortestPathStreamState::Reading;
                                continue;
                            }
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
                            // No row is left to reuse a retained search.
                            let held = self.source_bfs.take().map_or(0, |b| b.charged)
                                + self.source_layers.take().map_or(0, |l| l.charged);
                            self.reservation.shrink(held);
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
