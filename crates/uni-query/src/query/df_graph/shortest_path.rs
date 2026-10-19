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
//! bidirectional meet would choose differently. The expansion order is
//! deterministic: edge types in pattern order, then outgoing before incoming
//! for an undirected pattern, then neighbors as the adjacency returns them,
//! ordered by `(neighbor vid, eid)`. The same query over the same graph
//! therefore returns the same path every time, whichever storage tier holds
//! its edges.
//!
//! # Relationships
//!
//! The search records, with each vertex's predecessor, the **edge** it was
//! reached over, its type and its stored orientation (`Hop`). The path's
//! relationships are read from that record. They used to be looked up again
//! after the search, by scanning the predecessor's neighbors for the first
//! admitted edge to the next vertex, twice per hop (once to prefetch, once to
//! build), plus an adjacency probe per hop to recover the stored orientation.
//! That was most of the cost of a many-target query, and it was wrong for
//! `allShortestPaths` over parallel edges: each parallel edge makes a distinct
//! path, and the rows for them came back carrying the same relationship. A target filter such as
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
    EdgeAppendCtx, EntityPropertyCache, append_edge_to_struct_with, arrow_err, column_as_vid_array,
    compute_plan_properties, edge_struct_fields, exec_err, new_node_list_builder,
};
use crate::query::df_graph::traverse::{ReachableMapFilter, build_edge_property_filter};
use arrow::compute::take;
use arrow_array::builder::{ListBuilder, StructBuilder, UInt64Builder};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
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
use uni_store::storage::arrow_convert;
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

    /// Map entries whose value depends on the row — `{w: a.x}` — as the
    /// property name and the value compiled against the input schema. Each
    /// distinct set of values gets its own allow-set, which also holds
    /// `edge_property_conditions`. Empty when every entry is a constant.
    row_conditions: Vec<(String, Arc<dyn PhysicalExpr>)>,

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
            .field("row_conditions", &self.row_conditions)
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
    /// * `edge_property_conditions` - The relationship map's constant entries
    /// * `row_conditions` - The map's entries computed per row, compiled
    ///   against `input`'s schema
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
        row_conditions: Vec<(String, Arc<dyn PhysicalExpr>)>,
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
            row_conditions,
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
            self.row_conditions.clone(),
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
        let warm_max_hops = self.max_hops as usize;
        let per_row = !self.row_conditions.is_empty();
        let warm_fut: Pin<
            Box<dyn std::future::Future<Output = DFResult<WarmedEdgeFilter>> + Send>,
        > = Box::pin(async move {
            warm_ctx
                .ensure_adjacency_warmed(&warm_types, warm_direction)
                .await
                .map_err(exec_err)?;
            // A map with per-row values builds one allow-set per distinct
            // value set, batch by batch; the constant entries join each.
            if per_row {
                return Ok(WarmedEdgeFilter::Whole(EidFilter::AllAllowed));
            }
            // The map is read over the region each batch's sources can
            // reach, not over the whole type up front (P7).
            if let Some(reachable) = ReachableMapFilter::new(
                &warm_ctx,
                &warm_types,
                warm_direction,
                warm_max_hops,
                &warm_conditions,
                None,
                &[],
            ) {
                return Ok(WarmedEdgeFilter::Reachable(Box::new(reachable)));
            }
            build_edge_property_filter(&warm_ctx, &warm_types, warm_direction, &warm_conditions)
                .await
                .map(WarmedEdgeFilter::Whole)
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
            adjacency_reads: 0,
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
            reachable: None,
            edge_property_conditions: self.edge_property_conditions.clone(),
            row_conditions: self.row_conditions.clone(),
            keyed_filters: FxHashMap::default(),
            keyed_filter_bytes: 0,
            batch_keys: Vec::new(),
            row_keys: Vec::new(),
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

/// The edge filter warming produces.
enum WarmedEdgeFilter {
    /// Complete for every walk: no map, or the whole-type pre-scan.
    Whole(EidFilter),
    /// Grown per batch over the region its sources reach.
    Reachable(Box<ReachableMapFilter>),
}

/// The allow-set of one distinct set of per-row map values.
enum KeyedFilter {
    /// Grown per batch over the region its rows' sources reach.
    Reachable(Box<ReachableMapFilter>),
    /// Complete for every walk: a `NULL` value, which admits no edge, or the
    /// whole-type form when no edge type resolves to a name.
    Whole(EidFilter),
}

impl KeyedFilter {
    /// Approximate bytes held, for the reservation.
    fn approx_bytes(&self) -> usize {
        match self {
            KeyedFilter::Reachable(r) => r.approx_bytes(),
            KeyedFilter::Whole(f) => f.approx_bytes(),
        }
    }
}

/// Future that covers a batch's sources in every allow-set the batch needs.
type CoverKeysFuture =
    Pin<Box<dyn std::future::Future<Output = DFResult<Vec<(Vec<UniValue>, KeyedFilter)>>> + Send>>;

/// State machine for shortest path stream execution.
enum ShortestPathStreamState {
    /// Warming adjacency CSRs and building the edge filter before first batch.
    Warming(Pin<Box<dyn std::future::Future<Output = DFResult<WarmedEdgeFilter>> + Send>>),
    /// Processing input batches.
    Reading,
    /// Growing the map's region to this batch's sources before searching.
    Covering {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<ReachableMapFilter>> + Send>>,
        batch: RecordBatch,
    },
    /// Building or growing, for a map with per-row values, the allow-set of
    /// each distinct value set in this batch over its rows' sources.
    CoveringKeys {
        fut: CoverKeysFuture,
        batch: RecordBatch,
    },
    /// Fetching storage-backed properties for this batch's path entities before
    /// the synchronous column builders run. Path element structs carry user
    /// properties in a `properties` blob whose only synchronous accessor reads
    /// the L0 write buffers alone, so a flushed entity would come back with no
    /// properties. See [`EntityPropertyCache`].
    PrefetchingProperties {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<EntityPropertyCache>> + Send>>,
        input: RecordBatch,
        paths: Vec<Option<FoundPath>>,
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
    /// Superseded by `reachable` when that is set.
    edge_property_filter: EidFilter,

    /// The map read over the region the sources so far reach (P7). Its
    /// allow-set only grows, so a search retained across batches stays
    /// valid. Taken out while a batch's sources are being covered.
    reachable: Option<ReachableMapFilter>,

    /// The map's constant entries, joined to every per-row allow-set.
    edge_property_conditions: Vec<(String, UniValue)>,

    /// The map's entries computed per row; see the exec's field.
    row_conditions: Vec<(String, Arc<dyn PhysicalExpr>)>,

    /// One allow-set per distinct set of per-row values seen so far, so rows
    /// of later batches with the same values reuse it. A group's set is moved
    /// into `reachable` / `edge_property_filter` while the group is searched.
    keyed_filters: FxHashMap<Vec<UniValue>, KeyedFilter>,

    /// Bytes of `reservation` charged to `keyed_filters`.
    keyed_filter_bytes: usize,

    /// The distinct per-row value sets of the batch in flight.
    batch_keys: Vec<Vec<UniValue>>,

    /// For each row of the batch in flight, its value set's index into
    /// `batch_keys`. Empty when the map has no per-row entry.
    row_keys: Vec<usize>,

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

    /// Per-vertex adjacency lookups made since the counters were last told,
    /// reported as `QueryMetrics::shortest_path_adjacency_reads` once per
    /// batch rather than per lookup, so the hot loop touches no shared atomic.
    adjacency_reads: u64,

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
const SOURCE_BFS_BYTES_PER_VERTEX: usize = std::mem::size_of::<(Vid, Step)>()
    + std::mem::size_of::<(Vid, u32)>()
    + std::mem::size_of::<Vid>();

/// Charged per vertex a [`SourceLayers`] discovers: its `depth` entry, its
/// `predecessors` entry with the first predecessor, and its layer slot. A
/// further predecessor is charged one [`Step`].
const SOURCE_LAYERS_BYTES_PER_VERTEX: usize = std::mem::size_of::<(Vid, u32)>()
    + std::mem::size_of::<(Vid, Vec<Step>)>()
    + std::mem::size_of::<Step>()
    + std::mem::size_of::<Vid>();

/// A breadth-first search from one source, grown on demand for `shortestPath`.
///
/// Shared by every row with this source: each target settles it a little
/// further, and a target it has already passed costs one map lookup.
struct SourceBfs {
    /// Where the search starts.
    source: Vid,
    /// The per-row map values the search is gated by; empty without any.
    key: Vec<UniValue>,
    /// `parent[v]` is the vertex `v` was first reached from and the edge it
    /// was reached over. The source has no entry, which is what terminates
    /// [`rebuild_path`].
    parent: FxHashMap<Vid, Step>,
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
    fn new(source: Vid, key: Vec<UniValue>, graph_ctx: &GraphExecutionContext) -> Self {
        if let Some(counters) = graph_ctx.counters() {
            counters.add_shortest_path_search();
        }
        let mut depth = FxHashMap::default();
        depth.insert(source, 0);
        Self {
            source,
            key,
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
    /// The per-row map values the search is gated by; empty without any.
    key: Vec<UniValue>,
    /// Hops from the source for every vertex discovered, the source included.
    depth: FxHashMap<Vid, u32>,
    /// Every vertex one layer closer to the source with an admitted edge into
    /// the key, once per such edge, with that edge.
    predecessors: FxHashMap<Vid, Vec<Step>>,
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
    fn new(source: Vid, key: Vec<UniValue>, graph_ctx: &GraphExecutionContext) -> Self {
        if let Some(counters) = graph_ctx.counters() {
            counters.add_shortest_path_search();
        }
        let mut depth = FxHashMap::default();
        depth.insert(source, 0);
        Self {
            source,
            key,
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

/// One hop of a path: the edge taken, as the search found it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Hop {
    /// The edge.
    eid: Eid,
    /// The edge type it was found under.
    type_id: u32,
    /// Whether the edge is stored `(from -> to)` in walk order: found in the
    /// outgoing adjacency of the vertex the hop leaves.
    forward: bool,
}

/// How a vertex was reached: the vertex one hop closer to the source, and the
/// edge from it.
type Step = (Vid, Hop);

/// A path the operator emits: `nodes[i] -[hops[i]]- nodes[i + 1]`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FoundPath {
    nodes: Vec<Vid>,
    hops: Vec<Hop>,
}

impl FoundPath {
    /// The zero-length path at `v`.
    fn at(v: Vid) -> Self {
        Self {
            nodes: vec![v],
            hops: Vec::new(),
        }
    }
}

/// Walk `parent` back from `from` to the root, returning the path root-first.
fn rebuild_path(parent: &FxHashMap<Vid, Step>, from: Vid) -> FoundPath {
    let mut nodes = vec![from];
    let mut hops = Vec::new();
    let mut cur = from;
    while let Some(&(p, hop)) = parent.get(&cur) {
        nodes.push(p);
        hops.push(hop);
        cur = p;
    }
    nodes.reverse();
    hops.reverse();
    FoundPath { nodes, hops }
}

/// Charged per path of `len` nodes held in `result`: the two vector headers
/// plus the nodes and the hops between them.
///
/// The elements dominate for anything but a one-hop path, but the headers are
/// not negligible at the scale that matters — 64 000 four-hop paths are about
/// a third header.
fn path_bytes(len: usize) -> usize {
    std::mem::size_of::<FoundPath>()
        + len * std::mem::size_of::<Vid>()
        + len.saturating_sub(1) * std::mem::size_of::<Hop>()
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

    /// Collect into `out` every admitted edge out of `current`, in expansion
    /// order, as the step it makes; returns the adjacency lookups made.
    ///
    /// An undirected pattern looks the two directions up separately, outgoing
    /// first, so each edge comes back knowing its stored orientation. A
    /// self-loop is listed by both and so twice, which neither search minds:
    /// its far end is `current`, already discovered at a shallower depth.
    fn admitted_steps(&self, current: Vid, out: &mut Vec<(Vid, Hop)>) -> u64 {
        out.clear();
        let mut reads = 0;
        for &type_id in &self.edge_type_ids {
            for &dir in self.direction.expand() {
                reads += 1;
                let forward = dir == Direction::Outgoing;
                for (neighbor, eid) in self.graph_ctx.get_neighbors(current, type_id, dir) {
                    // The property map gates expansion, not the result set: the
                    // shortest path over permitted edges is not the shortest
                    // path filtered afterwards.
                    if self.edge_filter().contains(eid) {
                        out.push((
                            neighbor,
                            Hop {
                                eid,
                                type_id,
                                forward,
                            },
                        ));
                    }
                }
            }
        }
        reads
    }

    /// The allow-set the search gates edges on.
    fn edge_filter(&self) -> &EidFilter {
        match &self.reachable {
            Some(r) => &r.edges()[0],
            None => &self.edge_property_filter,
        }
    }

    /// Report the adjacency lookups made so far to the query's counters.
    fn flush_adjacency_reads(&mut self) {
        if self.adjacency_reads > 0 {
            if let Some(counters) = self.graph_ctx.counters() {
                counters.add_shortest_path_adjacency_reads(self.adjacency_reads);
            }
            self.adjacency_reads = 0;
        }
    }

    /// The single-source search for `source` under the per-row map values
    /// `key`, reusing the retained one when it already starts there with the
    /// same values.
    ///
    /// A different source's search is released first: one is retained at a time,
    /// so the reservation holds at most one search's state beyond the batch.
    fn take_source_bfs(&mut self, source: Vid, key: &[UniValue]) -> SourceBfs {
        match self.source_bfs.take() {
            Some(bfs) if bfs.source == source && bfs.key == key => bfs,
            other => {
                if let Some(old) = other {
                    self.reservation.shrink(old.charged);
                }
                SourceBfs::new(source, key.to_vec(), &self.graph_ctx)
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
    ) -> DFResult<Option<FoundPath>> {
        let mut steps = Vec::new();
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
            self.adjacency_reads += self.admitted_steps(current, &mut steps);
            for &(neighbor, hop) in &steps {
                if let std::collections::hash_map::Entry::Vacant(slot) = bfs.depth.entry(neighbor) {
                    slot.insert(hops_taken + 1);
                    // The first admitted edge in expansion order is the one
                    // the path reports, for the parent it settles on.
                    bfs.parent.insert(neighbor, (current, hop));
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

    /// The layered search for `source`, reusing the retained one when it
    /// already starts there. See [`Self::take_source_bfs`].
    fn take_source_layers(&mut self, source: Vid, key: &[UniValue]) -> SourceLayers {
        match self.source_layers.take() {
            Some(layers) if layers.source == source && layers.key == key => layers,
            other => {
                if let Some(old) = other {
                    self.reservation.shrink(old.charged);
                }
                SourceLayers::new(source, key.to_vec(), &self.graph_ctx)
            }
        }
    }

    /// Evaluate the per-row map values of `batch` into `batch_keys` and
    /// `row_keys`. A no-op when the map has no per-row entry.
    ///
    /// # Errors
    ///
    /// Returns an evaluation or decoding error.
    fn evaluate_row_keys(&mut self, batch: &RecordBatch) -> DFResult<()> {
        self.batch_keys.clear();
        self.row_keys.clear();
        if self.row_conditions.is_empty() {
            return Ok(());
        }
        let n = batch.num_rows();
        let columns: Vec<ArrayRef> = self
            .row_conditions
            .iter()
            .map(|(_, expr)| expr.evaluate(batch).and_then(|v| v.into_array(n)))
            .collect::<DFResult<_>>()?;
        let mut index: FxHashMap<Vec<UniValue>, usize> = FxHashMap::default();
        for row in 0..n {
            let key = columns
                .iter()
                .map(|c| arrow_convert::arrow_to_value(c.as_ref(), row, None))
                .collect::<anyhow::Result<Vec<UniValue>>>()
                .map_err(exec_err)?;
            let k = match index.get(&key) {
                Some(&k) => k,
                None => {
                    index.insert(key.clone(), self.batch_keys.len());
                    self.batch_keys.push(key);
                    self.batch_keys.len() - 1
                }
            };
            self.row_keys.push(k);
        }
        Ok(())
    }

    /// The future that builds or grows the allow-set of every value set in
    /// the batch in flight, each over the sources of its own rows.
    ///
    /// The sets it returns go back into `keyed_filters`.
    ///
    /// # Errors
    ///
    /// Returns an error when the source column cannot be read.
    fn cover_keys(&mut self, batch: &RecordBatch) -> DFResult<CoverKeysFuture> {
        let sources = batch
            .column_by_name(&self.source_column)
            .map(|c| column_as_vid_array(c.as_ref()))
            .transpose()?;
        let mut per_key: Vec<Vec<Vid>> = vec![Vec::new(); self.batch_keys.len()];
        if let Some(sources) = sources {
            let mut seen: rustc_hash::FxHashSet<(usize, u64)> = Default::default();
            for (row, &k) in self.row_keys.iter().enumerate() {
                if sources.is_valid(row) && seen.insert((k, sources.value(row))) {
                    per_key[k].push(Vid::from(sources.value(row)));
                }
            }
        }
        let work: Vec<(Vec<UniValue>, Option<KeyedFilter>, Vec<Vid>)> = self
            .batch_keys
            .iter()
            .zip(per_key)
            .map(|(key, srcs)| (key.clone(), self.keyed_filters.remove(key), srcs))
            .collect();
        let graph_ctx = Arc::clone(&self.graph_ctx);
        let edge_type_ids = self.edge_type_ids.clone();
        let direction = self.direction;
        let max_hops = self.max_hops as usize;
        let constant = self.edge_property_conditions.clone();
        let names: Vec<String> = self.row_conditions.iter().map(|(n, _)| n.clone()).collect();
        Ok(Box::pin(async move {
            let mut out = Vec::with_capacity(work.len());
            for (key, existing, srcs) in work {
                let filter = match existing {
                    Some(f) => f,
                    // Cypher equality with `NULL` is never true: the map
                    // admits no edge, and no read is needed to know it.
                    None if key.iter().any(UniValue::is_null) => {
                        KeyedFilter::Whole(EidFilter::Sparse(Default::default()))
                    }
                    None => {
                        let mut conditions = constant.clone();
                        conditions.extend(names.iter().cloned().zip(key.iter().cloned()));
                        match ReachableMapFilter::new(
                            &graph_ctx,
                            &edge_type_ids,
                            direction,
                            max_hops,
                            &conditions,
                            None,
                            &[],
                        ) {
                            Some(r) => KeyedFilter::Reachable(Box::new(r)),
                            None => KeyedFilter::Whole(
                                build_edge_property_filter(
                                    &graph_ctx,
                                    &edge_type_ids,
                                    direction,
                                    &conditions,
                                )
                                .await?,
                            ),
                        }
                    }
                };
                let filter = match filter {
                    KeyedFilter::Reachable(mut r) => {
                        r.cover(&srcs).await?;
                        KeyedFilter::Reachable(r)
                    }
                    whole => whole,
                };
                out.push((key, filter));
            }
            Ok(out)
        }))
    }

    /// Put covered allow-sets back and charge what they hold.
    ///
    /// # Errors
    ///
    /// Returns the pool's refusal when the allow-sets do not fit.
    fn store_keyed_filters(&mut self, covered: Vec<(Vec<UniValue>, KeyedFilter)>) -> DFResult<()> {
        self.keyed_filters.extend(covered);
        let want: usize = self
            .keyed_filters
            .iter()
            .map(|(k, f)| f.approx_bytes() + k.len() * std::mem::size_of::<UniValue>())
            .sum();
        charge(&mut self.reservation, &mut self.keyed_filter_bytes, want)
    }

    /// Make the allow-set of the batch's value set `k` the one the search
    /// gates on. A no-op when the map has no per-row entry.
    fn activate_key(&mut self, k: usize) -> DFResult<()> {
        if self.row_conditions.is_empty() {
            return Ok(());
        }
        match self.keyed_filters.remove(&self.batch_keys[k]) {
            Some(KeyedFilter::Reachable(r)) => self.reachable = Some(*r),
            Some(KeyedFilter::Whole(f)) => {
                self.reachable = None;
                self.edge_property_filter = f;
            }
            None => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "shortest path: no allow-set for a value set of this batch".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Return the allow-set [`Self::activate_key`] took for value set `k`.
    fn deactivate_key(&mut self, k: usize) {
        if self.row_conditions.is_empty() {
            return;
        }
        let filter = match self.reachable.take() {
            Some(r) => KeyedFilter::Reachable(Box::new(r)),
            None => KeyedFilter::Whole(std::mem::replace(
                &mut self.edge_property_filter,
                EidFilter::AllAllowed,
            )),
        };
        self.keyed_filters
            .insert(self.batch_keys[k].clone(), filter);
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
            let mut steps = Vec::new();
            for &current in &layers.current_layer {
                // Gating the forward pass is sufficient for the backward
                // reconstruction too: it walks `predecessors`, which is built
                // only from edges admitted here.
                self.adjacency_reads += self.admitted_steps(current, &mut steps);
                for &(neighbor, hop) in &steps {
                    let bytes = match layers.depth.get(&neighbor) {
                        // Already discovered: a predecessor only at the same
                        // depth, i.e. another shortest route into it. A
                        // parallel edge from the same vertex is one too: it
                        // is a different path.
                        Some(&d) if d == next_depth => {
                            layers
                                .predecessors
                                .entry(neighbor)
                                .or_default()
                                .push((current, hop));
                            std::mem::size_of::<Step>()
                        }
                        Some(_) => continue,
                        None => {
                            layers.depth.insert(neighbor, next_depth);
                            layers
                                .predecessors
                                .entry(neighbor)
                                .or_default()
                                .push((current, hop));
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
    ) -> DFResult<Vec<FoundPath>> {
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
        let mut result: Vec<FoundPath> = Vec::new();
        let mut result_bytes = 0usize;
        // Paths are built target-first and reversed once complete.
        let mut stack: Vec<(Vid, FoundPath)> = vec![(target, FoundPath::at(target))];
        let mut stack_bytes = path_bytes(1);

        while let Some((node, path)) = stack.pop() {
            stack_bytes -= path_bytes(path.nodes.len());
            if node == source {
                let mut full_path = path;
                full_path.nodes.reverse();
                full_path.hops.reverse();
                result_bytes += path_bytes(full_path.nodes.len());
                result.push(full_path);
            } else if let Some(preds) = layers.predecessors.get(&node) {
                for &(pred, hop) in preds {
                    let mut new_path = path.clone();
                    new_path.nodes.push(pred);
                    new_path.hops.push(hop);
                    stack_bytes += path_bytes(new_path.nodes.len());
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
    /// structs, for [`EntityPropertyCache::prefetch`].
    fn path_entities(paths: &[Option<FoundPath>]) -> (Vec<Vid>, Vec<Eid>) {
        let mut vids = Vec::new();
        let mut eids = Vec::new();
        for path in paths.iter().flatten() {
            vids.extend_from_slice(&path.nodes);
            eids.extend(path.hops.iter().map(|h| h.eid));
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
    ) -> DFResult<(RecordBatch, Vec<Option<FoundPath>>)> {
        let computed = self.compute_paths_inner(batch);
        self.flush_adjacency_reads();
        computed
    }

    /// [`Self::compute_paths`] without the counter bookkeeping.
    fn compute_paths_inner(
        &mut self,
        batch: RecordBatch,
    ) -> DFResult<(RecordBatch, Vec<Option<FoundPath>>)> {
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

        // Rows that need a search, grouped by source — and by the value set of
        // a map with per-row values, which decides the edges the search may
        // take — in order of first appearance. A NULL endpoint, a zero-length
        // match and a zero hop bound are answered without one.
        let mut group_of: FxHashMap<(Vid, usize), usize> = FxHashMap::default();
        let mut groups: Vec<(Vid, usize, Vec<usize>)> = Vec::new();
        for i in 0..batch.num_rows() {
            if source_vids.is_null(i) || target_vids.is_null(i) {
                continue;
            }
            let source = Vid::from(source_vids.value(i));
            if source == Vid::from(target_vids.value(i)) || self.max_hops == 0 {
                continue;
            }
            let key = self.row_keys.get(i).copied().unwrap_or(0);
            let g = *group_of.entry((source, key)).or_insert_with(|| {
                groups.push((source, key, Vec::new()));
                groups.len() - 1
            });
            groups[g].2.push(i);
        }

        // The answer for a row that needs no search. A zero-length path is only
        // a match when the pattern allows zero hops: `min_hops` defaults to 1,
        // so `[:T]` and `[:T*1..n]` must not report the source as reaching
        // itself for free.
        let min_hops = self.min_hops;
        let trivial = |i: usize| -> Option<FoundPath> {
            if source_vids.is_null(i) || target_vids.is_null(i) {
                return None;
            }
            let source = Vid::from(source_vids.value(i));
            (source == Vid::from(target_vids.value(i)) && min_hops == 0)
                .then(|| FoundPath::at(source))
        };

        if self.all_shortest {
            // allShortestPaths: each input row can produce multiple output rows
            let mut per_row: Vec<Vec<FoundPath>> = (0..batch.num_rows())
                .map(|i| trivial(i).into_iter().collect())
                .collect();
            for (source, k, rows) in &groups {
                self.activate_key(*k)?;
                let mut layers = {
                    let key = self.batch_keys.get(*k).cloned().unwrap_or_default();
                    self.take_source_layers(*source, &key)
                };
                for &i in rows {
                    let target = Vid::from(target_vids.value(i));
                    match self.all_shortest_paths_to(&mut layers, target) {
                        Ok(paths) => per_row[i] = paths,
                        Err(e) => {
                            // A refused search may be half a layer in; it is
                            // not reusable, so give its state back with it.
                            self.reservation.shrink(layers.charged);
                            self.deactivate_key(*k);
                            return Err(e);
                        }
                    }
                }
                self.source_layers = Some(layers);
                self.deactivate_key(*k);
            }

            let mut row_indices: Vec<u32> = Vec::new();
            let mut all_paths: Vec<Option<FoundPath>> = Vec::new();
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
                    let grew = path_bytes(path.nodes.len());
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
            let mut paths: Vec<Option<FoundPath>> = (0..batch.num_rows()).map(trivial).collect();
            for p in paths.iter().flatten() {
                let grew = path_bytes(p.nodes.len());
                self.reservation.try_grow(grew)?;
                self.batch_path_bytes += grew;
            }
            for (source, k, rows) in &groups {
                self.activate_key(*k)?;
                let mut bfs = {
                    let key = self.batch_keys.get(*k).cloned().unwrap_or_default();
                    self.take_source_bfs(*source, &key)
                };
                for &i in rows {
                    let target = Vid::from(target_vids.value(i));
                    let found = self
                        .settle_shortest_path(&mut bfs, target)
                        .and_then(|path| {
                            if let Some(p) = &path {
                                let grew = path_bytes(p.nodes.len());
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
                            self.deactivate_key(*k);
                            return Err(e);
                        }
                    }
                }
                self.source_bfs = Some(bfs);
                self.deactivate_key(*k);
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
        paths: &[Option<FoundPath>],
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
                Some(FoundPath { nodes, hops }) => {
                    // Add all nodes
                    for &vid in nodes {
                        super::common::append_node_to_struct_with(
                            nodes_builder.values(),
                            vid,
                            &query_ctx,
                            prop_cache,
                        );
                    }
                    nodes_builder.append(true);

                    // Each hop carries the edge the search took, its type and
                    // its stored orientation, so nothing is looked up again.
                    for (window, hop) in nodes.windows(2).zip(hops) {
                        let (src, dst) = if hop.forward {
                            (window[0], window[1])
                        } else {
                            (window[1], window[0])
                        };
                        let type_name = edge_ctx.type_name(hop.eid, Some(hop.type_id));
                        append_edge_to_struct_with(
                            rels_builder.values(),
                            hop.eid,
                            &type_name,
                            src.as_u64(),
                            dst.as_u64(),
                            &query_ctx,
                            prop_cache,
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
                    let values: Vec<u64> = p.nodes.iter().map(|v| v.as_u64()).collect();
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
            .map(|p| p.as_ref().map(|path| path.hops.len() as u64))
            .collect();
        columns.push(Arc::new(UInt64Array::from(lengths)));

        // Appended last, matching `build_schema`.
        if let Some(step_array) = step_array {
            columns.push(step_array);
        }

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(arrow_err)
    }
}

impl GraphShortestPathStream {
    /// Search `batch` and start prefetching its paths' properties.
    ///
    /// Returns the error to emit, if any; otherwise leaves the stream in the
    /// state to continue from (`PrefetchingProperties`, or `Reading` when every
    /// pair was dropped).
    fn search_batch(&mut self, batch: RecordBatch) -> Option<DFResult<RecordBatch>> {
        let (input, paths) = match self.compute_paths(batch) {
            Ok(pair) => pair,
            Err(e) => {
                self.state = ShortestPathStreamState::Reading;
                return Some(Err(e));
            }
        };
        if paths.is_empty() {
            // Every pair was dropped: nothing to prefetch or emit, so read on.
            self.state = ShortestPathStreamState::Reading;
            return None;
        }
        let (vids, eids) = Self::path_entities(&paths);
        let graph_ctx = self.graph_ctx.clone();
        let fut = Box::pin(async move {
            let query_ctx = graph_ctx.query_context();
            EntityPropertyCache::prefetch(&graph_ctx, &query_ctx, &vids, &eids).await
        });
        self.state = ShortestPathStreamState::PrefetchingProperties { fut, input, paths };
        None
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
                    Poll::Ready(Ok(warmed)) => {
                        match warmed {
                            WarmedEdgeFilter::Whole(filter) => self.edge_property_filter = filter,
                            WarmedEdgeFilter::Reachable(r) => self.reachable = Some(*r),
                        }
                        self.state = ShortestPathStreamState::Reading;
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
                    if let Err(e) = self.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(exec_err(e))));
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            if !self.row_conditions.is_empty() {
                                // Evaluate the per-row values, then cover each
                                // value set's sources before any row is searched.
                                let fut = self
                                    .evaluate_row_keys(&batch)
                                    .and_then(|()| self.cover_keys(&batch));
                                match fut {
                                    Ok(fut) => {
                                        self.state =
                                            ShortestPathStreamState::CoveringKeys { fut, batch };
                                        continue;
                                    }
                                    Err(e) => {
                                        self.state = ShortestPathStreamState::Done;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                }
                            }
                            if let Some(mut reachable) = self.reachable.take() {
                                // Grow the map's region to this batch's sources
                                // before any of its rows is searched.
                                let sources = match batch
                                    .column_by_name(&self.source_column)
                                    .map(|c| column_as_vid_array(c.as_ref()))
                                    .transpose()
                                {
                                    Ok(col) => col
                                        .map(|c| ReachableMapFilter::sources_of(&c))
                                        .unwrap_or_default(),
                                    Err(e) => {
                                        self.state = ShortestPathStreamState::Done;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                };
                                let fut = Box::pin(async move {
                                    reachable.cover(&sources).await?;
                                    Ok(reachable)
                                });
                                self.state = ShortestPathStreamState::Covering { fut, batch };
                                continue;
                            }
                            if let Some(out) = self.search_batch(batch) {
                                return Poll::Ready(Some(out));
                            }
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.state = ShortestPathStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // No row is left to reuse a retained search.
                            let held = self.source_bfs.take().map_or(0, |b| b.charged)
                                + self.source_layers.take().map_or(0, |l| l.charged)
                                + std::mem::take(&mut self.keyed_filter_bytes);
                            self.keyed_filters.clear();
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
                ShortestPathStreamState::Covering { mut fut, batch } => match fut.as_mut().poll(cx)
                {
                    Poll::Ready(Ok(reachable)) => {
                        self.reachable = Some(reachable);
                        if let Some(out) = self.search_batch(batch) {
                            return Poll::Ready(Some(out));
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = ShortestPathStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = ShortestPathStreamState::Covering { fut, batch };
                        return Poll::Pending;
                    }
                },
                ShortestPathStreamState::CoveringKeys { mut fut, batch } => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok(covered)) => {
                            if let Err(e) = self.store_keyed_filters(covered) {
                                self.state = ShortestPathStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                            if let Some(out) = self.search_batch(batch) {
                                return Poll::Ready(Some(out));
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            self.state = ShortestPathStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => {
                            self.state = ShortestPathStreamState::CoveringKeys { fut, batch };
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
