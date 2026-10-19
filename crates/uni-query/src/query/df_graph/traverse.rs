// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph traversal execution plans for DataFusion.
//!
//! This module provides graph traversal operators as DataFusion [`ExecutionPlan`]s:
//!
//! - [`GraphTraverseExec`]: Single-hop edge traversal
//! - [`GraphVariableLengthTraverseExec`]: Multi-hop BFS traversal (min..max hops)
//!
//! # Traversal Algorithm
//!
//! Traversal uses the CSR adjacency cache for O(1) neighbor lookups:
//!
//! ```text
//! Input Stream (source VIDs)
//!        │
//!        ▼
//! ┌──────────────────┐
//! │ For each batch:  │
//! │  1. Extract VIDs │
//! │  2. get_neighbors│
//! │  3. Expand rows  │
//! └──────────────────┘
//!        │
//!        ▼
//! Output Stream (source, edge, target)
//! ```
//!
//! L0 buffers are automatically overlaid for MVCC visibility.

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::bitmap::{EidFilter, VidFilter};
use crate::query::df_graph::common::{
    EdgeAppendCtx, EntityPropertyCache, append_edge_to_struct_with, append_node_to_struct_with,
    append_traversed_edge, arrow_err, build_edge_list_field, build_node_list_field,
    build_path_struct_field, column_as_vid_array, compute_plan_properties, exec_err,
    labels_data_type, new_edge_list_builder, new_node_list_builder,
};
use crate::query::df_graph::nfa::{
    NfaStateId, PathNfa, PathSelector, QppGroupBinding, QppGroupKind, VlpOutputMode,
};
use crate::query::df_graph::pred_dag::{PathEnumerator, PredecessorDag};
use crate::query::df_graph::scan::{
    PrefetchedProps, build_property_column_static, property_field, resolve_property_type,
};
use crate::query::planner::COL_FWD;
use arrow::compute::take;
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use std::ops::ControlFlow;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use rustc_hash::FxHashSet;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value as UniValue;
use uni_common::core::id::{Eid, Vid};
use uni_query_functions::expr_eval::cypher_eq;
use uni_store::storage::direction::Direction;

/// BFS result: (target_vid, hop_count, node_path, edge_path)
type BfsResult = (Vid, usize, Vec<Vid>, Vec<Eid>);

/// Expansion record: (original_row_idx, target_vid, hop_count, node_path, edge_path)
type ExpansionRecord = (usize, Vid, usize, Vec<Vid>, Vec<Eid>);

/// Collect the entities appearing on a batch's paths, for
/// [`EntityPropertyCache::prefetch`]. Duplicates are fine — the prefetch
/// de-duplicates before issuing its storage reads.
fn path_entities(expansions: &[ExpansionRecord]) -> (Vec<Vid>, Vec<Eid>) {
    let mut vids = Vec::new();
    let mut eids = Vec::new();
    for (_, _, _, node_path, edge_path) in expansions {
        vids.extend_from_slice(node_path);
        eids.extend_from_slice(edge_path);
    }
    (vids, eids)
}

/// Prepend nodes and edges from an existing path struct column into builders.
///
/// Used when a VLP extends a path that was partially built by a prior `BindFixedPath`.
/// Reads the nodes and relationships from the existing path at `row_idx` and appends
/// them to the provided builders. The caller should then skip the first VLP node
/// (which is the junction point already present in the existing path).
/// Deliberately does not use `append_traversed_edge`: the endpoints here are
/// already stored in the source path's Arrow columns, so re-deriving them
/// through the adjacency probe would be strictly more work for the same answer.
fn prepend_existing_path(
    existing_path: &arrow_array::StructArray,
    row_idx: usize,
    nodes_builder: &mut arrow_array::builder::ListBuilder<arrow_array::builder::StructBuilder>,
    rels_builder: &mut arrow_array::builder::ListBuilder<arrow_array::builder::StructBuilder>,
    query_ctx: &uni_store::runtime::context::QueryContext,
    prop_cache: Option<&EntityPropertyCache>,
) {
    // Read existing nodes
    let nodes_list = existing_path
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    let node_values = nodes_list.value(row_idx);
    let node_struct = node_values
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    let vid_col = node_struct
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    for i in 0..vid_col.len() {
        append_node_to_struct_with(
            nodes_builder.values(),
            Vid::from(vid_col.value(i)),
            query_ctx,
            prop_cache,
        );
    }

    // Read existing edges
    let rels_list = existing_path
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    let edge_values = rels_list.value(row_idx);
    let edge_struct = edge_values
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    let eid_col = edge_struct
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let type_col = edge_struct
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let src_col = edge_struct
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let dst_col = edge_struct
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    for i in 0..eid_col.len() {
        append_edge_to_struct_with(
            rels_builder.values(),
            Eid::from(eid_col.value(i)),
            type_col.value(i),
            src_col.value(i),
            dst_col.value(i),
            query_ctx,
            prop_cache,
        );
    }
}

/// Resolve edge property Arrow type, falling back to `LargeBinary` (CypherValue) for
/// schemaless properties. Unlike vertex properties, schemaless edge properties must
/// preserve original JSON value types (int, float, etc.) since edge types commonly
/// One expanded traversal row: `(input row, target vid, eid, edge type id, orientation)`.
///
/// The last element answers "is the input row's vertex this edge's stored
/// `_src_vid`?" and is `None` when the traversal did not track orientation —
/// which is every traversal that was not asked for [`COL_FWD`]. It is read only
/// when that column is being built, and its absence there is an error rather
/// than a default, so an untracked orientation can never be mistaken for a
/// backwards one.
type Expansion = (usize, Vid, u64, u32, Option<bool>);

/// lack explicit property definitions.
fn resolve_edge_property_type(
    prop: &str,
    schema_props: Option<
        &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
    >,
) -> DataType {
    if prop == "overflow_json" {
        DataType::LargeBinary
    } else if prop == COL_FWD {
        // Per-row traversal orientation, not a stored property. See [`COL_FWD`].
        DataType::Boolean
    } else if prop == "_created_at" || prop == "_updated_at" {
        // System-managed timestamps surfaced via `created_at(r)` /
        // `updated_at(r)`. Stored on every edge by the L0 buffer and
        // the on-disk Arrow tables as Timestamp(Nanosecond, UTC).
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()))
    } else {
        schema_props
            .and_then(|props| props.get(prop))
            .map(|meta| meta.r#type.to_arrow())
            .unwrap_or(DataType::LargeBinary)
    }
}

use crate::query::df_graph::common::merged_edge_schema_props;

/// Expansion tuple for variable-length traversal: (input_row_idx, target_vid, hop_count, node_path, edge_path)
type VarLengthExpansion = (usize, Vid, usize, Vec<Vid>, Vec<Eid>);

/// Build the `path` StructArray (`nodes` + `relationships` lists) for a
/// variable-length traversal batch.
///
/// `existing_path` is the incoming path column when a prior `BindFixedPath`
/// already bound the variable; its prefix is prepended and the VLP's first node
/// (the junction point) is skipped.
///
/// `fixed_type_name` supplies the relationship type name for every edge; pass
/// `None` to resolve each edge's type individually through the L0 visibility
/// chain.
fn build_vlp_path_column(
    expansions: &[VarLengthExpansion],
    existing_path: Option<&arrow_array::StructArray>,
    graph_ctx: &GraphExecutionContext,
    edge_type_ids: &[u32],
    fixed_type_name: Option<&str>,
    prop_cache: Option<&EntityPropertyCache>,
) -> DFResult<ArrayRef> {
    let mut nodes_builder = new_node_list_builder();
    let mut rels_builder = new_edge_list_builder();
    let query_ctx = graph_ctx.query_context();
    let edge_ctx = EdgeAppendCtx {
        graph_ctx,
        query_ctx: &query_ctx,
        edge_type_ids,
        prop_cache,
        fixed_type_name,
    };
    let mut path_validity = Vec::with_capacity(expansions.len());

    for (row_out_idx, (_, _, _, node_path, edge_path)) in expansions.iter().enumerate() {
        if node_path.is_empty() && edge_path.is_empty() {
            nodes_builder.append(false);
            rels_builder.append(false);
            path_validity.push(false);
            continue;
        }

        // Prepend existing path prefix if extending
        let skip_first_vlp_node = if let Some(existing) = existing_path {
            if !existing.is_null(row_out_idx) {
                prepend_existing_path(
                    existing,
                    row_out_idx,
                    &mut nodes_builder,
                    &mut rels_builder,
                    &query_ctx,
                    prop_cache,
                );
                true
            } else {
                false
            }
        } else {
            false
        };

        // Append VLP nodes (skip first if extending — it's the junction point)
        let start_idx = if skip_first_vlp_node { 1 } else { 0 };
        for vid in &node_path[start_idx..] {
            append_node_to_struct_with(nodes_builder.values(), *vid, &query_ctx, prop_cache);
        }
        nodes_builder.append(true);

        for (i, eid) in edge_path.iter().enumerate() {
            append_traversed_edge(
                rels_builder.values(),
                &edge_ctx,
                *eid,
                node_path[i],
                node_path[i + 1],
            );
        }
        rels_builder.append(true);
        path_validity.push(true);
    }

    // Finish builders and get ListArrays
    let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
    let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

    // Build the path struct fields
    let nodes_field = Arc::new(Field::new("nodes", nodes_array.data_type().clone(), true));
    let rels_field = Arc::new(Field::new(
        "relationships",
        rels_array.data_type().clone(),
        true,
    ));

    // Create the path struct array
    let path_struct = arrow_array::StructArray::try_new(
        vec![nodes_field, rels_field].into(),
        vec![nodes_array, rels_array],
        Some(arrow::buffer::NullBuffer::from(path_validity)),
    )
    .map_err(arrow_err)?;

    Ok(Arc::new(path_struct))
}

/// Single-hop graph traversal execution plan.
///
/// Expands each input row by traversing edges to find neighbors.
/// For each (source, edge, target) triple, produces one output row
/// containing the input columns plus target vertex and edge columns.
///
/// # Example
///
/// ```ignore
/// // Input: batch with _vid column
/// // Traverse KNOWS edges outgoing
/// let traverse = GraphTraverseExec::new(
///     input_plan,
///     "_vid",
///     vec![knows_type_id],
///     Direction::Outgoing,
///     "m",           // target variable
///     Some("r"),     // edge variable
///     None,          // no target label filter
///     graph_ctx,
/// );
///
/// // Output: input columns + m._vid + r._eid
/// ```
pub struct GraphTraverseExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Column name containing source VIDs.
    source_column: String,

    /// Edge type IDs to traverse.
    edge_type_ids: Vec<u32>,

    /// Traversal direction.
    direction: Direction,

    /// Variable name for target vertex columns.
    target_variable: String,

    /// Variable name for edge columns (if edge is bound).
    edge_variable: Option<String>,

    /// Edge properties to materialize (for pushdown hydration).
    edge_properties: Vec<String>,

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Target label name for property type resolution.
    target_label_name: Option<String>,

    /// Optional target label filter.
    target_label_id: Option<u16>,

    /// Labels an unlabelled target can carry, taken from the edge type's
    /// declared endpoints when they agree on every requested property's type.
    ///
    /// `Some` is what lets the unlabelled target be read columnar per label
    /// group rather than through the per-vertex map path.
    target_label_candidates: Option<Vec<String>>,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Whether this is an OPTIONAL MATCH (preserve unmatched source rows with NULLs).
    optional: bool,

    /// Variables introduced by the OPTIONAL MATCH pattern.
    /// Used to determine which columns should be null-extended on failure.
    optional_pattern_vars: HashSet<String>,

    /// Column name of an already-bound target VID (for cycle patterns like n-->k<--n).
    /// When set, only traversals that reach this VID are included.
    bound_target_column: Option<String>,

    /// Columns containing edge IDs from previous hops (for relationship uniqueness).
    /// Edges matching any of these IDs are excluded from traversal results.
    used_edge_columns: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphTraverseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphTraverseExec")
            .field("source_column", &self.source_column)
            .field("edge_type_ids", &self.edge_type_ids)
            .field("direction", &self.direction)
            .field("target_variable", &self.target_variable)
            .field("edge_variable", &self.edge_variable)
            .finish()
    }
}

impl GraphTraverseExec {
    /// Create a new single-hop traversal plan.
    ///
    /// # Arguments
    ///
    /// * `input` - Input plan providing source vertices
    /// * `source_column` - Column name containing source VIDs
    /// * `edge_type_ids` - Edge types to traverse
    /// * `direction` - Traversal direction
    /// * `target_variable` - Variable name for target vertices
    /// * `edge_variable` - Optional variable name for edges
    /// * `edge_properties` - Edge properties to materialize
    /// * `target_label_id` - Optional target label filter
    /// * `graph_ctx` - Graph execution context
    /// * `bound_target_column` - Column with already-bound target VID (for cycle patterns)
    /// * `used_edge_columns` - Columns with edge IDs to exclude (relationship uniqueness)
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
        target_variable: impl Into<String>,
        edge_variable: Option<String>,
        edge_properties: Vec<String>,
        target_properties: Vec<String>,
        target_label_name: Option<String>,
        target_label_id: Option<u16>,
        graph_ctx: Arc<GraphExecutionContext>,
        optional: bool,
        optional_pattern_vars: HashSet<String>,
        bound_target_column: Option<String>,
        used_edge_columns: Vec<String>,
    ) -> Self {
        let source_column = source_column.into();
        let target_variable = target_variable.into();

        // Resolve target property Arrow types from the schema
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        // With no target label the endpoint is still declared by the edge type,
        // so the target's properties can be typed from the candidate labels and
        // read columnar instead of through the per-vertex map path.
        let uniform_target = if target_label_name.is_none() {
            crate::query::df_graph::common::uniform_target_schema_props(
                &uni_schema,
                &edge_type_ids,
                direction,
                &target_properties,
            )
        } else {
            None
        };
        let target_label_candidates = uniform_target.as_ref().map(|(labels, _)| labels.clone());
        let label_props = match (target_label_name.as_deref(), uniform_target.as_ref()) {
            (Some(ln), _) => uni_schema.properties.get(ln),
            (None, Some((_, merged))) => Some(merged),
            (None, None) => None,
        };
        let merged_edge_props = merged_edge_schema_props(&uni_schema, &edge_type_ids);
        let edge_props = if merged_edge_props.is_empty() {
            None
        } else {
            Some(&merged_edge_props)
        };

        // Build output schema: input schema + target VID + target props + optional edge ID + edge properties
        let schema = Self::build_schema(
            input.schema(),
            &target_variable,
            edge_variable.as_deref(),
            &edge_properties,
            &target_properties,
            label_props,
            edge_props,
            optional,
        );

        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            source_column,
            edge_type_ids,
            direction,
            target_variable,
            edge_variable,
            edge_properties,
            target_properties,
            target_label_name,
            target_label_id,
            target_label_candidates,
            graph_ctx,
            optional,
            optional_pattern_vars,
            bound_target_column,
            used_edge_columns,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema.
    #[expect(
        clippy::too_many_arguments,
        reason = "Schema construction needs all field metadata"
    )]
    fn build_schema(
        input_schema: SchemaRef,
        target_variable: &str,
        edge_variable: Option<&str>,
        edge_properties: &[String],
        target_properties: &[String],
        label_props: Option<
            &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
        >,
        edge_props: Option<
            &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
        >,
        optional: bool,
    ) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add target VID column (nullable when optional — unmatched rows get NULL)
        let target_vid_name = format!("{}._vid", target_variable);
        fields.push(Field::new(&target_vid_name, DataType::UInt64, optional));

        // Add target ._labels column (List(Utf8)) for labels() and structural projection support
        fields.push(Field::new(
            format!("{}._labels", target_variable),
            labels_data_type(),
            true,
        ));

        // Add target vertex property columns
        for prop_name in target_properties {
            let col_name = format!("{}.{}", target_variable, prop_name);
            let arrow_type = resolve_property_type(prop_name, label_props);
            let uni_type = label_props
                .and_then(|p| p.get(prop_name))
                .map(|m| &m.r#type);
            fields.push(property_field(&col_name, arrow_type, uni_type));
        }

        // Add edge ID column if edge variable is bound
        if let Some(edge_var) = edge_variable {
            let edge_id_name = format!("{}._eid", edge_var);
            fields.push(Field::new(&edge_id_name, DataType::UInt64, optional));

            // Add edge _type column for type(r) support
            fields.push(Field::new(
                format!("{}._type", edge_var),
                DataType::Utf8,
                true,
            ));

            // Add edge property columns with types resolved from schema
            for prop_name in edge_properties {
                let prop_col_name = format!("{}.{}", edge_var, prop_name);
                let arrow_type = resolve_edge_property_type(prop_name, edge_props);
                let uni_type = edge_props.and_then(|p| p.get(prop_name)).map(|m| &m.r#type);
                fields.push(property_field(&prop_col_name, arrow_type, uni_type));
            }
        } else {
            // Add internal edge ID column for relationship uniqueness tracking
            // even when edge variable is not explicitly bound.
            let internal_eid_name = format!("__eid_to_{}", target_variable);
            fields.push(Field::new(&internal_eid_name, DataType::UInt64, optional));
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphTraverseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphTraverseExec: {} --[{:?}]--> {}",
            self.source_column, self.edge_type_ids, self.target_variable
        )?;
        if let Some(ref edge_var) = self.edge_variable {
            write!(f, " as {}", edge_var)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for GraphTraverseExec {
    fn name(&self) -> &str {
        "GraphTraverseExec"
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
                "GraphTraverseExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.source_column.clone(),
            self.edge_type_ids.clone(),
            self.direction,
            self.target_variable.clone(),
            self.edge_variable.clone(),
            self.edge_properties.clone(),
            self.target_properties.clone(),
            self.target_label_name.clone(),
            self.target_label_id,
            self.graph_ctx.clone(),
            self.optional,
            self.optional_pattern_vars.clone(),
            self.bound_target_column.clone(),
            self.used_edge_columns.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let slice_size = context.session_config().batch_size().max(1);
        // Captured before `context` moves into the input's `execute`.
        let reservation = MemoryConsumer::new(format!("GraphTraverseExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let warm_fut = self
            .graph_ctx
            .warming_future(self.edge_type_ids.clone(), self.direction);

        Ok(Box::pin(GraphTraverseStream {
            held_base: 0,
            prefetch_charged: false,
            target_props: None,
            target_props_map: None,
            target_label_candidates: self.target_label_candidates.clone().map(Arc::new),
            all_target_vids: Arc::new(Vec::new()),
            reservation,
            slice_size,
            input: input_stream,
            source_column: self.source_column.clone(),
            edge_type_ids: self.edge_type_ids.clone(),
            direction: self.direction,
            target_variable: self.target_variable.clone(),
            edge_variable: self.edge_variable.clone(),
            edge_properties: self.edge_properties.clone(),
            target_properties: self.target_properties.clone(),
            target_label_name: self.target_label_name.clone(),
            graph_ctx: self.graph_ctx.clone(),
            optional: self.optional,
            optional_pattern_vars: self.optional_pattern_vars.clone(),
            bound_target_column: self.bound_target_column.clone(),
            used_edge_columns: self.used_edge_columns.clone(),
            schema: self.schema.clone(),
            state: TraverseStreamState::Warming(warm_fut),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// State machine for traverse stream execution.
enum TraverseStreamState {
    /// Warming adjacency CSRs before first batch.
    Warming(Pin<Box<dyn std::future::Future<Output = DFResult<()>> + Send>>),
    /// Polling the input stream for batches.
    Reading,
    /// Materializing target vertex properties asynchronously.
    Materializing(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Materialising one chunk of a large expansion set at a time.
    ///
    /// A traversal fans out: one input batch of source rows expands to every
    /// target its rows reach, and `build_traverse_output_batch` hydrated *all*
    /// of them into one `RecordBatch`. LDBC IC9 produced a single batch of
    /// 2 869 951 rows, about 2.3 GB, which is exactly what `ExternalSorter` then
    /// failed to reserve.
    ///
    /// Slicing that batch afterwards does not help: `RecordBatch::slice` is
    /// zero-copy, so every slice pins the parent's buffers and the sorter can
    /// never free one. Hydrating a chunk at a time gives each output batch its
    /// own storage, which is what lets a downstream operator spill a batch and
    /// release it. See issue #202.
    Chunking {
        input: RecordBatch,
        expansions: Vec<Expansion>,
        offset: usize,
    },
    /// A chunk's hydration in flight; carries what is needed to resume.
    MaterializingChunk {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>,
        input: RecordBatch,
        expansions: Vec<Expansion>,
        next_offset: usize,
    },
    /// Handing a materialised batch out in `batch_size` slices.
    ///
    /// One input batch fans out to every target its rows reach, so a materialised
    /// batch can be far larger than the batch that produced it — LDBC IC9 expands
    /// a person's two-hop friends to every `Message` they created. Emitted whole,
    /// that hands a downstream sort one indivisible input and nothing to spill
    /// between: `ExternalSorter` asked for 4.4 GB in a single reservation.
    /// Slicing is what lets the spill path engage. See issue #202.
    ///
    /// `RecordBatch::slice` is zero-copy, so this does not reduce what the
    /// traversal materialises — only what it hands downstream at once.
    Slicing { batch: RecordBatch, offset: usize },
    /// Stream is done.
    Done,
}

/// Stream that performs single-hop traversal with async property materialization.
/// Vertex properties keyed by vid, the shape an unlabelled target read returns.
type TargetPropsMap = HashMap<Vid, HashMap<String, uni_common::Value>>;

struct GraphTraverseStream {
    /// Rows per emitted slice, from the session's `batch_size`.
    slice_size: usize,

    /// Input stream.
    input: SendableRecordBatchStream,

    /// Column name containing source VIDs.
    source_column: String,

    /// Edge type IDs to traverse.
    edge_type_ids: Vec<u32>,

    /// Traversal direction.
    direction: Direction,

    /// Variable name for target vertex (retained for diagnostics).
    #[expect(dead_code, reason = "Retained for debug logging and diagnostics")]
    target_variable: String,

    /// Variable name for edge (if bound).
    edge_variable: Option<String>,

    /// Edge properties to materialize.
    edge_properties: Vec<String>,

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Target label name for property resolution and filtering.
    target_label_name: Option<String>,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Whether this is an OPTIONAL MATCH.
    optional: bool,

    /// Variables introduced by the OPTIONAL MATCH pattern.
    optional_pattern_vars: HashSet<String>,

    /// Column name of an already-bound target VID (for cycle patterns like n-->k<--n).
    bound_target_column: Option<String>,

    /// Columns containing edge IDs from previous hops (for relationship uniqueness).
    used_edge_columns: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: TraverseStreamState,

    /// Metrics.
    metrics: BaselineMetrics,

    /// Target properties for the current expansion set, read once in key order.
    ///
    /// A `_vid IN (...)` lookup costs the span it straddles, so reading each
    /// output chunk's targets separately made every chunk pay the whole table:
    /// targets arrive in visit order, scattered. Reading the expansion set's
    /// targets once, sorted, and gathering each chunk from the result keeps the
    /// emitted batches chunked while the storage read stays key-local.
    ///
    /// `None` until the first chunk asks; reset per expansion set.
    target_props: Option<Arc<tokio::sync::OnceCell<PrefetchedProps>>>,

    /// What the expansion set and its input reserved, before the shared target
    /// read is added to it.
    held_base: usize,

    /// Whether the shared target read has been charged for this expansion set.
    prefetch_charged: bool,

    /// Candidate labels for an unlabelled target, when the edge type's declared
    /// endpoints agree on every requested property's type.
    target_label_candidates: Option<Arc<Vec<String>>>,

    /// The same, for an unlabelled target, whose read is map-shaped.
    ///
    /// An unlabelled target is not exotic: the planner collapses a multi-label
    /// edge endpoint to "no label", and that read fans out across candidate
    /// label tables, so paying it per chunk costs more than the labelled path
    /// ever did -- 246M index comparisons against 3.2M for the same 416k rows.
    target_props_map: Option<Arc<tokio::sync::OnceCell<TargetPropsMap>>>,

    /// Every target vid of the current expansion set, in visit order.
    all_target_vids: Arc<Vec<Vid>>,

    /// The query pool's accounting for what this traversal materializes.
    ///
    /// A traversal's output is bounded by input rows times the edge type's
    /// average fan-out, so it is not bounded by its input — the case behind the
    /// multi-GB LDBC IC9 peaks. Held on the stream because a sliced batch stays
    /// resident until the last slice is handed out, and released when the
    /// stream drops, which covers every path to `Done` without tracking each
    /// transition.
    reservation: MemoryReservation,
}

impl GraphTraverseStream {
    /// Expand neighbors synchronously and return expansions.
    /// Returns (row_idx, target_vid, eid_u64, edge_type_id).
    fn expand_neighbors(&self, batch: &RecordBatch) -> DFResult<Vec<Expansion>> {
        let source_col = batch.column_by_name(&self.source_column).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Source column '{}' not found",
                self.source_column
            ))
        })?;

        let source_vid_cow = column_as_vid_array(source_col.as_ref())?;
        let source_vids: &UInt64Array = &source_vid_cow;

        // If bound_target_column is set, get the expected target VIDs for each row.
        // This is used for cycle patterns like n-->k<--n where the target must match.
        let bound_target_cow = self
            .bound_target_column
            .as_ref()
            .and_then(|col| batch.column_by_name(col))
            .map(|c| column_as_vid_array(c.as_ref()))
            .transpose()?;
        let bound_target_vids: Option<&UInt64Array> = bound_target_cow.as_deref();

        // Collect edge ID arrays from previous hops for relationship uniqueness filtering.
        let used_edge_arrays = super::common::used_edge_id_arrays(batch, &self.used_edge_columns)?;

        let mut expanded_rows: Vec<Expansion> = Vec::new();
        let is_undirected = matches!(self.direction, Direction::Both);
        // Orientation is only tracked when someone asked for it. An undirected
        // hop otherwise costs exactly what it costs today; the second adjacency
        // lookup below is paid only by a query that actually calls
        // `startNode`/`endNode` over an undirected relationship.
        let track_orientation = is_undirected && self.edge_properties.iter().any(|p| p == COL_FWD);

        for (row_idx, source_vid) in source_vids.iter().enumerate() {
            let Some(src) = source_vid else {
                continue;
            };

            // Get expected target VID if this is a bound target pattern.
            // Distinguish between:
            // - no bound target column (no filtering),
            // - bound target present but NULL for this row (must produce no expansion),
            // - bound target present with VID.
            let expected_target = bound_target_vids.map(|arr| {
                if arr.is_null(row_idx) {
                    None
                } else {
                    Some(arr.value(row_idx))
                }
            });

            // Collect used edge IDs for this row from all previous hops
            let used_eids: HashSet<u64> = used_edge_arrays.for_row(row_idx).collect();

            let vid = Vid::from(src);
            // For Direction::Both, deduplicate edges by eid within each source.
            // This prevents the same edge being counted twice (once outgoing, once incoming).
            let mut seen_edges: HashSet<u64> = HashSet::new();

            for &edge_type in &self.edge_type_ids {
                let neighbors = self.graph_ctx.get_neighbors(vid, edge_type, self.direction);

                // Which of these edges leave `vid` along the arrow. Asking the
                // same path a second time — rather than threading a flag down
                // through the adjacency layers — is what keeps the answer right
                // for *every* layer: version-filtered reads and the transaction
                // L0 overlay both resolve orientation here exactly as they
                // resolve the undirected read above. A flag plumbed through only
                // the main CSR would be correct on one path and quietly wrong on
                // the other two.
                let outgoing_eids: Option<HashSet<u64>> = track_orientation.then(|| {
                    self.graph_ctx
                        .get_neighbors(vid, edge_type, Direction::Outgoing)
                        .into_iter()
                        .map(|(_, eid)| eid.as_u64())
                        .collect()
                });

                for (target_vid, eid) in neighbors {
                    let eid_u64 = eid.as_u64();

                    // Skip edges already used in previous hops (relationship uniqueness)
                    if used_eids.contains(&eid_u64) {
                        continue;
                    }

                    // Deduplicate edges for undirected patterns
                    if is_undirected && !seen_edges.insert(eid_u64) {
                        continue;
                    }

                    // Filter by bound target VID if set (for cycle patterns).
                    // NULL bound targets do not match anything.
                    if let Some(expected_opt) = expected_target {
                        let Some(expected) = expected_opt else {
                            continue;
                        };
                        if target_vid.as_u64() != expected {
                            continue;
                        }
                    }

                    // Filter by target label. VIDs no longer embed label
                    // information, so we resolve labels from the L0 chain and
                    // then the persisted VidLabelsIndex (the latter covers
                    // Lance-only vertices, e.g. on forks).
                    if let Some(ref label_name) = self.target_label_name {
                        let query_ctx = self.graph_ctx.query_context();
                        if let Some(vertex_labels) =
                            self.graph_ctx.resolve_vertex_labels(target_vid, &query_ctx)
                        {
                            // Labels resolved — require an actual match.
                            if !vertex_labels.contains(label_name) {
                                continue;
                            }
                        }
                        // else: unknown to L0 and the index → trust storage-level filtering
                    }

                    let is_fwd = outgoing_eids.as_ref().map(|fwd| fwd.contains(&eid_u64));
                    expanded_rows.push((row_idx, target_vid, eid_u64, edge_type, is_fwd));
                }
            }
        }

        Ok(expanded_rows)
    }
}

/// Resolve one traversal target's labels for a `_labels` output column.
///
/// Consults the L0 chain and then the persisted `VidLabelsIndex` via
/// `GraphExecutionContext::resolve_vertex_labels`, so labels that live only in
/// Lance storage (after a flush or on a fork) are not dropped. A vertex absent
/// from both falls back to `target_label_name` — the schema label that scoped
/// the traversal, which storage already filtered to — or an empty set.
/// The Arrow type an unlabelled target's property column must carry.
///
/// Mirrors what `GraphTraverseExec::build_schema` resolved from the candidate
/// labels, so the untyped read path fills the same column shape the typed one
/// would. Falls back to `LargeBinary` exactly when the schema did.
fn fallback_prop_type(
    prop_name: &str,
    label_candidates: Option<&[String]>,
    graph_ctx: &GraphExecutionContext,
) -> arrow::datatypes::DataType {
    let uni_schema = graph_ctx.storage().schema_manager().schema();
    label_candidates
        .and_then(|labels| {
            crate::query::df_graph::common::merged_label_props(
                &uni_schema,
                labels,
                std::slice::from_ref(&prop_name.to_string()),
            )
        })
        .map_or(arrow::datatypes::DataType::LargeBinary, |merged| {
            uni_store::runtime::columnar_scan::resolve_property_type(prop_name, Some(&merged))
        })
}

/// Group target vids by which candidate label each one carries.
///
/// Returns `None` if any vid resolves to none of the candidates, which sends the
/// caller back to the untyped map path rather than reading a vertex from a table
/// that may not hold its column -- that failure is a silently absent value, not
/// an error. Groups come out sorted by vid, which is what makes each group's
/// read a narrow `_vid` range.
fn group_targets_by_label(
    all_target_vids: &[Vid],
    candidates: &[String],
    graph_ctx: &GraphExecutionContext,
) -> Option<Vec<(String, Vec<Vid>)>> {
    let query_ctx = graph_ctx.query_context();
    let mut distinct: Vec<Vid> = all_target_vids.to_vec();
    distinct.sort_unstable();
    distinct.dedup();

    let mut groups: HashMap<&str, Vec<Vid>> = HashMap::new();
    for vid in distinct {
        let labels = graph_ctx.resolve_vertex_labels(vid, &query_ctx)?;
        // First candidate the vertex carries, in the candidates' own order, so a
        // multi-label vertex always lands in the same group.
        let chosen = candidates.iter().find(|c| labels.iter().any(|l| l == *c))?;
        groups.entry(chosen.as_str()).or_default().push(vid);
    }

    let mut out: Vec<(String, Vec<Vid>)> = groups
        .into_iter()
        .map(|(label, vids)| (label.to_string(), vids))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Some(out)
}

fn resolve_output_labels(
    vid: Vid,
    target_label_name: &Option<String>,
    graph_ctx: &GraphExecutionContext,
    query_ctx: &uni_store::runtime::context::QueryContext,
) -> Vec<String> {
    match graph_ctx.resolve_vertex_labels(vid, query_ctx) {
        Some(labels) => labels,
        // Unknown to both L0 and the index — trust the schema label that
        // scoped the traversal (storage already filtered to it).
        None => match target_label_name {
            Some(label_name) => vec![label_name.clone()],
            None => Vec::new(),
        },
    }
}

/// Build the target-vertex labels column for `hasLabel` filters and outputs.
///
/// Resolves each target's labels via `resolve_output_labels`, so `hasLabel(b,
/// "B")` and `labels(b)` evaluate correctly for vertices that live only in
/// Lance storage (e.g. on a fork). Every target vid must be present; use
/// `build_target_labels_column_opt` where unmatched (null) rows are possible.
fn build_target_labels_column(
    target_vids: &[Vid],
    target_label_name: &Option<String>,
    graph_ctx: &GraphExecutionContext,
) -> ArrayRef {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    let mut labels_builder = ListBuilder::new(StringBuilder::new());
    let query_ctx = graph_ctx.query_context();
    for vid in target_vids {
        let row_labels = resolve_output_labels(*vid, target_label_name, graph_ctx, &query_ctx);
        let values = labels_builder.values();
        for lbl in &row_labels {
            values.append_value(lbl);
        }
        labels_builder.append(true);
    }
    Arc::new(labels_builder.finish())
}

/// Build the target-vertex labels column, allowing unmatched (null) rows.
///
/// A `None` vid yields a null list row (an unmatched variable-length path); a
/// `Some` vid resolves via `resolve_output_labels`. This is the
/// variable-length-path counterpart of `build_target_labels_column`, sharing
/// the same L0-then-persisted-`VidLabelsIndex` resolution so labels are not
/// dropped for flushed or forked vertices.
fn build_target_labels_column_opt(
    target_vids: &[Option<Vid>],
    target_label_name: &Option<String>,
    graph_ctx: &GraphExecutionContext,
) -> ArrayRef {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    let mut labels_builder = ListBuilder::new(StringBuilder::new());
    let query_ctx = graph_ctx.query_context();
    for vid in target_vids {
        let Some(vid) = vid else {
            labels_builder.append(false);
            continue;
        };
        let row_labels = resolve_output_labels(*vid, target_label_name, graph_ctx, &query_ctx);
        let values = labels_builder.values();
        for lbl in &row_labels {
            values.append_value(lbl);
        }
        labels_builder.append(true);
    }
    Arc::new(labels_builder.finish())
}

/// Build target vertex property columns from storage and L0.
#[expect(
    clippy::too_many_arguments,
    reason = "Hydration needs the targets, what to read, and the two prefetch handles"
)]
async fn build_target_property_columns(
    target_vids: &[Vid],
    target_properties: &[String],
    target_label_name: &Option<String>,
    graph_ctx: &Arc<GraphExecutionContext>,
    // Filled once per expansion set, shared by every chunk cut from it.
    prefetch: Option<&tokio::sync::OnceCell<PrefetchedProps>>,
    // The unlabelled read's equivalent, keyed by vid rather than by row.
    prefetch_map: Option<&tokio::sync::OnceCell<TargetPropsMap>>,
    // Labels the unlabelled target can carry, when they are known well enough
    // to read it columnar per group.
    label_candidates: Option<&[String]>,
    // The whole expansion set's targets, which is what makes the read key-local
    // where `target_vids` alone is one scattered slice of it.
    all_target_vids: &[Vid],
) -> DFResult<Vec<ArrayRef>> {
    let mut columns = Vec::new();

    if let Some(label_name) = target_label_name {
        // `_all_props` is a whole-entity request with no narrower columnar
        // form, so it stays on the map path; everything else is read as
        // columns and never becomes a per-row map (#209).
        let wants_all = target_properties.iter().any(|p| p == "_all_props");
        if !wants_all {
            if let Some(cell) = prefetch {
                let props = cell
                    .get_or_try_init(|| {
                        crate::query::df_graph::scan::prefetch_vids_columnar(
                            graph_ctx,
                            label_name,
                            "t",
                            target_properties,
                            all_target_vids,
                        )
                    })
                    .await?;
                return props.gather(target_vids);
            }
            return crate::query::df_graph::scan::hydrate_vids_columnar(
                graph_ctx,
                label_name,
                // The scan names its columns `{variable}.{prop}`; the name is
                // internal to that call, since only the property columns are
                // returned and the caller positions them itself.
                "t",
                target_properties,
                target_vids,
            )
            .await;
        }

        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        let props_map = property_manager
            .get_batch_vertex_props_for_label(target_vids, label_name, Some(&query_ctx))
            .await
            .map_err(exec_err)?;

        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let label_props = uni_schema.properties.get(label_name.as_str());

        for prop_name in target_properties {
            let data_type = resolve_property_type(prop_name, label_props);
            let column =
                build_property_column_static(target_vids, &props_map, prop_name, &data_type)
                    .map_err(exec_err)?;
            columns.push(column);
        }
    } else {
        // No label name — use label-agnostic property lookup.
        //
        // `_all_props` is not an internal marker to be filtered out: it is the
        // wildcard `get_batch_vertex_props` understands, reading declared
        // per-label columns, the overflow blob and the L0 overlay. Stripping it
        // left nothing to request, so the storage read was skipped entirely and
        // every property came back null — `properties(b)` on an unlabelled
        // target rendered as `{}` while `labels(b)` stayed correct, because
        // label resolution goes through the VidLabelsIndex rather than the
        // planner's guess.
        //
        // An unlabelled target is not exotic: the planner collapses a
        // *multi-label* edge endpoint declaration to "no label" rather than to a
        // union, so `MATCH (a:A)-[:R]->(b)` takes this path whenever `R`
        // declares more than one destination label.
        //
        // The sibling `build_edge_adjacency_and_target_props` already passes the
        // sentinel straight through — that was the issue #135 fix, which reached
        // `GraphTraverseMainStream` and not this stream.
        let wants_all = target_properties.iter().any(|p| p == "_all_props");

        // Read the unlabelled target columnar when the edge type's declared
        // endpoints pin its type. Grouping by the vertex's own label keeps each
        // group's read on the table that actually holds the column, and the
        // whole expansion set is read at once so each group's vids form a sorted
        // key range rather than a scatter.
        if !wants_all
            && let (Some(candidates), Some(cell)) = (label_candidates, prefetch)
            && let Some(groups) = group_targets_by_label(all_target_vids, candidates, graph_ctx)
        {
            let props = cell
                .get_or_try_init(|| {
                    crate::query::df_graph::scan::prefetch_vids_columnar_grouped(
                        graph_ctx,
                        "t",
                        target_properties,
                        &groups,
                    )
                })
                .await?;
            return props.gather(target_vids);
        }

        let requested: Vec<&str> = if wants_all {
            // The wildcard subsumes any individually-named properties.
            vec!["_all_props"]
        } else {
            target_properties.iter().map(|s| s.as_str()).collect()
        };
        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        // Read the whole expansion set once, in vid order, and serve every
        // chunk from it. Chunk-at-a-time this read pays its span per candidate
        // label table, which is what left IC3's unlabelled `message` at 93 s
        // where the same query labelled runs in 4 s.
        let fetched;
        let props_map: &TargetPropsMap = if requested.is_empty() {
            fetched = TargetPropsMap::new();
            &fetched
        } else {
            // Reading the whole expansion set here was tried and measured
            // WORSE: 155 scans and 246M comparisons became 5 and 6.1M, and the
            // query went 92.9 s -> 140.5/142.6 s. This read returns a
            // `HashMap<String, Value>` per vertex, so hydrating 416k targets at
            // once trades the span win for 416k map allocations -- the per-row
            // map cost the columnar path exists to avoid (#209). Locality has
            // to come with columnar materialisation here, not instead of it.
            let _ = prefetch_map;
            fetched = property_manager
                .get_batch_vertex_props(target_vids, &requested, Some(&query_ctx))
                .await
                .map_err(exec_err)?;
            &fetched
        };

        for prop_name in target_properties {
            if prop_name == "_all_props" {
                columns.push(build_all_props_column(target_vids, props_map, graph_ctx));
            } else {
                // The schema types this column from the candidate labels when
                // they agree, so the fallback has to produce that same type --
                // emitting `LargeBinary` under an `Int64` field fails the batch
                // with "column types must match schema types".
                let data_type = fallback_prop_type(prop_name, label_candidates, graph_ctx);
                let column =
                    build_property_column_static(target_vids, props_map, prop_name, &data_type)
                        .map_err(exec_err)?;
                columns.push(column);
            }
        }
    }

    Ok(columns)
}

/// Build a CypherValue blob column from all vertex properties (L0 + storage).
fn build_all_props_column(
    target_vids: &[Vid],
    props_map: &HashMap<Vid, HashMap<String, uni_common::Value>>,
    graph_ctx: &Arc<GraphExecutionContext>,
) -> ArrayRef {
    use arrow_array::builder::LargeBinaryBuilder;

    let mut builder = LargeBinaryBuilder::new();
    let l0_ctx = graph_ctx.l0_context();
    for vid in target_vids {
        // Merge in `Value` space so typed values (temporals) are preserved.
        let mut merged_props: HashMap<String, uni_common::Value> = HashMap::new();
        if let Some(vid_props) = props_map.get(vid) {
            for (k, v) in vid_props.iter() {
                merged_props.insert(k.clone(), v.clone());
            }
        }
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if let Some(l0_props) = guard.vertex_properties.get(vid) {
                for (k, v) in l0_props.iter() {
                    merged_props.insert(k.clone(), v.clone());
                }
            }
        }
        if merged_props.is_empty() {
            builder.append_null();
        } else {
            builder.append_value(uni_common::cypher_value_codec::encode(
                &uni_common::Value::Map(merged_props),
            ));
        }
    }
    Arc::new(builder.finish())
}

/// Build edge ID, type, and property columns for bound edge variables.
async fn build_edge_columns(
    expansions: &[Expansion],
    edge_properties: &[String],
    edge_type_ids: &[u32],
    graph_ctx: &Arc<GraphExecutionContext>,
) -> DFResult<Vec<ArrayRef>> {
    let mut columns = Vec::new();

    let eids: Vec<Eid> = expansions
        .iter()
        .map(|(_, _, eid, _, _)| Eid::from(*eid))
        .collect();
    let eid_u64s: Vec<u64> = eids.iter().map(|e| e.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(eid_u64s)) as ArrayRef);

    // Edge _type column
    {
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let mut type_builder = arrow_array::builder::StringBuilder::new();
        for (_, _, _, edge_type_id, _) in expansions {
            if let Some(name) = uni_schema.edge_type_name_by_id_unified(*edge_type_id) {
                type_builder.append_value(&name);
            } else {
                type_builder.append_null();
            }
        }
        columns.push(Arc::new(type_builder.finish()) as ArrayRef);
    }

    if !edge_properties.is_empty() {
        let prop_name_refs: Vec<&str> = edge_properties.iter().map(|s| s.as_str()).collect();
        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        // The exact types present in *these* rows, not the plan-level
        // `edge_type_ids` superset: each expansion carries its own
        // `edge_type_id`, so the hint is precise and never over-scans. Without
        // it this call asks L0 for every EID's type and, on a reloaded store
        // where L0 is empty, scans all 15 LDBC edge types per call — 31.7 s and
        // 20% of IC5's HAS_MEMBER clause (#222).
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let hinted_types: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            expansions
                .iter()
                .filter(|(_, _, _, tid, _)| seen.insert(*tid))
                .filter_map(|(_, _, _, tid, _)| uni_schema.edge_type_name_by_id_unified(*tid))
                .collect()
        };

        let props_map = property_manager
            .get_batch_edge_props(
                &eids,
                &prop_name_refs,
                Some(&hinted_types),
                Some(&query_ctx),
            )
            .await
            .map_err(exec_err)?;

        let merged_edge_props = merged_edge_schema_props(&uni_schema, edge_type_ids);
        let edge_type_props = if merged_edge_props.is_empty() {
            None
        } else {
            Some(&merged_edge_props)
        };

        let vid_keys: Vec<Vid> = eids.iter().map(|e| Vid::from(e.as_u64())).collect();

        for prop_name in edge_properties {
            // Not a stored property at all: the traversal's own per-row
            // orientation, computed during expansion. See [`COL_FWD`].
            //
            // A missing value is an internal error rather than a null. Nulls
            // here would make the `CASE` the planner built fall to its ELSE
            // branch, which is a *wrong endpoint*, not a missing one — the exact
            // silent wrong answer this column exists to prevent.
            if prop_name == COL_FWD {
                let mut builder = arrow_array::builder::BooleanBuilder::new();
                for (_, _, _, _, is_fwd) in expansions {
                    let Some(fwd) = is_fwd else {
                        return Err(exec_err(anyhow::anyhow!(
                            "traversal was asked for `{COL_FWD}` but did not track \
                             orientation; this is a planner/executor mismatch, not a \
                             property that happens to be absent"
                        )));
                    };
                    builder.append_value(*fwd);
                }
                columns.push(Arc::new(builder.finish()) as ArrayRef);
                continue;
            }

            // System-managed edge timestamps live outside the property bag.
            // Read them directly from L0 (earliest creation, latest update).
            // Disk-only edges will surface as null — acceptable for the
            // common case where the queried edges are L0-resident in the
            // same session.
            if prop_name == "_created_at" || prop_name == "_updated_at" {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::new().with_timezone("UTC");
                let l0_ctx = graph_ctx.l0_context();
                for eid in &eids {
                    let mut value: Option<i64> = None;
                    for l0 in l0_ctx.iter_l0_buffers() {
                        let guard = l0.read();
                        let opt = if prop_name == "_created_at" {
                            guard.edge_created_at.get(eid).copied()
                        } else {
                            guard.edge_updated_at.get(eid).copied()
                        };
                        if let Some(ts) = opt {
                            value = Some(match value {
                                None => ts,
                                Some(cur) if prop_name == "_created_at" => cur.min(ts),
                                Some(cur) => cur.max(ts),
                            });
                        }
                    }
                    match value {
                        Some(ts) => builder.append_value(ts),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()) as ArrayRef);
                continue;
            }
            let data_type = resolve_edge_property_type(prop_name, edge_type_props);
            let column = build_property_column_static(&vid_keys, &props_map, prop_name, &data_type)
                .map_err(exec_err)?;
            columns.push(column);
        }
    }

    Ok(columns)
}

/// Build the output batch with target vertex properties.
///
/// This is a standalone async function so it can be boxed into a `Send` future
/// without borrowing from `GraphTraverseStream`.
/// The empty-expansions prologue shared by the async and sync batch builders.
///
/// Returns `Some(batch)` when there is nothing to expand — an empty batch for a
/// non-OPTIONAL traverse, or the NULL-padded unmatched rows for an OPTIONAL one
/// — and `None` when the caller should proceed with the real expansion.
///
/// Takes borrows throughout so the sync `poll_next` fast path keeps avoiding
/// the clones its owned-argument sibling pays.
fn traverse_empty_expansion_batch(
    input: &RecordBatch,
    schema: &SchemaRef,
    optional: bool,
    optional_pattern_vars: &HashSet<String>,
) -> DFResult<Option<RecordBatch>> {
    if !optional {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }
    let unmatched_reps = collect_unmatched_optional_group_rows(
        input,
        &HashSet::new(),
        schema,
        optional_pattern_vars,
    )?;
    if unmatched_reps.is_empty() {
        return Ok(Some(RecordBatch::new_empty(schema.clone())));
    }
    build_optional_null_batch_for_rows_with_optional_vars(
        input,
        &unmatched_reps,
        schema,
        optional_pattern_vars,
    )
    .map(Some)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Standalone async fn needs all context passed explicitly"
)]
async fn build_traverse_output_batch(
    input: RecordBatch,
    expansions: Vec<Expansion>,
    schema: SchemaRef,
    edge_variable: Option<String>,
    edge_properties: Vec<String>,
    edge_type_ids: Vec<u32>,
    target_properties: Vec<String>,
    target_label_name: Option<String>,
    graph_ctx: Arc<GraphExecutionContext>,
    optional: bool,
    optional_pattern_vars: HashSet<String>,
    target_props: Option<Arc<tokio::sync::OnceCell<PrefetchedProps>>>,
    target_props_map: Option<Arc<tokio::sync::OnceCell<TargetPropsMap>>>,
    target_label_candidates: Option<Arc<Vec<String>>>,
    all_target_vids: Arc<Vec<Vid>>,
) -> DFResult<RecordBatch> {
    if expansions.is_empty()
        && let Some(batch) =
            traverse_empty_expansion_batch(&input, &schema, optional, &optional_pattern_vars)?
    {
        return Ok(batch);
    }

    // Expand input columns via index array
    let indices: Vec<u64> = expansions
        .iter()
        .map(|(idx, _, _, _, _)| *idx as u64)
        .collect();
    let indices_array = UInt64Array::from(indices);
    let mut columns: Vec<ArrayRef> = input
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices_array, None))
        .collect::<Result<_, _>>()?;

    // Target VID column
    let target_vids: Vec<Vid> = expansions.iter().map(|(_, vid, _, _, _)| *vid).collect();
    let target_vid_u64s: Vec<u64> = target_vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(target_vid_u64s)));

    // Target labels column
    columns.push(build_target_labels_column(
        &target_vids,
        &target_label_name,
        &graph_ctx,
    ));

    // Target vertex property columns
    if !target_properties.is_empty() {
        let prop_cols = build_target_property_columns(
            &target_vids,
            &target_properties,
            &target_label_name,
            &graph_ctx,
            target_props.as_deref(),
            target_props_map.as_deref(),
            target_label_candidates.as_deref().map(Vec::as_slice),
            &all_target_vids,
        )
        .await?;
        columns.extend(prop_cols);
    }

    // Edge columns (bound or internal tracking)
    if edge_variable.is_some() {
        let edge_cols =
            build_edge_columns(&expansions, &edge_properties, &edge_type_ids, &graph_ctx).await?;
        columns.extend(edge_cols);
    } else {
        let eid_u64s: Vec<u64> = expansions.iter().map(|(_, _, eid, _, _)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(eid_u64s)));
    }

    let expanded_batch = RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)?;

    // Append null rows for unmatched optional sources
    if optional {
        let matched_indices: HashSet<usize> =
            expansions.iter().map(|(idx, _, _, _, _)| *idx).collect();
        let unmatched = collect_unmatched_optional_group_rows(
            &input,
            &matched_indices,
            &schema,
            &optional_pattern_vars,
        )?;

        if !unmatched.is_empty() {
            let null_batch = build_optional_null_batch_for_rows_with_optional_vars(
                &input,
                &unmatched,
                &schema,
                &optional_pattern_vars,
            )?;
            let combined = arrow::compute::concat_batches(&schema, [&expanded_batch, &null_batch])
                .map_err(arrow_err)?;
            return Ok(combined);
        }
    }

    Ok(expanded_batch)
}

/// Build a batch for specific unmatched source rows with NULL target/edge columns.
/// Used when OPTIONAL MATCH has some expansions but some source rows had none.
fn build_optional_null_batch_for_rows(
    input: &RecordBatch,
    unmatched_indices: &[usize],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let num_rows = unmatched_indices.len();
    let indices: Vec<u64> = unmatched_indices.iter().map(|&idx| idx as u64).collect();
    let indices_array = UInt64Array::from(indices);

    // Take the unmatched input rows
    let mut columns: Vec<ArrayRef> = Vec::new();
    for col in input.columns() {
        let taken = take(col.as_ref(), &indices_array, None)?;
        columns.push(taken);
    }
    // Fill remaining columns with nulls
    for field in schema.fields().iter().skip(input.num_columns()) {
        columns.push(arrow_array::new_null_array(field.data_type(), num_rows));
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)
}

pub(crate) fn is_optional_column_for_vars(col_name: &str, optional_vars: &HashSet<String>) -> bool {
    optional_vars.contains(col_name)
        || optional_vars.iter().any(|var| {
            // `var.` property columns (e.g. `x.name`), and the exact internal
            // edge-id column `__eid_to_<var>`. Match the edge-id column by full
            // name, not a suffix: `ends_with(var)` wrongly claims `__eid_to_xx`
            // for optional var `x`, so null-filling `x` would clobber `xx`.
            (col_name.starts_with(var.as_str()) && col_name[var.len()..].starts_with('.'))
                || col_name == format!("__eid_to_{var}")
        })
}

fn collect_unmatched_optional_group_rows(
    input: &RecordBatch,
    matched_indices: &HashSet<usize>,
    schema: &SchemaRef,
    optional_vars: &HashSet<String>,
) -> DFResult<Vec<usize>> {
    if input.num_rows() == 0 {
        return Ok(Vec::new());
    }

    if optional_vars.is_empty() {
        return Ok((0..input.num_rows())
            .filter(|idx| !matched_indices.contains(idx))
            .collect());
    }

    let source_vid_indices = source_group_key_columns(input, schema, optional_vars);

    // Group rows by non-optional VID bindings and preserve group order.
    let mut groups: HashMap<Vec<u8>, (usize, bool)> = HashMap::new(); // (first_row_idx, any_matched)
    let mut group_order: Vec<Vec<u8>> = Vec::new();

    for row_idx in 0..input.num_rows() {
        let key = compute_optional_group_key(input, row_idx, &source_vid_indices)?;
        let entry = groups.entry(key.clone());
        if matches!(entry, std::collections::hash_map::Entry::Vacant(_)) {
            group_order.push(key.clone());
        }
        let matched = matched_indices.contains(&row_idx);
        entry
            .and_modify(|(_, any_matched)| *any_matched |= matched)
            .or_insert((row_idx, matched));
    }

    Ok(group_order
        .into_iter()
        .filter_map(|key| {
            groups
                .get(&key)
                .and_then(|(first_idx, any_matched)| (!*any_matched).then_some(*first_idx))
        })
        .collect())
}

/// How a non-optional source column contributes to the grouping key.
///
/// Rows are grouped so a source fanned out by an earlier traversal is
/// null-filled once. A source variable carries its identity as a flat
/// `{var}._vid` column when bound natively, but as a single encoded entity
/// column — with no `_vid` anywhere beside it — when it came through
/// `collect()` + `UNWIND`. Keying only on the first made every such row key to
/// the empty vector, so the whole batch became one group and one matched row
/// suppressed the NULL rows for every other source.
///
/// An empty result stays meaningful and must: a pattern with no bound source at
/// all — `OPTIONAL MATCH ()-[r]->()` — genuinely is one group, and treating each
/// input row as its own there produces a spurious second row. The openCypher TCK
/// says so directly, in `Graph6[6]`.
enum OptionalGroupKeyColumn {
    FlatVid(usize),
    EncodedEntity(usize),
}

fn source_group_key_columns(
    input: &RecordBatch,
    schema: &SchemaRef,
    optional_vars: &HashSet<String>,
) -> Vec<OptionalGroupKeyColumn> {
    // The row's identity as it entered the OPTIONAL MATCH, when the planner
    // tagged it (see `optional_source`). Node ids alone merge entering rows
    // that bind the same node but differ elsewhere, or repeat.
    if let Some(idx) = super::optional_source::newest_source_row_column(schema, |name| {
        is_optional_column_for_vars(name, optional_vars)
    }) && idx < input.num_columns()
    {
        return vec![OptionalGroupKeyColumn::FlatVid(idx)];
    }

    let mut cols = Vec::new();
    let mut covered: HashSet<String> = HashSet::new();

    for (idx, field) in schema.fields().iter().enumerate() {
        if idx >= input.num_columns() || is_optional_column_for_vars(field.name(), optional_vars) {
            continue;
        }
        if let Some(var) = field.name().strip_suffix("._vid") {
            cols.push(OptionalGroupKeyColumn::FlatVid(idx));
            covered.insert(var.to_string());
        }
    }

    // A bound source variable with no `_vid` beside it: key on its encoded
    // bytes. Two encodings of one entity would key apart, which splits a group
    // rather than merging two — the direction that emits a spare NULL row
    // rather than dropping a real one.
    for (idx, field) in schema.fields().iter().enumerate() {
        if idx >= input.num_columns() || is_optional_column_for_vars(field.name(), optional_vars) {
            continue;
        }
        let name = field.name();
        if *field.data_type() == DataType::LargeBinary
            && !name.contains('.')
            && !name.starts_with("__")
            && !covered.contains(name)
        {
            cols.push(OptionalGroupKeyColumn::EncodedEntity(idx));
        }
    }

    cols
}

fn compute_optional_group_key(
    batch: &RecordBatch,
    row_idx: usize,
    source_vid_indices: &[OptionalGroupKeyColumn],
) -> DFResult<Vec<u8>> {
    let mut key = Vec::with_capacity(source_vid_indices.len() * std::mem::size_of::<u64>());
    for col in source_vid_indices {
        match *col {
            OptionalGroupKeyColumn::FlatVid(idx) => {
                let vid_cow = column_as_vid_array(batch.column(idx).as_ref())?;
                let arr: &UInt64Array = &vid_cow;
                if arr.is_null(row_idx) {
                    key.extend_from_slice(&u64::MAX.to_le_bytes());
                } else {
                    key.extend_from_slice(&arr.value(row_idx).to_le_bytes());
                }
            }
            OptionalGroupKeyColumn::EncodedEntity(idx) => {
                match batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                {
                    Some(ba) if ba.is_valid(row_idx) => {
                        // Length-prefixed so concatenation stays unambiguous.
                        let bytes = ba.value(row_idx);
                        key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                        key.extend_from_slice(bytes);
                    }
                    _ => key.extend_from_slice(&u64::MAX.to_le_bytes()),
                }
            }
        }
    }
    Ok(key)
}

fn build_optional_null_batch_for_rows_with_optional_vars(
    input: &RecordBatch,
    unmatched_indices: &[usize],
    schema: &SchemaRef,
    optional_vars: &HashSet<String>,
) -> DFResult<RecordBatch> {
    if optional_vars.is_empty() {
        return build_optional_null_batch_for_rows(input, unmatched_indices, schema);
    }

    let num_rows = unmatched_indices.len();
    let indices: Vec<u64> = unmatched_indices.iter().map(|&idx| idx as u64).collect();
    let indices_array = UInt64Array::from(indices);

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx < input.num_columns() {
            if is_optional_column_for_vars(field.name(), optional_vars) {
                columns.push(arrow_array::new_null_array(field.data_type(), num_rows));
            } else {
                let taken = take(input.column(col_idx).as_ref(), &indices_array, None)?;
                columns.push(taken);
            }
        } else {
            columns.push(arrow_array::new_null_array(field.data_type(), num_rows));
        }
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)
}

impl Stream for GraphTraverseStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            let state = std::mem::replace(&mut self.state, TraverseStreamState::Done);

            match state {
                TraverseStreamState::Warming(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        self.state = TraverseStreamState::Reading;
                        // Continue loop to start reading
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = TraverseStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = TraverseStreamState::Warming(fut);
                        return Poll::Pending;
                    }
                },
                TraverseStreamState::Reading => {
                    // Check timeout
                    if let Err(e) = self.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(exec_err(e))));
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            // Expand neighbors synchronously
                            let expansions = match self.expand_neighbors(&batch) {
                                Ok(exp) => exp,
                                Err(e) => {
                                    self.state = TraverseStreamState::Reading;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };

                            // Build output synchronously only when no properties need async hydration
                            if self.target_properties.is_empty() && self.edge_properties.is_empty()
                            {
                                let result = build_traverse_output_batch_sync(
                                    &batch,
                                    &expansions,
                                    &self.schema,
                                    self.edge_variable.as_ref(),
                                    &self.target_label_name,
                                    &self.graph_ctx,
                                    self.optional,
                                    &self.optional_pattern_vars,
                                );
                                if let Ok(ref r) = result {
                                    self.metrics.record_output(r.num_rows());
                                }
                                // Same slicing as the async arm below. This
                                // synchronous fast path — taken when the
                                // traversal materialises no target or edge
                                // properties — emitted its batch whole, so LDBC
                                // IC9 handed the sort one 2.3 GB input and the
                                // slicing added for #202 never applied to it.
                                match result {
                                    Ok(b) if b.num_rows() > self.slice_size => {
                                        if let Err(e) =
                                            self.reservation.try_resize(b.get_array_memory_size())
                                        {
                                            self.state = TraverseStreamState::Done;
                                            return Poll::Ready(Some(Err(e)));
                                        }
                                        self.state = TraverseStreamState::Slicing {
                                            batch: b,
                                            offset: 0,
                                        };
                                        continue;
                                    }
                                    other => {
                                        self.reservation.free();
                                        self.state = TraverseStreamState::Reading;
                                        return Poll::Ready(Some(other));
                                    }
                                }
                            }

                            // Properties needed — create async future for hydration
                            let schema = self.schema.clone();
                            let edge_variable = self.edge_variable.clone();
                            let edge_properties = self.edge_properties.clone();
                            let edge_type_ids = self.edge_type_ids.clone();
                            let target_properties = self.target_properties.clone();
                            let target_label_name = self.target_label_name.clone();
                            let graph_ctx = self.graph_ctx.clone();

                            let optional = self.optional;
                            let optional_pattern_vars = self.optional_pattern_vars.clone();

                            if expansions.len() > self.slice_size {
                                // Hydrate in chunks so each output batch owns its
                                // data — see `TraverseStreamState::Chunking`.
                                //
                                // Both are retained for the whole chunking loop,
                                // and neither was accounted (#242): the
                                // reservations on the other two paths cover the
                                // emitted batch, which is a different thing.
                                // `Expansion` is a fixed-size tuple, so the set
                                // sizes exactly.
                                let held = expansions.len() * std::mem::size_of::<Expansion>()
                                    + batch.get_array_memory_size();
                                if let Err(e) = self.reservation.try_resize(held) {
                                    self.state = TraverseStreamState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                                self.held_base = held;
                                self.prefetch_charged = false;
                                self.all_target_vids = Arc::new(
                                    expansions.iter().map(|(_, vid, _, _, _)| *vid).collect(),
                                );
                                self.target_props = Some(Arc::new(tokio::sync::OnceCell::new()));
                                self.target_props_map =
                                    Some(Arc::new(tokio::sync::OnceCell::new()));
                                self.state = TraverseStreamState::Chunking {
                                    input: batch,
                                    expansions,
                                    offset: 0,
                                };
                                continue;
                            }

                            // This arm builds the whole batch in one call, so
                            // the "whole expansion set" is simply its own
                            // targets -- but it still has to take the same
                            // typed read, because the schema was typed for it.
                            let expansions_for_prefetch: Vec<Vid> =
                                expansions.iter().map(|(_, vid, _, _, _)| *vid).collect();
                            let fut = build_traverse_output_batch(
                                batch,
                                expansions,
                                schema,
                                edge_variable,
                                edge_properties,
                                edge_type_ids,
                                target_properties,
                                target_label_name,
                                graph_ctx,
                                optional,
                                optional_pattern_vars,
                                Some(Arc::new(tokio::sync::OnceCell::new())),
                                Some(Arc::new(tokio::sync::OnceCell::new())),
                                self.target_label_candidates.clone(),
                                Arc::new(expansions_for_prefetch),
                            );

                            self.state = TraverseStreamState::Materializing(Box::pin(fut));
                            // Continue loop to poll the future
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.state = TraverseStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            self.state = TraverseStreamState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            self.state = TraverseStreamState::Reading;
                            return Poll::Pending;
                        }
                    }
                }
                TraverseStreamState::Materializing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.metrics.record_output(batch.num_rows());
                        if batch.num_rows() > self.slice_size {
                            // `try_resize` rather than `try_grow`: the
                            // reservation tracks what this stream *retains* at
                            // each transition, and a batch handed straight
                            // downstream is the consumer's from that point.
                            // Growing for every batch that passes through would
                            // charge the operator for memory it no longer holds
                            // and exhaust the pool on a long stream.
                            if let Err(e) =
                                self.reservation.try_resize(batch.get_array_memory_size())
                            {
                                self.state = TraverseStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                            self.state = TraverseStreamState::Slicing { batch, offset: 0 };
                            continue;
                        }
                        self.reservation.free();
                        self.state = TraverseStreamState::Reading;
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = TraverseStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = TraverseStreamState::Materializing(fut);
                        return Poll::Pending;
                    }
                },
                TraverseStreamState::Chunking {
                    input,
                    expansions,
                    offset,
                } => {
                    if offset >= expansions.len() {
                        // The chunks carry matches only. Which input rows
                        // matched nothing is a property of the whole expansion
                        // set, so their NULL rows are decided here, once:
                        // decided per chunk, every row whose matches fell in
                        // another chunk got a NULL row too, once per chunk.
                        let unmatched = if self.optional {
                            let matched: HashSet<usize> =
                                expansions.iter().map(|(idx, _, _, _, _)| *idx).collect();
                            collect_unmatched_optional_group_rows(
                                &input,
                                &matched,
                                &self.schema,
                                &self.optional_pattern_vars,
                            )
                            .and_then(|rows| {
                                (!rows.is_empty())
                                    .then(|| {
                                        build_optional_null_batch_for_rows_with_optional_vars(
                                            &input,
                                            &rows,
                                            &self.schema,
                                            &self.optional_pattern_vars,
                                        )
                                    })
                                    .transpose()
                            })
                        } else {
                            Ok(None)
                        };
                        // The expansion set and its input die here, so what they
                        // backed is released here.
                        self.reservation.free();
                        self.state = TraverseStreamState::Reading;
                        match unmatched {
                            Ok(Some(nulls)) => {
                                self.metrics.record_output(nulls.num_rows());
                                return Poll::Ready(Some(Ok(nulls)));
                            }
                            Ok(None) => continue,
                            Err(e) => {
                                self.state = TraverseStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    let end = (offset + self.slice_size).min(expansions.len());
                    let chunk: Vec<Expansion> = expansions[offset..end].to_vec();
                    let fut = build_traverse_output_batch(
                        input.clone(),
                        chunk,
                        self.schema.clone(),
                        self.edge_variable.clone(),
                        self.edge_properties.clone(),
                        self.edge_type_ids.clone(),
                        self.target_properties.clone(),
                        self.target_label_name.clone(),
                        self.graph_ctx.clone(),
                        // NULL rows for unmatched input rows are added once the
                        // last chunk is out, above.
                        false,
                        self.optional_pattern_vars.clone(),
                        self.target_props.clone(),
                        self.target_props_map.clone(),
                        self.target_label_candidates.clone(),
                        Arc::clone(&self.all_target_vids),
                    );
                    self.state = TraverseStreamState::MaterializingChunk {
                        fut: Box::pin(fut),
                        input,
                        expansions,
                        next_offset: end,
                    };
                    continue;
                }
                TraverseStreamState::MaterializingChunk {
                    mut fut,
                    input,
                    expansions,
                    next_offset,
                } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.metrics.record_output(batch.num_rows());
                        // The first chunk fills the shared target read, which
                        // then stays resident for the whole chunking loop -- so
                        // it is charged here, once, on top of the expansion set
                        // and input this arm already reserved.
                        //
                        // Like the scan's own reservation, this is taken after
                        // the read exists, so it bounds how long an over-budget
                        // result survives rather than preventing it. Bounding
                        // construction would mean reading the expansion set in
                        // windows, which is what trades the key locality away.
                        if !self.prefetch_charged
                            && let Some(props) = self.target_props.as_ref().and_then(|c| c.get())
                        {
                            let held = self.held_base.saturating_add(props.memory_bytes());
                            if let Err(e) = self.reservation.try_resize(held) {
                                self.state = TraverseStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                            self.prefetch_charged = true;
                        }
                        self.state = TraverseStreamState::Chunking {
                            input,
                            expansions,
                            offset: next_offset,
                        };
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = TraverseStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = TraverseStreamState::MaterializingChunk {
                            fut,
                            input,
                            expansions,
                            next_offset,
                        };
                        return Poll::Pending;
                    }
                },
                TraverseStreamState::Slicing { batch, offset } => {
                    let remaining = batch.num_rows() - offset;
                    if remaining == 0 {
                        // The parent batch dies here, so the reservation it
                        // backed is released here. Every slice handed out was a
                        // zero-copy view onto it, which is why it had to be held
                        // for the whole slicing phase rather than freed at the
                        // first emit.
                        self.reservation.free();
                        // Back to `Reading`, not `Done`: this stream emits one
                        // materialised batch per input batch, and the input may
                        // have more.
                        self.state = TraverseStreamState::Reading;
                        continue;
                    }
                    let take = self.slice_size.min(remaining);
                    let slice = batch.slice(offset, take);
                    self.state = TraverseStreamState::Slicing {
                        batch,
                        offset: offset + take,
                    };
                    return Poll::Ready(Some(Ok(slice)));
                }
                TraverseStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

/// Build output batch synchronously when no properties need async hydration.
///
/// Only called when both `target_properties` and `edge_properties` are empty,
/// so no property columns need to be materialized.
#[expect(clippy::too_many_arguments)]
fn build_traverse_output_batch_sync(
    input: &RecordBatch,
    expansions: &[Expansion],
    schema: &SchemaRef,
    edge_variable: Option<&String>,
    target_label_name: &Option<String>,
    graph_ctx: &GraphExecutionContext,
    optional: bool,
    optional_pattern_vars: &HashSet<String>,
) -> DFResult<RecordBatch> {
    if expansions.is_empty()
        && let Some(batch) =
            traverse_empty_expansion_batch(input, schema, optional, optional_pattern_vars)?
    {
        return Ok(batch);
    }

    let indices: Vec<u64> = expansions
        .iter()
        .map(|(idx, _, _, _, _)| *idx as u64)
        .collect();
    let indices_array = UInt64Array::from(indices);

    let mut columns: Vec<ArrayRef> = Vec::new();
    for col in input.columns() {
        let expanded = take(col.as_ref(), &indices_array, None)?;
        columns.push(expanded);
    }

    // Add target VID column
    let target_vids: Vec<u64> = expansions
        .iter()
        .map(|(_, vid, _, _, _)| vid.as_u64())
        .collect();
    columns.push(Arc::new(UInt64Array::from(target_vids)));

    // Add target ._labels column. Resolve via the L0 chain and then the
    // persisted VidLabelsIndex (shared with the async path), so labels that
    // live only in Lance storage after a flush or on a fork are not dropped.
    {
        let target_vids: Vec<Vid> = expansions.iter().map(|(_, vid, _, _, _)| *vid).collect();
        columns.push(build_target_labels_column(
            &target_vids,
            target_label_name,
            graph_ctx,
        ));
    }

    // Add edge columns if edge is bound (no properties in sync path)
    if edge_variable.is_some() {
        let edge_ids: Vec<u64> = expansions.iter().map(|(_, _, eid, _, _)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(edge_ids)));

        // Add edge _type column
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let mut type_builder = arrow_array::builder::StringBuilder::new();
        for (_, _, _, edge_type_id, _) in expansions {
            if let Some(name) = uni_schema.edge_type_name_by_id_unified(*edge_type_id) {
                type_builder.append_value(&name);
            } else {
                type_builder.append_null();
            }
        }
        columns.push(Arc::new(type_builder.finish()));
    } else {
        // Internal EID column for relationship uniqueness tracking (matches schema)
        let edge_ids: Vec<u64> = expansions.iter().map(|(_, _, eid, _, _)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(edge_ids)));
    }

    let expanded_batch = RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)?;

    if optional {
        let matched_indices: HashSet<usize> =
            expansions.iter().map(|(idx, _, _, _, _)| *idx).collect();
        let unmatched = collect_unmatched_optional_group_rows(
            input,
            &matched_indices,
            schema,
            optional_pattern_vars,
        )?;

        if !unmatched.is_empty() {
            let null_batch = build_optional_null_batch_for_rows_with_optional_vars(
                input,
                &unmatched,
                schema,
                optional_pattern_vars,
            )?;
            let combined = arrow::compute::concat_batches(schema, [&expanded_batch, &null_batch])
                .map_err(|e| {
                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
            })?;
            return Ok(combined);
        }
    }

    Ok(expanded_batch)
}

impl RecordBatchStream for GraphTraverseStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Adjacency map type: maps source VID to list of
/// (target_vid, eid, edge_type_name, properties, is_fwd).
///
/// Type name and properties are `Arc`-shared: an undirected (`Both`) traversal
/// stores every edge under both endpoints, which used to deep-clone the whole
/// property map per edge (review perf #5).
///
/// `is_fwd` is true when the map *key* is the edge's stored `_src_vid`. Storing
/// every edge under both endpoints is exactly what loses that fact, so it is
/// recorded at the point where both endpoints are still in hand. See
/// [`COL_FWD`].
type EdgeAdjacencyMap = HashMap<Vid, Vec<(Vid, Eid, Arc<str>, Arc<uni_common::Properties>, bool)>>;

/// Approximate heap bytes held by an [`EdgeAdjacencyMap`].
///
/// The pool's other reservations in this file are exact, because Arrow reports
/// its own buffer sizes. This one cannot be: the map is a Rust `HashMap` of
/// `Vec`s, and there is no equivalent accessor. An approximation the pool can
/// see is worth more than an exact number it cannot, since this is frequently
/// the largest thing a traversal holds — with no source pushdown it is bounded
/// by the edge type's table size and not by the input at all.
///
/// `Arc<str>` and `Arc<Properties>` are counted as pointers. They are shared
/// across the entries that reference them, so charging their full contents once
/// per reference would overstate the map by a large factor; the properties are
/// the property cache's memory, not the adjacency's.
///
/// What this deliberately does not cover: `build_edge_adjacency_map` holds an
/// intermediate `edges` vector and a `unique_edges` dedup copy alive at the
/// same time as the map it is building, so the true peak inside that function
/// is a small multiple of what this returns. Reserving there would need the
/// reservation threaded through four call sites, two of which are inside
/// futures that have none. The residency is what this bounds; the transient is
/// recorded here rather than silently ignored.
fn estimate_adjacency_bytes(adjacency: &EdgeAdjacencyMap) -> usize {
    const ENTRY: usize =
        std::mem::size_of::<(Vid, Eid, Arc<str>, Arc<uni_common::Properties>, bool)>();
    // A `HashMap` slot costs its key, its value handle and the control byte,
    // and hashbrown keeps load factor below 1, so the per-key overhead is a
    // meaningful share of a map with short adjacency lists.
    const SLOT: usize = std::mem::size_of::<Vid>()
        + std::mem::size_of::<Vec<(Vid, Eid, Arc<str>, Arc<uni_common::Properties>, bool)>>()
        + 8;
    adjacency
        .values()
        .map(|targets| SLOT + targets.capacity() * ENTRY)
        .sum()
}

/// Graph traversal execution plan for schemaless edge types (TraverseMainByType).
///
/// Unlike GraphTraverseExec which uses CSR adjacency for known types, this operator
/// queries the main edges table for schemaless types and builds an in-memory adjacency map.
///
/// # Example
///
/// ```ignore
/// // Traverse schemaless "CUSTOM" edges
/// let traverse = GraphTraverseMainExec::new(
///     input_plan,
///     "_vid",
///     "CUSTOM",
///     Direction::Outgoing,
///     "m",           // target variable
///     Some("r"),     // edge variable
///     vec![],        // edge properties
///     vec![],        // target properties
///     graph_ctx,
///     false,         // not optional
/// );
/// ```
pub struct GraphTraverseMainExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Column name containing source VIDs.
    source_column: String,

    /// Edge type names (not IDs, since schemaless types may not have IDs).
    /// Supports OR relationship types like `[:KNOWS|HATES]`.
    type_names: Vec<String>,

    /// Traversal direction.
    direction: Direction,

    /// Variable name for target vertex columns.
    target_variable: String,

    /// Variable name for edge columns (if edge is bound).
    edge_variable: Option<String>,

    /// Edge properties to materialize.
    edge_properties: Vec<String>,

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Whether this is an OPTIONAL MATCH (preserve unmatched source rows with NULLs).
    optional: bool,

    /// Variables introduced by the OPTIONAL MATCH pattern.
    optional_pattern_vars: HashSet<String>,

    /// Column name of an already-bound target VID (for patterns where target is in scope).
    /// When set, only traversals reaching this exact VID are included.
    bound_target_column: Option<String>,

    /// Columns containing edge IDs from previous hops (for relationship uniqueness).
    /// Edges matching any of these IDs are excluded from traversal results.
    used_edge_columns: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphTraverseMainExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphTraverseMainExec")
            .field("type_names", &self.type_names)
            .field("direction", &self.direction)
            .field("target_variable", &self.target_variable)
            .field("edge_variable", &self.edge_variable)
            .finish()
    }
}

impl GraphTraverseMainExec {
    /// Create a new schemaless traversal executor.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        type_names: Vec<String>,
        direction: Direction,
        target_variable: impl Into<String>,
        edge_variable: Option<String>,
        edge_properties: Vec<String>,
        target_properties: Vec<String>,
        graph_ctx: Arc<GraphExecutionContext>,
        optional: bool,
        optional_pattern_vars: HashSet<String>,
        bound_target_column: Option<String>,
        used_edge_columns: Vec<String>,
    ) -> Self {
        let source_column = source_column.into();
        let target_variable = target_variable.into();

        // Build output schema
        let schema = Self::build_schema(
            &input.schema(),
            &target_variable,
            &edge_variable,
            &edge_properties,
            &target_properties,
            optional,
        );

        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            source_column,
            type_names,
            direction,
            target_variable,
            edge_variable,
            edge_properties,
            target_properties,
            graph_ctx,
            optional,
            optional_pattern_vars,
            bound_target_column,
            used_edge_columns,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema for traversal.
    fn build_schema(
        input_schema: &SchemaRef,
        target_variable: &str,
        edge_variable: &Option<String>,
        edge_properties: &[String],
        target_properties: &[String],
        optional: bool,
    ) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add target ._vid column (only if not already in input, nullable for OPTIONAL MATCH)
        let target_vid_name = format!("{}._vid", target_variable);
        if input_schema.column_with_name(&target_vid_name).is_none() {
            fields.push(Field::new(target_vid_name, DataType::UInt64, true));
        }

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", target_variable);
        if input_schema.column_with_name(&target_labels_name).is_none() {
            fields.push(Field::new(target_labels_name, labels_data_type(), true));
        }

        // Add edge columns if edge variable is bound
        if let Some(edge_var) = edge_variable {
            fields.push(Field::new(
                format!("{}._eid", edge_var),
                DataType::UInt64,
                optional,
            ));

            // Add edge ._type column for type(r) support
            fields.push(Field::new(
                format!("{}._type", edge_var),
                DataType::Utf8,
                true,
            ));

            // Edge properties: LargeBinary (cv_encoded) to preserve value types.
            // Schemaless edges store properties as CypherValue blobs so that
            // Int, Float, etc. round-trip correctly through Arrow.
            for prop in edge_properties {
                let col_name = format!("{}.{}", edge_var, prop);
                // Per-row traversal orientation, not a stored property, so it is
                // a plain Boolean rather than a CypherValue blob. See [`COL_FWD`].
                if prop == COL_FWD {
                    fields.push(Field::new(&col_name, DataType::Boolean, true));
                    continue;
                }
                let mut metadata = std::collections::HashMap::new();
                metadata.insert("cv_encoded".to_string(), "true".to_string());
                fields.push(
                    Field::new(&col_name, DataType::LargeBinary, true).with_metadata(metadata),
                );
            }
        } else {
            // Add internal edge ID for anonymous relationships so BindPath can
            // reconstruct named paths (p = (a)-[:T]->(b)).
            fields.push(Field::new(
                format!("__eid_to_{}", target_variable),
                DataType::UInt64,
                optional,
            ));
        }

        // Target properties: all as LargeBinary (deferred to PropertyManager)
        for prop in target_properties {
            fields.push(Field::new(
                format!("{}.{}", target_variable, prop),
                DataType::LargeBinary,
                true,
            ));
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphTraverseMainExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphTraverseMainExec: types={:?}, direction={:?}",
            self.type_names, self.direction
        )
    }
}

impl ExecutionPlan for GraphTraverseMainExec {
    fn name(&self) -> &str {
        "GraphTraverseMainExec"
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
                "GraphTraverseMainExec expects exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            input: children[0].clone(),
            source_column: self.source_column.clone(),
            type_names: self.type_names.clone(),
            direction: self.direction,
            target_variable: self.target_variable.clone(),
            edge_variable: self.edge_variable.clone(),
            edge_properties: self.edge_properties.clone(),
            target_properties: self.target_properties.clone(),
            graph_ctx: self.graph_ctx.clone(),
            optional: self.optional,
            optional_pattern_vars: self.optional_pattern_vars.clone(),
            bound_target_column: self.bound_target_column.clone(),
            used_edge_columns: self.used_edge_columns.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let reservation = MemoryConsumer::new(format!("GraphTraverseMainExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(GraphTraverseMainStream::new(
            reservation,
            input_stream,
            self.source_column.clone(),
            self.type_names.clone(),
            self.direction,
            self.target_variable.clone(),
            self.edge_variable.clone(),
            self.edge_properties.clone(),
            self.target_properties.clone(),
            self.graph_ctx.clone(),
            self.optional,
            self.optional_pattern_vars.clone(),
            self.bound_target_column.clone(),
            self.used_edge_columns.clone(),
            self.schema.clone(),
            metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Maximum number of distinct source vids pushed into the edge scan; above
/// this a whole-type scan is cheaper than a giant `IN` filter (measured: a
/// 10k-vid IN over a 100k-edge table is ~2.6x slower than the full scan,
/// while small source sets are ~4.5x faster — see `schemaless_traversal`
/// bench).
const TRAVERSE_PUSHDOWN_MAX_SOURCES: usize = 1_024;

/// State machine for GraphTraverseMainStream.
///
/// The input is drained FIRST so the distinct source vids can be pushed into
/// the edge scan (review perf #5) — without this, a 1-source traversal
/// materialized every edge of the type. Memory note: buffering the input does
/// not change the operator's memory class; it already materialized the entire
/// edge type (and with pushdown materializes strictly less).
enum GraphTraverseMainState {
    /// Draining the input stream to learn the source-vid set.
    CollectingInput {
        input_stream: SendableRecordBatchStream,
        buffered: Vec<RecordBatch>,
    },
    /// Loading adjacency map (and hydrated target props) from storage.
    LoadingEdges {
        #[allow(clippy::type_complexity)]
        future: Pin<
            Box<
                dyn std::future::Future<
                        Output = DFResult<(EdgeAdjacencyMap, HashMap<Vid, uni_common::Properties>)>,
                    > + Send,
            >,
        >,
        buffered: Vec<RecordBatch>,
    },
    /// Replaying the buffered input batches against the loaded adjacency.
    Processing {
        adjacency: EdgeAdjacencyMap,
        target_props: Arc<HashMap<Vid, uni_common::Properties>>,
        buffered: std::vec::IntoIter<RecordBatch>,
    },
    /// Stream is done.
    Done,
}

/// Stream that executes schemaless edge traversal.
struct GraphTraverseMainStream {
    /// Bytes of buffered input held while expanding, carried from `Loading` so
    /// each expanded batch's reservation stays honest about everything the
    /// operator holds at once (#242).
    buffered_bytes: usize,

    /// Source column name.
    source_column: String,

    /// Target variable name.
    target_variable: String,

    /// Edge variable name.
    edge_variable: Option<String>,

    /// Edge properties to materialize.
    edge_properties: Vec<String>,

    /// Target properties to materialize.
    target_properties: Vec<String>,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Whether this is optional (preserve unmatched rows).
    optional: bool,

    /// Variables introduced by OPTIONAL pattern.
    optional_pattern_vars: HashSet<String>,

    /// Column name of an already-bound target VID (for filtering).
    bound_target_column: Option<String>,

    /// Columns containing edge IDs from previous hops (for relationship uniqueness).
    used_edge_columns: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Edge type names to traverse (retained for the deferred edge load).
    type_names: Vec<String>,

    /// Traversal direction (retained for the deferred edge load).
    direction: Direction,

    /// Stream state.
    state: GraphTraverseMainState,

    /// Metrics.
    metrics: BaselineMetrics,

    /// The query pool's accounting for the buffered input and the adjacency.
    ///
    /// This operator is a full pipelining break — it drains its entire input
    /// before emitting anything, so that the distinct source vids can be pushed
    /// into the edge scan — and then holds an adjacency map for the whole
    /// traversal. Without source pushdown that map covers the edge type's whole
    /// table and is bounded by nothing the query mentions.
    reservation: MemoryReservation,
}

impl GraphTraverseMainStream {
    /// Create a new traverse main stream.
    #[expect(clippy::too_many_arguments)]
    fn new(
        reservation: MemoryReservation,
        input_stream: SendableRecordBatchStream,
        source_column: String,
        type_names: Vec<String>,
        direction: Direction,
        target_variable: String,
        edge_variable: Option<String>,
        edge_properties: Vec<String>,
        target_properties: Vec<String>,
        graph_ctx: Arc<GraphExecutionContext>,
        optional: bool,
        optional_pattern_vars: HashSet<String>,
        bound_target_column: Option<String>,
        used_edge_columns: Vec<String>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        // The input is drained first (CollectingInput) so the edge load can
        // push the bounded source-vid set into the scan.
        Self {
            buffered_bytes: 0,
            reservation,
            source_column,
            target_variable,
            edge_variable,
            edge_properties,
            target_properties,
            graph_ctx,
            optional,
            optional_pattern_vars,
            bound_target_column,
            used_edge_columns,
            schema,
            type_names,
            direction,
            state: GraphTraverseMainState::CollectingInput {
                input_stream,
                buffered: Vec::new(),
            },
            metrics,
        }
    }

    /// Distinct source vids across the buffered input, or `None` when the set
    /// exceeds [`TRAVERSE_PUSHDOWN_MAX_SOURCES`] (whole-type scan is cheaper)
    /// or the source column cannot be read (the edge load then behaves exactly
    /// as before pushdown existed).
    fn collect_source_vids(&self, buffered: &[RecordBatch]) -> Option<HashSet<Vid>> {
        let mut vids: HashSet<Vid> = HashSet::new();
        for batch in buffered {
            let col = batch.column_by_name(&self.source_column)?;
            let arr = column_as_vid_array(col.as_ref()).ok()?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    vids.insert(Vid::from(arr.value(i)));
                    if vids.len() > TRAVERSE_PUSHDOWN_MAX_SOURCES {
                        return None;
                    }
                }
            }
        }
        Some(vids)
    }

    /// Expand input batch using adjacency map (synchronous version).
    fn expand_batch(
        &self,
        input: &RecordBatch,
        adjacency: &EdgeAdjacencyMap,
        target_props: &HashMap<Vid, uni_common::Properties>,
    ) -> DFResult<RecordBatch> {
        // Extract source VIDs from source column
        let source_col = input.column_by_name(&self.source_column).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Source column {} not found",
                self.source_column
            ))
        })?;

        let source_vid_cow = column_as_vid_array(source_col.as_ref())?;
        let source_vids: &UInt64Array = &source_vid_cow;

        // Read bound target VIDs if column exists
        let bound_target_cow = self
            .bound_target_column
            .as_ref()
            .and_then(|col| input.column_by_name(col))
            .map(|c| column_as_vid_array(c.as_ref()))
            .transpose()?;
        let expected_targets: Option<&UInt64Array> = bound_target_cow.as_deref();

        // Collect edge ID arrays from previous hops for relationship uniqueness filtering.
        let used_edge_arrays = super::common::used_edge_id_arrays(input, &self.used_edge_columns)?;

        // Build expansions:
        // (input_row_idx, target_vid, eid, edge_type, edge_props, is_fwd).
        // Type/props are Arc-shared with the adjacency map (pointer clones).
        // `is_fwd` rides along from the adjacency entry; see [`COL_FWD`].
        type Expansion = (usize, Vid, Eid, Arc<str>, Arc<uni_common::Properties>, bool);
        let mut expansions: Vec<Expansion> = Vec::new();

        for (row_idx, src_u64) in source_vids.iter().enumerate() {
            if let Some(src_u64) = src_u64 {
                let src_vid = Vid::from(src_u64);

                // Collect used edge IDs for this row from all previous hops
                let used_eids: HashSet<u64> = used_edge_arrays.for_row(row_idx).collect();

                if let Some(neighbors) = adjacency.get(&src_vid) {
                    for (target_vid, eid, edge_type, props, is_fwd) in neighbors {
                        // Skip edges already used in previous hops (relationship uniqueness)
                        if used_eids.contains(&eid.as_u64()) {
                            continue;
                        }

                        // Filter by bound target VID if set (for patterns where target is in scope).
                        // Only include traversals where the target matches the expected VID.
                        if let Some(targets) = expected_targets {
                            if targets.is_null(row_idx) {
                                continue;
                            }
                            let expected_vid = targets.value(row_idx);
                            if target_vid.as_u64() != expected_vid {
                                continue;
                            }
                        }

                        expansions.push((
                            row_idx,
                            *target_vid,
                            *eid,
                            Arc::clone(edge_type),
                            Arc::clone(props),
                            *is_fwd,
                        ));
                    }
                }
            }
        }

        // Handle OPTIONAL: preserve unmatched rows
        if expansions.is_empty() && self.optional {
            // No matches - return input with NULL columns appended
            let all_indices: Vec<usize> = (0..input.num_rows()).collect();
            return build_optional_null_batch_for_rows(input, &all_indices, &self.schema);
        }

        if expansions.is_empty() {
            // No matches, not optional - return empty batch
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Track matched rows for OPTIONAL handling
        let matched_rows: HashSet<usize> = if self.optional {
            expansions.iter().map(|(idx, _, _, _, _, _)| *idx).collect()
        } else {
            HashSet::new()
        };

        // Expand input columns using Arrow take()
        let mut columns: Vec<ArrayRef> = Vec::new();
        let indices: Vec<u64> = expansions
            .iter()
            .map(|(idx, _, _, _, _, _)| *idx as u64)
            .collect();
        let indices_array = UInt64Array::from(indices);

        for col in input.columns() {
            let expanded = take(col.as_ref(), &indices_array, None)?;
            columns.push(expanded);
        }

        // Add target ._vid column (only if not already in input)
        let target_vid_name = format!("{}._vid", self.target_variable);
        let target_vids: Vec<u64> = expansions
            .iter()
            .map(|(_, vid, _, _, _, _)| vid.as_u64())
            .collect();
        if input.schema().column_with_name(&target_vid_name).is_none() {
            columns.push(Arc::new(UInt64Array::from(target_vids)));
        }

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", self.target_variable);
        if input
            .schema()
            .column_with_name(&target_labels_name)
            .is_none()
        {
            use arrow_array::builder::{ListBuilder, StringBuilder};
            // Resolve labels from the L0 chain and then the persisted
            // VidLabelsIndex. The index covers vertices that live only in Lance
            // storage — notably on a fork, whose data is flushed to Lance before
            // branching — so a `hasLabel(b, "B")` filter over this column matches
            // there too. (GitHub #99)
            let query_ctx = self.graph_ctx.query_context();
            let mut labels_builder = ListBuilder::new(StringBuilder::new());
            for (_, target_vid, _, _, _, _) in &expansions {
                let row_labels = self
                    .graph_ctx
                    .resolve_vertex_labels(*target_vid, &query_ctx)
                    .unwrap_or_default();
                let values = labels_builder.values();
                for lbl in &row_labels {
                    values.append_value(lbl);
                }
                labels_builder.append(true);
            }
            columns.push(Arc::new(labels_builder.finish()));
        }

        // Add edge columns if edge variable is bound.
        // For anonymous relationships, emit internal edge IDs for BindPath.
        if self.edge_variable.is_some() {
            // Add edge ._eid column
            let eids: Vec<u64> = expansions
                .iter()
                .map(|(_, _, eid, _, _, _)| eid.as_u64())
                .collect();
            columns.push(Arc::new(UInt64Array::from(eids)));

            // Add edge ._type column
            {
                let mut type_builder = arrow_array::builder::StringBuilder::new();
                for (_, _, _, edge_type, _, _) in &expansions {
                    type_builder.append_value(edge_type);
                }
                columns.push(Arc::new(type_builder.finish()));
            }

            // Add edge property columns as cv_encoded LargeBinary to preserve types
            for prop_name in &self.edge_properties {
                // Not a stored property: the traversal's own per-row orientation,
                // carried here from the adjacency entry. See [`COL_FWD`].
                if prop_name == COL_FWD {
                    let mut builder = arrow_array::builder::BooleanBuilder::new();
                    for (_, _, _, _, _, is_fwd) in &expansions {
                        builder.append_value(*is_fwd);
                    }
                    columns.push(Arc::new(builder.finish()));
                    continue;
                }
                let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                if prop_name == "_all_props" {
                    // Serialize all edge properties to a CypherValue blob directly
                    // from `Value` so typed values (temporals) are preserved.
                    for (_, _, _, _, props, _) in &expansions {
                        if props.is_empty() {
                            builder.append_null();
                        } else {
                            let map: HashMap<String, uni_common::Value> =
                                props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            builder.append_value(uni_common::cypher_value_codec::encode(
                                &uni_common::Value::Map(map),
                            ));
                        }
                    }
                } else {
                    // Named property as cv_encoded CypherValue
                    for (_, _, _, _, props, _) in &expansions {
                        match props.get(prop_name) {
                            Some(uni_common::Value::Null) | None => builder.append_null(),
                            Some(val) => {
                                builder.append_value(uni_common::cypher_value_codec::encode(val));
                            }
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        } else {
            let eids: Vec<u64> = expansions
                .iter()
                .map(|(_, _, eid, _, _, _)| eid.as_u64())
                .collect();
            columns.push(Arc::new(UInt64Array::from(eids)));
        }

        // Add target property columns. Values are hydrated from `target_props`,
        // which merges the L0 buffers AND persisted (Lance) storage under the
        // query's MVCC visibility (see `build_edge_adjacency_and_target_props`).
        // Reading L0 alone here dropped every target property once the vertex
        // was auto-flushed out of L0 into Lance (issue #135).
        {
            for prop_name in &self.target_properties {
                if prop_name == "_all_props" {
                    // Build a full CypherValue blob from all target properties.
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for (_, target_vid, _, _, _, _) in &expansions {
                        // Merge in `Value` space so typed values (temporals) survive.
                        let merged_props: HashMap<String, uni_common::Value> = target_props
                            .get(target_vid)
                            .map(|props| {
                                props
                                    .iter()
                                    .filter(|(_, v)| !v.is_null())
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        if merged_props.is_empty() {
                            builder.append_null();
                        } else {
                            builder.append_value(uni_common::cypher_value_codec::encode(
                                &uni_common::Value::Map(merged_props),
                            ));
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                } else {
                    // Extract an individual property and encode as CypherValue.
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for (_, target_vid, _, _, _, _) in &expansions {
                        match target_props
                            .get(target_vid)
                            .and_then(|props| props.get(prop_name.as_str()))
                        {
                            Some(val) if !val.is_null() => {
                                builder.append_value(uni_common::cypher_value_codec::encode(val));
                            }
                            _ => builder.append_null(),
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
            }
        }

        let matched_batch =
            RecordBatch::try_new(self.schema.clone(), columns).map_err(arrow_err)?;

        // Handle OPTIONAL: append unmatched rows with NULLs
        if self.optional {
            let unmatched = collect_unmatched_optional_group_rows(
                input,
                &matched_rows,
                &self.schema,
                &self.optional_pattern_vars,
            )?;

            if unmatched.is_empty() {
                return Ok(matched_batch);
            }

            let unmatched_batch = build_optional_null_batch_for_rows_with_optional_vars(
                input,
                &unmatched,
                &self.schema,
                &self.optional_pattern_vars,
            )?;

            // Concatenate matched and unmatched batches
            use arrow::compute::concat_batches;
            concat_batches(&self.schema, &[matched_batch, unmatched_batch]).map_err(arrow_err)
        } else {
            Ok(matched_batch)
        }
    }
}

impl GraphExecutionContext {
    /// Records a schemaless traversal's full edge-type scan into the SSI read-set.
    ///
    /// `build_edge_adjacency_map` scans every edge of the traversed type(s) via
    /// `find_edges_by_type_names` up front, so the transaction's true read
    /// footprint is that whole adjacency — not just the neighbourhoods later
    /// expanded. Recording it here gives schemaless single-hop and
    /// variable-length traversal (which bypass the per-vertex `get_neighbors`
    /// path) the same item-level antidependency coverage the schema-typed
    /// `record_neighbor_reads` path provides. No-op for read-only transactions
    /// (`occ_read_set` is `Some` only for read-write transactions).
    fn record_edge_adjacency(&self, adjacency: &EdgeAdjacencyMap) {
        let Some(tx_l0) = &self.l0_context.transaction_l0 else {
            return;
        };
        let guard = tx_l0.read();
        let Some(read_set) = &guard.occ_read_set else {
            return;
        };
        let mut rs = read_set.lock();
        for (src, neighbors) in adjacency {
            rs.vertices.insert(*src);
            for (nbr, eid, _type, _props, _fwd) in neighbors {
                rs.vertices.insert(*nbr);
                rs.edges.insert(*eid);
            }
        }
    }
}

/// Build adjacency map from main edges table for given type names and direction.
///
/// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
/// Returns a HashMap mapping source VID -> Vec<(target_vid, eid, properties)>
/// Direction determines the key: Outgoing uses src_vid, Incoming uses dst_vid, Both adds entries for both.
///
/// `source_vids`, when supplied, is pushed into the persisted scan so only the
/// sources' incident edges are materialized instead of the whole edge type
/// (review perf #5). The L0 overlay below deliberately stays whole-type: L0 is
/// small, and unused adjacency entries are never expanded.
async fn build_edge_adjacency_map(
    graph_ctx: &GraphExecutionContext,
    type_names: &[String],
    direction: Direction,
    source_vids: Option<HashSet<Vid>>,
) -> DFResult<EdgeAdjacencyMap> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();

    // Step 1: Query main edges table for all type names, pushing the bounded
    // source set into the scan when present.
    let type_refs: Vec<&str> = type_names.iter().map(|s| s.as_str()).collect();
    let endpoint_vids: Option<Vec<Vid>> = source_vids.as_ref().map(|s| s.iter().copied().collect());
    let endpoint_filter = endpoint_vids.as_deref().map(|vids| {
        let side = match direction {
            Direction::Outgoing => uni_store::storage::EndpointSide::Src,
            Direction::Incoming => uni_store::storage::EndpointSide::Dst,
            Direction::Both => uni_store::storage::EndpointSide::Either,
        };
        (side, vids)
    });
    let edges_with_type = storage
        .find_edges_by_type_names_counted(&type_refs, endpoint_filter, graph_ctx.counters())
        .await
        .map_err(exec_err)?;

    // Edge filter matching the pushed-down scan, applied to the L0 overlay
    // below so the map (and therefore the SSI read-set recorded from it) has
    // ONE consistent footprint across both storage tiers.
    let edge_in_scope = |src: Vid, dst: Vid| -> bool {
        match &source_vids {
            None => true,
            Some(set) => match direction {
                Direction::Outgoing => set.contains(&src),
                Direction::Incoming => set.contains(&dst),
                Direction::Both => set.contains(&src) || set.contains(&dst),
            },
        }
    };

    // Preserve edge type name in the adjacency map for type(r) support
    let mut edges: Vec<(
        uni_common::Eid,
        uni_common::Vid,
        uni_common::Vid,
        String,
        uni_common::Properties,
    )> = edges_with_type.into_iter().collect();

    // Step 2: Overlay L0 buffers for all type names. Every L0 edge of the type
    // is examined, so each counts as an L0 row scanned (the persisted rows are
    // counted by the storage scan above).
    let mut l0_examined = 0usize;
    for l0 in l0_ctx.iter_l0_buffers() {
        let l0_guard = l0.read();

        for type_name in type_names {
            let l0_eids = l0_guard.eids_for_type(type_name);
            l0_examined += l0_eids.len();

            // For each L0 edge, extract its information
            for &eid in &l0_eids {
                if let Some(edge_ref) = l0_guard.graph.edge(eid) {
                    let src_vid = edge_ref.src_vid;
                    let dst_vid = edge_ref.dst_vid;
                    if !edge_in_scope(src_vid, dst_vid) {
                        continue;
                    }

                    // Get properties for this edge from L0
                    let props = l0_guard
                        .edge_properties
                        .get(&eid)
                        .cloned()
                        .unwrap_or_default();

                    edges.push((eid, src_vid, dst_vid, type_name.clone(), props));
                }
            }
        }
    }

    graph_ctx.count_l0_rows(l0_examined);
    graph_ctx.count_rows_scanned(l0_examined);

    // Step 3: Deduplicate by EID (L0 takes precedence)
    let mut seen_eids = HashSet::new();
    let mut unique_edges = Vec::new();
    for edge in edges.into_iter().rev() {
        if seen_eids.insert(edge.0) {
            unique_edges.push(edge);
        }
    }
    unique_edges.reverse();

    // Step 4: Filter out edges tombstoned in any L0 buffer
    let mut tombstoned_eids = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let l0_guard = l0.read();
        for eid in l0_guard.tombstones.keys() {
            tombstoned_eids.insert(*eid);
        }
    }
    if !tombstoned_eids.is_empty() {
        unique_edges.retain(|edge| !tombstoned_eids.contains(&edge.0));
    }

    // Step 5: Build adjacency map based on direction. Type name and props are
    // Arc-shared so the `Both` arm clones two pointers, not the property map.
    let mut adjacency: EdgeAdjacencyMap = HashMap::new();

    for (eid, src_vid, dst_vid, edge_type, props) in unique_edges {
        let edge_type: Arc<str> = edge_type.into();
        let props = Arc::new(props);
        // The last element of each entry is `is_fwd`: whether the key it is
        // filed under is the edge's stored `_src_vid`. Keyed by `src_vid` it is
        // true, keyed by `dst_vid` it is false — the one place both endpoints
        // are still in hand, and therefore the only place it can be recorded.
        match direction {
            Direction::Outgoing => {
                adjacency
                    .entry(src_vid)
                    .or_default()
                    .push((dst_vid, eid, edge_type, props, true));
            }
            Direction::Incoming => {
                adjacency
                    .entry(dst_vid)
                    .or_default()
                    .push((src_vid, eid, edge_type, props, false));
            }
            Direction::Both => {
                adjacency.entry(src_vid).or_default().push((
                    dst_vid,
                    eid,
                    Arc::clone(&edge_type),
                    Arc::clone(&props),
                    true,
                ));
                // A self-loop (src == dst) would otherwise be listed TWICE under
                // the same source — once per orientation — and an undirected match
                // `(a)-[:R]-(b)` over it would yield a duplicate row. It is a single
                // relationship, so list it only once. Its two endpoints are the
                // same vertex, so the `true` above names it correctly either way.
                if dst_vid != src_vid {
                    adjacency
                        .entry(dst_vid)
                        .or_default()
                        .push((src_vid, eid, edge_type, props, false));
                }
            }
        }
    }

    // SSI: a schemaless traversal physically scans the edge type above —
    // whole-type, or with source pushdown every edge incident to every actual
    // source — so record that read footprint for read-write transactions
    // (no-op otherwise). With pushdown this records exactly what the
    // transaction logically read; the whole-type scan was incidentally
    // (not load-bearing) more conservative under the item-level,
    // phantoms-out-of-scope read-set contract.
    graph_ctx.record_edge_adjacency(&adjacency);

    Ok(adjacency)
}

/// Build the edge adjacency map AND hydrate the traversed target vertices'
/// properties from BOTH the L0 buffers and persisted (Lance) storage.
///
/// The single-hop [`GraphTraverseMainStream`] used to materialise target
/// properties inline from the L0 buffers only (see `expand_batch`). Once the
/// auto-flush migrated a vertex's row out of L0 into Lance, that inline read
/// found nothing and emitted NULL for every target property — while the same
/// row stayed perfectly readable via a node scan (issue #135). Pre-fetching
/// through [`PropertyManager::get_batch_vertex_props`], which merges L0 +
/// storage under the query's MVCC visibility, closes that tier gap.
async fn build_edge_adjacency_and_target_props(
    graph_ctx: &GraphExecutionContext,
    type_names: &[String],
    direction: Direction,
    source_vids: Option<HashSet<Vid>>,
    target_properties: &[String],
) -> DFResult<(EdgeAdjacencyMap, HashMap<Vid, uni_common::Properties>)> {
    let adjacency = build_edge_adjacency_map(graph_ctx, type_names, direction, source_vids).await?;

    if target_properties.is_empty() {
        return Ok((adjacency, HashMap::new()));
    }

    // Collect the distinct target vids reachable through this adjacency. The map
    // is keyed by the traversal source; each neighbour tuple's first element is
    // the materialised target vid.
    let mut target_vids: Vec<Vid> = Vec::new();
    let mut seen: HashSet<Vid> = HashSet::new();
    for neighbors in adjacency.values() {
        for (tvid, _, _, _, _) in neighbors {
            if seen.insert(*tvid) {
                target_vids.push(*tvid);
            }
        }
    }

    // Decide which property names to fetch. `_all_props` is a wildcard: pass the
    // sentinel straight through to `get_batch_vertex_props`, which reads every
    // property across all tiers (declared per-label columns, the overflow/main
    // `props_json` blob, and the L0 overlay). Enumerating schema-declared names
    // here instead would yield nothing for schemaless labels, dropping the
    // target's properties (e.g. an intermediate node in a multi-hop traversal).
    let wants_all = target_properties.iter().any(|p| p == "_all_props");
    let mut names: HashSet<String> = target_properties.iter().cloned().collect();
    if wants_all {
        // Keep only the wildcard; individual names are subsumed by it.
        names.retain(|p| p == "_all_props");
    }

    if target_vids.is_empty() || names.is_empty() {
        return Ok((adjacency, HashMap::new()));
    }

    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let query_ctx = graph_ctx.query_context();
    let props = graph_ctx
        .property_manager()
        .get_batch_vertex_props(&target_vids, &name_refs, Some(&query_ctx))
        .await
        .map_err(exec_err)?;
    Ok((adjacency, props))
}

/// Build the edge-property `EidFilter` for a typed variable-length traversal.
///
/// For a pattern like `[r:KNOWS*1..3 {year: 1988}]` the inline edge-property
/// conditions must filter *flushed* (CSR/Lance) edges too — not only L0
/// in-memory edges as the original code did. We pre-scan every edge of the
/// traversed type(s) — flushed edges via `find_edges_by_type_names`, L0 edges
/// via the overlay, both handled (with tombstone/dedup semantics) by
/// [`build_edge_adjacency_map`] — evaluate the conditions against each edge's
/// materialized properties, and collect the passing Eids into an allow-set.
///
/// Returns [`EidFilter::AllAllowed`] when there are no conditions (nothing to
/// filter) or when no edge type id resolves to a name (cannot pre-scan —
/// degrade to the prior, less-restrictive behaviour rather than drop every
/// edge).
/// Visible to sibling modules under `df_graph`: `shortest_path` reuses this
/// to build the same precomputed `EidFilter` (issue #166 family).
/// Result of the warming phase: one entry per hop slot.
///
/// Both vectors are indexed by the slots the NFA stamps onto its transitions
/// and states, and are built from the same step list the NFA was compiled
/// from, so the indices cannot drift apart.
pub(super) struct WarmedFilters {
    pub edges: Vec<EidFilter>,
    pub vertices: Vec<VidFilter>,
    /// Narrows the accepting endpoints from the target's inline property
    /// map. `AllAllowed` whenever the pattern has no such map, or whenever the
    /// narrowing could not be established conservatively.
    pub accepting: VidFilter,
    /// Set instead of the three above for a simple pattern with a map: the
    /// maps are evaluated per input batch over the region the walk can reach,
    /// not over the whole edge type up front (P7).
    pub reachable: Option<ReachableMapFilter>,
}

/// One quantified-path-pattern hop's filter inputs.
///
/// A QPP can constrain each hop differently, so the executor builds one filter
/// per hop and the NFA hands out the matching slot index. Carries the hop's own
/// edge types and direction because those scope the scan that the predicate is
/// evaluated against.
#[derive(Debug, Clone)]
pub struct QppStepFilterSpec {
    pub edge_type_ids: Vec<u32>,
    pub direction: Direction,
    /// Needed to resolve the target node's property columns; `None` when the
    /// hop's target node has no property map.
    pub target_label: Option<String>,
    /// Equality conditions from the relationship's inline property map.
    pub edge_conditions: Vec<(String, UniValue)>,
    /// Equality conditions from the target node's inline property map.
    pub target_conditions: Vec<(String, UniValue)>,
}

/// Builds a `VidFilter` of the vertices reachable by one QPP hop that satisfy
/// that hop's target-node property map.
///
/// Precomputed during warming rather than checked at the gate because the gate
/// is synchronous: the only synchronous per-vertex accessor is the **L0-only**
/// visibility chain, which reports a flushed vertex as property-less and would
/// admit it (the trap behind #135 and #141). Candidates are just the vertices
/// this hop's edges can reach, so the batch read stays bounded by the hop
/// rather than by the label's cardinality, and it is projected to the map's own
/// property names so heavy columns are never decoded (#134).
///
/// Returns `AllAllowed` when there is nothing to constrain — no conditions or
/// no reachable candidates.
///
/// Known cost: a hop carrying *both* an edge map and a node map scans the same
/// edges twice, once here and once in `build_edge_property_filter`. Left as-is
/// rather than fused, because the scan only happens for hops that actually
/// carry a map and the fused version would duplicate the evaluation logic;
/// revisit if a profile shows it.
pub(super) async fn build_vertex_property_filter(
    graph_ctx: &Arc<GraphExecutionContext>,
    edge_type_ids: &[u32],
    direction: Direction,
    target_label: Option<&str>,
    conditions: &[(String, UniValue)],
) -> DFResult<VidFilter> {
    if conditions.is_empty() {
        return Ok(VidFilter::AllAllowed);
    }

    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let type_names: Vec<String> = edge_type_ids
        .iter()
        .filter_map(|id| uni_schema.edge_type_name_by_id_unified(*id))
        .collect();
    if type_names.is_empty() {
        return Ok(VidFilter::AllAllowed);
    }

    let adjacency = build_edge_adjacency_map(graph_ctx, &type_names, direction, None).await?;
    let mut candidates: Vec<Vid> = Vec::new();
    let mut seen: FxHashSet<u64> = FxHashSet::default();
    for entries in adjacency.values() {
        for (neighbor, _eid, _etype, _props, _fwd) in entries {
            if seen.insert(neighbor.as_u64()) {
                candidates.push(*neighbor);
            }
        }
    }
    if candidates.is_empty() {
        return Ok(VidFilter::AllAllowed);
    }

    let query_ctx = graph_ctx.query_context();
    let wanted: Vec<String> = conditions.iter().map(|(name, _)| name.clone()).collect();
    // A label lets the read target that label's table and project to just the
    // map's columns (#134). Without one, fall back to the label-free batch
    // read, which resolves per-label and then the schemaless main table — so an
    // unlabelled intermediate node still filters rather than being refused.
    let props_by_vid = if let Some(label) = target_label {
        graph_ctx
            .property_manager()
            .get_batch_vertex_props_for_label_projected(
                &candidates,
                label,
                Some(&query_ctx),
                Some(&wanted),
            )
            .await
    } else {
        let wanted_refs: Vec<&str> = wanted.iter().map(String::as_str).collect();
        graph_ctx
            .property_manager()
            .get_batch_vertex_props(&candidates, &wanted_refs, Some(&query_ctx))
            .await
    }
    .map_err(exec_err)?;

    let mut passing: Vec<u64> = Vec::new();
    let mut max_vid: u64 = 0;
    for vid in &candidates {
        let raw = vid.as_u64();
        max_vid = max_vid.max(raw);
        if props_by_vid
            .get(vid)
            .is_some_and(|props| map_matches(props, conditions))
        {
            passing.push(raw);
        }
    }
    Ok(VidFilter::from_vids(passing, max_vid as usize + 1))
}

/// Whether `props` satisfies every condition of an inline property map.
///
/// `{w: 1}` means `w = 1` under Cypher equality, so an integer property
/// matches a float literal of the same value, and a `null` literal or a
/// missing property matches nothing. Plain `Value` equality is structural and
/// had `{w: 1.0}` reject every edge with `w = 1` — an empty answer where
/// `WHERE r.w = 1.0` finds rows.
fn map_matches(props: &uni_common::Properties, conditions: &[(String, UniValue)]) -> bool {
    conditions.iter().all(|(name, expected)| {
        props
            .get(name)
            .is_some_and(|actual| cypher_eq(actual, expected) == Some(true))
    })
}

/// Whether a target vertex may satisfy its inline property map, for a filter
/// that only narrows.
///
/// Admits anything not known to fail: unread properties, a missing property
/// and a comparison that comes out `null` all leave the decision to the
/// `FilterExec` above the traversal.
fn map_admits(props: Option<&uni_common::Properties>, conditions: &[(String, UniValue)]) -> bool {
    props.is_none_or(|props| {
        conditions.iter().all(|(name, expected)| {
            props
                .get(name)
                .is_none_or(|actual| cypher_eq(actual, expected) != Some(false))
        })
    })
}

/// Read the target map's properties for `candidates`, projected to the map's
/// own names.
///
/// A label lets the read target that label's table (#134). Without one the
/// label-free batch read resolves per label and then the schemaless table.
async fn read_target_map_props(
    graph_ctx: &GraphExecutionContext,
    candidates: &[Vid],
    target_label: Option<&str>,
    conditions: &[(String, UniValue)],
) -> DFResult<HashMap<Vid, uni_common::Properties>> {
    let query_ctx = graph_ctx.query_context();
    let wanted: Vec<String> = conditions.iter().map(|(name, _)| name.clone()).collect();
    if let Some(label) = target_label {
        graph_ctx
            .property_manager()
            .get_batch_vertex_props_for_label_projected(
                candidates,
                label,
                Some(&query_ctx),
                Some(&wanted),
            )
            .await
    } else {
        let wanted_refs: Vec<&str> = wanted.iter().map(String::as_str).collect();
        graph_ctx
            .property_manager()
            .get_batch_vertex_props(candidates, &wanted_refs, Some(&query_ctx))
            .await
    }
    .map_err(exec_err)
}

/// Build a filter over the *accepting* vertices of a variable-length pattern
/// from the target node's inline property map.
///
/// Deliberately not [`build_vertex_property_filter`], which is fail-*closed*:
/// it drops a candidate whose properties could not be read. That polarity is
/// right where the filter is the authoritative check, and wrong here. This one
/// only ever narrows the search; the `FilterExec` the planner puts above the
/// traversal still applies the predicate and is what makes the answer correct.
/// A vertex wrongly excluded here is a row that no later stage can put back —
/// a missing result rather than a slow one — so anything unknown is admitted.
///
/// The payoff is real on a cyclic graph: without it every reachable vertex is
/// an accepting endpoint, so the enumeration walks paths to all of them and a
/// `LIMIT` on a pinned endpoint waits for paths it will discard. With it the
/// accepting set is the handful of vertices that can actually match.
pub(super) async fn build_accepting_vid_filter(
    graph_ctx: &Arc<GraphExecutionContext>,
    edge_type_ids: &[u32],
    direction: Direction,
    target_label: Option<&str>,
    conditions: &[(String, UniValue)],
) -> DFResult<VidFilter> {
    if conditions.is_empty() {
        return Ok(VidFilter::AllAllowed);
    }

    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let type_names: Vec<String> = edge_type_ids
        .iter()
        .filter_map(|id| uni_schema.edge_type_name_by_id_unified(*id))
        .collect();
    if type_names.is_empty() {
        return Ok(VidFilter::AllAllowed);
    }

    // Candidates are every vertex reachable by one of these edge types. An
    // accepting vertex at depth >= 1 is by construction one of them.
    let adjacency = build_edge_adjacency_map(graph_ctx, &type_names, direction, None).await?;
    let mut candidates: Vec<Vid> = Vec::new();
    let mut seen: FxHashSet<u64> = FxHashSet::default();
    for entries in adjacency.values() {
        for (neighbor, _eid, _etype, _props, _fwd) in entries {
            if seen.insert(neighbor.as_u64()) {
                candidates.push(*neighbor);
            }
        }
    }
    if candidates.is_empty() {
        return Ok(VidFilter::AllAllowed);
    }

    let props_by_vid =
        read_target_map_props(graph_ctx, &candidates, target_label, conditions).await?;

    let mut passing: Vec<u64> = Vec::new();
    let mut max_vid: u64 = 0;
    for vid in &candidates {
        let raw = vid.as_u64();
        max_vid = max_vid.max(raw);
        if map_admits(props_by_vid.get(vid), conditions) {
            passing.push(raw);
        }
    }

    // Narrowing nothing is the same as no filter, and cheaper to check.
    if passing.len() == candidates.len() {
        return Ok(VidFilter::AllAllowed);
    }
    Ok(VidFilter::from_vids(passing, max_vid as usize + 1))
}

/// Build the [`EidFilter`] for a variable-length pattern's inline edge
/// property map by pre-scanning every edge of the pattern's types.
///
/// The pre-scan goes through [`build_edge_adjacency_map`], so it counts toward
/// the query's `rows_scanned` (storage and L0 rows alike): a profile shows a
/// whole-type scan here instead of the anchor lookup alone.
pub(super) async fn build_edge_property_filter(
    graph_ctx: &Arc<GraphExecutionContext>,
    edge_type_ids: &[u32],
    direction: Direction,
    conditions: &[(String, UniValue)],
) -> DFResult<EidFilter> {
    if conditions.is_empty() {
        return Ok(EidFilter::AllAllowed);
    }

    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let type_names: Vec<String> = edge_type_ids
        .iter()
        .filter_map(|id| uni_schema.edge_type_name_by_id_unified(*id))
        .collect();
    if type_names.is_empty() {
        return Ok(EidFilter::AllAllowed);
    }

    // Enumerate every edge of these types (flushed + L0, tombstone-aware) with
    // properties materialized. `None` source-vids => whole-type scan, matching
    // the schemaless VLP path.
    let adjacency = build_edge_adjacency_map(graph_ctx, &type_names, direction, None).await?;

    let mut passing: Vec<u64> = Vec::new();
    let mut max_eid: u64 = 0;
    let mut seen: FxHashSet<u64> = FxHashSet::default();
    for edges in adjacency.values() {
        for (_neighbor, eid, _etype, props, _fwd) in edges {
            let raw = eid.as_u64();
            // `Direction::Both` lists each edge under both endpoints — dedup so
            // the density heuristic in `from_eids` sees a true count.
            if !seen.insert(raw) {
                continue;
            }
            max_eid = max_eid.max(raw);
            if map_matches(props, conditions) {
                passing.push(raw);
            }
        }
    }

    Ok(EidFilter::from_eids(passing, max_eid as usize + 1))
}

/// Property-read rounds the reachable filter may always spend before it
/// considers falling back to the whole-type pre-scan.
const REACHABLE_MIN_ROUNDS: usize = 16;

/// Edges of the type the whole-type pre-scan reads in the time one round of
/// the region read costs.
///
/// Each round is one batched property read, issued after the previous one
/// because the next frontier depends on its verdicts, so a deep walk pays a
/// fixed per-read cost once per hop. Measured (debug build, flushed chain
/// where every edge passes): about 0.8 ms per round, against about 5 us per
/// edge for the whole-type scan (100 ms at 20k edges, 390 ms at 80k). The
/// round budget is therefore the type's edge count over this, so falling back
/// happens only once the rounds already spent cost about what the whole-type
/// scan would. See the P7 entry of
/// `docs/proposals/open_issues_plan_2026_10_17.md`.
const EDGES_PER_ROUND: usize = 160;

/// Below this many edges of the pattern's types, a region covering most of
/// the type is no reason to fall back: either read costs a few milliseconds.
const REGION_FALLBACK_MIN_EDGES: usize = 4096;

/// One hop of a pattern whose maps [`ReachableMapFilter`] evaluates: the
/// edges it may take, and the maps on the edge and on the vertex it reaches.
struct RegionStep {
    edge_type_ids: Vec<u32>,
    type_names: Vec<String>,
    direction: Direction,
    edge_conditions: Vec<(String, UniValue)>,
    target_label: Option<String>,
    target_conditions: Vec<(String, UniValue)>,
}

/// The inline property maps of a variable-length, shortest-path or
/// quantified pattern, evaluated only over the part of the graph a walk from
/// the sources seen so far can reach.
///
/// The whole-type form ([`build_edge_property_filter`],
/// [`build_vertex_property_filter`], [`build_accepting_vid_filter`]) reads
/// every edge of the pattern's types before the first row, so an anchored
/// walk that visits a dozen edges paid for the whole type (P7). This one is
/// grown per input batch, before the batch is walked: a breadth-first pass
/// from the batch's sources, bounded by the hop limit, that expands only
/// through edges (and, for a quantified pattern's hop, vertices) whose maps
/// pass. Each layer's not-yet-evaluated edges are read in one batched
/// property read per hop slot, the vertices they reach likewise, and the
/// target map is read once per batch for the vertices first reached.
///
/// A simple pattern has one hop slot. A quantified pattern's iteration of
/// `k` hops has `k`, and hop `d` of a walk uses slot `d mod k` — the slots
/// [`PathNfa::from_qpp`] stamps on its transitions — so the pass tracks
/// `(vertex, slot)` pairs.
///
/// # Why the filters it builds are complete for the walk
///
/// The walk tests a slot-`s` edge only when it leaves a vertex it reached at
/// depth `d < max_hops` in slot `s` through passing edges and vertices, and
/// tests a slot's vertex map and the accepting filter only at a vertex
/// reached that way. The pass reaches every such `(vertex, slot)` at a depth
/// no greater than `d` (it keeps the shallowest depth, from every source of
/// every batch so far, and ignores the label constraints the walk adds, so
/// it reaches a superset), so it expanded that pair too and evaluated the
/// same edges and vertices. An edge or vertex it never evaluated is one the
/// walk never asks about, so the allow-sets may leave them out.
pub(super) struct ReachableMapFilter {
    graph_ctx: Arc<GraphExecutionContext>,
    steps: Vec<RegionStep>,
    max_hops: usize,
    /// The label and map of the pattern's final vertex.
    target_label: Option<String>,
    target_conditions: Vec<(String, UniValue)>,
    /// Shallowest depth each `(vertex, slot)` has been reached at.
    depth: rustc_hash::FxHashMap<(Vid, usize), u32>,
    /// Per slot, the edges whose map has been evaluated.
    evaluated_edges: Vec<FxHashSet<u64>>,
    /// Per slot, the vertices whose hop map has been evaluated.
    evaluated_step_vertices: Vec<FxHashSet<u64>>,
    /// Vertices whose target map has been evaluated.
    evaluated_vertices: FxHashSet<u64>,
    /// Allow-set of edges, one per slot, as the NFA expects.
    edges: Vec<EidFilter>,
    /// Allow-set of each slot's reached vertices, as the NFA expects.
    vertices: Vec<VidFilter>,
    /// Admitted endpoints.
    accepting: VidFilter,
    /// Property-read rounds spent so far.
    rounds: usize,
    /// Rounds allowed before falling back to the whole-type filters.
    max_rounds: usize,
    /// Approximate edges of the pattern's types: past half of them read one
    /// at a time, the whole-type scan is the cheaper way to finish.
    type_edges: usize,
    /// Fell back to the whole-type filters, which already cover everything.
    whole_type: bool,
}

impl ReachableMapFilter {
    /// A filter for the maps of a simple pattern, or `None` when there is
    /// nothing to filter or no edge type resolves to a name to read
    /// properties under.
    pub(super) fn new(
        graph_ctx: &Arc<GraphExecutionContext>,
        edge_type_ids: &[u32],
        direction: Direction,
        max_hops: usize,
        edge_conditions: &[(String, UniValue)],
        target_label: Option<&str>,
        target_conditions: &[(String, UniValue)],
    ) -> Option<Self> {
        let step = QppStepFilterSpec {
            edge_type_ids: edge_type_ids.to_vec(),
            direction,
            target_label: None,
            edge_conditions: edge_conditions.to_vec(),
            target_conditions: Vec::new(),
        };
        Self::for_steps(
            graph_ctx,
            &[step],
            max_hops,
            target_label,
            target_conditions,
        )
    }

    /// A filter for the per-hop maps of a quantified pattern, one slot per
    /// hop of its iteration, plus its final vertex's map. `None` when no map
    /// constrains anything, or when a hop with a map has no edge type that
    /// resolves to a name (the whole-type form then decides, as before).
    pub(super) fn for_steps(
        graph_ctx: &Arc<GraphExecutionContext>,
        steps: &[QppStepFilterSpec],
        max_hops: usize,
        target_label: Option<&str>,
        target_conditions: &[(String, UniValue)],
    ) -> Option<Self> {
        let any_map = steps
            .iter()
            .any(|s| !s.edge_conditions.is_empty() || !s.target_conditions.is_empty());
        if !any_map && target_conditions.is_empty() {
            return None;
        }
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let mut region_steps = Vec::with_capacity(steps.len());
        for s in steps {
            let type_names: Vec<String> = s
                .edge_type_ids
                .iter()
                .filter_map(|id| uni_schema.edge_type_name_by_id_unified(*id))
                .collect();
            if type_names.is_empty() {
                return None;
            }
            region_steps.push(RegionStep {
                edge_type_ids: s.edge_type_ids.clone(),
                type_names,
                direction: s.direction,
                edge_conditions: s.edge_conditions.clone(),
                target_label: s.target_label.clone(),
                target_conditions: s.target_conditions.clone(),
            });
        }
        let edges = region_steps
            .iter()
            .map(|s| {
                if s.edge_conditions.is_empty() {
                    EidFilter::AllAllowed
                } else {
                    EidFilter::Sparse(FxHashSet::default())
                }
            })
            .collect();
        let vertices = region_steps
            .iter()
            .map(|s| {
                if s.target_conditions.is_empty() {
                    VidFilter::AllAllowed
                } else {
                    VidFilter::Sparse(FxHashSet::default())
                }
            })
            .collect();
        let accepting = if target_conditions.is_empty() {
            VidFilter::AllAllowed
        } else {
            VidFilter::Sparse(FxHashSet::default())
        };
        let distinct_types: FxHashSet<u32> = region_steps
            .iter()
            .flat_map(|s| s.edge_type_ids.iter().copied())
            .collect();
        let type_edges: usize = distinct_types
            .iter()
            .map(|&t| graph_ctx.adjacency_manager().approx_edge_count(t))
            .sum();
        let k = region_steps.len();
        Some(Self {
            graph_ctx: Arc::clone(graph_ctx),
            steps: region_steps,
            max_hops,
            target_label: target_label.map(str::to_string),
            target_conditions: target_conditions.to_vec(),
            depth: rustc_hash::FxHashMap::default(),
            evaluated_edges: vec![FxHashSet::default(); k],
            evaluated_step_vertices: vec![FxHashSet::default(); k],
            evaluated_vertices: FxHashSet::default(),
            edges,
            vertices,
            accepting,
            rounds: 0,
            max_rounds: REACHABLE_MIN_ROUNDS.max(type_edges / EDGES_PER_ROUND),
            type_edges,
            whole_type: false,
        })
    }

    /// Approximate heap bytes the filter holds, for memory accounting.
    pub(super) fn approx_bytes(&self) -> usize {
        let slot = std::mem::size_of::<u64>() + 1;
        self.depth.capacity() * (std::mem::size_of::<((Vid, usize), u32)>() + 1)
            + self
                .evaluated_edges
                .iter()
                .chain(&self.evaluated_step_vertices)
                .map(FxHashSet::capacity)
                .sum::<usize>()
                * slot
            + self.evaluated_vertices.capacity() * slot
            + self
                .edges
                .iter()
                .map(EidFilter::approx_bytes)
                .sum::<usize>()
            + self
                .vertices
                .iter()
                .map(VidFilter::approx_bytes)
                .sum::<usize>()
            + self.accepting.approx_bytes()
    }

    /// The edge allow-sets, one per NFA slot.
    pub(super) fn edges(&self) -> &[EidFilter] {
        &self.edges
    }

    /// The vertex allow-sets of each slot's reached vertex, one per NFA slot.
    pub(super) fn vertices(&self) -> &[VidFilter] {
        &self.vertices
    }

    /// The admitted endpoints.
    pub(super) fn accepting(&self) -> &VidFilter {
        &self.accepting
    }

    /// The source vids of `column`, for [`Self::cover`].
    pub(super) fn sources_of(column: &UInt64Array) -> Vec<Vid> {
        let mut seen = FxHashSet::default();
        column
            .iter()
            .flatten()
            .filter(|raw| seen.insert(*raw))
            .map(Vid::from)
            .collect()
    }

    /// Whether the next read would cost more than finishing with the
    /// whole-type filters.
    fn over_budget(&self, fresh: usize) -> bool {
        let evaluated: usize = self.evaluated_edges.iter().map(FxHashSet::len).sum();
        self.rounds >= self.max_rounds
            || (self.type_edges >= REGION_FALLBACK_MIN_EDGES
                && evaluated + fresh > self.type_edges / 2)
    }

    /// Grow the filters to cover every walk from `sources`.
    ///
    /// # Errors
    ///
    /// Returns a storage error from a property read.
    pub(super) async fn cover(&mut self, sources: &[Vid]) -> DFResult<()> {
        if self.whole_type {
            return Ok(());
        }
        let k = self.steps.len();
        let mut frontier: Vec<(Vid, usize)> = Vec::new();
        for &v in sources {
            if self.depth.get(&(v, 0)).is_none_or(|&d| d > 0) {
                self.depth.insert((v, 0), 0);
                frontier.push((v, 0));
            }
        }
        let mut reached: Vec<Vid> = Vec::new();
        let mut depth = 0usize;
        while !frontier.is_empty() && depth < self.max_hops {
            self.graph_ctx.check_timeout().map_err(exec_err)?;
            // Every edge out of the frontier, as (slot, neighbor, eid).
            let mut out: Vec<(usize, Vid, Eid)> = Vec::new();
            for slot in 0..k {
                let from: Vec<Vid> = frontier
                    .iter()
                    .filter(|(_, s)| *s == slot)
                    .map(|(v, _)| *v)
                    .collect();
                if from.is_empty() {
                    continue;
                }
                let step = &self.steps[slot];
                for &type_id in &step.edge_type_ids {
                    out.extend(
                        self.graph_ctx
                            .get_neighbors_batch(&from, type_id, step.direction)
                            .into_iter()
                            .map(|(_, neighbor, eid)| (slot, neighbor, eid)),
                    );
                }
            }
            for slot in 0..k {
                if self.steps[slot].edge_conditions.is_empty() {
                    continue;
                }
                let fresh: Vec<Eid> = {
                    let mut seen = FxHashSet::default();
                    out.iter()
                        .filter(|(s, _, _)| *s == slot)
                        .map(|&(_, _, eid)| eid)
                        .filter(|eid| {
                            !self.evaluated_edges[slot].contains(&eid.as_u64())
                                && seen.insert(eid.as_u64())
                        })
                        .collect()
                };
                if !fresh.is_empty() {
                    if self.over_budget(fresh.len()) {
                        return self.fall_back_to_whole_type().await;
                    }
                    self.rounds += 1;
                    self.evaluate_edges(slot, &fresh).await?;
                }
            }
            out.retain(|&(slot, _, eid)| self.edges[slot].contains(eid));
            for slot in 0..k {
                if self.steps[slot].target_conditions.is_empty() {
                    continue;
                }
                let fresh: Vec<Vid> = {
                    let mut seen = FxHashSet::default();
                    out.iter()
                        .filter(|(s, _, _)| *s == slot)
                        .map(|&(_, v, _)| v)
                        .filter(|v| {
                            !self.evaluated_step_vertices[slot].contains(&v.as_u64())
                                && seen.insert(v.as_u64())
                        })
                        .collect()
                };
                if !fresh.is_empty() {
                    if self.over_budget(0) {
                        return self.fall_back_to_whole_type().await;
                    }
                    self.rounds += 1;
                    self.evaluate_step_vertices(slot, &fresh).await?;
                }
            }
            let next_depth = depth as u32 + 1;
            let mut next: Vec<(Vid, usize)> = Vec::new();
            for (slot, neighbor, _) in out {
                if !self.vertices[slot].contains(neighbor) {
                    continue;
                }
                if self.evaluated_vertices.insert(neighbor.as_u64())
                    && !self.target_conditions.is_empty()
                {
                    reached.push(neighbor);
                }
                let state = (neighbor, (slot + 1) % k);
                if self.depth.get(&state).is_none_or(|&d| d > next_depth) {
                    self.depth.insert(state, next_depth);
                    next.push(state);
                }
            }
            frontier = next;
            depth += 1;
        }
        if !reached.is_empty() {
            self.evaluate_targets(&reached).await?;
        }
        Ok(())
    }

    /// Read slot `slot`'s map properties for `eids` in one batched read and
    /// record verdicts.
    async fn evaluate_edges(&mut self, slot: usize, eids: &[Eid]) -> DFResult<()> {
        let step = &self.steps[slot];
        let names: Vec<&str> = step
            .edge_conditions
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        let query_ctx = self.graph_ctx.query_context();
        let props = self
            .graph_ctx
            .property_manager()
            .get_batch_edge_props(eids, &names, Some(&step.type_names), Some(&query_ctx))
            .await
            .map_err(exec_err)?;
        // Point reads, one row per edge asked for.
        self.graph_ctx.count_rows_scanned(eids.len());
        let EidFilter::Sparse(passing) = &mut self.edges[slot] else {
            unreachable!("an edge map's allow-set is built sparse");
        };
        for &eid in eids {
            self.evaluated_edges[slot].insert(eid.as_u64());
            // The edge accessor keys its result by the eid reinterpreted as a vid.
            if props
                .get(&Vid::from(eid.as_u64()))
                .is_some_and(|p| map_matches(p, &step.edge_conditions))
            {
                passing.insert(eid.as_u64());
            }
        }
        Ok(())
    }

    /// Read slot `slot`'s vertex map properties for `vids` and record the
    /// vertices that satisfy it.
    ///
    /// Fail-closed, as [`build_vertex_property_filter`] is: this allow-set is
    /// the authoritative check for the hop's vertex map, so a vertex whose
    /// properties could not be read does not pass.
    async fn evaluate_step_vertices(&mut self, slot: usize, vids: &[Vid]) -> DFResult<()> {
        let step = &self.steps[slot];
        let props = read_target_map_props(
            &self.graph_ctx,
            vids,
            step.target_label.as_deref(),
            &step.target_conditions,
        )
        .await?;
        self.graph_ctx.count_rows_scanned(vids.len());
        let VidFilter::Sparse(passing) = &mut self.vertices[slot] else {
            unreachable!("a hop's vertex map allow-set is built sparse");
        };
        for vid in vids {
            self.evaluated_step_vertices[slot].insert(vid.as_u64());
            if props
                .get(vid)
                .is_some_and(|p| map_matches(p, &step.target_conditions))
            {
                passing.insert(vid.as_u64());
            }
        }
        Ok(())
    }

    /// Read the target map's properties for `vids` and record the admitted.
    async fn evaluate_targets(&mut self, vids: &[Vid]) -> DFResult<()> {
        let props = read_target_map_props(
            &self.graph_ctx,
            vids,
            self.target_label.as_deref(),
            &self.target_conditions,
        )
        .await?;
        self.graph_ctx.count_rows_scanned(vids.len());
        let VidFilter::Sparse(admitted) = &mut self.accepting else {
            unreachable!("a target map's admitted set is built sparse");
        };
        for vid in vids {
            if map_admits(props.get(vid), &self.target_conditions) {
                admitted.insert(vid.as_u64());
            }
        }
        Ok(())
    }

    /// Replace the region-bounded filters with the whole-type ones, which
    /// cover every walk from every source.
    async fn fall_back_to_whole_type(&mut self) -> DFResult<()> {
        let mut edges = Vec::with_capacity(self.steps.len());
        let mut vertices = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            edges.push(
                build_edge_property_filter(
                    &self.graph_ctx,
                    &step.edge_type_ids,
                    step.direction,
                    &step.edge_conditions,
                )
                .await?,
            );
            vertices.push(
                build_vertex_property_filter(
                    &self.graph_ctx,
                    &step.edge_type_ids,
                    step.direction,
                    step.target_label.as_deref(),
                    &step.target_conditions,
                )
                .await?,
            );
        }
        let all_types: Vec<u32> = {
            let mut seen = FxHashSet::default();
            self.steps
                .iter()
                .flat_map(|s| s.edge_type_ids.iter().copied())
                .filter(|t| seen.insert(*t))
                .collect()
        };
        // A quantified pattern's hops may differ in direction; its final
        // vertex is a candidate whichever way the last hop went.
        let accepting_direction = if self.steps.len() == 1 {
            self.steps[0].direction
        } else {
            Direction::Both
        };
        self.accepting = build_accepting_vid_filter(
            &self.graph_ctx,
            &all_types,
            accepting_direction,
            self.target_label.as_deref(),
            &self.target_conditions,
        )
        .await?;
        self.edges = edges;
        self.vertices = vertices;
        self.whole_type = true;
        // The region state is dead weight from here on.
        self.depth = rustc_hash::FxHashMap::default();
        self.evaluated_edges = vec![FxHashSet::default(); self.steps.len()];
        self.evaluated_step_vertices = vec![FxHashSet::default(); self.steps.len()];
        self.evaluated_vertices = FxHashSet::default();
        Ok(())
    }
}

impl Stream for GraphTraverseMainStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            let state = std::mem::replace(&mut self.state, GraphTraverseMainState::Done);

            match state {
                GraphTraverseMainState::CollectingInput {
                    mut input_stream,
                    mut buffered,
                } => match input_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        buffered.push(batch);
                        self.state = GraphTraverseMainState::CollectingInput {
                            input_stream,
                            buffered,
                        };
                        // Continue loop to keep draining.
                    }
                    Poll::Ready(Some(Err(e))) => {
                        self.state = GraphTraverseMainState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        // Input fully drained: kick off the edge load with the
                        // bounded source set pushed into the scan (when small).
                        let source_vids = self.collect_source_vids(&buffered);
                        let loading_ctx = self.graph_ctx.clone();
                        let loading_types = self.type_names.clone();
                        let direction = self.direction;
                        let target_properties = self.target_properties.clone();
                        let fut = async move {
                            build_edge_adjacency_and_target_props(
                                &loading_ctx,
                                &loading_types,
                                direction,
                                source_vids,
                                &target_properties,
                            )
                            .await
                        };
                        self.state = GraphTraverseMainState::LoadingEdges {
                            future: Box::pin(fut),
                            buffered,
                        };
                        // Continue loop to poll the load future.
                    }
                    Poll::Pending => {
                        self.state = GraphTraverseMainState::CollectingInput {
                            input_stream,
                            buffered,
                        };
                        return Poll::Pending;
                    }
                },
                GraphTraverseMainState::LoadingEdges {
                    mut future,
                    buffered,
                } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok((adjacency, target_props))) => {
                        // The adjacency is resident from here until the stream
                        // ends, and is the largest thing this operator holds.
                        // The buffered input is already reserved; resizing to
                        // the sum keeps one reservation honest about both.
                        // Charged against one footprint, not summed per batch:
                        // `get_array_memory_size` reports whole buffer capacity,
                        // so buffered batches sliced from a common parent each
                        // report that parent and the sum bills it once per
                        // batch. Right for a single retained batch -- holding a
                        // slice really does keep the allocation resident --
                        // wrong the moment they are added up.
                        let mut footprint = crate::query::df_graph::common::BatchFootprint::new();
                        let buffered_bytes: usize = buffered.iter().map(|b| footprint.add(b)).sum();
                        self.buffered_bytes = buffered_bytes;
                        if let Err(e) = self
                            .reservation
                            .try_resize(buffered_bytes + estimate_adjacency_bytes(&adjacency))
                        {
                            self.state = GraphTraverseMainState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        // Move to processing state with loaded adjacency
                        self.state = GraphTraverseMainState::Processing {
                            adjacency,
                            target_props: Arc::new(target_props),
                            buffered: buffered.into_iter(),
                        };
                        // Continue loop to start processing
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = GraphTraverseMainState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = GraphTraverseMainState::LoadingEdges { future, buffered };
                        return Poll::Pending;
                    }
                },
                GraphTraverseMainState::Processing {
                    adjacency,
                    target_props,
                    mut buffered,
                } => {
                    // Check timeout
                    if let Err(e) = self.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(exec_err(e))));
                    }

                    match buffered.next() {
                        Some(batch) => {
                            // Expand batch using adjacency map
                            let result = self.expand_batch(&batch, &adjacency, &target_props);

                            // The expanded batch is this operator's peak and was
                            // outside the reservation, which covered only the
                            // buffered input and the adjacency map (#242). A
                            // fan-out expansion is larger than the input it came
                            // from — measured at a 13 MB ceiling, 200 000
                            // expanded rows passed while the pool saw 11.7 MB of
                            // adjacency and nothing else.
                            //
                            // `try_resize`, not `try_grow`: this replaces the
                            // previous batch's share, which went out with it.
                            // Growing would charge the operator for every batch
                            // it has ever emitted.
                            if let Ok(ref r) = result {
                                let held = self.buffered_bytes
                                    + estimate_adjacency_bytes(&adjacency)
                                    + r.get_array_memory_size();
                                if let Err(e) = self.reservation.try_resize(held) {
                                    self.state = GraphTraverseMainState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }

                            self.state = GraphTraverseMainState::Processing {
                                adjacency,
                                target_props,
                                buffered,
                            };

                            if let Ok(ref r) = result {
                                self.metrics.record_output(r.num_rows());
                            }
                            return Poll::Ready(Some(result));
                        }
                        None => {
                            self.state = GraphTraverseMainState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                GraphTraverseMainState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for GraphTraverseMainStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Variable-length graph traversal execution plan.
///
/// Performs BFS traversal from source vertices with configurable min/max hops.
/// Tracks visited nodes to avoid cycles.
///
/// # Example
///
/// ```ignore
/// // Find all nodes 1-3 hops away via KNOWS edges
/// let traverse = GraphVariableLengthTraverseExec::new(
///     input_plan,
///     "_vid",
///     knows_type_id,
///     Direction::Outgoing,
///     1,  // min_hops
///     3,  // max_hops
///     Some("p"), // path variable
///     graph_ctx,
/// );
/// ```
pub struct GraphVariableLengthTraverseExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Column name containing source VIDs.
    source_column: String,

    /// Edge type IDs to traverse.
    edge_type_ids: Vec<u32>,

    /// Traversal direction.
    direction: Direction,

    /// Minimum number of hops.
    min_hops: usize,

    /// Maximum number of hops.
    max_hops: usize,

    /// Variable name for target vertex columns.
    target_variable: String,

    /// Variable name for relationship list (r in `[r*]`) - holds `List<Edge>`.
    step_variable: Option<String>,

    /// Variable name for path (if path is bound).
    path_variable: Option<String>,

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Target label name for property type resolution.
    target_label_name: Option<String>,

    /// Whether this is an optional match (LEFT JOIN semantics).
    is_optional: bool,

    /// Column name of an already-bound target VID (for patterns where target is in scope).
    bound_target_column: Option<String>,

    /// Simple property equality conditions for per-edge L0 checking during BFS.
    /// Each entry is (property_name, expected_value).
    edge_property_conditions: Vec<(String, UniValue)>,
    /// Conditions from the target node's inline property map, used to narrow
    /// the accepting set. Purely a pruning input -- the planner's `FilterExec`
    /// above this operator is what makes the predicate authoritative.
    target_property_conditions: Vec<(String, UniValue)>,

    /// Edge ID columns from previous hops for cross-pattern relationship uniqueness.
    used_edge_columns: Vec<String>,

    /// Path semantics mode (Trail = no repeated edges, default for OpenCypher).
    path_mode: super::nfa::PathMode,

    /// Output mode determining BFS strategy.
    output_mode: super::nfa::VlpOutputMode,

    /// Per-hop filter inputs for a quantified path pattern; empty for a simple
    /// variable-length pattern, which uses the single `edge_property_conditions`
    /// above. Indexed by the slots the NFA stamps onto transitions and states.
    qpp_step_filters: Vec<QppStepFilterSpec>,

    /// GQL group variables bound inside a quantified path pattern, each
    /// emitted as a list column striding the matched path. Empty for a simple
    /// variable-length pattern, and for a quantified one whose inner variables
    /// are never referenced — the planner prunes those so the traversal can
    /// stay on the endpoint-only fast path.
    ///
    /// Iterated in this order by *both* `build_schema` and `build_output_batch`;
    /// the columns are positional, so a divergence would silently transpose two
    /// same-typed group columns rather than fail.
    qpp_group_bindings: Vec<QppGroupBinding>,

    /// Compiled NFA for path pattern matching.
    nfa: Arc<PathNfa>,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphVariableLengthTraverseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphVariableLengthTraverseExec")
            .field("source_column", &self.source_column)
            .field("edge_type_ids", &self.edge_type_ids)
            .field("direction", &self.direction)
            .field("min_hops", &self.min_hops)
            .field("max_hops", &self.max_hops)
            .field("target_variable", &self.target_variable)
            .finish()
    }
}

impl GraphVariableLengthTraverseExec {
    /// Create a new variable-length traversal plan.
    ///
    /// For QPP (Quantified Path Patterns), pass a pre-compiled NFA via `qpp_nfa`.
    /// For simple VLP patterns, pass `None` and the NFA will be compiled from
    /// `edge_type_ids`, `direction`, `min_hops`, `max_hops`.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
        target_variable: impl Into<String>,
        step_variable: Option<String>,
        path_variable: Option<String>,
        target_properties: Vec<String>,
        target_label_name: Option<String>,
        graph_ctx: Arc<GraphExecutionContext>,
        is_optional: bool,
        bound_target_column: Option<String>,
        edge_property_conditions: Vec<(String, UniValue)>,
        target_property_conditions: Vec<(String, UniValue)>,
        used_edge_columns: Vec<String>,
        path_mode: super::nfa::PathMode,
        output_mode: super::nfa::VlpOutputMode,
        qpp_nfa: Option<PathNfa>,
        qpp_step_filters: Vec<QppStepFilterSpec>,
        qpp_group_bindings: Vec<QppGroupBinding>,
    ) -> Self {
        let source_column = source_column.into();
        let target_variable = target_variable.into();

        // Resolve target property Arrow types from the schema
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let label_props = target_label_name
            .as_deref()
            .and_then(|ln| uni_schema.properties.get(ln));

        // Build output schema
        let schema = Self::build_schema(
            input.schema(),
            &target_variable,
            step_variable.as_deref(),
            path_variable.as_deref(),
            &target_properties,
            label_props,
            &qpp_group_bindings,
        );
        let properties = compute_plan_properties(schema.clone());

        // Use pre-compiled QPP NFA if provided, otherwise compile from VLP parameters
        let nfa = Arc::new(qpp_nfa.unwrap_or_else(|| {
            PathNfa::from_vlp(edge_type_ids.clone(), direction, min_hops, max_hops)
        }));

        Self {
            input,
            source_column,
            edge_type_ids,
            direction,
            min_hops,
            max_hops,
            target_variable,
            step_variable,
            path_variable,
            target_properties,
            target_label_name,
            is_optional,
            bound_target_column,
            edge_property_conditions,
            target_property_conditions,
            used_edge_columns,
            path_mode,
            output_mode,
            qpp_step_filters,
            qpp_group_bindings,
            nfa,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema.
    fn build_schema(
        input_schema: SchemaRef,
        target_variable: &str,
        step_variable: Option<&str>,
        path_variable: Option<&str>,
        target_properties: &[String],
        label_props: Option<
            &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
        >,
        qpp_group_bindings: &[QppGroupBinding],
    ) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add target VID column (only if not already in input)
        let target_vid_name = format!("{}._vid", target_variable);
        if input_schema.column_with_name(&target_vid_name).is_none() {
            fields.push(Field::new(target_vid_name, DataType::UInt64, true));
        }

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", target_variable);
        if input_schema.column_with_name(&target_labels_name).is_none() {
            fields.push(Field::new(target_labels_name, labels_data_type(), true));
        }

        // Add target vertex property columns (skip if already in input)
        for prop_name in target_properties {
            let col_name = format!("{}.{}", target_variable, prop_name);
            if input_schema.column_with_name(&col_name).is_none() {
                let arrow_type = resolve_property_type(prop_name, label_props);
                let uni_type = label_props
                    .and_then(|p| p.get(prop_name))
                    .map(|m| &m.r#type);
                fields.push(property_field(&col_name, arrow_type, uni_type));
            }
        }

        // Add hop count
        // Nullable: an OPTIONAL carrier row (no path matched) has no hop
        // count, and a NULL here is what lets the clause's closing
        // `OptionalFilterExec` tell it from a zero-hop match when the
        // traversal introduces no other identity column (an anonymous
        // relationship into an already-bound node).
        fields.push(Field::new("_hop_count", DataType::UInt64, true));

        // Add step variable (edge list) if bound
        if let Some(step_var) = step_variable {
            fields.push(build_edge_list_field(step_var));
        }

        // Add path struct if bound (only if not already in input from prior BindFixedPath)
        if let Some(path_var) = path_variable
            && input_schema.column_with_name(path_var).is_none()
        {
            fields.push(build_path_struct_field(path_var));
        }

        // Group variable columns are appended LAST, and in binding order, so
        // the positions of everything above are untouched and
        // `build_output_batch` can push in the same order. Two group columns of
        // the same kind are structurally identical, so a transposition here
        // would be a wrong answer rather than an error — see the name assertion
        // at the push site.
        for binding in qpp_group_bindings {
            fields.push(match binding.kind {
                QppGroupKind::Node => build_node_list_field(&binding.name),
                QppGroupKind::Edge => build_edge_list_field(&binding.name),
            });
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphVariableLengthTraverseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphVariableLengthTraverseExec: {} --[{:?}*{}..{}]--> target, mode={:?}{}",
            self.source_column,
            self.edge_type_ids,
            self.min_hops,
            self.max_hops,
            self.output_mode,
            if self.qpp_group_bindings.is_empty() {
                String::new()
            } else {
                format!(
                    ", groups=[{}]",
                    self.qpp_group_bindings
                        .iter()
                        .map(|b| b.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        )
    }
}

impl ExecutionPlan for GraphVariableLengthTraverseExec {
    fn name(&self) -> &str {
        "GraphVariableLengthTraverseExec"
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
                "GraphVariableLengthTraverseExec requires exactly one child".to_string(),
            ));
        }

        // Pass the existing NFA to avoid recompilation (important for QPP NFA)
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.source_column.clone(),
            self.edge_type_ids.clone(),
            self.direction,
            self.min_hops,
            self.max_hops,
            self.target_variable.clone(),
            self.step_variable.clone(),
            self.path_variable.clone(),
            self.target_properties.clone(),
            self.target_label_name.clone(),
            self.graph_ctx.clone(),
            self.is_optional,
            self.bound_target_column.clone(),
            self.edge_property_conditions.clone(),
            self.target_property_conditions.clone(),
            self.used_edge_columns.clone(),
            self.path_mode.clone(),
            self.output_mode.clone(),
            Some((*self.nfa).clone()),
            self.qpp_step_filters.clone(),
            self.qpp_group_bindings.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let pool_for_budget = Arc::clone(context.memory_pool());
        let reservation =
            MemoryConsumer::new(format!("GraphVariableLengthTraverseExec[{partition}]"))
                .register(context.memory_pool());
        // Read before `context` is moved into the input's `execute`.
        let slice_size = context.session_config().batch_size().max(1);
        let input_stream = self.input.execute(partition, context)?;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Warm the adjacency CSRs, then (when the pattern carries inline
        // edge-property conditions) build the real `EidFilter` from a property
        // pre-scan so that flushed CSR/Lance edges are filtered too — not just
        // L0 in-memory edges. See `build_edge_property_filter`.
        let graph_ctx = self.graph_ctx.clone();
        let edge_type_ids = self.edge_type_ids.clone();
        let direction = self.direction;
        let edge_property_conditions = self.edge_property_conditions.clone();
        let target_property_conditions = self.target_property_conditions.clone();
        let target_label_for_accepting = self.target_label_name.clone();
        let qpp_step_filters = self.qpp_step_filters.clone();
        let max_hops = self.max_hops;
        let warm_fut: Pin<Box<dyn std::future::Future<Output = DFResult<WarmedFilters>> + Send>> =
            Box::pin(async move {
                graph_ctx
                    .ensure_adjacency_warmed(&edge_type_ids, direction)
                    .await
                    .map_err(exec_err)?;

                // The maps are evaluated over the reachable region, batch by
                // batch: one hop slot for a simple pattern, one per hop of the
                // iteration for a quantified one, each with its own types,
                // direction and maps.
                let reachable = if qpp_step_filters.is_empty() {
                    ReachableMapFilter::new(
                        &graph_ctx,
                        &edge_type_ids,
                        direction,
                        max_hops,
                        &edge_property_conditions,
                        target_label_for_accepting.as_deref(),
                        &target_property_conditions,
                    )
                } else {
                    ReachableMapFilter::for_steps(
                        &graph_ctx,
                        &qpp_step_filters,
                        max_hops,
                        target_label_for_accepting.as_deref(),
                        &target_property_conditions,
                    )
                };
                if let Some(reachable) = reachable {
                    return Ok(WarmedFilters {
                        edges: vec![EidFilter::AllAllowed],
                        vertices: vec![VidFilter::AllAllowed],
                        accepting: VidFilter::AllAllowed,
                        reachable: Some(reachable),
                    });
                }

                let accepting = build_accepting_vid_filter(
                    &graph_ctx,
                    &edge_type_ids,
                    direction,
                    target_label_for_accepting.as_deref(),
                    &target_property_conditions,
                )
                .await?;

                if qpp_step_filters.is_empty() {
                    // Simple VLP: one step, one slot. Same single filter as before.
                    let edges = build_edge_property_filter(
                        &graph_ctx,
                        &edge_type_ids,
                        direction,
                        &edge_property_conditions,
                    )
                    .await?;
                    return Ok(WarmedFilters {
                        edges: vec![edges],
                        vertices: vec![VidFilter::AllAllowed],
                        accepting,
                        reachable: None,
                    });
                }

                // One filter per hop, in the same order the NFA's slots index.
                // A hop with no map costs no scan — both builders short-circuit
                // on empty conditions.
                let mut edges = Vec::with_capacity(qpp_step_filters.len());
                let mut vertices = Vec::with_capacity(qpp_step_filters.len());
                for step in &qpp_step_filters {
                    edges.push(
                        build_edge_property_filter(
                            &graph_ctx,
                            &step.edge_type_ids,
                            step.direction,
                            &step.edge_conditions,
                        )
                        .await?,
                    );
                    vertices.push(
                        build_vertex_property_filter(
                            &graph_ctx,
                            &step.edge_type_ids,
                            step.direction,
                            step.target_label.as_deref(),
                            &step.target_conditions,
                        )
                        .await?,
                    );
                }
                Ok(WarmedFilters {
                    edges,
                    vertices,
                    accepting,
                    reachable: None,
                })
            });

        Ok(Box::pin(GraphVariableLengthTraverseStream {
            memory_pool: pool_for_budget,
            reservation,
            input: input_stream,
            exec: Arc::new(self.clone_for_stream()),
            schema: self.schema.clone(),
            state: VarLengthStreamState::Warming(warm_fut),
            // Replaced by the warming result before any batch is read.
            edge_property_filters: vec![EidFilter::AllAllowed],
            vertex_property_filters: vec![VidFilter::AllAllowed],
            accepting_vid_filter: VidFilter::AllAllowed,
            reachable: None,
            metrics,
            slice_size,
            // Starts at one row, not at `slice_size`, and the difference is the
            // whole fix. Retuning only reacts to a pass that has already
            // happened, so a wide first chunk is unbounded by construction: on
            // a 10-row input a `slice_size` start swallows the batch and the
            // adaptation never gets a turn. Measured that way, an 11.1M-path
            // enumeration still asked for 1846 MB in one request.
            //
            // Sizing the first pass correctly would need to know fan-out before
            // expanding, which is exactly the cardinality estimate the system
            // does not have (#260). Absent that, starting conservative and
            // doubling is the safe direction: every row is expanded exactly
            // once whatever the chunking, so the ramp adds per-call overhead
            // over `O(log n)` passes rather than repeating any work.
            rows_per_chunk: 1,
            paused_row: None,
            pending_rows: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl GraphVariableLengthTraverseExec {
    /// Clone fields needed for stream (avoids cloning the full struct).
    fn clone_for_stream(&self) -> GraphVariableLengthTraverseExecData {
        GraphVariableLengthTraverseExecData {
            source_column: self.source_column.clone(),
            edge_type_ids: self.edge_type_ids.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            target_variable: self.target_variable.clone(),
            step_variable: self.step_variable.clone(),
            path_variable: self.path_variable.clone(),
            target_properties: self.target_properties.clone(),
            target_label_name: self.target_label_name.clone(),
            is_optional: self.is_optional,
            bound_target_column: self.bound_target_column.clone(),
            edge_property_conditions: self.edge_property_conditions.clone(),
            used_edge_columns: self.used_edge_columns.clone(),
            path_mode: self.path_mode.clone(),
            output_mode: self.output_mode.clone(),
            qpp_group_bindings: self.qpp_group_bindings.clone(),
            hops_per_iter: self.qpp_step_filters.len().max(1),
            nfa: self.nfa.clone(),
            graph_ctx: self.graph_ctx.clone(),
        }
    }
}

/// Data needed by the stream (without ExecutionPlan overhead).
#[expect(
    dead_code,
    reason = "Fields accessed via NFA; kept for with_new_children reconstruction"
)]
struct GraphVariableLengthTraverseExecData {
    source_column: String,
    edge_type_ids: Vec<u32>,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
    target_variable: String,
    step_variable: Option<String>,
    path_variable: Option<String>,
    target_properties: Vec<String>,
    target_label_name: Option<String>,
    is_optional: bool,
    bound_target_column: Option<String>,
    /// Simple property equality conditions for per-edge L0 checking during BFS.
    edge_property_conditions: Vec<(String, UniValue)>,
    used_edge_columns: Vec<String>,
    path_mode: super::nfa::PathMode,
    output_mode: super::nfa::VlpOutputMode,
    /// Group variables to materialize, in schema order. See the exec's field.
    qpp_group_bindings: Vec<QppGroupBinding>,
    /// Steps per quantifier iteration — the stride between successive elements
    /// of a group variable. Taken from the per-hop filter list rather than
    /// recomputed from `min_hops`, which is already the multiplied value.
    hops_per_iter: usize,
    nfa: Arc<PathNfa>,
    graph_ctx: Arc<GraphExecutionContext>,
}

/// Safety cap for frontier size to prevent OOM on pathological graphs.
/// Hop ceiling the planner supplies when a variable-length pattern writes no
/// upper bound (`[*]`, `[*2..]`).
///
/// Public because the traversal has to recognise its own default: a search that
/// stops here with more graph left has been truncated by a limit the *user did
/// not write*, and that is worth saying out loud. An explicitly written bound
/// is the user getting what they asked for and is not reported.
///
/// A pattern written `[*1..100]` is indistinguishable from `[*]` at this point
/// — the "was it written" bit does not survive the `LogicalPlan` boundary — so
/// such a query is reported as though the bound were the default. The warning
/// is true either way; only its framing is slightly off, and an explicit `..100`
/// is rare enough that threading a flag through the plan is not worth it.
pub const DEFAULT_MAX_HOPS: usize = 100;

const MAX_FRONTIER_SIZE: usize = 500_000;
/// Safety cap for predecessor pool size.
const MAX_PRED_POOL_SIZE: usize = 2_000_000;

/// Bounds path enumeration *while* it runs, instead of after it has finished.
///
/// `bfs_with_dag` keeps a node path and an edge path per enumerated path. Under
/// trail semantics on a cyclic graph that set is combinatorial: issue #285
/// reached 3 GB from a 30-vertex, 90-edge store, and over 180 GB on a real
/// 852-entity ownership graph with 39 cycles. The BFS itself is cheap and
/// terminates — `MAX_FRONTIER_SIZE` and `MAX_PRED_POOL_SIZE` see to that — but
/// they bound the search, not the enumeration that follows it.
///
/// The stream already reserves for the result of this work, and that is exactly
/// why it could not help: a reservation taken afterwards can only be refused for
/// memory the process has already committed, which is also why `max_memory`
/// overshot its limit twelvefold (issue #284). Growing the reservation from
/// inside the enumeration callback is what makes the limit bind — the pool
/// refuses, enumeration stops, and the query fails with a memory error instead
/// of the OS killing the process.
///
/// Both checks are amortized, because the callback runs once per path and a
/// pool round-trip or clock read per path would dominate the enumeration it is
/// meant to guard.
struct PathBudget {
    reservation: MemoryReservation,
    /// Bytes admitted since the last `try_grow`.
    pending: usize,
    /// Paths admitted since the last deadline check.
    since_deadline_check: u32,
    /// Set when the pool refused or the deadline passed; enumeration then stops
    /// and the caller turns this into the query's error.
    error: Option<datafusion::error::DataFusionError>,
}

/// Grow the reservation once per this many bytes of admitted paths.
const BUDGET_GROW_BYTES: usize = 1 << 20;
/// Check the deadline once per this many admitted paths.
const BUDGET_DEADLINE_STRIDE: u32 = 4096;

impl PathBudget {
    fn new(pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            reservation: MemoryConsumer::new("GraphVariableLengthTraverse[enumeration]")
                .register(pool),
            pending: 0,
            since_deadline_check: 0,
            error: None,
        }
    }

    /// Accounts one enumerated path and says whether enumeration may continue.
    fn admit(
        &mut self,
        nodes: &[Vid],
        edges: &[Eid],
        graph_ctx: &GraphExecutionContext,
    ) -> ControlFlow<()> {
        self.pending += std::mem::size_of::<BfsResult>()
            + std::mem::size_of_val(nodes)
            + std::mem::size_of_val(edges);
        if self.pending >= BUDGET_GROW_BYTES {
            if let Err(e) = self.reservation.try_grow(self.pending) {
                self.error = Some(e);
                return ControlFlow::Break(());
            }
            self.pending = 0;
        }

        self.since_deadline_check += 1;
        if self.since_deadline_check >= BUDGET_DEADLINE_STRIDE {
            self.since_deadline_check = 0;
            if let Err(e) = graph_ctx.check_timeout() {
                self.error = Some(exec_err(e));
                return ControlFlow::Break(());
            }
        }

        ControlFlow::Continue(())
    }

    fn tripped(&self) -> bool {
        self.error.is_some()
    }

    fn take_error(&mut self) -> Option<datafusion::error::DataFusionError> {
        self.error.take()
    }
}

impl GraphVariableLengthTraverseExecData {
    /// Report that the variable-length search hit a safety cap and stopped
    /// early, so the results below are incomplete.
    fn warn_search_truncated(&self, depth: u32, frontier: usize, pool: usize) {
        self.graph_ctx
            .push_warning(crate::types::QueryWarning::Other(format!(
                "Variable-length traversal stopped at depth {depth} after hitting a safety cap \
                 (frontier {frontier}/{MAX_FRONTIER_SIZE}, predecessor pool \
                 {pool}/{MAX_PRED_POOL_SIZE}); results are incomplete. Narrow the pattern with a \
                 smaller upper bound, a relationship type, or a label on the target."
            )));
    }

    /// Report that the search stopped at the *default* hop ceiling with more of
    /// the graph still reachable, so the rows below are incomplete.
    ///
    /// The safety caps already report their own truncation
    /// ([`Self::warn_search_truncated`]). This is the same consequence —
    /// results that look complete and are not — arriving through a different
    /// door, and it was silent until now: a 150-vertex chain answered
    /// `MATCH p=(a)-[:R*]->(b)` with 100 paths, a longest length of 100, and an
    /// empty warnings list.
    fn warn_default_hop_bound(&self, frontier: usize) {
        self.graph_ctx
            .push_warning(crate::types::QueryWarning::Other(format!(
                "Variable-length pattern has no upper bound, so the default of \
                 {DEFAULT_MAX_HOPS} hops applied, and {frontier} vertices were still \
                 reachable at that depth; results are incomplete. Write an explicit \
                 upper bound (for example `*1..{DEFAULT_MAX_HOPS}`) to state the depth \
                 you want."
            )));
    }

    /// Check if a vertex passes the target label filter.
    fn check_target_label(&self, vid: Vid) -> bool {
        if let Some(ref label_name) = self.target_label_name {
            let query_ctx = self.graph_ctx.query_context();
            // Resolve from the L0 chain then the persisted `VidLabelsIndex`, so a
            // flushed (Lance-only) vertex is judged against its real labels rather
            // than admitted by a fail-open L0-only miss (#141). A vertex unknown to
            // both still trusts storage-level filtering.
            match self.graph_ctx.resolve_vertex_labels(vid, &query_ctx) {
                Some(labels) => labels.contains(label_name),
                None => true, // unknown to L0 and the index — trust storage
            }
        } else {
            true
        }
    }

    /// Check if a vertex satisfies an NFA state constraint (QPP intermediate node label).
    fn check_state_constraint(&self, vid: Vid, constraint: &super::nfa::VertexConstraint) -> bool {
        match constraint {
            super::nfa::VertexConstraint::Label(label_name) => {
                let query_ctx = self.graph_ctx.query_context();
                // Resolve from the L0 chain then the persisted `VidLabelsIndex`, so a
                // flushed intermediate node is judged against its real labels rather
                // than admitted by a fail-open L0-only miss — otherwise a `*1..n`
                // QPP over-admits paths through wrong-label intermediates (#141).
                match self.graph_ctx.resolve_vertex_labels(vid, &query_ctx) {
                    Some(labels) => labels.contains(label_name),
                    None => true, // unknown to L0 and the index — trust storage
                }
            }
        }
    }

    /// Expand neighbors from a vertex through all NFA transitions from the given state.
    /// Returns (neighbor_vid, neighbor_eid, destination_nfa_state) triples.
    fn expand_neighbors(
        &self,
        vid: Vid,
        state: NfaStateId,
        eid_filters: &[EidFilter],
        vertex_filters: &[VidFilter],
        used_eids: &FxHashSet<u64>,
    ) -> Vec<(Vid, Eid, NfaStateId)> {
        let is_undirected = matches!(self.direction, Direction::Both);
        let mut results = Vec::new();

        for transition in self.nfa.transitions_from(state) {
            // A quantified pattern's inner source label, on the vertex this
            // hop starts a new iteration from.
            if let Some(constraint) = &transition.source_constraint
                && !self.check_state_constraint(vid, constraint)
            {
                continue;
            }
            let mut seen_edges: FxHashSet<u64> = FxHashSet::default();

            for &etype in &transition.edge_type_ids {
                for (neighbor, eid) in
                    self.graph_ctx
                        .get_neighbors(vid, etype, transition.direction)
                {
                    // Deduplicate edges for undirected patterns
                    if is_undirected && !seen_edges.insert(eid.as_u64()) {
                        continue;
                    }

                    // Edge-property predicate gate. When the pattern carries
                    // inline `{prop: value}` conditions, `eid_filter` is the
                    // allow-set of edges (flushed *and* L0) that satisfy them,
                    // built during warming by `build_edge_property_filter`. When
                    // there are no conditions it is `AllAllowed`. This replaces
                    // the old bifurcated check that filtered only L0 edges and
                    // let every flushed CSR/Lance edge through (H4).
                    // Per-hop gate. Fail closed on an out-of-range slot: it can
                    // only be a programmer error in the slot/filter lockstep,
                    // and failing open would re-create exactly the
                    // over-matching this change exists to remove.
                    debug_assert!(
                        transition.edge_filter_slot < eid_filters.len(),
                        "edge filter slot {} out of range ({} filters)",
                        transition.edge_filter_slot,
                        eid_filters.len()
                    );
                    if !eid_filters
                        .get(transition.edge_filter_slot)
                        .is_some_and(|f| f.contains(eid))
                    {
                        continue;
                    }

                    // Check cross-pattern relationship uniqueness
                    if used_eids.contains(&eid.as_u64()) {
                        continue;
                    }

                    // Check NFA state constraint on the destination state (QPP label filters)
                    if let Some(constraint) = self.nfa.state_constraint(transition.to)
                        && !self.check_state_constraint(neighbor, constraint)
                    {
                        continue;
                    }

                    // …and the destination's property map, if that hop had one.
                    // Precomputed during warming rather than read here: the
                    // only synchronous accessor is the L0-only chain, which
                    // reports a flushed vertex as property-less (#135/#141).
                    // Fails closed on a bad slot, as above.
                    if let Some(slot) = self.nfa.vertex_filter_slot(transition.to) {
                        debug_assert!(
                            slot < vertex_filters.len(),
                            "vertex filter slot {slot} out of range ({} filters)",
                            vertex_filters.len()
                        );
                        if !vertex_filters
                            .get(slot)
                            .is_some_and(|f| f.contains(neighbor))
                        {
                            continue;
                        }
                    }

                    results.push((neighbor, eid, transition.to));
                }
            }
        }

        results
    }

    /// NFA-driven BFS building the predecessor DAG for full path enumeration
    /// (Mode B), returning it with the accepting `(target, state, depth)`
    /// triples the search reached.
    ///
    /// The enumeration that turns this into paths is deliberately *not* done
    /// here. One source vertex in a cyclic graph can own more paths than any
    /// batch should hold, so the caller walks the DAG through a
    /// [`PathEnumerator`] it can pause -- which means the DAG has to outlive a
    /// single call.
    fn build_path_dag(
        &self,
        source: Vid,
        eid_filters: &[EidFilter],
        vertex_filters: &[VidFilter],
        used_eids: &FxHashSet<u64>,
        vid_filter: &VidFilter,
    ) -> (PredecessorDag, Vec<(Vid, NfaStateId, u32)>) {
        let nfa = &self.nfa;
        let selector = PathSelector::All;
        let mut dag = PredecessorDag::new(selector);
        let mut accepting: Vec<(Vid, NfaStateId, u32)> = Vec::new();

        // Handle zero-length paths (min_hops == 0).
        //
        // `vid_filter` is deliberately NOT consulted here. It is built from the
        // vertices reachable over this pattern's edge types, and the source of
        // a zero-length path need not be one of them -- an isolated vertex is
        // its own zero-length path and appears in no adjacency list, so
        // filtering on it would drop a row nothing downstream can restore. The
        // `FilterExec` above still applies the target predicate, so the row is
        // checked, just not here.
        if nfa.is_accepting(nfa.start_state()) && self.check_target_label(source) {
            accepting.push((source, nfa.start_state(), 0));
        }

        // Per-depth frontier BFS
        let mut frontier: Vec<(Vid, NfaStateId)> = vec![(source, nfa.start_state())];
        let mut depth: u32 = 0;

        while !frontier.is_empty() && depth < self.max_hops as u32 {
            depth += 1;
            let mut next_frontier: Vec<(Vid, NfaStateId)> = Vec::new();
            let mut seen_at_depth: FxHashSet<(Vid, NfaStateId)> = FxHashSet::default();

            for &(vid, state) in &frontier {
                for (neighbor, eid, dst_state) in
                    self.expand_neighbors(vid, state, eid_filters, vertex_filters, used_eids)
                {
                    // Record in predecessor DAG
                    dag.add_predecessor(neighbor, dst_state, vid, state, eid, depth);

                    // Add to next frontier (deduplicated per depth)
                    if seen_at_depth.insert((neighbor, dst_state)) {
                        next_frontier.push((neighbor, dst_state));

                        // Check if accepting
                        if nfa.is_accepting(dst_state)
                            && self.check_target_label(neighbor)
                            && vid_filter.contains(neighbor)
                        {
                            accepting.push((neighbor, dst_state, depth));
                        }
                    }
                }
            }

            // Safety cap. Abandoning the search mid-way makes the result
            // incomplete, and with a step or group variable bound that means
            // *missing rows*, not merely a missed endpoint — so say so rather
            // than returning a short answer that looks complete.
            if next_frontier.len() > MAX_FRONTIER_SIZE || dag.pool_len() > MAX_PRED_POOL_SIZE {
                self.warn_search_truncated(depth, next_frontier.len(), dag.pool_len());
                break;
            }

            frontier = next_frontier;
        }

        // The loop can exit three ways: the frontier emptied (the search is
        // complete), a safety cap tripped (already reported), or the hop
        // ceiling was reached with work left. Only the third is silent, and
        // only when the ceiling is the one the planner supplied rather than one
        // the user wrote.
        if !frontier.is_empty()
            && depth >= self.max_hops as u32
            && self.max_hops == DEFAULT_MAX_HOPS
        {
            self.warn_default_hop_bound(frontier.len());
        }

        (dag, accepting)
    }

    /// Build the DAG and enumerate every path out of it, in one call.
    ///
    /// The run-to-completion form, kept for callers that expand a whole chunk
    /// of input rows at once. Every admitted path is charged to `budget` before
    /// it is kept, so the memory limit and the deadline bind as the set accrues
    /// rather than after it is resident. See [`PathBudget`].
    fn bfs_with_dag(
        &self,
        source: Vid,
        eid_filters: &[EidFilter],
        vertex_filters: &[VidFilter],
        used_eids: &FxHashSet<u64>,
        vid_filter: &VidFilter,
        budget: &mut PathBudget,
    ) -> DFResult<Vec<BfsResult>> {
        let (dag, accepting) =
            self.build_path_dag(source, eid_filters, vertex_filters, used_eids, vid_filter);

        let mut results: Vec<BfsResult> = Vec::new();
        for &(target, state, depth) in &accepting {
            if budget.tripped() {
                break;
            }
            dag.enumerate_paths(
                source,
                target,
                state,
                depth,
                depth,
                &self.path_mode,
                &mut |nodes, edges| match budget.admit(nodes, edges, &self.graph_ctx) {
                    ControlFlow::Continue(()) => {
                        results.push((target, depth as usize, nodes.to_vec(), edges.to_vec()));
                        ControlFlow::Continue(())
                    }
                    ControlFlow::Break(()) => ControlFlow::Break(()),
                },
            );
        }

        if let Some(e) = budget.take_error() {
            return Err(e);
        }
        Ok(results)
    }

    /// NFA-driven BFS returning each reachable endpoint once per depth.
    ///
    /// Serves [`VlpOutputMode::Reachability`] only: it answers *whether* a
    /// trail reaches an endpoint, not *how many* do, so it is correct only
    /// where the consumer ignores multiplicity. Uses lightweight trail
    /// verification via `has_trail_valid_path()`.
    fn bfs_endpoints_only(
        &self,
        source: Vid,
        eid_filters: &[EidFilter],
        vertex_filters: &[VidFilter],
        used_eids: &FxHashSet<u64>,
        vid_filter: &VidFilter,
    ) -> Vec<(Vid, u32)> {
        let nfa = &self.nfa;

        // A plain `[:T*min..max]` trail decides from shortest distances (see
        // `trail_reach`); the hop-by-hop search below stays for every other
        // pattern and for a source that shortest distances cannot decide.
        // For the latter, no trail is longer than the relationships it can
        // reach, which bounds the search: on a cyclic graph a walk never runs
        // out of frontier, so it went on to the hop ceiling and then reported
        // a truncation that could not have hidden a trail.
        let mut trail_cap = usize::MAX;
        if self.path_mode == super::nfa::PathMode::Trail
            && let Some((min, max)) = nfa.simple_chain_range()
        {
            let hop = |v: Vid| -> Vec<(Vid, Eid)> {
                self.expand_neighbors(v, nfa.start_state(), eid_filters, vertex_filters, used_eids)
                    .into_iter()
                    .map(|(w, eid, _)| (w, eid))
                    .collect()
            };
            if let Some(found) = super::trail_reach::trail_endpoints(source, min, max, hop, |v| {
                self.check_target_label(v) && vid_filter.contains(v)
            }) {
                let mut results = Vec::with_capacity(found.endpoints.len() + 1);
                if min == 0 && self.check_target_label(source) {
                    results.push((source, 0));
                }
                results.extend(found.endpoints);
                if found.beyond_max > 0 && self.max_hops == DEFAULT_MAX_HOPS {
                    self.warn_default_hop_bound(found.beyond_max);
                }
                return results;
            }
            trail_cap = super::trail_reach::reachable_relationships(source, hop);
        }

        let selector = PathSelector::Any; // Only need existence, not all paths
        let mut dag = PredecessorDag::new(selector);
        let mut results: Vec<(Vid, u32)> = Vec::new();

        // Handle zero-length paths. `vid_filter` is skipped for the same reason
        // as in `build_path_dag`: it cannot speak for a vertex that appears in
        // no adjacency list.
        if nfa.is_accepting(nfa.start_state()) && self.check_target_label(source) {
            results.push((source, 0));
        }

        // Per-depth frontier BFS
        let mut frontier: Vec<(Vid, NfaStateId)> = vec![(source, nfa.start_state())];
        let mut depth: u32 = 0;

        let depth_limit = self.max_hops.min(trail_cap) as u32;
        while !frontier.is_empty() && depth < depth_limit {
            depth += 1;
            let mut next_frontier: Vec<(Vid, NfaStateId)> = Vec::new();
            let mut seen_at_depth: FxHashSet<(Vid, NfaStateId)> = FxHashSet::default();
            let mut accepting: Vec<(Vid, NfaStateId)> = Vec::new();

            for &(vid, state) in &frontier {
                for (neighbor, eid, dst_state) in
                    self.expand_neighbors(vid, state, eid_filters, vertex_filters, used_eids)
                {
                    dag.add_predecessor(neighbor, dst_state, vid, state, eid, depth);

                    if seen_at_depth.insert((neighbor, dst_state)) {
                        next_frontier.push((neighbor, dst_state));
                        if nfa.is_accepting(dst_state)
                            && self.check_target_label(neighbor)
                            && vid_filter.contains(neighbor)
                        {
                            accepting.push((neighbor, dst_state));
                        }
                    }
                }
            }

            // Trail validity is decided only once every predecessor at this
            // depth is recorded. Checking at first discovery saw whichever
            // predecessor the frontier happened to reach first; if that one's
            // path reused an edge the endpoint was dropped for good, although a
            // predecessor added later in the same depth had a valid trail — a
            // silent, order-dependent miss (undirected hops made it common).
            for (neighbor, state) in accepting {
                if dag.has_trail_valid_path(source, neighbor, state, depth, depth) {
                    results.push((neighbor, depth));
                }
            }

            // Safety cap. Abandoning the search mid-way makes the result
            // incomplete, and with a step or group variable bound that means
            // *missing rows*, not merely a missed endpoint — so say so rather
            // than returning a short answer that looks complete.
            if next_frontier.len() > MAX_FRONTIER_SIZE || dag.pool_len() > MAX_PRED_POOL_SIZE {
                self.warn_search_truncated(depth, next_frontier.len(), dag.pool_len());
                break;
            }

            frontier = next_frontier;
        }

        // The loop can exit three ways: the frontier emptied (the search is
        // complete), a safety cap tripped (already reported), or the hop
        // ceiling was reached with work left. Only the third is silent, and
        // only when the ceiling is the one the planner supplied rather than one
        // the user wrote, and only when a trail could be longer than it.
        if !frontier.is_empty()
            && depth >= self.max_hops as u32
            && self.max_hops == DEFAULT_MAX_HOPS
            && trail_cap > self.max_hops
        {
            self.warn_default_hop_bound(frontier.len());
        }

        results
    }
}

/// One input row whose path enumeration stopped part-way through.
///
/// A single source vertex in a cyclic graph can own more paths than any batch
/// should hold, and the enumeration used to be all-or-nothing: the operator
/// built every path before it could hand the first row downstream. That is why
/// an unbounded pattern cost the same with a `LIMIT` as without one (#285) --
/// the consumer could stop asking, but this could not stop producing.
///
/// Parking the DAG and the cursor here makes it ordinary back-pressure. The
/// operator emits one batch out of a row; if nothing pulls again, the rest of
/// that row's paths are never walked.
struct PausedRowEnumeration {
    /// The one-row input slice these paths expand from, re-emitted with every
    /// batch drained out of it. Expansions therefore always carry row index 0.
    chunk: RecordBatch,
    source: Vid,
    dag: PredecessorDag,
    accepting: Vec<(Vid, NfaStateId, u32)>,
    /// Next entry of `accepting` to open a cursor on.
    accept_idx: usize,
    /// The open cursor and the `(target, hops)` it is enumerating, if the last
    /// pass stopped inside an accepting entry rather than between two.
    cursor: Option<(PathEnumerator, Vid, usize)>,
}

/// State machine for variable-length traverse stream.
enum VarLengthStreamState {
    /// Warming adjacency CSRs before first batch. Resolves to the prebuilt
    /// edge-property `EidFilter` (the allow-set of edges — flushed *and* L0 —
    /// that satisfy the inline `{prop: value}` conditions), or
    /// `EidFilter::AllAllowed` when there are no conditions.
    Warming(Pin<Box<dyn std::future::Future<Output = DFResult<WarmedFilters>> + Send>>),
    /// Processing input batches.
    Reading,
    /// Growing the pattern's map filters over the region this batch's sources
    /// can reach, before the batch is walked. See [`ReachableMapFilter`].
    Covering {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<ReachableMapFilter>> + Send>>,
        batch: RecordBatch,
    },
    /// Fetching storage-backed properties for the entities on this batch's
    /// paths, before the (synchronous) column builders run. Only entered when
    /// the plan materializes entity structs.
    PrefetchingProperties {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<EntityPropertyCache>> + Send>>,
        input: RecordBatch,
        expansions: Vec<VarLengthExpansion>,
    },
    /// Materializing target vertex properties asynchronously.
    Materializing(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Expanding one row-chunk of the input batch held in `pending_rows`.
    ///
    /// The enumeration is the peak here, not the output: each expansion owns a
    /// node path and an edge path, so the set is bounded by paths times path
    /// length and by nothing else. Chunking the *materialization* — what the
    /// single-hop sibling does — would not touch that, because the whole set
    /// exists before materialization begins. Chunking the input rows is what
    /// bounds construction (#241).
    RowChunking,
    /// Stream is done.
    Done,
}

/// Rows to expand next, from what the last chunk actually produced.
///
/// Aims each pass at `target` expansions. Fan-out per row is unknowable ahead
/// of time and varies by orders of magnitude with depth, so a fixed width is
/// either far too coarse or pure overhead; this converges on whatever the data
/// is doing. Mirrors `retune_range_width` in the scan (#214).
fn retune_rows_per_chunk(rows: usize, expansions: usize, target: usize) -> usize {
    let target = target.max(1);
    if expansions > target {
        // Scale down by the ratio observed rather than halving, which would
        // take several passes to converge on a steep fan-out.
        (rows.saturating_mul(target) / expansions).max(1)
    } else if expansions.saturating_mul(2) < target {
        rows.saturating_mul(2)
    } else {
        rows.max(1)
    }
}

/// Stream for variable-length traversal.
struct GraphVariableLengthTraverseStream {
    input: SendableRecordBatchStream,
    exec: Arc<GraphVariableLengthTraverseExecData>,
    /// Pool the per-batch enumeration budget registers against. Held separately
    /// from `reservation` because enumeration happens under `&self`, and its
    /// peak has to be bounded as it accrues rather than after (see
    /// [`PathBudget`]).
    memory_pool: Arc<dyn MemoryPool>,
    schema: SchemaRef,
    state: VarLengthStreamState,
    /// Edge-property allow-set built during warming (see [`VarLengthStreamState::Warming`]).
    /// `AllAllowed` until warming completes (or when there are no edge-property
    /// conditions).
    edge_property_filters: Vec<EidFilter>,
    vertex_property_filters: Vec<VidFilter>,
    /// Narrows the accepting endpoints; `AllAllowed` until warming completes.
    accepting_vid_filter: VidFilter,
    /// The pattern's maps over the region reached so far, when they are
    /// evaluated that way; supersedes the edge and accepting filters above.
    /// Taken out while a batch's sources are being covered.
    reachable: Option<ReachableMapFilter>,
    metrics: BaselineMetrics,

    /// Target rows per output batch, and the expansion budget per pass.
    slice_size: usize,

    /// Input rows expanded per pass, retuned from the fan-out actually seen.
    ///
    /// The enumeration is multiplicative in a way the input row count cannot
    /// predict: on a mesh, one source row at depth 6 produces over a million
    /// paths, and the same row at depth 2 produces eleven. A fixed chunk width
    /// would be far too coarse for the first and pointless overhead for the
    /// second, so this tracks what the last pass actually produced. Same shape
    /// as the scan's `retune_range_width` (#214).
    rows_per_chunk: usize,

    /// A row whose path enumeration is part-done, and the DAG it is walking.
    ///
    /// Takes precedence over `pending_rows`: while this is set, every batch the
    /// operator emits comes out of the parked row's own chunk. See
    /// [`PausedRowEnumeration`].
    paused_row: Option<Box<PausedRowEnumeration>>,

    /// The input batch being consumed a row-chunk at a time, and where to
    /// resume in it.
    ///
    /// Held on the stream rather than threaded through every state variant:
    /// the expansion, the property prefetch and the target hydration all end by
    /// asking "what next", and this is the single place that answers.
    pending_rows: Option<(RecordBatch, usize)>,

    /// The query pool's accounting for this operator's expansion set.
    ///
    /// A variable-length expansion carries a node path and an edge path *per
    /// enumerated path*, so its size is bounded by paths times path length —
    /// bounded neither by the input nor by any table. The existing
    /// `MAX_FRONTIER_SIZE` and `MAX_PRED_POOL_SIZE` caps bound the search, not
    /// the enumeration that follows it, so this is where the query's budget
    /// gets a say.
    ///
    /// Since #241 the set covers one row-chunk rather than a whole input batch,
    /// so this reserves per pass. Measured: enumerating 11.1M paths asked for
    /// 1846 MB in a single request and failed against a 1 GiB pool; the same
    /// query now passes in bounded passes.
    reservation: MemoryReservation,
}

impl Stream for GraphVariableLengthTraverseStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            let state = std::mem::replace(&mut self.state, VarLengthStreamState::Done);

            match state {
                VarLengthStreamState::Warming(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(warmed)) => {
                        self.edge_property_filters = warmed.edges;
                        self.vertex_property_filters = warmed.vertices;
                        self.accepting_vid_filter = warmed.accepting;
                        self.reachable = warmed.reachable;
                        self.state = VarLengthStreamState::Reading;
                        // Continue loop to start reading
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VarLengthStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VarLengthStreamState::Warming(fut);
                        return Poll::Pending;
                    }
                },
                VarLengthStreamState::Reading => {
                    // Check timeout
                    if let Err(e) = self.exec.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(exec_err(e))));
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            if let Some(mut reachable) = self.reachable.take() {
                                // Grow the maps' region to this batch's sources
                                // before any of its rows is walked.
                                let sources = match batch
                                    .column_by_name(&self.exec.source_column)
                                    .map(|c| column_as_vid_array(c.as_ref()))
                                    .transpose()
                                {
                                    Ok(col) => col
                                        .map(|c| ReachableMapFilter::sources_of(&c))
                                        .unwrap_or_default(),
                                    Err(e) => {
                                        self.state = VarLengthStreamState::Done;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                };
                                let fut = Box::pin(async move {
                                    reachable.cover(&sources).await?;
                                    Ok(reachable)
                                });
                                self.state = VarLengthStreamState::Covering { fut, batch };
                                continue;
                            }
                            // Expansion happens a row-chunk at a time (#241),
                            // so the batch is parked and `RowChunking` drives.
                            self.pending_rows = Some((batch, 0));
                            self.state = VarLengthStreamState::RowChunking;
                            continue;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.state = VarLengthStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            self.state = VarLengthStreamState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            self.state = VarLengthStreamState::Reading;
                            return Poll::Pending;
                        }
                    }
                }
                VarLengthStreamState::Covering { mut fut, batch } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(reachable)) => {
                        self.reachable = Some(reachable);
                        self.pending_rows = Some((batch, 0));
                        self.state = VarLengthStreamState::RowChunking;
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VarLengthStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VarLengthStreamState::Covering { fut, batch };
                        return Poll::Pending;
                    }
                },
                VarLengthStreamState::RowChunking => {
                    if let Err(e) = self.exec.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(exec_err(e))));
                    }

                    // A row parked mid-enumeration outranks fresh input: its
                    // chunk is still the batch being emitted from, and pulling
                    // new rows first would strand it.
                    let (take_rows, input, expansions) =
                        if let Some(mut parked) = self.paused_row.take() {
                            let mut budget = PathBudget::new(&self.memory_pool);
                            let mut expansions: Vec<VarLengthExpansion> = Vec::new();
                            let exhausted = self.drain_paused_row(
                                &mut parked,
                                self.slice_size,
                                &mut budget,
                                &mut expansions,
                            );
                            if let Some(e) = budget.take_error() {
                                self.state = VarLengthStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }
                            let chunk = parked.chunk.clone();
                            if !exhausted {
                                self.paused_row = Some(parked);
                            }
                            (1, chunk, expansions)
                        } else {
                            let Some((held, offset)) = self.pending_rows.take() else {
                                self.state = VarLengthStreamState::Reading;
                                continue;
                            };
                            if offset >= held.num_rows() {
                                self.state = VarLengthStreamState::Reading;
                                continue;
                            }
                            let take_rows = self.rows_per_chunk.min(held.num_rows() - offset);
                            let chunk = held.slice(offset, take_rows);
                            self.pending_rows = Some((held, offset + take_rows));

                            // Build base batch synchronously (BFS + expand).
                            // The per-hop filters were built during warming and
                            // gate flushed edges/vertices by their properties.
                            // `accepting_vid_filter` is a different thing — it
                            // constrains the *final* vertex, and is built during
                            // warming from the target's inline property map.
                            let (edge_filters, vertex_filters, accepting) = match &self.reachable {
                                Some(r) => (r.edges(), r.vertices(), r.accepting()),
                                None => (
                                    self.edge_property_filters.as_slice(),
                                    self.vertex_property_filters.as_slice(),
                                    &self.accepting_vid_filter,
                                ),
                            };
                            let expanded =
                                self.expand_batch(chunk, edge_filters, vertex_filters, accepting);
                            match expanded {
                                Ok((input, expansions, parked)) => {
                                    self.paused_row = parked;
                                    (take_rows, input, expansions)
                                }
                                Err(e) => {
                                    self.state = VarLengthStreamState::Reading;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        };

                    {
                        {
                            // Aim the next pass at one batch's worth of
                            // expansions, from what this pass actually produced.
                            self.rows_per_chunk =
                                retune_rows_per_chunk(take_rows, expansions.len(), self.slice_size);

                            // The expansion set is this operator's peak, and it
                            // is Rust-side rather than Arrow: each entry owns a
                            // node path and an edge path. Estimated for the same
                            // reason the adjacency map is — the pool seeing an
                            // approximation beats it seeing nothing.
                            const EXPANSION: usize = std::mem::size_of::<VarLengthExpansion>();
                            let expansion_bytes: usize = expansions
                                .iter()
                                .map(|(_, _, _, nodes, edges)| {
                                    EXPANSION
                                        + nodes.capacity() * std::mem::size_of::<Vid>()
                                        + edges.capacity() * std::mem::size_of::<Eid>()
                                })
                                .sum();
                            if let Err(e) = self
                                .reservation
                                .try_resize(expansion_bytes + input.get_array_memory_size())
                            {
                                self.state = VarLengthStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }

                            if self.materializes_entity_structs() && !expansions.is_empty() {
                                let (vids, eids) = path_entities(&expansions);
                                let graph_ctx = self.exec.graph_ctx.clone();
                                let fut = Box::pin(async move {
                                    let query_ctx = graph_ctx.query_context();
                                    EntityPropertyCache::prefetch(
                                        &graph_ctx, &query_ctx, &vids, &eids,
                                    )
                                    .await
                                });
                                self.state = VarLengthStreamState::PrefetchingProperties {
                                    fut,
                                    input,
                                    expansions,
                                };
                                continue;
                            }

                            let base_batch =
                                match self.build_output_batch(&input, &expansions, None) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        self.state = VarLengthStreamState::Reading;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                };
                            if let Some(batch) = self.dispatch_base_batch(base_batch) {
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            // Continue loop to poll the hydration future
                        }
                    }
                }
                VarLengthStreamState::PrefetchingProperties {
                    mut fut,
                    input,
                    expansions,
                } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(cache)) => {
                        let base_batch =
                            match self.build_output_batch(&input, &expansions, Some(&cache)) {
                                Ok(b) => b,
                                Err(e) => {
                                    self.state = VarLengthStreamState::Reading;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };
                        if let Some(batch) = self.dispatch_base_batch(base_batch) {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        // Continue loop to poll the hydration future
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VarLengthStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VarLengthStreamState::PrefetchingProperties {
                            fut,
                            input,
                            expansions,
                        };
                        return Poll::Pending;
                    }
                },
                VarLengthStreamState::Materializing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = self.resume_state();
                        self.metrics.record_output(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VarLengthStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VarLengthStreamState::Materializing(fut);
                        return Poll::Pending;
                    }
                },
                VarLengthStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl GraphVariableLengthTraverseStream {
    /// Take up to `cap` more paths out of a paused row.
    ///
    /// Returns `true` when the row has no paths left, so the caller can drop
    /// the parked state. A `false` means the cap or the budget stopped it and
    /// the cursor is positioned exactly after the last path handed over --
    /// never before it, which would duplicate a row, and never after the next,
    /// which would drop one.
    fn drain_paused_row(
        &self,
        st: &mut PausedRowEnumeration,
        cap: usize,
        budget: &mut PathBudget,
        out: &mut Vec<VarLengthExpansion>,
    ) -> bool {
        loop {
            if out.len() >= cap || budget.tripped() {
                return false;
            }
            let (mut cursor, target, hops) = match st.cursor.take() {
                Some(open) => open,
                None => {
                    let Some(&(target, state, depth)) = st.accepting.get(st.accept_idx) else {
                        return true;
                    };
                    st.accept_idx += 1;
                    let cursor = PathEnumerator::new(
                        &st.dag,
                        st.source,
                        target,
                        state,
                        depth,
                        depth,
                        self.exec.path_mode.clone(),
                    );
                    (cursor, target, depth as usize)
                }
            };

            let mut stopped_short = false;
            // The stop reason is tracked by `stopped_short`: a `Break` from the
            // callback and a cursor that simply ran out mean different things
            // to the caller, and only the callback knows which happened.
            let _ = cursor.resume(&st.dag, &mut |nodes, edges| match budget.admit(
                nodes,
                edges,
                &self.exec.graph_ctx,
            ) {
                ControlFlow::Continue(()) => {
                    out.push((0, target, hops, nodes.to_vec(), edges.to_vec()));
                    if out.len() >= cap {
                        stopped_short = true;
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                }
                ControlFlow::Break(()) => {
                    stopped_short = true;
                    ControlFlow::Break(())
                }
            });

            if !cursor.is_finished() {
                st.cursor = Some((cursor, target, hops));
            }
            if stopped_short {
                return false;
            }
        }
    }

    /// Run the BFS for one input batch and return it alongside the expansions.
    ///
    /// Column materialization is deliberately *not* done here: entity structs
    /// need an async property pre-fetch keyed on the vids and eids this BFS
    /// discovers, so the caller decides whether to prefetch before calling
    /// [`Self::build_output_batch`].
    fn expand_batch(
        &self,
        batch: RecordBatch,
        eid_filters: &[EidFilter],
        vertex_filters: &[VidFilter],
        vid_filter: &VidFilter,
    ) -> DFResult<(
        RecordBatch,
        Vec<VarLengthExpansion>,
        Option<Box<PausedRowEnumeration>>,
    )> {
        let source_col = batch
            .column_by_name(&self.exec.source_column)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Source column '{}' not found",
                    self.exec.source_column
                ))
            })?;

        let source_vid_cow = column_as_vid_array(source_col.as_ref())?;
        let source_vids: &UInt64Array = &source_vid_cow;

        // Read bound target VIDs if column exists
        let bound_target_cow = self
            .exec
            .bound_target_column
            .as_ref()
            .and_then(|col| batch.column_by_name(col))
            .map(|c| column_as_vid_array(c.as_ref()))
            .transpose()?;
        let expected_targets: Option<&UInt64Array> = bound_target_cow.as_deref();

        // Extract used edge columns for cross-pattern relationship uniqueness
        let used_edge_arrays =
            super::common::used_edge_id_arrays(&batch, &self.exec.used_edge_columns)?;

        // Collect all BFS results. `budget` spans the whole batch: the limit is
        // on what this pass holds, not on any single source vertex, and one
        // source in a cyclic graph can exhaust it on its own.
        let mut expansions: Vec<VarLengthExpansion> = Vec::new();
        let mut paused: Option<Box<PausedRowEnumeration>> = None;
        let mut budget = PathBudget::new(&self.memory_pool);

        for (row_idx, source_vid) in source_vids.iter().enumerate() {
            let mut emitted_for_row = false;

            if let Some(src) = source_vid {
                let vid = Vid::from(src);

                // Collect used edge IDs from previous hops for this row
                let used_eids: FxHashSet<u64> = used_edge_arrays.for_row(row_idx).collect();

                // Dispatch to appropriate BFS mode based on output_mode
                match &self.exec.output_mode {
                    VlpOutputMode::Reachability => {
                        let endpoints = self.exec.bfs_endpoints_only(
                            vid,
                            eid_filters,
                            vertex_filters,
                            &used_eids,
                            vid_filter,
                        );
                        for (target, depth) in endpoints {
                            // Filter by bound target VID
                            if let Some(targets) = expected_targets {
                                if targets.is_null(row_idx) {
                                    continue;
                                }
                                if target.as_u64() != targets.value(row_idx) {
                                    continue;
                                }
                            }
                            expansions.push((row_idx, target, depth as usize, vec![], vec![]));
                            emitted_for_row = true;
                        }
                    }
                    // A lone source row whose endpoint is not already bound:
                    // the one shape where a pause is both useful and safe.
                    //
                    // Useful, because a chunk this narrow is what
                    // `retune_rows_per_chunk` converges to under a large
                    // fan-out, so it is exactly the exploding case. Safe,
                    // because nothing between here and the emitted batch drops
                    // a row -- with a bound endpoint the loop below filters
                    // *after* enumeration, so a pause could hand back fewer
                    // rows than the pass was asked for.
                    //
                    // Narrowing the accepting set by the bound endpoint instead
                    // would make the pause safe there too, and was written and
                    // then dropped: no query shape could be found that reaches
                    // it. `bound_target_column` is unset even for
                    // `MATCH (b {..}) WITH b MATCH p=(a)-[*]->(b)`, where the
                    // endpoint is enforced by a filter above this operator.
                    _ if batch.num_rows() == 1 => {
                        let (dag, mut accepting) = self.exec.build_path_dag(
                            vid,
                            eid_filters,
                            vertex_filters,
                            &used_eids,
                            vid_filter,
                        );
                        // A bound endpoint is applied to the *accepting set*
                        // rather than to the rows enumeration produces.
                        //
                        // Two things follow. The pause below becomes safe: the
                        // loop further down filters enumerated paths against
                        // the bound target, and pausing before that filter
                        // could hand back fewer rows than the pass was asked
                        // for -- narrowing first makes the filter a no-op, so
                        // there is nothing left to drop. And it is strictly
                        // less work, because paths to every other reachable
                        // endpoint are never walked at all.
                        if let Some(targets) = expected_targets {
                            if targets.is_null(row_idx) {
                                accepting.clear();
                            } else {
                                let wanted = targets.value(row_idx);
                                accepting.retain(|(target, _, _)| target.as_u64() == wanted);
                            }
                        }
                        let mut state = Box::new(PausedRowEnumeration {
                            chunk: batch.clone(),
                            source: vid,
                            dag,
                            accepting,
                            accept_idx: 0,
                            cursor: None,
                        });
                        let exhausted = self.drain_paused_row(
                            &mut state,
                            self.slice_size,
                            &mut budget,
                            &mut expansions,
                        );
                        if let Some(e) = budget.take_error() {
                            return Err(e);
                        }
                        emitted_for_row = !expansions.is_empty();
                        if !exhausted {
                            paused = Some(state);
                        }
                    }
                    _ => {
                        // FullPath, StepVariable, CountOnly, etc.
                        let bfs_results = self.exec.bfs_with_dag(
                            vid,
                            eid_filters,
                            vertex_filters,
                            &used_eids,
                            vid_filter,
                            &mut budget,
                        )?;
                        for (target, hop_count, node_path, edge_path) in bfs_results {
                            // Filter by bound target VID
                            if let Some(targets) = expected_targets {
                                if targets.is_null(row_idx) {
                                    continue;
                                }
                                if target.as_u64() != targets.value(row_idx) {
                                    continue;
                                }
                            }
                            expansions.push((row_idx, target, hop_count, node_path, edge_path));
                            emitted_for_row = true;
                        }
                    }
                }
            }

            if self.exec.is_optional && !emitted_for_row {
                // Preserve the source row with NULL optional bindings.
                // We use empty node/edge paths to mark unmatched rows.
                expansions.push((row_idx, Vid::from(u64::MAX), 0, vec![], vec![]));
            }
        }

        Ok((batch, expansions, paused))
    }

    /// Whether this batch materializes entity structs (path or step-variable
    /// columns), whose `properties` blobs need a storage-backed pre-fetch —
    /// the sync L0 chain alone loses every property once a vertex or edge has
    /// been flushed. Endpoint-only traversals build no such column and so keep
    /// the fully synchronous fast path.
    fn materializes_entity_structs(&self) -> bool {
        self.exec.step_variable.is_some()
            || self.exec.path_variable.is_some()
            || !self.exec.qpp_group_bindings.is_empty()
    }

    /// Where to go once a chunk's output has been handed downstream.
    ///
    /// Held in one place because expansion, property prefetch and target
    /// hydration all end by asking the same question, and answering it
    /// independently in each is how a half-consumed input batch gets dropped.
    fn resume_state(&self) -> VarLengthStreamState {
        if self.paused_row.is_some() || self.pending_rows.is_some() {
            VarLengthStreamState::RowChunking
        } else {
            VarLengthStreamState::Reading
        }
    }

    /// Decide what happens to a freshly built base batch.
    ///
    /// Returns `Some(batch)` to emit it immediately, or `None` having moved the
    /// stream into [`VarLengthStreamState::Materializing`] so the caller's loop
    /// polls the target-property hydration future.
    fn dispatch_base_batch(&mut self, base_batch: RecordBatch) -> Option<RecordBatch> {
        if self.exec.target_properties.is_empty() {
            self.state = self.resume_state();
            return Some(base_batch);
        }

        let fut = hydrate_vlp_target_properties(
            base_batch,
            self.schema.clone(),
            self.exec.target_variable.clone(),
            self.exec.target_properties.clone(),
            self.exec.target_label_name.clone(),
            self.exec.graph_ctx.clone(),
        );
        self.state = VarLengthStreamState::Materializing(Box::pin(fut));
        None
    }

    fn build_output_batch(
        &self,
        input: &RecordBatch,
        expansions: &[VarLengthExpansion],
        prop_cache: Option<&EntityPropertyCache>,
    ) -> DFResult<RecordBatch> {
        if expansions.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_rows = expansions.len();

        // Build index array
        let indices: Vec<u64> = expansions
            .iter()
            .map(|(idx, _, _, _, _)| *idx as u64)
            .collect();
        let indices_array = UInt64Array::from(indices);

        // Expand input columns
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col in input.columns() {
            let expanded = take(col.as_ref(), &indices_array, None)?;
            columns.push(expanded);
        }

        // Collect target VIDs and unmatched markers for use in multiple places.
        // Unmatched OPTIONAL rows use the sentinel VID (u64::MAX) — not empty paths,
        // because EndpointsOnly mode legitimately uses empty node/edge path vectors.
        let unmatched_rows: Vec<bool> = expansions
            .iter()
            .map(|(_, vid, _, _, _)| vid.as_u64() == u64::MAX)
            .collect();
        let target_vids: Vec<Option<u64>> = expansions
            .iter()
            .zip(unmatched_rows.iter())
            .map(
                |((_, vid, _, _, _), unmatched)| {
                    if *unmatched { None } else { Some(vid.as_u64()) }
                },
            )
            .collect();

        // Add target VID column (only if not already in input)
        let target_vid_name = format!("{}._vid", self.exec.target_variable);
        if input.schema().column_with_name(&target_vid_name).is_none() {
            columns.push(Arc::new(UInt64Array::from(target_vids.clone())));
        }

        // Add target ._labels column (only if not already in input). Resolve
        // via the L0 chain then the persisted VidLabelsIndex (shared with the
        // single-hop paths) so labels are not dropped for vertices that live
        // only in Lance storage after a flush or on a fork.
        let target_labels_name = format!("{}._labels", self.exec.target_variable);
        if input
            .schema()
            .column_with_name(&target_labels_name)
            .is_none()
        {
            let target_vid_opts: Vec<Option<Vid>> =
                target_vids.iter().map(|v| v.map(Vid::from)).collect();
            columns.push(build_target_labels_column_opt(
                &target_vid_opts,
                &self.exec.target_label_name,
                &self.exec.graph_ctx,
            ));
        }

        // Add null placeholder columns for target properties (hydrated async if needed, skip if already in input)
        for prop_name in &self.exec.target_properties {
            let full_prop_name = format!("{}.{}", self.exec.target_variable, prop_name);
            if input.schema().column_with_name(&full_prop_name).is_none() {
                let col_idx = columns.len();
                if col_idx < self.schema.fields().len() {
                    let field = self.schema.field(col_idx);
                    columns.push(arrow_array::new_null_array(field.data_type(), num_rows));
                }
            }
        }

        // Add hop count column
        let hop_counts: Vec<Option<u64>> = expansions
            .iter()
            .map(|(_, vid, hops, _, _)| (vid.as_u64() != u64::MAX).then_some(*hops as u64))
            .collect();
        columns.push(Arc::new(UInt64Array::from(hop_counts)));

        // Add step variable (edge list) column if bound
        if self.exec.step_variable.is_some() {
            let mut edges_builder = new_edge_list_builder();
            let query_ctx = self.exec.graph_ctx.query_context();
            let edge_ctx = EdgeAppendCtx {
                graph_ctx: &self.exec.graph_ctx,
                query_ctx: &query_ctx,
                edge_type_ids: &self.exec.edge_type_ids,
                prop_cache,
                fixed_type_name: None,
            };

            for (_, _, _, node_path, edge_path) in expansions {
                if node_path.is_empty() && edge_path.is_empty() {
                    // Null row for OPTIONAL MATCH unmatched
                    edges_builder.append_null();
                } else if edge_path.is_empty() {
                    // Zero-hop match: empty list
                    edges_builder.append(true);
                } else {
                    for (i, eid) in edge_path.iter().enumerate() {
                        append_traversed_edge(
                            edges_builder.values(),
                            &edge_ctx,
                            *eid,
                            node_path[i],
                            node_path[i + 1],
                        );
                    }
                    edges_builder.append(true);
                }
            }

            columns.push(Arc::new(edges_builder.finish()));
        }

        // Add path variable column if bound.
        // For named paths, we output a Path struct with nodes and relationships arrays.
        // If a path column already exists in input (from a prior BindFixedPath), extend it
        // rather than building from scratch.
        if let Some(path_var_name) = &self.exec.path_variable {
            let existing_path_col_idx = input
                .schema()
                .column_with_name(path_var_name)
                .map(|(idx, _)| idx);
            // Clone the Arc so we can read existing path without borrowing `columns`
            let existing_path_arc = existing_path_col_idx.map(|idx| columns[idx].clone());
            let existing_path = existing_path_arc
                .as_ref()
                .and_then(|arc| arc.as_any().downcast_ref::<arrow_array::StructArray>());

            let path_struct = build_vlp_path_column(
                expansions,
                existing_path,
                &self.exec.graph_ctx,
                &self.exec.edge_type_ids,
                None,
                prop_cache,
            )?;

            if let Some(idx) = existing_path_col_idx {
                columns[idx] = path_struct;
            } else {
                columns.push(path_struct);
            }
        }

        // Group variable columns, appended last and in binding order to match
        // `build_schema`. Two group columns of the same kind have identical
        // types, so a positional mismatch here would surface as a wrong answer
        // rather than an Arrow error — hence the name assertion per push.
        for binding in &self.exec.qpp_group_bindings {
            debug_assert_eq!(
                self.schema.field(columns.len()).name(),
                &binding.name,
                "group variable columns must be pushed in build_schema's order"
            );
            columns.push(self.build_group_column(binding, expansions, prop_cache)?);
        }
        debug_assert_eq!(columns.len(), self.schema.fields().len());

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(self.schema.clone(), columns).map_err(arrow_err)
    }

    /// Materialize one group variable: the strided slice of each matched path
    /// at this binding's offset, one element per quantifier iteration.
    fn build_group_column(
        &self,
        binding: &QppGroupBinding,
        expansions: &[VarLengthExpansion],
        prop_cache: Option<&EntityPropertyCache>,
    ) -> DFResult<ArrayRef> {
        let query_ctx = self.exec.graph_ctx.query_context();
        let edge_ctx = EdgeAppendCtx {
            graph_ctx: &self.exec.graph_ctx,
            query_ctx: &query_ctx,
            edge_type_ids: &self.exec.edge_type_ids,
            prop_cache,
            fixed_type_name: None,
        };
        let stride = self.exec.hops_per_iter.max(1);
        let mut builder = match binding.kind {
            QppGroupKind::Node => new_node_list_builder(),
            QppGroupKind::Edge => new_edge_list_builder(),
        };

        for (_, target, _, node_path, edge_path) in expansions {
            // OPTIONAL MATCH with no match: null, distinct from the empty list
            // a zero-iteration match produces.
            if target.as_u64() == u64::MAX {
                builder.append_null();
                continue;
            }

            let iterations = edge_path.len() / stride;
            for i in 0..iterations {
                let idx = binding.offset + i * stride;
                match binding.kind {
                    QppGroupKind::Node => {
                        let Some(vid) = node_path.get(idx) else {
                            continue;
                        };
                        append_node_to_struct_with(builder.values(), *vid, &query_ctx, prop_cache);
                    }
                    QppGroupKind::Edge => {
                        let Some(eid) = edge_path.get(idx) else {
                            continue;
                        };
                        append_traversed_edge(
                            builder.values(),
                            &edge_ctx,
                            *eid,
                            node_path[idx],
                            node_path[idx + 1],
                        );
                    }
                }
            }
            builder.append(true);
        }

        Ok(Arc::new(builder.finish()))
    }
}

impl RecordBatchStream for GraphVariableLengthTraverseStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Hydrate target vertex properties into a VLP batch.
///
/// The base batch already has null placeholder columns for target properties.
/// This function replaces them with actual property values fetched from storage.
async fn hydrate_vlp_target_properties(
    base_batch: RecordBatch,
    schema: SchemaRef,
    target_variable: String,
    target_properties: Vec<String>,
    target_label_name: Option<String>,
    graph_ctx: Arc<GraphExecutionContext>,
) -> DFResult<RecordBatch> {
    if base_batch.num_rows() == 0 || target_properties.is_empty() {
        return Ok(base_batch);
    }

    // Find the target VID column by exact name.
    // Schema layout: [input cols..., target._vid, target.prop1..., _hop_count, path?]
    //
    // IMPORTANT: When the target variable is already bound in the input (e.g., two MATCH
    // clauses referencing the same variable), there may be duplicate column names. We need
    // the LAST occurrence of target._vid, which is the one added by the VLP.
    let target_vid_col_name = format!("{}._vid", target_variable);
    let vid_col_idx = schema
        .fields()
        .iter()
        .enumerate()
        .rev()
        .find(|(_, f)| f.name() == &target_vid_col_name)
        .map(|(i, _)| i);

    let Some(vid_col_idx) = vid_col_idx else {
        return Ok(base_batch);
    };

    let vid_col = base_batch.column(vid_col_idx);
    let target_vid_cow = column_as_vid_array(vid_col.as_ref())?;
    let target_vid_array: &UInt64Array = &target_vid_cow;

    let target_vids: Vec<Vid> = target_vid_array
        .iter()
        // Preserve null rows by mapping them to a sentinel VID that never resolves
        // to stored properties. The output property columns remain NULL for these rows.
        .map(|v| Vid::from(v.unwrap_or(u64::MAX)))
        .collect();

    // Fetch properties from storage
    let mut property_columns: Vec<ArrayRef> = Vec::new();

    if let Some(ref label_name) = target_label_name {
        // Same split as `build_target_property_columns`: named properties are
        // read as columns, `_all_props` stays on the map path because a
        // whole-entity request has no narrower columnar form (#209).
        //
        // The null-row sentinel (`u64::MAX`) simply matches no stored row, so
        // the gather yields null for those rows — the behaviour this function
        // already depended on, now by construction rather than by a map miss.
        let wants_all = target_properties.iter().any(|p| p == "_all_props");
        if !wants_all {
            property_columns = crate::query::df_graph::scan::hydrate_vids_columnar(
                graph_ctx.as_ref(),
                label_name,
                // Internal to that call: only the property columns come back,
                // and this function positions them itself.
                "t",
                &target_properties,
                &target_vids,
            )
            .await?;
        } else {
            let property_manager = graph_ctx.property_manager();
            let query_ctx = graph_ctx.query_context();

            let props_map = property_manager
                .get_batch_vertex_props_for_label(&target_vids, label_name, Some(&query_ctx))
                .await
                .map_err(exec_err)?;

            let uni_schema = graph_ctx.storage().schema_manager().schema();
            let label_props = uni_schema.properties.get(label_name.as_str());

            for prop_name in &target_properties {
                let data_type = resolve_property_type(prop_name, label_props);
                let column =
                    build_property_column_static(&target_vids, &props_map, prop_name, &data_type)
                        .map_err(exec_err)?;
                property_columns.push(column);
            }
        }
    } else {
        // No label name — use label-agnostic property lookup.
        // This scans all label datasets, slower but correct for label-less traversals.
        //
        // Same wildcard handling as `build_target_property_columns`: stripping
        // `_all_props` left nothing to request, so a variable-length path to an
        // unlabelled target hydrated with no properties at all. See that
        // function for why an unlabelled target is common rather than exotic.
        let wants_all = target_properties.iter().any(|p| p == "_all_props");
        let requested: Vec<&str> = if wants_all {
            vec!["_all_props"]
        } else {
            target_properties.iter().map(|s| s.as_str()).collect()
        };
        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        let props_map = if !requested.is_empty() {
            property_manager
                .get_batch_vertex_props(&target_vids, &requested, Some(&query_ctx))
                .await
                .map_err(exec_err)?
        } else {
            std::collections::HashMap::new()
        };

        for prop_name in &target_properties {
            if prop_name == "_all_props" {
                // Build CypherValue blob from all vertex properties (L0 + storage),
                // staying in `Value` space so typed values (temporals) are preserved.
                use arrow_array::builder::LargeBinaryBuilder;

                let mut builder = LargeBinaryBuilder::new();
                let l0_ctx = graph_ctx.l0_context();
                for vid in &target_vids {
                    let mut merged_props: HashMap<String, uni_common::Value> = HashMap::new();
                    // Collect from storage-hydrated props
                    if let Some(vid_props) = props_map.get(vid) {
                        for (k, v) in vid_props.iter() {
                            merged_props.insert(k.clone(), v.clone());
                        }
                    }
                    // Overlay L0 properties
                    for l0 in l0_ctx.iter_l0_buffers() {
                        let guard = l0.read();
                        if let Some(l0_props) = guard.vertex_properties.get(vid) {
                            for (k, v) in l0_props.iter() {
                                merged_props.insert(k.clone(), v.clone());
                            }
                        }
                    }
                    if merged_props.is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(uni_common::cypher_value_codec::encode(
                            &uni_common::Value::Map(merged_props),
                        ));
                    }
                }
                property_columns.push(Arc::new(builder.finish()));
            } else {
                let column = build_property_column_static(
                    &target_vids,
                    &props_map,
                    prop_name,
                    &arrow::datatypes::DataType::LargeBinary,
                )
                .map_err(exec_err)?;
                property_columns.push(column);
            }
        }
    }

    // Rebuild batch replacing the null placeholder property columns with hydrated ones.
    // Find each property column by name — works regardless of column ordering
    // (schema-aware puts props before _hop_count; schemaless puts them after).
    // Use col_idx > vid_col_idx to only replace this VLP's own property columns,
    // not pre-existing input columns with the same name (duplicate variable binding).
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut prop_idx = 0;
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let is_target_prop = col_idx > vid_col_idx
            && target_properties
                .iter()
                .any(|p| *field.name() == format!("{}.{}", target_variable, p));
        if is_target_prop && prop_idx < property_columns.len() {
            new_columns.push(property_columns[prop_idx].clone());
            prop_idx += 1;
        } else {
            new_columns.push(base_batch.column(col_idx).clone());
        }
    }

    RecordBatch::try_new(schema, new_columns).map_err(arrow_err)
}

// ============================================================================
// GraphVariableLengthTraverseMainExec - VLP for schemaless edge types
// ============================================================================

/// Execution plan for variable-length path traversal on schemaless edge types.
///
/// This is similar to `GraphVariableLengthTraverseExec` but works with edge types
/// that don't have schema-defined IDs. It queries the main edges table by type name.
/// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
pub struct GraphVariableLengthTraverseMainExec {
    /// Input execution plan.
    input: Arc<dyn ExecutionPlan>,

    /// Column name containing source VIDs.
    source_column: String,

    /// Edge type names (not IDs, since schemaless types may not have IDs).
    type_names: Vec<String>,

    /// Traversal direction.
    direction: Direction,

    /// Minimum number of hops.
    min_hops: usize,

    /// Maximum number of hops.
    max_hops: usize,

    /// Variable name for target vertex columns.
    target_variable: String,

    /// Variable name for relationship list (r in `[r*]`) - holds `List<Edge>`.
    step_variable: Option<String>,

    /// Variable name for named path (p in `p = ...`) - holds `Path`.
    path_variable: Option<String>,

    /// Target vertex properties to materialize.
    target_properties: Vec<String>,

    /// Whether this is an optional match (LEFT JOIN semantics).
    is_optional: bool,

    /// Column name of an already-bound target VID (for patterns where target is in scope).
    bound_target_column: Option<String>,

    /// Edge property conditions to check during BFS (e.g., `{year: 1988}`).
    /// Each entry is (property_name, expected_value). All must match for an edge to be traversed.
    edge_property_conditions: Vec<(String, UniValue)>,

    /// Edge ID columns from previous hops for cross-pattern relationship uniqueness.
    used_edge_columns: Vec<String>,

    /// Path semantics mode (Trail = no repeated edges, default for OpenCypher).
    path_mode: super::nfa::PathMode,

    /// Output mode determining BFS strategy.
    output_mode: super::nfa::VlpOutputMode,

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphVariableLengthTraverseMainExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphVariableLengthTraverseMainExec")
            .field("source_column", &self.source_column)
            .field("type_names", &self.type_names)
            .field("direction", &self.direction)
            .field("min_hops", &self.min_hops)
            .field("max_hops", &self.max_hops)
            .field("target_variable", &self.target_variable)
            .finish()
    }
}

impl GraphVariableLengthTraverseMainExec {
    /// Create a new variable-length traversal plan for schemaless edges.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        type_names: Vec<String>,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
        target_variable: impl Into<String>,
        step_variable: Option<String>,
        path_variable: Option<String>,
        target_properties: Vec<String>,
        graph_ctx: Arc<GraphExecutionContext>,
        is_optional: bool,
        bound_target_column: Option<String>,
        edge_property_conditions: Vec<(String, UniValue)>,
        used_edge_columns: Vec<String>,
        path_mode: super::nfa::PathMode,
        output_mode: super::nfa::VlpOutputMode,
    ) -> Self {
        let source_column = source_column.into();
        let target_variable = target_variable.into();

        // Build output schema
        let schema = Self::build_schema(
            input.schema(),
            &target_variable,
            step_variable.as_deref(),
            path_variable.as_deref(),
            &target_properties,
        );
        let properties = compute_plan_properties(schema.clone());

        Self {
            input,
            source_column,
            type_names,
            direction,
            min_hops,
            max_hops,
            target_variable,
            step_variable,
            path_variable,
            target_properties,
            is_optional,
            bound_target_column,
            edge_property_conditions,
            used_edge_columns,
            path_mode,
            output_mode,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema.
    fn build_schema(
        input_schema: SchemaRef,
        target_variable: &str,
        step_variable: Option<&str>,
        path_variable: Option<&str>,
        target_properties: &[String],
    ) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add target VID column (only if not already in input)
        let target_vid_name = format!("{}._vid", target_variable);
        if input_schema.column_with_name(&target_vid_name).is_none() {
            fields.push(Field::new(target_vid_name, DataType::UInt64, true));
        }

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", target_variable);
        if input_schema.column_with_name(&target_labels_name).is_none() {
            fields.push(Field::new(target_labels_name, labels_data_type(), true));
        }

        // Add hop count
        // Nullable: an OPTIONAL carrier row (no path matched) has no hop
        // count, and a NULL here is what lets the clause's closing
        // `OptionalFilterExec` tell it from a zero-hop match when the
        // traversal introduces no other identity column (an anonymous
        // relationship into an already-bound node).
        fields.push(Field::new("_hop_count", DataType::UInt64, true));

        // Add step variable column (list of edge structs) if bound
        // This is the relationship variable like `r` in `[r*1..3]`
        if let Some(step_var) = step_variable {
            fields.push(build_edge_list_field(step_var));
        }

        // Add path struct if bound (only if not already in input from prior BindFixedPath)
        if let Some(path_var) = path_variable
            && input_schema.column_with_name(path_var).is_none()
        {
            fields.push(build_path_struct_field(path_var));
        }

        // Add target property columns (as LargeBinary for lazy hydration via PropertyManager)
        // Skip properties that are already in the input schema
        for prop in target_properties {
            let prop_name = format!("{}.{}", target_variable, prop);
            if input_schema.column_with_name(&prop_name).is_none() {
                fields.push(Field::new(prop_name, DataType::LargeBinary, true));
            }
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphVariableLengthTraverseMainExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphVariableLengthTraverseMainExec: {} --[{:?}*{}..{}]--> target",
            self.source_column, self.type_names, self.min_hops, self.max_hops
        )
    }
}

impl ExecutionPlan for GraphVariableLengthTraverseMainExec {
    fn name(&self) -> &str {
        "GraphVariableLengthTraverseMainExec"
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
                "GraphVariableLengthTraverseMainExec requires exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.source_column.clone(),
            self.type_names.clone(),
            self.direction,
            self.min_hops,
            self.max_hops,
            self.target_variable.clone(),
            self.step_variable.clone(),
            self.path_variable.clone(),
            self.target_properties.clone(),
            self.graph_ctx.clone(),
            self.is_optional,
            self.bound_target_column.clone(),
            self.edge_property_conditions.clone(),
            self.used_edge_columns.clone(),
            self.path_mode.clone(),
            self.output_mode.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let reservation =
            MemoryConsumer::new(format!("GraphVariableLengthTraverseMainExec[{partition}]"))
                .register(context.memory_pool());
        // Read before `context` is moved into the input's `execute`.
        let slice_size = context.session_config().batch_size().max(1);
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Build adjacency map from main edges table (async)
        let graph_ctx = self.graph_ctx.clone();
        let type_names = self.type_names.clone();
        let direction = self.direction;
        let load_fut = async move {
            // VLP frontier is unknown ahead of time: no source pushdown (whole-type).
            build_edge_adjacency_map(&graph_ctx, &type_names, direction, None).await
        };

        Ok(Box::pin(GraphVariableLengthTraverseMainStream {
            slice_size,
            // One row, for the reason given on the schema'd twin: retuning only
            // reacts to a pass that already happened, so a wide first chunk is
            // unbounded by construction.
            rows_per_chunk: 1,
            pending_rows: None,
            reservation,
            input: input_stream,
            source_column: self.source_column.clone(),
            type_names: self.type_names.clone(),
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            target_variable: self.target_variable.clone(),
            step_variable: self.step_variable.clone(),
            path_variable: self.path_variable.clone(),
            target_properties: self.target_properties.clone(),
            graph_ctx: self.graph_ctx.clone(),
            is_optional: self.is_optional,
            bound_target_column: self.bound_target_column.clone(),
            edge_property_conditions: self.edge_property_conditions.clone(),
            used_edge_columns: self.used_edge_columns.clone(),
            path_mode: self.path_mode.clone(),
            output_mode: self.output_mode.clone(),
            schema: self.schema.clone(),
            state: VarLengthMainStreamState::Loading(Box::pin(load_fut)),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// State machine for VLP schemaless stream.
enum VarLengthMainStreamState {
    /// Loading adjacency map from main edges table.
    Loading(Pin<Box<dyn std::future::Future<Output = DFResult<EdgeAdjacencyMap>> + Send>>),
    /// Processing input batches with loaded adjacency.
    Processing(EdgeAdjacencyMap),
    /// Fetching storage-backed properties for this batch's path entities before
    /// the synchronous column builders run. Mirrors the schema'd exec's state
    /// of the same purpose.
    PrefetchingProperties {
        adjacency: EdgeAdjacencyMap,
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<EntityPropertyCache>> + Send>>,
        input: RecordBatch,
        expansions: Vec<ExpansionRecord>,
    },
    /// Materializing properties for a batch.
    Materializing {
        adjacency: EdgeAdjacencyMap,
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>,
    },
    /// Expanding one row-chunk of the input batch held in `pending_rows`.
    ///
    /// The schemaless twin of [`VarLengthStreamState::RowChunking`], and for
    /// the same reason: the expansion set is the peak and it exists in full
    /// before materialization begins, so only chunking the *input rows* bounds
    /// construction (#241). Carries the adjacency because this stream keeps it
    /// in the state rather than on the stream.
    RowChunking(EdgeAdjacencyMap),
    /// Stream is done.
    Done,
}

/// Stream for variable-length traversal on schemaless edges.
struct GraphVariableLengthTraverseMainStream {
    /// Target rows per output batch, and the expansion budget per pass.
    slice_size: usize,
    /// Input rows expanded per pass, retuned from the fan-out actually seen.
    /// See [`retune_rows_per_chunk`].
    rows_per_chunk: usize,
    /// The input batch being consumed a row-chunk at a time, and where to
    /// resume in it.
    pending_rows: Option<(RecordBatch, usize)>,
    input: SendableRecordBatchStream,
    source_column: String,
    type_names: Vec<String>,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
    target_variable: String,
    /// Relationship variable like `r` in `[r*1..3]` - gets a List of edge structs.
    step_variable: Option<String>,
    path_variable: Option<String>,
    target_properties: Vec<String>,
    graph_ctx: Arc<GraphExecutionContext>,
    is_optional: bool,
    bound_target_column: Option<String>,
    /// Edge property conditions to check during BFS.
    edge_property_conditions: Vec<(String, UniValue)>,
    used_edge_columns: Vec<String>,
    path_mode: super::nfa::PathMode,
    output_mode: super::nfa::VlpOutputMode,
    schema: SchemaRef,
    state: VarLengthMainStreamState,
    metrics: BaselineMetrics,

    /// The query pool's accounting for the whole-type adjacency map.
    ///
    /// A variable-length frontier is not known ahead of time, so this operator
    /// loads the edge type's adjacency with **no source pushdown at all** and
    /// holds it for its whole life. That makes it the clearest case in the file
    /// of an allocation bounded by the data rather than by the query, and it was
    /// entirely invisible to the pool.
    reservation: MemoryReservation,
}

/// BFS result type: (target_vid, hop_count, node_path, edge_path)
type MainBfsResult = (Vid, usize, Vec<Vid>, Vec<Eid>);

impl GraphVariableLengthTraverseMainStream {
    /// Perform BFS from a source vertex using the adjacency map.
    ///
    /// `used_eids` contains edge IDs already bound by earlier pattern elements
    /// in the same MATCH clause, enforcing cross-pattern relationship uniqueness
    /// (Cypher semantics require all relationships in a MATCH to be distinct).
    fn bfs(
        &self,
        source: Vid,
        adjacency: &EdgeAdjacencyMap,
        used_eids: &FxHashSet<u64>,
    ) -> Vec<MainBfsResult> {
        let mut results = Vec::new();
        let mut queue: VecDeque<MainBfsResult> = VecDeque::new();

        queue.push_back((source, 0, vec![source], vec![]));

        while let Some((current, depth, node_path, edge_path)) = queue.pop_front() {
            // Emit result if within hop range (including zero-length patterns)
            if depth >= self.min_hops && depth <= self.max_hops {
                results.push((current, depth, node_path.clone(), edge_path.clone()));
            }

            // Stop if at max depth
            if depth >= self.max_hops {
                continue;
            }

            // Get neighbors from adjacency map
            if let Some(neighbors) = adjacency.get(&current) {
                let is_undirected = matches!(self.direction, Direction::Both);
                let mut seen_edges_at_hop: HashSet<u64> = HashSet::new();

                for (neighbor, eid, _edge_type, props, _fwd) in neighbors {
                    // Deduplicate edges for undirected patterns
                    if is_undirected && !seen_edges_at_hop.insert(eid.as_u64()) {
                        continue;
                    }

                    // Enforce relationship uniqueness per-path (Cypher semantics).
                    if edge_path.contains(eid) {
                        continue;
                    }

                    // Enforce cross-pattern relationship uniqueness: skip edges
                    // already bound by earlier pattern elements in the same MATCH.
                    if used_eids.contains(&eid.as_u64()) {
                        continue;
                    }

                    // Check edge property conditions (e.g., {year: 1988}) under
                    // Cypher equality, as every other walk does: structural
                    // `Value` equality rejected `{w: 0}` on `w = 0.0` and
                    // matched NaN with NaN. Nothing re-checks this.
                    if !map_matches(props, &self.edge_property_conditions) {
                        continue;
                    }

                    let mut new_node_path = node_path.clone();
                    new_node_path.push(*neighbor);
                    let mut new_edge_path = edge_path.clone();
                    new_edge_path.push(*eid);
                    queue.push_back((*neighbor, depth + 1, new_node_path, new_edge_path));
                }
            }
        }

        results
    }

    /// Each endpoint a trail reaches, once, for a reachability consumer;
    /// `None` when the output mode needs paths or shortest distances cannot
    /// decide (see `trail_reach`), and [`Self::bfs`] must list the trails.
    ///
    /// `bfs` enumerates every trail whatever the output mode, which on a
    /// cyclic graph is exponential in the edge count; an undirected pattern
    /// is cyclic as soon as it has one edge.
    fn reachable_endpoints(
        &self,
        source: Vid,
        adjacency: &EdgeAdjacencyMap,
        used_eids: &FxHashSet<u64>,
    ) -> Option<Vec<MainBfsResult>> {
        if !matches!(self.output_mode, super::nfa::VlpOutputMode::Reachability)
            || self.path_mode != super::nfa::PathMode::Trail
        {
            return None;
        }
        let is_undirected = matches!(self.direction, Direction::Both);
        // The same hop `bfs` takes: each relationship once, none bound
        // elsewhere in the pattern, and only those the property map admits.
        let neighbors = |v: Vid| -> Vec<(Vid, Eid)> {
            let mut seen: FxHashSet<u64> = FxHashSet::default();
            adjacency
                .get(&v)
                .into_iter()
                .flatten()
                .filter(|(_, eid, _, props, _)| {
                    (!is_undirected || seen.insert(eid.as_u64()))
                        && !used_eids.contains(&eid.as_u64())
                        && map_matches(props, &self.edge_property_conditions)
                })
                .map(|(w, eid, _, _, _)| (*w, *eid))
                .collect()
        };
        let found = super::trail_reach::trail_endpoints(
            source,
            self.min_hops,
            self.max_hops,
            neighbors,
            |_| true,
        )?;
        let mut results = Vec::with_capacity(found.endpoints.len() + 1);
        // A non-empty node path marks a match; an empty one marks the NULL
        // row of an optional pattern (see `build_output_batch`).
        if self.min_hops == 0 {
            results.push((source, 0, vec![source], vec![]));
        }
        results.extend(
            found
                .endpoints
                .into_iter()
                .map(|(v, d)| (v, d as usize, vec![source, v], vec![])),
        );
        Some(results)
    }

    /// Run the BFS for one input batch and return it alongside the expansions.
    ///
    /// Split from column materialization so entity-struct columns can have
    /// their properties pre-fetched from storage first — see
    /// [`EntityPropertyCache`].
    fn expand_batch(
        &self,
        batch: RecordBatch,
        adjacency: &EdgeAdjacencyMap,
    ) -> DFResult<(RecordBatch, Vec<ExpansionRecord>)> {
        let source_col = batch.column_by_name(&self.source_column).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Source column '{}' not found in input batch",
                self.source_column
            ))
        })?;

        let source_vid_cow = column_as_vid_array(source_col.as_ref())?;
        let source_vids: &UInt64Array = &source_vid_cow;

        // Read bound target VIDs if column exists
        let bound_target_cow = self
            .bound_target_column
            .as_ref()
            .and_then(|col| batch.column_by_name(col))
            .map(|c| column_as_vid_array(c.as_ref()))
            .transpose()?;
        let expected_targets: Option<&UInt64Array> = bound_target_cow.as_deref();

        // Extract used edge columns for cross-pattern relationship uniqueness
        let used_edge_arrays = super::common::used_edge_id_arrays(&batch, &self.used_edge_columns)?;

        // Collect BFS results: (original_row_idx, target_vid, hop_count, node_path, edge_path)
        let mut expansions: Vec<ExpansionRecord> = Vec::new();

        for (row_idx, source_opt) in source_vids.iter().enumerate() {
            let mut emitted_for_row = false;

            if let Some(source_u64) = source_opt {
                let source = Vid::from(source_u64);

                // Collect used edge IDs from previous hops for this row
                let used_eids: FxHashSet<u64> = used_edge_arrays.for_row(row_idx).collect();

                let bfs_results = self
                    .reachable_endpoints(source, adjacency, &used_eids)
                    .unwrap_or_else(|| self.bfs(source, adjacency, &used_eids));

                for (target, hops, node_path, edge_path) in bfs_results {
                    // Filter by bound target VID if set (for patterns where target is in scope).
                    // NULL bound targets do not match anything.
                    if let Some(targets) = expected_targets {
                        if targets.is_null(row_idx) {
                            continue;
                        }
                        let expected_vid = targets.value(row_idx);
                        if target.as_u64() != expected_vid {
                            continue;
                        }
                    }

                    expansions.push((row_idx, target, hops, node_path, edge_path));
                    emitted_for_row = true;
                }
            }

            if self.is_optional && !emitted_for_row {
                // Preserve source row with NULL optional bindings.
                expansions.push((row_idx, Vid::from(u64::MAX), 0, vec![], vec![]));
            }
        }

        Ok((batch, expansions))
    }

    /// Whether this batch materializes entity structs whose `properties` blobs
    /// need a storage-backed pre-fetch. See the schema'd exec's twin.
    fn materializes_entity_structs(&self) -> bool {
        self.step_variable.is_some() || self.path_variable.is_some()
    }

    /// Emit the base batch directly, or move into target-property hydration.
    /// See the schema'd exec's twin.
    /// Where to go once a chunk's output has been handed downstream.
    ///
    /// Same role as the schema'd twin's `resume_state`; carries the adjacency
    /// because this stream keeps it in the state.
    fn resume_state(&self, adjacency: EdgeAdjacencyMap) -> VarLengthMainStreamState {
        if self.pending_rows.is_some() {
            VarLengthMainStreamState::RowChunking(adjacency)
        } else {
            VarLengthMainStreamState::Processing(adjacency)
        }
    }

    fn dispatch_base_batch(
        &mut self,
        adjacency: EdgeAdjacencyMap,
        base_batch: RecordBatch,
    ) -> Option<RecordBatch> {
        if self.target_properties.is_empty() {
            self.state = self.resume_state(adjacency);
            return Some(base_batch);
        }

        let fut = hydrate_vlp_target_properties(
            base_batch,
            self.schema.clone(),
            self.target_variable.clone(),
            self.target_properties.clone(),
            None, // schemaless — no label name
            self.graph_ctx.clone(),
        );
        self.state = VarLengthMainStreamState::Materializing {
            adjacency,
            fut: Box::pin(fut),
        };
        None
    }

    fn build_output_batch(
        &self,
        batch: &RecordBatch,
        expansions: &[ExpansionRecord],
        prop_cache: Option<&EntityPropertyCache>,
    ) -> DFResult<RecordBatch> {
        if expansions.is_empty() {
            if self.is_optional {
                let all_indices: Vec<usize> = (0..batch.num_rows()).collect();
                return build_optional_null_batch_for_rows(batch, &all_indices, &self.schema);
            }
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_rows = expansions.len();
        self.metrics.record_output(num_rows);

        // Build output columns
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

        // Resolve this operator's edge type names to numeric ids once, so path
        // builders can recover the STORED orientation of flushed (L1-resident)
        // relationships via a directed-outgoing adjacency probe.
        let edge_type_ids: Vec<u32> = {
            let uni_schema = self.graph_ctx.storage().schema_manager().schema();
            self.type_names
                .iter()
                .filter_map(|name| uni_schema.edge_type_id_by_name(name))
                .collect()
        };

        // Expand input columns
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            let indices: Vec<u64> = expansions
                .iter()
                .map(|(idx, _, _, _, _)| *idx as u64)
                .collect();
            let take_indices = UInt64Array::from(indices);
            let expanded = arrow::compute::take(array, &take_indices, None)?;
            columns.push(expanded);
        }

        // Add target VID column (only if not already in input)
        let target_vid_name = format!("{}._vid", self.target_variable);
        if batch.schema().column_with_name(&target_vid_name).is_none() {
            let target_vids: Vec<Option<u64>> = expansions
                .iter()
                .map(|(_, vid, _, node_path, edge_path)| {
                    if node_path.is_empty() && edge_path.is_empty() {
                        None
                    } else {
                        Some(vid.as_u64())
                    }
                })
                .collect();
            columns.push(Arc::new(UInt64Array::from(target_vids)));
        }

        // Add target ._labels column (only if not already in input). Resolve
        // via the L0 chain then the persisted VidLabelsIndex (shared with the
        // single-hop paths) so labels are not dropped for vertices that live
        // only in Lance storage after a flush or on a fork. This schemaless
        // path has no scoping label, so a vertex absent from both yields no
        // labels rather than a fallback.
        let target_labels_name = format!("{}._labels", self.target_variable);
        if batch
            .schema()
            .column_with_name(&target_labels_name)
            .is_none()
        {
            let target_vid_opts: Vec<Option<Vid>> = expansions
                .iter()
                .map(|(_, vid, _, node_path, edge_path)| {
                    if node_path.is_empty() && edge_path.is_empty() {
                        None
                    } else {
                        Some(*vid)
                    }
                })
                .collect();
            columns.push(build_target_labels_column_opt(
                &target_vid_opts,
                &None,
                &self.graph_ctx,
            ));
        }

        // Add hop count column
        let hop_counts: Vec<Option<u64>> = expansions
            .iter()
            .map(|(_, vid, hops, _, _)| (vid.as_u64() != u64::MAX).then_some(*hops as u64))
            .collect();
        columns.push(Arc::new(UInt64Array::from(hop_counts)));

        // Add step variable column if bound (list of edge structs).
        if self.step_variable.is_some() {
            let mut edges_builder = new_edge_list_builder();
            let query_ctx = self.graph_ctx.query_context();
            // Only a fallback now: the pattern's type list cannot say which
            // type each individual edge is, so `[:A|B]` used to label every
            // edge "A|B". The probe resolves it per edge.
            let type_names_str = self.type_names.join("|");
            let edge_ctx = EdgeAppendCtx {
                graph_ctx: &self.graph_ctx,
                query_ctx: &query_ctx,
                edge_type_ids: &edge_type_ids,
                prop_cache,
                fixed_type_name: Some(&type_names_str),
            };

            for (_, _, _, node_path, edge_path) in expansions.iter() {
                if node_path.is_empty() && edge_path.is_empty() {
                    edges_builder.append_null();
                } else if edge_path.is_empty() {
                    // Zero-hop match: empty list.
                    edges_builder.append(true);
                } else {
                    for (i, eid) in edge_path.iter().enumerate() {
                        append_traversed_edge(
                            edges_builder.values(),
                            &edge_ctx,
                            *eid,
                            node_path[i],
                            node_path[i + 1],
                        );
                    }
                    edges_builder.append(true);
                }
            }

            columns.push(Arc::new(edges_builder.finish()) as ArrayRef);
        }

        // Add path variable column if bound.
        // If a path column already exists in input (from a prior BindFixedPath), extend it
        // rather than building from scratch.
        if let Some(path_var_name) = &self.path_variable {
            let existing_path_col_idx = batch
                .schema()
                .column_with_name(path_var_name)
                .map(|(idx, _)| idx);
            let existing_path_arc = existing_path_col_idx.map(|idx| columns[idx].clone());
            let existing_path = existing_path_arc
                .as_ref()
                .and_then(|arc| arc.as_any().downcast_ref::<arrow_array::StructArray>());

            let type_names_str = self.type_names.join("|");
            let path_struct = build_vlp_path_column(
                expansions,
                existing_path,
                &self.graph_ctx,
                &edge_type_ids,
                Some(&type_names_str),
                prop_cache,
            )?;

            if let Some(idx) = existing_path_col_idx {
                columns[idx] = path_struct;
            } else {
                columns.push(path_struct);
            }
        }

        // Add target property columns as NULL for now (skip if already in input).
        // Property hydration happens via PropertyManager in the query execution pipeline.
        for prop_name in &self.target_properties {
            let full_prop_name = format!("{}.{}", self.target_variable, prop_name);
            if batch.schema().column_with_name(&full_prop_name).is_none() {
                columns.push(arrow_array::new_null_array(
                    &DataType::LargeBinary,
                    num_rows,
                ));
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns).map_err(arrow_err)
    }
}

impl Stream for GraphVariableLengthTraverseMainStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            let state = std::mem::replace(&mut self.state, VarLengthMainStreamState::Done);

            match state {
                VarLengthMainStreamState::Loading(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(adjacency)) => {
                        if let Err(e) = self
                            .reservation
                            .try_resize(estimate_adjacency_bytes(&adjacency))
                        {
                            self.state = VarLengthMainStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        self.state = VarLengthMainStreamState::Processing(adjacency);
                        // Continue loop to start processing
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VarLengthMainStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VarLengthMainStreamState::Loading(fut);
                        return Poll::Pending;
                    }
                },
                VarLengthMainStreamState::Processing(adjacency) => {
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            // Expansion happens a row-chunk at a time (#241).
                            self.pending_rows = Some((batch, 0));
                            self.state = VarLengthMainStreamState::RowChunking(adjacency);
                            continue;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.state = VarLengthMainStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            self.state = VarLengthMainStreamState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            self.state = VarLengthMainStreamState::Processing(adjacency);
                            return Poll::Pending;
                        }
                    }
                }
                VarLengthMainStreamState::RowChunking(adjacency) => {
                    let Some((held, offset)) = self.pending_rows.take() else {
                        self.state = VarLengthMainStreamState::Processing(adjacency);
                        continue;
                    };
                    if offset >= held.num_rows() {
                        self.state = VarLengthMainStreamState::Processing(adjacency);
                        continue;
                    }
                    let take_rows = self.rows_per_chunk.min(held.num_rows() - offset);
                    let chunk = held.slice(offset, take_rows);
                    self.pending_rows = Some((held, offset + take_rows));

                    {
                        {
                            let (input, expansions) = match self.expand_batch(chunk, &adjacency) {
                                Ok(pair) => pair,
                                Err(e) => {
                                    self.state = VarLengthMainStreamState::Processing(adjacency);
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };

                            self.rows_per_chunk =
                                retune_rows_per_chunk(take_rows, expansions.len(), self.slice_size);

                            // The expansion set was invisible to the pool here:
                            // this stream reserved only its adjacency map, so
                            // its dominant allocation — a node path and an edge
                            // path per enumerated path — was never accounted at
                            // all. Measured before this: 106 MB of expansions
                            // passed an 8 MB ceiling without the pool noticing.
                            const EXPANSION: usize = std::mem::size_of::<ExpansionRecord>();
                            let expansion_bytes: usize = expansions
                                .iter()
                                .map(|(_, _, _, nodes, edges)| {
                                    EXPANSION
                                        + nodes.capacity() * std::mem::size_of::<Vid>()
                                        + edges.capacity() * std::mem::size_of::<Eid>()
                                })
                                .sum();
                            if let Err(e) = self.reservation.try_resize(
                                expansion_bytes
                                    + estimate_adjacency_bytes(&adjacency)
                                    + input.get_array_memory_size(),
                            ) {
                                self.state = VarLengthMainStreamState::Done;
                                return Poll::Ready(Some(Err(e)));
                            }

                            if self.materializes_entity_structs() && !expansions.is_empty() {
                                let (vids, eids) = path_entities(&expansions);
                                let graph_ctx = self.graph_ctx.clone();
                                let fut = Box::pin(async move {
                                    let query_ctx = graph_ctx.query_context();
                                    EntityPropertyCache::prefetch(
                                        &graph_ctx, &query_ctx, &vids, &eids,
                                    )
                                    .await
                                });
                                self.state = VarLengthMainStreamState::PrefetchingProperties {
                                    adjacency,
                                    fut,
                                    input,
                                    expansions,
                                };
                                continue;
                            }

                            let base_batch =
                                match self.build_output_batch(&input, &expansions, None) {
                                    Ok(b) => b,
                                    Err(e) => {
                                        self.state =
                                            VarLengthMainStreamState::Processing(adjacency);
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                };
                            if let Some(batch) = self.dispatch_base_batch(adjacency, base_batch) {
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            // Continue loop to poll the hydration future
                        }
                    }
                }
                VarLengthMainStreamState::PrefetchingProperties {
                    adjacency,
                    mut fut,
                    input,
                    expansions,
                } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(cache)) => {
                        let base_batch =
                            match self.build_output_batch(&input, &expansions, Some(&cache)) {
                                Ok(b) => b,
                                Err(e) => {
                                    self.state = VarLengthMainStreamState::Processing(adjacency);
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };
                        if let Some(batch) = self.dispatch_base_batch(adjacency, base_batch) {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        // Continue loop to poll the hydration future
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = VarLengthMainStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = VarLengthMainStreamState::PrefetchingProperties {
                            adjacency,
                            fut,
                            input,
                            expansions,
                        };
                        return Poll::Pending;
                    }
                },
                VarLengthMainStreamState::Materializing { adjacency, mut fut } => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok(batch)) => {
                            self.state = self.resume_state(adjacency);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(Err(e)) => {
                            self.state = VarLengthMainStreamState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => {
                            self.state = VarLengthMainStreamState::Materializing { adjacency, fut };
                            return Poll::Pending;
                        }
                    }
                }
                VarLengthMainStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for GraphVariableLengthTraverseMainStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traverse_schema_without_edge() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let output_schema =
            GraphTraverseExec::build_schema(input_schema, "m", None, &[], &[], None, None, false);

        // Schema: input + target VID + target _labels + internal edge ID
        assert_eq!(output_schema.fields().len(), 4);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "__eid_to_m");
    }

    /// White-box guard: the synchronous relationship-scan builder must resolve
    /// a target's labels from the persisted `VidLabelsIndex`, not L0 alone.
    ///
    /// Complements the end-to-end tests by pinning the private sync builder
    /// directly, so a future planner change that stops routing to it cannot
    /// silently drop this coverage. The vertex is flushed to Lance and the L0
    /// chain is empty, so the label can only come from the persisted index.
    #[tokio::test]
    async fn test_sync_builder_resolves_labels_from_persisted_index() {
        use arrow_array::{ListArray, StringArray};
        use uni_common::core::schema::{DataType as UniDataType, SchemaManager};
        use uni_store::runtime::l0::L0Buffer;
        use uni_store::runtime::property_manager::PropertyManager;
        use uni_store::runtime::writer::Writer;
        use uni_store::storage::manager::StorageManager;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let schema_manager = SchemaManager::load(path.join("schema.json")).await.unwrap();
        schema_manager.add_label("Person").unwrap();
        schema_manager
            .add_property("Person", "name", UniDataType::String, true)
            .unwrap();
        schema_manager.save().await.unwrap();
        let schema_manager = Arc::new(schema_manager);

        let storage = Arc::new(
            StorageManager::new(
                path.join("storage").to_str().unwrap(),
                schema_manager.clone(),
            )
            .await
            .unwrap(),
        );

        let writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap();
        let target_vid = writer.next_vid().await.unwrap();
        let mut props = HashMap::new();
        props.insert("name".to_string(), UniValue::from("Alice"));
        writer
            .insert_vertex_with_labels(target_vid, props, &["Person".to_string()], None)
            .await
            .unwrap();
        // Flush so the label lives only in the persisted index, not L0.
        writer.flush_to_l1(None).await.unwrap();

        let property_manager = Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            100,
        ));
        // Fresh empty L0 chain => the label is reachable only via the index.
        let empty_l0 = Arc::new(parking_lot::RwLock::new(L0Buffer::new(0, None)));
        let graph_ctx = GraphExecutionContext::new(storage.clone(), empty_l0, property_manager);

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));
        let input = RecordBatch::try_new(
            input_schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1u64]))],
        )
        .unwrap();
        let output_schema =
            GraphTraverseExec::build_schema(input_schema, "m", None, &[], &[], None, None, false);
        let expansions = vec![(0usize, target_vid, 0u64, 0u32, None)];

        let out = build_traverse_output_batch_sync(
            &input,
            &expansions,
            &output_schema,
            None,
            // No scoping label: labels must come from the index, not a fallback.
            &None,
            &graph_ctx,
            false,
            &HashSet::new(),
        )
        .unwrap();

        let labels_col = out.column(2).as_any().downcast_ref::<ListArray>().unwrap();
        let row0 = labels_col.value(0);
        let strs = row0.as_any().downcast_ref::<StringArray>().unwrap();
        let got: Vec<String> = strs.iter().flatten().map(|s| s.to_string()).collect();
        assert_eq!(
            got,
            vec!["Person".to_string()],
            "sync builder must resolve labels from the persisted VidLabelsIndex"
        );
    }

    /// White-box guard (#140): `resolve_vertex_labels` must not resurrect the
    /// labels of a flushed-then-deleted vertex from the stale persisted index.
    ///
    /// The vertex is flushed so its label lives only in `VidLabelsIndex`, then a
    /// tombstone is recorded in the live L0 (mirroring a committed-but-unflushed
    /// `DELETE`, which leaves the persisted index entry in place until the next
    /// flush). Before the fix the index fallback returned the stale
    /// `["Person"]`; the tombstone guard resolves the deleted vertex to an empty
    /// label set so every label predicate rejects it rather than admitting it.
    #[tokio::test]
    async fn test_resolve_labels_does_not_resurrect_deleted_vertex() {
        use uni_common::core::schema::{DataType as UniDataType, SchemaManager};
        use uni_store::runtime::l0::L0Buffer;
        use uni_store::runtime::property_manager::PropertyManager;
        use uni_store::runtime::writer::Writer;
        use uni_store::storage::manager::StorageManager;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        let schema_manager = SchemaManager::load(path.join("schema.json")).await.unwrap();
        schema_manager.add_label("Person").unwrap();
        schema_manager
            .add_property("Person", "name", UniDataType::String, true)
            .unwrap();
        schema_manager.save().await.unwrap();
        let schema_manager = Arc::new(schema_manager);

        let storage = Arc::new(
            StorageManager::new(
                path.join("storage").to_str().unwrap(),
                schema_manager.clone(),
            )
            .await
            .unwrap(),
        );

        let writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap();
        let deleted_vid = writer.next_vid().await.unwrap();
        let mut props = HashMap::new();
        props.insert("name".to_string(), UniValue::from("Alice"));
        writer
            .insert_vertex_with_labels(deleted_vid, props, &["Person".to_string()], None)
            .await
            .unwrap();
        // Flush so the label lives only in the persisted VidLabelsIndex.
        writer.flush_to_l1(None).await.unwrap();

        // Pre-condition for the bug: the index still holds the stale label after
        // the flush, so an unguarded fallback would resurrect it.
        assert_eq!(
            storage.get_labels_from_index(deleted_vid),
            Some(vec!["Person".to_string()]),
            "persisted index must still carry the label (the resurrection source)"
        );

        let property_manager = Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            100,
        ));
        // A live L0 carrying the vertex's tombstone (committed-but-unflushed DELETE).
        let l0 = Arc::new(parking_lot::RwLock::new(L0Buffer::new(0, None)));
        l0.write().vertex_tombstones.insert(deleted_vid);
        let graph_ctx = GraphExecutionContext::new(storage.clone(), l0, property_manager);
        let query_ctx = graph_ctx.query_context();

        let got = graph_ctx.resolve_vertex_labels(deleted_vid, &query_ctx);
        assert_eq!(
            got,
            Some(Vec::new()),
            "a deleted vertex must resolve to empty labels, not the stale persisted index entry"
        );
    }

    #[test]
    fn test_traverse_schema_with_edge() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let output_schema = GraphTraverseExec::build_schema(
            input_schema,
            "m",
            Some("r"),
            &[],
            &[],
            None,
            None,
            false,
        );

        // Schema: input + target VID + target _labels + edge EID + edge _type
        assert_eq!(output_schema.fields().len(), 5);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "r._eid");
        assert_eq!(output_schema.field(4).name(), "r._type");
    }

    #[test]
    fn test_traverse_schema_with_target_properties() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let target_props = vec!["name".to_string(), "age".to_string()];
        let output_schema = GraphTraverseExec::build_schema(
            input_schema,
            "m",
            Some("r"),
            &[],
            &target_props,
            None,
            None,
            false,
        );

        // a._vid, m._vid, m._labels, m.name, m.age, r._eid, r._type
        assert_eq!(output_schema.fields().len(), 7);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "m.name");
        assert_eq!(output_schema.field(4).name(), "m.age");
        assert_eq!(output_schema.field(5).name(), "r._eid");
        assert_eq!(output_schema.field(6).name(), "r._type");
    }

    #[test]
    fn test_variable_length_schema() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let output_schema = GraphVariableLengthTraverseExec::build_schema(
            input_schema,
            "b",
            None,
            Some("p"),
            &[],
            None,
            &[],
        );

        assert_eq!(output_schema.fields().len(), 5);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "b._vid");
        assert_eq!(output_schema.field(2).name(), "b._labels");
        assert_eq!(output_schema.field(3).name(), "_hop_count");
        assert_eq!(output_schema.field(4).name(), "p");
    }

    #[test]
    fn test_traverse_main_schema_without_edge() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let output_schema =
            GraphTraverseMainExec::build_schema(&input_schema, "m", &None, &[], &[], false);

        // a._vid, m._vid, m._labels, __eid_to_m
        assert_eq!(output_schema.fields().len(), 4);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "__eid_to_m");
    }

    #[test]
    fn test_traverse_main_schema_with_edge() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let output_schema = GraphTraverseMainExec::build_schema(
            &input_schema,
            "m",
            &Some("r".to_string()),
            &[],
            &[],
            false,
        );

        // a._vid, m._vid, m._labels, r._eid, r._type
        assert_eq!(output_schema.fields().len(), 5);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "r._eid");
        assert_eq!(output_schema.field(4).name(), "r._type");
    }

    #[test]
    fn test_traverse_main_schema_with_edge_properties() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let edge_props = vec!["weight".to_string(), "since".to_string()];
        let output_schema = GraphTraverseMainExec::build_schema(
            &input_schema,
            "m",
            &Some("r".to_string()),
            &edge_props,
            &[],
            false,
        );

        // a._vid, m._vid, m._labels, r._eid, r._type, r.weight, r.since
        assert_eq!(output_schema.fields().len(), 7);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "r._eid");
        assert_eq!(output_schema.field(4).name(), "r._type");
        assert_eq!(output_schema.field(5).name(), "r.weight");
        assert_eq!(output_schema.field(5).data_type(), &DataType::LargeBinary);
        assert_eq!(output_schema.field(6).name(), "r.since");
        assert_eq!(output_schema.field(6).data_type(), &DataType::LargeBinary);
    }

    #[test]
    fn test_traverse_main_schema_with_target_properties() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let target_props = vec!["name".to_string(), "age".to_string()];
        let output_schema = GraphTraverseMainExec::build_schema(
            &input_schema,
            "m",
            &Some("r".to_string()),
            &[],
            &target_props,
            false,
        );

        // a._vid, m._vid, m._labels, r._eid, r._type, m.name, m.age
        assert_eq!(output_schema.fields().len(), 7);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m._labels");
        assert_eq!(output_schema.field(3).name(), "r._eid");
        assert_eq!(output_schema.field(4).name(), "r._type");
        assert_eq!(output_schema.field(5).name(), "m.name");
        assert_eq!(output_schema.field(5).data_type(), &DataType::LargeBinary);
        assert_eq!(output_schema.field(6).name(), "m.age");
        assert_eq!(output_schema.field(6).data_type(), &DataType::LargeBinary);
    }
}
