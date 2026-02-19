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
use crate::query::df_graph::common::{
    append_edge_to_struct, append_node_to_struct, build_edge_list_field, build_path_struct_field,
    column_as_vid_array, compute_plan_properties, labels_data_type, new_edge_list_builder,
    new_node_list_builder,
};
use crate::query::df_graph::scan::{build_property_column_static, resolve_property_type};
use arrow::compute::take;
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::l0_visibility;
use uni_store::storage::direction::Direction;

/// BFS result: (target_vid, hop_count, node_path, edge_path)
type BfsResult = (Vid, usize, Vec<Vid>, Vec<Eid>);

/// Expansion record: (original_row_idx, target_vid, hop_count, node_path, edge_path)
type ExpansionRecord = (usize, Vid, usize, Vec<Vid>, Vec<Eid>);

/// Resolve edge property Arrow type, falling back to `LargeBinary` (CypherValue) for
/// schemaless properties. Unlike vertex properties, schemaless edge properties must
/// preserve original JSON value types (int, float, etc.) since edge types commonly
/// lack explicit property definitions.
fn resolve_edge_property_type(
    prop: &str,
    schema_props: Option<
        &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
    >,
) -> DataType {
    if prop == "overflow_json" {
        DataType::LargeBinary
    } else {
        schema_props
            .and_then(|props| props.get(prop))
            .map(|meta| meta.r#type.to_arrow())
            .unwrap_or(DataType::LargeBinary)
    }
}

fn merged_edge_schema_props(
    uni_schema: &uni_common::core::schema::Schema,
    edge_type_ids: &[u32],
) -> HashMap<String, uni_common::core::schema::PropertyMeta> {
    let mut merged: HashMap<String, uni_common::core::schema::PropertyMeta> = HashMap::new();
    let mut sorted_ids = edge_type_ids.to_vec();
    sorted_ids.sort_unstable();

    for edge_type_id in sorted_ids {
        if let Some(edge_type_name) = uni_schema.edge_type_name_by_id_unified(edge_type_id)
            && let Some(props) = uni_schema.properties.get(edge_type_name.as_str())
        {
            for (prop_name, meta) in props {
                match merged.get_mut(prop_name) {
                    Some(existing) => {
                        if existing.r#type != meta.r#type {
                            existing.r#type = uni_common::core::schema::DataType::CypherValue;
                        }
                        existing.nullable |= meta.nullable;
                    }
                    None => {
                        merged.insert(prop_name.clone(), meta.clone());
                    }
                }
            }
        }
    }

    merged
}

/// Expansion tuple for variable-length traversal: (input_row_idx, target_vid, hop_count, node_path, edge_path)
type VarLengthExpansion = (usize, Vid, usize, Vec<Vid>, Vec<Eid>);

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
    properties: PlanProperties,

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
        let label_props = target_label_name
            .as_deref()
            .and_then(|ln| uni_schema.properties.get(ln));
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
            fields.push(Field::new(&col_name, arrow_type, true));
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
                fields.push(Field::new(&prop_col_name, arrow_type, true));
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
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
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
    }
}

impl ExecutionPlan for GraphTraverseExec {
    fn name(&self) -> &str {
        "GraphTraverseExec"
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
        let input_stream = self.input.execute(partition, context)?;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        let warm_fut = self
            .graph_ctx
            .warming_future(self.edge_type_ids.clone(), self.direction);

        Ok(Box::pin(GraphTraverseStream {
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
    /// Stream is done.
    Done,
}

/// Stream that performs single-hop traversal with async property materialization.
struct GraphTraverseStream {
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
}

impl GraphTraverseStream {
    /// Expand neighbors synchronously and return expansions.
    /// Returns (row_idx, target_vid, eid_u64, edge_type_id).
    fn expand_neighbors(&self, batch: &RecordBatch) -> DFResult<Vec<(usize, Vid, u64, u32)>> {
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
        let used_edge_arrays: Vec<&UInt64Array> = self
            .used_edge_columns
            .iter()
            .filter_map(|col| {
                batch
                    .column_by_name(col)
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            })
            .collect();

        let mut expanded_rows: Vec<(usize, Vid, u64, u32)> = Vec::new();
        let is_undirected = matches!(self.direction, Direction::Both);

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
            let used_eids: HashSet<u64> = used_edge_arrays
                .iter()
                .filter_map(|arr| {
                    if arr.is_null(row_idx) {
                        None
                    } else {
                        Some(arr.value(row_idx))
                    }
                })
                .collect();

            let vid = Vid::from(src);
            // For Direction::Both, deduplicate edges by eid within each source.
            // This prevents the same edge being counted twice (once outgoing, once incoming).
            let mut seen_edges: HashSet<u64> = HashSet::new();

            for &edge_type in &self.edge_type_ids {
                let neighbors = self.graph_ctx.get_neighbors(vid, edge_type, self.direction);

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

                    // Filter by target label using L0 visibility.
                    // VIDs no longer embed label information, so we must look up labels.
                    if let Some(ref label_name) = self.target_label_name {
                        let query_ctx = self.graph_ctx.query_context();
                        if let Some(vertex_labels) =
                            l0_visibility::get_vertex_labels_optional(target_vid, &query_ctx)
                        {
                            // Vertex is in L0 — require actual label match
                            if !vertex_labels.contains(label_name) {
                                continue;
                            }
                        }
                        // else: vertex not in L0 → trust storage-level filtering
                    }

                    expanded_rows.push((row_idx, target_vid, eid_u64, edge_type));
                }
            }
        }

        Ok(expanded_rows)
    }
}

/// Build the output batch with target vertex properties.
///
/// This is a standalone async function so it can be boxed into a `Send` future
/// without borrowing from `GraphTraverseStream`.
#[expect(
    clippy::too_many_arguments,
    reason = "Standalone async fn needs all context passed explicitly"
)]
async fn build_traverse_output_batch(
    input: RecordBatch,
    expansions: Vec<(usize, Vid, u64, u32)>,
    schema: SchemaRef,
    edge_variable: Option<String>,
    edge_properties: Vec<String>,
    edge_type_ids: Vec<u32>,
    target_properties: Vec<String>,
    target_label_name: Option<String>,
    graph_ctx: Arc<GraphExecutionContext>,
    optional: bool,
    optional_pattern_vars: HashSet<String>,
) -> DFResult<RecordBatch> {
    if expansions.is_empty() {
        if !optional {
            return Ok(RecordBatch::new_empty(schema));
        }
        let unmatched_reps = collect_unmatched_optional_group_rows(
            &input,
            &HashSet::new(),
            &schema,
            &optional_pattern_vars,
        )?;
        if unmatched_reps.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }
        return build_optional_null_batch_for_rows_with_optional_vars(
            &input,
            &unmatched_reps,
            &schema,
            &optional_pattern_vars,
        );
    }

    // Build index array for take operation
    let indices: Vec<u64> = expansions
        .iter()
        .map(|(idx, _, _, _)| *idx as u64)
        .collect();
    let indices_array = UInt64Array::from(indices);

    // Expand input columns
    let mut columns: Vec<ArrayRef> = Vec::new();
    for col in input.columns() {
        let expanded = take(col.as_ref(), &indices_array, None)?;
        columns.push(expanded);
    }

    // Add target VID column
    let target_vids: Vec<Vid> = expansions.iter().map(|(_, vid, _, _)| *vid).collect();
    let target_vid_u64s: Vec<u64> = target_vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(target_vid_u64s)));

    // Add target ._labels column (from L0 buffers)
    {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        let query_ctx = graph_ctx.query_context();
        for vid in &target_vids {
            let row_labels: Vec<String> =
                match l0_visibility::get_vertex_labels_optional(*vid, &query_ctx) {
                    Some(labels) => {
                        // Vertex is in L0 — use actual labels only
                        labels
                    }
                    None => {
                        // Vertex not in L0 — trust schema label (storage already filtered)
                        if let Some(ref label_name) = target_label_name {
                            vec![label_name.clone()]
                        } else {
                            vec![]
                        }
                    }
                };
            let values = labels_builder.values();
            for lbl in &row_labels {
                values.append_value(lbl);
            }
            labels_builder.append(true);
        }
        columns.push(Arc::new(labels_builder.finish()));
    }

    // Add target vertex property columns (async)
    if !target_properties.is_empty() {
        if let Some(ref label_name) = target_label_name {
            let property_manager = graph_ctx.property_manager();
            let query_ctx = graph_ctx.query_context();

            let props_map = property_manager
                .get_batch_vertex_props_for_label(&target_vids, label_name, Some(&query_ctx))
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            // Resolve property types from the uni schema
            let uni_schema = graph_ctx.storage().schema_manager().schema();
            let label_props = uni_schema.properties.get(label_name.as_str());

            for prop_name in &target_properties {
                let data_type = resolve_property_type(prop_name, label_props);
                let column =
                    build_property_column_static(&target_vids, &props_map, prop_name, &data_type)?;
                columns.push(column);
            }
        } else {
            // No label name — use label-agnostic property lookup.
            // This scans all label datasets, slower but correct for label-less traversals.
            let non_internal_props: Vec<&str> = target_properties
                .iter()
                .filter(|p| *p != "_all_props")
                .map(|s| s.as_str())
                .collect();
            let property_manager = graph_ctx.property_manager();
            let query_ctx = graph_ctx.query_context();

            let props_map = if !non_internal_props.is_empty() {
                property_manager
                    .get_batch_vertex_props(&target_vids, &non_internal_props, Some(&query_ctx))
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
            } else {
                std::collections::HashMap::new()
            };

            for prop_name in &target_properties {
                if prop_name == "_all_props" {
                    // Build CypherValue blob from all vertex properties (L0 + storage)
                    use crate::query::df_graph::scan::encode_cypher_value;
                    use arrow_array::builder::LargeBinaryBuilder;

                    let mut builder = LargeBinaryBuilder::new();
                    let l0_ctx = graph_ctx.l0_context();
                    for vid in &target_vids {
                        let mut merged_props = serde_json::Map::new();
                        // Collect from storage-hydrated props
                        if let Some(vid_props) = props_map.get(vid) {
                            for (k, v) in vid_props.iter() {
                                let json_val: serde_json::Value = v.clone().into();
                                merged_props.insert(k.to_string(), json_val);
                            }
                        }
                        // Overlay L0 properties
                        for l0 in l0_ctx.iter_l0_buffers() {
                            let guard = l0.read();
                            if let Some(l0_props) = guard.vertex_properties.get(vid) {
                                for (k, v) in l0_props.iter() {
                                    let json_val: serde_json::Value = v.clone().into();
                                    merged_props.insert(k.to_string(), json_val);
                                }
                            }
                        }
                        if merged_props.is_empty() {
                            builder.append_null();
                        } else {
                            let json = serde_json::Value::Object(merged_props);
                            match encode_cypher_value(&json) {
                                Ok(bytes) => builder.append_value(bytes),
                                Err(_) => builder.append_null(),
                            }
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                } else {
                    let column = build_property_column_static(
                        &target_vids,
                        &props_map,
                        prop_name,
                        &arrow::datatypes::DataType::LargeBinary,
                    )?;
                    columns.push(column);
                }
            }
        }
    }

    // Add edge ID column and properties if edge is bound
    if edge_variable.is_some() {
        let eids: Vec<Eid> = expansions
            .iter()
            .map(|(_, _, eid, _)| Eid::from(*eid))
            .collect();
        let eid_u64s: Vec<u64> = eids.iter().map(|e| e.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(eid_u64s)));

        // Add edge _type column
        {
            let uni_schema = graph_ctx.storage().schema_manager().schema();
            let mut type_builder = arrow_array::builder::StringBuilder::new();
            for (_, _, _, edge_type_id) in &expansions {
                if let Some(name) = uni_schema.edge_type_name_by_id_unified(*edge_type_id) {
                    type_builder.append_value(&name);
                } else {
                    type_builder.append_null();
                }
            }
            columns.push(Arc::new(type_builder.finish()));
        }

        if !edge_properties.is_empty() {
            let prop_name_refs: Vec<&str> = edge_properties.iter().map(|s| s.as_str()).collect();
            let property_manager = graph_ctx.property_manager();
            let query_ctx = graph_ctx.query_context();

            let props_map = property_manager
                .get_batch_edge_props(&eids, &prop_name_refs, Some(&query_ctx))
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            let uni_schema = graph_ctx.storage().schema_manager().schema();
            let merged_edge_props = merged_edge_schema_props(&uni_schema, &edge_type_ids);
            let edge_type_props = if merged_edge_props.is_empty() {
                None
            } else {
                Some(&merged_edge_props)
            };

            // Use Vid::from(eid) as key — matches PropertyManager's return format
            let vid_keys: Vec<Vid> = eids.iter().map(|e| Vid::from(e.as_u64())).collect();

            for prop_name in &edge_properties {
                let data_type = resolve_edge_property_type(prop_name, edge_type_props);
                let column =
                    build_property_column_static(&vid_keys, &props_map, prop_name, &data_type)?;
                columns.push(column);
            }
        }
    } else {
        // Add internal edge ID column for relationship uniqueness tracking
        // even when edge variable is not explicitly bound.
        let eid_u64s: Vec<u64> = expansions.iter().map(|(_, _, eid, _)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(eid_u64s)));
    }

    let expanded_batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

    if optional {
        // Identify source groups that had no expansions and append one null row per group.
        let matched_indices: HashSet<usize> =
            expansions.iter().map(|(idx, _, _, _)| *idx).collect();
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
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
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
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

fn is_optional_column_for_vars(col_name: &str, optional_vars: &HashSet<String>) -> bool {
    for var in optional_vars {
        if col_name.starts_with(var.as_str()) && col_name[var.len()..].starts_with('.') {
            return true;
        }
        if col_name == var.as_str() {
            return true;
        }
        if col_name.starts_with("__eid_to_") && col_name.ends_with(var.as_str()) {
            return true;
        }
    }
    false
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

    let source_vid_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if idx >= input.num_columns() {
                return None;
            }
            let name = field.name();
            if !is_optional_column_for_vars(name, optional_vars) && name.ends_with("._vid") {
                Some(idx)
            } else {
                None
            }
        })
        .collect();

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

fn compute_optional_group_key(
    batch: &RecordBatch,
    row_idx: usize,
    source_vid_indices: &[usize],
) -> DFResult<Vec<u8>> {
    let mut key = Vec::with_capacity(source_vid_indices.len() * std::mem::size_of::<u64>());
    for &col_idx in source_vid_indices {
        let col = batch.column(col_idx);
        let vid_cow = column_as_vid_array(col.as_ref())?;
        let arr: &UInt64Array = &vid_cow;
        if arr.is_null(row_idx) {
            key.extend_from_slice(&u64::MAX.to_le_bytes());
        } else {
            key.extend_from_slice(&arr.value(row_idx).to_le_bytes());
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

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

impl Stream for GraphTraverseStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
                        return Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::Execution(e.to_string()),
                        )));
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
                                    &self.graph_ctx,
                                    self.optional,
                                    &self.optional_pattern_vars,
                                );
                                self.state = TraverseStreamState::Reading;
                                if let Ok(ref r) = result {
                                    self.metrics.record_output(r.num_rows());
                                }
                                return Poll::Ready(Some(result));
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
                        self.state = TraverseStreamState::Reading;
                        self.metrics.record_output(batch.num_rows());
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
fn build_traverse_output_batch_sync(
    input: &RecordBatch,
    expansions: &[(usize, Vid, u64, u32)],
    schema: &SchemaRef,
    edge_variable: Option<&String>,
    graph_ctx: &GraphExecutionContext,
    optional: bool,
    optional_pattern_vars: &HashSet<String>,
) -> DFResult<RecordBatch> {
    if expansions.is_empty() {
        if !optional {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let unmatched_reps = collect_unmatched_optional_group_rows(
            input,
            &HashSet::new(),
            schema,
            optional_pattern_vars,
        )?;
        if unmatched_reps.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        return build_optional_null_batch_for_rows_with_optional_vars(
            input,
            &unmatched_reps,
            schema,
            optional_pattern_vars,
        );
    }

    let indices: Vec<u64> = expansions
        .iter()
        .map(|(idx, _, _, _)| *idx as u64)
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
        .map(|(_, vid, _, _)| vid.as_u64())
        .collect();
    columns.push(Arc::new(UInt64Array::from(target_vids)));

    // Add target ._labels column (from L0 buffers)
    {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        let l0_ctx = graph_ctx.l0_context();
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        for (_, vid, _, _) in expansions {
            let mut row_labels: Vec<String> = Vec::new();
            for l0 in l0_ctx.iter_l0_buffers() {
                let guard = l0.read();
                if let Some(l0_labels) = guard.vertex_labels.get(vid) {
                    for lbl in l0_labels {
                        if !row_labels.contains(lbl) {
                            row_labels.push(lbl.clone());
                        }
                    }
                }
            }
            let values = labels_builder.values();
            for lbl in &row_labels {
                values.append_value(lbl);
            }
            labels_builder.append(true);
        }
        columns.push(Arc::new(labels_builder.finish()));
    }

    // Add edge columns if edge is bound (no properties in sync path)
    if edge_variable.is_some() {
        let edge_ids: Vec<u64> = expansions.iter().map(|(_, _, eid, _)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(edge_ids)));

        // Add edge _type column
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let mut type_builder = arrow_array::builder::StringBuilder::new();
        for (_, _, _, edge_type_id) in expansions {
            if let Some(name) = uni_schema.edge_type_name_by_id_unified(*edge_type_id) {
                type_builder.append_value(&name);
            } else {
                type_builder.append_null();
            }
        }
        columns.push(Arc::new(type_builder.finish()));
    } else {
        // Internal EID column for relationship uniqueness tracking (matches schema)
        let edge_ids: Vec<u64> = expansions.iter().map(|(_, _, eid, _)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(edge_ids)));
    }

    let expanded_batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

    if optional {
        let matched_indices: HashSet<usize> =
            expansions.iter().map(|(idx, _, _, _)| *idx).collect();
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

/// Adjacency map type: maps source VID to list of (target_vid, eid, edge_type_name, properties).
type EdgeAdjacencyMap = HashMap<Vid, Vec<(Vid, Eid, String, uni_common::Properties)>>;

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

    /// Column name of an already-bound target VID (for patterns where target is in scope).
    /// When set, only traversals reaching this exact VID are included.
    bound_target_column: Option<String>,

    /// Columns containing edge IDs from previous hops (for relationship uniqueness).
    /// Edges matching any of these IDs are excluded from traversal results.
    used_edge_columns: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

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

            // Edge properties: Utf8 for named props, LargeBinary for _all_props CypherValue
            for prop in edge_properties {
                if prop == "_all_props" {
                    fields.push(Field::new(
                        format!("{}._all_props", edge_var),
                        DataType::LargeBinary,
                        true,
                    ));
                } else {
                    fields.push(Field::new(
                        format!("{}.{}", edge_var, prop),
                        DataType::Utf8,
                        true,
                    ));
                }
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
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "GraphTraverseMainExec: types={:?}, direction={:?}",
                    self.type_names, self.direction
                )
            }
        }
    }
}

impl ExecutionPlan for GraphTraverseMainExec {
    fn name(&self) -> &str {
        "GraphTraverseMainExec"
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
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(GraphTraverseMainStream::new(
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

/// State machine for GraphTraverseMainStream.
enum GraphTraverseMainState {
    /// Loading adjacency map from main edges table.
    LoadingEdges {
        future: Pin<Box<dyn std::future::Future<Output = DFResult<EdgeAdjacencyMap>> + Send>>,
        input_stream: SendableRecordBatchStream,
    },
    /// Processing input stream with loaded adjacency.
    Processing {
        adjacency: EdgeAdjacencyMap,
        input_stream: SendableRecordBatchStream,
    },
    /// Stream is done.
    Done,
}

/// Stream that executes schemaless edge traversal.
struct GraphTraverseMainStream {
    /// Source column name.
    source_column: String,

    /// Edge type names (kept for debugging/future use).
    #[allow(dead_code)]
    type_names: Vec<String>,

    /// Traversal direction (kept for debugging/future use).
    #[allow(dead_code)]
    direction: Direction,

    /// Target variable name (kept for debugging/future use).
    #[allow(dead_code)]
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

    /// Column name of an already-bound target VID (for filtering).
    bound_target_column: Option<String>,

    /// Columns containing edge IDs from previous hops (for relationship uniqueness).
    used_edge_columns: Vec<String>,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: GraphTraverseMainState,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl GraphTraverseMainStream {
    /// Create a new traverse main stream.
    #[expect(clippy::too_many_arguments)]
    fn new(
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
        bound_target_column: Option<String>,
        used_edge_columns: Vec<String>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        // Start by loading the adjacency map from the main edges table
        let loading_ctx = graph_ctx.clone();
        let loading_types = type_names.clone();
        let fut =
            async move { build_edge_adjacency_map(&loading_ctx, &loading_types, direction).await };

        Self {
            source_column,
            type_names,
            direction,
            target_variable,
            edge_variable,
            edge_properties,
            target_properties,
            graph_ctx,
            optional,
            bound_target_column,
            used_edge_columns,
            schema,
            state: GraphTraverseMainState::LoadingEdges {
                future: Box::pin(fut),
                input_stream,
            },
            metrics,
        }
    }

    /// Expand input batch using adjacency map (synchronous version).
    fn expand_batch(
        &self,
        input: &RecordBatch,
        adjacency: &EdgeAdjacencyMap,
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
        let used_edge_arrays: Vec<&UInt64Array> = self
            .used_edge_columns
            .iter()
            .filter_map(|col| {
                input
                    .column_by_name(col)
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            })
            .collect();

        // Build expansions: (input_row_idx, target_vid, eid, edge_type, edge_props)
        type Expansion = (usize, Vid, Eid, String, uni_common::Properties);
        let mut expansions: Vec<Expansion> = Vec::new();

        for (row_idx, src_u64) in source_vids.iter().enumerate() {
            if let Some(src_u64) = src_u64 {
                let src_vid = Vid::from(src_u64);

                // Collect used edge IDs for this row from all previous hops
                let used_eids: HashSet<u64> = used_edge_arrays
                    .iter()
                    .filter_map(|arr| {
                        if arr.is_null(row_idx) {
                            None
                        } else {
                            Some(arr.value(row_idx))
                        }
                    })
                    .collect();

                if let Some(neighbors) = adjacency.get(&src_vid) {
                    for (target_vid, eid, edge_type, props) in neighbors {
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
                            edge_type.clone(),
                            props.clone(),
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
            expansions.iter().map(|(idx, _, _, _, _)| *idx).collect()
        } else {
            HashSet::new()
        };

        // Expand input columns using Arrow take()
        let mut columns: Vec<ArrayRef> = Vec::new();
        let indices: Vec<u64> = expansions
            .iter()
            .map(|(idx, _, _, _, _)| *idx as u64)
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
            .map(|(_, vid, _, _, _)| vid.as_u64())
            .collect();
        if input.schema().column_with_name(&target_vid_name).is_none() {
            columns.push(Arc::new(UInt64Array::from(target_vids.clone())));
        }

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", self.target_variable);
        if input
            .schema()
            .column_with_name(&target_labels_name)
            .is_none()
        {
            use arrow_array::builder::{ListBuilder, StringBuilder};
            let l0_ctx = self.graph_ctx.l0_context();
            let mut labels_builder = ListBuilder::new(StringBuilder::new());
            for (_, target_vid, _, _, _) in &expansions {
                let mut row_labels: Vec<String> = Vec::new();
                for l0 in l0_ctx.iter_l0_buffers() {
                    let guard = l0.read();
                    if let Some(l0_labels) = guard.vertex_labels.get(target_vid) {
                        for lbl in l0_labels {
                            if !row_labels.contains(lbl) {
                                row_labels.push(lbl.clone());
                            }
                        }
                    }
                }
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
                .map(|(_, _, eid, _, _)| eid.as_u64())
                .collect();
            columns.push(Arc::new(UInt64Array::from(eids)));

            // Add edge ._type column
            {
                let mut type_builder = arrow_array::builder::StringBuilder::new();
                for (_, _, _, edge_type, _) in &expansions {
                    type_builder.append_value(edge_type);
                }
                columns.push(Arc::new(type_builder.finish()));
            }

            // Add edge property columns
            for prop_name in &self.edge_properties {
                if prop_name == "_all_props" {
                    // Serialize all edge properties to CypherValue blob
                    use crate::query::df_graph::scan::encode_cypher_value;
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for (_, _, _, _, props) in &expansions {
                        if props.is_empty() {
                            builder.append_null();
                        } else {
                            let mut json_map = serde_json::Map::new();
                            for (k, v) in props.iter() {
                                let json_val: serde_json::Value = v.clone().into();
                                json_map.insert(k.clone(), json_val);
                            }
                            let json = serde_json::Value::Object(json_map);
                            match encode_cypher_value(&json) {
                                Ok(bytes) => builder.append_value(bytes),
                                Err(_) => builder.append_null(),
                            }
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                } else {
                    // Named property as Utf8
                    let mut builder = arrow_array::builder::StringBuilder::new();
                    for (_, _, _, _, props) in &expansions {
                        match props.get(prop_name) {
                            Some(uni_common::Value::String(s)) => builder.append_value(s),
                            Some(uni_common::Value::Null) | None => builder.append_null(),
                            Some(other) => builder.append_value(other.to_string()),
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
            }
        } else {
            let eids: Vec<u64> = expansions
                .iter()
                .map(|(_, _, eid, _, _)| eid.as_u64())
                .collect();
            columns.push(Arc::new(UInt64Array::from(eids)));
        }

        // Add target property columns (hydrate from L0 buffers)
        {
            use crate::query::df_graph::scan::encode_cypher_value;
            let l0_ctx = self.graph_ctx.l0_context();

            for prop_name in &self.target_properties {
                if prop_name == "_all_props" {
                    // Build full CypherValue blob from all L0 vertex properties
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for (_, target_vid, _, _, _) in &expansions {
                        let mut merged_props = serde_json::Map::new();
                        for l0 in l0_ctx.iter_l0_buffers() {
                            let guard = l0.read();
                            if let Some(props) = guard.vertex_properties.get(target_vid) {
                                for (k, v) in props.iter() {
                                    let json_val: serde_json::Value = v.clone().into();
                                    merged_props.insert(k.to_string(), json_val);
                                }
                            }
                        }
                        if merged_props.is_empty() {
                            builder.append_null();
                        } else {
                            let json = serde_json::Value::Object(merged_props);
                            match encode_cypher_value(&json) {
                                Ok(bytes) => builder.append_value(bytes),
                                Err(_) => builder.append_null(),
                            }
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                } else {
                    // Extract individual property from L0 and encode as CypherValue
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for (_, target_vid, _, _, _) in &expansions {
                        let mut found = false;
                        for l0 in l0_ctx.iter_l0_buffers() {
                            let guard = l0.read();
                            if let Some(props) = guard.vertex_properties.get(target_vid)
                                && let Some(val) = props.get(prop_name.as_str())
                                && !val.is_null()
                            {
                                let json_val: serde_json::Value = val.clone().into();
                                if let Ok(bytes) = encode_cypher_value(&json_val) {
                                    builder.append_value(bytes);
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            builder.append_null();
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
            }
        }

        let matched_batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        // Handle OPTIONAL: append unmatched rows with NULLs
        if self.optional {
            let unmatched: Vec<usize> = (0..input.num_rows())
                .filter(|idx| !matched_rows.contains(idx))
                .collect();

            if unmatched.is_empty() {
                return Ok(matched_batch);
            }

            let unmatched_batch =
                build_optional_null_batch_for_rows(input, &unmatched, &self.schema)?;

            // Concatenate matched and unmatched batches
            use arrow::compute::concat_batches;
            concat_batches(&self.schema, &[matched_batch, unmatched_batch])
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Ok(matched_batch)
        }
    }
}

/// Build adjacency map from main edges table for given type names and direction.
///
/// Supports OR relationship types like `[:KNOWS|HATES]` via multiple type_names.
/// Returns a HashMap mapping source VID -> Vec<(target_vid, eid, properties)>
/// Direction determines the key: Outgoing uses src_vid, Incoming uses dst_vid, Both adds entries for both.
async fn build_edge_adjacency_map(
    graph_ctx: &GraphExecutionContext,
    type_names: &[String],
    direction: Direction,
) -> DFResult<EdgeAdjacencyMap> {
    use uni_store::storage::main_edge::MainEdgeDataset;

    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let lancedb_store = storage.lancedb_store();

    // Step 1: Query main edges table for all type names
    let type_refs: Vec<&str> = type_names.iter().map(|s| s.as_str()).collect();
    let edges_with_type = MainEdgeDataset::find_edges_by_type_names(lancedb_store, &type_refs)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Preserve edge type name in the adjacency map for type(r) support
    let mut edges: Vec<(
        uni_common::Eid,
        uni_common::Vid,
        uni_common::Vid,
        String,
        uni_common::Properties,
    )> = edges_with_type.into_iter().collect();

    // Step 2: Overlay L0 buffers for all type names
    for l0 in l0_ctx.iter_l0_buffers() {
        let l0_guard = l0.read();

        for type_name in type_names {
            let l0_eids = l0_guard.eids_for_type(type_name);

            // For each L0 edge, extract its information
            for &eid in &l0_eids {
                if let Some(edge_ref) = l0_guard.graph.edge(eid) {
                    let src_vid = edge_ref.src_vid;
                    let dst_vid = edge_ref.dst_vid;

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

    // Step 3: Deduplicate by EID (L0 takes precedence)
    let mut seen_eids = HashSet::new();
    let mut unique_edges = Vec::new();
    for edge in edges.into_iter().rev() {
        if seen_eids.insert(edge.0) {
            unique_edges.push(edge);
        }
    }
    unique_edges.reverse();

    // Step 4: Build adjacency map based on direction
    let mut adjacency: EdgeAdjacencyMap = HashMap::new();

    for (eid, src_vid, dst_vid, edge_type, props) in unique_edges {
        match direction {
            Direction::Outgoing => {
                adjacency
                    .entry(src_vid)
                    .or_default()
                    .push((dst_vid, eid, edge_type, props));
            }
            Direction::Incoming => {
                adjacency
                    .entry(dst_vid)
                    .or_default()
                    .push((src_vid, eid, edge_type, props));
            }
            Direction::Both => {
                adjacency.entry(src_vid).or_default().push((
                    dst_vid,
                    eid,
                    edge_type.clone(),
                    props.clone(),
                ));
                adjacency
                    .entry(dst_vid)
                    .or_default()
                    .push((src_vid, eid, edge_type, props));
            }
        }
    }

    Ok(adjacency)
}

impl Stream for GraphTraverseMainStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, GraphTraverseMainState::Done);

            match state {
                GraphTraverseMainState::LoadingEdges {
                    mut future,
                    input_stream,
                } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(adjacency)) => {
                        // Move to processing state with loaded adjacency
                        self.state = GraphTraverseMainState::Processing {
                            adjacency,
                            input_stream,
                        };
                        // Continue loop to start processing
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = GraphTraverseMainState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = GraphTraverseMainState::LoadingEdges {
                            future,
                            input_stream,
                        };
                        return Poll::Pending;
                    }
                },
                GraphTraverseMainState::Processing {
                    adjacency,
                    mut input_stream,
                } => {
                    // Check timeout
                    if let Err(e) = self.graph_ctx.check_timeout() {
                        return Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::Execution(e.to_string()),
                        )));
                    }

                    match input_stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            // Expand batch using adjacency map
                            let result = self.expand_batch(&batch, &adjacency);

                            self.state = GraphTraverseMainState::Processing {
                                adjacency,
                                input_stream,
                            };

                            if let Ok(ref r) = result {
                                self.metrics.record_output(r.num_rows());
                            }
                            return Poll::Ready(Some(result));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            self.state = GraphTraverseMainState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            self.state = GraphTraverseMainState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => {
                            self.state = GraphTraverseMainState::Processing {
                                adjacency,
                                input_stream,
                            };
                            return Poll::Pending;
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

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

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
        );
        let properties = compute_plan_properties(schema.clone());

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
                fields.push(Field::new(&col_name, arrow_type, true));
            }
        }

        // Add hop count
        fields.push(Field::new("_hop_count", DataType::UInt64, false));

        // Add step variable (edge list) if bound
        if let Some(step_var) = step_variable {
            fields.push(build_edge_list_field(step_var));
        }

        // Add path struct if bound
        if let Some(path_var) = path_variable {
            fields.push(build_path_struct_field(path_var));
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphVariableLengthTraverseExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "GraphVariableLengthTraverseExec: {} --[{:?}*{}..{}]--> target",
                    self.source_column, self.edge_type_ids, self.min_hops, self.max_hops
                )
            }
        }
    }
}

impl ExecutionPlan for GraphVariableLengthTraverseExec {
    fn name(&self) -> &str {
        "GraphVariableLengthTraverseExec"
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
                "GraphVariableLengthTraverseExec requires exactly one child".to_string(),
            ));
        }

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

        Ok(Box::pin(GraphVariableLengthTraverseStream {
            input: input_stream,
            exec: Arc::new(self.clone_for_stream()),
            schema: self.schema.clone(),
            state: VarLengthStreamState::Warming(warm_fut),
            metrics,
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
            graph_ctx: self.graph_ctx.clone(),
        }
    }
}

/// Data needed by the stream (without ExecutionPlan overhead).
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
    graph_ctx: Arc<GraphExecutionContext>,
}

impl GraphVariableLengthTraverseExecData {
    /// Perform BFS from a source vertex.
    fn bfs(&self, source: Vid) -> Vec<BfsResult> {
        let mut results = Vec::new();
        let mut queue: VecDeque<BfsResult> = VecDeque::new();

        queue.push_back((source, 0, vec![source], vec![]));

        let is_undirected = matches!(self.direction, Direction::Both);

        while let Some((current, depth, node_path, edge_path)) = queue.pop_front() {
            // Emit result if within hop range (including zero-length patterns)
            if depth >= self.min_hops && depth <= self.max_hops {
                // Filter by target label (same logic as fixed-length traversal).
                // Only the final target node is constrained, not intermediate nodes.
                let label_ok = if let Some(ref label_name) = self.target_label_name {
                    let query_ctx = self.graph_ctx.query_context();
                    match l0_visibility::get_vertex_labels_optional(current, &query_ctx) {
                        Some(labels) => labels.contains(label_name),
                        None => true, // not in L0, trust storage
                    }
                } else {
                    true
                };

                if label_ok {
                    results.push((current, depth, node_path.clone(), edge_path.clone()));
                }
            }

            // Stop if at max depth
            if depth >= self.max_hops {
                continue;
            }

            // Get neighbors across all edge types
            let mut all_neighbors = Vec::new();
            for &edge_type_id in &self.edge_type_ids {
                let neighbors = self
                    .graph_ctx
                    .get_neighbors(current, edge_type_id, self.direction);
                all_neighbors.extend(neighbors);
            }

            // For Direction::Both, deduplicate edges by eid at each hop.
            // This prevents the same edge being found twice (once outgoing, once incoming).
            let mut seen_edges_at_hop: HashSet<u64> = HashSet::new();

            for (neighbor, eid) in all_neighbors {
                // Deduplicate edges for undirected patterns
                if is_undirected && !seen_edges_at_hop.insert(eid.as_u64()) {
                    continue;
                }

                // Enforce relationship uniqueness per-path (Cypher semantics).
                if edge_path.contains(&eid) {
                    continue;
                }

                let mut new_node_path = node_path.clone();
                new_node_path.push(neighbor);
                let mut new_edge_path = edge_path.clone();
                new_edge_path.push(eid);
                queue.push_back((neighbor, depth + 1, new_node_path, new_edge_path));
            }
        }

        results
    }
}

/// State machine for variable-length traverse stream.
enum VarLengthStreamState {
    /// Warming adjacency CSRs before first batch.
    Warming(Pin<Box<dyn std::future::Future<Output = DFResult<()>> + Send>>),
    /// Processing input batches.
    Reading,
    /// Materializing target vertex properties asynchronously.
    Materializing(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Stream is done.
    Done,
}

/// Stream for variable-length traversal.
struct GraphVariableLengthTraverseStream {
    input: SendableRecordBatchStream,
    exec: Arc<GraphVariableLengthTraverseExecData>,
    schema: SchemaRef,
    state: VarLengthStreamState,
    metrics: BaselineMetrics,
}

impl Stream for GraphVariableLengthTraverseStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, VarLengthStreamState::Done);

            match state {
                VarLengthStreamState::Warming(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
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
                        return Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::Execution(e.to_string()),
                        )));
                    }

                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            // Build base batch synchronously (BFS + expand)
                            let base_result = self.process_batch_base(batch);
                            let base_batch = match base_result {
                                Ok(b) => b,
                                Err(e) => {
                                    self.state = VarLengthStreamState::Reading;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };

                            // If no properties need async hydration, return directly
                            if self.exec.target_properties.is_empty() {
                                self.state = VarLengthStreamState::Reading;
                                return Poll::Ready(Some(Ok(base_batch)));
                            }

                            // Properties needed — create async future for hydration
                            let schema = self.schema.clone();
                            let target_variable = self.exec.target_variable.clone();
                            let target_properties = self.exec.target_properties.clone();
                            let target_label_name = self.exec.target_label_name.clone();
                            let graph_ctx = self.exec.graph_ctx.clone();

                            let fut = hydrate_vlp_target_properties(
                                base_batch,
                                schema,
                                target_variable,
                                target_properties,
                                target_label_name,
                                graph_ctx,
                            );

                            self.state = VarLengthStreamState::Materializing(Box::pin(fut));
                            // Continue loop to poll the future
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
                VarLengthStreamState::Materializing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = VarLengthStreamState::Reading;
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
    fn process_batch_base(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
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

        // Collect all BFS results
        let mut expansions: Vec<VarLengthExpansion> = Vec::new();

        for (row_idx, source_vid) in source_vids.iter().enumerate() {
            let mut emitted_for_row = false;

            if let Some(src) = source_vid {
                let vid = Vid::from(src);
                let bfs_results = self.exec.bfs(vid);

                for (target, hop_count, node_path, edge_path) in bfs_results {
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

                    expansions.push((row_idx, target, hop_count, node_path, edge_path));
                    emitted_for_row = true;
                }
            }

            if self.exec.is_optional && !emitted_for_row {
                // Preserve the source row with NULL optional bindings.
                // We use empty node/edge paths to mark unmatched rows.
                expansions.push((row_idx, Vid::from(u64::MAX), 0, vec![], vec![]));
            }
        }

        self.build_output_batch(&batch, &expansions)
    }

    fn build_output_batch(
        &self,
        input: &RecordBatch,
        expansions: &[VarLengthExpansion],
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
        // Unmatched OPTIONAL rows are represented by empty node/edge paths.
        let unmatched_rows: Vec<bool> = expansions
            .iter()
            .map(|(_, _, _, node_path, edge_path)| node_path.is_empty() && edge_path.is_empty())
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

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", self.exec.target_variable);
        if input
            .schema()
            .column_with_name(&target_labels_name)
            .is_none()
        {
            use arrow_array::builder::{ListBuilder, StringBuilder};
            let query_ctx = self.exec.graph_ctx.query_context();
            let mut labels_builder = ListBuilder::new(StringBuilder::new());
            for target_vid in &target_vids {
                let Some(vid_u64) = target_vid else {
                    labels_builder.append(false);
                    continue;
                };
                let vid = Vid::from(*vid_u64);
                let row_labels: Vec<String> =
                    match l0_visibility::get_vertex_labels_optional(vid, &query_ctx) {
                        Some(labels) => {
                            // Vertex is in L0 — use actual labels only
                            labels
                        }
                        None => {
                            // Vertex not in L0 — trust schema label (storage already filtered)
                            if let Some(ref label_name) = self.exec.target_label_name {
                                vec![label_name.clone()]
                            } else {
                                vec![]
                            }
                        }
                    };
                let values = labels_builder.values();
                for lbl in &row_labels {
                    values.append_value(lbl);
                }
                labels_builder.append(true);
            }
            columns.push(Arc::new(labels_builder.finish()));
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
        let hop_counts: Vec<u64> = expansions
            .iter()
            .map(|(_, _, hops, _, _)| *hops as u64)
            .collect();
        columns.push(Arc::new(UInt64Array::from(hop_counts)));

        // Add step variable (edge list) column if bound
        if self.exec.step_variable.is_some() {
            let mut edges_builder = new_edge_list_builder();
            let query_ctx = self.exec.graph_ctx.query_context();

            for (_, _, _, node_path, edge_path) in expansions {
                if node_path.is_empty() && edge_path.is_empty() {
                    // Null row for OPTIONAL MATCH unmatched
                    edges_builder.append_null();
                } else if edge_path.is_empty() {
                    // Zero-hop match: empty list
                    edges_builder.append(true);
                } else {
                    for (i, eid) in edge_path.iter().enumerate() {
                        let type_name = l0_visibility::get_edge_type(*eid, &query_ctx)
                            .unwrap_or_else(|| "UNKNOWN".to_string());
                        append_edge_to_struct(
                            edges_builder.values(),
                            *eid,
                            &type_name,
                            node_path[i].as_u64(),
                            node_path[i + 1].as_u64(),
                            &query_ctx,
                        );
                    }
                    edges_builder.append(true);
                }
            }

            columns.push(Arc::new(edges_builder.finish()));
        }

        // Add path variable column if bound.
        // For named paths, we output a Path struct with nodes and relationships arrays.
        if self.exec.path_variable.is_some() {
            let mut nodes_builder = new_node_list_builder();
            let mut rels_builder = new_edge_list_builder();
            let query_ctx = self.exec.graph_ctx.query_context();
            let mut path_validity = Vec::with_capacity(expansions.len());

            for (_, _, _, node_path, edge_path) in expansions {
                if node_path.is_empty() && edge_path.is_empty() {
                    nodes_builder.append(false);
                    rels_builder.append(false);
                    path_validity.push(false);
                    continue;
                }
                for vid in node_path {
                    append_node_to_struct(nodes_builder.values(), *vid, &query_ctx);
                }
                nodes_builder.append(true);

                for (i, eid) in edge_path.iter().enumerate() {
                    let type_name = l0_visibility::get_edge_type(*eid, &query_ctx)
                        .unwrap_or_else(|| "UNKNOWN".to_string());
                    append_edge_to_struct(
                        rels_builder.values(),
                        *eid,
                        &type_name,
                        node_path[i].as_u64(),
                        node_path[i + 1].as_u64(),
                        &query_ctx,
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
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            columns.push(Arc::new(path_struct));
        }

        self.metrics.record_output(num_rows);

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
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
        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        let props_map = property_manager
            .get_batch_vertex_props_for_label(&target_vids, label_name, Some(&query_ctx))
            .await
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let label_props = uni_schema.properties.get(label_name.as_str());

        for prop_name in &target_properties {
            let data_type = resolve_property_type(prop_name, label_props);
            let column =
                build_property_column_static(&target_vids, &props_map, prop_name, &data_type)?;
            property_columns.push(column);
        }
    } else {
        // No label name — use label-agnostic property lookup.
        // This scans all label datasets, slower but correct for label-less traversals.
        let non_internal_props: Vec<&str> = target_properties
            .iter()
            .filter(|p| *p != "_all_props")
            .map(|s| s.as_str())
            .collect();
        let property_manager = graph_ctx.property_manager();
        let query_ctx = graph_ctx.query_context();

        let props_map = if !non_internal_props.is_empty() {
            property_manager
                .get_batch_vertex_props(&target_vids, &non_internal_props, Some(&query_ctx))
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        } else {
            std::collections::HashMap::new()
        };

        for prop_name in &target_properties {
            if prop_name == "_all_props" {
                // Build CypherValue blob from all vertex properties (L0 + storage)
                use crate::query::df_graph::scan::encode_cypher_value;
                use arrow_array::builder::LargeBinaryBuilder;

                let mut builder = LargeBinaryBuilder::new();
                let l0_ctx = graph_ctx.l0_context();
                for vid in &target_vids {
                    let mut merged_props = serde_json::Map::new();
                    // Collect from storage-hydrated props
                    if let Some(vid_props) = props_map.get(vid) {
                        for (k, v) in vid_props.iter() {
                            let json_val: serde_json::Value = v.clone().into();
                            merged_props.insert(k.to_string(), json_val);
                        }
                    }
                    // Overlay L0 properties
                    for l0 in l0_ctx.iter_l0_buffers() {
                        let guard = l0.read();
                        if let Some(l0_props) = guard.vertex_properties.get(vid) {
                            for (k, v) in l0_props.iter() {
                                let json_val: serde_json::Value = v.clone().into();
                                merged_props.insert(k.to_string(), json_val);
                            }
                        }
                    }
                    if merged_props.is_empty() {
                        builder.append_null();
                    } else {
                        let json = serde_json::Value::Object(merged_props);
                        match encode_cypher_value(&json) {
                            Ok(bytes) => builder.append_value(bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                property_columns.push(Arc::new(builder.finish()));
            } else {
                let column = build_property_column_static(
                    &target_vids,
                    &props_map,
                    prop_name,
                    &arrow::datatypes::DataType::LargeBinary,
                )?;
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

    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
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

    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Output schema.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

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
        fields.push(Field::new("_hop_count", DataType::UInt64, false));

        // Add step variable column (list of edge structs) if bound
        // This is the relationship variable like `r` in `[r*1..3]`
        if let Some(step_var) = step_variable {
            fields.push(build_edge_list_field(step_var));
        }

        // Add path struct if bound
        if let Some(path_var) = path_variable {
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
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "GraphVariableLengthTraverseMainExec: {} --[{:?}*{}..{}]--> target",
                    self.source_column, self.type_names, self.min_hops, self.max_hops
                )
            }
        }
    }
}

impl ExecutionPlan for GraphVariableLengthTraverseMainExec {
    fn name(&self) -> &str {
        "GraphVariableLengthTraverseMainExec"
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
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Build adjacency map from main edges table (async)
        let graph_ctx = self.graph_ctx.clone();
        let type_names = self.type_names.clone();
        let direction = self.direction;
        let load_fut =
            async move { build_edge_adjacency_map(&graph_ctx, &type_names, direction).await };

        Ok(Box::pin(GraphVariableLengthTraverseMainStream {
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
    /// Materializing properties for a batch.
    Materializing {
        adjacency: EdgeAdjacencyMap,
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>,
    },
    /// Stream is done.
    Done,
}

/// Stream for variable-length traversal on schemaless edges.
struct GraphVariableLengthTraverseMainStream {
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
    schema: SchemaRef,
    state: VarLengthMainStreamState,
    metrics: BaselineMetrics,
}

/// BFS result type: (target_vid, hop_count, node_path, edge_path)
type MainBfsResult = (Vid, usize, Vec<Vid>, Vec<Eid>);

impl GraphVariableLengthTraverseMainStream {
    /// Perform BFS from a source vertex using the adjacency map.
    fn bfs(&self, source: Vid, adjacency: &EdgeAdjacencyMap) -> Vec<MainBfsResult> {
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

                for (neighbor, eid, _edge_type, _props) in neighbors {
                    // Deduplicate edges for undirected patterns
                    if is_undirected && !seen_edges_at_hop.insert(eid.as_u64()) {
                        continue;
                    }

                    // Enforce relationship uniqueness per-path (Cypher semantics).
                    if edge_path.contains(eid) {
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

    /// Process a batch using the adjacency map.
    fn process_batch(
        &self,
        batch: RecordBatch,
        adjacency: &EdgeAdjacencyMap,
    ) -> DFResult<RecordBatch> {
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

        // Collect BFS results: (original_row_idx, target_vid, hop_count, node_path, edge_path)
        let mut expansions: Vec<ExpansionRecord> = Vec::new();

        for (row_idx, source_opt) in source_vids.iter().enumerate() {
            let mut emitted_for_row = false;

            if let Some(source_u64) = source_opt {
                let source = Vid::from(source_u64);
                let bfs_results = self.bfs(source, adjacency);

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

        if expansions.is_empty() {
            if self.is_optional {
                let all_indices: Vec<usize> = (0..batch.num_rows()).collect();
                return build_optional_null_batch_for_rows(&batch, &all_indices, &self.schema);
            }
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_rows = expansions.len();
        self.metrics.record_output(num_rows);

        // Build output columns
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());

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

        // Add target ._labels column (only if not already in input)
        let target_labels_name = format!("{}._labels", self.target_variable);
        if batch
            .schema()
            .column_with_name(&target_labels_name)
            .is_none()
        {
            use arrow_array::builder::{ListBuilder, StringBuilder};
            let mut labels_builder = ListBuilder::new(StringBuilder::new());
            for (_, vid, _, node_path, edge_path) in expansions.iter() {
                if node_path.is_empty() && edge_path.is_empty() {
                    labels_builder.append(false);
                    continue;
                }
                let mut row_labels: Vec<String> = Vec::new();
                let labels =
                    l0_visibility::get_vertex_labels(*vid, &self.graph_ctx.query_context());
                for lbl in &labels {
                    if !row_labels.contains(lbl) {
                        row_labels.push(lbl.clone());
                    }
                }
                let values = labels_builder.values();
                for lbl in &row_labels {
                    values.append_value(lbl);
                }
                labels_builder.append(true);
            }
            columns.push(Arc::new(labels_builder.finish()));
        }

        // Add hop count column
        let hop_counts: Vec<u64> = expansions
            .iter()
            .map(|(_, _, hops, _, _)| *hops as u64)
            .collect();
        columns.push(Arc::new(UInt64Array::from(hop_counts)));

        // Add step variable column if bound (list of edge structs).
        if self.step_variable.is_some() {
            let mut edges_builder = new_edge_list_builder();
            let query_ctx = self.graph_ctx.query_context();
            let type_names_str = self.type_names.join("|");

            for (_, _, _, node_path, edge_path) in expansions.iter() {
                if node_path.is_empty() && edge_path.is_empty() {
                    edges_builder.append_null();
                } else if edge_path.is_empty() {
                    // Zero-hop match: empty list.
                    edges_builder.append(true);
                } else {
                    for (i, eid) in edge_path.iter().enumerate() {
                        append_edge_to_struct(
                            edges_builder.values(),
                            *eid,
                            &type_names_str,
                            node_path[i].as_u64(),
                            node_path[i + 1].as_u64(),
                            &query_ctx,
                        );
                    }
                    edges_builder.append(true);
                }
            }

            columns.push(Arc::new(edges_builder.finish()) as ArrayRef);
        }

        // Add path variable column if bound.
        if self.path_variable.is_some() {
            let mut nodes_builder = new_node_list_builder();
            let mut rels_builder = new_edge_list_builder();
            let query_ctx = self.graph_ctx.query_context();
            let type_names_str = self.type_names.join("|");
            let mut path_validity = Vec::with_capacity(expansions.len());

            for (_, _, _, node_path, edge_path) in expansions.iter() {
                if node_path.is_empty() && edge_path.is_empty() {
                    nodes_builder.append(false);
                    rels_builder.append(false);
                    path_validity.push(false);
                    continue;
                }
                for vid in node_path {
                    append_node_to_struct(nodes_builder.values(), *vid, &query_ctx);
                }
                nodes_builder.append(true);

                for (i, eid) in edge_path.iter().enumerate() {
                    append_edge_to_struct(
                        rels_builder.values(),
                        *eid,
                        &type_names_str,
                        node_path[i].as_u64(),
                        node_path[i + 1].as_u64(),
                        &query_ctx,
                    );
                }
                rels_builder.append(true);
                path_validity.push(true);
            }

            // Finish the builders to get the arrays
            let nodes_array = Arc::new(nodes_builder.finish()) as ArrayRef;
            let rels_array = Arc::new(rels_builder.finish()) as ArrayRef;

            // Build the path struct with nodes and relationships fields
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
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            columns.push(Arc::new(path_struct));
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

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl Stream for GraphVariableLengthTraverseMainStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(&mut self.state, VarLengthMainStreamState::Done);

            match state {
                VarLengthMainStreamState::Loading(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(adjacency)) => {
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
                            let base_batch = match self.process_batch(batch, &adjacency) {
                                Ok(b) => b,
                                Err(e) => {
                                    self.state = VarLengthMainStreamState::Processing(adjacency);
                                    return Poll::Ready(Some(Err(e)));
                                }
                            };

                            // If no properties need async hydration, return directly
                            if self.target_properties.is_empty() {
                                self.state = VarLengthMainStreamState::Processing(adjacency);
                                return Poll::Ready(Some(Ok(base_batch)));
                            }

                            // Create async hydration future
                            let schema = self.schema.clone();
                            let target_variable = self.target_variable.clone();
                            let target_properties = self.target_properties.clone();
                            let graph_ctx = self.graph_ctx.clone();

                            let fut = hydrate_vlp_target_properties(
                                base_batch,
                                schema,
                                target_variable,
                                target_properties,
                                None, // schemaless — no label name
                                graph_ctx,
                            );

                            self.state = VarLengthMainStreamState::Materializing {
                                adjacency,
                                fut: Box::pin(fut),
                            };
                            // Continue loop to poll the future
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
                VarLengthMainStreamState::Materializing { adjacency, mut fut } => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(Ok(batch)) => {
                            self.state = VarLengthMainStreamState::Processing(adjacency);
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
        assert_eq!(output_schema.field(5).data_type(), &DataType::Utf8);
        assert_eq!(output_schema.field(6).name(), "r.since");
        assert_eq!(output_schema.field(6).data_type(), &DataType::Utf8);
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
