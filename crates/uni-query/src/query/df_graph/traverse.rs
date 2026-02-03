// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

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
use crate::query::df_graph::scan::{build_property_column_static, resolve_property_type};
use arrow::compute::take;
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::l0_visibility;
use uni_store::storage::direction::Direction;

/// BFS result: (target_vid, hop_count, node_path, edge_path)
type BfsResult = (Vid, usize, Vec<Vid>, Vec<Eid>);

/// Resolve edge property Arrow type, falling back to `LargeBinary` (JSONB) for
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
    edge_type_ids: Vec<u16>,

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
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        edge_type_ids: Vec<u16>,
        direction: Direction,
        target_variable: impl Into<String>,
        edge_variable: Option<String>,
        edge_properties: Vec<String>,
        target_properties: Vec<String>,
        target_label_name: Option<String>,
        target_label_id: Option<u16>,
        graph_ctx: Arc<GraphExecutionContext>,
        optional: bool,
    ) -> Self {
        let source_column = source_column.into();
        let target_variable = target_variable.into();

        // Resolve target property Arrow types from the schema
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let label_props = target_label_name
            .as_deref()
            .and_then(|ln| uni_schema.properties.get(ln));

        let edge_props = edge_type_ids
            .first()
            .and_then(|&id| uni_schema.edge_type_name_by_id(id))
            .and_then(|name| uni_schema.properties.get(name));

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

        let properties = Self::compute_properties(schema.clone());

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

            // Add edge property columns with types resolved from schema
            for prop_name in edge_properties {
                let prop_col_name = format!("{}.{}", edge_var, prop_name);
                let arrow_type = resolve_edge_property_type(prop_name, edge_props);
                fields.push(Field::new(&prop_col_name, arrow_type, true));
            }
        }

        Arc::new(Schema::new(fields))
    }

    /// Compute plan properties.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
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
    edge_type_ids: Vec<u16>,

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

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: TraverseStreamState,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl GraphTraverseStream {
    /// Expand neighbors synchronously and return expansions.
    fn expand_neighbors(&self, batch: &RecordBatch) -> DFResult<Vec<(usize, Vid, u64)>> {
        let source_col = batch.column_by_name(&self.source_column).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Source column '{}' not found",
                self.source_column
            ))
        })?;

        let source_vids = source_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Source column is not UInt64".to_string(),
                )
            })?;

        let mut expanded_rows: Vec<(usize, Vid, u64)> = Vec::new();

        for (row_idx, source_vid) in source_vids.iter().enumerate() {
            let Some(src) = source_vid else {
                continue;
            };

            let vid = Vid::from(src);

            for &edge_type in &self.edge_type_ids {
                let neighbors = self.graph_ctx.get_neighbors(vid, edge_type, self.direction);

                for (target_vid, eid) in neighbors {
                    // Filter by target label using L0 visibility.
                    // VIDs no longer embed label information, so we must look up labels.
                    if let Some(ref label_name) = self.target_label_name {
                        let query_ctx = self.graph_ctx.query_context();
                        let vertex_labels = l0_visibility::get_vertex_labels(target_vid, &query_ctx);
                        // If L0 returns labels, check they contain the target label.
                        // If L0 returns empty, the vertex is in storage (not in L0), so we trust
                        // it was already filtered correctly by the dataset scan.
                        if !vertex_labels.is_empty() && !vertex_labels.contains(label_name) {
                            continue;
                        }
                    }

                    expanded_rows.push((row_idx, target_vid, eid.as_u64()));
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
    expansions: Vec<(usize, Vid, u64)>,
    schema: SchemaRef,
    edge_variable: Option<String>,
    edge_properties: Vec<String>,
    edge_type_ids: Vec<u16>,
    target_properties: Vec<String>,
    target_label_name: Option<String>,
    graph_ctx: Arc<GraphExecutionContext>,
    optional: bool,
) -> DFResult<RecordBatch> {
    if expansions.is_empty() {
        if !optional {
            return Ok(RecordBatch::new_empty(schema));
        }
        return build_optional_null_batch(&input, &schema);
    }

    let num_rows = expansions.len();

    // Build index array for take operation
    let indices: Vec<u64> = expansions.iter().map(|(idx, _, _)| *idx as u64).collect();
    let indices_array = UInt64Array::from(indices);

    // Expand input columns
    let mut columns: Vec<ArrayRef> = Vec::new();
    for col in input.columns() {
        let expanded = take(col.as_ref(), &indices_array, None)?;
        columns.push(expanded);
    }

    // Add target VID column
    let target_vids: Vec<Vid> = expansions.iter().map(|(_, vid, _)| *vid).collect();
    let target_vid_u64s: Vec<u64> = target_vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(target_vid_u64s)));

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
            // No label name — emit null columns for target properties
            for field in schema.fields().iter() {
                // Skip fields we've already added
                if columns.len() >= schema.fields().len() {
                    break;
                }
                // Only add nulls for remaining target property fields
                if columns.len() > input.num_columns() {
                    let null_col = arrow_array::new_null_array(field.data_type(), num_rows);
                    columns.push(null_col);
                    if columns.len() >= input.num_columns() + 1 + target_properties.len() {
                        break;
                    }
                }
            }
        }
    }

    // Add edge ID column and properties if edge is bound
    if edge_variable.is_some() {
        let eids: Vec<Eid> = expansions
            .iter()
            .map(|(_, _, eid)| Eid::from(*eid))
            .collect();
        let eid_u64s: Vec<u64> = eids.iter().map(|e| e.as_u64()).collect();
        columns.push(Arc::new(UInt64Array::from(eid_u64s)));

        if !edge_properties.is_empty() {
            let prop_name_refs: Vec<&str> = edge_properties.iter().map(|s| s.as_str()).collect();
            let property_manager = graph_ctx.property_manager();
            let query_ctx = graph_ctx.query_context();

            let props_map = property_manager
                .get_batch_edge_props(&eids, &prop_name_refs, Some(&query_ctx))
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            let uni_schema = graph_ctx.storage().schema_manager().schema();
            let edge_type_props = edge_type_ids
                .first()
                .and_then(|&id| uni_schema.edge_type_name_by_id(id))
                .and_then(|name| uni_schema.properties.get(name));

            // Use Vid::from(eid) as key — matches PropertyManager's return format
            let vid_keys: Vec<Vid> = eids.iter().map(|e| Vid::from(e.as_u64())).collect();

            for prop_name in &edge_properties {
                let data_type = resolve_edge_property_type(prop_name, edge_type_props);
                let column =
                    build_property_column_static(&vid_keys, &props_map, prop_name, &data_type)?;
                columns.push(column);
            }
        }
    }

    let expanded_batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

    if optional {
        // Identify source rows that had no expansions and append null rows for them
        let expanded_indices: HashSet<usize> = expansions.iter().map(|(idx, _, _)| *idx).collect();
        let unmatched: Vec<usize> = (0..input.num_rows())
            .filter(|idx| !expanded_indices.contains(idx))
            .collect();

        if !unmatched.is_empty() {
            let null_batch = build_optional_null_batch_for_rows(&input, &unmatched, &schema)?;
            let combined = arrow::compute::concat_batches(&schema, [&expanded_batch, &null_batch])
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            return Ok(combined);
        }
    }

    Ok(expanded_batch)
}

/// Build a batch where all input rows are preserved with NULL target/edge columns.
/// Used when OPTIONAL MATCH finds no expansions for the entire batch.
fn build_optional_null_batch(input: &RecordBatch, schema: &SchemaRef) -> DFResult<RecordBatch> {
    let num_rows = input.num_rows();
    let mut columns: Vec<ArrayRef> = input.columns().to_vec();
    // Fill remaining columns with nulls matching schema field types
    for field in schema.fields().iter().skip(input.num_columns()) {
        columns.push(arrow_array::new_null_array(field.data_type(), num_rows));
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
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
                                    self.optional,
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
    expansions: &[(usize, Vid, u64)],
    schema: &SchemaRef,
    edge_variable: Option<&String>,
    optional: bool,
) -> DFResult<RecordBatch> {
    if expansions.is_empty() {
        if !optional {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        return build_optional_null_batch(input, schema);
    }

    let indices: Vec<u64> = expansions.iter().map(|(idx, _, _)| *idx as u64).collect();
    let indices_array = UInt64Array::from(indices);

    let mut columns: Vec<ArrayRef> = Vec::new();
    for col in input.columns() {
        let expanded = take(col.as_ref(), &indices_array, None)?;
        columns.push(expanded);
    }

    // Add target VID column
    let target_vids: Vec<u64> = expansions.iter().map(|(_, vid, _)| vid.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(target_vids)));

    // Add edge ID column if edge is bound (no properties in sync path)
    if edge_variable.is_some() {
        let edge_ids: Vec<u64> = expansions.iter().map(|(_, _, eid)| *eid).collect();
        columns.push(Arc::new(UInt64Array::from(edge_ids)));
    }

    let expanded_batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

    if optional {
        let expanded_indices: HashSet<usize> = expansions.iter().map(|(idx, _, _)| *idx).collect();
        let unmatched: Vec<usize> = (0..input.num_rows())
            .filter(|idx| !expanded_indices.contains(idx))
            .collect();

        if !unmatched.is_empty() {
            let null_batch = build_optional_null_batch_for_rows(input, &unmatched, schema)?;
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

    /// Edge type ID to traverse.
    edge_type_id: u16,

    /// Traversal direction.
    direction: Direction,

    /// Minimum number of hops.
    min_hops: usize,

    /// Maximum number of hops.
    max_hops: usize,

    /// Variable name for path (if path is bound).
    path_variable: Option<String>,

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
            .field("edge_type_id", &self.edge_type_id)
            .field("direction", &self.direction)
            .field("min_hops", &self.min_hops)
            .field("max_hops", &self.max_hops)
            .finish()
    }
}

impl GraphVariableLengthTraverseExec {
    /// Create a new variable-length traversal plan.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        source_column: impl Into<String>,
        edge_type_id: u16,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
        path_variable: Option<String>,
        graph_ctx: Arc<GraphExecutionContext>,
    ) -> Self {
        let source_column = source_column.into();

        // Build output schema
        let schema = Self::build_schema(input.schema(), path_variable.as_deref());

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            input,
            source_column,
            edge_type_id,
            direction,
            min_hops,
            max_hops,
            path_variable,
            graph_ctx,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema.
    fn build_schema(input_schema: SchemaRef, path_variable: Option<&str>) -> SchemaRef {
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();

        // Add target VID column
        fields.push(Field::new("_target_vid", DataType::UInt64, false));

        // Add hop count
        fields.push(Field::new("_hop_count", DataType::UInt64, false));

        // Add path column if path variable is bound
        if let Some(path_var) = path_variable {
            // Path nodes/rels stored as List<Struct<{_id: Utf8}>> for downstream hydration
            let node_item = Field::new(
                "item",
                DataType::Struct(vec![Field::new("_id", DataType::Utf8, false)].into()),
                true,
            );
            let rel_item = Field::new(
                "item",
                DataType::Struct(vec![Field::new("_id", DataType::Utf8, false)].into()),
                true,
            );
            let nodes_field = Field::new("nodes", DataType::List(Arc::new(node_item)), false);
            let rels_field = Field::new("relationships", DataType::List(Arc::new(rel_item)), false);
            // Use the path variable name as the column name
            fields.push(Field::new(
                path_var,
                DataType::Struct(vec![nodes_field, rels_field].into()),
                false,
            ));
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
                    "GraphVariableLengthTraverseExec: {} --[{}*{}..{}]--> target",
                    self.source_column, self.edge_type_id, self.min_hops, self.max_hops
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
            self.edge_type_id,
            self.direction,
            self.min_hops,
            self.max_hops,
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

        let warm_fut = self
            .graph_ctx
            .warming_future(vec![self.edge_type_id], self.direction);

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
            edge_type_id: self.edge_type_id,
            direction: self.direction,
            min_hops: self.min_hops,
            max_hops: self.max_hops,
            path_variable: self.path_variable.clone(),
            graph_ctx: self.graph_ctx.clone(),
        }
    }
}

/// Data needed by the stream (without ExecutionPlan overhead).
struct GraphVariableLengthTraverseExecData {
    source_column: String,
    edge_type_id: u16,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
    path_variable: Option<String>,
    graph_ctx: Arc<GraphExecutionContext>,
}

impl GraphVariableLengthTraverseExecData {
    /// Perform BFS from a source vertex.
    fn bfs(&self, source: Vid) -> Vec<BfsResult> {
        let mut results = Vec::new();
        let mut visited: HashSet<Vid> = HashSet::new();
        let mut queue: VecDeque<BfsResult> = VecDeque::new();

        visited.insert(source);
        queue.push_back((source, 0, vec![source], vec![]));

        while let Some((current, depth, node_path, edge_path)) = queue.pop_front() {
            // Emit result if within hop range
            if depth >= self.min_hops && depth <= self.max_hops && depth > 0 {
                results.push((current, depth, node_path.clone(), edge_path.clone()));
            }

            // Stop if at max depth
            if depth >= self.max_hops {
                continue;
            }

            // Get neighbors
            let neighbors =
                self.graph_ctx
                    .get_neighbors(current, self.edge_type_id, self.direction);

            for (neighbor, eid) in neighbors {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    let mut new_node_path = node_path.clone();
                    new_node_path.push(neighbor);
                    let mut new_edge_path = edge_path.clone();
                    new_edge_path.push(eid);
                    queue.push_back((neighbor, depth + 1, new_node_path, new_edge_path));
                }
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
                            let result = self.process_batch(batch);
                            self.state = VarLengthStreamState::Reading;
                            return Poll::Ready(Some(result));
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
                VarLengthStreamState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl GraphVariableLengthTraverseStream {
    fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let source_col = batch
            .column_by_name(&self.exec.source_column)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Source column '{}' not found",
                    self.exec.source_column
                ))
            })?;

        let source_vids = source_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "Source column is not UInt64".to_string(),
                )
            })?;

        // Collect all BFS results
        let mut expansions: Vec<VarLengthExpansion> = Vec::new();

        for (row_idx, source_vid) in source_vids.iter().enumerate() {
            let Some(src) = source_vid else {
                continue;
            };

            let vid = Vid::from(src);
            let bfs_results = self.exec.bfs(vid);

            for (target, hop_count, node_path, edge_path) in bfs_results {
                expansions.push((row_idx, target, hop_count, node_path, edge_path));
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

        // Add target VID column
        let target_vids: Vec<u64> = expansions
            .iter()
            .map(|(_, vid, _, _, _)| vid.as_u64())
            .collect();
        columns.push(Arc::new(UInt64Array::from(target_vids)));

        // Add hop count column
        let hop_counts: Vec<u64> = expansions
            .iter()
            .map(|(_, _, hops, _, _)| *hops as u64)
            .collect();
        columns.push(Arc::new(UInt64Array::from(hop_counts)));

        // Add path column if bound
        if self.exec.path_variable.is_some() {
            use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder};

            let id_field = Arc::new(Field::new("_id", DataType::Utf8, false));
            let node_item_field = Arc::new(Field::new(
                "item",
                DataType::Struct(vec![id_field.clone()].into()),
                true,
            ));
            let rel_item_field = Arc::new(Field::new(
                "item",
                DataType::Struct(vec![id_field.clone()].into()),
                true,
            ));

            let nodes_list_field = Arc::new(Field::new(
                "nodes",
                DataType::List(node_item_field.clone()),
                false,
            ));
            let rels_list_field = Arc::new(Field::new(
                "relationships",
                DataType::List(rel_item_field.clone()),
                false,
            ));

            // Build ListBuilder<StructBuilder> for nodes and rels
            let make_list_struct_builder = || {
                ListBuilder::new(StructBuilder::new(
                    vec![id_field.clone()],
                    vec![Box::new(StringBuilder::new())],
                ))
            };

            let mut nodes_builder = make_list_struct_builder();
            let mut rels_builder = make_list_struct_builder();

            for (_, _, _, node_path, edge_path) in expansions {
                // Append nodes list
                let node_struct = nodes_builder.values();
                for vid in node_path {
                    node_struct
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(vid.as_u64().to_string());
                    node_struct.append(true);
                }
                nodes_builder.append(true);

                // Append relationships list
                let rel_struct = rels_builder.values();
                for eid in edge_path {
                    rel_struct
                        .field_builder::<StringBuilder>(0)
                        .unwrap()
                        .append_value(eid.as_u64().to_string());
                    rel_struct.append(true);
                }
                rels_builder.append(true);
            }

            let nodes_arr = nodes_builder.finish();
            let rels_arr = rels_builder.finish();

            let path_struct = arrow_array::StructArray::from(vec![
                (nodes_list_field, Arc::new(nodes_arr) as ArrayRef),
                (rels_list_field, Arc::new(rels_arr) as ArrayRef),
            ]);
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

        assert_eq!(output_schema.fields().len(), 2);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
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

        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "r._eid");
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

        // a._vid, m._vid, m.name, m.age, r._eid
        assert_eq!(output_schema.fields().len(), 5);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "m._vid");
        assert_eq!(output_schema.field(2).name(), "m.name");
        assert_eq!(output_schema.field(3).name(), "m.age");
        assert_eq!(output_schema.field(4).name(), "r._eid");
    }

    #[test]
    fn test_variable_length_schema() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "a._vid",
            DataType::UInt64,
            false,
        )]));

        let output_schema = GraphVariableLengthTraverseExec::build_schema(input_schema, Some("p"));

        assert_eq!(output_schema.fields().len(), 4);
        assert_eq!(output_schema.field(0).name(), "a._vid");
        assert_eq!(output_schema.field(1).name(), "_target_vid");
        assert_eq!(output_schema.field(2).name(), "_hop_count");
        assert_eq!(output_schema.field(3).name(), "p");
    }
}
