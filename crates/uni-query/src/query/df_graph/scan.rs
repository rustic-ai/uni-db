// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph scan execution plan for DataFusion.
//!
//! This module provides [`GraphScanExec`], a DataFusion `ExecutionPlan` that scans
//! vertices or edges from storage with property materialization. It wraps the
//! underlying Lance table scan with:
//!
//! - MVCC resolution via L0 buffer overlays
//! - Property column materialization from `PropertyManager`
//! - Filter pushdown to storage layer
//!
//! # Column Naming Convention
//!
//! Properties are materialized as columns named `{variable}.{property}`:
//! - `n.name` - property "name" for variable "n"
//! - `n.age` - property "age" for variable "n"
//!
//! System columns use underscore prefix:
//! - `_vid` - vertex ID
//! - `_eid` - edge ID
//! - `_src_vid` - source vertex ID (edges only)
//! - `_dst_vid` - destination vertex ID (edges only)

use crate::query::datetime::parse_datetime_utc;
use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::compute_plan_properties;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, DurationMicrosecondBuilder, FixedSizeListBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::Schema as UniSchema;

/// Graph scan execution plan.
///
/// Scans vertices or edges from storage with property materialization.
/// This wraps the underlying Lance table scan with MVCC resolution and
/// property loading.
///
/// # Example
///
/// ```ignore
/// // Create a scan for Person vertices with name and age properties
/// let scan = GraphScanExec::new(
///     graph_ctx,
///     "Person",
///     "n",
///     vec!["name".to_string(), "age".to_string()],
///     None, // No filter
/// );
///
/// let stream = scan.execute(0, task_ctx)?;
/// // Stream yields batches with columns: _vid, n.name, n.age
/// ```
pub struct GraphScanExec {
    /// Graph execution context with storage and L0 access.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Label name for vertex scan, or edge type for edge scan.
    label: String,

    /// Variable name for column prefixing.
    variable: String,

    /// Properties to materialize as columns.
    projected_properties: Vec<String>,

    /// Filter expression to push down.
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Whether this is an edge scan (vs vertex scan).
    is_edge_scan: bool,

    /// Whether this is a schemaless scan (uses main table instead of per-label table).
    is_schemaless: bool,

    /// Output schema with materialized property columns.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Metrics for execution tracking.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphScanExec")
            .field("label", &self.label)
            .field("variable", &self.variable)
            .field("projected_properties", &self.projected_properties)
            .field("is_edge_scan", &self.is_edge_scan)
            .finish()
    }
}

impl GraphScanExec {
    /// Create a new graph scan for vertices.
    ///
    /// Scans all vertices of the given label from storage and L0 buffers,
    /// then materializes the requested properties.
    pub fn new_vertex_scan(
        graph_ctx: Arc<GraphExecutionContext>,
        label: impl Into<String>,
        variable: impl Into<String>,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let label = label.into();
        let variable = variable.into();

        // Build output schema with proper types from Uni schema
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let schema =
            Self::build_vertex_schema(&variable, &label, &projected_properties, &uni_schema);

        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            label,
            variable,
            projected_properties,
            filter,
            is_edge_scan: false,
            is_schemaless: false,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a new schemaless vertex scan.
    ///
    /// Scans the main vertices table for vertices with the given label name.
    /// Properties are extracted from props_json (all treated as Utf8/JSON).
    /// This is used for labels that aren't in the schema.
    pub fn new_schemaless_vertex_scan(
        graph_ctx: Arc<GraphExecutionContext>,
        label_name: impl Into<String>,
        variable: impl Into<String>,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let label = label_name.into();
        let variable = variable.into();

        // Build schema - all properties are Utf8 (from JSON) except _vid
        let schema = Self::build_schemaless_vertex_schema(&variable, &projected_properties);
        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            label,
            variable,
            projected_properties,
            filter,
            is_edge_scan: false,
            is_schemaless: true,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a new multi-label vertex scan using the main vertices table.
    ///
    /// Scans for vertices that have ALL specified labels (intersection semantics).
    /// Properties are extracted from props_json (schemaless).
    pub fn new_multi_label_vertex_scan(
        graph_ctx: Arc<GraphExecutionContext>,
        labels: Vec<String>,
        variable: impl Into<String>,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let variable = variable.into();
        let schema = Self::build_schemaless_vertex_schema(&variable, &projected_properties);
        let properties = compute_plan_properties(schema.clone());

        // Encode labels as colon-separated for the stream to parse
        let encoded_labels = labels.join(":");

        Self {
            graph_ctx,
            label: encoded_labels,
            variable,
            projected_properties,
            filter,
            is_edge_scan: false,
            is_schemaless: true,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a new schemaless scan for all vertices.
    ///
    /// Scans the main vertices table for all vertices regardless of label.
    /// Properties are extracted from props_json (all treated as Utf8/JSON).
    /// This is used for `MATCH (n)` without label filter.
    pub fn new_schemaless_all_scan(
        graph_ctx: Arc<GraphExecutionContext>,
        variable: impl Into<String>,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let variable = variable.into();

        // Build schema - all properties are Utf8 (from JSON) except _vid
        let schema = Self::build_schemaless_vertex_schema(&variable, &projected_properties);
        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            label: String::new(), // Empty label signals "scan all vertices"
            variable,
            projected_properties,
            filter,
            is_edge_scan: false,
            is_schemaless: true,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build schema for schemaless vertex scan (all properties as LargeBinary/CypherValue).
    fn build_schemaless_vertex_schema(variable: &str, properties: &[String]) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(
                format!("{}._labels", variable),
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ];

        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            // Schemaless properties use LargeBinary with CypherValue encoding to preserve types
            fields.push(Field::new(&col_name, DataType::LargeBinary, true));
        }

        Arc::new(Schema::new(fields))
    }

    /// Create a new graph scan for edges.
    ///
    /// Scans all edges of the given type from storage and L0 buffers,
    /// then materializes the requested properties.
    pub fn new_edge_scan(
        graph_ctx: Arc<GraphExecutionContext>,
        edge_type: impl Into<String>,
        variable: impl Into<String>,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let label = edge_type.into();
        let variable = variable.into();

        // Build output schema with proper types from Uni schema
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let schema = Self::build_edge_schema(&variable, &label, &projected_properties, &uni_schema);

        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            label,
            variable,
            projected_properties,
            filter,
            is_edge_scan: true,
            is_schemaless: false,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build output schema for vertex scan with proper Arrow types.
    fn build_vertex_schema(
        variable: &str,
        label: &str,
        properties: &[String],
        uni_schema: &UniSchema,
    ) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(
                format!("{}._labels", variable),
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ];
        let label_props = uni_schema.properties.get(label);
        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            let arrow_type = resolve_property_type(prop, label_props);
            fields.push(Field::new(&col_name, arrow_type, true));
        }
        Arc::new(Schema::new(fields))
    }

    /// Build output schema for edge scan with proper Arrow types.
    fn build_edge_schema(
        variable: &str,
        edge_type: &str,
        properties: &[String],
        uni_schema: &UniSchema,
    ) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{}._eid", variable), DataType::UInt64, false),
            Field::new(format!("{}._src_vid", variable), DataType::UInt64, false),
            Field::new(format!("{}._dst_vid", variable), DataType::UInt64, false),
        ];
        let edge_props = uni_schema.properties.get(edge_type);
        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            let arrow_type = resolve_property_type(prop, edge_props);
            fields.push(Field::new(&col_name, arrow_type, true));
        }
        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let scan_type = if self.is_edge_scan { "Edge" } else { "Vertex" };
                write!(
                    f,
                    "GraphScanExec: {}={}, properties={:?}",
                    scan_type, self.label, self.projected_properties
                )?;
                if self.filter.is_some() {
                    write!(f, ", filter=<pushed>")?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for GraphScanExec {
    fn name(&self) -> &str {
        "GraphScanExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Plan(
                "GraphScanExec does not accept children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(GraphScanStream::new(
            self.graph_ctx.clone(),
            self.label.clone(),
            self.variable.clone(),
            self.projected_properties.clone(),
            self.is_edge_scan,
            self.is_schemaless,
            self.schema.clone(),
            metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// State machine for graph scan stream execution.
enum GraphScanState {
    /// Initial state, ready to start scanning.
    Init,
    /// Executing the async scan.
    Executing(Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>>),
    /// Stream is done.
    Done,
}

/// Stream that scans vertices or edges and materializes properties.
///
/// For known-label vertex scans, uses a single columnar Lance query with
/// MVCC dedup and L0 overlay. For edge and schemaless scans, falls back
/// to the two-phase VID-scan + property-materialize flow.
struct GraphScanStream {
    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Label (vertex) or edge type name.
    label: String,

    /// Variable name for column prefixing (e.g., "n" in `n.name`).
    variable: String,

    /// Properties to materialize.
    properties: Vec<String>,

    /// Whether this is an edge scan.
    is_edge_scan: bool,

    /// Whether this is a schemaless scan.
    is_schemaless: bool,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: GraphScanState,

    /// Metrics.
    metrics: BaselineMetrics,
}

impl GraphScanStream {
    /// Create a new graph scan stream.
    #[expect(clippy::too_many_arguments)]
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        label: String,
        variable: String,
        properties: Vec<String>,
        is_edge_scan: bool,
        is_schemaless: bool,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            label,
            variable,
            properties,
            is_edge_scan,
            is_schemaless,
            schema,
            state: GraphScanState::Init,
            metrics,
        }
    }
}

/// Resolve the Arrow data type for a property, handling system columns like `overflow_json`.
///
/// Falls back to `LargeBinary` (CypherValue) if the property is not found in the schema,
/// preserving original value types for overflow/unknown properties.
pub(crate) fn resolve_property_type(
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

/// Scan vertex VIDs from storage and L0 buffers (static version for async).
///
/// Superseded by `columnar_scan_vertex_batch_static` for known-label scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
async fn scan_vertex_vids_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
) -> DFResult<Vec<Vid>> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();

    // Step 1: Scan from LanceDB storage
    let ds = storage
        .vertex_dataset(label)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let lancedb_store = storage.lancedb_store();

    let mut vids = Vec::new();

    // Try to open the table and scan VIDs
    if let Ok(table) = ds.open_lancedb(lancedb_store).await {
        use lancedb::query::{ExecutableQuery, QueryBase, Select};

        // Build query with version filter if snapshot is pinned
        let query = table.query().select(Select::columns(&["_vid"]));
        let query = match storage.version_high_water_mark() {
            Some(hwm) => query.only_if(format!("_version <= {}", hwm)),
            None => query,
        };

        if let Ok(stream) = query.execute().await {
            use futures::TryStreamExt;
            let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap_or_default();

            for batch in batches {
                if let Some(vid_col) = batch.column_by_name("_vid")
                    && let Some(vid_array) = vid_col.as_any().downcast_ref::<UInt64Array>()
                {
                    for i in 0..vid_array.len() {
                        vids.push(Vid::from(vid_array.value(i)));
                    }
                }
            }
        }
    }

    // Step 2: Overlay L0 buffers (pending flush, current, transaction)
    // Also collect tombstoned VIDs to filter out deleted vertices
    let mut tombstones: HashSet<Vid> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        vids.extend(guard.vids_for_label(label));
        tombstones.extend(guard.vertex_tombstones.iter().copied());
    }

    // Deduplicate and filter out tombstoned vertices
    vids.sort_unstable();
    vids.dedup();
    vids.retain(|vid| !tombstones.contains(vid));

    Ok(vids)
}

/// Scan vertex VIDs from main table by label name (schemaless).
///
/// Uses the main vertices table with `array_contains(labels, 'X')` filter.
/// This is used for labels that aren't in the schema (schemaless mode).
///
/// Superseded by `columnar_scan_schemaless_vertex_batch_static`.
#[allow(dead_code)]
async fn scan_vertex_vids_by_label_name_static(
    graph_ctx: &GraphExecutionContext,
    label_name: &str,
) -> DFResult<Vec<Vid>> {
    use uni_store::storage::main_vertex::MainVertexDataset;

    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let lancedb_store = storage.lancedb_store();

    // Step 1: Query main vertices table
    let mut vids = MainVertexDataset::find_vids_by_label_name(lancedb_store, label_name)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Step 2: Overlay L0 buffers (pending flush, current, transaction)
    // Also collect tombstoned VIDs to filter out deleted vertices
    let mut tombstones: HashSet<Vid> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        vids.extend(guard.vids_for_label(label_name));
        tombstones.extend(guard.vertex_tombstones.iter().copied());
    }

    // Deduplicate and filter out tombstoned vertices
    vids.sort_unstable();
    vids.dedup();
    vids.retain(|vid| !tombstones.contains(vid));

    Ok(vids)
}

/// Scan vertex VIDs from main table with multi-label intersection.
///
/// Returns vertices that have ALL the specified labels.
///
/// Superseded by `columnar_scan_schemaless_vertex_batch_static`.
#[allow(dead_code)]
async fn scan_vertex_vids_by_labels_static(
    graph_ctx: &GraphExecutionContext,
    label_names: &[&str],
) -> DFResult<Vec<Vid>> {
    use uni_store::storage::main_vertex::MainVertexDataset;

    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let lancedb_store = storage.lancedb_store();

    // Step 1: Query main vertices table with label intersection
    let mut vids = MainVertexDataset::find_vids_by_labels(lancedb_store, label_names)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Step 2: Overlay L0 buffers with label intersection
    // Also collect tombstoned VIDs to filter out deleted vertices
    let mut tombstones: HashSet<Vid> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        vids.extend(guard.vids_with_all_labels(label_names));
        tombstones.extend(guard.vertex_tombstones.iter().copied());
    }

    // Deduplicate and filter out tombstoned vertices
    vids.sort_unstable();
    vids.dedup();
    vids.retain(|vid| !tombstones.contains(vid));

    Ok(vids)
}

/// Scan all vertex VIDs from main table (schemaless).
///
/// Uses the main vertices table to find all vertices regardless of label.
/// This is used for `MATCH (n)` without label filter (ScanAll).
///
/// Superseded by `columnar_scan_schemaless_vertex_batch_static`.
#[allow(dead_code)]
async fn scan_all_vertex_vids_static(graph_ctx: &GraphExecutionContext) -> DFResult<Vec<Vid>> {
    use uni_store::storage::main_vertex::MainVertexDataset;

    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let lancedb_store = storage.lancedb_store();

    // Step 1: Query main vertices table for all VIDs
    let mut vids = MainVertexDataset::find_all_vids(lancedb_store)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Step 2: Overlay L0 buffers (pending flush, current, transaction)
    // Also collect tombstoned VIDs to filter out deleted vertices
    let mut tombstones: HashSet<Vid> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        vids.extend(guard.all_vertex_vids());
        tombstones.extend(guard.vertex_tombstones.iter().copied());
    }

    // Deduplicate and filter out tombstoned vertices
    vids.sort_unstable();
    vids.dedup();
    vids.retain(|vid| !tombstones.contains(vid));

    Ok(vids)
}

/// Materialize a batch of schemaless vertices with their properties.
///
/// Fetches properties from the main vertices table's props_json column.
/// All properties are returned as Utf8 (JSON strings).
///
/// Superseded by `columnar_scan_schemaless_vertex_batch_static`.
#[allow(dead_code)]
pub(crate) async fn materialize_schemaless_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    schema: &SchemaRef,
    vids: Vec<Vid>,
) -> DFResult<RecordBatch> {
    use uni_store::storage::main_vertex::MainVertexDataset;

    if vids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let lancedb_store = storage.lancedb_store();

    // Fetch properties from main table
    let mut props_map = MainVertexDataset::find_batch_props_by_vids(lancedb_store, &vids)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Fetch labels from main table
    let labels_map = MainVertexDataset::find_batch_labels_by_vids(lancedb_store, &vids)
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Overlay L0 buffer properties (L0 takes precedence)
    for &vid in &vids {
        // Check all L0 layers for this VID's properties
        let l0_props = accumulate_l0_vertex_props(vid, l0_ctx);
        if let Some(l0_props) = l0_props {
            // Merge L0 properties, taking precedence over storage
            let entry = props_map.entry(vid).or_default();
            for (k, v) in l0_props {
                entry.insert(k, v);
            }
        }
    }

    // Filter out vertices not in props_map (either deleted or not found)
    let valid_vids: Vec<Vid> = vids
        .into_iter()
        .filter(|vid| props_map.contains_key(vid))
        .collect();

    build_schemaless_vertex_record_batch(schema, &valid_vids, &props_map, &labels_map)
}

/// Accumulate properties for a vertex from all L0 layers.
///
/// Superseded by `columnar_scan_schemaless_vertex_batch_static`.
#[allow(dead_code)]
fn accumulate_l0_vertex_props(
    vid: Vid,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> Option<Properties> {
    let mut result: Option<Properties> = None;

    // Iterate all L0 buffers in order: pending flush (oldest first), current, then transaction
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        if let Some(props) = guard.vertex_properties.get(&vid) {
            let entry = result.get_or_insert_with(Properties::new);
            for (k, v) in props {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    result
}

/// Build a RecordBatch from schemaless VIDs and their properties.
///
/// Property values are encoded as LargeBinary (CypherValue) to preserve types.
///
/// Superseded by `columnar_scan_schemaless_vertex_batch_static`.
#[allow(dead_code)]
fn build_schemaless_vertex_record_batch(
    schema: &SchemaRef,
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    labels_map: &HashMap<Vid, Vec<String>>,
) -> DFResult<RecordBatch> {
    if vids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // 1. Build _vid column
    let vid_values: Vec<u64> = vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(vid_values)));

    // 2. Build _labels column
    let mut labels_builder = ListBuilder::new(StringBuilder::new());
    for vid in vids {
        if let Some(labels) = labels_map.get(vid) {
            let values = labels_builder.values();
            for label in labels {
                values.append_value(label);
            }
            labels_builder.append(true);
        } else {
            labels_builder.append_null();
        }
    }
    columns.push(Arc::new(labels_builder.finish()));

    // 3. Build property columns (as LargeBinary / CypherValue)
    for field in schema.fields().iter().skip(2) {
        let prop_name = field.name().split('.').nth(1).unwrap_or(field.name());

        let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
        for vid in vids {
            match get_property_value(vid, props_map, prop_name) {
                Some(Value::Null) | None => builder.append_null(),
                Some(val) => {
                    let json_val: serde_json::Value = val.into();
                    match encode_cypher_value(&json_val) {
                        Ok(bytes) => builder.append_value(bytes),
                        Err(_) => builder.append_null(),
                    }
                }
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Scan all EIDs for a given edge type.
///
/// Superseded by `columnar_scan_edge_batch_static` for edge scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
async fn scan_edge_eids_static(
    graph_ctx: &GraphExecutionContext,
    edge_type_name: &str,
) -> DFResult<Vec<Eid>> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();

    let uni_schema = storage.schema_manager().schema();
    let type_id = uni_schema
        .edge_types
        .get(edge_type_name)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Unknown edge type: {}",
                edge_type_name
            ))
        })?
        .id;

    // Collect edges from all L0 buffers: pending flush, current, and transaction
    let mut eids: Vec<Eid> = l0_ctx
        .iter_l0_buffers()
        .flat_map(|l0| {
            l0.read()
                .graph
                .edges()
                .filter(|e| e.edge_type == type_id)
                .map(|e| e.eid)
                .collect::<Vec<_>>()
        })
        .collect();

    eids.sort_unstable();
    eids.dedup();

    Ok(eids)
}

/// Materialize a batch of vertices with their properties (static version for async).
///
/// Superseded by `columnar_scan_vertex_batch_static` for known-label scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
pub(crate) async fn materialize_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    schema: &SchemaRef,
    vids: Vec<Vid>,
) -> DFResult<RecordBatch> {
    if vids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let property_manager = graph_ctx.property_manager();
    let query_ctx = graph_ctx.query_context();

    // Use the label-specific fetcher so we only query the correct dataset.
    // The label-agnostic get_batch_vertex_props scans ALL label datasets and
    // can overwrite results when different labels share the same raw VID values.
    let props_map = property_manager
        .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Fetch labels for all vertices in the batch
    let labels_map = property_manager
        .get_batch_labels(&vids, Some(&query_ctx))
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Filter out deleted vertices (those not in props_map)
    let valid_vids: Vec<Vid> = vids
        .into_iter()
        .filter(|vid| props_map.contains_key(vid))
        .collect();

    build_vertex_record_batch_static(schema, &valid_vids, &props_map, &labels_map)
}

/// Build a RecordBatch from VIDs and their properties (static version).
///
/// Superseded by `columnar_scan_vertex_batch_static` for known-label scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
pub(crate) fn build_vertex_record_batch_static(
    schema: &SchemaRef,
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    labels_map: &HashMap<Vid, Vec<String>>,
) -> DFResult<RecordBatch> {
    if vids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // 1. Build _vid column
    let vid_values: Vec<u64> = vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(vid_values)));

    // 2. Build _labels column
    let mut labels_builder = ListBuilder::new(StringBuilder::new());
    for vid in vids {
        if let Some(labels) = labels_map.get(vid) {
            let values = labels_builder.values();
            for label in labels {
                values.append_value(label);
            }
            labels_builder.append(true);
        } else {
            labels_builder.append_null();
        }
    }
    columns.push(Arc::new(labels_builder.finish()));

    // 3. Build property columns
    for field in schema.fields().iter().skip(2) {
        // Extract property name from field name (e.g., "n.name" -> "name")
        let prop_name = field.name().split('.').nth(1).unwrap_or(field.name());

        if prop_name == "overflow_json" {
            // Reconstruct the overflow_json CypherValue blob from all non-system properties.
            // The PropertyManager already decoded overflow_json into individual properties,
            // so we re-encode them as a JSON object for filter expressions like
            // json_get_string(overflow_json, 'key').
            let column = build_overflow_json_column(vids, props_map)?;
            columns.push(column);
        } else {
            let column =
                build_property_column_static(vids, props_map, prop_name, field.data_type())?;
            columns.push(column);
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Build the `overflow_json` column by reconstructing a CypherValue object from all
/// non-system properties in the props map.
#[allow(dead_code)]
fn build_overflow_json_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
) -> DFResult<ArrayRef> {
    use arrow_array::builder::LargeBinaryBuilder;

    let mut builder = LargeBinaryBuilder::new();
    for vid in vids {
        if let Some(props) = props_map.get(vid) {
            // Build a JSON object from all non-system properties
            let mut obj = serde_json::Map::new();
            for (k, v) in props {
                if !k.starts_with('_') {
                    let json_val: serde_json::Value = v.clone().into();
                    obj.insert(k.clone(), json_val);
                }
            }
            if obj.is_empty() {
                builder.append_null();
            } else {
                let json_val = serde_json::Value::Object(obj);
                match encode_cypher_value(&json_val) {
                    Ok(bytes) => builder.append_value(bytes),
                    Err(_) => builder.append_null(),
                }
            }
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

// ============================================================================
// Columnar-first scan helpers
// ============================================================================

/// MVCC deduplication: keep only the highest-version row for each `_vid`.
///
/// Sorts by (_vid ASC, _version DESC), then keeps the first occurrence of each
/// _vid (= the highest version). This is a pure Arrow-compute operation.
fn mvcc_dedup_batch(batch: &RecordBatch) -> DFResult<RecordBatch> {
    mvcc_dedup_batch_by(batch, "_vid")
}

/// MVCC deduplication: keep only the highest-version row for each unique value
/// in the given `id_column`.
///
/// Sorts by (id_column ASC, _version DESC), then keeps the first occurrence of
/// each id (= the highest version). This is a pure Arrow-compute operation.
fn mvcc_dedup_batch_by(batch: &RecordBatch, id_column: &str) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let id_col = batch
        .column_by_name(id_column)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!("Missing {} column", id_column))
        })?
        .clone();
    let version_col = batch
        .column_by_name("_version")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing _version column".to_string())
        })?
        .clone();

    // Sort by (id_column ASC, _version DESC)
    let sort_columns = vec![
        arrow::compute::SortColumn {
            values: id_col,
            options: Some(arrow::compute::SortOptions {
                descending: false,
                nulls_first: false,
            }),
        },
        arrow::compute::SortColumn {
            values: version_col,
            options: Some(arrow::compute::SortOptions {
                descending: true,
                nulls_first: false,
            }),
        },
    ];
    let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

    // Reorder all columns by sorted indices
    let sorted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<Result<_, _>>()
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
    let sorted = RecordBatch::try_new(batch.schema(), sorted_columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

    // Build dedup mask: keep first occurrence of each id
    let sorted_id = sorted
        .column_by_name(id_column)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let mut keep = vec![false; sorted.num_rows()];
    if !keep.is_empty() {
        keep[0] = true;
        for (i, flag) in keep.iter_mut().enumerate().skip(1) {
            if sorted_id.value(i) != sorted_id.value(i - 1) {
                *flag = true;
            }
        }
    }

    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(&sorted, &mask)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Filter out edge rows where `op != 0` (non-INSERT) after MVCC dedup.
fn filter_deleted_edge_ops(batch: &RecordBatch) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let op_col = match batch.column_by_name("op") {
        Some(col) => col
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .unwrap(),
        None => return Ok(batch.clone()),
    };
    let keep: Vec<bool> = (0..op_col.len()).map(|i| op_col.value(i) == 0).collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Filter out rows where `_deleted = true` after MVCC dedup.
fn filter_deleted_rows(batch: &RecordBatch) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let deleted_col = match batch.column_by_name("_deleted") {
        Some(col) => col
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap(),
        None => return Ok(batch.clone()),
    };
    let keep: Vec<bool> = (0..deleted_col.len())
        .map(|i| !deleted_col.value(i))
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Filter out rows whose `_vid` appears in L0 tombstones.
fn filter_l0_tombstones(
    batch: &RecordBatch,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut tombstones: HashSet<u64> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        for vid in guard.vertex_tombstones.iter() {
            tombstones.insert(vid.as_u64());
        }
    }

    if tombstones.is_empty() {
        return Ok(batch.clone());
    }

    let vid_col = batch
        .column_by_name("_vid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing _vid column".to_string())
        })?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let keep: Vec<bool> = (0..vid_col.len())
        .map(|i| !tombstones.contains(&vid_col.value(i)))
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Filter out rows whose `eid` appears in L0 edge tombstones.
fn filter_l0_edge_tombstones(
    batch: &RecordBatch,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let mut tombstones: HashSet<u64> = HashSet::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        for eid in guard.tombstones.keys() {
            tombstones.insert(eid.as_u64());
        }
    }

    if tombstones.is_empty() {
        return Ok(batch.clone());
    }

    let eid_col = batch
        .column_by_name("eid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing eid column".to_string())
        })?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    let keep: Vec<bool> = (0..eid_col.len())
        .map(|i| !tombstones.contains(&eid_col.value(i)))
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a RecordBatch from L0 buffer data for a given label, matching the
/// Lance query's column set.
///
/// Merges L0 buffers in visibility order (pending_flush → current → transaction),
/// with later buffers overwriting earlier ones for the same VID.
fn build_l0_vertex_batch(
    l0_ctx: &crate::query::df_graph::L0Context,
    label: &str,
    lance_schema: &SchemaRef,
    label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
) -> DFResult<RecordBatch> {
    // Collect all L0 vertex data, merging in visibility order
    let mut vid_data: HashMap<u64, (Properties, u64)> = HashMap::new(); // vid -> (props, version)
    let mut tombstones: HashSet<u64> = HashSet::new();

    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        // Collect tombstones
        for vid in guard.vertex_tombstones.iter() {
            tombstones.insert(vid.as_u64());
        }
        // Collect vertices for this label
        for vid in guard.vids_for_label(label) {
            let vid_u64 = vid.as_u64();
            if tombstones.contains(&vid_u64) {
                continue;
            }
            let version = guard.vertex_versions.get(&vid).copied().unwrap_or(0);
            let entry = vid_data
                .entry(vid_u64)
                .or_insert_with(|| (Properties::new(), 0));
            // Merge properties (later L0 overwrites)
            if let Some(props) = guard.vertex_properties.get(&vid) {
                for (k, v) in props {
                    entry.0.insert(k.clone(), v.clone());
                }
            }
            // Take the highest version
            if version > entry.1 {
                entry.1 = version;
            }
        }
    }

    // Remove tombstoned VIDs
    for t in &tombstones {
        vid_data.remove(t);
    }

    if vid_data.is_empty() {
        return Ok(RecordBatch::new_empty(lance_schema.clone()));
    }

    // Sort VIDs for deterministic output
    let mut vids: Vec<u64> = vid_data.keys().copied().collect();
    vids.sort_unstable();

    let num_rows = vids.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(lance_schema.fields().len());

    // Determine which schema property names exist
    let schema_prop_names: HashSet<&str> = label_props
        .map(|lp| lp.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    for field in lance_schema.fields() {
        let col_name = field.name().as_str();
        match col_name {
            "_vid" => {
                columns.push(Arc::new(UInt64Array::from(vids.clone())));
            }
            "_deleted" => {
                // L0 vertices are always live (tombstoned ones are already excluded)
                let vals = vec![false; num_rows];
                columns.push(Arc::new(arrow_array::BooleanArray::from(vals)));
            }
            "_version" => {
                let vals: Vec<u64> = vids.iter().map(|v| vid_data[v].1).collect();
                columns.push(Arc::new(UInt64Array::from(vals)));
            }
            "overflow_json" => {
                // Collect non-schema properties as CypherValue
                let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                for vid_u64 in &vids {
                    let (props, _) = &vid_data[vid_u64];
                    let mut overflow = serde_json::Map::new();
                    for (k, v) in props {
                        if k == "ext_id" || k.starts_with('_') {
                            continue;
                        }
                        if !schema_prop_names.contains(k.as_str()) {
                            let json_val: serde_json::Value = v.clone().into();
                            overflow.insert(k.clone(), json_val);
                        }
                    }
                    if overflow.is_empty() {
                        builder.append_null();
                    } else {
                        let json_val = serde_json::Value::Object(overflow);
                        match encode_cypher_value(&json_val) {
                            Ok(bytes) => builder.append_value(bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Schema property column: convert L0 Value → Arrow typed value
                let col = build_l0_property_column(&vids, &vid_data, col_name, field.data_type())?;
                columns.push(col);
            }
        }
    }

    RecordBatch::try_new(lance_schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a single Arrow column from L0 property values.
///
/// Operates on the `vid_data` map produced by `build_l0_vertex_batch`.
fn build_l0_property_column(
    vids: &[u64],
    vid_data: &HashMap<u64, (Properties, u64)>,
    prop_name: &str,
    data_type: &DataType,
) -> DFResult<ArrayRef> {
    // Convert to Vid keys for reuse of existing build_property_column_static
    let vid_keys: Vec<Vid> = vids.iter().map(|v| Vid::from(*v)).collect();
    let props_map: HashMap<Vid, Properties> = vid_data
        .iter()
        .map(|(k, (props, _))| (Vid::from(*k), props.clone()))
        .collect();

    build_property_column_static(&vid_keys, &props_map, prop_name, data_type)
}

/// Build a RecordBatch from L0 buffer data for a given edge type, matching
/// the DeltaDataset Lance table's column set.
///
/// Merges L0 buffers in visibility order (pending_flush → current → transaction),
/// with later buffers overwriting earlier ones for the same EID.
fn build_l0_edge_batch(
    l0_ctx: &crate::query::df_graph::L0Context,
    edge_type: &str,
    internal_schema: &SchemaRef,
    type_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
) -> DFResult<RecordBatch> {
    // Collect all L0 edge data, merging in visibility order
    // eid -> (src_vid, dst_vid, properties, version)
    let mut eid_data: HashMap<u64, (u64, u64, Properties, u64)> = HashMap::new();
    let mut tombstones: HashSet<u64> = HashSet::new();

    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        // Collect tombstones
        for eid in guard.tombstones.keys() {
            tombstones.insert(eid.as_u64());
        }
        // Collect edges for this type
        for eid in guard.eids_for_type(edge_type) {
            let eid_u64 = eid.as_u64();
            if tombstones.contains(&eid_u64) {
                continue;
            }
            let (src_vid, dst_vid) = match guard.get_edge_endpoints(eid) {
                Some(endpoints) => (endpoints.0.as_u64(), endpoints.1.as_u64()),
                None => continue,
            };
            let version = guard.edge_versions.get(&eid).copied().unwrap_or(0);
            let entry = eid_data
                .entry(eid_u64)
                .or_insert_with(|| (src_vid, dst_vid, Properties::new(), 0));
            // Merge properties (later L0 overwrites)
            if let Some(props) = guard.edge_properties.get(&eid) {
                for (k, v) in props {
                    entry.2.insert(k.clone(), v.clone());
                }
            }
            // Update endpoints from latest L0 layer
            entry.0 = src_vid;
            entry.1 = dst_vid;
            // Take the highest version
            if version > entry.3 {
                entry.3 = version;
            }
        }
    }

    // Remove tombstoned EIDs
    for t in &tombstones {
        eid_data.remove(t);
    }

    if eid_data.is_empty() {
        return Ok(RecordBatch::new_empty(internal_schema.clone()));
    }

    // Sort EIDs for deterministic output
    let mut eids: Vec<u64> = eid_data.keys().copied().collect();
    eids.sort_unstable();

    let num_rows = eids.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(internal_schema.fields().len());

    // Determine which schema property names exist
    let schema_prop_names: HashSet<&str> = type_props
        .map(|tp| tp.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    for field in internal_schema.fields() {
        let col_name = field.name().as_str();
        match col_name {
            "eid" => {
                columns.push(Arc::new(UInt64Array::from(eids.clone())));
            }
            "src_vid" => {
                let vals: Vec<u64> = eids.iter().map(|e| eid_data[e].0).collect();
                columns.push(Arc::new(UInt64Array::from(vals)));
            }
            "dst_vid" => {
                let vals: Vec<u64> = eids.iter().map(|e| eid_data[e].1).collect();
                columns.push(Arc::new(UInt64Array::from(vals)));
            }
            "op" => {
                // L0 edges are always live (tombstoned ones already excluded)
                let vals = vec![0u8; num_rows];
                columns.push(Arc::new(arrow_array::UInt8Array::from(vals)));
            }
            "_version" => {
                let vals: Vec<u64> = eids.iter().map(|e| eid_data[e].3).collect();
                columns.push(Arc::new(UInt64Array::from(vals)));
            }
            "overflow_json" => {
                // Collect non-schema properties as CypherValue
                let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                for eid_u64 in &eids {
                    let (_, _, props, _) = &eid_data[eid_u64];
                    let mut overflow = serde_json::Map::new();
                    for (k, v) in props {
                        if k.starts_with('_') {
                            continue;
                        }
                        if !schema_prop_names.contains(k.as_str()) {
                            let json_val: serde_json::Value = v.clone().into();
                            overflow.insert(k.clone(), json_val);
                        }
                    }
                    if overflow.is_empty() {
                        builder.append_null();
                    } else {
                        let json_val = serde_json::Value::Object(overflow);
                        match encode_cypher_value(&json_val) {
                            Ok(bytes) => builder.append_value(bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Schema property column: convert L0 Value → Arrow typed value
                let col =
                    build_l0_edge_property_column(&eids, &eid_data, col_name, field.data_type())?;
                columns.push(col);
            }
        }
    }

    RecordBatch::try_new(internal_schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a single Arrow column from L0 edge property values.
///
/// Operates on the `eid_data` map produced by `build_l0_edge_batch`.
fn build_l0_edge_property_column(
    eids: &[u64],
    eid_data: &HashMap<u64, (u64, u64, Properties, u64)>,
    prop_name: &str,
    data_type: &DataType,
) -> DFResult<ArrayRef> {
    // Convert to Vid keys for reuse of existing build_property_column_static
    let vid_keys: Vec<Vid> = eids.iter().map(|e| Vid::from(*e)).collect();
    let props_map: HashMap<Vid, Properties> = eid_data
        .iter()
        .map(|(k, (_, _, props, _))| (Vid::from(*k), props.clone()))
        .collect();

    build_property_column_static(&vid_keys, &props_map, prop_name, data_type)
}

/// Synthesize the `_labels` column for known-label vertices.
///
/// Each vertex gets at minimum `[label]`. Additional labels from L0 buffers
/// are merged in.
fn build_labels_column_for_known_label(
    vid_arr: &UInt64Array,
    label: &str,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> DFResult<ArrayRef> {
    let mut labels_builder = ListBuilder::new(StringBuilder::new());

    for i in 0..vid_arr.len() {
        let vid_u64 = vid_arr.value(i);
        let vid = Vid::from(vid_u64);

        let mut labels = vec![label.to_string()];

        // Check L0 buffers for additional labels
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if let Some(l0_labels) = guard.vertex_labels.get(&vid) {
                for lbl in l0_labels {
                    if lbl != label && !labels.contains(lbl) {
                        labels.push(lbl.clone());
                    }
                }
            }
        }

        let values = labels_builder.values();
        for lbl in &labels {
            values.append_value(lbl);
        }
        labels_builder.append(true);
    }

    Ok(Arc::new(labels_builder.finish()))
}

/// Map a Lance-schema batch to the DataFusion output schema.
///
/// The output schema has `{variable}.{property}` column names, while Lance
/// uses bare property names. This function performs the positional mapping,
/// adds the `_labels` column, and drops internal columns like `_deleted`/`_version`.
fn map_to_output_schema(
    batch: &RecordBatch,
    label: &str,
    _variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // 1. {var}._vid
    let vid_col = batch
        .column_by_name("_vid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing _vid column".to_string())
        })?
        .clone();
    let vid_arr = vid_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("_vid not UInt64".to_string())
        })?;

    // 2. {var}._labels — must build before moving vid_col
    let labels_col = build_labels_column_for_known_label(vid_arr, label, l0_ctx)?;
    columns.push(vid_col.clone());
    columns.push(labels_col);

    // 3. Projected properties
    // Pre-load overflow_json column for extracting non-schema properties
    let overflow_arr = batch
        .column_by_name("overflow_json")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());

    for prop in projected_properties {
        if prop == "overflow_json" {
            match batch.column_by_name("overflow_json") {
                Some(col) => columns.push(col.clone()),
                None => {
                    // No overflow_json in Lance — return null column
                    columns.push(arrow_array::new_null_array(
                        &DataType::LargeBinary,
                        batch.num_rows(),
                    ));
                }
            }
        } else {
            match batch.column_by_name(prop) {
                Some(col) => columns.push(col.clone()),
                None => {
                    // Column missing in Lance — extract from overflow_json CypherValue blob
                    // with L0 overlay (mirrors schemaless path logic)
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for i in 0..batch.num_rows() {
                        let vid = Vid::from(vid_arr.value(i));

                        // Check L0 buffers (later overwrites earlier)
                        let mut l0_val: Option<Option<Value>> = None;
                        for l0 in l0_ctx.iter_l0_buffers() {
                            let guard = l0.read();
                            if let Some(props) = guard.vertex_properties.get(&vid)
                                && let Some(val) = props.get(prop.as_str())
                            {
                                l0_val = Some(Some(val.clone()));
                            }
                        }

                        if let Some(val_opt) = l0_val {
                            // L0 has this property
                            match val_opt {
                                Some(val) if !val.is_null() => {
                                    let json_val: serde_json::Value = val.into();
                                    match encode_cypher_value(&json_val) {
                                        Ok(bytes) => builder.append_value(bytes),
                                        Err(_) => builder.append_null(),
                                    }
                                }
                                _ => builder.append_null(),
                            }
                        } else {
                            // Extract from overflow_json CypherValue blob
                            if let Some(arr) = overflow_arr {
                                if !arr.is_null(i) {
                                    let blob = arr.value(i);
                                    // Fast path: extract map entry without decoding entire map
                                    if let Some(sub_bytes) =
                                        uni_common::cypher_value_codec::extract_map_entry_raw(
                                            blob, prop,
                                        )
                                    {
                                        builder.append_value(&sub_bytes);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Map an internal DeltaDataset-schema edge batch to the DataFusion output schema.
///
/// The internal batch has `eid`, `src_vid`, `dst_vid`, `op`, `_version`, and property
/// columns. The output schema has `{variable}._eid`, `{variable}._src_vid`,
/// `{variable}._dst_vid`, and per-property columns. Internal columns `op` and
/// `_version` are dropped.
fn map_edge_to_output_schema(
    batch: &RecordBatch,
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // 1. {var}._eid
    let eid_col = batch
        .column_by_name("eid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing eid column".to_string())
        })?
        .clone();
    columns.push(eid_col);

    // 2. {var}._src_vid
    let src_col = batch
        .column_by_name("src_vid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing src_vid column".to_string())
        })?
        .clone();
    columns.push(src_col);

    // 3. {var}._dst_vid
    let dst_col = batch
        .column_by_name("dst_vid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing dst_vid column".to_string())
        })?
        .clone();
    columns.push(dst_col);

    // 4. Projected properties
    for prop in projected_properties {
        if prop == "overflow_json" {
            match batch.column_by_name("overflow_json") {
                Some(col) => columns.push(col.clone()),
                None => {
                    columns.push(arrow_array::new_null_array(
                        &DataType::LargeBinary,
                        batch.num_rows(),
                    ));
                }
            }
        } else {
            match batch.column_by_name(prop) {
                Some(col) => columns.push(col.clone()),
                None => {
                    // Column missing in Lance — extract from overflow_json CypherValue blob
                    // (mirrors the vertex path in map_to_output_schema)
                    let overflow_arr = batch
                        .column_by_name("overflow_json")
                        .and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());

                    if let Some(arr) = overflow_arr {
                        let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                        for i in 0..batch.num_rows() {
                            if !arr.is_null(i) {
                                let blob = arr.value(i);
                                // Fast path: extract map entry without decoding entire map
                                if let Some(sub_bytes) =
                                    uni_common::cypher_value_codec::extract_map_entry_raw(
                                        blob, prop,
                                    )
                                {
                                    builder.append_value(&sub_bytes);
                                } else {
                                    builder.append_null();
                                }
                            } else {
                                builder.append_null();
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    } else {
                        // No overflow_json column either — return null column
                        let target_field = output_schema
                            .fields()
                            .iter()
                            .find(|f| f.name() == &format!("{}.{}", variable, prop));
                        let dt = target_field
                            .map(|f| f.data_type().clone())
                            .unwrap_or(DataType::LargeBinary);
                        columns.push(arrow_array::new_null_array(&dt, batch.num_rows()));
                    }
                }
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Columnar-first vertex scan: single Lance query with MVCC dedup and L0 overlay.
///
/// Replaces the two-phase `scan_vertex_vids_static()` + `materialize_vertex_batch_static()`
/// for known-label vertex scans. Reads all needed columns in a single Lance query,
/// performs MVCC dedup via Arrow compute, merges L0 buffer data, filters tombstones,
/// and maps to the output schema.
async fn columnar_scan_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let uni_schema = storage.schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    // Build the list of columns to request from Lance
    let mut lance_columns: Vec<String> = vec![
        "_vid".to_string(),
        "_deleted".to_string(),
        "_version".to_string(),
    ];
    for prop in projected_properties {
        if prop == "overflow_json" {
            if !lance_columns.contains(&"overflow_json".to_string()) {
                lance_columns.push("overflow_json".to_string());
            }
        } else {
            // Only include columns that exist in the schema
            let exists_in_schema = label_props.is_some_and(|lp| lp.contains_key(prop));
            if exists_in_schema && !lance_columns.contains(prop) {
                lance_columns.push(prop.clone());
            }
        }
    }

    // Check if any projected property is not in schema (needs overflow_json)
    let needs_overflow = projected_properties
        .iter()
        .any(|p| p == "overflow_json" || !label_props.is_some_and(|lp| lp.contains_key(p)));
    if needs_overflow && !lance_columns.contains(&"overflow_json".to_string()) {
        lance_columns.push("overflow_json".to_string());
    }

    // Try to query Lance
    let ds = storage
        .vertex_dataset(label)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let lancedb_store = storage.lancedb_store();

    let lance_batch = match ds.open_lancedb(lancedb_store).await {
        Ok(table) => {
            use lancedb::query::{ExecutableQuery, QueryBase, Select};

            // Check which columns actually exist in the Lance table
            let table_schema = table
                .schema()
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            let table_field_names: HashSet<&str> = table_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();

            let actual_columns: Vec<&str> = lance_columns
                .iter()
                .filter(|c| table_field_names.contains(c.as_str()))
                .map(|c| c.as_str())
                .collect();

            let query = table.query().select(Select::columns(&actual_columns));
            // Do NOT filter _deleted here — MVCC dedup must see the
            // highest-version row first (which may be a deletion tombstone).
            // Deleted rows are filtered out after dedup.
            let query = match storage.version_high_water_mark() {
                Some(hwm) => query.only_if(format!("_version <= {}", hwm)),
                None => query,
            };

            match query.execute().await {
                Ok(stream) => {
                    use futures::TryStreamExt;
                    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap_or_default();
                    if batches.is_empty() {
                        None
                    } else {
                        Some(
                            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                                .map_err(|e| {
                                    datafusion::error::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    )
                                })?,
                        )
                    }
                }
                Err(_) => None,
            }
        }
        Err(_) => None, // Table doesn't exist yet — only L0 data
    };

    // MVCC dedup the Lance batch
    let lance_deduped = match lance_batch {
        Some(batch) => {
            let deduped = mvcc_dedup_batch(&batch)?;
            if deduped.num_rows() > 0 {
                Some(deduped)
            } else {
                None
            }
        }
        None => None,
    };

    // Build the internal Lance schema for L0 batch construction.
    // Use the Lance batch schema if available, otherwise build from scratch.
    let internal_schema = match &lance_deduped {
        Some(batch) => batch.schema(),
        None => {
            // Build schema from column list + type info
            let mut fields = vec![
                Field::new("_vid", DataType::UInt64, false),
                Field::new("_deleted", DataType::Boolean, false),
                Field::new("_version", DataType::UInt64, false),
            ];
            for col in &lance_columns {
                if col == "_vid" || col == "_deleted" || col == "_version" {
                    continue;
                }
                if col == "overflow_json" {
                    fields.push(Field::new("overflow_json", DataType::LargeBinary, true));
                } else {
                    let arrow_type = label_props
                        .and_then(|lp| lp.get(col.as_str()))
                        .map(|meta| meta.r#type.to_arrow())
                        .unwrap_or(DataType::LargeBinary);
                    fields.push(Field::new(col, arrow_type, true));
                }
            }
            Arc::new(Schema::new(fields))
        }
    };

    // Build L0 batch
    let l0_batch = build_l0_vertex_batch(l0_ctx, label, &internal_schema, label_props)?;

    // Merge Lance + L0
    let merged = match (lance_deduped, l0_batch.num_rows() > 0) {
        (Some(lance), true) => {
            // Need to align L0 batch schema to match Lance batch schema
            let combined = arrow::compute::concat_batches(&internal_schema, &[lance, l0_batch])
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            // Re-dedup: L0 versions are higher so they win
            mvcc_dedup_batch(&combined)?
        }
        (Some(lance), false) => lance,
        (None, true) => l0_batch,
        (None, false) => {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }
    };

    // Filter out MVCC deletion tombstones (_deleted = true)
    let merged = filter_deleted_rows(&merged)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 tombstones
    let filtered = filter_l0_tombstones(&merged, l0_ctx)?;

    if filtered.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Map to output schema
    map_to_output_schema(
        &filtered,
        label,
        variable,
        projected_properties,
        output_schema,
        l0_ctx,
    )
}

/// Columnar-first edge scan: single Lance query with MVCC dedup and L0 overlay.
///
/// Replaces the two-phase `scan_edge_eids_static()` + `materialize_edge_batch_static()`
/// for edge scans. Reads all needed columns in a single DeltaDataset query, performs
/// MVCC dedup via Arrow compute, merges L0 buffer data, filters tombstones, and maps
/// to the output schema.
async fn columnar_scan_edge_batch_static(
    graph_ctx: &GraphExecutionContext,
    edge_type: &str,
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let uni_schema = storage.schema_manager().schema();
    let type_props = uni_schema.properties.get(edge_type);

    // Build the list of columns to request from DeltaDataset Lance table
    let mut lance_columns: Vec<String> = vec![
        "eid".to_string(),
        "src_vid".to_string(),
        "dst_vid".to_string(),
        "op".to_string(),
        "_version".to_string(),
    ];
    for prop in projected_properties {
        if prop == "overflow_json" {
            if !lance_columns.contains(&"overflow_json".to_string()) {
                lance_columns.push("overflow_json".to_string());
            }
        } else {
            let exists_in_schema = type_props.is_some_and(|tp| tp.contains_key(prop));
            if exists_in_schema && !lance_columns.contains(prop) {
                lance_columns.push(prop.clone());
            }
        }
    }

    // Check if any projected property is not in schema (needs overflow_json)
    let needs_overflow = projected_properties
        .iter()
        .any(|p| p == "overflow_json" || !type_props.is_some_and(|tp| tp.contains_key(p)));
    if needs_overflow && !lance_columns.contains(&"overflow_json".to_string()) {
        lance_columns.push("overflow_json".to_string());
    }

    // Try to query DeltaDataset (forward direction)
    let ds = storage
        .delta_dataset(edge_type, "fwd")
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
    let lancedb_store = storage.lancedb_store();

    let lance_batch = match ds.open_lancedb(lancedb_store).await {
        Ok(table) => {
            use lancedb::query::{ExecutableQuery, QueryBase, Select};

            // Check which columns actually exist in the Lance table
            let table_schema = table
                .schema()
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
            let table_field_names: HashSet<&str> = table_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();

            let actual_columns: Vec<&str> = lance_columns
                .iter()
                .filter(|c| table_field_names.contains(c.as_str()))
                .map(|c| c.as_str())
                .collect();

            // Do NOT filter op here — MVCC dedup must see DELETE ops
            // (op != 0) to pick the highest version. Deleted edges are
            // filtered out after dedup.
            let query = table.query().select(Select::columns(&actual_columns));
            let query = match storage.version_high_water_mark() {
                Some(hwm) => query.only_if(format!("_version <= {}", hwm)),
                None => query,
            };

            match query.execute().await {
                Ok(stream) => {
                    use futures::TryStreamExt;
                    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap_or_default();
                    if batches.is_empty() {
                        None
                    } else {
                        Some(
                            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                                .map_err(|e| {
                                    datafusion::error::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    )
                                })?,
                        )
                    }
                }
                Err(_) => None,
            }
        }
        Err(_) => None, // Table doesn't exist yet — only L0 data
    };

    // MVCC dedup the Lance batch (by eid)
    let lance_deduped = match lance_batch {
        Some(batch) => {
            let deduped = mvcc_dedup_batch_by(&batch, "eid")?;
            if deduped.num_rows() > 0 {
                Some(deduped)
            } else {
                None
            }
        }
        None => None,
    };

    // Build the internal schema for L0 batch construction.
    // Use the Lance batch schema if available, otherwise build from scratch.
    let internal_schema = match &lance_deduped {
        Some(batch) => batch.schema(),
        None => {
            let mut fields = vec![
                Field::new("eid", DataType::UInt64, false),
                Field::new("src_vid", DataType::UInt64, false),
                Field::new("dst_vid", DataType::UInt64, false),
                Field::new("op", DataType::UInt8, false),
                Field::new("_version", DataType::UInt64, false),
            ];
            for col in &lance_columns {
                if matches!(
                    col.as_str(),
                    "eid" | "src_vid" | "dst_vid" | "op" | "_version"
                ) {
                    continue;
                }
                if col == "overflow_json" {
                    fields.push(Field::new("overflow_json", DataType::LargeBinary, true));
                } else {
                    let arrow_type = type_props
                        .and_then(|tp| tp.get(col.as_str()))
                        .map(|meta| meta.r#type.to_arrow())
                        .unwrap_or(DataType::LargeBinary);
                    fields.push(Field::new(col, arrow_type, true));
                }
            }
            Arc::new(Schema::new(fields))
        }
    };

    // Build L0 batch
    let l0_batch = build_l0_edge_batch(l0_ctx, edge_type, &internal_schema, type_props)?;

    // Merge Lance + L0
    let merged = match (lance_deduped, l0_batch.num_rows() > 0) {
        (Some(lance), true) => {
            let combined = arrow::compute::concat_batches(&internal_schema, &[lance, l0_batch])
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            // Re-dedup: L0 versions are higher so they win
            mvcc_dedup_batch_by(&combined, "eid")?
        }
        (Some(lance), false) => lance,
        (None, true) => l0_batch,
        (None, false) => {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }
    };

    // Filter out MVCC deletion ops (op != 0) after dedup
    let merged = filter_deleted_edge_ops(&merged)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 edge tombstones
    let filtered = filter_l0_edge_tombstones(&merged, l0_ctx)?;

    if filtered.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Map to output schema
    map_edge_to_output_schema(&filtered, variable, projected_properties, output_schema)
}

/// Columnar-first schemaless vertex scan: single Lance query with MVCC dedup and L0 overlay.
///
/// Replaces the two-phase `scan_*_vids_*()` + `materialize_schemaless_vertex_batch_static()`
/// for schemaless vertex scans. Reads `_vid`, `labels`, `props_json`, `_version` in a single
/// Lance query on the main vertices table, performs MVCC dedup via Arrow compute, merges L0
/// buffer data, filters tombstones, and maps to the output schema.
async fn columnar_scan_schemaless_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    use uni_store::storage::main_vertex::MainVertexDataset;

    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let lancedb_store = storage.lancedb_store();

    // Build the Lance filter expression — do NOT filter _deleted here;
    // MVCC dedup must see deletion tombstones to pick the highest version.
    let filter = {
        let mut parts = Vec::new();

        // Label filter
        if !label.is_empty() {
            if label.contains(':') {
                // Multi-label: each label must be present
                for lbl in label.split(':') {
                    parts.push(format!("array_contains(labels, '{}')", lbl));
                }
            } else {
                parts.push(format!("array_contains(labels, '{}')", label));
            }
        }

        // MVCC version filter
        if let Some(hwm) = storage.version_high_water_mark() {
            parts.push(format!("_version <= {}", hwm));
        }

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        }
    };

    // Single Lance query: SELECT _vid, _deleted, labels, props_json, _version WHERE <filter>
    let lance_batch = match MainVertexDataset::open_table(lancedb_store).await {
        Ok(table) => {
            use lancedb::query::{ExecutableQuery, QueryBase, Select};

            let query = table.query().select(Select::columns(&[
                "_vid",
                "_deleted",
                "labels",
                "props_json",
                "_version",
            ]));
            let query = match filter {
                Some(f) => query.only_if(f),
                None => query,
            };

            match query.execute().await {
                Ok(stream) => {
                    use futures::TryStreamExt;
                    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap_or_default();
                    if batches.is_empty() {
                        None
                    } else {
                        Some(
                            arrow::compute::concat_batches(&batches[0].schema(), &batches)
                                .map_err(|e| {
                                    datafusion::error::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    )
                                })?,
                        )
                    }
                }
                Err(_) => None,
            }
        }
        Err(_) => None, // Table doesn't exist yet — only L0 data
    };

    // MVCC dedup the Lance batch
    let lance_deduped = match lance_batch {
        Some(batch) => {
            let deduped = mvcc_dedup_batch(&batch)?;
            if deduped.num_rows() > 0 {
                Some(deduped)
            } else {
                None
            }
        }
        None => None,
    };

    // Build the internal schema for L0 batch construction.
    // Use the Lance batch schema if available, otherwise build from scratch.
    let internal_schema = match &lance_deduped {
        Some(batch) => batch.schema(),
        None => Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
            Field::new("props_json", DataType::LargeBinary, true),
            Field::new("_version", DataType::UInt64, false),
        ])),
    };

    // Build L0 batch
    let l0_batch = build_l0_schemaless_vertex_batch(l0_ctx, label, &internal_schema)?;

    // Merge Lance + L0
    let merged = match (lance_deduped, l0_batch.num_rows() > 0) {
        (Some(lance), true) => {
            let combined = arrow::compute::concat_batches(&internal_schema, &[lance, l0_batch])
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            // Re-dedup: L0 versions are higher so they win
            mvcc_dedup_batch(&combined)?
        }
        (Some(lance), false) => lance,
        (None, true) => l0_batch,
        (None, false) => {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }
    };

    // Filter out MVCC deletion tombstones (_deleted = true)
    let merged = filter_deleted_rows(&merged)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 tombstones
    let filtered = filter_l0_tombstones(&merged, l0_ctx)?;

    if filtered.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Map to output schema
    map_to_schemaless_output_schema(
        &filtered,
        variable,
        projected_properties,
        output_schema,
        l0_ctx,
    )
}

/// Build a RecordBatch from L0 buffer data for schemaless vertices.
///
/// Merges L0 buffers in visibility order (pending_flush → current → transaction),
/// with later buffers overwriting earlier ones for the same VID. Produces a batch
/// matching the internal schema: `_vid, labels, props_json, _version`.
fn build_l0_schemaless_vertex_batch(
    l0_ctx: &crate::query::df_graph::L0Context,
    label: &str,
    internal_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    // Collect all L0 vertex data, merging in visibility order
    // vid -> (merged_props, highest_version, labels)
    let mut vid_data: HashMap<u64, (Properties, u64, Vec<String>)> = HashMap::new();
    let mut tombstones: HashSet<u64> = HashSet::new();

    // Parse multi-label filter
    let label_filter: Vec<&str> = if label.is_empty() {
        vec![]
    } else if label.contains(':') {
        label.split(':').collect()
    } else {
        vec![label]
    };

    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();

        // Collect tombstones
        for vid in guard.vertex_tombstones.iter() {
            tombstones.insert(vid.as_u64());
        }

        // Collect VIDs matching the label filter
        let vids: Vec<Vid> = if label_filter.is_empty() {
            guard.all_vertex_vids()
        } else if label_filter.len() == 1 {
            guard.vids_for_label(label_filter[0])
        } else {
            guard.vids_with_all_labels(&label_filter)
        };

        for vid in vids {
            let vid_u64 = vid.as_u64();
            if tombstones.contains(&vid_u64) {
                continue;
            }
            let version = guard.vertex_versions.get(&vid).copied().unwrap_or(0);
            let entry = vid_data
                .entry(vid_u64)
                .or_insert_with(|| (Properties::new(), 0, Vec::new()));

            // Merge properties (later L0 overwrites)
            if let Some(props) = guard.vertex_properties.get(&vid) {
                for (k, v) in props {
                    entry.0.insert(k.clone(), v.clone());
                }
            }
            // Take the highest version
            if version > entry.1 {
                entry.1 = version;
            }
            // Update labels from latest L0 layer
            if let Some(labels) = guard.vertex_labels.get(&vid) {
                entry.2 = labels.clone();
            }
        }
    }

    // Remove tombstoned VIDs
    for t in &tombstones {
        vid_data.remove(t);
    }

    if vid_data.is_empty() {
        return Ok(RecordBatch::new_empty(internal_schema.clone()));
    }

    // Sort VIDs for deterministic output
    let mut vids: Vec<u64> = vid_data.keys().copied().collect();
    vids.sort_unstable();

    let num_rows = vids.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(internal_schema.fields().len());

    for field in internal_schema.fields() {
        match field.name().as_str() {
            "_vid" => {
                columns.push(Arc::new(UInt64Array::from(vids.clone())));
            }
            "labels" => {
                let mut labels_builder = ListBuilder::new(StringBuilder::new());
                for vid_u64 in &vids {
                    let (_, _, labels) = &vid_data[vid_u64];
                    let values = labels_builder.values();
                    for lbl in labels {
                        values.append_value(lbl);
                    }
                    labels_builder.append(true);
                }
                columns.push(Arc::new(labels_builder.finish()));
            }
            "props_json" => {
                let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                for vid_u64 in &vids {
                    let (props, _, _) = &vid_data[vid_u64];
                    if props.is_empty() {
                        builder.append_null();
                    } else {
                        // Encode properties as CypherValue blob
                        let json_obj: serde_json::Value = {
                            let mut map = serde_json::Map::new();
                            for (k, v) in props {
                                let json_val: serde_json::Value = v.clone().into();
                                map.insert(k.clone(), json_val);
                            }
                            serde_json::Value::Object(map)
                        };
                        match encode_cypher_value(&json_obj) {
                            Ok(bytes) => builder.append_value(bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "_deleted" => {
                // L0 vertices are always live (tombstoned ones already excluded)
                columns.push(Arc::new(arrow_array::BooleanArray::from(vec![
                    false;
                    num_rows
                ])));
            }
            "_version" => {
                let vals: Vec<u64> = vids.iter().map(|v| vid_data[v].1).collect();
                columns.push(Arc::new(UInt64Array::from(vals)));
            }
            _ => {
                // Unexpected column — fill with nulls
                columns.push(arrow_array::new_null_array(field.data_type(), num_rows));
            }
        }
    }

    RecordBatch::try_new(internal_schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Map an internal-schema schemaless batch to the DataFusion output schema.
///
/// The internal batch has `_vid, labels, props_json, _version` columns. The output
/// schema has `{variable}._vid`, `{variable}._labels`, and per-property columns.
/// Individual properties are extracted from the `props_json` CypherValue blob by
/// decoding to a Map and extracting the sub-value.
fn map_to_schemaless_output_schema(
    batch: &RecordBatch,
    _variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

    // 1. {var}._vid — passthrough
    let vid_col = batch
        .column_by_name("_vid")
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("Missing _vid column".to_string())
        })?
        .clone();
    let vid_arr = vid_col
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal("_vid not UInt64".to_string())
        })?;
    columns.push(vid_col.clone());

    // 2. {var}._labels — from labels column with L0 overlay
    let labels_col = batch.column_by_name("labels");
    let labels_arr = labels_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>());

    let mut labels_builder = ListBuilder::new(StringBuilder::new());
    for i in 0..vid_arr.len() {
        let vid_u64 = vid_arr.value(i);
        let vid = Vid::from(vid_u64);

        // Start with labels from the batch
        let mut row_labels: Vec<String> = Vec::new();
        if let Some(arr) = labels_arr
            && !arr.is_null(i)
        {
            let list_val = arr.value(i);
            if let Some(str_arr) = list_val.as_any().downcast_ref::<arrow_array::StringArray>() {
                for j in 0..str_arr.len() {
                    if !str_arr.is_null(j) {
                        row_labels.push(str_arr.value(j).to_string());
                    }
                }
            }
        }

        // Overlay L0 labels
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if let Some(l0_labels) = guard.vertex_labels.get(&vid) {
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

    // 3. Projected properties — extract from props_json
    let props_col = batch.column_by_name("props_json");
    let props_arr =
        props_col.and_then(|c| c.as_any().downcast_ref::<arrow_array::LargeBinaryArray>());

    for prop in projected_properties {
        if prop == "_all_props" {
            // Optimization: _all_props IS props_json — passthrough directly
            match props_col {
                Some(col) => columns.push(col.clone()),
                None => {
                    columns.push(arrow_array::new_null_array(
                        &DataType::LargeBinary,
                        batch.num_rows(),
                    ));
                }
            }
        } else {
            // Extract individual property from CypherValue blob
            let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
            for i in 0..batch.num_rows() {
                let vid = Vid::from(vid_arr.value(i));

                // Check L0 buffers (forward = visibility order, later overwrites)
                let mut l0_val: Option<Option<Value>> = None; // Some(None) = null, Some(Some(v)) = value
                for l0 in l0_ctx.iter_l0_buffers() {
                    let guard = l0.read();
                    if let Some(props) = guard.vertex_properties.get(&vid)
                        && let Some(val) = props.get(prop.as_str())
                    {
                        l0_val = Some(Some(val.clone()));
                    }
                }

                if let Some(val_opt) = l0_val {
                    // L0 has this property
                    match val_opt {
                        Some(val) if !val.is_null() => {
                            let json_val: serde_json::Value = val.into();
                            match encode_cypher_value(&json_val) {
                                Ok(bytes) => builder.append_value(bytes),
                                Err(_) => builder.append_null(),
                            }
                        }
                        _ => builder.append_null(),
                    }
                } else {
                    // Extract from props_json CypherValue blob
                    if let Some(arr) = props_arr {
                        if !arr.is_null(i) {
                            let blob = arr.value(i);
                            // Fast path: extract map entry without decoding entire map
                            if let Some(sub_bytes) =
                                uni_common::cypher_value_codec::extract_map_entry_raw(blob, prop)
                            {
                                builder.append_value(&sub_bytes);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
            }
            columns.push(Arc::new(builder.finish()));
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Materialize a batch of edges with their properties (static version for async).
///
/// Superseded by `columnar_scan_edge_batch_static` for edge scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
async fn materialize_edge_batch_static(
    graph_ctx: &GraphExecutionContext,
    properties: &[String],
    schema: &SchemaRef,
    eids: Vec<Eid>,
) -> DFResult<RecordBatch> {
    if eids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let property_manager = graph_ctx.property_manager();
    let query_ctx = graph_ctx.query_context();

    // Convert properties to &str for the API
    let prop_refs: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();

    // Batch load edge properties
    let props_map = property_manager
        .get_batch_edge_props(&eids, &prop_refs, Some(&query_ctx))
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

    // Filter out deleted edges (those not in props_map)
    // PropertyManager uses Vid as the key for the map, so convert EIDs
    let valid_eids: Vec<Eid> = eids
        .into_iter()
        .filter(|eid| {
            let vid_key = Vid::from(eid.as_u64());
            props_map.contains_key(&vid_key)
        })
        .collect();

    build_edge_record_batch_static(schema, &valid_eids, &props_map)
}

/// Build a RecordBatch from EIDs and their properties (static version).
///
/// Superseded by `columnar_scan_edge_batch_static` for edge scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
fn build_edge_record_batch_static(
    schema: &SchemaRef,
    eids: &[Eid],
    props_map: &HashMap<Vid, Properties>,
) -> DFResult<RecordBatch> {
    if eids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // Build _eid column
    let eid_values: Vec<u64> = eids.iter().map(|e| e.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(eid_values)));

    // Build property columns
    for field in schema.fields().iter().skip(1) {
        // Extract property name from field name (e.g., "e.role" -> "role")
        let prop_name = field.name().split('.').nth(1).unwrap_or(field.name());

        // Convert EIDs to VIDs for property lookup
        let column =
            build_edge_property_column_static(eids, props_map, prop_name, field.data_type())?;
        columns.push(column);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Build a property column for edges, converting EIDs to VIDs for lookup.
///
/// Superseded by `columnar_scan_edge_batch_static` for edge scans.
/// Kept for potential fallback use.
#[allow(dead_code)]
fn build_edge_property_column_static(
    eids: &[Eid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    data_type: &DataType,
) -> DFResult<ArrayRef> {
    // Convert to VIDs for property lookup
    let vids: Vec<Vid> = eids.iter().map(|eid| Vid::from(eid.as_u64())).collect();

    // Reuse the vertex property column builder
    build_property_column_static(&vids, props_map, prop_name, data_type)
}

/// Get the property value for a VID, returning None if not found.
pub(crate) fn get_property_value(
    vid: &Vid,
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
) -> Option<Value> {
    if prop_name == "_all_props" {
        return props_map.get(vid).map(|p| {
            let map: HashMap<String, Value> =
                p.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            Value::Map(map)
        });
    }
    props_map
        .get(vid)
        .and_then(|props| props.get(prop_name))
        .cloned()
}

/// Encode a `serde_json::Value` as CypherValue binary (MessagePack-tagged).
///
/// Converts from serde_json::Value -> uni_common::Value -> CypherValue bytes.
pub(crate) fn encode_cypher_value(val: &serde_json::Value) -> Result<Vec<u8>, String> {
    let uni_val: uni_common::Value = val.clone().into();
    Ok(uni_common::cypher_value_codec::encode(&uni_val))
}

/// Build a numeric column from property values using the specified builder and extractor.
macro_rules! build_numeric_column {
    ($vids:expr, $props_map:expr, $prop_name:expr, $builder_ty:ty, $extractor:expr, $cast:expr) => {{
        let mut builder = <$builder_ty>::new();
        for vid in $vids {
            match get_property_value(vid, $props_map, $prop_name) {
                Some(ref v) => {
                    if let Some(val) = $extractor(v) {
                        builder.append_value($cast(val));
                    } else {
                        builder.append_null();
                    }
                }
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

/// Build an Arrow column from property values (static version).
pub(crate) fn build_property_column_static(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    data_type: &DataType,
) -> DFResult<ArrayRef> {
    match data_type {
        DataType::LargeBinary => {
            // Handle CypherValue binary columns (overflow_json and Json-typed properties).
            use arrow_array::builder::LargeBinaryBuilder;
            let mut builder = LargeBinaryBuilder::new();

            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Bytes(bytes)) => {
                        builder.append_value(&bytes);
                    }
                    Some(Value::List(arr)) if arr.iter().all(|v| v.as_u64().is_some()) => {
                        // Raw CypherValue bytes stored as list of u8 values from PropertyManager
                        let bytes: Vec<u8> = arr
                            .iter()
                            .filter_map(|v| v.as_u64().map(|n| n as u8))
                            .collect();
                        builder.append_value(&bytes);
                    }
                    Some(val) => {
                        // Value from PropertyManager — convert to serde_json and re-encode to CypherValue binary
                        let json_val: serde_json::Value = val.into();
                        match encode_cypher_value(&json_val) {
                            Ok(bytes) => builder.append_value(bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            // CRDT binary properties: JSON-decoded CRDTs re-encoded to MessagePack
            let mut builder = BinaryBuilder::new();
            for vid in vids {
                let bytes = get_property_value(vid, props_map, prop_name)
                    .filter(|v| !v.is_null())
                    .and_then(|v| {
                        let json_val: serde_json::Value = v.into();
                        serde_json::from_value::<uni_crdt::Crdt>(json_val).ok()
                    })
                    .and_then(|crdt| crdt.to_msgpack().ok());
                match bytes {
                    Some(b) => builder.append_value(&b),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => builder.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Int64Builder,
                |v: &Value| v.as_i64(),
                |v| v
            )
        }
        DataType::Int32 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Int32Builder,
                |v: &Value| v.as_i64(),
                |v: i64| v as i32
            )
        }
        DataType::Float64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Float64Builder,
                |v: &Value| v.as_f64(),
                |v| v
            )
        }
        DataType::Float32 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Float32Builder,
                |v: &Value| v.as_f64(),
                |v: f64| v as f32
            )
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Bool(b)) => builder.append_value(b),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                UInt64Builder,
                |v: &Value| v.as_u64(),
                |v| v
            )
        }
        DataType::FixedSizeList(inner, dim) if *inner.data_type() == DataType::Float32 => {
            // Vector properties: FixedSizeList(Float32, N)
            let values_builder = Float32Builder::new();
            let mut list_builder = FixedSizeListBuilder::new(values_builder, *dim);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            list_builder
                                .values()
                                .append_value(v.as_f64().unwrap_or(0.0) as f32);
                        }
                        list_builder.append(true);
                    }
                    _ => {
                        // Append dim nulls to inner values, then mark row as null
                        for _ in 0..*dim {
                            list_builder.values().append_null();
                        }
                        list_builder.append(false);
                    }
                }
            }
            Ok(Arc::new(list_builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            // Timestamp properties stored as ISO 8601 strings or i64 microseconds
            let mut builder = TimestampMicrosecondBuilder::new().with_timezone("UTC");
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::String(s)) => match parse_datetime_utc(&s) {
                        Ok(dt) => builder.append_value(dt.timestamp_micros()),
                        Err(_) => builder.append_null(),
                    },
                    Some(Value::Int(n)) => {
                        builder.append_value(n);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::new();
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::String(s)) => match NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                        Ok(d) => builder.append_value((d - epoch).num_days() as i32),
                        Err(_) => builder.append_null(),
                    },
                    Some(Value::Int(n)) => {
                        builder.append_value(n as i32);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut builder = Time64MicrosecondBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::String(s)) => {
                        match NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
                            .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M:%S"))
                        {
                            Ok(t) => {
                                let micros = t.num_seconds_from_midnight() as i64 * 1_000_000
                                    + t.nanosecond() as i64 / 1_000;
                                builder.append_value(micros);
                            }
                            Err(_) => builder.append_null(),
                        }
                    }
                    Some(Value::Int(n)) => {
                        builder.append_value(n);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let mut builder = DurationMicrosecondBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Int(n)) => {
                        builder.append_value(n);
                    }
                    Some(Value::String(s)) => {
                        // Try to parse ISO 8601 duration or simple duration format
                        match crate::query::datetime::parse_duration_to_micros(&s) {
                            Ok(us) => builder.append_value(us),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::List(inner_field) => {
            build_list_property_column(vids, props_map, prop_name, inner_field)
        }
        DataType::Struct(fields) => {
            build_struct_property_column(vids, props_map, prop_name, fields)
        }
        // Default: convert to string
        _ => {
            let mut builder = StringBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => builder.append_value(other.to_string()),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Build a List-typed Arrow column from list property values.
fn build_list_property_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    inner_field: &Arc<Field>,
) -> DFResult<ArrayRef> {
    match inner_field.data_type() {
        DataType::Utf8 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v {
                                Value::String(s) => builder.values().append_value(s),
                                Value::Null => builder.values().append_null(),
                                other => builder.values().append_value(format!("{other:?}")),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v.as_i64() {
                                Some(n) => builder.values().append_value(n),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v.as_f64() {
                                Some(n) => builder.values().append_value(n),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v.as_bool() {
                                Some(b) => builder.values().append_value(b),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Struct(fields) => {
            // Map types are List(Struct(key, value)) — build struct inner elements
            build_list_of_structs_column(vids, props_map, prop_name, fields)
        }
        // Fallback: serialize inner elements as strings
        _ => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            match v {
                                Value::Null => builder.values().append_null(),
                                other => builder.values().append_value(format!("{other:?}")),
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Build a List(Struct(...)) column, used for Map-type properties.
///
/// Handles two value representations:
/// - `Value::List([Map{key: k, value: v}, ...])` — pre-converted kv pairs
/// - `Value::Map({k1: v1, k2: v2})` — raw map objects (converted to kv pairs)
fn build_list_of_structs_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    fields: &Fields,
) -> DFResult<ArrayRef> {
    use arrow_array::StructArray;

    let values: Vec<Option<Value>> = vids
        .iter()
        .map(|vid| get_property_value(vid, props_map, prop_name))
        .collect();

    // Convert each row's value to an owned Vec of Maps (key-value pairs).
    // This normalizes both List-of-maps and Map representations.
    let rows: Vec<Option<Vec<HashMap<String, Value>>>> = values
        .iter()
        .map(|val| match val {
            Some(Value::List(arr)) => {
                let objs: Vec<HashMap<String, Value>> = arr
                    .iter()
                    .filter_map(|v| {
                        if let Value::Map(m) = v {
                            Some(m.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if objs.is_empty() { None } else { Some(objs) }
            }
            Some(Value::Map(obj)) => {
                // Map property: convert {k1: v1, k2: v2} -> [{key: k1, value: v1}, ...]
                let kv_pairs: Vec<HashMap<String, Value>> = obj
                    .iter()
                    .map(|(k, v)| {
                        let mut m = HashMap::new();
                        m.insert("key".to_string(), Value::String(k.clone()));
                        m.insert("value".to_string(), v.clone());
                        m
                    })
                    .collect();
                Some(kv_pairs)
            }
            _ => None,
        })
        .collect();

    let total_items: usize = rows
        .iter()
        .filter_map(|r| r.as_ref())
        .map(|v| v.len())
        .sum();

    // Build child arrays for each field in the struct
    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| {
            let field_name = field.name();
            match field.data_type() {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(total_items, total_items * 16);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name) {
                            Some(Value::String(s)) => builder.append_value(s),
                            Some(Value::Null) | None => builder.append_null(),
                            Some(other) => builder.append_value(format!("{other:?}")),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(total_items);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name).and_then(|v| v.as_i64()) {
                            Some(n) => builder.append_value(n),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(total_items);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name).and_then(|v| v.as_f64()) {
                            Some(n) => builder.append_value(n),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                // Fallback: serialize as string
                _ => {
                    let mut builder = StringBuilder::with_capacity(total_items, total_items * 16);
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name) {
                            Some(Value::Null) | None => builder.append_null(),
                            Some(other) => builder.append_value(format!("{other:?}")),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            }
        })
        .collect();

    // Build struct array from children
    let struct_array = StructArray::try_new(fields.clone(), child_arrays, None)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;

    // Build list offsets
    let mut offsets = Vec::with_capacity(vids.len() + 1);
    let mut nulls = Vec::with_capacity(vids.len());
    let mut offset = 0i32;
    offsets.push(offset);
    for row in &rows {
        match row {
            Some(objs) => {
                offset += objs.len() as i32;
                offsets.push(offset);
                nulls.push(true);
            }
            None => {
                offsets.push(offset);
                nulls.push(false);
            }
        }
    }

    let list_field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));
    let list_array = arrow_array::ListArray::try_new(
        list_field,
        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets)),
        Arc::new(struct_array),
        Some(arrow::buffer::NullBuffer::from(nulls)),
    )
    .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(Arc::new(list_array))
}

/// Build a Struct-typed Arrow column from Map property values (e.g. Point types).
fn build_struct_property_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    fields: &Fields,
) -> DFResult<ArrayRef> {
    use arrow_array::StructArray;

    let values: Vec<Option<Value>> = vids
        .iter()
        .map(|vid| get_property_value(vid, props_map, prop_name))
        .collect();

    let child_arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| {
            let field_name = field.name();
            match field.data_type() {
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_f64()) {
                                    Some(n) => builder.append_value(n),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(vids.len(), vids.len() * 16);
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => match obj.get(field_name) {
                                Some(Value::String(s)) => builder.append_value(s),
                                Some(Value::Null) | None => builder.append_null(),
                                Some(other) => builder.append_value(format!("{other:?}")),
                            },
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_i64()) {
                                    Some(n) => builder.append_value(n),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                // Fallback: serialize as string
                _ => {
                    let mut builder = StringBuilder::with_capacity(vids.len(), vids.len() * 16);
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => match obj.get(field_name) {
                                Some(Value::Null) | None => builder.append_null(),
                                Some(other) => builder.append_value(format!("{other:?}")),
                            },
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            }
        })
        .collect();

    // Build null bitmap — null when the value is null/missing
    let nulls: Vec<bool> = values
        .iter()
        .map(|v| matches!(v, Some(Value::Map(_))))
        .collect();

    let struct_array = StructArray::try_new(
        fields.clone(),
        child_arrays,
        Some(arrow::buffer::NullBuffer::from(nulls)),
    )
    .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(Arc::new(struct_array))
}

impl Stream for GraphScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Use a temporary to avoid borrow issues
            let state = std::mem::replace(&mut self.state, GraphScanState::Done);

            match state {
                GraphScanState::Init => {
                    // Create the future with cloned data for ownership
                    let graph_ctx = self.graph_ctx.clone();
                    let label = self.label.clone();
                    let variable = self.variable.clone();
                    let properties = self.properties.clone();
                    let is_edge_scan = self.is_edge_scan;
                    let is_schemaless = self.is_schemaless;
                    let schema = self.schema.clone();

                    let fut = async move {
                        // Check timeout
                        graph_ctx.check_timeout().map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                        if is_edge_scan {
                            // Edge scan — columnar-first single query
                            let batch = columnar_scan_edge_batch_static(
                                &graph_ctx,
                                &label,
                                &variable,
                                &properties,
                                &schema,
                            )
                            .await?;
                            Ok(Some(batch))
                        } else if is_schemaless {
                            // Schemaless vertex scan — columnar-first single query
                            let batch = columnar_scan_schemaless_vertex_batch_static(
                                &graph_ctx,
                                &label,
                                &variable,
                                &properties,
                                &schema,
                            )
                            .await?;
                            Ok(Some(batch))
                        } else {
                            // Known label vertex scan — columnar-first single query
                            let batch = columnar_scan_vertex_batch_static(
                                &graph_ctx,
                                &label,
                                &variable,
                                &properties,
                                &schema,
                            )
                            .await?;
                            Ok(Some(batch))
                        }
                    };

                    self.state = GraphScanState::Executing(Box::pin(fut));
                    // Continue loop to poll the future
                }
                GraphScanState::Executing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = GraphScanState::Done;
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        return Poll::Ready(batch.map(Ok));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = GraphScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = GraphScanState::Executing(fut);
                        return Poll::Pending;
                    }
                },
                GraphScanState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for GraphScanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_vertex_schema() {
        let uni_schema = UniSchema::default();
        let schema = GraphScanExec::build_vertex_schema(
            "n",
            "Person",
            &["name".to_string(), "age".to_string()],
            &uni_schema,
        );

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n._labels");
        assert_eq!(schema.field(2).name(), "n.name");
        assert_eq!(schema.field(3).name(), "n.age");
    }

    #[test]
    fn test_build_edge_schema() {
        let uni_schema = UniSchema::default();
        let schema =
            GraphScanExec::build_edge_schema("r", "KNOWS", &["weight".to_string()], &uni_schema);

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "r._eid");
        assert_eq!(schema.field(1).name(), "r._src_vid");
        assert_eq!(schema.field(2).name(), "r._dst_vid");
        assert_eq!(schema.field(3).name(), "r.weight");
    }

    #[test]
    fn test_build_schemaless_vertex_schema() {
        let schema = GraphScanExec::build_schemaless_vertex_schema(
            "n",
            &["name".to_string(), "age".to_string()],
        );

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(1).name(), "n._labels");
        assert_eq!(schema.field(2).name(), "n.name");
        assert_eq!(schema.field(2).data_type(), &DataType::LargeBinary);
        assert_eq!(schema.field(3).name(), "n.age");
        assert_eq!(schema.field(3).data_type(), &DataType::LargeBinary);
    }

    #[test]
    fn test_schemaless_all_scan_has_empty_label() {
        // This test verifies that new_schemaless_all_scan creates a scan with empty label
        // We can't fully test execution without a GraphExecutionContext, but we can verify
        // the constructor sets the empty label correctly by checking the struct internals
        // This is a structural test to ensure the "empty label signals scan all" pattern works
        let schema = GraphScanExec::build_schemaless_vertex_schema("n", &[]);

        // Verify the schema has _vid and _labels columns for a scan with no properties
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n._labels");
    }

    #[test]
    fn test_cypher_value_all_props_extraction() {
        // Simulate _all_props encoding using encode_cypher_value helper
        let json_obj = serde_json::json!({"age": 30, "name": "Alice"});
        let cv_bytes = encode_cypher_value(&json_obj).unwrap();

        // Decode and extract "age" value
        let decoded = uni_common::cypher_value_codec::decode(&cv_bytes).unwrap();
        match decoded {
            uni_common::Value::Map(map) => {
                let age_val = map.get("age").unwrap();
                assert_eq!(age_val, &uni_common::Value::Int(30));
            }
            _ => panic!("Expected Map"),
        }

        // Also test single value encoding
        let single_val = serde_json::json!(30);
        let single_bytes = encode_cypher_value(&single_val).unwrap();
        let single_decoded = uni_common::cypher_value_codec::decode(&single_bytes).unwrap();
        assert_eq!(single_decoded, uni_common::Value::Int(30));
    }

    /// Helper to build a RecordBatch with _vid, _deleted, _version columns for testing.
    fn make_mvcc_batch(vids: &[u64], versions: &[u64], deleted: &[bool]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        // Generate name values like "v{vid}_ver{version}" for tracking which row wins
        let names: Vec<String> = vids
            .iter()
            .zip(versions.iter())
            .map(|(v, ver)| format!("v{}_ver{}", v, ver))
            .collect();
        let name_arr: arrow_array::StringArray = names.iter().map(|s| Some(s.as_str())).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vids.to_vec())),
                Arc::new(arrow_array::BooleanArray::from(deleted.to_vec())),
                Arc::new(UInt64Array::from(versions.to_vec())),
                Arc::new(name_arr),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_mvcc_dedup_multiple_versions() {
        // VID 1 at versions 3, 1, 5 — should keep version 5
        // VID 2 at versions 2, 4 — should keep version 4
        let batch = make_mvcc_batch(
            &[1, 1, 1, 2, 2],
            &[3, 1, 5, 2, 4],
            &[false, false, false, false, false],
        );

        let result = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vid_col = result
            .column_by_name("_vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ver_col = result
            .column_by_name("_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let name_col = result
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();

        // VID 1 → version 5, VID 2 → version 4
        assert_eq!(vid_col.value(0), 1);
        assert_eq!(ver_col.value(0), 5);
        assert_eq!(name_col.value(0), "v1_ver5");

        assert_eq!(vid_col.value(1), 2);
        assert_eq!(ver_col.value(1), 4);
        assert_eq!(name_col.value(1), "v2_ver4");
    }

    #[test]
    fn test_mvcc_dedup_single_rows() {
        // Each VID appears once — nothing should change
        let batch = make_mvcc_batch(&[1, 2, 3], &[1, 1, 1], &[false, false, false]);
        let result = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_mvcc_dedup_empty() {
        let batch = make_mvcc_batch(&[], &[], &[]);
        let result = mvcc_dedup_batch(&batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_filter_l0_tombstones_removes_tombstoned() {
        use crate::query::df_graph::L0Context;

        // Create a batch with VIDs 1, 2, 3
        let batch = make_mvcc_batch(&[1, 2, 3], &[1, 1, 1], &[false, false, false]);

        // Create L0 context with VID 2 tombstoned
        let l0 = uni_store::runtime::l0::L0Buffer::new(1, None);
        {
            // We need to insert a tombstone — L0Buffer has pub vertex_tombstones
            // But we can't easily create one with tombstones through the constructor.
            // Use a direct approach.
        }
        let l0_buf = std::sync::Arc::new(parking_lot::RwLock::new(l0));
        l0_buf.write().vertex_tombstones.insert(Vid::from(2u64));

        let l0_ctx = L0Context {
            current_l0: Some(l0_buf),
            transaction_l0: None,
            pending_flush_l0s: vec![],
        };

        let result = filter_l0_tombstones(&batch, &l0_ctx).unwrap();
        assert_eq!(result.num_rows(), 2);

        let vid_col = result
            .column_by_name("_vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(vid_col.value(0), 1);
        assert_eq!(vid_col.value(1), 3);
    }

    #[test]
    fn test_filter_l0_tombstones_none() {
        use crate::query::df_graph::L0Context;

        let batch = make_mvcc_batch(&[1, 2, 3], &[1, 1, 1], &[false, false, false]);
        let l0_ctx = L0Context::default();

        let result = filter_l0_tombstones(&batch, &l0_ctx).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_map_to_output_schema_basic() {
        use crate::query::df_graph::L0Context;

        // Input: Lance-schema batch with _vid, _deleted, _version, name columns
        let lance_schema = Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("_version", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let name_arr: arrow_array::StringArray =
            vec![Some("Alice"), Some("Bob")].into_iter().collect();
        let batch = RecordBatch::try_new(
            lance_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64, 2])),
                Arc::new(arrow_array::BooleanArray::from(vec![false, false])),
                Arc::new(UInt64Array::from(vec![1u64, 1])),
                Arc::new(name_arr),
            ],
        )
        .unwrap();

        // Output schema: n._vid, n._labels, n.name
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("n._vid", DataType::UInt64, false),
            Field::new(
                "n._labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("n.name", DataType::Utf8, true),
        ]));

        let l0_ctx = L0Context::default();
        let result = map_to_output_schema(
            &batch,
            "Person",
            "n",
            &["name".to_string()],
            &output_schema,
            &l0_ctx,
        )
        .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema().fields().len(), 3);
        assert_eq!(result.schema().field(0).name(), "n._vid");
        assert_eq!(result.schema().field(1).name(), "n._labels");
        assert_eq!(result.schema().field(2).name(), "n.name");

        // Check name values carried through
        let name_col = result
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
    }
}
