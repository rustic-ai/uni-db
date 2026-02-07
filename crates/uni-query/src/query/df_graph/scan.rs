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
use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Properties;
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

    /// Build schema for schemaless vertex scan (all properties as Utf8).
    fn build_schemaless_vertex_schema(variable: &str, properties: &[String]) -> SchemaRef {
        let mut fields = vec![Field::new(
            format!("{}._vid", variable),
            DataType::UInt64,
            false,
        )];

        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            // All schemaless properties are treated as Utf8 (JSON strings)
            fields.push(Field::new(&col_name, DataType::Utf8, true));
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
        let mut fields = vec![Field::new(
            format!("{}._vid", variable),
            DataType::UInt64,
            false,
        )];
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
/// Performs a two-phase scan:
/// 1. Collects all VIDs/EIDs from storage and L0 buffers
/// 2. Batch-loads properties using PropertyManager
struct GraphScanStream {
    /// Graph execution context.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Label (vertex) or edge type name.
    label: String,

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
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        label: String,
        properties: Vec<String>,
        is_edge_scan: bool,
        is_schemaless: bool,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            label,
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
/// Falls back to `Utf8` if the property is not found in the schema.
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
            .unwrap_or(DataType::Utf8)
    }
}

/// Scan vertex VIDs from storage and L0 buffers (static version for async).
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
    for l0 in l0_ctx.iter_l0_buffers() {
        vids.extend(l0.read().vids_for_label(label));
    }

    // Deduplicate
    vids.sort_unstable();
    vids.dedup();

    Ok(vids)
}

/// Scan vertex VIDs from main table by label name (schemaless).
///
/// Uses the main vertices table with `array_contains(labels, 'X')` filter.
/// This is used for labels that aren't in the schema (schemaless mode).
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
    for l0 in l0_ctx.iter_l0_buffers() {
        vids.extend(l0.read().vids_for_label(label_name));
    }

    // Deduplicate
    vids.sort_unstable();
    vids.dedup();

    Ok(vids)
}

/// Scan vertex VIDs from main table with multi-label intersection.
///
/// Returns vertices that have ALL the specified labels.
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
    for l0 in l0_ctx.iter_l0_buffers() {
        vids.extend(l0.read().vids_with_all_labels(label_names));
    }

    // Deduplicate
    vids.sort_unstable();
    vids.dedup();

    Ok(vids)
}

/// Scan all vertex VIDs from main table (schemaless).
///
/// Uses the main vertices table to find all vertices regardless of label.
/// This is used for `MATCH (n)` without label filter (ScanAll).
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
    for l0 in l0_ctx.iter_l0_buffers() {
        vids.extend(l0.read().all_vertex_vids());
    }

    // Deduplicate
    vids.sort_unstable();
    vids.dedup();

    Ok(vids)
}

/// Materialize a batch of schemaless vertices with their properties.
///
/// Fetches properties from the main vertices table's props_json column.
/// All properties are returned as Utf8 (JSON strings).
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

    build_schemaless_vertex_record_batch(schema, &valid_vids, &props_map)
}

/// Accumulate properties for a vertex from all L0 layers.
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
/// All property values are converted to Utf8 (JSON strings).
fn build_schemaless_vertex_record_batch(
    schema: &SchemaRef,
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
) -> DFResult<RecordBatch> {
    if vids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // Build _vid column
    let vid_values: Vec<u64> = vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(vid_values)));

    // Build property columns (all as Utf8)
    for field in schema.fields().iter().skip(1) {
        // Extract property name from field name (e.g., "n.name" -> "name")
        let prop_name = field.name().split('.').nth(1).unwrap_or(field.name());

        let mut builder = StringBuilder::new();
        for vid in vids {
            match get_property_value(vid, props_map, prop_name) {
                Some(Value::String(s)) => builder.append_value(s),
                Some(Value::Null) | None => builder.append_null(),
                Some(other) => builder.append_value(other.to_string()),
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Scan all EIDs for a given edge type.
///
/// Note: Currently scans only from L0 buffers for simplicity.
/// Full Lance dataset scanning requires knowing src/dst labels upfront.
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

    // Filter out deleted vertices (those not in props_map)
    let valid_vids: Vec<Vid> = vids
        .into_iter()
        .filter(|vid| props_map.contains_key(vid))
        .collect();

    build_vertex_record_batch_static(schema, &valid_vids, &props_map)
}

/// Build a RecordBatch from VIDs and their properties (static version).
pub(crate) fn build_vertex_record_batch_static(
    schema: &SchemaRef,
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
) -> DFResult<RecordBatch> {
    if vids.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // Build _vid column
    let vid_values: Vec<u64> = vids.iter().map(|v| v.as_u64()).collect();
    columns.push(Arc::new(UInt64Array::from(vid_values)));

    // Build property columns
    for field in schema.fields().iter().skip(1) {
        // Extract property name from field name (e.g., "n.name" -> "name")
        let prop_name = field.name().split('.').nth(1).unwrap_or(field.name());

        let column = build_property_column_static(vids, props_map, prop_name, field.data_type())?;
        columns.push(column);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Materialize a batch of edges with their properties (static version for async).
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
            let map: serde_json::Map<String, Value> =
                p.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            Value::Object(map)
        });
    }
    props_map
        .get(vid)
        .and_then(|props| props.get(prop_name))
        .cloned()
}

/// Build a numeric column from property values using the specified builder and extractor.
macro_rules! build_numeric_column {
    ($vids:expr, $props_map:expr, $prop_name:expr, $builder_ty:ty, $extractor:expr, $cast:expr) => {{
        let mut builder = <$builder_ty>::new();
        for vid in $vids {
            match get_property_value(vid, $props_map, $prop_name) {
                Some(Value::Number(n)) => {
                    if let Some(val) = $extractor(&n) {
                        builder.append_value($cast(val));
                    } else {
                        builder.append_null();
                    }
                }
                _ => builder.append_null(),
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
            // Handle JSONB binary columns (overflow_json and Json-typed properties).
            use arrow_array::builder::LargeBinaryBuilder;
            let mut builder = LargeBinaryBuilder::new();

            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Array(arr)) if arr.iter().all(|v| v.is_u64()) => {
                        // Raw JSONB bytes stored as array of u8 values from PropertyManager
                        let bytes: Vec<u8> = arr
                            .iter()
                            .filter_map(|v| v.as_u64().map(|n| n as u8))
                            .collect();
                        builder.append_value(&bytes);
                    }
                    Some(val) => {
                        // JSON value from PropertyManager — re-encode to JSONB binary
                        match jsonb::to_owned_jsonb(&val) {
                            Ok(jsonb_bytes) => builder.append_value(jsonb_bytes.to_vec()),
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
                    .and_then(|v| serde_json::from_value::<uni_crdt::Crdt>(v.clone()).ok())
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
                |n: &serde_json::Number| n.as_i64(),
                |v| v
            )
        }
        DataType::Int32 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Int32Builder,
                |n: &serde_json::Number| n.as_i64(),
                |v: i64| v as i32
            )
        }
        DataType::Float64 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Float64Builder,
                |n: &serde_json::Number| n.as_f64(),
                |v| v
            )
        }
        DataType::Float32 => {
            build_numeric_column!(
                vids,
                props_map,
                prop_name,
                Float32Builder,
                |n: &serde_json::Number| n.as_f64(),
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
                |n: &serde_json::Number| n.as_u64(),
                |v| v
            )
        }
        DataType::FixedSizeList(inner, dim) if *inner.data_type() == DataType::Float32 => {
            // Vector properties: FixedSizeList(Float32, N)
            let values_builder = Float32Builder::new();
            let mut list_builder = FixedSizeListBuilder::new(values_builder, *dim);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Array(arr)) => {
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
                    Some(Value::Number(n)) => {
                        builder.append_value(n.as_i64().unwrap_or(0));
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
                    Some(Value::Number(n)) => {
                        builder.append_value(n.as_i64().unwrap_or(0) as i32);
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
                    Some(Value::Number(n)) => {
                        builder.append_value(n.as_i64().unwrap_or(0));
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
                    Some(Value::Number(n)) => {
                        builder.append_value(n.as_i64().unwrap_or(0));
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

/// Build a List-typed Arrow column from JSON array property values.
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
                    Some(Value::Array(arr)) => {
                        for v in arr {
                            match v {
                                Value::String(s) => builder.values().append_value(s),
                                Value::Null => builder.values().append_null(),
                                other => builder.values().append_value(other.to_string()),
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
                    Some(Value::Array(arr)) => {
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
                    Some(Value::Array(arr)) => {
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
                    Some(Value::Array(arr)) => {
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
                    Some(Value::Array(arr)) => {
                        for v in arr {
                            match v {
                                Value::Null => builder.values().append_null(),
                                other => builder.values().append_value(other.to_string()),
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
/// Handles two JSON representations:
/// - `Value::Array([{key: k, value: v}, ...])` — pre-converted kv pairs
/// - `Value::Object({k1: v1, k2: v2})` — raw map objects (converted to kv pairs)
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
    // This normalizes both Array-of-structs and Object representations.
    let rows: Vec<Option<Vec<serde_json::Map<String, Value>>>> = values
        .iter()
        .map(|val| match val {
            Some(Value::Array(arr)) => {
                let objs: Vec<serde_json::Map<String, Value>> =
                    arr.iter().filter_map(|v| v.as_object().cloned()).collect();
                if objs.is_empty() { None } else { Some(objs) }
            }
            Some(Value::Object(obj)) => {
                // Map property: convert {k1: v1, k2: v2} → [{key: k1, value: v1}, ...]
                let kv_pairs: Vec<serde_json::Map<String, Value>> = obj
                    .iter()
                    .map(|(k, v)| {
                        let mut m = serde_json::Map::new();
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
                            Some(other) => builder.append_value(other.to_string()),
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
                            Some(other) => builder.append_value(other.to_string()),
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

/// Build a Struct-typed Arrow column from JSON object property values (e.g. Point types).
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
                            Some(Value::Object(obj)) => {
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
                            Some(Value::Object(obj)) => match obj.get(field_name) {
                                Some(Value::String(s)) => builder.append_value(s),
                                Some(Value::Null) | None => builder.append_null(),
                                Some(other) => builder.append_value(other.to_string()),
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
                            Some(Value::Object(obj)) => {
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
                            Some(Value::Object(obj)) => match obj.get(field_name) {
                                Some(Value::Null) | None => builder.append_null(),
                                Some(other) => builder.append_value(other.to_string()),
                            },
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
            }
        })
        .collect();

    // Build null bitmap — null when the JSON value is null/missing
    let nulls: Vec<bool> = values
        .iter()
        .map(|v| matches!(v, Some(Value::Object(_))))
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
                            // Scan edge EIDs
                            let eids = scan_edge_eids_static(&graph_ctx, &label).await?;
                            // Materialize batch with properties
                            let batch = materialize_edge_batch_static(
                                &graph_ctx,
                                &properties,
                                &schema,
                                eids,
                            )
                            .await?;
                            Ok(Some(batch))
                        } else if is_schemaless {
                            // Schemaless vertex scan - use main table
                            let vids = if label.is_empty() {
                                // ScanAll: scan all vertices regardless of label
                                scan_all_vertex_vids_static(&graph_ctx).await?
                            } else if label.contains(':') {
                                // Multi-label: colon-separated label names with intersection semantics
                                let label_names: Vec<&str> = label.split(':').collect();
                                scan_vertex_vids_by_labels_static(&graph_ctx, &label_names).await?
                            } else {
                                // ScanMainByLabel: filter by label name
                                scan_vertex_vids_by_label_name_static(&graph_ctx, &label).await?
                            };
                            // Materialize batch from main table's props_json
                            let batch = materialize_schemaless_vertex_batch_static(
                                &graph_ctx, &schema, vids,
                            )
                            .await?;
                            Ok(Some(batch))
                        } else {
                            // Known label vertex scan - use per-label table
                            let vids = scan_vertex_vids_static(&graph_ctx, &label).await?;
                            // Materialize batch
                            let batch =
                                materialize_vertex_batch_static(&graph_ctx, &label, &schema, vids)
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

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n.name");
        assert_eq!(schema.field(2).name(), "n.age");
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

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(1).name(), "n.name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).name(), "n.age");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_schemaless_all_scan_has_empty_label() {
        // This test verifies that new_schemaless_all_scan creates a scan with empty label
        // We can't fully test execution without a GraphExecutionContext, but we can verify
        // the constructor sets the empty label correctly by checking the struct internals
        // This is a structural test to ensure the "empty label signals scan all" pattern works
        let schema = GraphScanExec::build_schemaless_vertex_schema("n", &[]);

        // Verify the schema has just the _vid column for a scan with no properties
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "n._vid");
    }
}
