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
use crate::query::df_graph::common::{
    arrow_err, compute_plan_properties, exec_err, labels_data_type,
};
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
    Time64NanosecondBuilder, TimestampNanosecondBuilder, UInt64Builder,
};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
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
use uni_common::core::id::Vid;
use uni_common::core::schema::Schema as UniSchema;
use uni_store::backend::types::{FilterExpr, Scalar};

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

    /// Filter expression to push down (used for L0 short-circuit and
    /// single-VID Lance pushdown). For multi-VID IN-list pushdown, use
    /// `vid_list_filter` — see issue #55 PR #4.
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Multi-VID IN-list filter to push to Lance as `_vid IN (v1, v2, ...)`.
    /// Set when the planner has resolved a static set of vids from an
    /// `Expr::In { Property(_, "_vid"), List }` predicate. Bypasses the
    /// PhysicalExpr roundtrip used by `filter`. See issue #55 PR #4.
    vid_list_filter: Option<Vec<u64>>,

    /// Pre-rendered Lance filter string for indexed-property pushdown
    /// (e.g. `name = 'foo'`). AND-combined with the VID filter at scan time.
    /// Populated by the planner when an indexed-property equality / IN
    /// predicate is detected — Lance turns it into a hash-index lookup.
    /// See issue #57.
    extra_lance_filter: Option<String>,

    /// Arrow-side equivalent of `extra_lance_filter`, applied to the merged
    /// (Lance + L0) batch in-process so the scan output reflects only
    /// matching rows even when data is still in L0 (Lance pushdown alone
    /// can't reach uncommitted/unflushed rows). See issue #57.
    extra_runtime_filter: Option<Arc<dyn PhysicalExpr>>,

    /// Whether this is a schemaless scan (uses main table instead of per-label table).
    is_schemaless: bool,

    /// Output schema with materialized property columns.
    schema: SchemaRef,

    /// Cached plan properties.
    properties: Arc<PlanProperties>,

    /// Metrics for execution tracking.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphScanExec")
            .field("label", &self.label)
            .field("variable", &self.variable)
            .field("projected_properties", &self.projected_properties)
            .field(
                "vid_list_filter_len",
                &self.vid_list_filter.as_ref().map(Vec::len),
            )
            .finish()
    }
}

impl GraphScanExec {
    /// Attach a multi-VID IN-list filter to this scan, pushing
    /// `_vid IN (v1, v2, ...)` to Lance at execute time. Use this for
    /// pre-resolved vid sets (e.g. from `UNWIND $list AS e WHERE id(x)=e.field`).
    /// See issue #55 PR #4.
    pub fn with_vid_list_filter(mut self, vids: Vec<u64>) -> Self {
        self.vid_list_filter = Some(vids);
        self
    }

    /// Attach a pre-rendered Lance filter string for indexed-property
    /// pushdown. AND-combined with any VID filter at scan time. See
    /// issue #57.
    pub fn with_extra_lance_filter(mut self, filter: String) -> Self {
        self.extra_lance_filter = Some(filter);
        self
    }

    /// Attach the Arrow-side counterpart of `extra_lance_filter`. Applied to
    /// the merged (Lance + L0) batch so the scan output is index-bound even
    /// for not-yet-flushed L0 rows. See issue #57.
    pub fn with_extra_runtime_filter(mut self, filter: Arc<dyn PhysicalExpr>) -> Self {
        self.extra_runtime_filter = Some(filter);
        self
    }

    /// Whether this scan has an extra Lance filter pushed in. Used by the
    /// EXPLAIN/IndexUsage path to confirm the planner actually pushed.
    pub fn has_extra_lance_filter(&self) -> bool {
        self.extra_lance_filter.is_some()
    }

    /// Execute this scan once with a runtime-supplied list of VIDs as the
    /// pushdown filter (`_vid IN (v1, v2, ...)`). Returns a single merged
    /// `RecordBatch`. Used by `VidLookupJoinExec` (issue #55 PR #5) for
    /// cross-MATCH dynamic pushdown — the build side materializes its keys at
    /// runtime, then the probe scan runs once with those keys.
    ///
    /// Only supported for vertex and schemaless-vertex scans; edge scans
    /// have a different shape and aren't currently a join target for this
    /// optimization.
    pub(crate) async fn execute_with_vid_filter(&self, vids: &[u64]) -> DFResult<RecordBatch> {
        if self.is_schemaless {
            columnar_scan_schemaless_vertex_batch_static(
                &self.graph_ctx,
                &self.label,
                &self.variable,
                &self.projected_properties,
                &self.schema,
                &self.filter,
                Some(vids),
                self.extra_lance_filter.as_deref(),
                self.extra_runtime_filter.as_ref(),
            )
            .await
        } else {
            columnar_scan_vertex_batch_static(
                &self.graph_ctx,
                &self.label,
                &self.variable,
                &self.projected_properties,
                &self.schema,
                &self.filter,
                Some(vids),
                self.extra_lance_filter.as_deref(),
                self.extra_runtime_filter.as_ref(),
                // No per-node metric: this is the probe side of
                // `VidLookupJoinExec`, which does not expose this scan as a
                // child, so nothing would collect it (#179). The query-level
                // counters still see it.
                None,
            )
            .await
        }
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
            vid_list_filter: None,
            extra_lance_filter: None,
            extra_runtime_filter: None,
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
        Self::new_schemaless_inner(
            graph_ctx,
            label_name.into(),
            variable.into(),
            projected_properties,
            filter,
        )
    }

    /// Shared body of the schemaless vertex-scan constructors.
    ///
    /// `label` carries the variant-specific encoding: a single label name, the
    /// colon-joined multi-label set, or the empty string for "scan all".
    fn new_schemaless_inner(
        graph_ctx: Arc<GraphExecutionContext>,
        label: String,
        variable: String,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        // Filter out system columns that are already materialized as dedicated columns
        // (_vid as UInt64, _labels as List<Utf8>). If these appear in projected_properties
        // (e.g., from collect_properties_from_plan extracting _vid from filter expressions),
        // they would create duplicate columns with conflicting types.
        let projected_properties: Vec<String> = projected_properties
            .into_iter()
            .filter(|p| p != "_vid" && p != "_labels")
            .collect();

        let uni_schema = graph_ctx.storage().schema_manager().schema();
        let schema =
            Self::build_schemaless_vertex_schema(&variable, &projected_properties, &uni_schema);
        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            label,
            variable,
            projected_properties,
            filter,
            vid_list_filter: None,
            extra_lance_filter: None,
            extra_runtime_filter: None,
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
        // Encode labels as colon-separated for the stream to parse
        let encoded_labels = labels.join(":");

        Self::new_schemaless_inner(
            graph_ctx,
            encoded_labels,
            variable.into(),
            projected_properties,
            filter,
        )
    }

    /// Create a new schemaless scan for all vertices.
    ///
    /// Scans the main vertices table for all vertices regardless of label.
    /// Properties are extracted from props_json with types resolved from the schema.
    /// This is used for `MATCH (n)` without label filter.
    pub fn new_schemaless_all_scan(
        graph_ctx: Arc<GraphExecutionContext>,
        variable: impl Into<String>,
        projected_properties: Vec<String>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        // Empty label signals "scan all vertices"
        Self::new_schemaless_inner(
            graph_ctx,
            String::new(),
            variable.into(),
            projected_properties,
            filter,
        )
    }

    /// Build schema for schemaless vertex scan.
    ///
    /// Resolves property types from all labels in the schema. Falls back to
    /// LargeBinary (CypherValue encoding) for properties not found in any
    /// label's schema.
    fn build_schemaless_vertex_schema(
        variable: &str,
        properties: &[String],
        uni_schema: &uni_common::core::schema::Schema,
    ) -> SchemaRef {
        // Merge property metadata from all labels for type resolution.
        let mut merged: std::collections::HashMap<&str, &uni_common::core::schema::PropertyMeta> =
            std::collections::HashMap::new();
        for label_props in uni_schema.properties.values() {
            for (name, meta) in label_props {
                merged.entry(name.as_str()).or_insert(meta);
            }
        }

        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(format!("{}._labels", variable), labels_data_type(), true),
        ];

        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            let uni_type = merged.get(prop.as_str()).map(|meta| &meta.r#type);
            let arrow_type = uni_type
                .map(|t| t.to_arrow())
                .unwrap_or(DataType::LargeBinary);
            fields.push(property_field(&col_name, arrow_type, uni_type));
        }

        Arc::new(Schema::new(fields))
    }

    /// Build output schema for vertex scan with proper Arrow types.
    pub(crate) fn build_vertex_schema(
        variable: &str,
        label: &str,
        properties: &[String],
        uni_schema: &UniSchema,
    ) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(format!("{}._labels", variable), labels_data_type(), true),
        ];
        let label_props = uni_schema.properties.get(label);
        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            let arrow_type = resolve_property_type(prop, label_props);
            let uni_type = label_props
                .and_then(|props| props.get(prop))
                .map(|m| &m.r#type);
            fields.push(property_field(&col_name, arrow_type, uni_type));
        }
        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for GraphScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only vertex scans exist; the edge-scan path was removed as dead code.
        let scan_type = "Vertex";
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
        // Named so `collect_plan_metrics` can find it; a scan that consults no
        // index still registers the metric at zero, which is what lets
        // `index_hits: Some(0)` mean "asked and did not" rather than "unknown".
        let index_consulted =
            MetricBuilder::new(&self.metrics).counter("index_consulted", partition);

        Ok(Box::pin(GraphScanStream::new(
            self.graph_ctx.clone(),
            self.label.clone(),
            self.variable.clone(),
            self.projected_properties.clone(),
            self.is_schemaless,
            self.filter.clone(),
            self.vid_list_filter.clone(),
            self.extra_lance_filter.clone(),
            self.extra_runtime_filter.clone(),
            self.schema.clone(),
            metrics,
            index_consulted,
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

    /// Whether this is a schemaless scan.
    is_schemaless: bool,

    /// Pushed-down filter expression (used for VID short-circuit in L0 scans).
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Multi-VID IN-list filter for Lance pushdown. See issue #55 PR #4.
    vid_list_filter: Option<Vec<u64>>,

    /// Extra Lance filter string (e.g. `name = 'foo'`) for indexed-property
    /// pushdown. See issue #57.
    extra_lance_filter: Option<String>,

    /// Arrow-side equivalent of `extra_lance_filter`. See issue #57.
    extra_runtime_filter: Option<Arc<dyn PhysicalExpr>>,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: GraphScanState,

    /// Metrics.
    metrics: BaselineMetrics,

    /// Per-node count of scans that consulted a scalar index, surfaced as
    /// `OperatorStats::index_hits`.
    index_consulted: Count,
}

impl GraphScanStream {
    /// Create a new graph scan stream.
    #[expect(clippy::too_many_arguments)]
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        label: String,
        variable: String,
        properties: Vec<String>,
        is_schemaless: bool,
        filter: Option<Arc<dyn PhysicalExpr>>,
        vid_list_filter: Option<Vec<u64>>,
        extra_lance_filter: Option<String>,
        extra_runtime_filter: Option<Arc<dyn PhysicalExpr>>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        index_consulted: Count,
    ) -> Self {
        Self {
            graph_ctx,
            label,
            variable,
            properties,
            is_schemaless,
            filter,
            vid_list_filter,
            extra_lance_filter,
            extra_runtime_filter,
            schema,
            state: GraphScanState::Init,
            metrics,
            index_consulted,
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
    } else if prop == "_created_at" || prop == "_updated_at" {
        // System-managed timestamps surfaced via `created_at(n)` /
        // `updated_at(n)`. Stored on every vertex/edge by the L0 buffer
        // and the on-disk Arrow tables as Timestamp(Nanosecond, UTC).
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    } else {
        schema_props
            .and_then(|props| props.get(prop))
            .map(|meta| meta.r#type.to_arrow())
            .unwrap_or(DataType::LargeBinary)
    }
}

/// Build a scan-output `Field`, tagging raw `Bytes` columns for the final read.
///
/// `DataType::Bytes`, `DataType::CypherValue`, and `DataType::Duration` all map to Arrow
/// `LargeBinary`, but only `Bytes` stores raw (un-codec'd) bytes. The projection read
/// (`record_batches_to_rows`) cannot tell them apart from the Arrow type alone, so it
/// would decode a raw `Bytes` column with the CypherValue MessagePack codec and corrupt
/// it. Stamping `uni_raw_bytes=true` lets the read route the column to the raw-bytes
/// branch of `arrow_to_value` instead.
pub(crate) fn property_field(
    col_name: &str,
    arrow_type: DataType,
    uni_type: Option<&uni_common::DataType>,
) -> Field {
    let field = Field::new(col_name, arrow_type, true);
    if matches!(uni_type, Some(uni_common::DataType::Bytes)) {
        field.with_metadata(std::collections::HashMap::from([(
            "uni_raw_bytes".to_string(),
            "true".to_string(),
        )]))
    } else {
        field
    }
}

// ============================================================================
// Columnar-first scan helpers
// ============================================================================

/// MVCC deduplication: keep only the highest-version row for each `_vid`.
///
/// Sorts by (_vid ASC, _version DESC), then keeps the first occurrence of each
/// _vid (= the highest version). This is a pure Arrow-compute operation.
#[cfg(test)]
fn mvcc_dedup_batch(batch: &RecordBatch) -> DFResult<RecordBatch> {
    mvcc_dedup_batch_by(batch, "_vid")
}

/// Dedup a Lance batch and return `Some` only when rows remain.
///
/// Wraps the common pattern of dedup + empty-check that appears in every
/// columnar scan path (vertex, edge, schemaless).
fn mvcc_dedup_to_option(
    batch: Option<RecordBatch>,
    id_column: &str,
) -> DFResult<Option<RecordBatch>> {
    match batch {
        Some(b) => {
            let deduped = mvcc_dedup_batch_by(&b, id_column)?;
            Ok(if deduped.num_rows() > 0 {
                Some(deduped)
            } else {
                None
            })
        }
        None => Ok(None),
    }
}

/// Merge a deduped Lance batch with an L0 batch, re-deduplicating the combined
/// result. Returns an empty batch (against `output_schema`) when both inputs
/// are empty.
///
/// `counters`, when present, records how many rows each tier contributed. This
/// is the one place in the scan path that already knows the answer — the
/// `match` below exists precisely to distinguish storage-only, L0-only and
/// both — so counting here costs a pair of adds and needs no new branching.
/// Rows are counted **before** the combined dedup, so the numbers describe what
/// each tier *served*, not what survived MVCC resolution.
fn merge_lance_and_l0(
    lance_deduped: Option<RecordBatch>,
    l0_batch: RecordBatch,
    internal_schema: &SchemaRef,
    id_column: &str,
    counters: Option<&Arc<uni_store::QueryCounters>>,
) -> DFResult<Option<RecordBatch>> {
    let has_l0 = l0_batch.num_rows() > 0;
    if let Some(c) = counters {
        let lance_rows = lance_deduped.as_ref().map_or(0, |b| b.num_rows());
        let l0_rows = l0_batch.num_rows();
        c.add_storage_rows(lance_rows);
        c.add_l0_rows(l0_rows);
        c.add_rows_scanned(lance_rows + l0_rows);
    }
    match (lance_deduped, has_l0) {
        (Some(lance), true) => {
            let combined = arrow::compute::concat_batches(internal_schema, &[lance, l0_batch])
                .map_err(arrow_err)?;
            Ok(Some(mvcc_dedup_batch_by(&combined, id_column)?))
        }
        (Some(lance), false) => Ok(Some(lance)),
        (None, true) => Ok(Some(l0_batch)),
        (None, false) => Ok(None),
    }
}

/// Drop rows superseded by a newer persisted version that the pushed
/// property predicate filtered out (issue #57 × MVCC-append tables).
///
/// Lance evaluates a pushed property predicate per ROW, before the per-vid
/// max-`_version` pick — so when a vid's property was rewritten and
/// re-flushed, its CURRENT row fails the predicate and never reaches the
/// dedup, while the stale still-matching row wins it by default. Re-reads
/// `_vid`/`_version` for the candidate vids WITHOUT the property predicate
/// (per-label table when `label_table` is `Some`, the main vertex table
/// otherwise) and keeps only rows carrying their vid's true maximum
/// persisted version. Must run on the RAW filtered batch, before
/// [`mvcc_dedup_to_option`].
async fn drop_superseded_pushdown_rows(
    storage: &Arc<uni_store::storage::manager::StorageManager>,
    label_table: Option<&str>,
    batch: RecordBatch,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    let (Some(vid_col), Some(ver_col)) = (
        batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
        batch
            .column_by_name("_version")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
    ) else {
        return Err(datafusion::error::DataFusionError::Execution(
            "pushdown version verification: scan batch missing _vid/_version".to_string(),
        ));
    };

    let mut candidates: Vec<u64> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();
    for i in 0..vid_col.len() {
        let vid = vid_col.value(i);
        if seen.insert(vid) {
            candidates.push(vid);
        }
    }

    // True max persisted version per candidate vid — unfiltered apart from
    // the vid list, so rewritten-key rows and deletion tombstones are seen.
    // Chunked to bound the `_vid IN (…)` filter-string size.
    const VERIFY_CHUNK: usize = 1000;
    let mut max_ver: HashMap<u64, u64> = HashMap::with_capacity(candidates.len());
    for chunk in candidates.chunks(VERIFY_CHUNK) {
        let filter = FilterExpr::one_of("_vid", chunk.iter().map(|v| Scalar::UInt(*v)));
        let scanned = match label_table {
            Some(label) => {
                storage
                    .scan_vertex_table(label, &["_vid", "_version"], Some(&filter))
                    .await
            }
            None => {
                storage
                    .scan_main_vertex_table(&["_vid", "_version"], Some(&filter))
                    .await
            }
        }
        .map_err(exec_err)?;
        let Some(vbatch) = scanned else { continue };
        let (Some(v_vid), Some(v_ver)) = (
            vbatch
                .column_by_name("_vid")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
            vbatch
                .column_by_name("_version")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>()),
        ) else {
            return Err(datafusion::error::DataFusionError::Execution(
                "pushdown version verification: rescan missing _vid/_version".to_string(),
            ));
        };
        for i in 0..v_vid.len() {
            let entry = max_ver.entry(v_vid.value(i)).or_insert(0);
            *entry = (*entry).max(v_ver.value(i));
        }
    }

    let keep: arrow_array::BooleanArray = (0..batch.num_rows())
        .map(|i| {
            Some(
                max_ver
                    .get(&vid_col.value(i))
                    .is_none_or(|&max| ver_col.value(i) >= max),
            )
        })
        .collect();
    arrow::compute::filter_record_batch(&batch, &keep).map_err(arrow_err)
}

/// Push `col_name` into `columns` if not already present.
///
/// Avoids the verbose `!columns.contains(&col_name.to_string())` pattern
/// that creates a temporary `String` allocation on every check.
fn push_column_if_absent(columns: &mut Vec<String>, col_name: &str) {
    if !columns.iter().any(|c| c == col_name) {
        columns.push(col_name.to_string());
    }
}

/// Extract a property value from an overflow_json CypherValue blob.
///
/// Returns the raw CypherValue bytes for `prop` if found in the blob,
/// or `None` if the blob is null or the key is absent.
fn extract_from_overflow_blob(
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    row: usize,
    prop: &str,
) -> Option<Vec<u8>> {
    let arr = overflow_arr?;
    if arr.is_null(row) {
        return None;
    }
    uni_common::cypher_value_codec::extract_map_entry_raw(arr.value(row), prop)
}

/// Build a `LargeBinary` column by extracting a property from overflow_json
/// blobs, with L0 buffer overlay.
///
/// For each row, checks L0 buffers first (later buffers take precedence).
/// If the property is not in L0, falls back to extracting from the
/// overflow_json CypherValue blob.
fn build_overflow_property_column(
    num_rows: usize,
    vid_arr: &UInt64Array,
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    prop: &str,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> ArrayRef {
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));

        // Check L0 buffers (later overwrites earlier)
        let l0_val = resolve_l0_property(&vid, prop, l0_ctx);

        if let Some(val_opt) = l0_val {
            append_value_as_cypher_binary(&mut builder, val_opt.as_ref());
        } else if let Some(bytes) = extract_from_overflow_blob(overflow_arr, i, prop) {
            builder.append_value(&bytes);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}

/// Resolve a property value from the L0 visibility chain.
///
/// Returns `Some(Some(val))` when the property exists with a non-null value,
/// `Some(None)` when it exists but is null, and `None` when no L0 buffer
/// has the property.
fn resolve_l0_property(
    vid: &Vid,
    prop: &str,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> Option<Option<Value>> {
    let mut result = None;
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        if let Some(props) = guard.vertex_properties.get(vid)
            && let Some(val) = props.get(prop)
        {
            result = Some(Some(val.clone()));
        }
    }
    result
}

/// Append a `Value` to a `LargeBinaryBuilder` as CypherValue bytes.
///
/// Encoded directly via the CypherValue codec so typed values (temporals,
/// nested lists/maps) round-trip losslessly. Null values produce null entries.
fn append_value_as_cypher_binary(
    builder: &mut arrow_array::builder::LargeBinaryBuilder,
    val: Option<&Value>,
) {
    match val {
        Some(v) if !v.is_null() => {
            builder.append_value(uni_common::cypher_value_codec::encode(v));
        }
        _ => builder.append_null(),
    }
}

/// Build the `_all_props` column by overlaying L0 buffer properties onto
/// the batch's `props_json` column.
///
/// For each row, decodes the stored CypherValue blob, merges in any L0 buffer
/// properties (in visibility order: pending → current → transaction), and
/// re-encodes the result. This ensures `properties()` and `keys()` reflect
/// uncommitted L0 mutations.
fn build_all_props_column_with_l0_overlay(
    num_rows: usize,
    vid_arr: &UInt64Array,
    props_arr: Option<&arrow_array::LargeBinaryArray>,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> ArrayRef {
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));

        // 1. Decode props_json blob from storage (stays in `Value` space so
        //    typed values such as temporals are preserved).
        let mut merged_props: HashMap<String, Value> = HashMap::new();
        if let Some(arr) = props_arr
            && !arr.is_null(i)
            && let Ok(uni_common::Value::Map(map)) =
                uni_common::cypher_value_codec::decode(arr.value(i))
        {
            merged_props.extend(map);
        }

        // 2. Overlay L0 properties (visibility order: pending → current → transaction)
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if let Some(l0_props) = guard.vertex_properties.get(&vid) {
                for (k, v) in l0_props {
                    merged_props.insert(k.clone(), v.clone());
                }
            }
        }

        // 3. Encode merged result directly via the CypherValue codec.
        if merged_props.is_empty() {
            builder.append_null();
        } else {
            builder.append_value(uni_common::cypher_value_codec::encode(&Value::Map(
                merged_props,
            )));
        }
    }
    Arc::new(builder.finish())
}

/// Build `_all_props` for a schema-based scan by merging:
/// 1. Schema-defined columns from the batch
/// 2. Overflow_json properties
/// 3. L0 buffer properties
fn build_all_props_column_for_schema_scan(
    batch: &RecordBatch,
    vid_arr: &UInt64Array,
    overflow_arr: Option<&arrow_array::LargeBinaryArray>,
    projected_properties: &[String],
    l0_ctx: &crate::query::df_graph::L0Context,
) -> ArrayRef {
    // Collect schema-defined property column names (non-internal, non-overflow, non-_all_props)
    let schema_props: Vec<&str> = projected_properties
        .iter()
        .filter(|p| *p != "overflow_json" && *p != "_all_props" && !p.starts_with('_'))
        .map(String::as_str)
        .collect();

    let num_rows = batch.num_rows();
    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
    for i in 0..num_rows {
        let vid = Vid::from(vid_arr.value(i));
        // Build the merged map in `Value` space so typed values (temporals,
        // nested lists/maps) are preserved through to the CypherValue blob.
        let mut merged_props: HashMap<String, Value> = HashMap::new();

        // 1. Schema-defined columns
        for &prop in &schema_props {
            if let Some(col) = batch.column_by_name(prop) {
                let val = uni_store::storage::arrow_convert::arrow_to_value(col.as_ref(), i, None);
                if !val.is_null() {
                    merged_props.insert(prop.to_string(), val);
                }
            }
        }

        // 2. Overflow_json properties
        if let Some(arr) = overflow_arr
            && !arr.is_null(i)
            && let Ok(uni_common::Value::Map(map)) =
                uni_common::cypher_value_codec::decode(arr.value(i))
        {
            merged_props.extend(map);
        }

        // 3. L0 buffer overlay (pending → current → transaction)
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if let Some(l0_props) = guard.vertex_properties.get(&vid) {
                for (k, v) in l0_props {
                    merged_props.insert(k.clone(), v.clone());
                }
            }
        }

        if merged_props.is_empty() {
            builder.append_null();
        } else {
            builder.append_value(uni_common::cypher_value_codec::encode(&Value::Map(
                merged_props,
            )));
        }
    }
    Arc::new(builder.finish())
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
    let indices = arrow::compute::lexsort_to_indices(&sort_columns, None).map_err(arrow_err)?;

    // Reorder all columns by sorted indices
    let sorted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<Result<_, _>>()
        .map_err(arrow_err)?;
    let sorted = RecordBatch::try_new(batch.schema(), sorted_columns).map_err(arrow_err)?;

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
    arrow::compute::filter_record_batch(&sorted, &mask).map_err(arrow_err)
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
    arrow::compute::filter_record_batch(batch, &mask).map_err(arrow_err)
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
    arrow::compute::filter_record_batch(batch, &mask).map_err(arrow_err)
}

/// Drop rows for a known-label scan whose newest L0 label-overwrite no longer
/// includes the scanned label(s).
///
/// A flushed vertex's stored `labels` array still lists a label after a
/// `REMOVE n:Label` — the removal only updated L0. The label-scan candidate set
/// unions that stale flushed row, and neither the `_deleted` nor the vid-tombstone
/// filter drops it. When the newest L0 buffer carrying the vid flagged it in
/// `vertex_label_overwrites` (a `SET`/`REMOVE` that resolved its full label set),
/// that set is authoritative: keep the row only if it still contains every
/// requested label. Otherwise the label was resurrected in `MATCH (n:Label)`.
///
/// `label` may be `"A:B"` (all required) or empty (bare `MATCH (n)` — nothing to
/// filter). Mirrors the multi-label membership check in
/// `build_l0_schemaless_vertex_batch`.
///
/// # Errors
/// Returns an error if the `_vid` column is missing or the mask filter fails.
fn filter_l0_label_overwrites(
    batch: &RecordBatch,
    label: &str,
    l0_ctx: &crate::query::df_graph::L0Context,
) -> DFResult<RecordBatch> {
    if batch.num_rows() == 0 || label.is_empty() {
        return Ok(batch.clone());
    }
    let required: Vec<&str> = label.split(':').collect();

    // vid -> resolved label set from the NEWEST buffer that marked it as a full
    // label overwrite. `iter_l0_buffers` yields oldest -> newest, so later writes
    // win.
    let mut overwritten: HashMap<u64, Vec<String>> = HashMap::new();
    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        for vid in guard.vertex_label_overwrites.iter() {
            let labels = guard.vertex_labels.get(vid).cloned().unwrap_or_default();
            overwritten.insert(vid.as_u64(), labels);
        }
    }
    if overwritten.is_empty() {
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
        .map(|i| match overwritten.get(&vid_col.value(i)) {
            // The vid's newest overwrite resolved its full label set: keep only
            // if that set still contains every requested label.
            Some(resolved) => required.iter().all(|lf| resolved.iter().any(|l| l == lf)),
            // No overwrite for this vid: the stored (Lance or L0) labels stand.
            None => true,
        })
        .collect();
    let mask = arrow_array::BooleanArray::from(keep);
    arrow::compute::filter_record_batch(batch, &mask).map_err(arrow_err)
}

/// Extract a target VID from a DataFusion physical filter expression.
///
/// Looks for patterns like `_vid = <uint64_literal>` or `<uint64_literal> = _vid`
/// in the top-level expression or as a conjunct of an AND chain. Returns the first
/// VID literal found, or `None` if the filter does not contain such a pattern.
///
/// This also handles `CAST(literal AS UInt64)` which DataFusion may insert when
/// the original literal is Int64.
fn extract_vid_from_physical_filter(filter: &Arc<dyn PhysicalExpr>) -> Option<u64> {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;

    // Try to match this expression as `_vid = literal`
    if let Some(bin) = filter.as_any().downcast_ref::<BinaryExpr>() {
        if bin.op() == &Operator::Eq {
            // Check both directions: col = lit and lit = col
            if let Some(vid) = try_extract_vid_eq(bin.left(), bin.right()) {
                return Some(vid);
            }
            if let Some(vid) = try_extract_vid_eq(bin.right(), bin.left()) {
                return Some(vid);
            }
        }
        // Recurse into AND conjuncts
        if bin.op() == &Operator::And {
            if let Some(vid) = extract_vid_from_physical_filter(bin.left()) {
                return Some(vid);
            }
            return extract_vid_from_physical_filter(bin.right());
        }
    }
    None
}

/// Try to extract a VID value from a `(column_expr, value_expr)` pair where
/// `column_expr` is a `Column` named `_vid` and `value_expr` is a UInt64 or
/// non-negative Int64 literal (possibly wrapped in a CAST to UInt64).
fn try_extract_vid_eq(
    col_side: &Arc<dyn PhysicalExpr>,
    val_side: &Arc<dyn PhysicalExpr>,
) -> Option<u64> {
    use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};

    // Check that col_side is Column("_vid") or Column("variable._vid")
    let col = col_side.as_any().downcast_ref::<Column>()?;
    if col.name() != "_vid" && !col.name().ends_with("._vid") {
        return None;
    }

    // Try direct literal
    if let Some(lit) = val_side.as_any().downcast_ref::<Literal>() {
        return scalar_to_u64(lit.value());
    }

    // Try CAST(literal AS UInt64)
    if let Some(cast) = val_side.as_any().downcast_ref::<CastExpr>()
        && let Some(lit) = cast.expr().as_any().downcast_ref::<Literal>()
    {
        return scalar_to_u64(lit.value());
    }

    None
}

/// Convert a `ScalarValue` to `u64` if it is a non-negative integer type.
fn scalar_to_u64(sv: &datafusion::common::ScalarValue) -> Option<u64> {
    use datafusion::common::ScalarValue;
    match sv {
        ScalarValue::UInt64(Some(v)) => Some(*v),
        ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as u64),
        ScalarValue::UInt32(Some(v)) => Some(*v as u64),
        ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as u64),
        _ => None,
    }
}

/// Build a RecordBatch from L0 buffer data for a given label, matching the
/// Lance query's column set.
///
/// Merges L0 buffers in visibility order (pending_flush → current → transaction),
/// with later buffers overwriting earlier ones for the same VID.
///
/// When `target_vids` is `Some`, only those VIDs are collected (direct HashMap
/// lookups instead of iterating all VIDs for the label). This must mirror the
/// Lance-side VID pushdown — otherwise L0-only (unflushed) rows bypass the
/// filter and the scan emits the full label table. See issue #72 item 1.
fn build_l0_vertex_batch(
    l0_ctx: &crate::query::df_graph::L0Context,
    label: &str,
    lance_schema: &SchemaRef,
    label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
    target_vids: Option<&[u64]>,
) -> DFResult<RecordBatch> {
    // Collect all L0 vertex data, merging in visibility order
    let mut vid_data: HashMap<u64, (Properties, u64)> = HashMap::new(); // vid -> (props, version)
    let mut tombstones: HashSet<u64> = HashSet::new();
    // System-managed timestamps: created_at takes the earliest seen
    // timestamp across L0 buffers (preserving the original creation
    // moment when a row has been touched in multiple buffers); updated_at
    // takes the latest (most recent write). Used by `created_at(n)` /
    // `updated_at(n)` Cypher functions.
    let mut vid_created_at: HashMap<u64, i64> = HashMap::new();
    let mut vid_updated_at: HashMap<u64, i64> = HashMap::new();

    for l0 in l0_ctx.iter_l0_buffers() {
        let guard = l0.read();
        // Collect tombstones
        for vid in guard.vertex_tombstones.iter() {
            tombstones.insert(vid.as_u64());
        }
        // Collect vertices — restrict to target_vids (single- or multi-VID
        // pushdown from id(x) = ? / id(x) IN [...]) when set, else all
        // vertices for the label. See issue #72 item 1: without this filter,
        // freshly-inserted L0 rows bypass the IN-list pushdown that Lance
        // already honors, defeating the optimization.
        let candidate_vids: Vec<Vid> = if let Some(tvs) = target_vids {
            let mut out = Vec::with_capacity(tvs.len());
            for &tv in tvs {
                let vid = Vid::from(tv);
                if guard.vertex_properties.contains_key(&vid)
                    && (label.is_empty()
                        || guard
                            .label_to_vids
                            .get(label)
                            .is_some_and(|s| s.contains(&vid)))
                {
                    out.push(vid);
                }
            }
            out
        } else {
            guard.vids_for_label(label)
        };
        for vid in candidate_vids {
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
            // Merge system timestamps: earliest creation, latest update
            if let Some(&ts) = guard.vertex_created_at.get(&vid) {
                vid_created_at
                    .entry(vid_u64)
                    .and_modify(|cur| {
                        if ts < *cur {
                            *cur = ts;
                        }
                    })
                    .or_insert(ts);
            }
            if let Some(&ts) = guard.vertex_updated_at.get(&vid) {
                vid_updated_at
                    .entry(vid_u64)
                    .and_modify(|cur| {
                        if ts > *cur {
                            *cur = ts;
                        }
                    })
                    .or_insert(ts);
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
            "_created_at" => {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::new().with_timezone("UTC");
                for v in &vids {
                    match vid_created_at.get(v) {
                        Some(&ts) => builder.append_value(ts),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "_updated_at" => {
                let mut builder =
                    arrow_array::builder::TimestampNanosecondBuilder::new().with_timezone("UTC");
                for v in &vids {
                    match vid_updated_at.get(v) {
                        Some(&ts) => builder.append_value(ts),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "overflow_json" => {
                // Collect non-schema properties as CypherValue
                let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                for vid_u64 in &vids {
                    let (props, _) = &vid_data[vid_u64];
                    let mut overflow: HashMap<String, Value> = HashMap::new();
                    for (k, v) in props {
                        if k == "ext_id" || k.starts_with('_') {
                            continue;
                        }
                        if !schema_prop_names.contains(k.as_str()) {
                            overflow.insert(k.clone(), v.clone());
                        }
                    }
                    if overflow.is_empty() {
                        builder.append_null();
                    } else {
                        builder.append_value(uni_common::cypher_value_codec::encode(&Value::Map(
                            overflow,
                        )));
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "_labels" => {
                // Rows in this batch exist only in L0, so there is no stored
                // label set to carry. Emitting nulls is not a shortcut: a null
                // row makes `build_labels_column_for_known_label` fall back to
                // `[label]`, and its L0 overlay then resolves the true set from
                // `vertex_labels` — the same path these rows took before
                // `_labels` joined the projection, and the one that is already
                // correct for unflushed vertices.
                //
                // Without this arm the column falls through to
                // `build_l0_property_column`, which does not handle
                // `List<Utf8>`, and `RecordBatch::try_new` below fails.
                let mut builder = ListBuilder::new(StringBuilder::new())
                    .with_field(Arc::new(Field::new("item", DataType::Utf8, true)));
                for _ in 0..num_rows {
                    builder.append_null();
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

    RecordBatch::try_new(lance_schema.clone(), columns).map_err(arrow_err)
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

/// Build the `_labels` column for known-label vertices.
///
/// Reads `_labels` from the stored Lance batch if available. Falls back to
/// `[label]` when the column is absent (legacy data). Additional labels from
/// L0 buffers are merged in.
fn build_labels_column_for_known_label(
    vid_arr: &UInt64Array,
    label: &str,
    l0_ctx: &crate::query::df_graph::L0Context,
    batch_labels_col: Option<&arrow_array::ListArray>,
) -> DFResult<ArrayRef> {
    use uni_store::storage::arrow_convert::labels_from_list_array;

    let mut labels_builder = ListBuilder::new(StringBuilder::new());

    for i in 0..vid_arr.len() {
        let vid = Vid::from(vid_arr.value(i));

        // Start with labels from the stored column, falling back to [label]
        let mut labels = match batch_labels_col {
            Some(list_arr) => {
                let stored = labels_from_list_array(list_arr, i);
                if stored.is_empty() {
                    vec![label.to_string()]
                } else {
                    stored
                }
            }
            None => vec![label.to_string()],
        };

        // Ensure the scanned label is present (defensive)
        if !labels.iter().any(|l| l == label) {
            labels.push(label.to_string());
        }

        // Merge additional labels from L0 buffers, honoring label-overwrite
        // markers: a vid flagged in `vertex_label_overwrites` has its full label
        // set resolved by a SET/REMOVE, which REPLACES the stored labels (newest
        // buffer wins) — so a REMOVE of the scanned label is respected rather
        // than resurrected by the union or the defensive push above.
        let mut overwrite_labels: Option<Vec<String>> = None;
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if guard.vertex_label_overwrites.contains(&vid) {
                overwrite_labels = guard.vertex_labels.get(&vid).cloned();
            } else if let Some(l0_labels) = guard.vertex_labels.get(&vid) {
                for lbl in l0_labels {
                    if !labels.contains(lbl) {
                        labels.push(lbl.clone());
                    }
                }
            }
        }
        if let Some(resolved) = overwrite_labels {
            labels = resolved;
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

    // 2. {var}._labels — read from stored column, overlay L0 additions
    let batch_labels_col = batch
        .column_by_name("_labels")
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>());
    let labels_col = build_labels_column_for_known_label(vid_arr, label, l0_ctx, batch_labels_col)?;
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
        } else if prop == "_all_props" {
            // Build _all_props from overflow_json + L0 overlay.
            // Fast path: if no L0 buffer has vertex property mutations AND
            // there are no schema columns to merge, pass through overflow_json.
            let any_l0_has_vertex_props = l0_ctx.iter_l0_buffers().any(|l0| {
                let guard = l0.read();
                !guard.vertex_properties.is_empty()
            });
            // Check if this label has schema-defined columns (besides system columns)
            let has_schema_cols = projected_properties
                .iter()
                .any(|p| p != "overflow_json" && p != "_all_props" && !p.starts_with('_'));

            if !any_l0_has_vertex_props && !has_schema_cols {
                // No L0 mutations, no schema cols to merge: overflow_json IS _all_props
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
                // Need to merge: schema columns + overflow_json + L0 overlay
                let col = build_all_props_column_for_schema_scan(
                    batch,
                    vid_arr,
                    overflow_arr,
                    projected_properties,
                    l0_ctx,
                );
                columns.push(col);
            }
        } else {
            match batch.column_by_name(prop) {
                Some(col) => columns.push(col.clone()),
                None => {
                    // Column missing in Lance -- extract from overflow_json
                    // CypherValue blob with L0 overlay
                    let col = build_overflow_property_column(
                        batch.num_rows(),
                        vid_arr,
                        overflow_arr,
                        prop,
                        l0_ctx,
                    );
                    columns.push(col);
                }
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns).map_err(arrow_err)
}

/// Columnar-first vertex scan: single Lance query with MVCC dedup and L0 overlay.
///
/// Replaces the two-phase `scan_vertex_vids_static()` + `materialize_vertex_batch_static()`
/// for known-label vertex scans. Reads all needed columns in a single Lance query,
/// performs MVCC dedup via Arrow compute, merges L0 buffer data, filters tombstones,
/// and maps to the output schema.
#[expect(clippy::too_many_arguments)]
/// Hydrate `vids` through the columnar scan path, aligned to `vids` order.
///
/// The traversal used to reach target properties through
/// `PropertyManager::get_batch_vertex_props*`, which scans the same rows and
/// then shreds the `RecordBatch` into a `HashMap<Vid, HashMap<String, Value>>`
/// for the caller to walk back into an Arrow array. That cost scales with the
/// target *table* rather than with the rows produced: growing a target table 5x
/// with rows no edge reaches raised a traversal's peak 10.6x while its output
/// stayed at 60,000 rows, and reading one column cost 86x the scan path over
/// the same data (#209).
///
/// This routes the same request through the scan path instead, which already
/// does the Lance read, MVCC dedup, L0 merge and tombstone filtering in Arrow.
/// Reusing it rather than adding a storage-side columnar API is deliberate:
/// `uni-store` cannot see this module, so a `PropertyManager` variant would
/// have to reimplement version ranking and the L0 overlay — a second
/// implementation of the part where a mistake is a wrong answer.
///
/// # Ordering
///
/// The scan returns rows in scan order and omits vids with no visible row, so
/// the result is gathered back into `vids` order here. A vid with no row — not
/// visible under this snapshot — yields null in every column, which is how the
/// map API's "absent from the map" signal survives. Duplicate vids in `vids`
/// are fine: each occurrence gathers the same row.
pub(crate) async fn hydrate_vids_columnar(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    properties: &[String],
    vids: &[Vid],
) -> DFResult<Vec<ArrayRef>> {
    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let output_schema =
        GraphScanExec::build_vertex_schema(variable, label, properties, &uni_schema);

    let raw: Vec<u64> = vids.iter().map(|v| v.as_u64()).collect();
    let batch = columnar_scan_vertex_batch_static(
        graph_ctx,
        label,
        variable,
        properties,
        &output_schema,
        &None,
        Some(&raw),
        None,
        None,
        None,
    )
    .await?;

    // Map each returned vid to its row, then gather. One u64 hash per row,
    // against one HashMap<String, Value> allocation per row on the old path.
    let vid_col = batch
        .column_by_name(&format!("{variable}._vid"))
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "columnar hydration returned no _vid column".to_string(),
            )
        })?;
    let mut row_of: HashMap<u64, u32> = HashMap::with_capacity(vid_col.len());
    for row in 0..vid_col.len() {
        if !vid_col.is_null(row) {
            // A later row wins, matching the scan path's own MVCC dedup, which
            // has already reduced this to one row per vid.
            row_of.insert(vid_col.value(row), row as u32);
        }
    }
    let indices: arrow_array::UInt32Array = raw
        .iter()
        .map(|vid| row_of.get(vid).copied())
        .collect::<Vec<Option<u32>>>()
        .into();

    // Skip `_vid`/`_labels`; the caller wants the property columns only, in the
    // order it asked for them.
    let mut columns = Vec::with_capacity(properties.len());
    for (idx, _) in properties.iter().enumerate() {
        let col = batch.column(idx + 2);
        columns.push(arrow::compute::take(col.as_ref(), &indices, None).map_err(arrow_err)?);
    }
    Ok(columns)
}

pub(crate) async fn columnar_scan_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
    filter: &Option<Arc<dyn PhysicalExpr>>,
    vid_list_filter: Option<&[u64]>,
    extra_lance_filter: Option<&str>,
    extra_runtime_filter: Option<&Arc<dyn PhysicalExpr>>,
    // Per-node sink for `index_hits`. Separate from the query-level counters
    // because `collect_plan_metrics` reports per operator: copying a
    // query-wide total onto every node would print the same number on a
    // projection as on the scan that did the work.
    index_consulted: Option<&Count>,
) -> DFResult<RecordBatch> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();
    let uni_schema = storage.schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    // Extract target VID from filter for short-circuit lookup. Single-VID
    // pushdown is from the WHERE-clause `id(x) = $literal` path; multi-VID
    // pushdown is from the IN-list path (`UNWIND ... WHERE id(x) = e.field`,
    // see issue #55 PR #4).
    let target_vid = filter.as_ref().and_then(extract_vid_from_physical_filter);

    // Build the list of columns to request from Lance
    let mut lance_columns: Vec<String> = vec![
        "_vid".to_string(),
        "_deleted".to_string(),
        "_version".to_string(),
    ];
    // `_labels` is REQUIRED, not a projection nicety. Without it
    // `build_labels_column_for_known_label` fabricates `[label]`, and that
    // fabricated set is what the executor writes back — truncating a
    // multi-label vertex on DELETE, SET/REMOVE label, and even a plain
    // `SET n.prop`, as well as returning a wrong `labels(n)`.
    //
    // Requesting it is safe on legacy tables that predate the column:
    // `StorageManager::scan_vertex_table_counted` narrows the projection to
    // physically-present columns, and the builder's `[label]` fallback covers
    // the absence.
    push_column_if_absent(&mut lance_columns, "_labels");
    for prop in projected_properties {
        if prop == "overflow_json" {
            push_column_if_absent(&mut lance_columns, "overflow_json");
        } else if prop == "_created_at" || prop == "_updated_at" {
            // System-managed timestamps live on every vertex table regardless
            // of label schema. Request them directly from Lance.
            push_column_if_absent(&mut lance_columns, prop);
        } else {
            let exists_in_schema = label_props.is_some_and(|lp| lp.contains_key(prop));
            if exists_in_schema {
                push_column_if_absent(&mut lance_columns, prop);
            }
        }
    }

    // Ensure overflow_json is present when any projected property is not in the schema
    // (excluding system-managed columns like `_created_at` / `_updated_at`).
    let needs_overflow = projected_properties.iter().any(|p| {
        p == "overflow_json"
            || (!matches!(p.as_str(), "_created_at" | "_updated_at")
                && !label_props.is_some_and(|lp| lp.contains_key(p)))
    });
    if needs_overflow {
        push_column_if_absent(&mut lance_columns, "overflow_json");
    }

    // Push _vid filter to Lance for O(log N) BTree index lookup instead of full scan.
    // Prefer the multi-VID list (formats as `_vid IN (...)`); fall back to
    // single-VID `_vid = N` from the WHERE-clause path. AND-combined with
    // any indexed-property pushdown (issue #57).
    let vid_part = match (vid_list_filter, target_vid) {
        (Some(vs), _) if !vs.is_empty() => Some(FilterExpr::one_of(
            "_vid",
            vs.iter().map(|v| Scalar::UInt(*v)),
        )),
        (_, Some(v)) => Some(FilterExpr::equals("_vid", Scalar::UInt(v))),
        _ => None,
    };
    // `extra_lance_filter` arrives as rendered SQL from the planner's
    // hash-index pushdown, so it stays `Raw` — nothing in the engine parses it.
    let combined_filter = match (vid_part, extra_lance_filter) {
        (Some(v), Some(e)) => Some(FilterExpr::all([v, FilterExpr::Raw(e.to_string())])),
        (Some(v), None) => Some(v),
        (None, Some(e)) => Some(FilterExpr::Raw(e.to_string())),
        (None, None) => None,
    };
    let lance_columns_refs: Vec<&str> = lance_columns.iter().map(|s| s.as_str()).collect();

    // M5h.2: route through plugin Storage if one is registered for
    // this label. v1 ships reads only — writes still go to native
    // backend. v1 ignores `combined_filter` when delegating (the
    // planner re-filters via the surrounding Filter node); per-plugin
    // filter pushdown is a v1.1 follow-up (`TODO(M5h.2-filter)`).
    let plugin_batch: Option<arrow::record_batch::RecordBatch> = match graph_ctx.plugin_registry() {
        Some(reg) => match reg.lookup_label_storage(label) {
            Some(plugin_storage) => {
                let mut stream = plugin_storage.read_batch(label, None).await.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "plugin Storage::read_batch({label}) failed: {} (code 0x{:x})",
                        e.message, e.code
                    ))
                })?;
                use futures::StreamExt;
                let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
                let mut schema_ref: Option<SchemaRef> = None;
                while let Some(b) = stream.next().await {
                    let b = b.map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "plugin Storage stream({label}) errored: {e}"
                        ))
                    })?;
                    if schema_ref.is_none() {
                        schema_ref = Some(b.schema());
                    }
                    batches.push(b);
                }
                if let Some(s) = schema_ref {
                    Some(arrow::compute::concat_batches(&s, &batches).map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "plugin Storage concat({label}) failed: {e}"
                        ))
                    })?)
                } else {
                    None
                }
            }
            None => None,
        },
        None => None,
    };

    // Track whether the batch came through the property-filtered native scan:
    // plugin batches ignore `combined_filter` (re-filtered by the planner), so
    // they need no stale-version verification.
    let (lance_batch, pushdown_filtered) = match plugin_batch {
        Some(b) => (Some(b), false),
        None => (
            {
                // A scan-local counter set, merged into the query's afterwards.
                // Taking a delta on the shared set instead would misattribute
                // whenever two scans of the same query overlap; `merge_from`
                // exists for exactly this fan-out.
                let scan_local = Arc::new(uni_store::QueryCounters::new());
                let batch = storage
                    .scan_vertex_table_counted(
                        label,
                        &lance_columns_refs,
                        combined_filter.as_ref(),
                        Some(&scan_local),
                    )
                    .await
                    .map_err(exec_err)?;
                if let Some(q) = graph_ctx.counters() {
                    q.merge_from(&scan_local);
                }
                if let Some(m) = index_consulted {
                    m.add(scan_local.index_scans() as usize);
                }
                batch
            },
            extra_lance_filter.is_some(),
        ),
    };

    // A pushed property predicate hides a vid's CURRENT row from the scan when
    // that row no longer matches (MVCC-append: the stale still-matching row
    // would win the dedup by default) — drop superseded rows first.
    let lance_batch = match (lance_batch, pushdown_filtered) {
        (Some(b), true) => Some(drop_superseded_pushdown_rows(storage, Some(label), b).await?),
        (b, _) => b,
    };

    // MVCC dedup the Lance batch
    let lance_deduped = mvcc_dedup_to_option(lance_batch, "_vid")?;

    // Build the internal Lance schema for L0 batch construction.
    // Use the Lance batch schema if available, otherwise build from scratch.
    let internal_schema = match &lance_deduped {
        Some(batch) => batch.schema(),
        None => {
            let mut fields = vec![
                Field::new("_vid", DataType::UInt64, false),
                Field::new("_deleted", DataType::Boolean, false),
                Field::new("_version", DataType::UInt64, false),
            ];
            for col in &lance_columns {
                if matches!(col.as_str(), "_vid" | "_deleted" | "_version") {
                    continue;
                }
                if col == "overflow_json" {
                    fields.push(Field::new("overflow_json", DataType::LargeBinary, true));
                } else if col == "_labels" {
                    // Typed explicitly: falling through to the `label_props`
                    // lookup below would default it to LargeBinary, since
                    // `_labels` is never a declared user property.
                    fields.push(Field::new(
                        "_labels",
                        crate::query::df_graph::common::labels_data_type(),
                        true,
                    ));
                } else if col == "_created_at" || col == "_updated_at" {
                    fields.push(Field::new(
                        col,
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        true,
                    ));
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

    // Build L0 batch. Prefer the multi-VID list when present (IN-list pushdown
    // from issue #55 PR #4 — must restrict L0 to the same VID set Lance was
    // filtered against, see issue #72 item 1). Fall back to single-VID
    // (`id(x) = $literal` short-circuit). One-element buffer keeps the
    // borrowed slice alive for the single-VID case.
    let single_vid_buf: [u64; 1];
    let l0_target_vids: Option<&[u64]> = match (vid_list_filter, target_vid) {
        (Some(vs), _) if !vs.is_empty() => Some(vs),
        (_, Some(v)) => {
            single_vid_buf = [v];
            Some(&single_vid_buf)
        }
        _ => None,
    };
    let l0_batch =
        build_l0_vertex_batch(l0_ctx, label, &internal_schema, label_props, l0_target_vids)?;

    // Merge Lance + L0
    let Some(merged) = merge_lance_and_l0(
        lance_deduped,
        l0_batch,
        &internal_schema,
        "_vid",
        graph_ctx.counters(),
    )?
    else {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    };

    // Filter out MVCC deletion tombstones (_deleted = true)
    let merged = filter_deleted_rows(&merged)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 tombstones
    let filtered = filter_l0_tombstones(&merged, l0_ctx)?;

    // Symmetric with the schemaless path: drop a flushed row whose scanned label
    // was REMOVE'd in L0 (a no-op unless a vid carries a label-overwrite marker
    // that no longer includes `label`).
    let filtered = filter_l0_label_overwrites(&filtered, label, l0_ctx)?;

    if filtered.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Map to output schema
    let mapped = map_to_output_schema(
        &filtered,
        label,
        variable,
        projected_properties,
        output_schema,
        l0_ctx,
    )?;

    // Apply indexed-property runtime filter (issue #57). Lance has already
    // filtered the on-disk side via `extra_lance_filter`; this catches any
    // L0 rows that slipped through the merge.
    apply_runtime_filter(mapped, extra_runtime_filter)
}

/// Apply the indexed-property runtime filter, if present, to a `RecordBatch`.
/// Returns the filtered batch (or the original if no filter is set). Rows
/// where the predicate evaluates to NULL are treated as non-matching, same
/// as DataFusion `FilterExec`. See issue #57.
fn apply_runtime_filter(
    batch: RecordBatch,
    runtime_filter: Option<&Arc<dyn PhysicalExpr>>,
) -> DFResult<RecordBatch> {
    let Some(filter) = runtime_filter else {
        return Ok(batch);
    };
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    let result = filter.evaluate(&batch)?;
    let array = result.into_array(batch.num_rows())?;
    let bools = array
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "indexed-property runtime filter did not produce a BooleanArray".to_string(),
            )
        })?;
    arrow::compute::filter_record_batch(&batch, bools).map_err(arrow_err)
}

/// Columnar-first schemaless vertex scan: single Lance query with MVCC dedup and L0 overlay.
///
/// Replaces the two-phase `scan_*_vids_*()` + `materialize_schemaless_vertex_batch_static()`
/// for schemaless vertex scans. Reads `_vid`, `labels`, `props_json`, `_version` in a single
/// Lance query on the main vertices table, performs MVCC dedup via Arrow compute, merges L0
/// buffer data, filters tombstones, and maps to the output schema.
#[expect(clippy::too_many_arguments)]
async fn columnar_scan_schemaless_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
    filter: &Option<Arc<dyn PhysicalExpr>>,
    vid_list_filter: Option<&[u64]>,
    extra_lance_filter: Option<&str>,
    extra_runtime_filter: Option<&Arc<dyn PhysicalExpr>>,
) -> DFResult<RecordBatch> {
    let storage = graph_ctx.storage();
    let l0_ctx = graph_ctx.l0_context();

    // Extract target VID from filter for short-circuit lookup. See the
    // detailed comment on the per-label scan for the IN-list path
    // (issue #55 PR #4).
    let target_vid = filter.as_ref().and_then(extract_vid_from_physical_filter);

    // Build the Lance filter expression — do NOT filter _deleted here;
    // MVCC dedup must see deletion tombstones to pick the highest version.
    let filter = {
        let mut parts: Vec<FilterExpr> = Vec::new();

        // VID point-lookup filter — uses BTree index on _vid. Prefer the
        // multi-VID list (formats as `_vid IN (...)`); fall back to single-VID.
        match (vid_list_filter, target_vid) {
            (Some(vs), _) if !vs.is_empty() => parts.push(FilterExpr::one_of(
                "_vid",
                vs.iter().map(|v| Scalar::UInt(*v)),
            )),
            (_, Some(vid)) => parts.push(FilterExpr::equals("_vid", Scalar::UInt(vid))),
            _ => {}
        }

        // Label filter
        if !label.is_empty() {
            // Multi-label: each label must be present. The label travels as a
            // `Scalar`, so `to_sql` escapes it — the previous `format!` did not.
            for lbl in label.split(':') {
                parts.push(FilterExpr::array_contains(
                    "labels",
                    Scalar::Str(lbl.to_string()),
                ));
            }
        }

        // Indexed-property pushdown — issue #57. Already-rendered SQL from the
        // planner, so it stays `Raw`.
        if let Some(extra) = extra_lance_filter {
            parts.push(FilterExpr::Raw(extra.to_string()));
        }

        if parts.is_empty() {
            None
        } else {
            Some(FilterExpr::all(parts))
        }
    };

    // Single Lance query via StorageManager domain method. Counted, so this
    // scan appears in `scans_reported` — the schemaless path was invisible to
    // the counters the labelled path already reports through.
    let lance_batch = storage
        .scan_main_vertex_table_counted(
            &["_vid", "_deleted", "labels", "props_json", "_version"],
            filter.as_ref(),
            graph_ctx.counters(),
        )
        .await
        .map_err(exec_err)?;

    // A pushed property predicate hides a vid's CURRENT row from the scan when
    // that row no longer matches (MVCC-append: the stale still-matching row
    // would win the dedup by default) — drop superseded rows first.
    let lance_batch = match (lance_batch, extra_lance_filter.is_some()) {
        (Some(b), true) => Some(drop_superseded_pushdown_rows(storage, None, b).await?),
        (b, _) => b,
    };

    // MVCC dedup the Lance batch
    let lance_deduped = mvcc_dedup_to_option(lance_batch, "_vid")?;

    // Build the internal schema for L0 batch construction.
    // Use the Lance batch schema if available, otherwise build from scratch.
    let internal_schema = match &lance_deduped {
        Some(batch) => batch.schema(),
        None => Arc::new(Schema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("_deleted", DataType::Boolean, false),
            Field::new("labels", labels_data_type(), false),
            Field::new("props_json", DataType::LargeBinary, true),
            Field::new("_version", DataType::UInt64, false),
        ])),
    };

    // Build L0 batch. Prefer the multi-VID list when present (IN-list pushdown
    // from issue #55 PR #4 — must restrict L0 to match Lance filtering, see
    // issue #72 item 1). Fall back to single-VID.
    let single_vid_buf: [u64; 1];
    let l0_target_vids: Option<&[u64]> = match (vid_list_filter, target_vid) {
        (Some(vs), _) if !vs.is_empty() => Some(vs),
        (_, Some(v)) => {
            single_vid_buf = [v];
            Some(&single_vid_buf)
        }
        _ => None,
    };
    let l0_batch =
        build_l0_schemaless_vertex_batch(l0_ctx, label, &internal_schema, l0_target_vids)?;

    // Merge Lance + L0
    let Some(merged) = merge_lance_and_l0(
        lance_deduped,
        l0_batch,
        &internal_schema,
        "_vid",
        graph_ctx.counters(),
    )?
    else {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    };

    // Filter out MVCC deletion tombstones (_deleted = true)
    let merged = filter_deleted_rows(&merged)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 tombstones
    let filtered = filter_l0_tombstones(&merged, l0_ctx)?;

    // Drop stale flushed rows whose label was REMOVE'd in L0 (the flushed
    // `labels` array still lists it, but the newest L0 overwrite doesn't).
    let filtered = filter_l0_label_overwrites(&filtered, label, l0_ctx)?;

    if filtered.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Map to output schema
    let mapped = map_to_schemaless_output_schema(
        &filtered,
        variable,
        projected_properties,
        output_schema,
        l0_ctx,
    )?;

    // Apply indexed-property runtime filter — issue #57.
    apply_runtime_filter(mapped, extra_runtime_filter)
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
    target_vids: Option<&[u64]>,
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

        // Collect VIDs matching the label filter — short-circuit when target_vids is set
        // (see issue #72 item 1; multi-VID IN-list must filter L0 too).
        let vids: Vec<Vid> = if let Some(tvs) = target_vids {
            let mut out = Vec::with_capacity(tvs.len());
            for &tv in tvs {
                let vid = Vid::from(tv);
                if !guard.vertex_properties.contains_key(&vid) {
                    continue;
                }
                let label_ok = if label_filter.is_empty() {
                    true
                } else if let Some(labels) = guard.vertex_labels.get(&vid) {
                    label_filter
                        .iter()
                        .all(|lf| labels.contains(&lf.to_string()))
                } else {
                    false
                };
                if label_ok {
                    out.push(vid);
                }
            }
            out
        } else if label_filter.is_empty() {
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
                        // Encode properties as a CypherValue blob directly from
                        // `Value` so typed values (temporals) are preserved.
                        let map: HashMap<String, Value> =
                            props.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        builder
                            .append_value(uni_common::cypher_value_codec::encode(&Value::Map(map)));
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

    RecordBatch::try_new(internal_schema.clone(), columns).map_err(arrow_err)
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

        // Overlay L0 labels, honoring label-overwrite markers.
        //
        // A vid flagged in `vertex_label_overwrites` had its FULL label set
        // resolved by a `SET`/`REMOVE n:Label`; that buffer's labels REPLACE the
        // stored batch labels (newest buffer wins; buffers iterate oldest →
        // newest). A vid without the marker only contributes additive labels
        // (union). Without the replace, a union-only overlay could never drop a
        // label, so a `REMOVE n:Label` resurrected the removed label in
        // `labels(n)`.
        let mut overwrite_labels: Option<Vec<String>> = None;
        for l0 in l0_ctx.iter_l0_buffers() {
            let guard = l0.read();
            if guard.vertex_label_overwrites.contains(&vid) {
                overwrite_labels = guard.vertex_labels.get(&vid).cloned();
            } else if let Some(l0_labels) = guard.vertex_labels.get(&vid) {
                for lbl in l0_labels {
                    if !row_labels.contains(lbl) {
                        row_labels.push(lbl.clone());
                    }
                }
            }
        }
        if let Some(resolved) = overwrite_labels {
            row_labels = resolved;
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
            // Fast path: if no L0 buffer has vertex property mutations,
            // the raw props_json passthrough is correct.
            let any_l0_has_vertex_props = l0_ctx.iter_l0_buffers().any(|l0| {
                let guard = l0.read();
                !guard.vertex_properties.is_empty()
            });
            if !any_l0_has_vertex_props {
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
                let col = build_all_props_column_with_l0_overlay(
                    batch.num_rows(),
                    vid_arr,
                    props_arr,
                    l0_ctx,
                );
                columns.push(col);
            }
        } else {
            // Extract individual property from CypherValue blob with L0 overlay.
            // The raw column is LargeBinary (CypherValue-encoded). If the output
            // schema expects a typed column (e.g., Utf8 for String properties),
            // decode the CypherValue and build the correct Arrow type.
            let expected_type = output_schema
                .field_with_name(&format!("{_variable}.{prop}"))
                .map(|f| f.data_type().clone())
                .unwrap_or(DataType::LargeBinary);

            if expected_type == DataType::LargeBinary {
                let col = build_overflow_property_column(
                    batch.num_rows(),
                    vid_arr,
                    props_arr,
                    prop,
                    l0_ctx,
                );
                columns.push(col);
            } else {
                // Decode CypherValue to the expected type via build_property_column_static.
                let mut prop_values: HashMap<Vid, Properties> = HashMap::new();
                for i in 0..batch.num_rows() {
                    let vid = Vid::from(vid_arr.value(i));
                    let resolved =
                        resolve_l0_property(&vid, prop, l0_ctx)
                            .flatten()
                            .or_else(|| {
                                extract_from_overflow_blob(props_arr, i, prop).and_then(|bytes| {
                                    uni_common::cypher_value_codec::decode(&bytes).ok()
                                })
                            });
                    if let Some(val) = resolved {
                        prop_values.insert(vid, HashMap::from([(prop.to_string(), val)]));
                    }
                }
                let vids: Vec<Vid> = (0..batch.num_rows())
                    .map(|i| Vid::from(vid_arr.value(i)))
                    .collect();
                let col = build_property_column_static(&vids, &prop_values, prop, &expected_type)
                    .unwrap_or_else(|_| {
                        arrow_array::new_null_array(&expected_type, batch.num_rows())
                    });
                columns.push(col);
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns).map_err(arrow_err)
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
                        // Potential raw CypherValue bytes stored as list<u8> from PropertyManager.
                        // Guard against misclassifying normal integer lists (e.g. [42, 43]) as bytes.
                        let bytes: Vec<u8> = arr
                            .iter()
                            .filter_map(|v| v.as_u64().map(|n| n as u8))
                            .collect();
                        if uni_common::cypher_value_codec::decode(&bytes).is_ok() {
                            builder.append_value(&bytes);
                        } else {
                            builder.append_value(uni_common::cypher_value_codec::encode(
                                &Value::List(arr),
                            ));
                        }
                    }
                    Some(val) => {
                        // Encode any other property value directly via the
                        // CypherValue codec so typed values (temporals, including
                        // BTIC, and nested lists/maps) round-trip losslessly.
                        builder.append_value(uni_common::cypher_value_codec::encode(&val));
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
                    Some(Value::Vector(v)) => {
                        for val in v {
                            list_builder.values().append_value(val);
                        }
                        list_builder.append(true);
                    }
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
        DataType::FixedSizeList(inner, dim) if *inner.data_type() == DataType::UInt8 => {
            // Binary-vector properties: FixedSizeList(UInt8, N). Accepts a native
            // `BinaryVector` or a `List` of byte-ints (the literal input form).
            let values_builder = arrow_array::builder::UInt8Builder::new();
            let mut list_builder = FixedSizeListBuilder::new(values_builder, *dim);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::BinaryVector(b)) if b.len() == *dim as usize => {
                        for byte in b {
                            list_builder.values().append_value(byte);
                        }
                        list_builder.append(true);
                    }
                    Some(Value::List(arr)) if arr.len() == *dim as usize => {
                        let mut ok = true;
                        for v in &arr {
                            match v.as_i64() {
                                Some(n @ 0..=255) => list_builder.values().append_value(n as u8),
                                _ => {
                                    list_builder.values().append_value(0);
                                    ok = false;
                                }
                            }
                        }
                        list_builder.append(ok);
                    }
                    _ => {
                        for _ in 0..*dim {
                            list_builder.values().append_null();
                        }
                        list_builder.append(false);
                    }
                }
            }
            Ok(Arc::new(list_builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            // Timestamp properties stored as Value::Temporal, ISO 8601 strings, or i64 nanoseconds
            let mut builder = TimestampNanosecondBuilder::new().with_timezone("UTC");
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(tv)) => match tv {
                        uni_common::TemporalValue::DateTime {
                            nanos_since_epoch, ..
                        }
                        | uni_common::TemporalValue::LocalDateTime {
                            nanos_since_epoch, ..
                        } => {
                            builder.append_value(nanos_since_epoch);
                        }
                        uni_common::TemporalValue::Date { days_since_epoch } => {
                            builder.append_value(days_since_epoch as i64 * 86_400_000_000_000);
                        }
                        _ => builder.append_null(),
                    },
                    Some(Value::String(s)) => match parse_datetime_utc(&s) {
                        Ok(dt) => builder.append_value(dt.timestamp_nanos_opt().unwrap_or(0)),
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
                    Some(Value::Temporal(uni_common::TemporalValue::Date { days_since_epoch })) => {
                        builder.append_value(days_since_epoch);
                    }
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
        DataType::Time64(TimeUnit::Nanosecond) => {
            let mut builder = Time64NanosecondBuilder::new();
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(
                        uni_common::TemporalValue::LocalTime {
                            nanos_since_midnight,
                        }
                        | uni_common::TemporalValue::Time {
                            nanos_since_midnight,
                            ..
                        },
                    )) => {
                        builder.append_value(nanos_since_midnight);
                    }
                    Some(Value::Temporal(_)) => builder.append_null(),
                    Some(Value::String(s)) => {
                        match NaiveTime::parse_from_str(&s, "%H:%M:%S%.f")
                            .or_else(|_| NaiveTime::parse_from_str(&s, "%H:%M:%S"))
                        {
                            Ok(t) => {
                                let nanos = t.num_seconds_from_midnight() as i64 * 1_000_000_000
                                    + t.nanosecond() as i64;
                                builder.append_value(nanos);
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
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut values: Vec<Option<arrow::datatypes::IntervalMonthDayNano>> =
                Vec::with_capacity(vids.len());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(uni_common::TemporalValue::Duration {
                        months,
                        days,
                        nanos,
                    })) => {
                        values.push(Some(arrow::datatypes::IntervalMonthDayNano {
                            months: months as i32,
                            days: days as i32,
                            nanoseconds: nanos,
                        }));
                    }
                    Some(Value::Int(_n)) => {
                        values.push(None);
                    }
                    _ => values.push(None),
                }
            }
            let arr: arrow_array::IntervalMonthDayNanoArray = values.into_iter().collect();
            Ok(Arc::new(arr))
        }
        DataType::List(inner_field) => {
            build_list_property_column(vids, props_map, prop_name, inner_field)
        }
        // Sparse-vector struct must be matched BEFORE the generic struct arm
        // below (whose `build_struct_property_column` only knows scalar fields
        // and would emit Utf8 for the `List` children).
        DataType::Struct(_) if uni_common::core::schema::is_sparse_vector_struct(data_type) => {
            let values: Vec<Option<Value>> = vids
                .iter()
                .map(|vid| get_property_value(vid, props_map, prop_name))
                .collect();
            Ok(uni_store::storage::arrow_convert::build_sparse_vector_array(&values))
        }
        DataType::Struct(fields) => {
            build_struct_property_column(vids, props_map, prop_name, fields)
        }
        DataType::FixedSizeBinary(24) => {
            // BTIC temporal interval columns: encode as FixedSizeBinary(24)
            use arrow_array::builder::FixedSizeBinaryBuilder;
            const BTIC_LEN: i32 = 24;
            let mut builder = FixedSizeBinaryBuilder::with_capacity(vids.len(), BTIC_LEN);
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::Temporal(uni_common::TemporalValue::Btic { lo, hi, meta })) => {
                        match uni_btic::Btic::new(lo, hi, meta) {
                            Ok(b) => {
                                builder
                                    .append_value(uni_btic::encode::encode(&b))
                                    .map_err(arrow_err)?;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "BTIC coercion failed for property '{}': invalid value (lo={}, hi={}, meta={:#x}): {}",
                                    prop_name,
                                    lo,
                                    hi,
                                    meta,
                                    e
                                );
                                builder.append_null()
                            }
                        }
                    }
                    Some(Value::String(s)) => match uni_btic::parse::parse_btic_literal(&s) {
                        Ok(b) => {
                            builder
                                .append_value(uni_btic::encode::encode(&b))
                                .map_err(arrow_err)?;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "BTIC coercion failed for property '{}': '{}' is not a valid BTIC literal: {}",
                                prop_name,
                                s,
                                e
                            );
                            builder.append_null()
                        }
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
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
        DataType::LargeBinary
            if inner_field
                .metadata()
                .get("uni_raw_bytes")
                .is_some_and(|v| v == "true") =>
        {
            // Typed `List(Bytes)`: store each buffer verbatim in a `LargeBinary`
            // child. The child field (reused from the schema) carries the
            // `uni_raw_bytes` marker so the read path decodes it as raw `Bytes`.
            // CV-encoded `LargeBinary` lists lack the marker and keep the string
            // fallback below — no pattern-comprehension/VLP regression.
            let mut builder = ListBuilder::new(arrow_array::builder::LargeBinaryBuilder::new())
                .with_field(inner_field.clone());
            for vid in vids {
                match get_property_value(vid, props_map, prop_name) {
                    Some(Value::List(arr)) => {
                        for v in arr {
                            if let Value::Bytes(b) = v {
                                builder.values().append_value(b);
                            } else {
                                builder.values().append_null();
                            }
                        }
                        builder.append(true);
                    }
                    _ => builder.append(false),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Multi-vector (`List<FixedSizeList<Float32>>`): build the typed
        // multi-vector column from the owned L0 values, mirroring the write path.
        // Without this arm the value falls to the string fallback below and yields
        // `List<Utf8>`, which mismatches the declared schema type when the result
        // batch is assembled (`RETURN d.tokens` on unflushed/L0 rows).
        DataType::FixedSizeList(child, dim) if matches!(child.data_type(), DataType::Float32) => {
            let values: Vec<Option<Value>> = vids
                .iter()
                .map(|vid| get_property_value(vid, props_map, prop_name))
                .collect();
            Ok(uni_store::storage::arrow_convert::build_multivector_array(
                &values,
                *dim as usize,
            ))
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
                // Typed `Map(_, Bytes)` value child: the schema field carries the
                // `uni_raw_bytes` marker; store each buffer verbatim in a
                // `LargeBinary` so the read path (`try_reconstruct_map`) decodes it
                // as raw `Bytes`. CV-encoded values lack the marker → string fallback.
                DataType::LargeBinary
                    if field
                        .metadata()
                        .get("uni_raw_bytes")
                        .is_some_and(|v| v == "true") =>
                {
                    let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
                    for obj in rows.iter().flatten().flatten() {
                        match obj.get(field_name) {
                            Some(Value::Bytes(b)) => builder.append_value(b),
                            _ => builder.append_null(),
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

/// Convert a TemporalValue into a HashMap matching the Arrow struct field names,
/// so that `build_struct_property_column` can extract fields uniformly.
fn temporal_to_struct_map(tv: &uni_common::value::TemporalValue) -> HashMap<String, Value> {
    use uni_common::value::TemporalValue;
    let mut m = HashMap::new();
    match tv {
        TemporalValue::DateTime {
            nanos_since_epoch,
            offset_seconds,
            timezone_name,
        } => {
            m.insert("nanos_since_epoch".into(), Value::Int(*nanos_since_epoch));
            m.insert("offset_seconds".into(), Value::Int(*offset_seconds as i64));
            if let Some(tz) = timezone_name {
                m.insert("timezone_name".into(), Value::String(tz.clone()));
            }
        }
        TemporalValue::LocalDateTime { nanos_since_epoch } => {
            m.insert("nanos_since_epoch".into(), Value::Int(*nanos_since_epoch));
        }
        TemporalValue::Time {
            nanos_since_midnight,
            offset_seconds,
        } => {
            m.insert(
                "nanos_since_midnight".into(),
                Value::Int(*nanos_since_midnight),
            );
            m.insert("offset_seconds".into(), Value::Int(*offset_seconds as i64));
        }
        TemporalValue::LocalTime {
            nanos_since_midnight,
        } => {
            m.insert(
                "nanos_since_midnight".into(),
                Value::Int(*nanos_since_midnight),
            );
        }
        TemporalValue::Date { days_since_epoch } => {
            m.insert(
                "days_since_epoch".into(),
                Value::Int(*days_since_epoch as i64),
            );
        }
        TemporalValue::Duration {
            months,
            days,
            nanos,
        } => {
            m.insert("months".into(), Value::Int(*months));
            m.insert("days".into(), Value::Int(*days));
            m.insert("nanos".into(), Value::Int(*nanos));
        }
        TemporalValue::Btic { lo, hi, meta } => {
            m.insert("lo".into(), Value::Int(*lo));
            m.insert("hi".into(), Value::Int(*hi));
            m.insert("meta".into(), Value::Int(*meta as i64));
        }
    }
    m
}

/// Build a Struct-typed Arrow column from Map property values (e.g. Point types).
fn build_struct_property_column(
    vids: &[Vid],
    props_map: &HashMap<Vid, Properties>,
    prop_name: &str,
    fields: &Fields,
) -> DFResult<ArrayRef> {
    use arrow_array::StructArray;

    // Convert raw values, expanding Temporal values into Map representation
    // so the struct field extraction below works uniformly.
    let values: Vec<Option<Value>> = vids
        .iter()
        .map(|vid| {
            let val = get_property_value(vid, props_map, prop_name);
            match val {
                Some(Value::Temporal(ref tv)) => Some(Value::Map(temporal_to_struct_map(tv))),
                other => other,
            }
        })
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
                DataType::Timestamp(_, _) => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(vids.len());
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
                DataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(vids.len());
                    for val in &values {
                        match val {
                            Some(Value::Map(obj)) => {
                                match obj.get(field_name).and_then(|v| v.as_i64()) {
                                    Some(n) => builder.append_value(n as i32),
                                    None => builder.append_null(),
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }
                DataType::Time64(_) => {
                    let mut builder = Time64NanosecondBuilder::with_capacity(vids.len());
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
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
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
                    let is_schemaless = self.is_schemaless;
                    let filter = self.filter.clone();
                    let vid_list_filter = self.vid_list_filter.clone();
                    let extra_lance_filter = self.extra_lance_filter.clone();
                    let extra_runtime_filter = self.extra_runtime_filter.clone();
                    let schema = self.schema.clone();
                    let index_consulted = self.index_consulted.clone();

                    let fut = async move {
                        graph_ctx.check_timeout().map_err(exec_err)?;

                        let batch = if is_schemaless {
                            columnar_scan_schemaless_vertex_batch_static(
                                &graph_ctx,
                                &label,
                                &variable,
                                &properties,
                                &schema,
                                &filter,
                                vid_list_filter.as_deref(),
                                extra_lance_filter.as_deref(),
                                extra_runtime_filter.as_ref(),
                            )
                            .await?
                        } else {
                            columnar_scan_vertex_batch_static(
                                &graph_ctx,
                                &label,
                                &variable,
                                &properties,
                                &schema,
                                &filter,
                                vid_list_filter.as_deref(),
                                extra_lance_filter.as_deref(),
                                extra_runtime_filter.as_ref(),
                                Some(&index_consulted),
                            )
                            .await?
                        };
                        Ok(Some(batch))
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
    fn test_build_schemaless_vertex_schema() {
        let empty_schema = uni_common::core::schema::Schema::default();
        let schema = GraphScanExec::build_schemaless_vertex_schema(
            "n",
            &["name".to_string(), "age".to_string()],
            &empty_schema,
        );

        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(1).name(), "n._labels");
        assert_eq!(schema.field(2).name(), "n.name");
        // With empty schema, falls back to LargeBinary
        assert_eq!(schema.field(2).data_type(), &DataType::LargeBinary);
        assert_eq!(schema.field(3).name(), "n.age");
        assert_eq!(schema.field(3).data_type(), &DataType::LargeBinary);
    }

    #[test]
    fn test_schemaless_all_scan_has_empty_label() {
        let empty_schema = uni_common::core::schema::Schema::default();
        let schema = GraphScanExec::build_schemaless_vertex_schema("n", &[], &empty_schema);

        // Verify the schema has _vid and _labels columns for a scan with no properties
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "n._vid");
        assert_eq!(schema.field(1).name(), "n._labels");
    }

    #[test]
    fn test_cypher_value_all_props_extraction() {
        // Encode a property map directly via the CypherValue codec (the path the
        // `_all_props` builders use).
        let map: HashMap<String, Value> = [
            ("age".to_string(), Value::Int(30)),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]
        .into_iter()
        .collect();
        let cv_bytes = uni_common::cypher_value_codec::encode(&Value::Map(map));

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
        let single_bytes = uni_common::cypher_value_codec::encode(&Value::Int(30));
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
            Field::new("n._labels", labels_data_type(), true),
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
