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

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    arrow_err, compute_plan_properties, exec_err, labels_data_type,
};
// Materialisation helpers now live in `uni-store` so the storage layer can own
// the columnar read pipeline (#209). Re-exported here: `resolve_property_type`
// and `property_field` are `pub(crate)` with callers across this crate.
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
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
use uni_store::runtime::columnar_scan::{
    build_overflow_property_column, drop_superseded_pushdown_rows, extract_from_overflow_blob,
    filter_deleted_rows, filter_l0_label_overwrites, filter_l0_tombstones, merge_lance_and_l0,
    mvcc_dedup_to_option, resolve_l0_property,
};
pub(crate) use uni_store::runtime::columnar_scan::{
    build_property_column_static, property_field, resolve_property_type,
};

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
        context: Arc<TaskContext>,
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
            context.session_config().batch_size(),
            context.memory_pool(),
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
    /// The scan finished; hand its rows out in `batch_size` slices.
    ///
    /// The scan builds one `RecordBatch` for the whole result. Emitting it whole
    /// gives every downstream operator a single indivisible input, and an
    /// operator that buffers — sort, hash aggregate, join — then has nothing to
    /// spill *between*: `ExternalSorter` asked for 5.1 GB in one reservation on
    /// LDBC IC9 and failed, with a disk manager available the whole time
    /// (`DiskManagerMode` defaults to `OsTmpDirectory`). Slicing is what lets the
    /// spill path engage. See issue #202.
    ///
    /// `RecordBatch::slice` is zero-copy, so this does not reduce what the scan
    /// itself holds — the whole result is still built before the first slice is
    /// emitted. Making the scan produce batches incrementally is the follow-up.
    Slicing { batch: RecordBatch, offset: usize },
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
    /// Rows per emitted slice, from the session's `batch_size`.
    slice_size: usize,

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

    /// The query pool's accounting for the whole-result batch below.
    ///
    /// The scan builds one `RecordBatch` for the entire result and holds it for
    /// as long as it is slicing, so the reservation lives on the stream rather
    /// than inside the scan future — the memory is resident across every poll
    /// that follows, not just while it is being built. It is released when the
    /// stream is dropped.
    ///
    /// The slices handed downstream are zero-copy views onto this batch, so the
    /// buffers stay alive while any consumer holds one. Accounting for the
    /// batch once, here, is what makes the pool see the largest single
    /// allocation this system makes (#242).
    reservation: MemoryReservation,
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
        slice_size: usize,
        pool: &Arc<dyn MemoryPool>,
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
            slice_size: slice_size.max(1),
            reservation: MemoryConsumer::new("GraphScanExec").register(pool),
        }
    }
}

// ============================================================================
// Columnar-first scan helpers
// ============================================================================

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

/// Columnar-first vertex scan: single Lance query with MVCC dedup and L0 overlay.
///
/// Replaces the two-phase `scan_vertex_vids_static()` + `materialize_vertex_batch_static()`
/// for known-label vertex scans. Reads all needed columns in a single Lance query,
/// performs MVCC dedup via Arrow compute, merges L0 buffer data, filters tombstones,
/// and maps to the output schema.
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
    // Chunk the vid list, bounding how much is resident at once.
    //
    // The `_vid` index is used either way, and the index work itself does not
    // scale with the table: `index_comparisons` is ~1 per requested vid and
    // barely moves when the table grows 5x (60,000 -> 61,440). What scales is
    // what happens *after* the lookup — the matching rows are scattered across
    // proportionally more pages in a larger table, and unchunked they are all
    // materialised at once. Chunking caps the peak at one chunk's worth:
    // 60,000 vids read from a 300k-row table went from 815 MiB to 226 MiB,
    // and stopped tracking the table's size.
    //
    // `VidLookupJoinExec` already chunks this exact shape at the same constant.
    // Note the trade: at 100% selectivity — asking for every row in the table —
    // one full scan beats six chunked ones, so this costs ~66 MiB on the small
    // fixture. A selectivity-aware choice would beat a fixed constant.
    let mut parts: Vec<RecordBatch> = Vec::new();
    for chunk in raw.chunks(crate::query::df_graph::vid_lookup_join::MAX_VIDS_PER_CHUNK) {
        parts.push(
            columnar_scan_vertex_batch_static(
                graph_ctx,
                label,
                variable,
                properties,
                &output_schema,
                &None,
                Some(chunk),
                None,
                None,
                None,
            )
            .await?,
        );
    }
    let batch = if parts.len() == 1 {
        parts
            .pop()
            .unwrap_or_else(|| RecordBatch::new_empty(Arc::clone(&output_schema)))
    } else {
        arrow::compute::concat_batches(&output_schema, &parts).map_err(arrow_err)?
    };

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

#[expect(clippy::too_many_arguments)]
pub(crate) async fn columnar_scan_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    // Retained so the three call sites are untouched. Column naming is the
    // caller's `output_schema`, which `map_to_output_schema` maps positionally.
    _variable: &str,
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
    // Everything below the DataFusion boundary lives in `uni-store` now, so
    // crates beneath the query layer can read columnarly too (#209). What stays
    // here is exactly what is DataFusion-shaped: resolving the physical filter
    // to a vid, the per-node metric, and the runtime filter.
    //
    // Single-VID pushdown is the WHERE-clause `id(x) = $literal` path; the
    // multi-VID list comes from the IN-list path (`UNWIND ... WHERE id(x) =
    // e.field`, issue #55 PR #4).
    let target_vid = filter.as_ref().and_then(extract_vid_from_physical_filter);

    let mut index_hits = 0usize;
    let mapped = uni_store::runtime::columnar_scan::columnar_scan_vertex_batch(
        graph_ctx.storage(),
        graph_ctx.l0_context(),
        graph_ctx.plugin_registry(),
        graph_ctx.counters(),
        uni_store::runtime::columnar_scan::ColumnarVertexScanRequest {
            label,
            projected_properties,
            output_schema,
            target_vid,
            vid_list_filter,
            extra_lance_filter,
        },
        Some(&mut index_hits),
    )
    .await
    .map_err(exec_err)?;
    if let Some(m) = index_consulted {
        m.add(index_hits);
    }

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
        (Some(b), true) => Some(
            drop_superseded_pushdown_rows(storage, None, b)
                .await
                .map_err(exec_err)?,
        ),
        (b, _) => b,
    };

    // MVCC dedup the Lance batch
    let lance_deduped = mvcc_dedup_to_option(lance_batch, "_vid").map_err(exec_err)?;

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
    )
    .map_err(exec_err)?
    else {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    };

    // Filter out MVCC deletion tombstones (_deleted = true)
    let merged = filter_deleted_rows(&merged).map_err(exec_err)?;
    if merged.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Filter L0 tombstones
    let filtered = filter_l0_tombstones(&merged, l0_ctx).map_err(exec_err)?;

    // Drop stale flushed rows whose label was REMOVE'd in L0 (the flushed
    // `labels` array still lists it, but the newest L0 overwrite doesn't).
    let filtered = filter_l0_label_overwrites(&filtered, label, l0_ctx).map_err(exec_err)?;

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
                // A build failure used to become an all-NULL column of the right
                // type, with no log: every row of the property read as absent and
                // nothing distinguished that from the property genuinely being
                // absent (#233). The enclosing function already returns a
                // `DFResult`, so the failure has somewhere to go.
                let col = build_property_column_static(&vids, &prop_values, prop, &expected_type)
                    .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "building column for property '{prop}': {e}"
                    ))
                })?;
                columns.push(col);
            }
        }
    }

    RecordBatch::try_new(output_schema.clone(), columns).map_err(arrow_err)
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
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        match batch {
                            // Hand the result out in `batch_size` slices rather
                            // than as one batch — see `GraphScanState::Slicing`.
                            Some(b) if b.num_rows() > 0 => {
                                // Reserve before slicing begins. The batch is
                                // already built by this point -- the scan is one
                                // async call that returns the whole result -- so
                                // this bounds how long an over-budget result
                                // survives rather than preventing its
                                // construction. Making the scan itself
                                // incremental is #214/#240; until then this is
                                // the earliest point the size is known.
                                if let Err(e) = self.reservation.try_grow(b.get_array_memory_size())
                                {
                                    self.state = GraphScanState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                                self.state = GraphScanState::Slicing {
                                    batch: b,
                                    offset: 0,
                                };
                            }
                            other => {
                                self.state = GraphScanState::Done;
                                return Poll::Ready(other.map(Ok));
                            }
                        }
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
                GraphScanState::Slicing { batch, offset } => {
                    let remaining = batch.num_rows() - offset;
                    if remaining == 0 {
                        self.state = GraphScanState::Done;
                        return Poll::Ready(None);
                    }
                    let take = self.slice_size.min(remaining);
                    let slice = batch.slice(offset, take);
                    self.state = GraphScanState::Slicing {
                        batch,
                        offset: offset + take,
                    };
                    return Poll::Ready(Some(Ok(slice)));
                }
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
}
