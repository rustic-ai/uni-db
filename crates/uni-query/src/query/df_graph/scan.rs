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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::Schema as UniSchema;
use uni_store::backend::types::{CmpOp, FilterExpr, Scalar};
use uni_store::runtime::columnar_scan::{
    L0PropertyPrefilter, build_overflow_property_column, drop_superseded_pushdown_rows,
    extract_from_overflow_blob, filter_deleted_rows, filter_l0_label_overwrites,
    filter_l0_tombstones, merge_lance_and_l0, mvcc_dedup_to_option, resolve_l0_property,
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
/// A `_vid` list that is not known until another operator has run (#179).
///
/// `VidLookupJoinExec` cannot pass its probe a vid list at plan time — the set
/// comes from materialising the build side. That is why the probe used to be
/// driven through a bespoke helper instead of `execute()`, which in turn is why
/// it was not a child and was invisible to every `children()` walk: profiling,
/// the operator-activation gate, and DataFusion's own optimizer rules.
///
/// Handing the scan a shared slot instead lets the join publish the vids just
/// before executing the probe through the ordinary `ExecutionPlan` API, so the
/// probe can be a real child. The join writes once per chunk and the scan reads
/// it on each `execute()`.
#[derive(Debug, Default)]
pub(crate) struct DynamicVidFilter {
    vids: parking_lot::RwLock<Option<Vec<u64>>>,
}

impl DynamicVidFilter {
    /// Publish the vid set the next `execute()` should restrict to.
    pub(crate) fn set(&self, vids: Option<Vec<u64>>) {
        *self.vids.write() = vids;
    }

    /// The currently published set, if any.
    pub(crate) fn get(&self) -> Option<Vec<u64>> {
        self.vids.read().clone()
    }
}

/// An indexed-property restriction that is not known until another operator
/// has run — the property counterpart of [`DynamicVidFilter`].
///
/// `PropertyLookupJoinExec` materializes its build side, collects the distinct
/// join-key values, and publishes them here as `prop IN (...)` in both of the
/// forms the indexed-property pushdown uses: a Lance SQL string, which a scalar
/// index serves for flushed rows, and an Arrow predicate applied to the merged
/// (Lance + L0) batch, which is what keeps unflushed rows exact. The scan reads
/// the slot on each `execute()`; `None` means "no restriction", the arm a large
/// key set takes.
#[derive(Debug, Default)]
pub(crate) struct DynamicPropertyFilter {
    filter: parking_lot::RwLock<Option<(String, Arc<dyn PhysicalExpr>)>>,
}

impl DynamicPropertyFilter {
    /// Publish the restriction the next `execute()` should apply.
    pub(crate) fn set(&self, filter: Option<(String, Arc<dyn PhysicalExpr>)>) {
        *self.filter.write() = filter;
    }

    /// The currently published restriction, if any.
    pub(crate) fn get(&self) -> Option<(String, Arc<dyn PhysicalExpr>)> {
        self.filter.read().clone()
    }
}

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

    /// Rows a `LIMIT` above this scan will keep, when the planner could prove
    /// pushing it down is safe (#239).
    ///
    /// This is **not** `ScanRequest::limit`, and deliberately so: that field
    /// truncates raw Lance rows below MVCC dedup, which returns stale values
    /// (see `ScanRequest::with_limit`). This one bounds the `_vid` **range
    /// walk** instead — it stops the walk once enough deduped rows have been
    /// emitted, and keeps the range width from growing past what the limit can
    /// consume. Every row a range produces is still fully version-resolved and
    /// L0-merged before anything is counted, so narrowing the walk can only
    /// change how much is read, never which rows win.
    fetch: Option<usize>,

    /// A vid list published by an operator above, resolved at `execute()` time
    /// rather than at plan time (#179). Only `VidLookupJoinExec` sets this.
    ///
    /// When present and populated it takes precedence over `vid_list_filter`,
    /// which is the planner's static equivalent. `None` inside the slot means
    /// "no restriction" — the high-selectivity arm, where reading the whole
    /// table beats a vid list (#237).
    dynamic_vid_filter: Option<Arc<DynamicVidFilter>>,

    /// An indexed-property restriction published by an operator above at
    /// `execute()` time. Only `PropertyLookupJoinExec` sets this. When the slot
    /// holds a filter it is AND-ed onto `extra_lance_filter` /
    /// `extra_runtime_filter`; an empty slot means "no restriction".
    dynamic_property_filter: Option<Arc<DynamicPropertyFilter>>,

    /// Equality constraints the scan may use to skip L0 rows that cannot
    /// match before building them (W3b), set by the planner from the scan's
    /// Cypher filter, which stays authoritative above the scan.
    ///
    /// A schemaless (main-table) scan always reads its L0 prefilter here. The
    /// per-label path recovers its own from `extra_runtime_filter` and falls
    /// back to this one when that yields none; the planner sets it there for
    /// an `ext_id` equality, which is pushed to Lance as SQL only and so has
    /// no runtime filter to recover a prefilter from.
    planner_l0_prefilter: Option<L0PropertyPrefilter>,

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
    /// A copy of this scan carrying a slot an operator above will fill in
    /// before executing it (#179). See [`DynamicVidFilter`].
    ///
    /// By reference because the caller holds the probe as a `&dyn ExecutionPlan`
    /// downcast, which it cannot move out of.
    pub(crate) fn with_dynamic_vid_filter(&self, slot: Arc<DynamicVidFilter>) -> Self {
        self.with_dynamic_vid_filter_opt(Some(slot))
    }

    /// Attach the equality prefilter the scan applies to its L0 rows.
    ///
    /// See the `planner_l0_prefilter` field; `None` keeps every L0 row.
    pub(crate) fn with_planner_l0_prefilter(
        mut self,
        prefilter: Option<L0PropertyPrefilter>,
    ) -> Self {
        self.planner_l0_prefilter = prefilter;
        self
    }

    /// A copy of this scan with fresh metrics and the given vid slot.
    fn with_dynamic_vid_filter_opt(&self, slot: Option<Arc<DynamicVidFilter>>) -> Self {
        Self {
            graph_ctx: self.graph_ctx.clone(),
            label: self.label.clone(),
            variable: self.variable.clone(),
            projected_properties: self.projected_properties.clone(),
            filter: self.filter.clone(),
            vid_list_filter: self.vid_list_filter.clone(),
            extra_lance_filter: self.extra_lance_filter.clone(),
            extra_runtime_filter: self.extra_runtime_filter.clone(),
            dynamic_vid_filter: slot,
            dynamic_property_filter: self.dynamic_property_filter.clone(),
            planner_l0_prefilter: self.planner_l0_prefilter.clone(),
            is_schemaless: self.is_schemaless,
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            fetch: self.fetch,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// A copy of this scan carrying a property-restriction slot an operator
    /// above fills in before executing it. See [`DynamicPropertyFilter`].
    pub(crate) fn with_dynamic_property_filter(&self, slot: Arc<DynamicPropertyFilter>) -> Self {
        let mut wired = self.with_dynamic_vid_filter_opt(self.dynamic_vid_filter.clone());
        wired.dynamic_property_filter = Some(slot);
        wired
    }

    /// The label this scan reads, its variable, and whether it reads the
    /// schemaless main table rather than a per-label table.
    pub(crate) fn scan_target(&self) -> (&str, &str, bool) {
        (&self.label, &self.variable, self.is_schemaless)
    }

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
    /// Rows in this scan's table, cached where possible (#260).
    ///
    /// Zero means "unknown" — a fork, a pinned view, or a table that has not
    /// been counted — and every caller treats that as a reason not to read the
    /// whole table.
    pub(crate) async fn cached_table_rows(&self) -> usize {
        let key = uni_store::storage::cardinality::CardinalityKey::Vertex(self.label.clone());
        match self.graph_ctx.storage().cached_row_count(&key, None) {
            Some(rows) => rows as usize,
            None => self
                .graph_ctx
                .storage()
                .refresh_row_count(&key)
                .await
                .ok()
                .flatten()
                .unwrap_or(0) as usize,
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
            dynamic_vid_filter: None,
            dynamic_property_filter: None,
            planner_l0_prefilter: None,
            is_schemaless: false,
            schema,
            properties,
            fetch: None,
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
        let schema = Self::build_schemaless_vertex_schema(
            &variable,
            &label,
            &projected_properties,
            &uni_schema,
        );
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
            dynamic_vid_filter: None,
            dynamic_property_filter: None,
            planner_l0_prefilter: None,
            is_schemaless: true,
            schema,
            properties,
            fetch: None,
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
    /// `label` is the scan's label encoding: one label, the colon-joined set
    /// a vertex must carry all of, or empty for "scan all". A property takes
    /// a declared type only when one of the scanned labels declares it (and
    /// those that do agree), since every scanned vertex then carries a label
    /// whose writes are coerced to that type. Anything else is LargeBinary
    /// (CypherValue encoding), which keeps each value as it was written.
    ///
    /// It used to merge the declarations of every label in the schema by
    /// property name, so a `v: Float64` declared on one label typed `s.v` on
    /// an unrelated schemaless label, and an integer stored there read back
    /// widened: `9007199254740993` as `9007199254740992.0`.
    fn build_schemaless_vertex_schema(
        variable: &str,
        label: &str,
        properties: &[String],
        uni_schema: &uni_common::core::schema::Schema,
    ) -> SchemaRef {
        let scanned: Vec<&str> = label.split(':').filter(|l| !l.is_empty()).collect();
        let declared = |prop: &str| {
            uni_schema
                .scoped_property_meta(uni_common::core::schema::EntityScope::AllOf(&scanned), prop)
        };

        let mut fields = vec![
            Field::new(format!("{}._vid", variable), DataType::UInt64, false),
            Field::new(format!("{}._labels", variable), labels_data_type(), true),
        ];

        for prop in properties {
            let col_name = format!("{}.{}", variable, prop);
            let uni_type = declared(prop.as_str()).map(|meta| &meta.r#type);
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

        // A dynamic slot wins over the planner's static list: it is only ever
        // set by the operator immediately above, which knows the vids this run
        // needs. An attached-but-empty slot means "no vid restriction" and is a
        // deliberate value, not a miss — see `DynamicVidFilter`.
        let vid_list_filter = match self.dynamic_vid_filter.as_ref() {
            Some(slot) => slot.get(),
            None => self.vid_list_filter.clone(),
        };

        // A published property restriction narrows whatever the planner pushed
        // statically; both halves are conjoined so neither can widen the other.
        let (extra_lance_filter, extra_runtime_filter) = match self
            .dynamic_property_filter
            .as_ref()
            .and_then(|slot| slot.get())
        {
            Some((lance, runtime)) => (
                Some(match &self.extra_lance_filter {
                    Some(fixed) => format!("({fixed}) AND ({lance})"),
                    None => lance,
                }),
                Some(match &self.extra_runtime_filter {
                    Some(fixed) => {
                        Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                            Arc::clone(fixed),
                            datafusion::logical_expr::Operator::And,
                            runtime,
                        )) as Arc<dyn PhysicalExpr>
                    }
                    None => runtime,
                }),
            ),
            None => (
                self.extra_lance_filter.clone(),
                self.extra_runtime_filter.clone(),
            ),
        };

        Ok(Box::pin(GraphScanStream::new(
            self.graph_ctx.clone(),
            self.label.clone(),
            self.variable.clone(),
            self.projected_properties.clone(),
            self.is_schemaless,
            self.filter.clone(),
            vid_list_filter,
            extra_lance_filter,
            extra_runtime_filter,
            self.schema.clone(),
            metrics,
            index_consulted,
            context.session_config().batch_size(),
            context.memory_pool(),
            self.fetch,
            self.planner_l0_prefilter.clone(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    /// Accept a `LIMIT` from above (#239).
    ///
    /// Safe here in a way `ScanRequest::limit` is not: this does not truncate
    /// the rows the backend returns, it narrows the `_vid` **range walk** that
    /// decides which rows are asked for. Ranges are the one partitioning of
    /// this scan that MVCC dedup survives — `_vid` is the dedup key, so every
    /// version of a vid falls inside exactly one range — and the walk simply
    /// continues if a range under-delivers. So a limit can make the scan read
    /// less, never answer differently.
    ///
    /// The planner only offers this for a bare labelled scan under a limit with
    /// no ordering; see `df_planner::push_fetch_into_scan` for the guard.
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self {
            graph_ctx: self.graph_ctx.clone(),
            label: self.label.clone(),
            variable: self.variable.clone(),
            projected_properties: self.projected_properties.clone(),
            filter: self.filter.clone(),
            vid_list_filter: self.vid_list_filter.clone(),
            extra_lance_filter: self.extra_lance_filter.clone(),
            extra_runtime_filter: self.extra_runtime_filter.clone(),
            dynamic_vid_filter: self.dynamic_vid_filter.clone(),
            dynamic_property_filter: self.dynamic_property_filter.clone(),
            planner_l0_prefilter: self.planner_l0_prefilter.clone(),
            is_schemaless: self.is_schemaless,
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            fetch: limit,
            metrics: self.metrics.clone(),
        }))
    }
}

/// Widest `_vid` range a single scan call will ask for.
///
/// The walk doubles its range whenever one comes back under-full, which is how
/// it crosses a sparse label's gaps without a scan per empty stretch. The cap
/// stops that doubling from turning into a whole-table read the moment it lands
/// on a dense region again.
const RANGE_WIDTH_MAX: u64 = 1 << 22;

/// How far the walk must have widened before a `LIMIT` makes a seek worth it.
///
/// Seeking costs `O(log(distance))` filtered counts and saves `width -
/// slice_size` rows of reading, so it pays only once the gap-crossing has
/// widened the range well past a slice. Measured on LDBC SF1, `LIMIT 1`,
/// `rows_scanned`:
///
/// | label | vids begin at | before | after |
/// |---|---|---|---|
/// | `Person`  | 0         | 8 192   | 8 192 |
/// | `Post`    | 100 384   | 22 496  | 8 192 |
/// | `Comment` | 1 103 989 | 984 971 | 8 192 |
///
/// `Comment` is the case worth having: its width had doubled to ~1M by the
/// time the march reached the label's first row, so the first productive range
/// read nearly half the table to answer `LIMIT 1`. `Post` crosses only 100k
/// vids and lands on a range that is already near a slice's worth, where the
/// counts cost more than the rows they save — hence the factor rather than
/// seeking on every gap.
const SEEK_MIN_WIDTH_FACTOR: u64 = 8;

/// Bytes a single range should aim to return.
///
/// The walk exists to bound the scan's peak, and the peak is *bytes*, so this
/// is what the width is tuned against. An earlier version aimed at one output
/// batch's worth of **rows** instead, which is the same target only for a row
/// of average width — and it cost a full scan dearly. Measured on LDBC SF1
/// `Message` (3 055 774 rows), `RETURN count(n)`:
///
/// | | scans | time |
/// |---|---|---|
/// | before the walk | 1 | 1.16 s |
/// | walk targeting 8192 rows | 374 | 8.20 s |
///
/// One Lance round trip per 8192 rows is 374 of them on that table, and the
/// per-call overhead dominated everything the walk saved. Tuning on bytes lets
/// a narrow projection — `id(n)` is 8 bytes a row — take ranges hundreds of
/// times wider for the same peak, while a wide row still gets small ones.
///
/// 64 MiB is chosen to sit far above any realistic single output batch (so the
/// common case takes one range and pays no extra round trip) and far below the
/// multi-GB peaks #214 exists to prevent.
const RANGE_TARGET_BYTES: usize = 64 * 1024 * 1024;

/// Share of the query's whole budget one range may aim at.
///
/// The walk exists so the scan fits the pool, so the pool is what it should
/// size itself against — a fixed constant is either far too coarse for a small
/// budget or needless round trips for a large one. A sixteenth leaves room for
/// every other operator in the plan while still letting a 1 GiB budget take
/// ranges at the [`RANGE_TARGET_BYTES`] cap.
const RANGE_POOL_FRACTION: usize = 16;

/// Smallest range budget worth taking. Below this the round trips cost more
/// than the bound saves.
const RANGE_TARGET_BYTES_MIN: usize = 64 * 1024;

/// What one range should aim to return, given the budget it has to fit inside.
///
/// An unbounded or unknown pool gets the flat cap; a bounded one gets a share
/// of itself, so a query run under a tight `max_memory` is bounded finely and a
/// production query is not chopped into needless round trips.
fn range_target_bytes(pool: &Arc<dyn MemoryPool>) -> usize {
    match pool.memory_limit() {
        datafusion::execution::memory_pool::MemoryLimit::Finite(bytes) => {
            (bytes / RANGE_POOL_FRACTION).clamp(RANGE_TARGET_BYTES_MIN, RANGE_TARGET_BYTES)
        }
        _ => RANGE_TARGET_BYTES,
    }
}

/// Pick the next `_vid` range width from what the last one actually returned.
///
/// A range yields at most one live row per vid, so `rows <= width` always and
/// the width is an upper bound the label's density pulls down. Aiming each
/// range at [`RANGE_TARGET_BYTES`] keeps the peak bounded whether the label's
/// vids are packed or scattered, and whether its rows are narrow or wide — a
/// fixed width would read one row per scan on a sparse label and a whole
/// batch's worth on a dense one.
fn retune_range_width(width: u64, batch_bytes: usize, target_bytes: usize) -> u64 {
    let bytes = batch_bytes as u64;
    let target = target_bytes.max(1) as u64;
    if bytes == 0 || bytes.saturating_mul(2) < target {
        // Under-full: this range held less than half the budget, so the vids
        // here are sparser or the rows narrower than the width assumed.
        width.saturating_mul(2).min(RANGE_WIDTH_MAX)
    } else if bytes > target {
        // Over-full: scale down by the ratio actually observed rather than
        // halving, which would take several ranges to converge.
        (width.saturating_mul(target) / bytes).max(1)
    } else {
        width
    }
}

/// What a scan does after the batch in flight has been handed out.
///
/// The scan walks a label three ways — whole, by vid list (#55 pushdown), or by
/// `_vid` range (#214) — and they differ only in what "next" means. Naming that
/// explicitly is what keeps "this batch is the last" and "this chunk is the
/// last" from collapsing into the same `None`, which is the bug an
/// `Option<(vids, cursor)>` invited once a second walk existed.
#[derive(Clone)]
enum ScanResume {
    /// Nothing follows: the scan was unchunked, or the walk is finished.
    Finished,
    /// Continue the vid-list walk at `cursor`.
    Vids { vids: Arc<Vec<u64>>, cursor: usize },
    /// Continue the range walk at `[lo, lo + width)`.
    Range { lo: u64, width: u64 },
}

/// What the end-of-range probe learned when a range came back empty.
///
/// The walk asks a different question depending on whether a `LIMIT` bounds it,
/// and collapsing both answers into a `bool` is what made the expensive case
/// invisible: "there is more above" and "there is more above, and it starts
/// *here*" are not the same fact (#239).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeekOutcome {
    /// Rows remain above, position unknown. Widen and continue the march.
    ContinueHere,
    /// Rows remain above and the next one is at this `_vid`. Jump to it.
    ResumeAt(u64),
    /// No rows at or above the probe point; the label is finished.
    Exhausted,
}

/// State machine for graph scan stream execution.
enum GraphScanState {
    /// Initial state, ready to start scanning.
    Init,
    /// Scan a vid-filtered result one chunk of vids at a time.
    ///
    /// Holds the vids not yet scanned. Slicing alone did not bound what the
    /// scan *builds* — `RecordBatch::slice` is zero-copy, so every slice pins
    /// the parent's buffers and the whole result is resident before the first
    /// one is handed out. Scanning a chunk at a time gives each output batch
    /// its own storage, so the peak is one chunk rather than the whole result
    /// (#214). The traversal reached the same conclusion for the same reason;
    /// see `TraverseStreamState::Chunking`.
    ///
    /// Chunking is safe here precisely because the unit is a *vid* range. Every
    /// version of a vid falls inside one chunk, so the MVCC dedup that keeps
    /// the highest `_version` per vid still sees all the candidates, and the L0
    /// overlay is scoped to the same vid set it was asked for. Chunking by
    /// arriving storage batch would break both.
    ///
    /// Only reached for a vid set larger than one output batch; see `Init`.
    Chunking { vids: Arc<Vec<u64>>, cursor: usize },
    /// Deciding whether a full-label scan is big enough to walk in ranges.
    ///
    /// One metadata-only row count. Below one output batch the whole result is
    /// already within the bound chunking exists to impose, and the extra round
    /// trips would be pure cost on the most common query in the system.
    /// `None` from the count means the storage layer declined to answer
    /// cheaply — on a fork it would have scanned — so the walk is skipped and
    /// the scan reads whole, exactly as it did before #214.
    Sizing(Pin<Box<dyn std::future::Future<Output = DFResult<Option<usize>>> + Send>>),
    /// Walk a full-label scan in `_vid` ranges, `[lo, lo + width)` at a time.
    ///
    /// Sound for the same reason `Chunking` is: `_vid` is the MVCC dedup key,
    /// so a range partitions the row space along the axis the dedup groups on.
    /// Every version of a vid lands in exactly one range.
    RangeChunking { lo: u64, width: u64 },
    /// An empty range came back; asking whether anything remains above it.
    ///
    /// A range walk has no upper bound to stop at — the id allocator lives on
    /// the `Writer`, which a read path does not hold, and `ScanRequest` has no
    /// ordering to read a maximum from. So emptiness is ambiguous between "past
    /// the end" and "a gap", and this resolves it exactly. Paid only when a
    /// range is empty: normally once, just past the end.
    ConfirmingEnd {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<SeekOutcome>> + Send>>,
        lo: u64,
        width: u64,
    },
    /// Executing the async scan.
    ///
    /// `resume` carries where to continue when this call finishes, and is
    /// [`ScanResume::Finished`] for an unchunked scan — which is what makes
    /// "the scan is done" and "this chunk is done" distinguishable.
    Executing {
        fut: Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>>,
        resume: ScanResume,
    },
    /// A scan call finished; hand its rows out in `batch_size` slices.
    ///
    /// Emitting a batch whole gives every downstream operator a single
    /// indivisible input, and an operator that buffers — sort, hash aggregate,
    /// join — then has nothing to spill *between*: `ExternalSorter` asked for
    /// 5.1 GB in one reservation on LDBC IC9 and failed, with a disk manager
    /// available the whole time (`DiskManagerMode` defaults to
    /// `OsTmpDirectory`). Slicing is what lets the spill path engage. See
    /// issue #202.
    ///
    /// Slicing bounds what the scan *emits*, never what it *builds*:
    /// `RecordBatch::slice` is zero-copy, so each slice pins the parent's
    /// buffers. Bounding construction is the chunked states' job — `Chunking`
    /// for a vid-filtered scan, `RangeChunking` for a full-label one (#214) —
    /// and this state is reached from all three, so the batch it holds is a
    /// whole result or one chunk depending on which fed it.
    Slicing {
        batch: RecordBatch,
        offset: usize,
        resume: ScanResume,
    },
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

    /// Rows a `LIMIT` above this scan will keep, when the planner proved the
    /// pushdown safe (#239). `None` means read the label whole.
    fetch: Option<usize>,

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

    /// The schemaless scan's L0 equality prefilter (W3b).
    planner_l0_prefilter: Option<L0PropertyPrefilter>,

    /// Output schema.
    schema: SchemaRef,

    /// Stream state.
    state: GraphScanState,

    /// Metrics.
    metrics: BaselineMetrics,

    /// Per-node count of scans that consulted a scalar index, surfaced as
    /// `OperatorStats::index_hits`.
    index_consulted: Count,

    /// Bytes one `_vid` range should aim to return, derived from the query's
    /// budget at construction. See [`range_target_bytes`].
    range_target_bytes: usize,

    /// The query pool's accounting for the batch being sliced below.
    ///
    /// That batch is the whole result for an unchunked scan and one chunk for a
    /// chunked one; either way the scan holds it for as long as it is slicing,
    /// so the reservation lives on the stream rather than inside the scan
    /// future — the memory is resident across every poll that follows, not just
    /// while it is being built. It is released when the stream is dropped.
    ///
    /// The slices handed downstream are zero-copy views onto that batch, so the
    /// buffers stay alive while any consumer holds one. Accounting for it once,
    /// here, is what lets the pool see the scan at all (#242).
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
        fetch: Option<usize>,
        planner_l0_prefilter: Option<L0PropertyPrefilter>,
    ) -> Self {
        Self {
            range_target_bytes: range_target_bytes(pool),
            fetch,
            graph_ctx,
            label,
            variable,
            properties,
            is_schemaless,
            filter,
            vid_list_filter,
            extra_lance_filter,
            extra_runtime_filter,
            planner_l0_prefilter,
            schema,
            state: GraphScanState::Init,
            metrics,
            index_consulted,
            slice_size: slice_size.max(1),
            reservation: MemoryConsumer::new("GraphScanExec").register(pool),
        }
    }

    /// One scan call, restricted to `vid_list_filter`.
    ///
    /// Factored out so a chunk and a whole-result scan go through the same
    /// code. They differ only in the vid list handed down: `None` and the
    /// stream's own filter are the unchunked cases, a slice of it is a chunk.
    fn scan_future(
        &self,
        vid_list_filter: Option<Vec<u64>>,
        vid_range: Option<(u64, u64)>,
    ) -> Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>> {
        let graph_ctx = self.graph_ctx.clone();
        let label = self.label.clone();
        let variable = self.variable.clone();
        let properties = self.properties.clone();
        let is_schemaless = self.is_schemaless;
        let filter = self.filter.clone();
        let extra_lance_filter = self.extra_lance_filter.clone();
        let extra_runtime_filter = self.extra_runtime_filter.clone();
        let planner_l0_prefilter = self.planner_l0_prefilter.clone();
        let schema = self.schema.clone();
        let index_consulted = self.index_consulted.clone();

        Box::pin(async move {
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
                    vid_range,
                    extra_lance_filter.as_deref(),
                    extra_runtime_filter.as_ref(),
                    planner_l0_prefilter.as_ref(),
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
                    vid_range,
                    extra_lance_filter.as_deref(),
                    extra_runtime_filter.as_ref(),
                    planner_l0_prefilter.as_ref(),
                    Some(&index_consulted),
                )
                .await?
            };
            Ok(Some(batch))
        })
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
    if let Some(bin) = filter.downcast_ref::<BinaryExpr>() {
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
    let col = col_side.downcast_ref::<Column>()?;
    if col.name() != "_vid" && !col.name().ends_with("._vid") {
        return None;
    }

    // Try direct literal
    if let Some(lit) = val_side.downcast_ref::<Literal>() {
        return scalar_to_u64(lit.value());
    }

    // Try CAST(literal AS UInt64)
    if let Some(cast) = val_side.downcast_ref::<CastExpr>()
        && let Some(lit) = cast.expr().downcast_ref::<Literal>()
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
/// Requested target vids per target-table row at or above which deduplicating
/// the request pays for itself.
///
/// Hydration is called once per traversal batch, not once per query — measured
/// at ~8 192 vids per call carrying ~1 400-2 500 distinct ones — so the choice
/// is made per call and has to be cheap. The statistic is the target table's
/// row count, which `count_rows` answers from fragment metadata rather than by
/// reading rows.
///
/// The two arms diverge on table size, not on request size. Deduplicating
/// shrinks the `IN` list, which is worth real time against a *small* target
/// table and costs a hash set over the request against a large one, where the
/// list was already selective. Warm min-of-3 on LDBC SF1, hydration isolated by
/// differential (`examples/vertex_selectivity_probe.rs`):
///
/// | traversal | target rows | requests | distinct | without | with |
/// |---|---|---|---|---|---|
/// | `HAS_CREATOR`->Person | 9 892 | 2 052 169 | 9 343 | 4 247 ms | 921 ms |
/// | `KNOWS`->Person | 9 892 | 180 623 | 8 466 | 364 ms | 195 ms |
/// | `REPLY_OF`->Comment | 2 052 169 | 1 040 749 | 441 704 | 5 732 ms | 7 601 ms |
///
/// At a per-call request of ~8 192, a ratio of 8 admits the two Person targets
/// (8 192 * 8 >= 9 892) and excludes the Comment one (8 192 * 8 < 2 052 169),
/// with two orders of magnitude of margin on each side.
///
/// **Fitted to three shapes on one dataset**, and named so the next person
/// re-measures rather than trusts it -- the same caveat #221's rustdoc makes
/// about the edge-side constants, which is why #237 refused to copy those
/// across rather than measure the vertex side on its own terms. Sorting was
/// tried first, on the theory that the loss came from discarding
/// traversal-order locality; order-preserving deduplication measured the same,
/// so the cost is the deduplication itself and the choice has to be
/// conditional rather than reordered.
const DEDUP_TABLE_RATIO: usize = 8;

/// Requested vids per table row at which one unfiltered pass replaces chunked
/// `_vid IN (...)` lookups (#237).
///
/// Four, meaning 25% of the table.
const VERTEX_SCAN_SELECTIVITY_DIVISOR: usize = 4;

/// Whether one pass over the label's table beats chunked vid lookups.
///
/// # Measured
///
/// `uni-store/examples/vertex_arm_probe.rs`, release, min-of-3, over a table
/// written through the ordinary path so its `_vid` BTree exists — and the probe
/// refuses to report unless `index_comparisons` proves the lookup arm actually
/// used it:
///
/// | rows | scan | crossover K | K/N |
/// |---|---|---|---|
/// | 30 000 | 1.7 ms | ~280 | ~0.9% |
/// | 300 000 | 4.0 ms | ~800 | ~0.27% |
/// | 1 000 000 | 7.0 ms | ~1 100 | ~0.11% |
///
/// Two things follow. The scan arm is flat in K and the lookup arm is linear, so
/// a crossover exists at every size; and **K/N is not stable** — it falls about
/// 8x across a 33x range of rows, because the columnar scan grows far slower
/// than linearly. #237 predicted exactly this when it refused to let #221's
/// edge-side ratio be copied across, and it is why the threshold below is not
/// derived from those crossovers.
///
/// # Why the threshold is far above the crossover
///
/// Time is not the only axis, and the two disagree. Just past the crossover the
/// scan arm is barely faster while materialising the whole table to return a
/// few hundred rows: at K=1 000 of 1 000 000 it would save 0.7 ms and read 1 000x
/// the rows. Chunking exists here to bound peak residency — 60 000 vids from a
/// 300k-row table went 815 MiB -> 226 MiB — and trading that away for
/// sub-millisecond wins would undo it.
///
/// So the switch waits until the memory is comparable anyway, where the time win
/// is large rather than marginal. At 25% the scan reads 4x the rows for a ~50x
/// speed-up (208 ms -> 4.0 ms at 300k); at 50%, 2x the rows for ~100x (412 ms ->
/// 4.0 ms). Below that the chunked arm keeps the residency bound it was written
/// for.
///
/// Returns `false` when the table size is unknown — a fork, a pinned view, or a
/// declined count — because an unknown size is not a reason to read a whole
/// table.
pub(crate) fn vertex_scan_beats_lookup(requested: usize, table_rows: usize) -> bool {
    table_rows > 0 && requested.saturating_mul(VERTEX_SCAN_SELECTIVITY_DIVISOR) >= table_rows
}

/// Distinct target vids, in first-occurrence order.
///
/// Correctness does not depend on whether this is used: the gather in
/// [`hydrate_vids_columnar`] is keyed by vid, so duplicates fan back out from
/// whichever list was fetched and the output is one row per *request* either
/// way. Only the amount read changes.
fn dedup_targets(raw: &[u64]) -> Vec<u64> {
    let mut seen = std::collections::HashSet::with_capacity(raw.len());
    raw.iter().copied().filter(|v| seen.insert(*v)).collect()
}

pub(crate) async fn hydrate_vids_columnar(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    properties: &[String],
    vids: &[Vid],
) -> DFResult<Vec<ArrayRef>> {
    prefetch_vids_columnar(graph_ctx, label, variable, properties, vids)
        .await?
        .gather(vids)
}

/// Target property columns read once, ready to gather by vid.
///
/// Splitting the read from the gather is what lets a traversal pay the storage
/// cost for its whole expansion set at once, in `_vid` order, and then serve
/// each output chunk from the result -- see [`prefetch_vids_columnar`].
pub(crate) struct PrefetchedProps {
    row_of: HashMap<u64, u32>,
    columns: Vec<ArrayRef>,
}

impl PrefetchedProps {
    /// Bytes this read holds, for the query pool.
    ///
    /// Counted per distinct allocation rather than by summing
    /// `get_array_memory_size`, because the columns can be slices of one scan's
    /// buffers and summing their parents' capacities charges the same
    /// allocation once per column (#261).
    pub(crate) fn memory_bytes(&self) -> usize {
        let mut footprint = crate::query::df_graph::common::BatchFootprint::new();
        let columns = footprint.add_arrays(&self.columns);
        // The vid -> row map is the other half of what is held, and it is not
        // Arrow-shaped: one entry per distinct vid read.
        let index =
            self.row_of.capacity() * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        columns + index
    }

    /// Gather one output column per requested property, one row per vid.
    ///
    /// A vid with no row (deleted, or not of this label) gathers as null, which
    /// is what the per-chunk read did for the same case.
    pub(crate) fn gather(&self, vids: &[Vid]) -> DFResult<Vec<ArrayRef>> {
        let indices: arrow_array::UInt32Array = vids
            .iter()
            .map(|vid| self.row_of.get(&vid.as_u64()).copied())
            .collect::<Vec<Option<u32>>>()
            .into();
        let mut out = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            out.push(arrow::compute::take(col.as_ref(), &indices, None).map_err(arrow_err)?);
        }
        Ok(out)
    }
}

/// Read property columns for targets that span several labels.
///
/// Each group is read columnar from its own label's table -- typed, and key-local
/// because the vids are sorted -- and the groups are then concatenated into one
/// [`PrefetchedProps`], which gathers exactly as the single-label one does,
/// including yielding null for a vid no group returned.
///
/// The caller must have established that every label here declares every
/// requested property at the same type (see `uniform_target_schema_props`);
/// otherwise the concatenation below has nothing well-typed to produce.
///
/// # Errors
///
/// Propagates each group's read, and fails if the groups disagree on a column
/// type after all -- which would mean the uniformity check and the schema have
/// drifted apart.
pub(crate) async fn prefetch_vids_columnar_grouped(
    graph_ctx: &GraphExecutionContext,
    variable: &str,
    properties: &[String],
    groups: &[(String, Vec<Vid>)],
) -> DFResult<PrefetchedProps> {
    let mut row_of: HashMap<u64, u32> = HashMap::new();
    let mut per_group: Vec<Vec<ArrayRef>> = Vec::with_capacity(groups.len());
    let mut offset: u32 = 0;

    for (label, vids) in groups {
        let group = prefetch_vids_columnar(graph_ctx, label, variable, properties, vids).await?;
        let rows = group
            .columns
            .first()
            .map_or(0, |c| u32::try_from(c.len()).unwrap_or(u32::MAX));
        for (vid, row) in &group.row_of {
            row_of.insert(*vid, row.saturating_add(offset));
        }
        offset = offset.saturating_add(rows);
        per_group.push(group.columns);
    }

    let mut columns = Vec::with_capacity(properties.len());
    for idx in 0..properties.len() {
        let arrays: Vec<&dyn arrow_array::Array> = per_group
            .iter()
            .filter_map(|g| g.get(idx).map(|c| c.as_ref()))
            .collect();
        columns.push(arrow::compute::concat(&arrays).map_err(arrow_err)?);
    }

    Ok(PrefetchedProps { row_of, columns })
}

/// Read the property columns for `vids`, fetching in `_vid` order.
///
/// # Errors
///
/// Propagates the storage read, and fails if the scan returns no `_vid` column.
pub(crate) async fn prefetch_vids_columnar(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    variable: &str,
    properties: &[String],
    vids: &[Vid],
) -> DFResult<PrefetchedProps> {
    let uni_schema = graph_ctx.storage().schema_manager().schema();
    let output_schema =
        GraphScanExec::build_vertex_schema(variable, label, properties, &uni_schema);

    let raw: Vec<u64> = vids.iter().map(|v| v.as_u64()).collect();

    // Fetch each distinct vid once, when the duplication makes that pay (#237).
    //
    // `vids` is one entry per *traversal target*, not per vertex, so a target
    // reached by many edges appears many times.
    // `count_rows` on the target table, which is metadata-only and so cheap
    // enough to ask per call. `None` means the storage layer declined to answer
    // cheaply (a forked session would have scanned), and an unknown table size
    // is not a reason to pay for deduplication.
    // Cached where possible (#260), measured on a miss. Same contract as the
    // `vertex_row_count` call this replaces — `None` for a fork or a pinned
    // view, and zero standing for "declined" — but paid once per table rather
    // than once per call.
    let cardinality_key =
        uni_store::storage::cardinality::CardinalityKey::Vertex(label.to_string());
    let target_rows = match graph_ctx.storage().cached_row_count(&cardinality_key, None) {
        Some(rows) => rows as usize,
        None => graph_ctx
            .storage()
            .refresh_row_count(&cardinality_key)
            .await
            .ok()
            .flatten()
            .unwrap_or(0) as usize,
    };
    let pays = target_rows > 0 && raw.len().saturating_mul(DEDUP_TABLE_RATIO) >= target_rows;
    let deduped = pays.then(|| dedup_targets(&raw));
    let unsorted: &[u64] = deduped.as_deref().unwrap_or(&raw);

    // Fetch in key order, so each chunk below covers a narrow `_vid` range.
    //
    // A `_vid IN (...)` lookup costs the *span* it straddles -- the distance
    // from its smallest key to its largest -- not the number of vids it asks
    // for. Measured on LDBC SF1's 3.06M-row `vertices_Message`, 8192 vids per
    // scan in every arm, only the span varied:
    //
    // | span      | index_comparisons | ms  |
    // |-----------|-------------------|-----|
    // | 8 192     | 12 288            | 137 |
    // | 65 536    | 69 632            | 154 |
    // | 524 288   | 528 384           | 335 |
    // | 2 000 000 | 2 002 944         | 886 |
    //
    // Comparisons track the span to within one 4096-entry page. Traversal
    // emits targets in visit order, scattered across the whole table, so
    // chunking them as they arrive gave *every* chunk the full span: 51 chunks
    // x ~2.4M = 123M comparisons to read one property off 416k rows, 48 s.
    // Sorting first gives each chunk its own ~1/51 slice of the key space.
    //
    // Sorting *within* a chunk is not what pays and was measured too: the same
    // scattered 8192 vids sorted and shuffled both cost 2 899 968 comparisons,
    // identical. The list has to be ordered before it is cut, not after.
    //
    // Correctness does not depend on the order: the gather below gathers by
    // `raw` against a vid-keyed row map, so the output stays one row per
    // request in request order however the fetch was sequenced.
    let sorted = (unsorted.len() > 1).then(|| {
        let mut v = unsorted.to_vec();
        v.sort_unstable();
        v
    });
    let fetch_vids: &[u64] = sorted.as_deref().unwrap_or(unsorted);

    // Chunk the vid list, bounding how much is resident at once.
    //
    // The `_vid` index is used either way. The index work costs the span each
    // chunk straddles, which is why the list is sorted above: "~1 comparison
    // per requested vid", as this comment used to claim, holds only for a
    // dense list and was measured on one. A scattered chunk pays its whole
    // range -- 8192 vids spread over 2M keys cost 2 002 944 comparisons, the
    // same 8192 vids packed together cost 12 288.
    //
    // What chunking itself bounds is what happens *after* the lookup -- the
    // matching rows are scattered across proportionally more pages in a larger
    // table, and unchunked they are all materialised at once. Chunking caps the
    // peak at one chunk's worth: 60,000 vids read from a 300k-row table went
    // from 815 MiB to 226 MiB, and stopped tracking the table's size.
    //
    // `VidLookupJoinExec` already chunks this exact shape at the same constant.
    //
    // At high selectivity one unfiltered pass replaces the chunks entirely —
    // see `vertex_scan_beats_lookup` for the measurement and for why the
    // threshold is not the point where the scan merely becomes faster.
    let scan_whole_table = vertex_scan_beats_lookup(fetch_vids.len(), target_rows);
    let mut parts: Vec<RecordBatch> = Vec::new();
    if scan_whole_table {
        // No vid filter: every row comes back and the gather below selects the
        // requested ones, exactly as it does from the chunked parts.
        parts.push(
            columnar_scan_vertex_batch_static(
                graph_ctx,
                label,
                variable,
                properties,
                &output_schema,
                &None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?,
        );
    } else {
        for chunk in fetch_vids.chunks(crate::query::df_graph::vid_lookup_join::MAX_VIDS_PER_CHUNK)
        {
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
                    None,
                    None,
                )
                .await?,
            );
        }
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
    // Skip `_vid`/`_labels`; the caller wants the property columns only, in the
    // order it asked for them.
    let columns = (0..properties.len())
        .map(|idx| Arc::clone(batch.column(idx + 2)))
        .collect();
    Ok(PrefetchedProps { row_of, columns })
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn columnar_scan_vertex_batch_static(
    graph_ctx: &GraphExecutionContext,
    label: &str,
    // Column naming is the caller's `output_schema`, which
    // `map_to_output_schema` maps positionally; the variable only names the
    // runtime filter's columns, for recovering its equality (W3b).
    variable: &str,
    projected_properties: &[String],
    output_schema: &SchemaRef,
    filter: &Option<Arc<dyn PhysicalExpr>>,
    vid_list_filter: Option<&[u64]>,
    // Half-open `_vid` range this call is restricted to, when the caller walks
    // the label in bounded ranges instead of reading it whole (#214).
    vid_range: Option<(u64, u64)>,
    extra_lance_filter: Option<&str>,
    extra_runtime_filter: Option<&Arc<dyn PhysicalExpr>>,
    // Used when `extra_runtime_filter` yields no L0 prefilter: the planner's
    // own, for a predicate pushed to Lance without a runtime filter (`ext_id`).
    planner_l0_prefilter: Option<&L0PropertyPrefilter>,
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
    // The runtime filter below stays authoritative; this structured copy only
    // lets the storage layer skip L0 rows that cannot match (W3b).
    let l0_prefilter = extra_runtime_filter
        .and_then(|f| l0_property_prefilter(f, &format!("{variable}.")))
        .or_else(|| planner_l0_prefilter.cloned());

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
            vid_range,
            extra_lance_filter,
            l0_prefilter: l0_prefilter.as_ref(),
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

/// An equality prefilter for a schemaless scan's L0 rows, read from the
/// scan's Cypher filter (W3b).
///
/// A schemaless scan gets no indexed-property runtime filter to recover one
/// from, so this reads the Cypher predicate directly. Recognized: `AND` (the
/// first recognized conjunct is used — any one conjunct is a superset of the
/// conjunction), `{var}.{prop} = v` with `v` a literal or a parameter on
/// either side, `{var}.{prop} IN` a literal list or a list parameter, and an
/// `OR` of such on the same property. A NULL value matches nothing, so it is
/// dropped from the set; a NULL equality is not recognized. Anything else
/// returns `None`, which keeps every L0 row — a miss costs the old read,
/// never a row.
pub(crate) fn schemaless_l0_prefilter(
    filter: Option<&uni_cypher::ast::Expr>,
    variable: &str,
    params: &HashMap<String, Value>,
) -> Option<L0PropertyPrefilter> {
    use uni_cypher::ast::{BinaryOp, Expr};

    fn property_of<'e>(e: &'e Expr, variable: &str) -> Option<&'e str> {
        match e {
            Expr::Property(base, prop)
                if matches!(base.as_ref(), Expr::Variable(v) if v == variable)
                    && !uni_common::core::schema::is_reserved_property_name(prop) =>
            {
                Some(prop.as_str())
            }
            _ => None,
        }
    }
    fn constant(e: &Expr, params: &HashMap<String, Value>) -> Option<Value> {
        match e {
            Expr::Literal(lit) => Some(lit.to_value()),
            Expr::Parameter(name) => params.get(name).cloned(),
            _ => None,
        }
    }
    fn leaf(
        e: &Expr,
        variable: &str,
        params: &HashMap<String, Value>,
    ) -> Option<(String, Vec<Value>)> {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let (prop, value) =
                    match (property_of(left, variable), property_of(right, variable)) {
                        (Some(p), None) => (p, constant(right, params)?),
                        (None, Some(p)) => (p, constant(left, params)?),
                        _ => return None,
                    };
                (!value.is_null()).then(|| (prop.to_string(), vec![value]))
            }
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                let (lp, mut lv) = leaf(left, variable, params)?;
                let (rp, rv) = leaf(right, variable, params)?;
                (lp == rp).then(|| {
                    lv.extend(rv);
                    (lp, lv)
                })
            }
            Expr::In { expr, list } => {
                let prop = property_of(expr, variable)?;
                let items = match list.as_ref() {
                    Expr::List(items) => items
                        .iter()
                        .map(|i| constant(i, params))
                        .collect::<Option<Vec<_>>>()?,
                    other => match constant(other, params)? {
                        Value::List(items) => items,
                        _ => return None,
                    },
                };
                Some((
                    prop.to_string(),
                    items.into_iter().filter(|v| !v.is_null()).collect(),
                ))
            }
            _ => None,
        }
    }
    fn first(
        e: &Expr,
        variable: &str,
        params: &HashMap<String, Value>,
    ) -> Option<(String, Vec<Value>)> {
        match e {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => first(left, variable, params).or_else(|| first(right, variable, params)),
            other => leaf(other, variable, params),
        }
    }

    let (property, values) = first(filter?, variable, params)?;
    Some(L0PropertyPrefilter {
        property,
        one_of: Some(values),
        bounds: Vec::new(),
    })
}

/// Recover equality and range constraints on one property from the
/// indexed-property runtime filter, as an [`L0PropertyPrefilter`] the storage
/// layer can apply to L0 rows before materializing them (W3b, F1).
///
/// The planner builds the runtime filter and the Lance string from the same
/// indexed conjuncts but hands the scan no structured form, so this reads one
/// back from the physical expression. Anything not recognized returns `None`,
/// which keeps the unrestricted L0 read — the failure mode of a miss is the old
/// cost, never a wrong row. What *is* recognized must describe a superset of
/// the rows the filter accepts:
///
/// * `AND` — the conjuncts are collected and grouped by property; any one
///   property's constraints, all of them conjoined, are a superset of the
///   whole conjunction. A property pinned by an equality is preferred, since
///   it is the most selective; a conjunct not recognized is simply not used;
/// * `OR` — both sides must pin the same column by equality, and the values
///   union; a range under an `OR` gives up;
/// * `<`, `<=`, `>`, `>=` — one side a bare property column, the other a
///   literal; a literal on the left mirrors the operator;
/// * the column side must be a bare `Column` named `{prefix}{prop}`: a cast on
///   it can make unequal stored values compare equal (`CAST(5.5 AS BIGINT)`);
/// * the literal side may be cast, which is evaluated here exactly as
///   DataFusion would, and a NULL or non-scalar literal gives up.
fn l0_property_prefilter(
    filter: &Arc<dyn PhysicalExpr>,
    prefix: &str,
) -> Option<L0PropertyPrefilter> {
    let mut found: Vec<L0PropertyPrefilter> = Vec::new();
    collect_l0_conjunct_prefilters(filter, prefix, &mut found);
    let chosen = found
        .iter()
        .find(|f| f.one_of.is_some())
        .or_else(|| found.first())?
        .property
        .clone();
    let mut merged = L0PropertyPrefilter {
        property: chosen,
        one_of: None,
        bounds: Vec::new(),
    };
    for f in found.into_iter().filter(|f| f.property == merged.property) {
        if merged.one_of.is_none() {
            merged.one_of = f.one_of;
        }
        merged.bounds.extend(f.bounds);
    }
    Some(merged)
}

/// Push one [`L0PropertyPrefilter`] per recognized conjunct of `filter`.
fn collect_l0_conjunct_prefilters(
    filter: &Arc<dyn PhysicalExpr>,
    prefix: &str,
    out: &mut Vec<L0PropertyPrefilter>,
) {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::BinaryExpr;

    if let Some(bin) = filter.downcast_ref::<BinaryExpr>()
        && *bin.op() == Operator::And
    {
        collect_l0_conjunct_prefilters(bin.left(), prefix, out);
        collect_l0_conjunct_prefilters(bin.right(), prefix, out);
        return;
    }
    if let Some(leaf) = l0_leaf_prefilter(filter, prefix) {
        out.push(leaf);
    }
}

/// The prefilter a single non-`AND` conjunct describes, if recognized.
fn l0_leaf_prefilter(filter: &Arc<dyn PhysicalExpr>, prefix: &str) -> Option<L0PropertyPrefilter> {
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, InListExpr};

    let property_of = |e: &Arc<dyn PhysicalExpr>| -> Option<String> {
        let col = e.downcast_ref::<datafusion::physical_expr::expressions::Column>()?;
        let prop = col.name().strip_prefix(prefix)?;
        (!prop.is_empty() && !uni_common::core::schema::is_reserved_property_name(prop))
            .then(|| prop.to_string())
    };

    if let Some(bin) = filter.downcast_ref::<BinaryExpr>() {
        // `(op with the property on the left, op with it on the right)`.
        let range_ops = match bin.op() {
            Operator::Lt => Some((CmpOp::Lt, CmpOp::Gt)),
            Operator::LtEq => Some((CmpOp::LtEq, CmpOp::GtEq)),
            Operator::Gt => Some((CmpOp::Gt, CmpOp::Lt)),
            Operator::GtEq => Some((CmpOp::GtEq, CmpOp::LtEq)),
            _ => None,
        };
        if let Some((left_op, right_op)) = range_ops {
            let (property, op, lit) = match (property_of(bin.left()), property_of(bin.right())) {
                (Some(p), None) => (p, left_op, bin.right()),
                (None, Some(p)) => (p, right_op, bin.left()),
                _ => return None,
            };
            return Some(L0PropertyPrefilter {
                property,
                one_of: None,
                bounds: vec![(op, physical_literal_value(lit)?)],
            });
        }
        match bin.op() {
            Operator::Eq => {
                let (prop, lit) = match (property_of(bin.left()), property_of(bin.right())) {
                    (Some(p), None) => (p, bin.right()),
                    (None, Some(p)) => (p, bin.left()),
                    _ => return None,
                };
                return Some(L0PropertyPrefilter {
                    property: prop,
                    one_of: Some(vec![physical_literal_value(lit)?]),
                    bounds: Vec::new(),
                });
            }
            Operator::Or => {
                let l = l0_leaf_prefilter(bin.left(), prefix)?;
                let r = l0_leaf_prefilter(bin.right(), prefix)?;
                let (Some(mut lv), Some(rv)) = (l.one_of, r.one_of) else {
                    return None;
                };
                if l.property != r.property || !l.bounds.is_empty() || !r.bounds.is_empty() {
                    return None;
                }
                lv.extend(rv);
                return Some(L0PropertyPrefilter {
                    property: l.property,
                    one_of: Some(lv),
                    bounds: Vec::new(),
                });
            }
            Operator::And => {
                // Reached only under an `OR`: one side alone is a superset.
                return l0_leaf_prefilter(bin.left(), prefix)
                    .or_else(|| l0_leaf_prefilter(bin.right(), prefix));
            }
            _ => return None,
        }
    }
    if let Some(in_list) = filter.downcast_ref::<InListExpr>() {
        if in_list.negated() {
            return None;
        }
        let property = property_of(in_list.expr())?;
        let values = in_list
            .list()
            .iter()
            .map(physical_literal_value)
            .collect::<Option<Vec<_>>>()?;
        return Some(L0PropertyPrefilter {
            property,
            one_of: Some(values),
            bounds: Vec::new(),
        });
    }
    None
}

/// The value a literal operand (possibly cast) evaluates to, as a [`Value`].
///
/// `None` for anything but a non-null boolean, integer, float or string.
fn physical_literal_value(e: &Arc<dyn PhysicalExpr>) -> Option<Value> {
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::{CastExpr, Literal};

    let scalar = if let Some(lit) = e.downcast_ref::<Literal>() {
        lit.value().clone()
    } else {
        let cast = e.downcast_ref::<CastExpr>()?;
        let lit = cast.expr().downcast_ref::<Literal>()?;
        lit.value().cast_to(cast.cast_type()).ok()?
    };
    match scalar {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(Value::String(s)),
        ScalarValue::Int8(Some(v)) => Some(Value::Int(v.into())),
        ScalarValue::Int16(Some(v)) => Some(Value::Int(v.into())),
        ScalarValue::Int32(Some(v)) => Some(Value::Int(v.into())),
        ScalarValue::Int64(Some(v)) => Some(Value::Int(v)),
        ScalarValue::UInt8(Some(v)) => Some(Value::Int(v.into())),
        ScalarValue::UInt16(Some(v)) => Some(Value::Int(v.into())),
        ScalarValue::UInt32(Some(v)) => Some(Value::Int(v.into())),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v).ok().map(Value::Int),
        ScalarValue::Float32(Some(v)) => Some(Value::Float(v.into())),
        ScalarValue::Float64(Some(v)) => Some(Value::Float(v)),
        ScalarValue::Boolean(Some(b)) => Some(Value::Bool(b)),
        _ => None,
    }
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
    vid_range: Option<(u64, u64)>,
    extra_lance_filter: Option<&str>,
    extra_runtime_filter: Option<&Arc<dyn PhysicalExpr>>,
    l0_prefilter: Option<&L0PropertyPrefilter>,
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
            // A range is two comparisons however wide it gets, where an IN list
            // would grow with it — same shape the labelled path uses.
            _ => {
                if let Some((lo, hi)) = vid_range {
                    parts.push(FilterExpr::compare("_vid", CmpOp::GtEq, Scalar::UInt(lo)));
                    parts.push(FilterExpr::compare("_vid", CmpOp::Lt, Scalar::UInt(hi)));
                }
            }
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

    // `props_json` carries every property of every vertex as a blob, and it is
    // read only by the projected-property loop below. A query that projects
    // nothing from the row — `count(n)`, `id(n)` — was still paying to read
    // and ship it for every row.
    //
    // The cost is not only the bytes. The `_vid` range walk sizes each range
    // against a byte budget, so fat rows buy narrow ranges: at LDBC SF1 a
    // `MATCH (n) RETURN count(n)` took 18 ranges carrying the blob and the
    // per-range overhead dominated. Dropping the column when nothing reads it
    // makes the same budget buy far wider ranges *and* moves less data.
    //
    // `projected_properties` is the whole test: `_all_props` arrives through it
    // like any other name, so an empty list is the only case where no reader
    // exists. The internal schema is taken from the scanned batch below, so
    // omitting the column here removes it consistently -- the L0 builder's
    // `props_json` arm stops firing and `column_by_name` returns `None`, which
    // only the projected-property loop would have consulted.
    //
    // **This guard does not fire for `MATCH (n) RETURN count(n)` today**, and
    // that is worth knowing before anyone measures it and calls it dead. That
    // query arrives here with `projected_properties == ["_all_props"]` and an
    // output schema of `n._vid, n._labels, n._all_props` -- the planner asks
    // for every property of every vertex in order to count them. Nothing in
    // this file can tell that the blob is unread, because by the time the
    // request arrives the projection already claims a reader. Closing that is
    // a column-pruning pass (there is none; see #184/#185), or a narrower rule
    // that an aggregate over an entity does not need the entity's properties.
    // Until then the blob is read regardless and this branch waits for it.
    let needs_props_blob = !projected_properties.is_empty();
    let scan_columns: &[&str] = if needs_props_blob {
        &["_vid", "_deleted", "labels", "props_json", "_version"]
    } else {
        &["_vid", "_deleted", "labels", "_version"]
    };

    // Single Lance query via StorageManager domain method. Counted, so this
    // scan appears in `scans_reported` — the schemaless path was invisible to
    // the counters the labelled path already reports through.
    let lance_batch = storage
        .scan_main_vertex_table_counted(scan_columns, filter.as_ref(), graph_ctx.counters())
        .await
        .map_err(exec_err)?;

    // A pushed property predicate hides a vid's CURRENT row from the scan when
    // that row no longer matches (MVCC-append: the stale still-matching row
    // would win the dedup by default) — drop superseded rows first. The label
    // filter does the same to a vertex relabeled away from `label`: its
    // current row lacks the label, so its older row, which has it, was the
    // only one left and the vertex matched a label it no longer carries.
    let row_filtered = extra_lance_filter.is_some() || !label.is_empty();
    let lance_batch = match (lance_batch, row_filtered) {
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
    let l0_targets: SchemalessL0Targets<'_> = match (vid_list_filter, target_vid) {
        (Some(vs), _) if !vs.is_empty() => SchemalessL0Targets::Vids(vs),
        (_, Some(v)) => {
            single_vid_buf = [v];
            SchemalessL0Targets::Vids(&single_vid_buf)
        }
        // Same restriction Lance was given. Without it every range would
        // re-emit the whole L0 set and the chunks would duplicate rather than
        // partition — the labelled path carries the identical note.
        _ => match vid_range {
            Some((lo, hi)) => SchemalessL0Targets::Range(lo, hi),
            None => SchemalessL0Targets::All,
        },
    };
    let label_only = uni_store::runtime::columnar_scan::label_only_l0_base_props(
        storage,
        l0_ctx,
        label,
        |vid| match l0_targets {
            SchemalessL0Targets::Vids(vids) => vids.contains(&vid),
            SchemalessL0Targets::Range(lo, hi) => vid >= lo && vid < hi,
            SchemalessL0Targets::All => true,
            SchemalessL0Targets::Filtered { .. } => {
                unreachable!("the prefilter narrows the targets after this read")
            }
        },
        graph_ctx.counters(),
    )
    .await
    .map_err(exec_err)?;
    // W3b for the main table: skip L0 rows a pushed equality cannot match
    // before building them. The flushed side has no property pushdown here,
    // so its vids — every one of them stays — are the shadow set.
    let kept: Option<HashSet<u64>> = match (l0_prefilter, l0_targets) {
        (Some(pf), SchemalessL0Targets::All | SchemalessL0Targets::Range(..)) => {
            let range = match l0_targets {
                SchemalessL0Targets::Range(lo, hi) => Some((lo, hi)),
                _ => None,
            };
            let shadowed = lance_deduped
                .as_ref()
                .and_then(|b| b.column_by_name("_vid"))
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .map(|a| a.values().to_vec())
                .unwrap_or_default();
            uni_store::runtime::columnar_scan::schemaless_l0_prefilter_candidates(
                l0_ctx,
                label,
                range,
                pf,
                &label_only,
                shadowed,
            )
        }
        _ => None,
    };
    let l0_targets = match (&kept, l0_targets) {
        (Some(vids), SchemalessL0Targets::Range(lo, hi)) => SchemalessL0Targets::Filtered {
            range: Some((lo, hi)),
            vids,
        },
        (Some(vids), _) => SchemalessL0Targets::Filtered { range: None, vids },
        (None, t) => t,
    };
    let l0_batch =
        build_l0_schemaless_vertex_batch(l0_ctx, label, &internal_schema, l0_targets, &label_only)?;

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
/// Which L0 vids a schemaless scan should contribute.
///
/// Mirrors `uni_store::runtime::columnar_scan::L0VertexTargets`, which the
/// labelled path uses; kept local because this builder walks `L0Context`
/// rather than the store's own buffers.
#[derive(Clone, Copy)]
enum SchemalessL0Targets<'a> {
    /// Exactly these vids, from an `id(x) = ?` or `id(x) IN [...]` pushdown.
    Vids(&'a [u64]),
    /// Every matching vid in the half-open range `[lo, hi)` (#214).
    Range(u64, u64),
    /// Every matching vid.
    All,
    /// The vids `All` (or `Range`, when `range` is set) selects that are also
    /// in `vids`: the survivors of an equality prefilter
    /// (`columnar_scan::schemaless_l0_prefilter_candidates`), each built
    /// exactly as the unrestricted walk would build it.
    Filtered {
        range: Option<(u64, u64)>,
        vids: &'a HashSet<u64>,
    },
}

fn build_l0_schemaless_vertex_batch(
    l0_ctx: &crate::query::df_graph::L0Context,
    label: &str,
    internal_schema: &SchemaRef,
    targets: SchemalessL0Targets<'_>,
    label_only: &HashMap<u64, (Properties, u64)>,
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
        let vids: Vec<Vid> = if let SchemalessL0Targets::Vids(tvs) = targets {
            let mut out = Vec::with_capacity(tvs.len());
            for &tv in tvs {
                let vid = Vid::from(tv);
                if !guard.vertex_properties.contains_key(&vid) && !label_only.contains_key(&tv) {
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
        } else if let SchemalessL0Targets::Filtered { range, vids } = targets {
            // Probe the kept set with the unrestricted walk's membership rule.
            vids.iter()
                .filter(|raw| range.is_none_or(|(lo, hi)| **raw >= lo && **raw < hi))
                .map(|raw| Vid::from(*raw))
                .filter(|vid| {
                    if label_filter.is_empty() {
                        guard.vertex_properties.contains_key(vid)
                    } else {
                        label_filter.iter().all(|l| {
                            guard
                                .label_to_vids
                                .get(*l)
                                .is_some_and(|set| set.contains(vid))
                        })
                    }
                })
                .collect()
        } else {
            let mut selected = if label_filter.is_empty() {
                guard.all_vertex_vids()
            } else if label_filter.len() == 1 {
                guard.vids_for_label(label_filter[0])
            } else {
                guard.vids_with_all_labels(&label_filter)
            };
            if let SchemalessL0Targets::Range(lo, hi) = targets {
                selected.retain(|vid| {
                    let v = vid.as_u64();
                    v >= lo && v < hi
                });
            }
            selected
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

    // A relabeled vertex with no L0 property row starts from its flushed
    // properties, at the relabel's version (see `label_only_l0_base_props`).
    for (vid, (base, version)) in label_only {
        if let Some(entry) = vid_data.get_mut(vid) {
            entry.0 = base.clone();
            entry.1 = entry.1.max(*version);
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
                )
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                columns.push(col);
            } else {
                // Decode CypherValue to the expected type via build_property_column_static.
                let mut prop_values: HashMap<Vid, Properties> = HashMap::new();
                for i in 0..batch.num_rows() {
                    let vid = Vid::from(vid_arr.value(i));
                    // A corrupt overflow blob used to make the property simply
                    // absent, which downstream reads as NULL — indistinguishable
                    // from a property this row genuinely lacks (#233 class).
                    let resolved =
                        match resolve_l0_property(&vid, prop, l0_ctx).flatten() {
                            Some(v) => Some(v),
                            None => match extract_from_overflow_blob(props_arr, i, prop).map_err(
                                |e| datafusion::error::DataFusionError::Execution(e.to_string()),
                            )? {
                                Some(bytes) => {
                                    Some(uni_common::cypher_value_codec::decode(&bytes).map_err(
                                        |e| {
                                            datafusion::error::DataFusionError::Execution(format!(
                                                "overflow property `{prop}` failed to decode: {e}"
                                            ))
                                        },
                                    )?)
                                }
                                None => None,
                            },
                        };
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
                    // Chunk only when the vid set is larger than one output
                    // batch. Below that the whole result already fits the bound
                    // chunking exists to impose, and the extra round trips
                    // would be pure cost — the traversal measured ~6% for
                    // chunking unconditionally, which is why it gates too.
                    //
                    // Sorted so chunk k holds only vids below chunk k+1's. The
                    // scan sorts by `(_vid ASC, _version DESC)` internally, so
                    // sorting here makes the concatenation of the chunks
                    // identical to the unchunked result, order included, rather
                    // than merely equal as a set.
                    match self.vid_list_filter.clone() {
                        Some(mut vids) if vids.len() > self.slice_size => {
                            vids.sort_unstable();
                            vids.dedup();
                            self.state = GraphScanState::Chunking {
                                vids: Arc::new(vids),
                                cursor: 0,
                            };
                        }
                        whole => {
                            // A full-label scan and a full-graph `ScanAll` are
                            // both walkable by `_vid` range; a single-vid short
                            // circuit (`self.filter`) returns one row and is not
                            // worth a walk.
                            //
                            // The schemaless arm used to be excluded here
                            // because its scan function carried no range. It
                            // does now, and the exclusion was expensive: with
                            // no chunked state to bound construction, a
                            // `ScanAll` built every vertex as one batch, which
                            // the pool can only charge for after the fact --
                            // 711.8 MB reserved and ~2.2 GB resident at LDBC
                            // SF1 for a bare `MATCH (n)`. Sizing it needs a
                            // count over the shared `vertices` table rather
                            // than a per-label one, which is the only reason a
                            // label-less scan could not answer the question.
                            if whole.is_none() && self.filter.is_none() {
                                let storage = Arc::clone(self.graph_ctx.storage());
                                let label = self.label.clone();
                                let schemaless = self.is_schemaless;
                                // Unflushed rows are read by the same walk, so
                                // they count toward its size. Flushed storage
                                // alone sized an all-L0 label as empty and built
                                // it whole, whether or not a flush had landed.
                                let l0_rows = self
                                    .graph_ctx
                                    .l0_context()
                                    .vertex_count((!schemaless).then_some(label.as_str()));
                                self.state = GraphScanState::Sizing(Box::pin(async move {
                                    let flushed = if schemaless {
                                        storage.main_vertex_row_count().await.map_err(exec_err)?
                                    } else {
                                        storage.vertex_row_count(&label).await.map_err(exec_err)?
                                    };
                                    Ok(flushed.map(|rows| rows.saturating_add(l0_rows)))
                                }));
                            } else {
                                self.state = GraphScanState::Executing {
                                    fut: self.scan_future(whole, None),
                                    resume: ScanResume::Finished,
                                };
                            }
                        }
                    }
                }
                GraphScanState::Sizing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(rows)) => {
                        // `None` = no cheap count available (a forked session,
                        // where counting scans the branch). Reading whole is
                        // the unbounded path, but it is the one this query
                        // already had; paying a full materialization to decide
                        // whether to avoid one is strictly worse.
                        let rows = rows.unwrap_or(0);
                        if rows > self.slice_size {
                            self.state = GraphScanState::RangeChunking {
                                lo: 0,
                                width: self.slice_size as u64,
                            };
                        } else {
                            self.state = GraphScanState::Executing {
                                fut: self.scan_future(None, None),
                                resume: ScanResume::Finished,
                            };
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = GraphScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = GraphScanState::Sizing(fut);
                        return Poll::Pending;
                    }
                },
                GraphScanState::RangeChunking { lo, width } => {
                    let hi = lo.saturating_add(width);
                    if hi == lo {
                        // `lo` is already `u64::MAX`; there is no range left to
                        // ask for and no vid above it.
                        self.reservation.free();
                        self.state = GraphScanState::Done;
                        return Poll::Ready(None);
                    }
                    self.state = GraphScanState::Executing {
                        fut: self.scan_future(None, Some((lo, hi))),
                        resume: ScanResume::Range { lo, width },
                    };
                }
                GraphScanState::ConfirmingEnd { mut fut, lo, width } => match fut.as_mut().poll(cx)
                {
                    Poll::Ready(Ok(SeekOutcome::ContinueHere)) => {
                        // A gap, not the end. Widen so a sparse label costs a
                        // scan per gap rather than a scan per empty range.
                        self.state = GraphScanState::RangeChunking {
                            lo,
                            width: width.saturating_mul(2).min(RANGE_WIDTH_MAX),
                        };
                    }
                    Poll::Ready(Ok(SeekOutcome::ResumeAt(min_vid))) => {
                        // The seek found the label's next populated vid, so the
                        // widening that crossed the gap has done its job and
                        // would otherwise make the *first productive* range
                        // enormous. Reset to one slice's width: under a fetch
                        // bound that is all the limit can consume, and if the
                        // region turns out sparse the ordinary retune widens it
                        // again from here.
                        self.state = GraphScanState::RangeChunking {
                            lo: min_vid.max(lo),
                            width: self.slice_size as u64,
                        };
                    }
                    Poll::Ready(Ok(SeekOutcome::Exhausted)) => {
                        self.reservation.free();
                        self.state = GraphScanState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(e)) => {
                        self.reservation.free();
                        self.state = GraphScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = GraphScanState::ConfirmingEnd { fut, lo, width };
                        return Poll::Pending;
                    }
                },
                GraphScanState::Chunking { vids, cursor } => {
                    if cursor >= vids.len() {
                        self.reservation.free();
                        self.state = GraphScanState::Done;
                        return Poll::Ready(None);
                    }
                    let end = (cursor + self.slice_size).min(vids.len());
                    let chunk = vids[cursor..end].to_vec();
                    self.state = GraphScanState::Executing {
                        fut: self.scan_future(Some(chunk), None),
                        resume: ScanResume::Vids { vids, cursor: end },
                    };
                }
                GraphScanState::Executing { mut fut, resume } => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        match batch {
                            // Hand the result out in `batch_size` slices rather
                            // than as one batch — see `GraphScanState::Slicing`.
                            Some(b) if b.num_rows() > 0 => {
                                // `try_resize`, not `try_grow`: for a chunked
                                // scan this is the *replacement* of the previous
                                // chunk's reservation, which was freed when its
                                // last slice went out. Growing would accumulate
                                // every chunk and report a peak the scan never
                                // holds.
                                //
                                // For a scan that is still unchunked — a
                                // schemaless one, or a forked session where the
                                // row count cannot be had cheaply — the batch is
                                // already built by this point, so the
                                // reservation bounds how long an over-budget
                                // result survives rather than preventing its
                                // construction.
                                if let Err(e) =
                                    self.reservation.try_resize(b.get_array_memory_size())
                                {
                                    self.state = GraphScanState::Done;
                                    return Poll::Ready(Some(Err(e)));
                                }
                                // Advance a range walk before the batch goes
                                // out, retuning the width from what this range
                                // actually held.
                                let resume = match resume {
                                    ScanResume::Range { lo, width } => ScanResume::Range {
                                        lo: lo.saturating_add(width),
                                        width: retune_range_width(
                                            width,
                                            b.get_array_memory_size(),
                                            self.range_target_bytes,
                                        ),
                                    },
                                    other => other,
                                };
                                self.state = GraphScanState::Slicing {
                                    batch: b,
                                    offset: 0,
                                    resume,
                                };
                            }
                            other => match resume {
                                // An empty chunk does not end a chunked scan:
                                // one vid range can hold no live row — every
                                // candidate deleted or superseded — while later
                                // ranges do. Ending here would silently truncate
                                // the result at the first such range.
                                ScanResume::Vids { vids, cursor } => {
                                    self.state = GraphScanState::Chunking { vids, cursor };
                                }
                                // For a range walk emptiness is ambiguous, so
                                // ask storage rather than assume either way.
                                ScanResume::Range { lo, width } => {
                                    let next_lo = lo.saturating_add(width);
                                    let storage = Arc::clone(self.graph_ctx.storage());
                                    let label = self.label.clone();
                                    let schemaless = self.is_schemaless;
                                    // With a fetch bound, ask *where* the label
                                    // resumes rather than merely whether it
                                    // does, and restart the walk there at the
                                    // original narrow width. Without one, the
                                    // cheaper boolean is right: a full scan is
                                    // going to read those rows regardless, so
                                    // the seek would be pure overhead (#239).
                                    // Only when a fetch bounds the read *and*
                                    // the march has widened enough that the
                                    // saved rows outweigh the seek's counts.
                                    let seek = self.fetch.is_some()
                                        && width
                                            >= (self.slice_size as u64)
                                                .saturating_mul(SEEK_MIN_WIDTH_FACTOR);
                                    // Flushed storage cannot see unflushed rows,
                                    // and vids are allocated across labels, so a
                                    // label's newer rows can lie past a gap its
                                    // flushed rows never reach. Asking storage
                                    // alone ended the walk in that gap and
                                    // silently dropped every row above it.
                                    let l0_next =
                                        self.graph_ctx.l0_context().min_vertex_vid_at_or_above(
                                            (!schemaless).then_some(label.as_str()),
                                            next_lo,
                                        );
                                    self.state = GraphScanState::ConfirmingEnd {
                                        fut: Box::pin(async move {
                                            let flushed = if seek {
                                                // A schemaless walk is over the
                                                // shared table, so its gap seek
                                                // has to ask that table too --
                                                // asking a per-label one would
                                                // report the walk exhausted at
                                                // the first gap.
                                                if schemaless {
                                                    storage
                                                        .main_vertex_min_vid_at_or_above(next_lo)
                                                        .await
                                                } else {
                                                    storage
                                                        .vertex_min_vid_at_or_above(&label, next_lo)
                                                        .await
                                                }
                                                .map(|min| min.map(SeekOutcome::ResumeAt))
                                                .map(|o| o.unwrap_or(SeekOutcome::Exhausted))
                                                .map_err(exec_err)
                                            } else {
                                                if schemaless {
                                                    storage
                                                        .main_vertex_rows_at_or_above(next_lo)
                                                        .await
                                                } else {
                                                    storage
                                                        .vertex_rows_at_or_above(&label, next_lo)
                                                        .await
                                                }
                                                .map(|more| {
                                                    if more {
                                                        SeekOutcome::ContinueHere
                                                    } else {
                                                        SeekOutcome::Exhausted
                                                    }
                                                })
                                                .map_err(exec_err)
                                            }?;
                                            Ok(match (flushed, l0_next) {
                                                (SeekOutcome::Exhausted, Some(vid)) => {
                                                    SeekOutcome::ResumeAt(vid)
                                                }
                                                (SeekOutcome::ResumeAt(a), Some(b)) => {
                                                    SeekOutcome::ResumeAt(a.min(b))
                                                }
                                                (outcome, _) => outcome,
                                            })
                                        }),
                                        lo: next_lo,
                                        width,
                                    };
                                }
                                ScanResume::Finished => {
                                    self.reservation.free();
                                    self.state = GraphScanState::Done;
                                    return Poll::Ready(other.map(Ok));
                                }
                            },
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.reservation.free();
                        self.state = GraphScanState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = GraphScanState::Executing { fut, resume };
                        return Poll::Pending;
                    }
                },
                GraphScanState::Slicing {
                    batch,
                    offset,
                    resume,
                } => {
                    let remaining = batch.num_rows() - offset;
                    if remaining == 0 {
                        // Release this chunk before the next is built, so the
                        // peak is one chunk and not their sum. A consumer may
                        // still hold the last slice, which pins these buffers —
                        // the same brief under-count the traversal accepts at
                        // its own chunk boundary.
                        self.reservation.free();
                        match resume {
                            ScanResume::Vids { vids, cursor } => {
                                self.state = GraphScanState::Chunking { vids, cursor };
                            }
                            ScanResume::Range { lo, width } => {
                                self.state = GraphScanState::RangeChunking { lo, width };
                            }
                            ScanResume::Finished => {
                                self.state = GraphScanState::Done;
                                return Poll::Ready(None);
                            }
                        }
                        continue;
                    }
                    let take = self.slice_size.min(remaining);
                    let slice = batch.slice(offset, take);
                    self.state = GraphScanState::Slicing {
                        batch,
                        offset: offset + take,
                        resume,
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

    /// An unknown table size never licenses reading the whole table.
    ///
    /// Zero is what a fork, a pinned view, or an uncounted table yields, and it
    /// is the one input where guessing wrong is unbounded: the scan arm would
    /// read every row of a table whose size is precisely what is not known.
    #[test]
    fn an_unknown_table_size_keeps_the_chunked_arm() {
        assert!(!vertex_scan_beats_lookup(1_000_000, 0));
        assert!(!vertex_scan_beats_lookup(0, 0));
    }

    /// The threshold is 25% of the table, and it is a threshold on the
    /// *requested* count rather than on the table's size alone.
    #[test]
    fn the_switch_happens_at_a_quarter_of_the_table() {
        // 300k-row table: the measured crossover is near K=800, and the switch
        // deliberately waits until 75 000 — see `vertex_scan_beats_lookup` for
        // why the time-optimal point is the wrong threshold.
        assert!(!vertex_scan_beats_lookup(800, 300_000));
        assert!(!vertex_scan_beats_lookup(74_999, 300_000));
        assert!(vertex_scan_beats_lookup(75_000, 300_000));
        assert!(vertex_scan_beats_lookup(300_000, 300_000));
    }

    /// A huge request cannot overflow its way into the wrong arm.
    #[test]
    fn a_saturating_request_does_not_wrap() {
        assert!(vertex_scan_beats_lookup(usize::MAX, 10));
    }

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
            "Person",
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
        let schema = GraphScanExec::build_schemaless_vertex_schema("n", "", &[], &empty_schema);

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
