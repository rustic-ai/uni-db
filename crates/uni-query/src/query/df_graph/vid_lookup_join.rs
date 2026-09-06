// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cross-MATCH dynamic VID-filter pushdown (issue #55 PR #5+#6).
//!
//! Specializes equi-joins where one of the equi-pairs is on the probe side's
//! `_vid` column. The probe is a `GraphScanExec`; the build is any plan whose
//! output we can materialize. At execute time:
//!
//!   1. Run the build side fully and collect its rows.
//!   2. Extract distinct VIDs from the build side's anchor-pair column.
//!   3. Push them as `_vid IN (...)` to the probe scan via
//!      `GraphScanExec::execute_with_vid_filter`. If the build VID set
//!      exceeds `MAX_VIDS_PER_CHUNK` we chunk into multiple `_vid IN`
//!      filters and concat the batches — bounded list size, indexed lookup
//!      preserved at scale.
//!   4. Index probe by `_vid` and join in memory. Non-anchor equi-pairs
//!      become per-candidate post-filters.
//!
//! Output column order is `left.schema() ++ right.schema()` in plan order
//! (matches `HashJoinExec`'s convention) regardless of which side is the
//! probe — important because downstream operators reference columns by
//! index.
//!
//! ## When the planner emits this
//!
//! See `try_emit_vid_lookup_join` in `df_planner.rs`. Conditions:
//! - The join is INNER or LEFT outer (RIGHT outer falls back to
//!   `HashJoinExec` — we'd need NULL-padding for the build's *complement*,
//!   which our materialize-then-probe shape can't produce).
//! - At least one equi-pair has the probe side equal to
//!   `Property(Variable(scan_var), "_vid")`. That pair becomes the
//!   "anchor" — its values drive the IN-list pushdown.
//! - The probe-side planned subtree is a top-level `GraphScanExec`.
//! - All non-anchor equi-pairs compile to `Column` references on both
//!   sides (no computed expressions).
//! - The anchor build-side column is `UInt64` (a VID).
//!
//! Any failed condition → planner emits `HashJoinExec` instead.
//!
//! ## Out of scope
//!
//! - RIGHT outer joins (preserving probe-side rows that don't match).
//!   Rejected at the planner.
//! - Computed build-side expressions in any equi-pair. Rejected at the
//!   planner; falls back to `HashJoinExec`.
//! - Anchor-pair build column types other than `UInt64`. Rejected at the
//!   planner.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::builder::UInt32Builder;
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

use super::common::compute_plan_properties;
use super::scan::GraphScanExec;

/// Maximum VIDs per `_vid IN (...)` chunk. Larger build sets are split into
/// multiple sequential probe scans whose results are concatenated. Mirrors
/// the equivalent cap in `df_planner.rs`.
pub(crate) const MAX_VIDS_PER_CHUNK: usize = 10_000;

/// Which side of the join is the probe (the `GraphScanExec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    Left,
    Right,
}

/// One equi-join pair. Indices are into the respective side's schema.
/// `pairs[0]` is always the anchor — its probe side is the `_vid` column
/// whose values drive the IN-list pushdown.
#[derive(Debug, Clone, Copy)]
pub struct EquiPair {
    pub left_col_idx: usize,
    pub right_col_idx: usize,
}

impl EquiPair {
    /// Column index on the build side, given which side is the probe.
    fn build_col(&self, probe_side: ProbeSide) -> usize {
        match probe_side {
            ProbeSide::Left => self.right_col_idx,
            ProbeSide::Right => self.left_col_idx,
        }
    }

    /// Column index on the probe side, given which side is the probe.
    fn probe_col(&self, probe_side: ProbeSide) -> usize {
        match probe_side {
            ProbeSide::Left => self.left_col_idx,
            ProbeSide::Right => self.right_col_idx,
        }
    }
}

/// Join semantic. RIGHT outer is rejected at the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VidJoinKind {
    Inner,
    Left,
}

/// Cross-MATCH dynamic VID-filter pushdown operator.
pub struct VidLookupJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    probe_side: ProbeSide,
    /// Equi-pairs. Index 0 is the anchor (probe side is `_vid`); the rest
    /// are post-match filters during the in-memory hash join.
    pairs: Vec<EquiPair>,
    join_kind: VidJoinKind,
    /// Output schema = `left.schema() ++ right.schema()` in plan order.
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for VidLookupJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VidLookupJoinExec")
            .field("probe_side", &self.probe_side)
            .field("pairs", &self.pairs.len())
            .field("join_kind", &self.join_kind)
            .finish()
    }
}

impl VidLookupJoinExec {
    /// Construct a new VID-lookup-join.
    ///
    /// The output schema is `left.schema()` concatenated with
    /// `right.schema()`. The caller (the planner pre-check) must ensure:
    /// - `pairs[0]`'s probe side is the `_vid` column (UInt64).
    /// - `pairs[0]`'s build side is a UInt64 column.
    /// - The `probe_side`'s child is a `GraphScanExec`.
    /// - `pairs` is non-empty.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        probe_side: ProbeSide,
        pairs: Vec<EquiPair>,
        join_kind: VidJoinKind,
    ) -> DFResult<Self> {
        if pairs.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: pairs must be non-empty".into(),
            ));
        }
        let probe_plan = match probe_side {
            ProbeSide::Left => &left,
            ProbeSide::Right => &right,
        };
        if probe_plan
            .as_any()
            .downcast_ref::<GraphScanExec>()
            .is_none()
        {
            return Err(datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: probe-side child must be a GraphScanExec".into(),
            ));
        }
        // For a LEFT outer join this operator null-pads unmatched BUILD rows,
        // so it is the PROBE side's columns that receive NULLs and must be
        // nullable.
        let widen = match join_kind {
            VidJoinKind::Left => Some(probe_side),
            VidJoinKind::Inner => None,
        };
        let output_schema = concat_schemas(&left.schema(), &right.schema(), widen);
        let properties = compute_plan_properties(output_schema.clone());
        Ok(Self {
            left,
            right,
            probe_side,
            pairs,
            join_kind,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn build_child(&self) -> &Arc<dyn ExecutionPlan> {
        match self.probe_side {
            ProbeSide::Left => &self.right,
            ProbeSide::Right => &self.left,
        }
    }

    fn probe_child(&self) -> &Arc<dyn ExecutionPlan> {
        match self.probe_side {
            ProbeSide::Left => &self.left,
            ProbeSide::Right => &self.right,
        }
    }
}

impl DisplayAs for VidLookupJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VidLookupJoinExec: probe={:?}, pairs={}, kind={:?}",
            self.probe_side,
            self.pairs.len(),
            self.join_kind
        )
    }
}

impl ExecutionPlan for VidLookupJoinExec {
    fn name(&self) -> &str {
        "VidLookupJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Expose the build child only — DataFusion's plan walker will see
        // exactly the side that we'll execute through its standard
        // `execute()` API. The probe is driven via the GraphScanExec
        // helper at runtime and isn't a child in the traditional sense.
        vec![self.build_child()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "VidLookupJoinExec expects exactly one child (the build side); got {}",
                children.len()
            )));
        }
        let new_build = children.into_iter().next().unwrap();
        let (new_left, new_right) = match self.probe_side {
            ProbeSide::Left => (self.left.clone(), new_build),
            ProbeSide::Right => (new_build, self.right.clone()),
        };
        Ok(Arc::new(Self::try_new(
            new_left,
            new_right,
            self.probe_side,
            self.pairs.clone(),
            self.join_kind,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let build = self.build_child().clone();
        let probe = self.probe_child().clone();
        let probe_side = self.probe_side;
        let pairs = self.pairs.clone();
        let join_kind = self.join_kind;
        let output_schema = self.output_schema.clone();
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();

        let fut = async move {
            run_join(
                build,
                probe,
                probe_side,
                pairs,
                join_kind,
                left_schema,
                right_schema,
                output_schema.clone(),
                partition,
                context,
            )
            .await
        };

        Ok(Box::pin(VidLookupJoinStream {
            state: VidLookupJoinStreamState::Running(Box::pin(fut)),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Stream state machine
// ---------------------------------------------------------------------------

enum VidLookupJoinStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    Done,
}

struct VidLookupJoinStream {
    state: VidLookupJoinStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for VidLookupJoinStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        match &mut self.state {
            VidLookupJoinStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = VidLookupJoinStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = VidLookupJoinStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            VidLookupJoinStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for VidLookupJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// Core join logic
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_join(
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    probe_side: ProbeSide,
    pairs: Vec<EquiPair>,
    join_kind: VidJoinKind,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    partition: usize,
    context: Arc<TaskContext>,
) -> DFResult<RecordBatch> {
    // Everything this operator materializes is reserved from the query's memory
    // pool. It replaced a `HashJoinExec`, which is pool-accounted and spillable,
    // with one that is neither, so the plan-shape choice that installed it also
    // removed the join path's accounting (#242). Nothing first-party reserved at
    // all, which is why multi-GB peaks here never tripped a pool that was
    // configured and working the whole time.
    //
    // The reservation covers the intermediates for as long as this function
    // holds them and is released when it returns: the output batch belongs to
    // the consumer from that point, and holding a reservation against memory
    // this operator no longer owns would double-count it downstream.
    //
    // `try_grow` failing surfaces as `ResourcesExhausted`, which is the point --
    // a refused reservation is a query that stops, rather than a process that
    // grows until the OS ends it.
    let reservation = MemoryConsumer::new(format!("VidLookupJoinExec[{partition}]"))
        .register(context.memory_pool());

    // 1. Materialize the build side fully.
    //
    // Accumulated batch by batch rather than through `try_collect`, so the
    // budget is checked as the build side grows instead of after all of it is
    // already resident.
    let mut build_stream = build.execute(partition, Arc::clone(&context))?;
    let mut build_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = build_stream.next().await {
        let batch = batch?;
        reservation.try_grow(batch.get_array_memory_size())?;
        build_batches.push(batch);
    }

    if build_batches.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema));
    }

    // 2. Extract distinct VIDs from the anchor build column.
    let anchor = pairs[0];
    let build_anchor_col_idx = anchor.build_col(probe_side);
    let mut vid_set: HashSet<u64> = HashSet::new();
    for batch in &build_batches {
        let keys = AnchorKeys::new(batch.column(build_anchor_col_idx))?;
        for i in 0..keys.len() {
            if let Some(v) = keys.key(i) {
                vid_set.insert(v);
            }
        }
    }

    // 3. Execute the probe scan with chunked IN-list filters and concat
    // the batches. With cap-busting build sizes we chunk into
    // MAX_VIDS_PER_CHUNK pieces; total Lance work scales the same as a
    // single big scan, but no chunk's IN-list exceeds the safe bound.
    let probe_scan = probe
        .as_any()
        .downcast_ref::<GraphScanExec>()
        .expect("planner ensured probe is GraphScanExec");
    let probe_batch = if vid_set.is_empty() {
        // No build VIDs → no probe rows to fetch. Still need an empty
        // batch with the correct schema for downstream NULL-padding logic.
        RecordBatch::new_empty(probe_scan.schema())
    } else {
        let vids: Vec<u64> = vid_set.iter().copied().collect();
        let mut chunks: Vec<RecordBatch> = Vec::new();
        let mut chunk_bytes = 0usize;
        for chunk in vids.chunks(MAX_VIDS_PER_CHUNK) {
            let batch = probe_scan.execute_with_vid_filter(chunk).await?;
            if batch.num_rows() > 0 {
                let size = batch.get_array_memory_size();
                reservation.try_grow(size)?;
                chunk_bytes += size;
                chunks.push(batch);
            }
        }
        if chunks.is_empty() {
            RecordBatch::new_empty(probe_scan.schema())
        } else if chunks.len() == 1 {
            chunks.into_iter().next().unwrap()
        } else {
            let schema = chunks[0].schema();
            // `concat_batches` builds the combined batch while every chunk is
            // still live, so the true peak here is both at once. Reserving only
            // the result would under-report exactly at the moment this operator
            // uses the most memory.
            reservation.try_grow(chunk_bytes)?;
            let combined = arrow::compute::concat_batches(&schema, &chunks)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            drop(chunks);
            reservation.shrink(chunk_bytes);
            combined
        }
    };

    // 4. Index probe by `_vid`. The probe scan's schema always carries a
    // `_vid` column at a known position relative to its projected
    // properties; we resolve by name so a future schema-shape change
    // doesn't silently break us.
    let probe_vid_idx = locate_vid_column(&probe_batch.schema())?;
    let probe_anchor_col_idx = anchor.probe_col(probe_side);
    // Sanity: anchor's probe column SHOULD be the `_vid` column. If the
    // planner classified differently, fail loudly.
    if probe_anchor_col_idx != probe_vid_idx {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "VidLookupJoinExec: anchor probe column idx {} != probe schema's _vid idx {} \
             (planner pre-check should have aligned these)",
            probe_anchor_col_idx, probe_vid_idx
        )));
    }
    let probe_index = build_probe_vid_index(&probe_batch, probe_vid_idx)?;

    // 5. Walk build rows; for each, find probe candidates by anchor VID
    // and post-filter by non-anchor pairs. Record matching pairs as
    // (build_batch_idx, build_row_idx, probe_row_idx) for batched take(...)
    // at the end. For LEFT-outer, also note any build row with zero
    // matches so we can emit NULL-padded.
    let n_non_anchor = pairs.len() - 1;
    let mut matches: Vec<JoinMatch> = Vec::new();
    let mut unmatched: Vec<(usize, usize)> = Vec::new(); // (build_batch_idx, build_row_idx)

    for (build_batch_idx, build_batch) in build_batches.iter().enumerate() {
        let build_anchor_keys = AnchorKeys::new(build_batch.column(build_anchor_col_idx))?;
        for build_row_idx in 0..build_anchor_keys.len() {
            if build_anchor_keys.is_null(build_row_idx) {
                if join_kind == VidJoinKind::Left {
                    unmatched.push((build_batch_idx, build_row_idx));
                }
                continue;
            }
            // Out of `u64` range cannot name a vid, so it matches nothing.
            let Some(key) = build_anchor_keys.key(build_row_idx) else {
                if join_kind == VidJoinKind::Left {
                    unmatched.push((build_batch_idx, build_row_idx));
                }
                continue;
            };
            let Some(probe_rows) = probe_index.get(&key) else {
                if join_kind == VidJoinKind::Left {
                    unmatched.push((build_batch_idx, build_row_idx));
                }
                continue;
            };

            // For each candidate probe row, check the non-anchor pairs.
            let mut had_match_for_this_build_row = false;
            for &probe_row_idx in probe_rows {
                let mut all_match = true;
                for pair in &pairs[1..1 + n_non_anchor] {
                    let build_col_idx = pair.build_col(probe_side);
                    let probe_col_idx = pair.probe_col(probe_side);
                    if !values_equal(
                        build_batch.column(build_col_idx),
                        build_row_idx,
                        probe_batch.column(probe_col_idx),
                        probe_row_idx,
                    )? {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    matches.push(JoinMatch {
                        build_batch_idx,
                        build_row_idx,
                        probe_row_idx,
                    });
                    had_match_for_this_build_row = true;
                }
            }
            if !had_match_for_this_build_row && join_kind == VidJoinKind::Left {
                unmatched.push((build_batch_idx, build_row_idx));
            }
        }
    }

    // 6. Emit one combined RecordBatch in left-then-right plan order.
    emit_joined_batch(
        &build_batches,
        &probe_batch,
        &matches,
        &unmatched,
        probe_side,
        &left_schema,
        &right_schema,
        &output_schema,
    )
}

// ---------------------------------------------------------------------------
// Hash-join helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct JoinMatch {
    build_batch_idx: usize,
    build_row_idx: usize,
    probe_row_idx: usize,
}

fn build_probe_vid_index(
    probe_batch: &RecordBatch,
    probe_vid_idx: usize,
) -> DFResult<HashMap<u64, Vec<usize>>> {
    let arr = probe_batch
        .column(probe_vid_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: probe `_vid` column is not UInt64".into(),
            )
        })?;
    let mut index: HashMap<u64, Vec<usize>> = HashMap::with_capacity(arr.len());
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            index.entry(arr.value(i)).or_default().push(i);
        }
    }
    Ok(index)
}

/// Compare two cells for equality. Used by the non-anchor equi-pair filter.
/// Uses `ScalarValue` for type-erased comparison so the operator works for
/// any column type Arrow can lift into a `ScalarValue` (which covers all
/// types we currently materialize from Lance).
fn values_equal(a_col: &ArrayRef, a_row: usize, b_col: &ArrayRef, b_row: usize) -> DFResult<bool> {
    // Cypher comparison against NULL yields null (unknown), which for a join
    // equi-pair means "not a match" — a null on either side must never join.
    // `ScalarValue`'s `PartialEq` instead treats NULL == NULL as true, so guard
    // the null case explicitly before the type-erased comparison.
    if a_col.is_null(a_row) || b_col.is_null(b_row) {
        return Ok(false);
    }
    let a = ScalarValue::try_from_array(a_col, a_row)?;
    let b = ScalarValue::try_from_array(b_col, b_row)?;
    Ok(a == b)
}

/// Find the `_vid` column in a probe batch's schema.
fn locate_vid_column(schema: &SchemaRef) -> DFResult<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .find_map(|(i, f)| {
            if f.name() == "_vid" || f.name().ends_with("._vid") {
                Some(i)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: probe schema has no _vid column".into(),
            )
        })
}

/// Concatenate two schemas in plan order. Field names kept as-is; Cypher
/// variable-naming rules guarantee uniqueness across the two sides.
///
/// `widen` names the side that gets NULL-extended by an outer join, whose
/// fields must therefore be marked nullable. This mirrors DataFusion's own
/// `build_join_schema`, which widens the outer side for exactly this reason.
///
/// Copying fields verbatim was a live defect: `GraphScanExec` declares
/// `{var}._vid` as `Field::new(.., UInt64, false)`, so a LEFT-outer NULL pad
/// wrote nulls into a non-nullable column and `RecordBatch::try_new` rejected
/// the whole batch. Every unmatched OPTIONAL row would have failed the query.
fn concat_schemas(left: &SchemaRef, right: &SchemaRef, widen: Option<ProbeSide>) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::with_capacity(left.fields().len() + right.fields().len());
    let widen_left = widen == Some(ProbeSide::Left);
    let widen_right = widen == Some(ProbeSide::Right);
    for f in left.fields() {
        fields.push(if widen_left {
            f.as_ref().clone().with_nullable(true)
        } else {
            f.as_ref().clone()
        });
    }
    for f in right.fields() {
        fields.push(if widen_right {
            f.as_ref().clone().with_nullable(true)
        } else {
            f.as_ref().clone()
        });
    }
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------
// Output batch construction
// ---------------------------------------------------------------------------

/// Reads the build-side anchor column as vids, whatever integer type it has.
///
/// A vid is a `u64`, but a vid stored in a Cypher property arrives as `Int64`
/// (Cypher has no unsigned integer). Both are accepted; an `Int64` outside
/// `u64` range cannot name any vid, so it yields `None` and the row is treated
/// as unmatched.
///
/// This exists as one accessor because the previous shape — two independent
/// `downcast_ref::<UInt64Array>()` calls, the second carrying
/// `.expect("validated above")` where "above" was a *different loop* — meant a
/// change to one silently broke the other, as a panic inside a stream future.
struct AnchorKeys<'a> {
    u64_arr: Option<&'a UInt64Array>,
    i64_arr: Option<&'a arrow::array::Int64Array>,
    raw: &'a ArrayRef,
}

impl<'a> AnchorKeys<'a> {
    fn new(raw: &'a ArrayRef) -> DFResult<Self> {
        let u64_arr = raw.as_any().downcast_ref::<UInt64Array>();
        let i64_arr = raw.as_any().downcast_ref::<arrow::array::Int64Array>();
        if u64_arr.is_none() && i64_arr.is_none() {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "VidLookupJoinExec: build anchor column is not UInt64/Int64 (got {:?})",
                raw.data_type()
            )));
        }
        Ok(Self {
            u64_arr,
            i64_arr,
            raw,
        })
    }

    fn len(&self) -> usize {
        self.raw.len()
    }

    fn is_null(&self, i: usize) -> bool {
        self.raw.is_null(i)
    }

    /// The vid at `i`, or `None` if null or outside `u64` range.
    fn key(&self, i: usize) -> Option<u64> {
        if self.raw.is_null(i) {
            return None;
        }
        if let Some(a) = self.u64_arr {
            return Some(a.value(i));
        }
        self.i64_arr.and_then(|a| u64::try_from(a.value(i)).ok())
    }
}

/// Build the output RecordBatch from inner-match indices and (for LEFT
/// outer) NULL-padded unmatched build rows. Output column order is
/// `left_schema ++ right_schema` regardless of which side is the probe.
#[allow(clippy::too_many_arguments)]
fn emit_joined_batch(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    matches: &[JoinMatch],
    unmatched: &[(usize, usize)],
    probe_side: ProbeSide,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let total_rows = matches.len() + unmatched.len();
    if total_rows == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Group match build rows by their batch index for a single take(...) per
    // build batch.
    let n_build_batches = build_batches.len();
    let mut match_take_per_build_batch: Vec<Vec<u32>> =
        (0..n_build_batches).map(|_| Vec::new()).collect();
    let mut match_probe_take: Vec<u32> = Vec::with_capacity(matches.len());
    for m in matches {
        match_take_per_build_batch[m.build_batch_idx].push(m.build_row_idx as u32);
        match_probe_take.push(m.probe_row_idx as u32);
    }

    // Same for unmatched build rows (LEFT outer only).
    let mut unmatched_take_per_build_batch: Vec<Vec<u32>> =
        (0..n_build_batches).map(|_| Vec::new()).collect();
    for &(bb_idx, br_idx) in unmatched {
        unmatched_take_per_build_batch[bb_idx].push(br_idx as u32);
    }

    // Build "build side" output columns: take match rows and unmatched
    // rows from each build batch, then concat across batches.
    let n_build_cols = build_batches[0].num_columns();
    let mut build_columns: Vec<ArrayRef> = Vec::with_capacity(n_build_cols);
    // ALL match rows first (across every build batch), THEN all unmatched rows.
    //
    // The order is load-bearing and was previously wrong. Interleaving per
    // batch — `[m(b0), u(b0), m(b1), u(b1), …]` — does not line up with the
    // probe side below, which is built as `[all matches…, all NULLs…]`. With a
    // single build batch the two orders coincide, which is why this survived:
    // it only diverges with >=2 build batches AND an unmatched row in a
    // non-final batch, and then it pairs the wrong build row with the wrong
    // probe values — silently, with no error.
    //
    // Matches are accumulated in build-batch order by the caller's loop, so
    // grouping them per batch here preserves the same order as
    // `match_probe_take`.
    for col_idx in 0..n_build_cols {
        let mut chunks: Vec<ArrayRef> = Vec::new();
        for batch_idx in 0..n_build_batches {
            if !match_take_per_build_batch[batch_idx].is_empty() {
                chunks.push(take_indices(
                    build_batches[batch_idx].column(col_idx),
                    &match_take_per_build_batch[batch_idx],
                )?);
            }
        }
        for batch_idx in 0..n_build_batches {
            if !unmatched_take_per_build_batch[batch_idx].is_empty() {
                chunks.push(take_indices(
                    build_batches[batch_idx].column(col_idx),
                    &unmatched_take_per_build_batch[batch_idx],
                )?);
            }
        }
        build_columns.push(concat_arrays(&chunks)?);
    }

    // Build "probe side" output columns: take match rows from probe batch,
    // then NULL-pad for unmatched.
    let n_probe_cols = probe_batch.num_columns();
    let mut probe_columns: Vec<ArrayRef> = Vec::with_capacity(n_probe_cols);
    let probe_match_arr = take_indices_u32_slice(&match_probe_take);
    let n_unmatched = unmatched.len();
    for col_idx in 0..n_probe_cols {
        let probe_col = probe_batch.column(col_idx);
        let matched_part = if match_probe_take.is_empty() {
            arrow_array::new_empty_array(probe_col.data_type())
        } else {
            arrow::compute::take(probe_col.as_ref(), &probe_match_arr, None)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?
        };
        if n_unmatched == 0 {
            probe_columns.push(matched_part);
        } else {
            let null_part = arrow_array::new_null_array(probe_col.data_type(), n_unmatched);
            probe_columns.push(concat_arrays(&[matched_part, null_part])?);
        }
    }

    // Compose left/right output columns based on which side is build.
    let (left_columns, right_columns) = match probe_side {
        ProbeSide::Left => (probe_columns, build_columns),
        ProbeSide::Right => (build_columns, probe_columns),
    };

    let _ = (left_schema, right_schema); // kept in signature for symmetry / debugging

    let mut all_columns = left_columns;
    all_columns.extend(right_columns);

    RecordBatch::try_new(output_schema.clone(), all_columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

fn take_indices(col: &ArrayRef, indices: &[u32]) -> DFResult<ArrayRef> {
    let take_array = take_indices_u32_slice(indices);
    arrow::compute::take(col.as_ref(), &take_array, None)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

fn take_indices_u32_slice(indices: &[u32]) -> arrow_array::UInt32Array {
    let mut b = UInt32Builder::with_capacity(indices.len());
    for &i in indices {
        b.append_value(i);
    }
    b.finish()
}

fn concat_arrays(arrays: &[ArrayRef]) -> DFResult<ArrayRef> {
    if arrays.len() == 1 {
        return Ok(arrays[0].clone());
    }
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    arrow::compute::concat(&refs)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    //! Unit tests for the output-construction path.
    //!
    //! # Why these target `emit_joined_batch` and not `execute()`
    //!
    //! `run_join` does
    //! `probe.as_any().downcast_ref::<GraphScanExec>().expect(..)`, so handing
    //! `execute()` a synthetic probe panics — driving it needs a live
    //! `GraphContext` (storage, schema, L0), i.e. an integration test that
    //! re-imposes every planner guard. Both defects examined here live entirely
    //! inside `emit_joined_batch`, so testing it directly loses nothing and is
    //! the only way to reach them at all.
    //!
    //! Reachability, measured 2026-08-14 by
    //! `bugs::vid_lookup_join_reachability`: the INNER path **does** fire from
    //! Cypher (`MATCH (a:A) MATCH (b:B) WHERE id(a) = id(b)`, which over
    //! multi-label nodes means "nodes that are both A and B") and agrees with
    //! its `HashJoinExec` fallback. The LEFT path does **not** — the anchor loop
    //! matches the left expression first, giving `(Left, ProbeSide::Left)`,
    //! which the planner rejects. `unmatched` is non-empty only for LEFT, so
    //! everything below is the *unreachable* half of this operator.

    use super::*;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::DataType;

    fn u64_col(v: &[u64]) -> ArrayRef {
        Arc::new(UInt64Array::from(v.to_vec()))
    }
    fn i64_col(v: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(v.to_vec()))
    }

    /// Build side: `[("a.k", UInt64, false), ("a.x", Int64, true)]`.
    fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a.k", DataType::UInt64, false),
            Field::new("a.x", DataType::Int64, true),
        ]))
    }

    /// Probe side. `vid_nullable = false` mirrors production exactly:
    /// `GraphScanExec` declares `Field::new("{var}._vid", UInt64, false)`
    /// (`scan.rs:378,402`).
    fn probe_schema(vid_nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("b._vid", DataType::UInt64, vid_nullable),
            Field::new("b.y", DataType::Int64, true),
        ]))
    }

    /// **B1, fixed.** LEFT-outer NULL padding must succeed.
    ///
    /// `GraphScanExec` declares `{var}._vid` non-nullable (`scan.rs:378,402`),
    /// so writing NULLs into it made `RecordBatch::try_new` reject the whole
    /// batch — every unmatched OPTIONAL row would have failed the query.
    /// `concat_schemas` now widens the null-extended side, as DataFusion's own
    /// `build_join_schema` does for outer joins.
    #[test]
    fn left_outer_null_pad_succeeds_with_widened_schema() {
        let bs = build_schema();
        let ps = probe_schema(false);
        let out = concat_schemas(&bs, &ps, Some(ProbeSide::Right));

        // The root cause, asserted independently of arrow's error text so this
        // stays meaningful across arrow upgrades.
        // The scan declares it non-nullable; the OUTPUT schema must not.
        assert!(
            !ps.field(0).is_nullable(),
            "precondition: the probe scan's _vid is non-nullable, as in production"
        );
        assert!(
            out.field(2).is_nullable(),
            "the null-extended side must be widened in the output schema"
        );

        let build = RecordBatch::try_new(bs.clone(), vec![u64_col(&[10, 99]), i64_col(&[1, 2])])
            .expect("build batch");
        let probe = RecordBatch::try_new(ps.clone(), vec![u64_col(&[10]), i64_col(&[100])])
            .expect("probe batch");

        let matches = vec![JoinMatch {
            build_batch_idx: 0,
            build_row_idx: 0,
            probe_row_idx: 0,
        }];
        let unmatched = vec![(0usize, 1usize)];

        let batch = emit_joined_batch(
            std::slice::from_ref(&build),
            &probe,
            &matches,
            &unmatched,
            ProbeSide::Right,
            &bs,
            &ps,
            &out,
        )
        .expect(
            "LEFT-outer NULL padding must succeed: `concat_schemas` widens the \
             null-extended side, so `b._vid` is nullable in the OUTPUT schema \
             even though `GraphScanExec` declares it non-nullable on the scan.",
        );

        // The unmatched build row is NULL-padded on the probe side.
        let vids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(!vids.is_null(0), "matched row keeps its probe vid");
        assert!(vids.is_null(1), "unmatched build row must be NULL-padded");
    }

    /// **B2.** With ≥2 build batches and an unmatched row in a *non-final*
    /// batch, build and probe columns are assembled in different orders and the
    /// output pairs the wrong rows together — silently, with no error.
    ///
    /// Build columns go `[matches(b0), unmatched(b0), matches(b1), …]` while
    /// probe columns go `[all matches…, all NULLs…]`. With one build batch the
    /// two orders coincide, which is why any hand-written smoke test misses it.
    ///
    /// **B2, fixed.** Emitting all matches before all unmatched rows makes the
    /// two sides agree by construction rather than by coincidence.
    #[test]
    fn multi_build_batch_left_outer_aligns_rows() {
        let bs = build_schema();
        let ps = probe_schema(true);
        let out = concat_schemas(&bs, &ps, Some(ProbeSide::Right));

        // batch 0: key 10 matches, key 99 does not. batch 1: key 20 matches.
        let b0 = RecordBatch::try_new(bs.clone(), vec![u64_col(&[10, 99]), i64_col(&[1, 2])])
            .expect("build batch 0");
        let b1 = RecordBatch::try_new(bs.clone(), vec![u64_col(&[20]), i64_col(&[3])])
            .expect("build batch 1");
        let probe =
            RecordBatch::try_new(ps.clone(), vec![u64_col(&[10, 20]), i64_col(&[100, 200])])
                .expect("probe batch");

        let matches = vec![
            JoinMatch {
                build_batch_idx: 0,
                build_row_idx: 0,
                probe_row_idx: 0,
            },
            JoinMatch {
                build_batch_idx: 1,
                build_row_idx: 0,
                probe_row_idx: 1,
            },
        ];
        let unmatched = vec![(0usize, 1usize)];

        let batch = emit_joined_batch(
            &[b0, b1],
            &probe,
            &matches,
            &unmatched,
            ProbeSide::Right,
            &bs,
            &ps,
            &out,
        )
        .expect("emit");

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ys = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let got: Vec<(u64, Option<i64>)> = (0..batch.num_rows())
            .map(|i| {
                (
                    keys.value(i),
                    if ys.is_null(i) {
                        None
                    } else {
                        Some(ys.value(i))
                    },
                )
            })
            .collect();

        // The CORRECT answer is [(10, Some(100)), (99, None), (20, Some(200))].
        // Asserting the wrong answer documents the live behaviour and makes this
        // test invert cleanly the moment anyone fixes the ordering.
        assert_eq!(
            got,
            vec![(10, Some(100)), (20, Some(200)), (99, None)],
            "build and probe columns must align: every matched build row keeps \
             its own probe values, and only the unmatched row is NULL-padded. \
             Before the fix this produced (99, Some(200)) and (20, None) — the \
             unmatched row stole a matched row's payload, silently."
        );
    }

    /// Control: one build batch, same data — the two assembly orders coincide,
    /// so the result is correct. Pins that B2's trigger is specifically ≥2
    /// build batches.
    #[test]
    fn single_build_batch_left_outer_is_correct() {
        let bs = build_schema();
        let ps = probe_schema(true);
        let out = concat_schemas(&bs, &ps, Some(ProbeSide::Right));

        let build = RecordBatch::try_new(bs.clone(), vec![u64_col(&[10, 99]), i64_col(&[1, 2])])
            .expect("build");
        let probe =
            RecordBatch::try_new(ps.clone(), vec![u64_col(&[10]), i64_col(&[100])]).expect("probe");

        let matches = vec![JoinMatch {
            build_batch_idx: 0,
            build_row_idx: 0,
            probe_row_idx: 0,
        }];
        let unmatched = vec![(0usize, 1usize)];

        let batch = emit_joined_batch(
            std::slice::from_ref(&build),
            &probe,
            &matches,
            &unmatched,
            ProbeSide::Right,
            &bs,
            &ps,
            &out,
        )
        .expect("emit");

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ys = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.value(0), 10);
        assert_eq!(ys.value(0), 100);
        assert_eq!(keys.value(1), 99);
        assert!(ys.is_null(1), "the unmatched build row must be NULL-padded");
    }

    /// Control: INNER (no unmatched rows) over two build batches is correct —
    /// with nothing to NULL-pad, the two orders cannot diverge. This is why the
    /// reachable half of the operator is unaffected by B2.
    #[test]
    fn inner_join_multi_batch_is_correct() {
        let bs = build_schema();
        let ps = probe_schema(true);
        let out = concat_schemas(&bs, &ps, Some(ProbeSide::Right));

        let b0 = RecordBatch::try_new(bs.clone(), vec![u64_col(&[10]), i64_col(&[1])]).unwrap();
        let b1 = RecordBatch::try_new(bs.clone(), vec![u64_col(&[20]), i64_col(&[3])]).unwrap();
        let probe =
            RecordBatch::try_new(ps.clone(), vec![u64_col(&[10, 20]), i64_col(&[100, 200])])
                .unwrap();

        let matches = vec![
            JoinMatch {
                build_batch_idx: 0,
                build_row_idx: 0,
                probe_row_idx: 0,
            },
            JoinMatch {
                build_batch_idx: 1,
                build_row_idx: 0,
                probe_row_idx: 1,
            },
        ];

        let batch = emit_joined_batch(
            &[b0, b1],
            &probe,
            &matches,
            &[],
            ProbeSide::Right,
            &bs,
            &ps,
            &out,
        )
        .expect("emit");

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ys = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((keys.value(0), ys.value(0)), (10, 100));
        assert_eq!((keys.value(1), ys.value(1)), (20, 200));
    }
}
