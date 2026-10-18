// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Sideways information passing into an indexed property scan (A7).
//!
//! `MATCH (a:N {uid: $x}), (b:N) WHERE b.uid = a.name` binds `a` to one row,
//! yet was planned as a hash join of that row against a scan of every `N`:
//! the bound value of `a.name` never reached `b`'s scan, so `b.uid`'s index
//! could not serve it. This operator replaces such a join when the probe side
//! is a scan whose join key is an indexed property:
//!
//!   1. Run the build side fully and collect the distinct non-null values of
//!      its anchor key.
//!   2. If the key set is small against the probe table, publish
//!      `prop IN (keys)` into the probe scan's `DynamicPropertyFilter` — as a
//!      Lance SQL filter, which the scalar index serves for flushed rows, and as
//!      an Arrow predicate for the merged batch, which keeps unflushed L0 rows
//!      exact — and execute the probe once per chunk of keys. Otherwise publish
//!      nothing and run one unrestricted probe, the scan the hash join read.
//!   3. Join in memory on exact key equality; other equi-pairs are checked per
//!      candidate.
//!
//! [`VidLookupJoinExec`](super::vid_lookup_join::VidLookupJoinExec) is the
//! `_vid`-keyed sibling; the output layout, join kinds and null-padding are
//! shared with it.
//!
//! # Why the rewrite is exact
//!
//! The planner emits this only when every equi-pair is a bare column on both
//! sides of one exact native key type (see `try_emit_property_lookup_join` in
//! `df_planner.rs`), and the anchor key is a string or a 64-bit integer. For
//! those types Arrow equality is Cypher equality, so:
//!
//! - a NULL key matches nothing (and is never published);
//! - restricting the probe scan to `prop IN (keys)` removes only rows whose key
//!   no build row could equal;
//! - the probe chain between this operator and the scan is row-wise (a filter,
//!   a pass-through projection, the SSI read-set recorder), so restricting the
//!   scan restricts its output to the same rows.
//!
//! Mixed types — a float against an integer, a string against a number — need
//! Cypher's comparison and never reach this operator; the planner keeps the
//! hash join with its re-check filter for them.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::{Column, InListExpr, Literal};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use futures::{Stream, StreamExt};
use uni_store::backend::types::{FilterExpr, Scalar};

use super::ReadSetRecordingExec;
use super::common::{BatchFootprint, compute_plan_properties};
use super::scan::{DynamicPropertyFilter, GraphScanExec, vertex_scan_beats_lookup};
use super::vid_lookup_join::{
    EquiPair, JoinMatch, ProbeSide, VidJoinKind, concat_schemas, emit_joined_batch, values_equal,
};

/// Most distinct keys a lookup publishes; a larger build reads the probe once.
///
/// Each key becomes one index probe and one literal in the pushed `IN` list,
/// while the unrestricted scan costs one pass however many keys there are, so
/// past some count the scan wins whatever the table size. 4,096 keeps the
/// lookup arm well inside the regime where `vertex_scan_beats_lookup` (25% of
/// the table) is the binding condition for any table of 16k rows or more.
pub(crate) const MAX_LOOKUP_KEYS: usize = 4_096;

/// Keys per published `IN` list; larger key sets probe once per chunk.
///
/// Bounds the size of one Lance filter string and of one Arrow `IN` set.
const MAX_KEYS_PER_CHUNK: usize = 1_024;
const _: () = assert!(MAX_KEYS_PER_CHUNK <= MAX_LOOKUP_KEYS);

/// The probe-side scan a lookup can restrict, reached through row-wise nodes.
///
/// Walks from `plan` through [`ReadSetRecordingExec`], an unprojected
/// [`FilterExec`] and a [`ProjectionExec`] that passes `key_column` through
/// unchanged, to a [`GraphScanExec`]. Anything else is not a probe.
pub(crate) fn probe_scan<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    key_column: &str,
) -> Option<&'a GraphScanExec> {
    if let Some(scan) = plan.downcast_ref::<GraphScanExec>() {
        return Some(scan);
    }
    let child = row_wise_child(plan, key_column)?;
    probe_scan(child, key_column)
}

/// The single child of a node that maps each input row to at most one output
/// row and passes `key_column` through, or `None`.
fn row_wise_child<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    key_column: &str,
) -> Option<&'a Arc<dyn ExecutionPlan>> {
    if let Some(rs) = plan.downcast_ref::<ReadSetRecordingExec>() {
        return Some(rs.input());
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return filter.projection().is_none().then(|| filter.input());
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let passes = projection.expr().iter().any(|e| {
            e.alias == key_column
                && e.expr
                    .downcast_ref::<Column>()
                    .is_some_and(|c| c.name() == key_column)
        });
        return passes.then(|| projection.input());
    }
    None
}

/// `plan` with its probe scan wired to read `slot`, rebuilding the row-wise
/// nodes above it so read-set recording and residual filters stay in place.
fn wire_probe(
    plan: &Arc<dyn ExecutionPlan>,
    key_column: &str,
    slot: &Arc<DynamicPropertyFilter>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.downcast_ref::<GraphScanExec>() {
        return Ok(Arc::new(
            scan.with_dynamic_property_filter(Arc::clone(slot)),
        ));
    }
    let child = row_wise_child(plan, key_column).ok_or_else(|| {
        DataFusionError::Plan(
            "PropertyLookupJoinExec: probe side must be a row-wise chain over a GraphScanExec"
                .into(),
        )
    })?;
    let wired = wire_probe(child, key_column, slot)?;
    Arc::clone(plan).with_new_children(vec![wired])
}

/// Join-key types the lookup can push down exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LookupKeyType {
    /// `Utf8`, `LargeUtf8` or `Utf8View`, the same on both sides.
    String,
    /// `Int64` on both sides.
    Int64,
}

impl LookupKeyType {
    /// The lookup key type for an Arrow type, if it has an exact pushdown.
    pub(crate) fn of(dt: &DataType) -> Option<Self> {
        match dt {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Self::String),
            DataType::Int64 => Some(Self::Int64),
            _ => None,
        }
    }
}

/// What the probe side looks up by: the indexed property and its scan column.
#[derive(Debug, Clone)]
pub struct ProbeKey {
    /// Storage column (the property name) the Lance filter names.
    pub(crate) property: String,
    /// The scan's output column for it, `{variable}.{property}`.
    pub(crate) column: String,
    pub(crate) key_type: LookupKeyType,
}

impl ProbeKey {
    /// A probe key on `property` of `variable`, typed `key_type`.
    pub(crate) fn new(variable: &str, property: &str, key_type: LookupKeyType) -> Self {
        Self {
            property: property.to_string(),
            column: format!("{variable}.{property}"),
            key_type,
        }
    }
}

/// Equi-join that looks the probe side up by the build side's key values.
pub struct PropertyLookupJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    probe_side: ProbeSide,
    /// `pairs[0]` is the anchor: its probe column is `probe_key.column`.
    pairs: Vec<EquiPair>,
    join_kind: VidJoinKind,
    probe_key: ProbeKey,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    probe_filter: Arc<DynamicPropertyFilter>,
}

impl fmt::Debug for PropertyLookupJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyLookupJoinExec")
            .field("probe_side", &self.probe_side)
            .field("probe_key", &self.probe_key)
            .field("pairs", &self.pairs.len())
            .field("join_kind", &self.join_kind)
            .finish()
    }
}

impl PropertyLookupJoinExec {
    /// Build the operator, wiring the probe side's scan to a fresh filter slot.
    ///
    /// # Errors
    ///
    /// Returns a plan error if `pairs` is empty, or if the probe side is not a
    /// row-wise chain over a [`GraphScanExec`] that outputs `probe_key.column`
    /// at the anchor's probe index.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        probe_side: ProbeSide,
        pairs: Vec<EquiPair>,
        join_kind: VidJoinKind,
        probe_key: ProbeKey,
    ) -> DFResult<Self> {
        let anchor = *pairs.first().ok_or_else(|| {
            DataFusionError::Plan("PropertyLookupJoinExec: pairs must be non-empty".into())
        })?;
        let probe_plan = match probe_side {
            ProbeSide::Left => &left,
            ProbeSide::Right => &right,
        };
        let probe_schema = probe_plan.schema();
        let anchor_field = probe_schema.field(anchor.probe_col(probe_side));
        if anchor_field.name() != &probe_key.column {
            return Err(DataFusionError::Plan(format!(
                "PropertyLookupJoinExec: anchor probe column is `{}`, expected `{}`",
                anchor_field.name(),
                probe_key.column
            )));
        }
        let probe_filter = Arc::new(DynamicPropertyFilter::default());
        let probe_wired = wire_probe(probe_plan, &probe_key.column, &probe_filter)?;
        let (left, right) = match probe_side {
            ProbeSide::Left => (probe_wired, right),
            ProbeSide::Right => (left, probe_wired),
        };
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
            probe_key,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            probe_filter,
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

impl DisplayAs for PropertyLookupJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PropertyLookupJoinExec: probe={:?}, key={}, pairs={}, kind={:?}",
            self.probe_side,
            self.probe_key.column,
            self.pairs.len(),
            self.join_kind
        )
    }
}

impl ExecutionPlan for PropertyLookupJoinExec {
    fn name(&self) -> &str {
        "PropertyLookupJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [left, right]: [Arc<dyn ExecutionPlan>; 2] =
            children.try_into().map_err(|c: Vec<_>| {
                DataFusionError::Plan(format!(
                    "PropertyLookupJoinExec expects two children (build and probe); got {}",
                    c.len()
                ))
            })?;
        Ok(Arc::new(Self::try_new(
            left,
            right,
            self.probe_side,
            self.pairs.clone(),
            self.join_kind,
            self.probe_key.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let job = LookupJob {
            build: self.build_child().clone(),
            probe: self.probe_child().clone(),
            probe_filter: self.probe_filter.clone(),
            probe_side: self.probe_side,
            pairs: self.pairs.clone(),
            join_kind: self.join_kind,
            probe_key: self.probe_key.clone(),
            left_schema: self.left.schema(),
            right_schema: self.right.schema(),
            output_schema: self.output_schema.clone(),
        };
        Ok(Box::pin(LookupStream {
            state: Some(Box::pin(job.run(partition, context))),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

type JoinFuture = Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>;

/// Yields the single joined batch, then ends.
struct LookupStream {
    state: Option<JoinFuture>,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for LookupStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        let Some(fut) = self.state.as_mut() else {
            return Poll::Ready(None);
        };
        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.state = None;
                if let Ok(batch) = &result {
                    self.metrics.record_output(batch.num_rows());
                }
                Poll::Ready(Some(result))
            }
        }
    }
}

impl RecordBatchStream for LookupStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Everything one partition's run needs, owned so the future is `'static`.
struct LookupJob {
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    probe_filter: Arc<DynamicPropertyFilter>,
    probe_side: ProbeSide,
    pairs: Vec<EquiPair>,
    join_kind: VidJoinKind,
    probe_key: ProbeKey,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
}

impl LookupJob {
    async fn run(self, partition: usize, context: Arc<TaskContext>) -> DFResult<RecordBatch> {
        let reservation = MemoryConsumer::new(format!("PropertyLookupJoinExec[{partition}]"))
            .register(context.memory_pool());
        let mut footprint = BatchFootprint::new();

        let mut build_batches: Vec<RecordBatch> = Vec::new();
        let mut build_stream = self.build.execute(partition, Arc::clone(&context))?;
        while let Some(batch) = build_stream.next().await {
            let batch = batch?;
            reservation.try_grow(footprint.add(&batch))?;
            build_batches.push(batch);
        }
        if build_batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(RecordBatch::new_empty(self.output_schema));
        }

        let anchor = self.pairs[0];
        let build_key_idx = anchor.build_col(self.probe_side);
        let probe_key_idx = anchor.probe_col(self.probe_side);

        // Distinct non-null keys, in first-seen order so chunks are stable.
        let mut seen: HashSet<ScalarValue> = HashSet::new();
        let mut keys: Vec<ScalarValue> = Vec::new();
        for batch in &build_batches {
            let col = batch.column(build_key_idx);
            for row in 0..col.len() {
                if col.is_null(row) {
                    continue;
                }
                let key = ScalarValue::try_from_array(col, row)?;
                if seen.insert(key.clone()) {
                    keys.push(key);
                }
            }
        }
        drop(seen);

        let probe_schema = self.probe.schema();
        let probe_batch = if keys.is_empty() {
            RecordBatch::new_empty(probe_schema)
        } else {
            let table_rows = match probe_scan(&self.probe, &self.probe_key.column) {
                Some(scan) => scan.cached_table_rows().await,
                None => 0,
            };
            let lookup =
                keys.len() <= MAX_LOOKUP_KEYS && !vertex_scan_beats_lookup(keys.len(), table_rows);
            let restrictions: Vec<Option<(String, Arc<dyn PhysicalExpr>)>> = if lookup {
                keys.chunks(MAX_KEYS_PER_CHUNK)
                    .map(|chunk| self.restriction(chunk).map(Some))
                    .collect::<DFResult<_>>()?
            } else {
                vec![None]
            };
            let mut parts: Vec<RecordBatch> = Vec::new();
            for restriction in restrictions {
                self.probe_filter.set(restriction);
                let mut stream = self.probe.execute(partition, Arc::clone(&context))?;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        reservation.try_grow(footprint.add(&batch))?;
                        parts.push(batch);
                    }
                }
            }
            self.probe_filter.set(None);
            match parts.len() {
                0 => RecordBatch::new_empty(probe_schema),
                1 => parts.pop().expect("one part"),
                _ => arrow::compute::concat_batches(&probe_schema, &parts)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            }
        };

        // Probe rows by key.
        let probe_keys = probe_batch.column(probe_key_idx);
        let mut index: HashMap<ScalarValue, Vec<usize>> = HashMap::new();
        for row in 0..probe_keys.len() {
            if !probe_keys.is_null(row) {
                index
                    .entry(ScalarValue::try_from_array(probe_keys, row)?)
                    .or_default()
                    .push(row);
            }
        }

        let outer = self.join_kind == VidJoinKind::Left;
        let mut matches: Vec<JoinMatch> = Vec::new();
        let mut unmatched: Vec<(usize, usize)> = Vec::new();
        for (build_batch_idx, build_batch) in build_batches.iter().enumerate() {
            let build_keys = build_batch.column(build_key_idx);
            for build_row_idx in 0..build_batch.num_rows() {
                let candidates = if build_keys.is_null(build_row_idx) {
                    None
                } else {
                    index.get(&ScalarValue::try_from_array(build_keys, build_row_idx)?)
                };
                let mut matched = false;
                for &probe_row_idx in candidates.into_iter().flatten() {
                    if self.residual_pairs_match(
                        build_batch,
                        build_row_idx,
                        &probe_batch,
                        probe_row_idx,
                    )? {
                        matches.push(JoinMatch {
                            build_batch_idx,
                            build_row_idx,
                            probe_row_idx,
                        });
                        matched = true;
                    }
                }
                if !matched && outer {
                    unmatched.push((build_batch_idx, build_row_idx));
                }
            }
        }

        emit_joined_batch(
            &build_batches,
            &probe_batch,
            &matches,
            &unmatched,
            self.probe_side,
            &self.left_schema,
            &self.right_schema,
            &self.output_schema,
        )
    }

    /// Whether every non-anchor equi-pair holds for this candidate pair.
    fn residual_pairs_match(
        &self,
        build: &RecordBatch,
        build_row: usize,
        probe: &RecordBatch,
        probe_row: usize,
    ) -> DFResult<bool> {
        for pair in &self.pairs[1..] {
            if !values_equal(
                build.column(pair.build_col(self.probe_side)),
                build_row,
                probe.column(pair.probe_col(self.probe_side)),
                probe_row,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The Lance filter and Arrow predicate restricting the probe to `keys`.
    fn restriction(&self, keys: &[ScalarValue]) -> DFResult<(String, Arc<dyn PhysicalExpr>)> {
        let scalars = keys
            .iter()
            .map(|k| scalar_for_lance(k, self.probe_key.key_type))
            .collect::<DFResult<Vec<_>>>()?;
        let lance = FilterExpr::one_of(self.probe_key.property.clone(), scalars)
            .to_sql()
            .map_err(|e| DataFusionError::Plan(format!("PropertyLookupJoinExec: {e}")))?;

        let scan = probe_scan(&self.probe, &self.probe_key.column).ok_or_else(|| {
            DataFusionError::Plan("PropertyLookupJoinExec: probe scan vanished".into())
        })?;
        let scan_schema = scan.schema();
        let col_idx = scan_schema.index_of(&self.probe_key.column)?;
        let col_type = scan_schema.field(col_idx).data_type().clone();
        let list = keys
            .iter()
            .map(|k| {
                let lit = k.cast_to(&col_type)?;
                Ok(Arc::new(Literal::new(lit)) as Arc<dyn PhysicalExpr>)
            })
            .collect::<DFResult<Vec<_>>>()?;
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&self.probe_key.column, col_idx));
        let runtime = InListExpr::try_new(column, list, false, &scan_schema)?;
        Ok((lance, Arc::new(runtime)))
    }
}

/// A build key as the storage scalar the Lance filter renders.
fn scalar_for_lance(key: &ScalarValue, key_type: LookupKeyType) -> DFResult<Scalar> {
    match (key_type, key) {
        (
            LookupKeyType::String,
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
        ) => Ok(Scalar::Str(s.clone())),
        (LookupKeyType::Int64, ScalarValue::Int64(Some(v))) => Ok(Scalar::Int(*v)),
        _ => Err(DataFusionError::Internal(format!(
            "PropertyLookupJoinExec: key {key:?} does not have the planned type {key_type:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exact_key_types_have_a_lookup() {
        assert_eq!(
            LookupKeyType::of(&DataType::Utf8),
            Some(LookupKeyType::String)
        );
        assert_eq!(
            LookupKeyType::of(&DataType::LargeUtf8),
            Some(LookupKeyType::String)
        );
        assert_eq!(
            LookupKeyType::of(&DataType::Int64),
            Some(LookupKeyType::Int64)
        );
        for dt in [
            DataType::Float64,
            DataType::Int32,
            DataType::LargeBinary,
            DataType::Boolean,
        ] {
            assert_eq!(LookupKeyType::of(&dt), None, "{dt:?}");
        }
    }

    #[test]
    fn keys_render_as_the_storage_scalar_of_their_type() {
        assert_eq!(
            scalar_for_lance(
                &ScalarValue::Utf8(Some("it's".into())),
                LookupKeyType::String
            )
            .unwrap(),
            Scalar::Str("it's".into())
        );
        assert_eq!(
            scalar_for_lance(&ScalarValue::Int64(Some(-3)), LookupKeyType::Int64).unwrap(),
            Scalar::Int(-3)
        );
        assert!(scalar_for_lance(&ScalarValue::Int64(Some(1)), LookupKeyType::String).is_err());
        assert!(scalar_for_lance(&ScalarValue::Utf8(None), LookupKeyType::String).is_err());
    }

    #[test]
    fn an_in_list_of_quoted_keys_renders_escaped() {
        let sql = FilterExpr::one_of("uid", [Scalar::Str("a'b".into()), Scalar::Str("c".into())])
            .to_sql()
            .unwrap();
        assert!(sql.contains("'a''b'"), "{sql}");
        assert!(
            !sql.contains("\"uid\""),
            "a double-quoted name is a string literal: {sql}"
        );
    }
}
