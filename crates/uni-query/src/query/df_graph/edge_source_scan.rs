// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Edge-driven source scan for an unconstrained traversal source.
//!
//! `MATCH ()-[r:R]->() …` has nothing to anchor its source on, so it used to
//! plan as every vertex in the graph followed by a traversal from each one. At
//! LDBC SF1 that is 3.18M vertices to emit 21654 `WORK_AT` edges, 5.7 s where
//! the label-narrowed plan takes 0.26 s; narrowing needs the edge type's
//! endpoints to be verified, and is limited to one declared source label.
//!
//! [`EdgeSourceScanExec`] instead reads the type's adjacency first. It
//! collects every vertex the adjacency layers hold an entry for, for each
//! edge type along the traversal's direction
//! (`GraphExecutionContext::edge_source_candidates`), publishes that set into
//! the vid slot of the unchanged all-vertex scan below it, and runs the scan.
//! The traversal above is the same operator as before and sees the same
//! adjacency.
//!
//! # Why the result is unchanged
//!
//! The scan returns exactly the rows the unrestricted scan returns whose vid
//! is a candidate: the same operator under the same visibility (MVCC, L0,
//! fork branch, bulk gate), only restricted by `_vid`. The candidates are a
//! superset of the vertices the traversal can return a neighbor for, because
//! they are read from the same layers `get_neighbors` reads (Main CSR,
//! overlays, the shadow CSR for a versioned read, and the transaction L0).
//! A vertex the restriction drops therefore has no neighbor of these types in
//! this direction, and the non-optional traversal would have dropped it too.
//! Each vertex is still scanned once, so multiplicity is unchanged; undirected
//! patterns and self-loops are produced by the traversal exactly as before.
//!
//! # When it does not restrict
//!
//! - Inside a read-write transaction, whose read set must record every vertex
//!   the pattern depended on: a vertex with no `R` edge yet is a read ("no
//!   edges here") that a concurrent edge insert has to conflict with. The slot
//!   is published empty, which is the full scan.
//! - When the candidates cover a large share of the vertex table
//!   (`vertex_scan_beats_lookup`), where one full scan beats chunked
//!   `_vid IN (…)` lookups — the arm `VidLookupJoinExec` takes for the same
//!   reason.
//!
//! The planner emits it only under a fixed-length, non-optional traversal over
//! native edge types whose source is an unfiltered, non-optional `ScanAll`
//! (`DataFusionPlanner::plan_traverse`).

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use uni_store::storage::direction::Direction;

use super::GraphExecutionContext;
use super::ReadSetRecordingExec;
use super::scan::{DynamicVidFilter, GraphScanExec, vertex_scan_beats_lookup};
use super::vid_lookup_join::MAX_VIDS_PER_CHUNK;

/// Bytes charged per candidate vid: the `HashSet` entry plus its copy in the
/// published, sorted list.
const BYTES_PER_CANDIDATE: usize = 2 * std::mem::size_of::<u64>() + 8;

/// Restricts an all-vertex scan to the vertices that have an edge of the
/// traversal's types along its direction. See the [module docs](self).
pub struct EdgeSourceScanExec {
    /// The all-vertex scan, wired to `slot` (optionally under the read-set
    /// wrapper — the same one level [`probe_scan`](super::vid_lookup_join::probe_scan)
    /// looks through).
    input: Arc<dyn ExecutionPlan>,
    /// The vid restriction `input` reads on each `execute()`.
    slot: Arc<DynamicVidFilter>,
    /// Held while a restriction is published and the scan executed with it.
    exec_lock: Arc<parking_lot::Mutex<()>>,
    graph_ctx: Arc<GraphExecutionContext>,
    /// Edge types the traversal above walks.
    edge_type_ids: Vec<u32>,
    /// Direction the traversal above walks them in.
    direction: Direction,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for EdgeSourceScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EdgeSourceScanExec")
            .field("edge_type_ids", &self.edge_type_ids)
            .field("direction", &self.direction)
            .finish_non_exhaustive()
    }
}

impl EdgeSourceScanExec {
    /// Wraps `scan`, an all-vertex [`GraphScanExec`] (bare, or directly under a
    /// [`ReadSetRecordingExec`]), so it reads only the sources of
    /// `edge_type_ids` along `direction`.
    ///
    /// # Errors
    ///
    /// Fails when `scan` is not such a plan.
    pub fn try_new(
        scan: Arc<dyn ExecutionPlan>,
        graph_ctx: Arc<GraphExecutionContext>,
        edge_type_ids: Vec<u32>,
        direction: Direction,
    ) -> DFResult<Self> {
        let slot = Arc::new(DynamicVidFilter::default());
        let input = wire(&scan, &slot)?;
        Ok(Self {
            input,
            slot,
            exec_lock: Arc::default(),
            graph_ctx,
            edge_type_ids,
            direction,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Edge types whose sources this scan is restricted to.
    #[must_use]
    pub fn edge_type_ids(&self) -> &[u32] {
        &self.edge_type_ids
    }

    /// Direction the sources are taken along.
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.direction
    }
}

/// `plan` with its scan reading `slot`, keeping a read-set wrapper above it.
fn wire(
    plan: &Arc<dyn ExecutionPlan>,
    slot: &Arc<DynamicVidFilter>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let scan = super::vid_lookup_join::probe_scan(plan).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(
            "EdgeSourceScanExec: child must be a GraphScanExec".into(),
        )
    })?;
    let wired: Arc<dyn ExecutionPlan> = Arc::new(scan.with_dynamic_vid_filter(Arc::clone(slot)));
    if plan.downcast_ref::<GraphScanExec>().is_some() {
        Ok(wired)
    } else {
        debug_assert!(plan.downcast_ref::<ReadSetRecordingExec>().is_some());
        Arc::clone(plan).with_new_children(vec![wired])
    }
}

impl DisplayAs for EdgeSourceScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EdgeSourceScanExec: edge_types={:?}, direction={:?}",
            self.edge_type_ids, self.direction
        )
    }
}

impl ExecutionPlan for EdgeSourceScanExec {
    fn name(&self) -> &str {
        "EdgeSourceScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            datafusion::error::DataFusionError::Internal(
                "EdgeSourceScanExec requires exactly one child".to_string(),
            )
        })?;
        // Re-wire rather than trust the new child: a rebuilt scan does not
        // carry this node's slot, and an unwired scan would read every vertex
        // while this node believed it was restricting it.
        Ok(Arc::new(Self {
            input: wire(&child, &self.slot)?,
            slot: Arc::clone(&self.slot),
            exec_lock: Arc::clone(&self.exec_lock),
            graph_ctx: Arc::clone(&self.graph_ctx),
            edge_type_ids: self.edge_type_ids.clone(),
            direction: self.direction,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = Arc::clone(&self.input);
        let slot = Arc::clone(&self.slot);
        let exec_lock = Arc::clone(&self.exec_lock);
        let graph_ctx = Arc::clone(&self.graph_ctx);
        let edge_type_ids = self.edge_type_ids.clone();
        let direction = self.direction;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let candidates_metric: Count =
            MetricBuilder::new(&self.metrics).counter("edge_source_candidates", partition);
        let full_scan_metric: Count =
            MetricBuilder::new(&self.metrics).counter("edge_source_full_scan", partition);
        let reservation = MemoryConsumer::new(format!("EdgeSourceScanExec[{partition}]"))
            .register(context.memory_pool());

        let streams = async move {
            let restrictions: Vec<Option<Vec<u64>>> = if graph_ctx.records_read_set() {
                full_scan_metric.add(1);
                vec![None]
            } else {
                graph_ctx
                    .ensure_adjacency_warmed(&edge_type_ids, direction)
                    .await
                    .map_err(super::common::exec_err)?;
                let candidates = graph_ctx.edge_source_candidates(&edge_type_ids, direction);
                reservation.try_grow(candidates.len().saturating_mul(BYTES_PER_CANDIDATE))?;
                candidates_metric.add(candidates.len());
                // The all-vertex scan reads the shared `vertices` table. An
                // unknown size (a fork branch, a pinned view) keeps the lookup.
                let table_rows = graph_ctx
                    .storage()
                    .main_vertex_row_count()
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                if candidates.is_empty() {
                    Vec::new()
                } else if vertex_scan_beats_lookup(candidates.len(), table_rows) {
                    full_scan_metric.add(1);
                    vec![None]
                } else {
                    let mut vids: Vec<u64> = candidates.into_iter().map(|v| v.as_u64()).collect();
                    vids.sort_unstable();
                    vids.chunks(MAX_VIDS_PER_CHUNK)
                        .map(|chunk| Some(chunk.to_vec()))
                        .collect()
                }
            };

            // The scan reads the slot inside `execute()`, so each stream keeps
            // the restriction published just before it was created.
            //
            // Publishing and executing happen under one lock so two executions
            // of this node (an `Apply` re-running its body, a re-executed
            // plan) cannot hand each other's restriction to the scan.
            let mut parts: Vec<SendableRecordBatchStream> = Vec::with_capacity(restrictions.len());
            {
                let _publishing = exec_lock.lock();
                for restriction in restrictions {
                    slot.set(restriction);
                    parts.push(input.execute(partition, Arc::clone(&context))?);
                }
                slot.set(None);
            }
            // Keep the reservation for as long as the chunks are being read.
            Ok::<_, datafusion::error::DataFusionError>(futures::stream::iter(parts).flatten().map(
                move |batch| {
                    let _held = &reservation;
                    batch
                },
            ))
        };

        let stream = futures::stream::once(streams)
            .try_flatten()
            .map(move |batch| {
                if let Ok(b) = &batch {
                    baseline.record_output(b.num_rows());
                }
                batch
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
