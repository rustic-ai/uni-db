// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Algorithm adapter — a Python guest algorithm driving GraphCompute kernels.
//!
//! A Python algorithm is `def my_algo(gc, *args)` where `gc` is an injected
//! [`GcSession`](crate::graph_compute::GcSession). The guest drives the coarse
//! kernels through `gc` and calls `gc.emit(name, handle)`; the adapter then
//! prepends a `nodeId` column (slot→Vid) and assembles the declared output batch
//! (proposal §4.6). This closes the loader's "no query-time host callback" gap by
//! threading the host projection into the guest, and the "no loop bounding" gap
//! via the session's cooperative deadline.
//!
//! v1 projects the whole graph (all labels / all edge types); a per-algorithm
//! projection spec is a follow-up.
//
// Rust guideline compliant

#![cfg(feature = "pyo3")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use pyo3::prelude::*;
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::algorithm::{AlgorithmContext, AlgorithmProvider, AlgorithmSignature};
use uni_plugin_builtin::algorithms::bridge::{
    AlgorithmHostBridge, ProjectionPlan, await_projections, build_projections,
};
use uni_plugin_builtin::algorithms::graph_compute::{
    AlgoSession, Arena, DEFAULT_ARENA_MAX_HANDLES, WorkBudget, next_session_epoch,
};

use crate::graph_compute::new_session;
use crate::runtime::PyPluginRuntime;

/// Default wall-clock deadline for a guest algorithm invocation (loader gap b).
const DEFAULT_DEADLINE_SECS: u64 = 30;

/// Algorithm adapter dispatching to a Python callable driving the kernels.
#[derive(Debug)]
pub struct PyAlgorithm {
    runtime: Arc<PyPluginRuntime>,
    local_name: SmolStr,
    signature: AlgorithmSignature,
}

impl PyAlgorithm {
    /// Constructs an algorithm adapter binding `local_name` against the runtime.
    #[must_use]
    pub fn new(
        runtime: Arc<PyPluginRuntime>,
        local_name: impl Into<SmolStr>,
        signature: AlgorithmSignature,
    ) -> Self {
        Self {
            runtime,
            local_name: local_name.into(),
            signature,
        }
    }
}

impl AlgorithmProvider for PyAlgorithm {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "python algorithm: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| FnError::new(0x801, "python algorithm: host is not the bridge"))?;

        let callable = self.runtime.get(self.local_name.as_str()).ok_or_else(|| {
            FnError::new(
                0x830,
                format!(
                    "python algorithm callable `{}` not in runtime",
                    self.local_name
                ),
            )
        })?;
        let local_name = self.local_name.clone();

        let mut json_args: Vec<serde_json::Value> = serde_json::from_str(ctx.config_json)
            .map_err(|e| FnError::new(0x802, format!("python algorithm: bad config json: {e}")))?;
        // A trailing projection-config object scopes/weights the graph the guest
        // sees (issue #151) and may pre-declare named `scopes`; strip it before
        // the remaining args reach the guest.
        let plan = ProjectionPlan::take_from_args(&mut json_args)?;
        let projections = build_projections(bridge, &plan);
        let (work_cap, arena_bytes) = bridge.graph_compute_caps();
        let deadline_ms_cap = bridge.graph_compute_deadline_ms();

        let out_schema: SchemaRef = Arc::new(Schema::new(self.signature.output_fields.clone()));
        let schema_for_batch = Arc::clone(&out_schema);
        let expected_cols = uni_plugin_builtin::algorithms::graph_compute::guest_emit_columns(
            &self.signature.output_fields,
        );

        let stream = futures::stream::once(async move {
            let bound = await_projections(projections)
                .await
                .map_err(|e| DataFusionError::Execution(format!("python algorithm: {e}")))?;
            let graph = Arc::clone(&bound.primary);

            // An explicit grant is authoritative and may raise the ceiling;
            // otherwise the size-derived default (proposal §9), sized across
            // *every* bound projection — a guest with named scopes can do O(V+E)
            // work on each, so sizing from the primary alone under-budgets it.
            let budget = WorkBudget::resolve(work_cap, bound.total_vertices(), bound.total_edges());
            let session = Arc::new(parking_lot::Mutex::new(
                AlgoSession::new(
                    next_session_epoch().map_err(|e| {
                        DataFusionError::Execution(format!("python algorithm: {e}"))
                    })?,
                    budget,
                    Arena::new(arena_bytes, DEFAULT_ARENA_MAX_HANDLES),
                )
                .with_expected_columns(expected_cols),
            ));
            // The primary binds FIRST: `bind_graph` treats the first bind as the
            // primary projection, which `emit` keys its `nodeId` column to.
            let (g, scopes) = {
                let mut sess = session.lock();
                let g = sess.bind_graph(Arc::clone(&graph));
                let scopes: Vec<(String, i64)> = bound
                    .named
                    .iter()
                    .map(|(name, proj)| {
                        (
                            name.clone(),
                            crate::graph_compute::to_i64(sess.bind_graph(Arc::clone(proj))),
                        )
                    })
                    .collect();
                (g, scopes)
            };
            let deadline_ms = deadline_ms_cap.unwrap_or(DEFAULT_DEADLINE_SECS * 1000);
            let started = Instant::now();
            let deadline_at = started.checked_add(Duration::from_millis(deadline_ms));

            // Arm a wall-clock watchdog on the guest thread BEFORE running it, so
            // a guest that never calls a kernel (`while True: pass`) is still
            // interrupted (the cooperative per-kernel check only fires on calls).
            // Both attaches run on this same tokio worker thread (no await
            // between), so the captured thread id is the one the guest runs on.
            let tid = crate::watchdog::current_thread_id_attached()
                .map_err(|e| DataFusionError::Execution(format!("python algorithm tid: {e}")))?;
            // #233: a failed spawn used to leave the watchdog silently absent,
            // so the deadline this block documents would not be enforced.
            let watchdog = deadline_at
                .map(|d| crate::watchdog::DeadlineWatchdog::arm(tid, d))
                .transpose()
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "python algorithm: could not arm the deadline watchdog, so the query \
                         deadline could not be enforced: {e}"
                    ))
                })?;

            // Drive the guest under the GIL. No await happens inside this block,
            // so the GIL is held only for the synchronous guest run.
            let call_result: Result<(), DataFusionError> = Python::attach(|py| {
                let gc = new_session(Arc::clone(&session), g, deadline_at, scopes.clone());
                let gc_obj = Py::new(py, gc)
                    .map_err(|e| DataFusionError::Execution(format!("GcSession: {e}")))?;
                let mut py_args: Vec<Py<PyAny>> = vec![gc_obj.into_any()];
                for v in &json_args {
                    py_args.push(json_to_py(py, v)?);
                }
                let bound = callable.bind(py);
                let tuple = pyo3::types::PyTuple::new(py, py_args)
                    .map_err(|e| DataFusionError::Execution(format!("py tuple: {e}")))?;
                bound.call1(tuple).map(|_| ()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "python algorithm `{local_name}` (0x867 if timeout): {e}"
                    ))
                })
            });

            // Drop the watchdog OUTSIDE the GIL scope (else Drop::join deadlocks
            // against its own GIL acquisition), then cancel any interrupt that
            // may have landed after the guest returned, so it cannot surface on a
            // later CALL sharing this worker thread.
            drop(watchdog);
            crate::watchdog::cancel_pending_interrupt(tid);

            // A guest fault does not carry a kernel code across the Python
            // boundary, so classify incompleteness from host-side state: an
            // elapsed deadline is a Timeout (the watchdog fired), a drained
            // budget is Exhausted. Other faults surface verbatim (§5.2).
            if let Err(orig) = call_result {
                let (spent, budget, crumbs) = {
                    let s = session.lock();
                    (
                        s.work_spent_units(),
                        s.work_budget_units(),
                        s.trace_breadcrumbs(),
                    )
                };
                let deadline_elapsed = deadline_at.is_some_and(|d| Instant::now() >= d);
                return Err(
                    uni_plugin_builtin::algorithms::graph_compute::error::incomplete_tag_after_guest(
                        local_name.as_str(),
                        deadline_elapsed,
                        spent,
                        budget,
                        started.elapsed().as_millis() as u64,
                        &crumbs,
                    )
                    .map_or(orig, DataFusionError::Execution),
                );
            }
            let emitted = session
                .lock()
                .finish_emitted()
                .map_err(|e| DataFusionError::Execution(format!("python algorithm emit: {e}")))?;
            build_batch(&schema_for_batch, &graph, &emitted)
                .map_err(|e| DataFusionError::Execution(format!("python algorithm emit: {e}")))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Converts a JSON argument to a Python object (int / float / string / bool).
fn json_to_py(py: Python<'_>, v: &serde_json::Value) -> Result<Py<PyAny>, DataFusionError> {
    use pyo3::IntoPyObjectExt;
    let obj = match v {
        serde_json::Value::Number(n) if n.is_i64() => n
            .as_i64()
            .into_py_any(py)
            .map_err(|e| DataFusionError::Execution(format!("i64→py: {e}")))?,
        serde_json::Value::Number(n) => n
            .as_f64()
            .unwrap_or(0.0)
            .into_py_any(py)
            .map_err(|e| DataFusionError::Execution(format!("f64→py: {e}")))?,
        serde_json::Value::String(s) => s
            .into_py_any(py)
            .map_err(|e| DataFusionError::Execution(format!("str→py: {e}")))?,
        serde_json::Value::Bool(b) => b
            .into_py_any(py)
            .map_err(|e| DataFusionError::Execution(format!("bool→py: {e}")))?,
        other => {
            return Err(DataFusionError::Execution(format!(
                "python algorithm: unsupported arg {other}"
            )));
        }
    };
    Ok(obj)
}

/// Assembles the output batch from emitted columns + a `nodeId` column.
fn build_batch(
    schema: &SchemaRef,
    graph: &uni_algo::algo::GraphProjection,
    emitted: &[(String, Vec<f64>)],
) -> Result<RecordBatch, FnError> {
    let n = graph.vertex_count();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if field.name() == "nodeId" {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "vids fit i64 in practice; Cypher integers are i64"
            )]
            let ids: Vec<i64> = (0..n as u32)
                .map(|slot| graph.to_vid(slot).as_u64() as i64)
                .collect();
            columns.push(Arc::new(Int64Array::from(ids)));
            continue;
        }
        let (_, values) = emitted
            .iter()
            .find(|(name, _)| name == field.name())
            .ok_or_else(|| {
                FnError::new(
                    0x869,
                    format!("guest did not emit declared column `{}`", field.name()),
                )
            })?;
        match field.data_type() {
            DataType::Float64 => columns.push(Arc::new(Float64Array::from(values.clone()))),
            #[expect(
                clippy::cast_possible_truncation,
                reason = "int columns hold whole f64 values"
            )]
            DataType::Int64 => {
                let ints: Vec<i64> = values.iter().map(|&v| v as i64).collect();
                columns.push(Arc::new(Int64Array::from(ints)));
            }
            other => {
                return Err(FnError::new(
                    0x862,
                    format!("unsupported emit column type {other:?}"),
                ));
            }
        }
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| FnError::new(0x15, format!("python algorithm batch: {e}")))
}
