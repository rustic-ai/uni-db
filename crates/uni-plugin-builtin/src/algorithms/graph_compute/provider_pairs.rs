// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! First-party `uni.algo.gcoverlap` — all-pairs neighbourhood overlap.
//!
//! Dogfoods the bulk `all_pairs_overlap` kernel and the per-edge `emit_pairs`
//! egress (proposal §4.3 C-3). The result is a per-*edge* `(srcId, dstId, value)`
//! table — a shape a `[V]` map cannot express — so it uses the
//! [`PairList`](crate::algorithms::graph_compute::value::PairList) sink
//! rather than the `[V]`/`nodeId` path of `gcpagerank`. With the default `count`
//! metric each row's value is the adjacent pair's triangle support, the basis for
//! triangle counting and k-truss.
//!
//! # CALL shape
//! `CALL uni.algo.gcoverlap([metric[, pairMode[, k[, {nodeLabels, edgeTypes}]]]])`
//! yielding `(srcId INT, dstId INT, value FLOAT)`. `metric` is one of `count`
//! (default), `jaccard`, `overlap`, `cosine`, `adamic_adar`; `pairMode` is
//! `adjacent` (default) or `topk` (keep the `k` highest-value pairs).
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use uni_plugin::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
};
use uni_plugin::traits::procedure::NamedArgType;
use uni_plugin::traits::scalar::ArgType;

use super::graph_compute_slice_req;
use super::session::{AlgoSession, GraphCompute, OverlapMetric, PairSpec};
use super::{Arena, WorkBudget};
use crate::algorithms::bridge::AlgorithmHostBridge;

/// Default overlap metric (raw shared-neighbour count = triangle support).
const DEFAULT_METRIC: &str = "count";

/// All-pairs neighbourhood-overlap provider authored on the GraphCompute catalog.
///
/// See the [module docs](self) for the CALL shape and the per-edge egress.
pub struct GraphComputeOverlapProvider {
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for GraphComputeOverlapProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphComputeOverlapProvider")
            .finish_non_exhaustive()
    }
}

impl Default for GraphComputeOverlapProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphComputeOverlapProvider {
    /// Constructs the provider with its fixed `(srcId, dstId, value)` signature.
    #[must_use]
    pub fn new() -> Self {
        let output_fields = vec![
            Field::new("srcId", DataType::Int64, false),
            Field::new("dstId", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ];
        Self {
            signature: AlgorithmSignature {
                output_fields,
                docs: "uni.algo.gcoverlap([metric[, pairMode[, k[, config]]]]) — all-pairs \
                       neighbourhood overlap over adjacent vertex pairs (triangle support / \
                       k-truss basis) driven through the GraphCompute kernel catalog"
                    .to_owned(),
                args: overlap_args(),
                slices: vec![graph_compute_slice_req()],
                df_composable: true,
            },
        }
    }
}

/// The declared positional arguments of `uni.algo.gcoverlap` (proposal §4.6).
fn overlap_args() -> Vec<NamedArgType> {
    vec![
        NamedArgType {
            name: "metric".into(),
            ty: ArgType::Primitive(DataType::Utf8),
            default: Some(ScalarValue::Utf8(Some(DEFAULT_METRIC.to_owned()))),
            doc: "count (default) | jaccard | overlap | cosine | adamic_adar.".to_owned(),
        },
        NamedArgType {
            name: "pairMode".into(),
            ty: ArgType::Primitive(DataType::Utf8),
            default: Some(ScalarValue::Utf8(Some("adjacent".to_owned()))),
            doc: "adjacent (default, every adjacent pair) | topk.".to_owned(),
        },
        NamedArgType {
            name: "k".into(),
            ty: ArgType::Primitive(DataType::Int64),
            default: Some(ScalarValue::Int64(Some(0))),
            doc: "For pairMode = topk: the number of highest-value pairs to keep.".to_owned(),
        },
        NamedArgType {
            name: "config".into(),
            ty: ArgType::CypherValue,
            default: Some(ScalarValue::Null),
            doc: "Optional {nodeLabels, edgeTypes} projection filter.".to_owned(),
        },
    ]
}

/// The parsed positional arguments of a `gcoverlap` CALL.
struct OverlapArgs {
    metric: OverlapMetric,
    spec: PairSpec,
    projection: GraphProjectionSpec,
}

/// Maps a metric name to its [`OverlapMetric`].
fn parse_metric(name: &str) -> Result<OverlapMetric, FnError> {
    match name {
        "count" => Ok(OverlapMetric::Count),
        "jaccard" => Ok(OverlapMetric::Jaccard),
        "overlap" => Ok(OverlapMetric::Overlap),
        "cosine" => Ok(OverlapMetric::Cosine),
        "adamic_adar" => Ok(OverlapMetric::AdamicAdar),
        other => Err(FnError::new(
            0x802,
            format!("gcoverlap: unknown metric `{other}`"),
        )),
    }
}

/// Parses the positional `config_json` array into [`OverlapArgs`].
fn parse_config(config_json: &str) -> Result<OverlapArgs, FnError> {
    let args: Vec<serde_json::Value> = serde_json::from_str(config_json)
        .map_err(|e| FnError::new(0x802, format!("gcoverlap: bad config json: {e}")))?;

    let metric = args
        .first()
        .and_then(serde_json::Value::as_str)
        .map_or_else(|| parse_metric(DEFAULT_METRIC), parse_metric)?;

    let pair_mode = args
        .get(1)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("adjacent");
    let spec = if pair_mode == "topk" {
        // #233 Tier 1: a missing or non-integer `k` used to default to 0,
        // which is a valid `TopKCandidates` value meaning "return nothing" —
        // so `uni.gc.overlap` answered with zero rows instead of reporting
        // that the argument was unusable.
        let Some(k) = args.get(2).and_then(serde_json::Value::as_u64) else {
            return Err(FnError::new(
                0x803,
                "gcoverlap: pair mode `topk` requires an integer k as the third argument",
            ));
        };
        PairSpec::TopKCandidates(u32::try_from(k).unwrap_or(u32::MAX))
    } else {
        PairSpec::AdjacentPairs
    };

    // First-party provider: keep the ergonomic whole-graph default (G9 opt-in);
    // a named nodeLabels/edgeTypes below still scopes it.
    let mut projection = GraphProjectionSpec {
        project_all: true,
        ..GraphProjectionSpec::default()
    };
    if let Some(serde_json::Value::Object(cfg)) = args.get(3) {
        GraphProjectionSpec::reject_scopes(cfg, "uni.algo.gcoverlap")?;
        if let Some(labels) = cfg.get("nodeLabels").and_then(serde_json::Value::as_array) {
            projection.node_labels = labels
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect();
        }
        if let Some(types) = cfg.get("edgeTypes").and_then(serde_json::Value::as_array) {
            projection.edge_types = types
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect();
        }
    }
    Ok(OverlapArgs {
        metric,
        spec,
        projection,
    })
}

impl AlgorithmProvider for GraphComputeOverlapProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "gcoverlap: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| FnError::new(0x801, "gcoverlap: host is not the algorithm bridge"))?;

        let args = parse_config(ctx.config_json)?;
        let projection = bridge.project_for_graph_compute(&args.projection);
        let (work_cap, arena_bytes) = bridge.graph_compute_caps();
        let deadline_ms = bridge.graph_compute_deadline_ms();

        let out_schema = Arc::new(Schema::new(self.signature.output_fields.clone()));
        let schema_for_batch = Arc::clone(&out_schema);
        let stream = futures::stream::once(async move {
            let graph = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("gcoverlap: {e}")))?;

            // Install the native-work budget: an explicit grant is authoritative
            // and may raise the ceiling; otherwise the size-derived default (§9).
            let budget = WorkBudget::resolve(
                work_cap,
                graph.vertex_count() as u64,
                graph.edge_count() as u64,
            );
            let arena = Arena::new(arena_bytes, super::DEFAULT_ARENA_MAX_HANDLES);
            let deadline_at = deadline_ms
                .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
            // srcId/dstId are real data columns, so no expected-columns contract.
            let mut session = AlgoSession::new(
                super::next_session_epoch()
                    .map_err(|e| DataFusionError::Execution(format!("gcoverlap: {e}")))?,
                budget,
                arena,
            )
            .with_deadline(deadline_at);
            let g = session.bind_graph(Arc::clone(&graph));

            let started = std::time::Instant::now();
            let map_incomplete = |e: FnError, session: &AlgoSession| {
                super::error::incomplete_tag_for(
                    &e,
                    "uni.algo.gcoverlap",
                    started.elapsed().as_millis() as u64,
                    0,
                    session.work_spent_units(),
                    session.work_budget_units(),
                    &session.trace_breadcrumbs(),
                )
                .map_or_else(
                    || DataFusionError::Execution(format!("gcoverlap: {e}")),
                    DataFusionError::Execution,
                )
            };
            let pairs = session
                .all_pairs_overlap(g, args.spec, args.metric)
                .map_err(|e| map_incomplete(e, &session))?;
            session
                .emit_pairs(pairs)
                .map_err(|e| map_incomplete(e, &session))?;
            let rows = session.take_emitted_pairs();

            let mut src = Vec::with_capacity(rows.len());
            let mut dst = Vec::with_capacity(rows.len());
            let mut val = Vec::with_capacity(rows.len());
            for (s, d, v) in rows {
                src.push(s);
                dst.push(d);
                val.push(v);
            }
            RecordBatch::try_new(
                schema_for_batch,
                vec![
                    Arc::new(Int64Array::from(src)),
                    Arc::new(Int64Array::from(dst)),
                    Arc::new(Float64Array::from(val)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
