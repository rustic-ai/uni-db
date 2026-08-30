// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Vector / FTS / hybrid search procedure bodies, relocated from
//! `procedure_call.rs` during the M4 vector/fts/search port.
//!
//! Each `run_*` function takes a [`QueryProcedureHost`] (the snapshot
//! wrapper) rather than a `&GraphExecutionContext`, so the
//! `procedures_plugin/{vector,fts,search}.rs` impls can call them from
//! `ProcedurePlugin::invoke` after downcasting `ctx.host`. The legacy
//! match arms in `procedure_call.rs::execute_procedure` are deleted
//! once the plugin registrations land.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    Float32Builder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DFResult;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::DistanceMetric;

use crate::query::df_graph::common::{arrow_err, calculate_score, exec_err};
use crate::query::df_graph::procedure_call::{
    create_empty_batch, extract_optional_filter, map_yield_to_canonical, require_string_arg,
};
use crate::query::df_graph::scan::resolve_property_type;
use crate::query::executor::procedure_host::QueryProcedureHost;
use uni_query_functions::similar_to::normalize_bm25;

// Rust guideline compliant

// ---------------------------------------------------------------------------
// Argument helpers (kept here because vector/fts/search are the only callers)
// ---------------------------------------------------------------------------

pub(crate) fn extract_optional_threshold(args: &[Value], index: usize) -> Option<f64> {
    args.get(index)
        .and_then(|v| if v.is_null() { None } else { v.as_f64() })
}

pub(crate) fn require_int_arg(args: &[Value], index: usize, description: &str) -> DFResult<usize> {
    args.get(index)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "{description} must be an integer"
            ))
        })
}

/// Extract a vector from a `Value` (either `Value::Vector` or a list of
/// numbers).
pub(crate) fn extract_vector(val: &Value) -> DFResult<Vec<f32>> {
    match val {
        Value::Vector(vec) => Ok(vec.clone()),
        Value::List(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                if let Some(f) = v.as_f64() {
                    out.push(f as f32);
                } else {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Query vector must contain numbers".to_string(),
                    ));
                }
            }
            Ok(out)
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Query vector must be a list or vector".to_string(),
        )),
    }
}

/// Extract a multi-vector (a list of token vectors) from a `Value`, for
/// late-interaction (ColBERT / MaxSim) queries and document properties.
///
/// Accepts a `Value::List` whose elements are each a vector (`Value::Vector`
/// or a list of numbers, via [`extract_vector`]).
pub(crate) fn extract_vector_list(val: &Value) -> DFResult<Vec<Vec<f32>>> {
    match val {
        Value::List(arr) => arr.iter().map(extract_vector).collect(),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Multi-vector query must be a list of vectors".to_string(),
        )),
    }
}

/// Parse a distance-metric name (case-insensitive); defaults to `Cosine`.
fn parse_distance_metric(s: &str) -> DistanceMetric {
    match s.to_ascii_lowercase().as_str() {
        "dot" => DistanceMetric::Dot,
        "l2" | "euclidean" => DistanceMetric::L2,
        _ => DistanceMetric::Cosine,
    }
}

/// Parse the `nprobes` / `refine_factor` / `ef_search` ANN tuning knobs from a
/// procedure's `options` map (`None` for any = Lance default). Applies to dense
/// and multi-vector queries alike. `ef_search` accepts `ef` as an alias and sets
/// the HNSW search-time beam width (candidate list size).
fn parse_vector_query_opts(
    options_map: Option<&HashMap<String, Value>>,
) -> uni_store::VectorQueryOpts {
    let nprobes = options_map
        .and_then(|m| m.get("nprobes"))
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);
    let refine_factor = options_map
        .and_then(|m| m.get("refine_factor"))
        .and_then(|v| v.as_u64())
        .map(|r| r as u32);
    let ef = options_map
        .and_then(|m| m.get("ef_search").or_else(|| m.get("ef")))
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);
    uni_store::VectorQueryOpts {
        nprobes,
        refine_factor,
        ef,
    }
}

/// Detects whether a property is a multi-vector (`List<FixedSizeList<Float32>>`)
/// column, given the label's property metadata.
fn is_multivector_property(
    property: &str,
    label_props: Option<&HashMap<String, uni_common::core::schema::PropertyMeta>>,
) -> bool {
    matches!(
        resolve_property_type(property, label_props),
        arrow_schema::DataType::List(ref inner)
            if matches!(inner.data_type(), arrow_schema::DataType::FixedSizeList(_, _))
    )
}

// ---------------------------------------------------------------------------
// Reranker configuration + per-call reranker context
// ---------------------------------------------------------------------------

/// Sentinel reranker alias selecting in-process MaxSim (late-interaction /
/// ColBERT) scoring instead of a neural cross-encoder model.
pub(super) const MAXSIM_RERANKER: &str = "maxsim";

/// Configuration for an optional reranking stage.
///
/// When `alias` equals [`MAXSIM_RERANKER`], scoring is in-process MaxSim over a
/// stored multi-vector property (`maxsim_query` is the per-token query and
/// `metric` its similarity metric); otherwise it is a neural cross-encoder.
pub(super) struct RerankerConfig {
    pub alias: String,
    pub property: String,
    pub k: usize,
    pub query_override: Option<String>,
    /// MaxSim query multi-vector; `Some` only for the [`MAXSIM_RERANKER`] alias.
    pub maxsim_query: Option<Vec<Vec<f32>>>,
    /// Similarity metric for MaxSim scoring (default `Cosine`).
    pub metric: DistanceMetric,
}

pub(super) fn parse_reranker_options(
    options_map: Option<&HashMap<String, Value>>,
    k: usize,
    default_text_property: Option<&str>,
) -> Option<RerankerConfig> {
    let map = options_map?;
    let alias = map.get("reranker")?.as_str()?.to_string();
    let property = map
        .get("reranker_property")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| default_text_property.map(String::from))
        .unwrap_or_default();
    // Rerank at least `k` candidates and at most 1000, but never below `k`.
    // `Ord::clamp` panics when `min > max`, so a search `k > 1000` would abort;
    // compose `max`/`min` (with an upper bound that can never fall below `k`)
    // to stay panic-free for every `k`.
    let upper = k.max(1000);
    let reranker_k = map
        .get("reranker_k")
        .and_then(|v| v.as_u64())
        .map(|v| (v as usize).max(k).min(upper))
        .unwrap_or_else(|| k.saturating_mul(3).min(upper));
    let query_override = map
        .get("reranker_query")
        .and_then(|v| v.as_str())
        .map(String::from);
    // MaxSim mode: parse the per-token query multi-vector and metric. The query
    // is parsed best-effort here; a missing/malformed query is reported as a
    // hard error at rerank time so a requested maxsim never silently no-ops.
    let (maxsim_query, metric) = if alias == MAXSIM_RERANKER {
        let q = map
            .get("maxsim_query")
            .and_then(|v| extract_vector_list(v).ok());
        let m = map
            .get("maxsim_metric")
            .and_then(|v| v.as_str())
            .map(parse_distance_metric)
            .unwrap_or(DistanceMetric::Cosine);
        (q, m)
    } else {
        (None, DistanceMetric::Cosine)
    };
    Some(RerankerConfig {
        alias,
        property,
        k: reranker_k,
        query_override,
        maxsim_query,
        metric,
    })
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

/// Context from a cross-encoder reranking stage.
pub(super) struct RerankContext {
    pub scores: HashMap<Vid, f32>,
    pub props: HashMap<Vid, uni_common::Properties>,
}

/// Run the optional cross-encoder rerank stage over retrieval results.
///
/// With no `reranker_config` the results pass through untouched. Otherwise the
/// reranker query is `query_override` when set, else `fallback_query`.
async fn apply_rerank(
    host: &QueryProcedureHost,
    results: Vec<(Vid, f32)>,
    label: &str,
    reranker_config: Option<&RerankerConfig>,
    fallback_query: &str,
    k: usize,
) -> DFResult<(Vec<(Vid, f32)>, Option<RerankContext>)> {
    let Some(rcfg) = reranker_config else {
        return Ok((results, None));
    };
    let reranker_query = rcfg.query_override.as_deref().unwrap_or(fallback_query);
    let (reranked, ctx) = rerank_candidates(host, results, label, reranker_query, rcfg, k).await?;
    Ok((reranked, Some(ctx)))
}

async fn rerank_candidates(
    host: &QueryProcedureHost,
    candidates: Vec<(Vid, f32)>,
    label: &str,
    query_text: &str,
    config: &RerankerConfig,
    k: usize,
) -> DFResult<(Vec<(Vid, f32)>, RerankContext)> {
    let vids: Vec<Vid> = candidates.iter().map(|(v, _)| *v).collect();

    let property_manager = host.property_manager().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot rerank: property manager not available on host".to_string(),
        )
    })?;
    let query_ctx = host.query_context();
    let props_map = property_manager
        .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
        .await
        .map_err(exec_err)?;

    // MaxSim (late-interaction / ColBERT) rerank: no neural model — score each
    // candidate's stored multi-vector property against the query multi-vector
    // in-process. Pure CPU; `reranker_k` (clamped <= 1000) bounds the work.
    if config.alias == MAXSIM_RERANKER {
        let query = config.maxsim_query.as_ref().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "maxsim reranker requires a valid 'maxsim_query' option (a list of vectors)"
                    .to_string(),
            )
        })?;
        let mut scored: Vec<(Vid, f32)> = Vec::with_capacity(vids.len());
        for vid in &vids {
            // Missing/empty multi-vector property -> no document tokens -> score 0.
            let doc_tokens = props_map
                .get(vid)
                .and_then(|p| p.get(&config.property))
                .map(extract_vector_list)
                .transpose()?
                .unwrap_or_default();
            let score = uni_query_functions::similar_to::maxsim(query, &doc_tokens, &config.metric)
                .map_err(exec_err)?;
            scored.push((*vid, score));
        }
        let rerank_map: HashMap<Vid, f32> = scored.iter().copied().collect();
        let mut reranked = scored;
        reranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                // Deterministic tie-break by ascending vid, matching the DAAT storage path
                // (`HeapEntry`), so top-k *membership* at a score tie is stable rather than
                // dependent on HashSet/HashMap candidate iteration order (issue #95).
                .then_with(|| a.0.as_u64().cmp(&b.0.as_u64()))
        });
        reranked.truncate(k);
        return Ok((
            reranked,
            RerankContext {
                scores: rerank_map,
                props: props_map,
            },
        ));
    }

    let doc_texts: Vec<String> = vids
        .iter()
        .map(|vid| {
            props_map
                .get(vid)
                .and_then(|p| p.get(&config.property))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();

    let runtime = host.xervo_runtime().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot rerank: Uni-Xervo runtime not configured".to_string(),
        )
    })?;
    let reranker = runtime.reranker(&config.alias).await.map_err(exec_err)?;
    let doc_refs: Vec<&str> = doc_texts.iter().map(|s| s.as_str()).collect();
    let scored = reranker.rerank(query_text, &doc_refs).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Reranker inference failed: {e}"))
    })?;

    let mut reranked: Vec<(Vid, f32)> = scored
        .iter()
        .map(|sd| (vids[sd.index], sigmoid(sd.score)))
        .collect();
    reranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            // Deterministic tie-break by ascending vid, matching the DAAT storage path
            // (`HeapEntry`), so top-k *membership* at a score tie is stable rather than
            // dependent on HashSet/HashMap candidate iteration order (issue #95).
            .then_with(|| a.0.as_u64().cmp(&b.0.as_u64()))
    });
    reranked.truncate(k);

    let rerank_map: HashMap<Vid, f32> = scored
        .iter()
        .map(|sd| (vids[sd.index], sigmoid(sd.score)))
        .collect();

    Ok((
        reranked,
        RerankContext {
            scores: rerank_map,
            props: props_map,
        },
    ))
}

/// Default Lance candidate over-fetch multiplier for native multi-vector
/// (ColBERT / MaxSim) first-stage retrieval. Lance ANN over the flushed set is
/// only a *candidate generator*; the exact MaxSim re-rank picks the true top-k,
/// so we pull a few times `k` to preserve recall.
pub(crate) const MULTIVECTOR_OVER_FETCH: usize = 4;
/// Hard cap on the number of candidates re-scored in-process, bounding CPU.
const MULTIVECTOR_MAX_CANDIDATES: usize = 1000;

/// First-stage native multi-vector (ColBERT / MaxSim) retrieval **with L0
/// visibility**.
///
/// Single-vector search merges unflushed L0 rows into Lance's ranking directly,
/// because the in-process distance is on the identical scale as Lance's
/// `_distance`. Multi-vector cannot: Lance's `_distance` is an opaque internal
/// aggregate whose scale cannot be matched against an in-process MaxSim. So this
/// treats Lance as a pure **candidate generator** over flushed/indexed data,
/// unions its hits with the live L0 vids for the label (minus tombstones), and
/// re-scores *every* candidate in-process with exact MaxSim. Flushed and
/// unflushed rows therefore share one consistent, exact ranking, and a query
/// sees recent writes without an explicit `flush()`.
///
/// Returns the top-`k` `(vid, maxsim_similarity)` (higher = better) plus the
/// fetched property map, which the caller reuses to materialise node columns
/// without a second fetch. `retrieval_k` is the Lance over-fetch count.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn multivector_rerank(
    storage: &uni_store::storage::StorageManager,
    property_manager: &uni_store::PropertyManager,
    query_ctx: &uni_store::QueryContext,
    label: &str,
    property: &str,
    query: &[Vec<f32>],
    k: usize,
    retrieval_k: usize,
    filter: Option<&str>,
    opts: uni_store::VectorQueryOpts,
    metric: &DistanceMetric,
) -> DFResult<(Vec<(Vid, f32)>, HashMap<Vid, uni_common::Properties>)> {
    // 1. Candidate generation over flushed/indexed data.
    //
    //    MUVERA fast path (main line only): if the property has a MUVERA index, encode
    //    the query into a Fixed-Dimensional Encoding and run the single-vector ANN over
    //    the derived `__fde_*` column (Dot metric). This replaces the heavier native
    //    multi-vector ANN with a fast single-vector ANN; the exact MaxSim re-rank below
    //    is unchanged. On forks we keep the native brute-force branch scan (the FDE index
    //    isn't branched), and `multivector_search` also handles L0-only corpora.
    let lance_hits = {
        let muvera_hits = if storage.fork_scope().is_none() {
            let schema = storage.schema_manager().schema();
            schema
                .vector_index_for_property(label, property)
                .and_then(|cfg| uni_store::storage::muvera_index::fde_spec_for_config(&schema, cfg))
        } else {
            None
        };
        match muvera_hits {
            Some(spec) => {
                let encoder =
                    uni_common::muvera::FdeEncoder::new(&spec.params).map_err(exec_err)?;
                let fde_q = encoder.encode_query(query).map_err(exec_err)?;
                storage
                    .muvera_fde_candidates(
                        label,
                        &spec.derived_col,
                        &fde_q,
                        retrieval_k,
                        filter,
                        opts,
                        Some(query_ctx),
                    )
                    .await
                    .map_err(exec_err)?
            }
            None => storage
                .multivector_search(
                    label,
                    property,
                    query,
                    retrieval_k,
                    filter,
                    opts,
                    Some(query_ctx),
                )
                .await
                .map_err(exec_err)?,
        }
    };

    // 2. Live L0 candidates (and tombstones) for the label.
    let (l0_live, tombstoned) = uni_store::collect_l0_label_candidates(query_ctx, label);

    // 3. Union(Lance, L0) minus tombstoned, deduped, capped.
    let mut seen: std::collections::HashSet<Vid> = std::collections::HashSet::new();
    let mut candidates: Vec<Vid> = Vec::new();
    for (vid, _) in &lance_hits {
        if !tombstoned.contains(vid) && seen.insert(*vid) {
            candidates.push(*vid);
        }
    }
    for vid in l0_live {
        if !tombstoned.contains(&vid) && seen.insert(vid) {
            candidates.push(vid);
        }
    }
    // On a fork the candidate set is the full branch scan (incl. inherited
    // rows) unordered, so truncating would silently drop recall — score them
    // all (brute-force). On the main path Lance pre-limits to `retrieval_k`, so
    // the cap is just a safety net.
    if storage.fork_scope().is_none() {
        candidates.truncate(MULTIVECTOR_MAX_CANDIDATES);
    }

    // 4. Fetch token properties for all candidates (L0 + Lance merged, tombstone
    //    aware).
    let props_map = property_manager
        .get_batch_vertex_props_for_label(&candidates, label, Some(query_ctx))
        .await
        .map_err(exec_err)?;

    // 5. Exact in-process MaxSim re-score. A vid absent from `props_map` is not
    //    visible (filtered by tombstone / version) and is dropped; a vid present
    //    but missing the property has no document tokens and scores 0 (matching
    //    the cross-encoder MaxSim rerank path). A dimension mismatch propagates
    //    as a hard error.
    let mut scored: Vec<(Vid, f32)> = Vec::with_capacity(candidates.len());
    for vid in &candidates {
        let Some(props) = props_map.get(vid) else {
            continue;
        };
        let doc_tokens = match props.get(property) {
            Some(v) => extract_vector_list(v)?,
            None => Vec::new(),
        };
        let score = uni_query_functions::similar_to::maxsim(query, &doc_tokens, metric)
            .map_err(exec_err)?;
        scored.push((*vid, score));
    }

    // 6. Top-k by similarity (higher = better).
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            // Deterministic tie-break by ascending vid, matching the DAAT storage path
            // (`HeapEntry`), so top-k *membership* at a score tie is stable rather than
            // dependent on HashSet/HashMap candidate iteration order (issue #95).
            .then_with(|| a.0.as_u64().cmp(&b.0.as_u64()))
    });
    scored.truncate(k);

    Ok((scored, props_map))
}

/// Scored sparse-vector retrieval with exact dot-product re-scoring and L0
/// union — the sparse analogue of [`multivector_rerank`]. The sparse index is a
/// flushed-only candidate generator; this helper unions live L0 rows, fetches
/// properties MVCC/tombstone-aware, and re-scores *every* candidate exactly via
/// `sparse_dot`, so a query sees recent writes (and never a tombstoned/stale
/// row) without an explicit `flush()`.
///
/// Returns the top-`k` `(vid, dot_score)` (higher = better) plus the fetched
/// property map for node materialisation. `retrieval_k` is the index over-fetch.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn sparse_rerank(
    storage: &uni_store::storage::StorageManager,
    property_manager: &uni_store::PropertyManager,
    query_ctx: &uni_store::QueryContext,
    label: &str,
    property: &str,
    query: &uni_sparse_vector::SparseVector,
    k: usize,
    retrieval_k: usize,
) -> DFResult<(Vec<(Vid, f32)>, HashMap<Vid, uni_common::Properties>)> {
    // 1. Flushed candidate generation via the sparse index (term-matching vids).
    let query_pairs: Vec<(u32, f32)> = query.iter().collect();
    let flushed = storage
        .sparse_search(label, property, &query_pairs, retrieval_k)
        .await
        .map_err(exec_err)?;

    // 2. Live L0 candidates (and tombstones) for the label.
    let (l0_live, tombstoned) = uni_store::collect_l0_label_candidates(query_ctx, label);

    // 3. Union(flushed, L0) minus tombstoned, deduped.
    let mut seen: std::collections::HashSet<Vid> = std::collections::HashSet::new();
    let mut candidates: Vec<Vid> = Vec::new();
    for (vid, _) in &flushed {
        if !tombstoned.contains(vid) && seen.insert(*vid) {
            candidates.push(*vid);
        }
    }
    for vid in l0_live {
        if !tombstoned.contains(&vid) && seen.insert(vid) {
            candidates.push(vid);
        }
    }

    // 4. Fetch properties for all candidates (MVCC/tombstone aware: a vid that
    //    is not visible under this snapshot is absent from `props_map`).
    let props_map = property_manager
        .get_batch_vertex_props_for_label(&candidates, label, Some(query_ctx))
        .await
        .map_err(exec_err)?;

    // 5. Exact dot re-score. Absent vid → not visible → dropped. Present but
    //    missing the property → no document terms → score 0. The fetched
    //    property is the latest (L0-merged) value, so an L0 update re-scores
    //    against the new weights even though the stale flushed posting matched.
    let mut scored: Vec<(Vid, f32)> = Vec::with_capacity(candidates.len());
    for vid in &candidates {
        let Some(props) = props_map.get(vid) else {
            continue;
        };
        let score = match props.get(property) {
            Some(uni_common::Value::SparseVector { indices, values }) => {
                match uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()) {
                    Ok(doc) => uni_sparse_vector::ops::sparse_dot(query, &doc),
                    Err(_) => 0.0,
                }
            }
            _ => 0.0,
        };
        // A zero score means no query-term overlap — not a sparse match. Dropping
        // it keeps the L0 brute-force path and the flushed-index path (which only
        // surfaces term-overlapping docs via its `term_id IN (...)` filter)
        // returning the same set, instead of the L0 path padding top-k with
        // irrelevant zero-overlap docs.
        if score > 0.0 {
            scored.push((*vid, score));
        }
    }

    // 6. Top-k by similarity (higher = better).
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            // Deterministic tie-break by ascending vid, matching the DAAT storage path
            // (`HeapEntry`), so top-k *membership* at a score tie is stable rather than
            // dependent on HashSet/HashMap candidate iteration order (issue #95).
            .then_with(|| a.0.as_u64().cmp(&b.0.as_u64()))
    });
    scored.truncate(k);
    Ok((scored, props_map))
}

/// Parse the `query` argument of `uni.sparse.query` into a [`SparseVector`].
/// Accepts a `Value::SparseVector` (the typical query-parameter form) or a map
/// `{indices: [...], values: [...]}`.
fn extract_sparse_query(val: &Value) -> DFResult<uni_sparse_vector::SparseVector> {
    use datafusion::error::DataFusionError;
    match val {
        Value::SparseVector { indices, values } => {
            uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).map_err(|e| {
                DataFusionError::Execution(format!("uni.sparse.query: invalid query vector: {e}"))
            })
        }
        Value::Map(m) => {
            let list = |key: &str| -> DFResult<&Vec<Value>> {
                match m.get(key) {
                    Some(Value::List(l)) => Ok(l),
                    _ => Err(DataFusionError::Execution(format!(
                        "uni.sparse.query: query map missing '{key}' list"
                    ))),
                }
            };
            let idx_list = list("indices")?;
            let val_list = list("values")?;
            let indices: Vec<u32> = idx_list
                .iter()
                .map(|v| v.as_i64().map(|i| i as u32))
                .collect::<Option<_>>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "uni.sparse.query: 'indices' must be integers".to_string(),
                    )
                })?;
            let values: Vec<f32> = val_list
                .iter()
                .map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Option<_>>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "uni.sparse.query: 'values' must be numbers".to_string(),
                    )
                })?;
            if indices.len() != values.len() {
                return Err(DataFusionError::Execution(
                    "uni.sparse.query: 'indices' and 'values' length mismatch".to_string(),
                ));
            }
            uni_sparse_vector::SparseVector::from_pairs(
                indices.into_iter().zip(values).collect(),
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("uni.sparse.query: invalid query vector: {e}"))
            })
        }
        _ => Err(DataFusionError::Execution(
            "uni.sparse.query: third argument (query) must be a sparse vector or {indices,values} map"
                .to_string(),
        )),
    }
}

/// `uni.sparse.query(label, property, query, k, filter?, threshold?, options?)`.
pub(crate) async fn run_sparse_query(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.sparse.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.sparse.query: second argument (property)")?;
    let query_val = args.get(2).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.sparse.query: third argument (query) is required".to_string(),
        )
    })?;
    // A string query is auto-embedded via the sparse index's configured model;
    // otherwise it's an explicit sparse vector / {indices,values} map.
    let query = match query_val {
        Value::String(text) => auto_embed_sparse_text(host, &label, &property, text).await?,
        other => extract_sparse_query(other)?,
    };
    let k = require_int_arg(args, 3, "uni.sparse.query: fourth argument (k)")?;
    // `filter` is accepted for API symmetry; MVCC/tombstone visibility is
    // already enforced by the property fetch in `sparse_rerank`.
    // `uni.sparse.query` does not yet scope candidates by a user predicate (only MVCC /
    // tombstone visibility from the property fetch applies). Reject a non-null `filter`
    // explicitly rather than silently ignoring it, so a caller is not misled into
    // believing results are constrained (issue #95; the filtered/hybrid surface is #114).
    if extract_optional_filter(args, 4).is_some() {
        return Err(datafusion::error::DataFusionError::Execution(
            "uni.sparse.query: the `filter` argument is not yet supported — results are not \
             scoped by it. Omit it, or pre-filter via a hybrid/Cypher query."
                .to_string(),
        ));
    }
    let threshold = extract_optional_threshold(args, 5);
    let options_map = args
        .get(6)
        .and_then(|v| if v.is_null() { None } else { v.as_object() });
    let over_fetch = options_map
        .and_then(|m| m.get("over_fetch"))
        .and_then(|v| v.as_f64())
        .filter(|f| *f >= 1.0)
        .unwrap_or(MULTIVECTOR_OVER_FETCH as f64);
    let retrieval_k = (((k as f64) * over_fetch).ceil() as usize).max(k);

    let storage = host.storage();
    let query_ctx = host.query_context();
    let property_manager = host.property_manager().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot run sparse query: property manager not available on host".to_string(),
        )
    })?;

    let (mut results, props) = sparse_rerank(
        storage,
        property_manager,
        &query_ctx,
        &label,
        &property,
        &query,
        k,
        retrieval_k,
    )
    .await?;

    if let Some(min_score) = threshold {
        results.retain(|(_, s)| *s >= min_score as f32);
    }
    if results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    // Emit the exact dot score as `score` and reuse the fetched props for node
    // materialisation by routing through the rerank context (bypasses
    // `calculate_score`). The metric is cosmetic on this path.
    let metric = DistanceMetric::Cosine;
    let rerank_ctx = RerankContext {
        scores: results.iter().copied().collect(),
        props,
    };
    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: Some(&rerank_ctx),
    };
    build_search_result_batch(
        &results,
        &label,
        RetrievalScore::Distance(&metric),
        &batch_ctx,
    )
    .await
}

// ---------------------------------------------------------------------------
// Auto-embed
// ---------------------------------------------------------------------------

async fn auto_embed_text(
    host: &QueryProcedureHost,
    label: &str,
    property: &str,
    query_text: &str,
) -> DFResult<Vec<f32>> {
    let storage = host.storage();
    let uni_schema = storage.schema_manager().schema();
    let index_config = uni_schema.vector_index_for_property(label, property);

    let embedding_config = index_config
        .and_then(|cfg| cfg.embedding_config.as_ref())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Cannot auto-embed: vector index for {label}.{property} has no embedding_config. \
                 Either provide a pre-computed vector or create the index with embedding options."
            ))
        })?;

    let runtime = host.xervo_runtime().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot auto-embed: Uni-Xervo runtime not configured".to_string(),
        )
    })?;

    let embedder = runtime
        .embedding(&embedding_config.alias)
        .await
        .map_err(exec_err)?;

    let prefixed_query = match &embedding_config.query_prefix {
        Some(prefix) => format!("{prefix}{query_text}"),
        None => query_text.to_string(),
    };

    let embeddings = embedder
        .embed(&[prefixed_query.as_str()])
        .await
        .map_err(exec_err)?
        .vectors;
    embeddings.into_iter().next().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Embedding service returned no results".to_string(),
        )
    })
}

/// Embed a text query into a `SparseVector` via the sparse index's configured
/// xervo model. The sparse encoder is symmetric (no `query_prefix`), unlike the
/// dense path.
async fn auto_embed_sparse_text(
    host: &QueryProcedureHost,
    label: &str,
    property: &str,
    query_text: &str,
) -> DFResult<uni_sparse_vector::SparseVector> {
    let storage = host.storage();
    let uni_schema = storage.schema_manager().schema();
    let embedding_config = uni_schema
        .sparse_index_for_property(label, property)
        .and_then(|cfg| cfg.embedding_config.clone())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Cannot auto-embed: sparse index for {label}.{property} has no embedding_config. \
                 Either pass a sparse vector or create the index with embedding options."
            ))
        })?;

    let runtime = host.xervo_runtime().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot auto-embed: Uni-Xervo runtime not configured".to_string(),
        )
    })?;

    let embedder = runtime
        .sparse_embedder(&embedding_config.alias)
        .await
        .map_err(exec_err)?;
    let pairs = embedder
        .embed(&[query_text])
        .await
        .map_err(exec_err)?
        .vectors
        .into_iter()
        .next()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Sparse embedding service returned no results".to_string(),
            )
        })?;
    uni_sparse_vector::SparseVector::from_pairs(pairs).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Sparse embedding produced an invalid vector: {e}"
        ))
    })
}

// ---------------------------------------------------------------------------
// Batch builders
// ---------------------------------------------------------------------------

pub(super) struct HybridScoreContext<'a> {
    pub vec_score_map: &'a HashMap<Vid, f32>,
    pub fts_score_map: &'a HashMap<Vid, f32>,
    /// Raw sparse dot scores by vid (empty when `uni.search` has no `sparse`
    /// property). Reported unnormalized, mirroring `uni.sparse.query`'s `score`.
    pub sparse_score_map: &'a HashMap<Vid, f32>,
    pub fts_max: f32,
    pub metric: &'a DistanceMetric,
}

pub(super) struct BatchBuildCtx<'a> {
    pub yield_items: &'a [(String, Option<String>)],
    pub target_properties: &'a HashMap<String, Vec<String>>,
    pub host: &'a QueryProcedureHost,
    pub schema: &'a SchemaRef,
    pub rerank_ctx: Option<&'a RerankContext>,
}

/// Columnar properties the node yield(s) will actually emit: the union of
/// `target_properties[output]` over every `node`-canonical yield.
///
/// `build_node_yield_columns` emits exactly these props, so restricting the
/// property fetch to this set is lossless while skipping unread heavy columns
/// (issue #134). Returns `Some(empty)` when no node yield narrows to any prop
/// (nothing is emitted, so nothing need be fetched).
fn node_yield_requested_props(batch_ctx: &BatchBuildCtx<'_>) -> Option<Vec<String>> {
    let mut requested: Vec<String> = Vec::new();
    for (name, alias) in batch_ctx.yield_items {
        if map_yield_to_canonical(name) != "node" {
            continue;
        }
        let output_name = alias.as_ref().unwrap_or(name);
        if let Some(props) = batch_ctx.target_properties.get(output_name) {
            for p in props {
                if !requested.contains(p) {
                    requested.push(p.clone());
                }
            }
        }
    }
    Some(requested)
}

/// Load the vertex properties the node yield(s) need, or an empty map when
/// none are needed.
///
/// Returns empty when a rerank stage already materialised the properties (the
/// caller uses `RerankContext::props` instead) or when no yield is
/// node-canonical.
async fn hydrate_node_props(
    vids: &[Vid],
    label: &str,
    batch_ctx: &BatchBuildCtx<'_>,
) -> DFResult<HashMap<Vid, uni_common::Properties>> {
    let has_node_yield = batch_ctx
        .yield_items
        .iter()
        .any(|(name, _)| map_yield_to_canonical(name) == "node");
    if batch_ctx.rerank_ctx.is_some() || !has_node_yield {
        return Ok(HashMap::new());
    }

    let property_manager = batch_ctx.host.property_manager().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "Cannot materialise node properties: property manager not available on host"
                .to_string(),
        )
    })?;
    // Only fetch the properties actually emitted for the node yield(s) —
    // `build_node_yield_columns` emits exactly `target_properties[output]`,
    // so pruning the Lance fetch to that set is lossless and avoids decoding
    // unread heavy columns such as `List(Vector)` (issue #134).
    let requested = node_yield_requested_props(batch_ctx);
    let query_ctx = batch_ctx.host.query_context();
    property_manager
        .get_batch_vertex_props_for_label_projected(
            vids,
            label,
            Some(&query_ctx),
            requested.as_deref(),
        )
        .await
        .map_err(exec_err)
}

fn build_node_yield_columns(
    vids: &[Vid],
    label: &str,
    output_name: &str,
    target_properties: &HashMap<String, Vec<String>>,
    props_map: &HashMap<Vid, uni_common::Properties>,
    label_props: Option<&std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>>,
) -> DFResult<Vec<ArrayRef>> {
    let num_rows = vids.len();
    let mut columns = Vec::new();

    let mut vid_builder = UInt64Builder::with_capacity(num_rows);
    for vid in vids {
        vid_builder.append_value(vid.as_u64());
    }
    columns.push(Arc::new(vid_builder.finish()) as ArrayRef);

    let mut var_builder = StringBuilder::with_capacity(num_rows, num_rows * 20);
    for vid in vids {
        var_builder.append_value(vid.to_string());
    }
    columns.push(Arc::new(var_builder.finish()) as ArrayRef);

    let mut labels_builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
    for _ in 0..num_rows {
        labels_builder.values().append_value(label);
        labels_builder.append(true);
    }
    columns.push(Arc::new(labels_builder.finish()) as ArrayRef);

    if let Some(props) = target_properties.get(output_name) {
        for prop_name in props {
            let data_type = resolve_property_type(prop_name, label_props);
            let column = crate::query::df_graph::scan::build_property_column_static(
                vids, props_map, prop_name, &data_type,
            )
            .map_err(exec_err)?;
            columns.push(column);
        }
    }

    Ok(columns)
}

/// How the raw retrieval number attached to each hit becomes the yielded `score`.
///
/// Vector-style retrieval returns a *distance* (smaller is better), which
/// [`calculate_score`] inverts into a similarity. Full-text retrieval returns a
/// *BM25 relevance* (larger is better), which must be squashed with
/// [`normalize_bm25`] instead — pushing BM25 through the L2 arm of
/// `calculate_score` yields `1/(1+bm25)`, which is strictly *decreasing* in
/// relevance and silently inverts `ORDER BY score DESC`.
#[derive(Clone, Copy)]
pub(crate) enum RetrievalScore<'a> {
    /// Raw value is a distance under `metric`; smaller is better.
    Distance(&'a DistanceMetric),
    /// Raw value is a BM25 relevance; larger is better.
    Bm25 { k: f32 },
}

impl RetrievalScore<'_> {
    /// Map one raw retrieval value onto the yielded `score` scale, where
    /// larger always means "better match".
    pub(crate) fn to_score(self, raw: f32) -> f32 {
        match self {
            RetrievalScore::Distance(metric) => calculate_score(raw, metric),
            RetrievalScore::Bm25 { k } => normalize_bm25(raw, k),
        }
    }
}

async fn build_search_result_batch(
    results: &[(Vid, f32)],
    label: &str,
    scoring: RetrievalScore<'_>,
    batch_ctx: &BatchBuildCtx<'_>,
) -> DFResult<Option<RecordBatch>> {
    let num_rows = results.len();
    let vids: Vec<Vid> = results.iter().map(|(vid, _)| *vid).collect();
    let distances: Vec<f32> = results.iter().map(|(_, d)| *d).collect();

    let retrieval_scores: Vec<f32> = distances.iter().map(|raw| scoring.to_score(*raw)).collect();

    let uni_schema = batch_ctx.host.storage().schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    let owned_props = hydrate_node_props(&vids, label, batch_ctx).await?;
    let props_map = batch_ctx.rerank_ctx.map_or(&owned_props, |r| &r.props);

    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, alias) in batch_ctx.yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical {
            "node" => {
                columns.extend(build_node_yield_columns(
                    &vids,
                    label,
                    output_name,
                    batch_ctx.target_properties,
                    props_map,
                    label_props,
                )?);
            }
            "distance" => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for dist in &distances {
                    builder.append_value(*dist as f64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for (i, vid) in vids.iter().enumerate() {
                    let score = batch_ctx
                        .rerank_ctx
                        .and_then(|rctx| rctx.scores.get(vid).copied())
                        .unwrap_or(retrieval_scores[i]);
                    builder.append_value(score);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "rerank_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    match batch_ctx.rerank_ctx.and_then(|rctx| rctx.scores.get(vid)) {
                        Some(&s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(batch_ctx.schema.clone(), columns).map_err(arrow_err)?;
    Ok(Some(batch))
}

async fn build_hybrid_search_batch(
    results: &[(Vid, f32)],
    scores: &HybridScoreContext<'_>,
    label: &str,
    batch_ctx: &BatchBuildCtx<'_>,
) -> DFResult<Option<RecordBatch>> {
    let num_rows = results.len();
    let vids: Vec<Vid> = results.iter().map(|(vid, _)| *vid).collect();
    let fused_scores: Vec<f32> = results.iter().map(|(_, s)| *s).collect();

    let uni_schema = batch_ctx.host.storage().schema_manager().schema();
    let label_props = uni_schema.properties.get(label);

    let owned_props = hydrate_node_props(&vids, label, batch_ctx).await?;
    let props_map = batch_ctx.rerank_ctx.map_or(&owned_props, |r| &r.props);

    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, alias) in batch_ctx.yield_items {
        let output_name = alias.as_ref().unwrap_or(name);
        let canonical = map_yield_to_canonical(name);

        match canonical {
            "node" => {
                columns.extend(build_node_yield_columns(
                    &vids,
                    label,
                    output_name,
                    batch_ctx.target_properties,
                    props_map,
                    label_props,
                )?);
            }
            "vid" => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for vid in &vids {
                    builder.append_value(vid.as_u64() as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for (i, vid) in vids.iter().enumerate() {
                    let score = batch_ctx
                        .rerank_ctx
                        .and_then(|rctx| rctx.scores.get(vid).copied())
                        .unwrap_or(fused_scores[i]);
                    builder.append_value(score);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "rerank_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    match batch_ctx.rerank_ctx.and_then(|rctx| rctx.scores.get(vid)) {
                        Some(&s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "vector_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&dist) = scores.vec_score_map.get(vid) {
                        let score = calculate_score(dist, scores.metric);
                        builder.append_value(score);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "fts_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&raw_score) = scores.fts_score_map.get(vid) {
                        let norm = if scores.fts_max > 0.0 {
                            raw_score / scores.fts_max
                        } else {
                            0.0
                        };
                        builder.append_value(norm);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "sparse_score" => {
                let mut builder = Float32Builder::with_capacity(num_rows);
                for vid in &vids {
                    // Raw dot product, unnormalized — matches `uni.sparse.query`.
                    if let Some(&dot) = scores.sparse_score_map.get(vid) {
                        builder.append_value(dot);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            "distance" => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for vid in &vids {
                    if let Some(&dist) = scores.vec_score_map.get(vid) {
                        builder.append_value(dist as f64);
                    } else {
                        builder.append_null();
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, 0);
                for _ in 0..num_rows {
                    builder.append_null();
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let batch = RecordBatch::try_new(batch_ctx.schema.clone(), columns).map_err(arrow_err)?;
    Ok(Some(batch))
}

// ---------------------------------------------------------------------------
// Public entry points (called by procedures_plugin)
// ---------------------------------------------------------------------------

/// `uni.vector.query(label, property, query, k, filter?, threshold?, options?)`.
///
/// `k` is required. `threshold`, when given, is a **minimum similarity** on the
/// same scale as the yielded `score` (larger is a better match) — identical on
/// the dense, auto-embedded and multi-vector paths.
pub(crate) async fn run_vector_query(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.vector.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.vector.query: second argument (property)")?;

    let query_val = args.get(2).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.vector.query: third argument (query) is required".to_string(),
        )
    })?;

    let storage = host.storage();

    // First-stage multi-vector (ColBERT / MaxSim) retrieval: when the queried
    // property is a `List<Vector>` column, the query is a list of token vectors
    // and there is no cross-encoder rerank stage. (The dense + `reranker:'maxsim'`
    // path below is a different call shape: a dense ANN property reranked by a
    // separate multi-vector `reranker_property`.)
    let is_multivector = {
        let sch = storage.schema_manager().schema();
        is_multivector_property(&property, sch.properties.get(&label))
    };
    if is_multivector {
        let k = require_int_arg(args, 3, "uni.vector.query: fourth argument (k)")?;
        let filter = extract_optional_filter(args, 4);
        // The multi-vector `score` is an exact MaxSim *similarity* (higher is
        // better) and `threshold` is a minimum similarity — the same meaning
        // the dense path below now uses.
        let threshold = extract_optional_threshold(args, 5);
        let options_map = args
            .get(6)
            .and_then(|v| if v.is_null() { None } else { v.as_object() });
        let opts = parse_vector_query_opts(options_map);
        let queries = extract_vector_list(query_val)?;
        let query_ctx = host.query_context();

        // Default Cosine for multi-vector (ColBERT) when the property has no index.
        let metric = {
            let sch = storage.schema_manager().schema();
            sch.vector_index_for_property(&label, &property)
                .map(|config| config.metric.clone())
                .unwrap_or(DistanceMetric::Cosine)
        };

        // Lance is a candidate generator; over-fetch (`over_fetch` option, or a
        // default multiple of `k`) preserves recall before the exact re-rank.
        let over_fetch = options_map
            .and_then(|m| m.get("over_fetch"))
            .and_then(|v| v.as_f64())
            .filter(|f| *f >= 1.0)
            .unwrap_or(MULTIVECTOR_OVER_FETCH as f64);
        let retrieval_k = (((k as f64) * over_fetch).ceil() as usize).max(k);

        let property_manager = host.property_manager().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "Cannot run multi-vector query: property manager not available on host".to_string(),
            )
        })?;

        let (mut results, props) = multivector_rerank(
            storage,
            property_manager,
            &query_ctx,
            &label,
            &property,
            &queries,
            k,
            retrieval_k,
            filter.as_deref(),
            opts,
            &metric,
        )
        .await?;

        if let Some(min_sim) = threshold {
            results.retain(|(_, sim)| *sim >= min_sim as f32);
        }
        if results.is_empty() {
            return Ok(Some(create_empty_batch(schema.clone())?));
        }

        // Emit the exact MaxSim similarity as `score` (and reuse the fetched
        // props for node materialisation) by routing through the rerank context,
        // which bypasses `calculate_score`.
        let rerank_ctx = RerankContext {
            scores: results.iter().copied().collect(),
            props,
        };
        let batch_ctx = BatchBuildCtx {
            yield_items,
            target_properties,
            host,
            schema,
            rerank_ctx: Some(&rerank_ctx),
        };
        return build_search_result_batch(
            &results,
            &label,
            RetrievalScore::Distance(&metric),
            &batch_ctx,
        )
        .await;
    }

    let query_text_from_arg = query_val.as_str().map(String::from);
    let query_vector: Vec<f32> = if let Some(ref query_text) = query_text_from_arg {
        auto_embed_text(host, &label, &property, query_text).await?
    } else {
        extract_vector(query_val)?
    };

    let k = require_int_arg(args, 3, "uni.vector.query: fourth argument (k)")?;
    let filter = extract_optional_filter(args, 4);
    let threshold = extract_optional_threshold(args, 5);
    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| if v.is_null() { None } else { v.as_object() });
    let reranker_config = parse_reranker_options(options_map, k, None);
    let vec_opts = parse_vector_query_opts(options_map);

    if let Some(ref rcfg) = reranker_config {
        // MaxSim scores against `maxsim_query` (a multi-vector), not the text
        // query, so it is exempt from the cross-encoder's reranker_query rule.
        if rcfg.alias != MAXSIM_RERANKER
            && query_text_from_arg.is_none()
            && rcfg.query_override.is_none()
        {
            return Err(datafusion::error::DataFusionError::Execution(
                "Cannot rerank: query is a pre-computed vector. \
                 Provide reranker_query in options."
                    .to_string(),
            ));
        }
        if rcfg.property.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "reranker_property is required when using reranker with uni.vector.query"
                    .to_string(),
            ));
        }
    }

    let retrieval_k = reranker_config.as_ref().map_or(k, |rc| rc.k);
    let query_ctx = host.query_context();
    let mut results = storage
        .vector_search(
            &label,
            &property,
            &query_vector,
            retrieval_k,
            filter.as_deref(),
            vec_opts,
            Some(&query_ctx),
        )
        .await
        .map_err(exec_err)?;

    let schema_manager = storage.schema_manager();
    let uni_schema = schema_manager.schema();
    let metric = uni_schema
        .vector_index_for_property(&label, &property)
        .map(|config| config.metric.clone())
        .unwrap_or(DistanceMetric::L2);

    // `threshold` is a **minimum similarity**, on the same scale as the yielded
    // `score`, on every path of this procedure — dense, multi-vector and
    // auto-embedded alike. It used to be a *maximum distance* here only, so the
    // same argument meant opposite things depending on which branch the query
    // took and moving a property to a `List<Vector>` column silently inverted
    // the filter.
    if let Some(min_sim) = threshold {
        results.retain(|(_, dist)| calculate_score(*dist, &metric) >= min_sim as f32);
    }

    if results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    let (results, rerank_ctx) = apply_rerank(
        host,
        results,
        &label,
        reranker_config.as_ref(),
        query_text_from_arg.as_deref().unwrap_or(""),
        k,
    )
    .await?;

    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: rerank_ctx.as_ref(),
    };
    build_search_result_batch(
        &results,
        &label,
        RetrievalScore::Distance(&metric),
        &batch_ctx,
    )
    .await
}

/// `uni.fts.query(label, property, search_term, k, filter?, threshold?, options?)`.
///
/// `k` is required. The yielded `score` is BM25 squashed to `[0, 1)` by
/// `normalize_bm25` (larger is a better match), and `threshold` is a minimum on
/// that same scale. `options.fts_k` tunes the saturation constant, matching
/// `similar_to`.
pub(crate) async fn run_fts_query(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.fts.query: first argument (label)")?;
    let property = require_string_arg(args, 1, "uni.fts.query: second argument (property)")?;
    let search_term = require_string_arg(args, 2, "uni.fts.query: third argument (search_term)")?;
    let k = require_int_arg(args, 3, "uni.fts.query: fourth argument (k)")?;
    let filter = extract_optional_filter(args, 4);
    let threshold = extract_optional_threshold(args, 5);
    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| if v.is_null() { None } else { v.as_object() });
    let reranker_config = parse_reranker_options(options_map, k, Some(&property));

    // BM25 saturation constant, matching `similar_to`'s `fts_k` option so the
    // two full-text entry points report scores on one scale.
    let fts_k = options_map
        .and_then(|m| m.get("fts_k"))
        .and_then(|v| v.as_f64())
        .map(|v| v as f32)
        .filter(|v| *v > 0.0)
        .unwrap_or(1.0);

    let retrieval_k = reranker_config.as_ref().map_or(k, |rc| rc.k);
    let storage = host.storage();
    let query_ctx = host.query_context();

    let mut results = storage
        .fts_search(
            &label,
            &property,
            &search_term,
            retrieval_k,
            filter.as_deref(),
            Some(&query_ctx),
        )
        .await
        .map_err(exec_err)?;

    // `threshold` is a minimum on the *yielded* `score`, so it must be compared
    // on the normalized scale the caller sees — not against the raw BM25 value,
    // which has both a different range and (before the `RetrievalScore::Bm25`
    // fix) the opposite direction.
    if let Some(min_score) = threshold {
        results.retain(|(_, score)| normalize_bm25(*score, fts_k) as f64 >= min_score);
    }

    if results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    let (results, rerank_ctx) = apply_rerank(
        host,
        results,
        &label,
        reranker_config.as_ref(),
        &search_term,
        k,
    )
    .await?;

    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: rerank_ctx.as_ref(),
    };
    build_search_result_batch(
        &results,
        &label,
        RetrievalScore::Bm25 { k: fts_k },
        &batch_ctx,
    )
    .await
}

/// Parse three-way fusion weights `[vector, fts, sparse]` from `options.weights`.
///
/// Falls back to equal thirds when the option is absent or not a 3-element
/// numeric array. Used only on the weighted three-way (dense + text + sparse)
/// path; the two-way path keeps its single `alpha` knob.
fn parse_three_weights(options_map: Option<&HashMap<String, Value>>) -> [f32; 3] {
    const EQUAL_THIRD: f32 = 1.0 / 3.0;
    options_map
        .and_then(|m| m.get("weights"))
        .and_then(|v| v.as_array())
        .and_then(|arr| {
            let w: Vec<f32> = arr
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect();
            (w.len() == 3).then_some([w[0], w[1], w[2]])
        })
        .unwrap_or([EQUAL_THIRD, EQUAL_THIRD, EQUAL_THIRD])
}

/// `uni.search(label, properties, query_text, query_vector?, k, filter?, options?)`.
pub(crate) async fn run_hybrid_search(
    host: &QueryProcedureHost,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let label = require_string_arg(args, 0, "uni.search: first argument (label)")?;

    let properties_val = args.get(1).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "uni.search: second argument (properties) is required".to_string(),
        )
    })?;

    let (vector_prop, fts_prop, sparse_prop) = if let Some(obj) = properties_val.as_object() {
        let vec_prop = obj
            .get("vector")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let fts_prop = obj
            .get("fts")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let sparse_prop = obj
            .get("sparse")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        (vec_prop, fts_prop, sparse_prop)
    } else if let Some(prop) = properties_val.as_str() {
        // A bare string names a single property used for both dense + FTS; sparse
        // is opt-in only (it needs a paired query vector), so it stays absent here.
        (Some(prop.to_string()), Some(prop.to_string()), None)
    } else {
        return Err(datafusion::error::DataFusionError::Execution(
            "Properties must be an object {vector: '...', fts: '...', sparse: '...'} or a string"
                .to_string(),
        ));
    };

    let query_text = require_string_arg(args, 2, "uni.search: third argument (query_text)")?;

    let query_vector: Option<Vec<f32>> = args.get(3).and_then(|v| {
        if v.is_null() {
            return None;
        }
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect()
        })
    });

    let k = require_int_arg(args, 4, "uni.search: fifth argument (k)")?;
    let filter = extract_optional_filter(args, 5);

    let options_val = args.get(6);
    let options_map = options_val.and_then(|v| v.as_object());
    let fusion_method = options_map
        .and_then(|m| m.get("method"))
        .and_then(|v| v.as_str())
        .unwrap_or("rrf")
        .to_string();
    let alpha = options_map
        .and_then(|m| m.get("alpha"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.5) as f32;
    let over_fetch_factor = options_map
        .and_then(|m| m.get("over_fetch"))
        .and_then(|v| v.as_f64())
        .unwrap_or(2.0) as f32;
    let rrf_k = options_map
        .and_then(|m| m.get("rrf_k"))
        .and_then(|v| v.as_u64())
        .unwrap_or(60) as usize;

    let reranker_config = parse_reranker_options(options_map, k, fts_prop.as_deref());

    let over_fetch_k = (k as f32 * over_fetch_factor).ceil() as usize;
    let effective_retrieval_k = reranker_config
        .as_ref()
        .map_or(over_fetch_k, |rc| rc.k.max(over_fetch_k));

    let storage = host.storage();
    let query_ctx = host.query_context();

    let mut vector_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref vec_prop) = vector_prop {
        let qvec = if let Some(ref v) = query_vector {
            v.clone()
        } else {
            // Propagate an auto-embed failure instead of swallowing it to an
            // empty vector. Swallowing left `qvec` empty, silently skipping the
            // dense arm so hybrid search degraded to FTS-only with no error —
            // inconsistent with the fts/sparse arms below, which use `?`.
            auto_embed_text(host, &label, vec_prop, &query_text).await?
        };

        if !qvec.is_empty() {
            vector_results = storage
                .vector_search(
                    &label,
                    vec_prop,
                    &qvec,
                    effective_retrieval_k,
                    filter.as_deref(),
                    uni_store::VectorQueryOpts::default(),
                    Some(&query_ctx),
                )
                .await
                .map_err(exec_err)?;
        }
    }

    let mut fts_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref fts_prop) = fts_prop {
        fts_results = storage
            .fts_search(
                &label,
                fts_prop,
                &query_text,
                effective_retrieval_k,
                filter.as_deref(),
                Some(&query_ctx),
            )
            .await
            .map_err(exec_err)?;
    }

    // Sparse arm: opt-in via a `sparse` property plus an `options.sparse_query`
    // (a `SparseVector` or `{indices, values}` map). Reuse `sparse_rerank` so the
    // arm inherits the same flushed∪L0 / MVCC / tombstone correctness as
    // `uni.sparse.query`. Absent ⇒ `sparse_results` stays empty ⇒ fusion below is
    // a no-op for the sparse source and the two-way result is byte-identical.
    let mut sparse_results: Vec<(Vid, f32)> = Vec::new();
    if let Some(ref sparse_prop) = sparse_prop {
        let sparse_query_val = options_map.and_then(|m| m.get("sparse_query"));
        if let Some(sq_val) = sparse_query_val.filter(|v| !v.is_null()) {
            let sparse_query = extract_sparse_query(sq_val)?;
            let property_manager = host.property_manager().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "uni.search: sparse arm requires a property manager on the host".to_string(),
                )
            })?;
            let (scored, _props) = sparse_rerank(
                storage,
                property_manager,
                &query_ctx,
                &label,
                sparse_prop,
                &sparse_query,
                effective_retrieval_k,
                effective_retrieval_k,
            )
            .await?;
            sparse_results = scored;
        }
    }

    let fused_results = match fusion_method.as_str() {
        "weighted" => {
            if sparse_results.is_empty() {
                // Two-way weighted path is unchanged.
                crate::query::fusion::fuse_weighted(&vector_results, &fts_results, alpha)
            } else {
                // Three-way weighted with per-source normalization. Weights come
                // from `options.weights = [vector, fts, sparse]`, defaulting to
                // equal thirds; `alpha` is the two-way-only knob.
                let weights = parse_three_weights(options_map);
                use crate::query::fusion::NormKind;
                crate::query::fusion::fuse_weighted_sources(&[
                    (&vector_results, weights[0], NormKind::DistanceToSim),
                    (&fts_results, weights[1], NormKind::ScoreByMax),
                    (&sparse_results, weights[2], NormKind::ScoreByMax),
                ])
            }
        }
        // Relative-score fusion (Weaviate `relativeScore`): per-source min-max
        // normalize to [0, 1] then weighted-sum across all sources. Distinct from
        // `weighted` in that it always min-max normalizes every source uniformly
        // (never the two-way `alpha` shim). `rsf` is an accepted alias.
        "relative_score" | "rsf" => {
            let weights = parse_three_weights(options_map);
            use crate::query::fusion::NormKind;
            crate::query::fusion::fuse_weighted_sources(&[
                (&vector_results, weights[0], NormKind::DistanceToSim),
                (&fts_results, weights[1], NormKind::ScoreByMax),
                (&sparse_results, weights[2], NormKind::ScoreByMax),
            ])
        }
        // Distribution-Based Score Fusion (Qdrant DBSF): per-source z-score
        // normalize (robust to a single outlier stretching one list) then sum.
        // Weights default to equal thirds; equal weights make it a plain z-sum
        // (uniform scaling is rank-invariant).
        "dbsf" => {
            let weights = parse_three_weights(options_map);
            use crate::query::fusion::NormKind;
            crate::query::fusion::fuse_dbsf(&[
                (&vector_results, weights[0], NormKind::DistanceToSim),
                (&fts_results, weights[1], NormKind::ScoreByMax),
                (&sparse_results, weights[2], NormKind::ScoreByMax),
            ])
        }
        _ => crate::query::fusion::fuse_rrf_multi(
            &[&vector_results, &fts_results, &sparse_results],
            rrf_k,
        ),
    };

    let (final_results, rerank_ctx) = if let Some(ref rcfg) = reranker_config {
        let candidates: Vec<_> = fused_results.into_iter().take(rcfg.k).collect();
        if candidates.is_empty() {
            return Ok(Some(create_empty_batch(schema.clone())?));
        }
        let reranker_query = rcfg.query_override.as_deref().unwrap_or(&query_text);
        let (reranked, ctx) =
            rerank_candidates(host, candidates, &label, reranker_query, rcfg, k).await?;
        (reranked, Some(ctx))
    } else {
        let results: Vec<_> = fused_results.into_iter().take(k).collect();
        (results, None)
    };

    if final_results.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    let vec_score_map: HashMap<Vid, f32> = vector_results.iter().cloned().collect();
    let fts_score_map: HashMap<Vid, f32> = fts_results.iter().cloned().collect();
    let sparse_score_map: HashMap<Vid, f32> = sparse_results.iter().cloned().collect();
    let fts_max = fts_results.iter().map(|(_, s)| *s).fold(0.0f32, f32::max);

    let uni_schema = storage.schema_manager().schema();
    let metric = vector_prop
        .as_ref()
        .and_then(|vp| {
            uni_schema
                .vector_index_for_property(&label, vp)
                .map(|config| config.metric.clone())
        })
        .unwrap_or(DistanceMetric::L2);

    let score_ctx = HybridScoreContext {
        vec_score_map: &vec_score_map,
        fts_score_map: &fts_score_map,
        sparse_score_map: &sparse_score_map,
        fts_max,
        metric: &metric,
    };

    let batch_ctx = BatchBuildCtx {
        yield_items,
        target_properties,
        host,
        schema,
        rerank_ctx: rerank_ctx.as_ref(),
    };
    build_hybrid_search_batch(&final_results, &score_ctx, &label, &batch_ctx).await
}
