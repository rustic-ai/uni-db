// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `similar_to()` expression function — unified similarity scoring.
//!
//! Dispatches to vector cosine similarity, BM25 full-text scoring, or
//! hybrid fusion based on schema types. Returns a float in `[0, 1]`
//! where higher means more similar.
//!
//! This is a **point computation** (score one bound node), not a search
//! (top-K index scan). It works in WHERE, RETURN, WITH, ORDER BY, and
//! Locy rule bodies.

use anyhow::Result;
use uni_common::Value;
use uni_common::core::schema::{DistanceMetric, Schema};

use crate::fusion;

/// Named error types for `similar_to()` validation failures.
#[derive(Debug, thiserror::Error)]
pub enum SimilarToError {
    #[error("similar_to: property '{label}.{property}' has no vector or full-text index")]
    NoIndex { label: String, property: String },

    #[error(
        "similar_to: source {source_index} is FTS-indexed but query is a vector (FTS cannot score against vectors)"
    )]
    TypeMismatch { source_index: usize },

    #[error(
        "similar_to: source {source_index} is a vector property but query is a string, and the index has no embedding config for auto-embedding"
    )]
    NoEmbeddingConfig { source_index: usize },

    #[error("similar_to: weights length ({weights_len}) != sources length ({sources_len})")]
    WeightsLengthMismatch {
        weights_len: usize,
        sources_len: usize,
    },

    #[error("similar_to: weights must sum to 1.0 (got {sum})")]
    WeightsNotNormalized { sum: f32 },

    #[error("similar_to: unknown method '{method}', expected 'rrf' or 'weighted'")]
    InvalidMethod { method: String },

    #[error("similar_to: {message}")]
    InvalidOption { message: String },

    #[error("similar_to: vector dimensions mismatch: {a} vs {b}")]
    DimensionMismatch { a: usize, b: usize },

    #[error("similar_to: expected vector or list of numbers, got {actual}")]
    InvalidVectorValue { actual: String },

    #[error("similar_to: weighted fusion requires 'weights' option")]
    WeightsRequired,

    #[error("similar_to takes 2 or 3 arguments (sources, queries [, options]), got {count}")]
    InvalidArity { count: usize },

    #[error("similar_to requires GraphExecutionContext")]
    NoGraphContext,
}

/// Fusion method for multi-source scoring.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum FusionMethod {
    /// Reciprocal Rank Fusion (default). Falls back to equal-weight
    /// fusion in point-computation context.
    #[default]
    Rrf,
    /// Weighted sum of per-source scores.
    Weighted,
}

/// Options for `similar_to()` controlling fusion and scoring behavior.
#[derive(Debug, Clone)]
pub struct SimilarToOptions {
    /// Fusion algorithm when multiple sources are present.
    pub method: FusionMethod,
    /// Per-source weights for weighted fusion. Must sum to 1.0.
    pub weights: Option<Vec<f32>>,
    /// RRF constant k (default 60).
    pub k: usize,
    /// BM25 saturation constant for FTS normalization (default 1.0).
    pub fts_k: f32,
}

impl Default for SimilarToOptions {
    fn default() -> Self {
        Self {
            method: FusionMethod::Rrf,
            weights: None,
            k: 60,
            fts_k: 1.0,
        }
    }
}

/// Parse options from a `Value::Map`.
pub fn parse_options(value: &Value) -> Result<SimilarToOptions, SimilarToError> {
    let map = match value {
        Value::Map(m) => m,
        Value::Null => return Ok(SimilarToOptions::default()),
        _ => {
            return Err(SimilarToError::InvalidOption {
                message: format!("options must be a map, got {:?}", value),
            });
        }
    };

    let mut opts = SimilarToOptions::default();

    if let Some(method_val) = map.get("method") {
        match method_val.as_str() {
            Some("rrf") => opts.method = FusionMethod::Rrf,
            Some("weighted") => opts.method = FusionMethod::Weighted,
            Some(other) => {
                return Err(SimilarToError::InvalidMethod {
                    method: other.to_string(),
                });
            }
            None => {
                return Err(SimilarToError::InvalidOption {
                    message: "'method' must be a string ('rrf' or 'weighted')".to_string(),
                });
            }
        }
    }

    if let Some(weights_val) = map.get("weights") {
        match weights_val {
            Value::List(list) => {
                let weights: Result<Vec<f32>, SimilarToError> = list
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .map(|f| f as f32)
                            .ok_or_else(|| SimilarToError::InvalidOption {
                                message: "weight must be a number".to_string(),
                            })
                    })
                    .collect();
                opts.weights = Some(weights?);
            }
            _ => {
                return Err(SimilarToError::InvalidOption {
                    message: "'weights' must be a list of numbers".to_string(),
                });
            }
        }
    }

    if let Some(k_val) = map.get("k") {
        opts.k = k_val
            .as_i64()
            .ok_or_else(|| SimilarToError::InvalidOption {
                message: "'k' must be an integer".to_string(),
            })? as usize;
    }

    if let Some(fts_k_val) = map.get("fts_k") {
        opts.fts_k = fts_k_val
            .as_f64()
            .ok_or_else(|| SimilarToError::InvalidOption {
                message: "'fts_k' must be a number".to_string(),
            })? as f32;
    }

    Ok(opts)
}

/// What type of source a property represents.
#[derive(Debug, Clone)]
pub enum SourceType {
    /// Vector property with a vector index.
    Vector {
        metric: DistanceMetric,
        has_embedding_config: bool,
    },
    /// String property with a full-text index.
    Fts,
}

/// Resolve the source type for a property given the schema.
pub fn resolve_source_type(
    schema: &Schema,
    label: &str,
    property: &str,
) -> Result<SourceType, SimilarToError> {
    // Check vector index first
    if let Some(vec_config) = schema.vector_index_for_property(label, property) {
        return Ok(SourceType::Vector {
            metric: vec_config.metric.clone(),
            has_embedding_config: vec_config.embedding_config.is_some(),
        });
    }

    // Check full-text index
    if schema
        .fulltext_index_for_property(label, property)
        .is_some()
    {
        return Ok(SourceType::Fts);
    }

    Err(SimilarToError::NoIndex {
        label: label.to_string(),
        property: property.to_string(),
    })
}

/// Compute cosine similarity between two vectors, returning a score in [-1, 1].
///
/// Arithmetic is performed in f64 (see `cosine_similarity_inner`) and the
/// clamped result is narrowed to f32.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32, SimilarToError> {
    cosine_similarity_inner(a, b).map(|sim| sim as f32)
}

/// Score two vectors using the specified distance metric, returning a similarity
/// score where higher means more similar.
///
/// - **Cosine**: raw cosine similarity in \[-1, 1\] (delegates to [`cosine_similarity`]).
/// - **L2**: `1 / (1 + d²)` where d² is squared Euclidean distance; range (0, 1\].
/// - **Dot**: raw dot product (for normalised vectors equals cosine similarity).
pub fn score_vectors(a: &[f32], b: &[f32], metric: &DistanceMetric) -> Result<f32, SimilarToError> {
    if a.len() != b.len() {
        return Err(SimilarToError::DimensionMismatch {
            a: a.len(),
            b: b.len(),
        });
    }
    let distance = metric.compute_distance(a, b);
    match metric {
        DistanceMetric::Cosine => cosine_similarity(a, b),
        // compute_distance returns -dot (LanceDB convention: lower = more similar).
        // Negate to recover the actual dot product as a similarity score.
        DistanceMetric::Dot => Ok(-distance),
        // L2 and all other metrics (#[non_exhaustive]): normalise via calculate_score.
        _ => Ok(calculate_score(distance, metric)),
    }
}

/// Compute the MaxSim (late-interaction / ColBERT) score between a query and a
/// document, each represented as a set of token vectors.
///
/// MaxSim is `Σ_i max_j score_vectors(query_i, doc_j, metric)`: for every query
/// token take its best match across all document tokens, then sum. Per-token
/// scoring reuses [`score_vectors`], so the metric's normalisation matches the
/// rest of the engine. Default metric for late interaction is `Cosine`.
///
/// An empty query, or a query token with no document tokens to match against,
/// contributes `0` rather than erroring.
///
/// # Errors
/// Returns [`SimilarToError::DimensionMismatch`] if a query token and a document
/// token differ in length.
pub fn maxsim(
    query: &[Vec<f32>],
    doc: &[Vec<f32>],
    metric: &DistanceMetric,
) -> Result<f32, SimilarToError> {
    let mut total = 0.0_f32;
    for q in query {
        let mut best: Option<f32> = None;
        for d in doc {
            let sim = score_vectors(q, d, metric)?;
            best = Some(best.map_or(sim, |b| b.max(sim)));
        }
        // `None` means the document had no tokens: that query token scores 0.
        total += best.unwrap_or(0.0);
    }
    Ok(total)
}

/// Convert a raw distance value into a normalised similarity score.
///
/// The conversion depends on the distance metric:
/// - **Cosine**: `(2 - d) / 2` (LanceDB cosine distance ranges 0..2)
/// - **Dot**: `-d` — [`DistanceMetric::compute_distance`] returns `-dot`, so
///   negating recovers the dot product as the similarity measure
/// - **L2** and others: `1 / (1 + d)`
pub fn calculate_score(distance: f32, metric: &DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => (2.0 - distance) / 2.0,
        DistanceMetric::Dot => -distance,
        _ => 1.0 / (1.0 + distance),
    }
}

/// Normalize a BM25 score to [0, 1] using a saturation function.
///
/// `normalized = score / (score + fts_k)` where `fts_k` defaults to 1.0.
pub fn normalize_bm25(score: f32, fts_k: f32) -> f32 {
    if score <= 0.0 {
        return 0.0;
    }
    score / (score + fts_k)
}

/// Compute pure vector-vs-vector similarity (no storage access needed).
///
/// Both values must be `Value::List` of numbers or `Value::Vector`: the result
/// is their cosine similarity. Two multi-vectors (lists of token vectors) score
/// by MaxSim with a cosine per token, `v2` being the query and `v1` the
/// document — the same score the multi-vector index ranks by.
///
/// The cosine is computed and returned in f64 whatever the input width. f32
/// inputs (`Value::Vector`) are widened, never the result narrowed: rounding
/// an exact 0.8 to f32 gives 0.800000011920929, which passes `> 0.8`.
pub fn eval_similar_to_pure(v1: &Value, v2: &Value) -> Result<Value> {
    // NULL propagates: a NULL operand yields NULL, not an error. `values_to_array`
    // renders `Value::Null` as an Arrow null, matching 3VL semantics.
    if matches!(v1, Value::Null) || matches!(v2, Value::Null) {
        return Ok(Value::Null);
    }
    if let (Some(doc), Some(query)) = (as_multivector(v1), as_multivector(v2)) {
        let score = maxsim(&query?, &doc?, &DistanceMetric::Cosine)?;
        return Ok(Value::Float(f64::from(score)));
    }
    // Fast path: at least one input is a List — use f64 to avoid f32 precision loss.
    let has_list = matches!(v1, Value::List(_)) || matches!(v2, Value::List(_));
    let f64_vecs = has_list
        .then(|| value_to_f64_vec(v1).ok().zip(value_to_f64_vec(v2).ok()))
        .flatten();
    if let Some((vec1, vec2)) = f64_vecs {
        let sim = cosine_similarity_inner(&vec1, &vec2)?;
        return Ok(Value::Float(sim));
    }
    // Fallback: `Value::Vector` operands (stored f32 data), widened to f64.
    let vec1 = value_to_f32_vec(v1)?;
    let vec2 = value_to_f32_vec(v2)?;
    let sim = cosine_similarity_inner(&vec1, &vec2)?;
    Ok(Value::Float(sim))
}

/// Reads `v` as a multi-vector: a non-empty list whose items are all vectors.
///
/// `None` when `v` is not one (a flat vector, a scalar list, an empty list);
/// `Some(Err)` when an item is a list with a non-numeric element.
fn as_multivector(v: &Value) -> Option<Result<Vec<Vec<f32>>, SimilarToError>> {
    let Value::List(items) = v else {
        return None;
    };
    let is_token = |item: &Value| matches!(item, Value::Vector(_) | Value::List(_));
    if items.is_empty() || !items.iter().all(is_token) {
        return None;
    }
    Some(items.iter().map(value_to_f32_vec).collect())
}

/// Compute the sparse dot product between two sparse vectors (no storage access).
///
/// Each operand may be a `Value::SparseVector` or the structural
/// `{indices: [...], values: [...]}` `Value::Map` — the latter is how a sparse
/// param arrives at a scalar UDF, where the Arrow `Struct` is decoded without
/// schema context. A `NULL` operand propagates to `NULL` (3VL semantics,
/// matching [`eval_similar_to_pure`]). Non-overlapping term sets yield
/// `Value::Float(0.0)`.
///
/// This is the SPLADE/learned-sparse analogue of [`eval_similar_to_pure`]:
/// dot product is the canonical sparse similarity, so no normalization is
/// applied (higher means more similar, unbounded above).
///
/// # Errors
/// Returns an error if either non-null operand is neither a sparse vector nor a
/// well-formed `{indices, values}` map, or if its weights are non-finite.
pub fn eval_sparse_similar_to_pure(v1: &Value, v2: &Value) -> Result<Value> {
    if matches!(v1, Value::Null) || matches!(v2, Value::Null) {
        return Ok(Value::Null);
    }
    let a = value_to_sparse(v1)?;
    let b = value_to_sparse(v2)?;
    Ok(Value::Float(f64::from(uni_sparse_vector::ops::sparse_dot(
        &a, &b,
    ))))
}

/// Reconstruct a kernel [`uni_sparse_vector::SparseVector`] from either a
/// `Value::SparseVector` or a `{indices, values}` `Value::Map`.
fn value_to_sparse(v: &Value) -> Result<uni_sparse_vector::SparseVector> {
    match v {
        Value::SparseVector { indices, values } => {
            uni_sparse_vector::SparseVector::new(indices.clone(), values.clone())
                .map_err(|e| anyhow::anyhow!("sparse_similar_to: invalid sparse vector: {e}"))
        }
        Value::Map(m) => {
            let as_list = |key: &str| -> Result<&Vec<Value>> {
                match m.get(key) {
                    Some(Value::List(l)) => Ok(l),
                    _ => Err(anyhow::anyhow!(
                        "sparse_similar_to: map operand missing '{key}' list"
                    )),
                }
            };
            let indices: Vec<u32> = as_list("indices")?
                .iter()
                .map(|x| {
                    let i = x.as_i64().ok_or_else(|| {
                        anyhow::anyhow!("sparse_similar_to: 'indices' must be integers")
                    })?;
                    // A term id must fit in u32; reject negative or too-large
                    // values instead of silently wrapping them (e.g. -1 -> u32::MAX)
                    // into an unrelated term.
                    u32::try_from(i).map_err(|_| {
                        anyhow::anyhow!(
                            "sparse_similar_to: index {i} out of range for a sparse term id (0..=4294967295)"
                        )
                    })
                })
                .collect::<Result<_>>()?;
            let values: Vec<f32> = as_list("values")?
                .iter()
                .map(|x| x.as_f64().map(|f| f as f32))
                .collect::<Option<_>>()
                .ok_or_else(|| anyhow::anyhow!("sparse_similar_to: 'values' must be numbers"))?;
            // `from_pairs` canonicalizes (sort + sum) since a map carries no
            // ascending-order guarantee.
            uni_sparse_vector::SparseVector::from_pairs(indices.into_iter().zip(values).collect())
                .map_err(|e| anyhow::anyhow!("sparse_similar_to: invalid sparse map: {e}"))
        }
        _ => Err(anyhow::anyhow!(
            "sparse_similar_to arguments must be sparse vectors or {{indices, values}} maps"
        )),
    }
}

/// Compute the raw cosine similarity (in f64) between two equal-length vectors,
/// clamped to [-1, 1]. Returns 0 when either vector has zero magnitude.
///
/// Generic over the element type so both the f32 and f64 callers share one body;
/// arithmetic is always performed in f64 to preserve precision.
fn cosine_similarity_inner<T: Copy + Into<f64>>(a: &[T], b: &[T]) -> Result<f64, SimilarToError> {
    if a.len() != b.len() {
        return Err(SimilarToError::DimensionMismatch {
            a: a.len(),
            b: b.len(),
        });
    }
    let mut dot = 0.0f64;
    let mut mag1 = 0.0f64;
    let mut mag2 = 0.0f64;
    for (&x, &y) in a.iter().zip(b.iter()) {
        let (x, y): (f64, f64) = (x.into(), y.into());
        dot += x * y;
        mag1 += x * x;
        mag2 += y * y;
    }
    let mag1 = mag1.sqrt();
    let mag2 = mag2.sqrt();
    if mag1 == 0.0 || mag2 == 0.0 {
        return Ok(0.0);
    }
    Ok((dot / (mag1 * mag2)).clamp(-1.0, 1.0))
}

/// Convert a Value to a numeric vector for vector operations.
///
/// Generic over the element type: `cast` maps the `f64` source value onto the
/// target representation (identity for `f64`, narrowing for `f32`). List elements
/// are read as full-precision `f64` before casting, while `Value::Vector` data is
/// already `f32` and is widened to `f64` first.
fn value_to_vec<T>(v: &Value, cast: impl Fn(f64) -> T) -> Result<Vec<T>, SimilarToError> {
    match v {
        Value::Vector(vec) => Ok(vec.iter().map(|&x| cast(x as f64)).collect()),
        Value::List(list) => list
            .iter()
            .map(|v| {
                v.as_f64()
                    .map(&cast)
                    .ok_or_else(|| SimilarToError::InvalidOption {
                        message: "vector element must be a number".to_string(),
                    })
            })
            .collect(),
        _ => Err(SimilarToError::InvalidVectorValue {
            actual: format!("{v:?}"),
        }),
    }
}

/// Convert a Value to a `Vec<f64>` for high-precision vector operations.
fn value_to_f64_vec(v: &Value) -> Result<Vec<f64>, SimilarToError> {
    value_to_vec(v, |f| f)
}

/// Convert a Value to a `Vec<f32>` for vector operations.
pub fn value_to_f32_vec(v: &Value) -> Result<Vec<f32>, SimilarToError> {
    value_to_vec(v, |f| f as f32)
}

/// Validate options against the number of sources.
pub fn validate_options(opts: &SimilarToOptions, num_sources: usize) -> Result<(), SimilarToError> {
    if let Some(ref weights) = opts.weights {
        if weights.len() != num_sources {
            return Err(SimilarToError::WeightsLengthMismatch {
                weights_len: weights.len(),
                sources_len: num_sources,
            });
        }
        let sum: f32 = weights.iter().sum();
        if (sum - 1.0).abs() > 0.01 {
            return Err(SimilarToError::WeightsNotNormalized { sum });
        }
    }
    Ok(())
}

/// Validate per-pair type compatibility.
///
/// Returns an error if a Vector query is paired with an FTS source,
/// or a String query is paired with a Vector source that has no embedding config.
pub fn validate_pair(
    source_type: &SourceType,
    query_is_vector: bool,
    query_is_string: bool,
    source_index: usize,
) -> Result<(), SimilarToError> {
    match source_type {
        SourceType::Fts if query_is_vector => Err(SimilarToError::TypeMismatch { source_index }),
        SourceType::Vector {
            has_embedding_config: false,
            ..
        } if query_is_string => Err(SimilarToError::NoEmbeddingConfig { source_index }),
        _ => Ok(()),
    }
}

/// Fuse multiple per-source scores into a single score.
pub fn fuse_scores(scores: &[f32], opts: &SimilarToOptions) -> Result<f32, SimilarToError> {
    if scores.len() == 1 {
        return Ok(scores[0]);
    }

    match opts.method {
        FusionMethod::Weighted => {
            let weights = opts
                .weights
                .as_ref()
                .ok_or(SimilarToError::WeightsRequired)?;
            Ok(fusion::fuse_weighted_multi(scores, weights))
        }
        FusionMethod::Rrf => {
            // In point-computation context, RRF degenerates to equal-weight fusion.
            // The caller (similar_to_expr.rs) already emits QueryWarning::RrfPointContext
            // unconditionally when method == Rrf && num_sources > 1.
            let (score, _) = fusion::fuse_rrf_point(scores);
            Ok(score)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_parse_options_default() {
        let opts = parse_options(&Value::Null).unwrap();
        assert_eq!(opts.method, FusionMethod::Rrf);
        assert_eq!(opts.k, 60);
        assert!((opts.fts_k - 1.0).abs() < 1e-6);
        assert!(opts.weights.is_none());
    }

    #[test]
    fn test_maxsim_hand_computed() {
        // query = {e0, e1}; doc = {e0, [0.5,0.5]}. With Dot:
        //   q0=e0 -> max(e0·e0=1, e0·[.5,.5]=0.5) = 1.0
        //   q1=e1 -> max(e1·e0=0, e1·[.5,.5]=0.5) = 0.5
        // MaxSim = 1.5
        let query = vec![vec![1.0_f32, 0.0], vec![0.0_f32, 1.0]];
        let doc = vec![vec![1.0_f32, 0.0], vec![0.5_f32, 0.5]];
        let score = maxsim(&query, &doc, &DistanceMetric::Dot).unwrap();
        assert!((score - 1.5).abs() < 1e-6, "got {score}");
    }

    #[test]
    fn test_maxsim_edge_cases() {
        let metric = DistanceMetric::Cosine;
        // Empty query -> 0.
        assert_eq!(maxsim(&[], &[vec![1.0_f32, 0.0]], &metric).unwrap(), 0.0);
        // Empty doc -> every query token scores 0 -> 0.
        let empty_doc: Vec<Vec<f32>> = vec![];
        assert_eq!(
            maxsim(&[vec![1.0_f32, 0.0]], &empty_doc, &metric).unwrap(),
            0.0
        );
        // Dimension mismatch between a query token and a doc token -> error.
        let err = maxsim(&[vec![1.0_f32, 0.0]], &[vec![1.0_f32, 0.0, 0.0]], &metric);
        assert!(matches!(err, Err(SimilarToError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_maxsim_metric_changes_score() {
        // Non-unit vectors so Dot and Cosine diverge: q=[2,0], d=[3,0].
        //   Dot    = 2*3 = 6.0
        //   Cosine = (2*3) / (|2|*|3|) = 1.0
        let q = vec![vec![2.0_f32, 0.0]];
        let d = vec![vec![3.0_f32, 0.0]];
        let dot = maxsim(&q, &d, &DistanceMetric::Dot).unwrap();
        let cos = maxsim(&q, &d, &DistanceMetric::Cosine).unwrap();
        assert!((dot - 6.0).abs() < 1e-6, "dot got {dot}");
        assert!((cos - 1.0).abs() < 1e-6, "cosine got {cos}");
    }

    #[test]
    fn test_parse_options_weighted() {
        let mut map = HashMap::new();
        map.insert("method".to_string(), Value::String("weighted".to_string()));
        map.insert(
            "weights".to_string(),
            Value::List(vec![Value::Float(0.7), Value::Float(0.3)]),
        );
        let opts = parse_options(&Value::Map(map)).unwrap();
        assert_eq!(opts.method, FusionMethod::Weighted);
        let weights = opts.weights.unwrap();
        assert!((weights[0] - 0.7).abs() < 1e-6);
        assert!((weights[1] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn test_parse_options_rrf_with_k() {
        let mut map = HashMap::new();
        map.insert("method".to_string(), Value::String("rrf".to_string()));
        map.insert("k".to_string(), Value::Int(30));
        let opts = parse_options(&Value::Map(map)).unwrap();
        assert_eq!(opts.method, FusionMethod::Rrf);
        assert_eq!(opts.k, 30);
    }

    #[test]
    fn test_parse_options_fts_k() {
        let mut map = HashMap::new();
        map.insert("fts_k".to_string(), Value::Float(2.0));
        let opts = parse_options(&Value::Map(map)).unwrap();
        assert!((opts.fts_k - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_parse_options_invalid_method() {
        let mut map = HashMap::new();
        map.insert("method".to_string(), Value::String("invalid".to_string()));
        assert!(parse_options(&Value::Map(map)).is_err());
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&v, &v).unwrap();
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_dimension_mismatch() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!(cosine_similarity(&a, &b).is_err());
    }

    #[test]
    fn test_normalize_bm25() {
        assert!((normalize_bm25(0.0, 1.0) - 0.0).abs() < 1e-6);
        assert!((normalize_bm25(1.0, 1.0) - 0.5).abs() < 1e-6);
        assert!((normalize_bm25(9.0, 1.0) - 0.9).abs() < 1e-6);
        assert!((normalize_bm25(99.0, 1.0) - 0.99).abs() < 1e-4);
    }

    #[test]
    fn test_normalize_bm25_custom_k() {
        assert!((normalize_bm25(2.0, 2.0) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_eval_similar_to_pure() {
        let v1 = Value::List(vec![Value::Float(1.0), Value::Float(0.0)]);
        let v2 = Value::List(vec![Value::Float(1.0), Value::Float(0.0)]);
        let result = eval_similar_to_pure(&v1, &v2).unwrap();
        match result {
            Value::Float(f) => assert!((f - 1.0).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_similar_to_pure_vector_type() {
        let v1 = Value::Vector(vec![1.0, 0.0]);
        let v2 = Value::Vector(vec![0.0, 1.0]);
        let result = eval_similar_to_pure(&v1, &v2).unwrap();
        match result {
            Value::Float(f) => assert!((f - 0.0).abs() < 1e-6),
            _ => panic!("Expected Float"),
        }
    }

    /// Two stored (f32) vectors with an exact cosine of 0.8 score exactly
    /// 0.8 — not 0.8 rounded to f32, which exceeds 0.8.
    #[test]
    fn test_eval_similar_to_pure_vector_type_keeps_f64() {
        let v1 = Value::Vector(vec![3.0, 4.0]);
        let v2 = Value::Vector(vec![0.0, 1.0]);
        assert_eq!(eval_similar_to_pure(&v1, &v2).unwrap(), Value::Float(0.8));
    }

    /// Two multi-vectors score by MaxSim, the second operand being the query.
    #[test]
    fn test_eval_similar_to_pure_multivector_is_maxsim() {
        let tok = |x: f32, y: f32| Value::Vector(vec![x, y]);
        let doc = Value::List(vec![tok(1.0, 0.0), tok(0.0, 1.0)]);
        let query = Value::List(vec![tok(1.0, 0.0), tok(0.0, 1.0), tok(1.0, 0.0)]);
        // Every query token finds an exact match: 1 + 1 + 1.
        assert_eq!(
            eval_similar_to_pure(&doc, &query).unwrap(),
            Value::Float(3.0)
        );
        // Reversed, the two-token operand is the query: 1 + 1.
        assert_eq!(
            eval_similar_to_pure(&query, &doc).unwrap(),
            Value::Float(2.0)
        );
    }

    #[test]
    fn test_validate_options_weights_length() {
        let opts = SimilarToOptions {
            weights: Some(vec![0.5]),
            ..Default::default()
        };
        assert!(validate_options(&opts, 2).is_err());
    }

    #[test]
    fn test_validate_options_weights_sum() {
        let opts = SimilarToOptions {
            weights: Some(vec![0.5, 0.3]),
            ..Default::default()
        };
        assert!(validate_options(&opts, 2).is_err());
    }

    #[test]
    fn test_validate_options_ok() {
        let opts = SimilarToOptions {
            weights: Some(vec![0.7, 0.3]),
            ..Default::default()
        };
        assert!(validate_options(&opts, 2).is_ok());
    }

    #[test]
    fn test_validate_pair_fts_vector_query() {
        assert!(validate_pair(&SourceType::Fts, true, false, 0).is_err());
    }

    #[test]
    fn test_validate_pair_vector_string_no_embed() {
        let st = SourceType::Vector {
            metric: DistanceMetric::Cosine,
            has_embedding_config: false,
        };
        assert!(validate_pair(&st, false, true, 0).is_err());
    }

    #[test]
    fn test_validate_pair_vector_string_with_embed() {
        let st = SourceType::Vector {
            metric: DistanceMetric::Cosine,
            has_embedding_config: true,
        };
        assert!(validate_pair(&st, false, true, 0).is_ok());
    }

    #[test]
    fn test_validate_pair_vector_vector() {
        let st = SourceType::Vector {
            metric: DistanceMetric::Cosine,
            has_embedding_config: false,
        };
        assert!(validate_pair(&st, true, false, 0).is_ok());
    }

    #[test]
    fn test_validate_pair_fts_string() {
        assert!(validate_pair(&SourceType::Fts, false, true, 0).is_ok());
    }

    #[test]
    fn test_fuse_scores_single() {
        let opts = SimilarToOptions::default();
        let score = fuse_scores(&[0.8], &opts).unwrap();
        assert!((score - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_scores_weighted() {
        let opts = SimilarToOptions {
            method: FusionMethod::Weighted,
            weights: Some(vec![0.7, 0.3]),
            ..Default::default()
        };
        let score = fuse_scores(&[0.8, 0.6], &opts).unwrap();
        assert!((score - 0.74).abs() < 1e-6);
    }

    #[test]
    fn test_fuse_scores_rrf_fallback() {
        let opts = SimilarToOptions::default();
        let score = fuse_scores(&[0.8, 0.6], &opts).unwrap();
        // RRF in point context falls back to equal weights: (0.8 + 0.6) / 2 = 0.7
        assert!((score - 0.7).abs() < 1e-6);
    }

    // -----------------------------------------------------------------------
    // score_vectors() tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_score_vectors_cosine_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let score = score_vectors(&v, &v, &DistanceMetric::Cosine).unwrap();
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_cosine_matches_raw() {
        // score_vectors with Cosine delegates to cosine_similarity
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.8, 0.6, 0.0];
        let raw = cosine_similarity(&a, &b).unwrap();
        let scored = score_vectors(&a, &b, &DistanceMetric::Cosine).unwrap();
        assert!((raw - scored).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_l2() {
        // [1,0,0] vs [0,1,0]: L2 squared distance = 2, score = 1/(1+2) ≈ 0.333
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let score = score_vectors(&a, &b, &DistanceMetric::L2).unwrap();
        assert!((score - 1.0 / 3.0).abs() < 1e-5);
    }

    #[test]
    fn test_score_vectors_l2_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let score = score_vectors(&v, &v, &DistanceMetric::L2).unwrap();
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_dot() {
        // [1,0,0] dot [0.8,0.6,0] = 0.8
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.8, 0.6, 0.0];
        let score = score_vectors(&a, &b, &DistanceMetric::Dot).unwrap();
        assert!((score - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_dot_identical() {
        let v = vec![1.0, 0.0, 0.0];
        let score = score_vectors(&v, &v, &DistanceMetric::Dot).unwrap();
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_score_vectors_dimension_mismatch() {
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!(score_vectors(&a, &b, &DistanceMetric::Cosine).is_err());
    }
}
