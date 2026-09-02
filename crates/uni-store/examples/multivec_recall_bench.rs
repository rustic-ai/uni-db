// Rust guideline compliant
//! Recall@k benchmark for native multi-vector (ColBERT / MaxSim) indexing on the
//! pinned Lance stack (`lance = 7.0.0`), issue #96 Phase 2.
//!
//! This is the VALIDATION GATE for building a native multi-vector first-stage
//! index (vs Phase 1's in-process rerank-only). It generates a fixed,
//! deterministic corpus of variable-token multi-vectors, computes a brute-force
//! MaxSim ground-truth top-k, and measures `recall@k` of Lance's native
//! multi-vector retrieval for: no index (Flat / brute force — should be ≈1.0 and
//! confirms Lance MaxSim matches our reference); IVF_FLAT (IVF partitioning, no
//! quantization); and IVF_PQ (IVF + product quantization). Each indexed mode is
//! measured at default probing and at full probing (all partitions).
//!
//! The numbers gate the Phase-2 decision: a native first-stage index is worth
//! building only if IVF recall is high enough (≈0.95+); otherwise rerank-only
//! (Phase 1) stands. Run with:
//! `cargo run -p uni-store --example multivec_recall_bench --release`

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, RecordBatch, UInt64Array};
use tempfile::TempDir;
use uni_store::backend::StorageBackend;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::types::{
    DistanceMetric, FilterExpr, VectorIndexKind, VectorIndexParams, VectorQueryOpts,
};

/// Token-vector dimension.
const DIM: i32 = 64;
/// Number of documents in the corpus.
const N_DOCS: u64 = 800;
/// Number of query tokens.
const N_QUERY_TOKENS: usize = 4;
/// Top-k for recall.
const K: usize = 10;
/// IVF partitions.
const N_PARTITIONS: u32 = 16;

/// Deterministic xorshift PRNG so the corpus is reproducible across runs
/// (`Math.random`-style nondeterminism would make recall numbers unstable).
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A pseudo-random `f32` in `[-1, 1)`.
    fn next_f32(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }

    /// A unit-norm token vector of dimension `DIM`.
    fn unit_vector(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..DIM).map(|_| self.next_f32()).collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= norm;
        }
        v
    }
}

/// A document: an id and a variable-count set of token vectors.
struct Doc {
    id: u64,
    tokens: Vec<Vec<f32>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut rng = Rng(0x9E3779B97F4A7C15);

    // Corpus: each doc has 2..=6 token vectors (variable count).
    let docs: Vec<Doc> = (0..N_DOCS)
        .map(|id| {
            let n_tokens = 2 + (rng.next_u64() % 5) as usize; // 2..=6
            let tokens = (0..n_tokens).map(|_| rng.unit_vector()).collect();
            Doc { id, tokens }
        })
        .collect();
    let query: Vec<Vec<f32>> = (0..N_QUERY_TOKENS).map(|_| rng.unit_vector()).collect();

    let total_tokens: usize = docs.iter().map(|d| d.tokens.len()).sum();
    println!(
        "Corpus: {N_DOCS} docs, {total_tokens} tokens, dim {DIM}, {N_QUERY_TOKENS} query tokens, k={K}\n"
    );

    // Brute-force MaxSim ground truth (cosine == dot for unit vectors).
    let truth = top_k_ids(brute_force_maxsim(&docs, &query));
    println!("Ground-truth top-{K} (brute-force MaxSim): {truth:?}\n");

    let tmp = TempDir::new().context("temp dir")?;
    let uri = tmp.path().to_str().context("non-utf8 temp path")?;
    let backend = LanceDbBackend::connect(uri, None)
        .await
        .context("connect")?;

    // No index (Flat / brute force) — confirms Lance MaxSim matches our reference.
    let flat = build_table(&backend, "flat", &docs).await?;
    report(
        "no-index (Flat)",
        &truth,
        &query_topk(&backend, &flat, &query, None).await?,
    );

    // IVF_FLAT, measured at default probing AND probing every partition (full
    // nprobes = best-case recall, but no speedup, so it bounds what tuning buys).
    backend
        .create_vector_index(
            &flat,
            "vector",
            "flat_ivf",
            VectorIndexParams {
                kind: VectorIndexKind::IvfFlat {
                    num_partitions: N_PARTITIONS,
                },
                metric: DistanceMetric::Cosine,
            },
        )
        .await
        .context("create IVF_FLAT index")?;
    report(
        "IVF_FLAT (default nprobes)",
        &truth,
        &query_topk(&backend, &flat, &query, None).await?,
    );
    report(
        "IVF_FLAT (all partitions)",
        &truth,
        &query_topk(&backend, &flat, &query, Some(N_PARTITIONS as usize)).await?,
    );

    // IVF_PQ (fresh table so the IVF_FLAT index does not interfere).
    let pq = build_table(&backend, "pq", &docs).await?;
    backend
        .create_vector_index(
            &pq,
            "vector",
            "pq_ivf",
            VectorIndexParams {
                kind: VectorIndexKind::IvfPq {
                    num_partitions: N_PARTITIONS,
                    num_sub_vectors: 8,
                    num_bits: 8,
                },
                metric: DistanceMetric::Cosine,
            },
        )
        .await
        .context("create IVF_PQ index")?;
    report(
        "IVF_PQ (default nprobes)",
        &truth,
        &query_topk(&backend, &pq, &query, None).await?,
    );
    report(
        "IVF_PQ (all partitions)",
        &truth,
        &query_topk(&backend, &pq, &query, Some(N_PARTITIONS as usize)).await?,
    );

    println!(
        "\nGate: a native first-stage multi-vector index is worth building only if IVF\n\
         recall@{K} is high enough (≈0.95+). Otherwise Phase 1 rerank-only stands."
    );
    Ok(())
}

/// Cosine similarity of two unit-norm vectors (== dot product).
fn cos(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Brute-force MaxSim per doc: `Σ_i max_j cos(q_i, d_j)`. Returns (id, score) desc.
fn brute_force_maxsim(docs: &[Doc], query: &[Vec<f32>]) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = docs
        .iter()
        .map(|doc| {
            let score: f32 = query
                .iter()
                .map(|q| {
                    doc.tokens
                        .iter()
                        .map(|d| cos(q, d))
                        .fold(f32::NEG_INFINITY, f32::max)
                })
                .sum();
            (doc.id, score)
        })
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1));
    scored
}

/// Top-k ids from a descending `(id, score)` list.
fn top_k_ids(ranked: Vec<(u64, f32)>) -> Vec<u64> {
    ranked.into_iter().take(K).map(|(id, _)| id).collect()
}

/// Builds and stores a `{id, vector: List<FixedSizeList<Float32, DIM>>}` table.
///
/// # Errors
/// Returns an error if Arrow assembly or table creation fails.
async fn build_table(backend: &LanceDbBackend, name: &str, docs: &[Doc]) -> Result<String> {
    let ids = UInt64Array::from(docs.iter().map(|d| d.id).collect::<Vec<_>>());
    let mut vectors = ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), DIM));
    for doc in docs {
        for tok in &doc.tokens {
            vectors.values().values().append_slice(tok);
            vectors.values().append(true);
        }
        vectors.append(true);
    }
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn Array>),
        ("vector", Arc::new(vectors.finish()) as Arc<dyn Array>),
    ])
    .context("assemble batch")?;
    backend
        .create_table(name, vec![batch])
        .await
        .with_context(|| format!("create table {name}"))?;
    Ok(name.to_string())
}

/// Runs a multi-vector MaxSim query and returns the top-k ids (best first).
///
/// # Errors
/// Returns an error if the query fails to build, execute, or decode.
async fn query_topk(
    backend: &LanceDbBackend,
    table: &str,
    query: &[Vec<f32>],
    nprobes: Option<usize>,
) -> Result<Vec<u64>> {
    if query.is_empty() {
        return Err(anyhow!("empty query"));
    }
    let batches: Vec<RecordBatch> = backend
        .multivector_search(
            table,
            "vector",
            query,
            K,
            DistanceMetric::Cosine,
            FilterExpr::Literal(true),
            VectorQueryOpts {
                nprobes,
                ..Default::default()
            },
            None,
        )
        .await?;

    let mut out: Vec<(u64, f32)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("missing id column"))?;
        let dist = batch
            .column_by_name("_distance")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
            .ok_or_else(|| anyhow!("missing _distance column"))?;
        for row in 0..batch.num_rows() {
            out.push((ids.value(row), dist.value(row)));
        }
    }
    out.sort_by(|a, b| a.1.total_cmp(&b.1)); // ascending distance = best first
    Ok(out.into_iter().take(K).map(|(id, _)| id).collect())
}

/// Prints `recall@k` of `got` against the ground-truth `truth`.
fn report(label: &str, truth: &[u64], got: &[u64]) {
    let truth_set: BTreeSet<u64> = truth.iter().copied().collect();
    let hit = got.iter().filter(|id| truth_set.contains(id)).count();
    let recall = hit as f32 / truth.len() as f32;
    println!(
        "{label:>14}: recall@{K} = {recall:.3}  ({hit}/{})  top-k={got:?}",
        truth.len()
    );
}
