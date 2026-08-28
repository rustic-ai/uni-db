// Rust guideline compliant
//! Validation probe for native multi-vector (ColBERT / MaxSim) support in the
//! pinned Lance stack (`lance = 7.0.0`), issue #96.
//!
//! This is a throwaway de-risking harness, not shipping code. It answers the two
//! Tier-0 questions from `docs/proposals/multivector_colbert_maxsim.md` before we
//! commit to an implementation:
//!
//!   1. Does a `List<FixedSizeList<Float32, dim>>` column + a multi-vector query
//!      yield MaxSim-ordered results through the SAME `StorageBackend` API surface
//!      uni-store uses in production (`backend/lance.rs::vector_search`)?
//!   2. Which distance metric (L2 / Cosine / Dot) reproduces canonical ColBERT
//!      MaxSim (`score = Σ_i max_j q_i·d_j`) ordering?
//!
//! It also exercises the storage half: rows carry a VARIABLE number of token
//! vectors (1, 2, and 3 per row), which is the whole point of multi-vector.
//!
//! Run with (the `lance-backend` feature is on by default):
//!   cargo run -p uni-store --example multivec_lance_probe

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, RecordBatch, UInt64Array};
use tempfile::TempDir;
use uni_store::backend::StorageBackend;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::types::{DistanceMetric, FilterExpr, VectorQueryOpts};

/// Embedding dimension for the probe's token vectors.
const DIM: i32 = 3;

/// One document: a stable id and its set of per-token vectors (the multi-vector).
struct Doc {
    id: u64,
    tokens: Vec<[f32; 3]>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Orthonormal basis so dot products are trivial to hand-verify.
    let e0 = [1.0_f32, 0.0, 0.0];
    let e1 = [0.0_f32, 1.0, 0.0];
    let e2 = [0.0_f32, 0.0, 1.0];

    // Query has two token vectors: q0 = e0, q1 = e1.
    // MaxSim(doc) = max_j(e0·d_j) + max_j(e1·d_j).
    let query: Vec<[f32; 3]> = vec![e0, e1];

    // Docs with deliberately VARIABLE token counts (1, 2, 3) so we prove the
    // storage layer accepts a variable-count set per row.
    let docs = vec![
        Doc {
            id: 1,
            tokens: vec![e0, e1],
        }, // MaxSim = 1 + 1 = 2.0  (2 tokens)
        Doc {
            id: 2,
            tokens: vec![e0, e2, e1],
        }, // MaxSim = 1 + 1 = 2.0  (3 tokens)
        Doc {
            id: 3,
            tokens: vec![e0],
        }, // MaxSim = 1 + 0 = 1.0  (1 token)
        Doc {
            id: 4,
            tokens: vec![e2],
        }, // MaxSim = 0 + 0 = 0.0  (1 token)
    ];

    // Hand-computed expectation: ids 1 and 2 tie for the top (2.0), id 3 next
    // (1.0), id 4 last (0.0). A correct MaxSim ranking puts {1,2} before {3}
    // before {4} -- i.e. lowest Lance `_distance` for the highest MaxSim.
    println!("Expected MaxSim (dot): id1=2.0, id2=2.0, id3=1.0, id4=0.0");
    println!("Expected ranking:      [{{1,2}} (tie)] > 3 > 4\n");

    let tmp = TempDir::new().context("create temp dir")?;
    let uri = tmp.path().to_str().context("temp path is not utf-8")?;

    let backend = LanceDbBackend::connect(uri, None)
        .await
        .context("connect")?;

    let batch = build_batch(&docs)?;
    backend
        .create_table("docs", vec![batch])
        .await
        .context("create_table")?;

    println!(
        "Stored {} docs in a List<FixedSizeList<Float32,{DIM}>> column.\n",
        docs.len()
    );

    // Probe each metric. We assert ordering only for the metrics that should
    // reproduce MaxSim (Dot, and Cosine since vectors are unit-norm); L2 is
    // printed for reference.
    for (metric, assert_maxsim) in [
        (DistanceMetric::Dot, true),
        (DistanceMetric::Cosine, true),
        (DistanceMetric::L2, false),
    ] {
        let ranked = run_maxsim_query(&backend, &query, metric)
            .await
            .with_context(|| format!("multivector query with {metric:?}"))?;

        let order: Vec<u64> = ranked.iter().map(|(id, _)| *id).collect();
        println!("{metric:?}: returned order (best->worst) = {order:?}");
        for (id, dist) in &ranked {
            println!("    id={id}  _distance={dist:.4}");
        }

        if assert_maxsim {
            verify_maxsim_order(metric, &order)?;
            println!("    OK: {metric:?} reproduces MaxSim ranking.\n");
        } else {
            println!("    (reference only, no assertion)\n");
        }
    }

    println!(
        "PROBE PASSED: native multi-vector storage + MaxSim retrieval works \
              on lance 7.0.0 via the production StorageBackend API, with \
              no vector index required."
    );
    Ok(())
}

/// Builds a `{id: UInt64, vector: List<FixedSizeList<Float32, DIM>>}` batch.
///
/// # Errors
/// Returns an error if Arrow batch assembly fails.
fn build_batch(docs: &[Doc]) -> Result<RecordBatch> {
    let ids = UInt64Array::from(docs.iter().map(|d| d.id).collect::<Vec<_>>());

    // ListBuilder<FixedSizeListBuilder<Float32Builder>> == List<FixedSizeList<f32>>.
    let mut vectors = ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), DIM));
    for doc in docs {
        for tok in &doc.tokens {
            vectors.values().values().append_slice(tok);
            vectors.values().append(true);
        }
        vectors.append(true);
    }
    let vectors = vectors.finish();

    // Derive the schema from the built arrays so the nested field names/nullability
    // match exactly what Lance expects.
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn Array>),
        ("vector", Arc::new(vectors) as Arc<dyn Array>),
    ])
    .context("assemble record batch")
}

/// Runs a multi-vector (MaxSim) search and returns `(id, _distance)` best-first.
///
/// Runs the multi-vector (MaxSim) query through the production backend API,
/// which is the point of the probe: it exercises the same
/// `StorageBackend::multivector_search` path real queries take, against a
/// `List`-typed column.
///
/// # Errors
/// Returns an error if the query fails to build, execute, or decode.
async fn run_maxsim_query(
    backend: &LanceDbBackend,
    query: &[[f32; 3]],
    metric: DistanceMetric,
) -> Result<Vec<(u64, f32)>> {
    if query.is_empty() {
        return Err(anyhow!("empty query"));
    }
    let tokens: Vec<Vec<f32>> = query.iter().map(|t| t.to_vec()).collect();
    let batches: Vec<RecordBatch> = backend
        .multivector_search(
            "docs",
            "vector",
            &tokens,
            docs_count(),
            metric,
            FilterExpr::Literal(true),
            VectorQueryOpts::default(),
            None,
        )
        .await?;

    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("missing/typed-wrong id column"))?;
        let dist = batch
            .column_by_name("_distance")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
            .ok_or_else(|| anyhow!("missing/typed-wrong _distance column"))?;
        for row in 0..batch.num_rows() {
            out.push((ids.value(row), dist.value(row)));
        }
    }
    // Lance returns ascending `_distance` (closest first); keep that order but
    // make it explicit so the assertion does not depend on backend stability.
    out.sort_by(|a, b| a.1.total_cmp(&b.1));
    Ok(out)
}

/// Number of docs in the fixture (used as the query `limit`).
fn docs_count() -> usize {
    4
}

/// Asserts the returned order matches the hand-computed MaxSim ranking.
///
/// The two MaxSim=2.0 docs (ids 1, 2) must occupy the first two positions in
/// either order, id 3 third, id 4 last.
///
/// # Errors
/// Returns an error describing the mismatch if the ordering is wrong.
fn verify_maxsim_order(metric: DistanceMetric, order: &[u64]) -> Result<()> {
    if order.len() != 4 {
        return Err(anyhow!(
            "{metric:?}: expected 4 results, got {}",
            order.len()
        ));
    }
    let top_two: std::collections::BTreeSet<u64> = order[..2].iter().copied().collect();
    let expected_top: std::collections::BTreeSet<u64> = [1, 2].into_iter().collect();
    if top_two != expected_top {
        return Err(anyhow!(
            "{metric:?}: top-2 should be {{1,2}} (MaxSim=2.0), got {top_two:?}"
        ));
    }
    if order[2] != 3 {
        return Err(anyhow!(
            "{metric:?}: rank-3 should be id 3 (MaxSim=1.0), got {}",
            order[2]
        ));
    }
    if order[3] != 4 {
        return Err(anyhow!(
            "{metric:?}: rank-4 should be id 4 (MaxSim=0.0), got {}",
            order[3]
        ));
    }
    Ok(())
}
