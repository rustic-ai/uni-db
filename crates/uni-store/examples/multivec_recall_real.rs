// Rust guideline compliant
//! Recall@k benchmark for native multi-vector (ColBERT / MaxSim) indexing on
//! **real** ColBERT embeddings, issue #96 Phase-2 gate.
//!
//! Companion to `multivec_recall_bench.rs` (which used synthetic random vectors).
//! This one reads per-token multi-vectors produced by the local uni-xervo ColBERT
//! model (via `uni-xervo/examples/encode_corpus_multivec.rs` over a real IR
//! corpus), so the recall numbers reflect real, semantically-clustered embeddings
//! — the fair test of whether a native first-stage index can reproduce MaxSim.
//!
//! It measures `recall@k` and latency (averaged over ALL queries) of the pinned
//! Lance stack for no-index brute force (the baseline), `IVF_PQ + refine_factor`
//! across an nprobes sweep (the speed/recall frontier), and `IVF_HNSW_SQ`.
//!
//! Inputs: `$BENCH_DIR/docs.bin`, `$BENCH_DIR/queries.bin` (MVEC format; `OUT_DIR`
//! is avoided because cargo reserves it). Run:
//! `BENCH_DIR=/tmp/colbert_bench cargo run -p uni-store --example multivec_recall_real --release`

use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
use arrow_array::{Array, Float32Array, RecordBatch, UInt64Array};
use uni_common::muvera::{DEFAULT_FDE_SEED, FdeEncoder, FdeParams};
use uni_store::backend::StorageBackend;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::types::{
    DistanceMetric, FilterExpr, VectorIndexKind, VectorIndexParams, VectorQueryOpts,
};

/// Top-k for recall.
const K: usize = 10;
/// IVF partitions.
const N_PARTITIONS: u32 = 256;

/// A document/query: its index and per-token vectors.
struct Item {
    tokens: Vec<Vec<f32>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // NB: not `OUT_DIR` — cargo reserves that env var for build scripts.
    let out_dir = std::env::var("BENCH_DIR").unwrap_or_else(|_| "/tmp/colbert_bench".to_string());
    let (dim, docs) = read_mvec(&format!("{out_dir}/docs.bin")).context("read docs.bin")?;
    let (qdim, queries) =
        read_mvec(&format!("{out_dir}/queries.bin")).context("read queries.bin")?;
    anyhow::ensure!(dim == qdim, "doc/query dim mismatch: {dim} vs {qdim}");

    let total_tokens: usize = docs.iter().map(|d| d.tokens.len()).sum();
    let avg_q: f64 =
        queries.iter().map(|q| q.tokens.len()).sum::<usize>() as f64 / queries.len() as f64;
    println!(
        "Real ColBERT corpus: {} docs ({total_tokens} tokens), {} queries (avg {avg_q:.0} tokens), dim {dim}, k={K}\n",
        docs.len(),
        queries.len(),
    );

    // Brute-force MaxSim ground-truth top-k per query.
    let truth: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| top_k_ids(brute_force_maxsim(&docs, &q.tokens)))
        .collect();

    let tmp = tempfile::TempDir::new().context("temp dir")?;
    let uri = tmp.path().to_str().context("non-utf8 path")?;
    let backend = LanceDbBackend::connect(uri, None)
        .await
        .context("connect")?;

    // no-index (Flat / brute force) — sanity (≈1.0) AND the latency baseline a
    // first-stage index must beat to be worth building.
    let flat = build_table(&backend, "flat", &docs, dim).await?;
    report(
        "no-index (Flat, exact)",
        &avg_recall(&backend, &flat, &queries, &truth, None, None).await?,
    );

    // IVF_PQ: the only config that cleared the recall gate (with refine). Sweep
    // nprobes to find the speed/recall operating point — a first-stage index only
    // earns its keep if recall stays >=0.95 at LOW nprobes (where it is fast).
    let pq = build_table(&backend, "pq", &docs, dim).await?;
    let nsub = pick_num_sub_vectors(dim);
    backend
        .create_vector_index(
            &pq,
            "vector",
            "pq_ivf",
            VectorIndexParams {
                kind: VectorIndexKind::IvfPq {
                    num_partitions: N_PARTITIONS,
                    num_sub_vectors: nsub,
                    num_bits: 8,
                },
                metric: DistanceMetric::Cosine,
            },
        )
        .await
        .context("IVF_PQ")?;
    for np in [8usize, 16, 32, 64, 128, 256] {
        let label = format!("IVF_PQ+refine10 (np={np})");
        report(
            &label,
            &avg_recall(&backend, &pq, &queries, &truth, Some(np), Some(10)).await?,
        );
    }

    // IVF_HNSW_SQ at full probing, for comparison.
    let hnsw = build_table(&backend, "hnsw", &docs, dim).await?;
    backend
        .create_vector_index(
            &hnsw,
            "vector",
            "hnsw_sq",
            VectorIndexParams {
                kind: VectorIndexKind::HnswSq {
                    // Lance's HnswBuildParams::default(), which is what
                    // lancedb's builder default resolved to.
                    m: 20,
                    ef_construction: 150,
                    num_partitions: N_PARTITIONS,
                },
                metric: DistanceMetric::Cosine,
            },
        )
        .await
        .context("IVF_HNSW_SQ")?;
    report(
        "IVF_HNSW_SQ (np=256)",
        &avg_recall(
            &backend,
            &hnsw,
            &queries,
            &truth,
            Some(N_PARTITIONS as usize),
            None,
        )
        .await?,
    );

    println!(
        "\nGate: a native first-stage index is worth building if it reaches recall@{K} >=0.95\n\
         at an nprobes LOW enough to be faster than the no-index (Flat) baseline above."
    );

    // ---------------------------------------------------------------------
    // Phase-3 MUVERA: encode each multi-vector into ONE fixed-dim FDE vector,
    // run a single-vector ANN over the FDE column (Dot ≈ MaxSim), then re-rank
    // the candidates by EXACT MaxSim. Sweep (k_sim, reps, d_proj) and the
    // retrieval over-fetch to find the recall/dim/latency frontier vs the
    // native multi-vector index above.
    // ---------------------------------------------------------------------
    println!("\n=== MUVERA (FDE first-stage + exact MaxSim re-rank) ===");
    for &(k_sim, reps, d_proj) in &[(4u32, 20u32, 16u32), (4, 8, 8), (5, 20, 16)] {
        let params = FdeParams {
            k_sim,
            reps,
            d_proj,
            input_dim: dim as u32,
            seed: DEFAULT_FDE_SEED,
        };
        if params.validate().is_err() {
            println!("  (k_sim={k_sim} reps={reps} d_proj={d_proj}) invalid params, skipped");
            continue;
        }
        let fde_dim = params.fde_dim();
        let encoder = FdeEncoder::new(&params).map_err(|e| anyhow!("FDE encoder: {e}"))?;
        let name = format!("fde_{k_sim}_{reps}_{d_proj}");
        let fde_table = build_fde_table(&backend, &name, &docs, &encoder, fde_dim).await?;
        let nsub = pick_num_sub_vectors(fde_dim);
        backend
            .create_vector_index(
                &fde_table,
                "fde",
                "fde_ivf",
                VectorIndexParams {
                    kind: VectorIndexKind::IvfPq {
                        num_partitions: N_PARTITIONS,
                        num_sub_vectors: nsub,
                        num_bits: 8,
                    },
                    metric: DistanceMetric::Dot,
                },
            )
            .await
            .context("MUVERA IVF_PQ over FDE")?;
        for &over in &[1usize, 2, 4, 8] {
            let retrieval_k = (K * over).max(K);
            let r = muvera_recall(
                &backend,
                &fde_table,
                &docs,
                &queries,
                &truth,
                &encoder,
                retrieval_k,
                Some(16),
            )
            .await?;
            println!(
                "  MUVERA k_sim={k_sim} reps={reps} d_proj={d_proj} fde_dim={fde_dim} over={over}x: \
                 recall@{K} = {:.3}   {:.1} ms/query",
                r.0, r.1
            );
        }
    }
    println!(
        "\nGate: MUVERA is worth building if recall@{K} >=0.95 after re-rank at LOWER total\n\
         query latency than the native multi-vector IVF_PQ above at equal recall."
    );
    Ok(())
}

/// Build a `{id, fde: FixedSizeList<Float32, fde_dim>}` table of document FDEs.
async fn build_fde_table(
    backend: &LanceDbBackend,
    name: &str,
    docs: &[Item],
    encoder: &FdeEncoder,
    fde_dim: usize,
) -> Result<String> {
    let ids = UInt64Array::from((0..docs.len() as u64).collect::<Vec<_>>());
    let mut fde = FixedSizeListBuilder::new(Float32Builder::new(), fde_dim as i32);
    for doc in docs {
        let v = encoder
            .encode_doc(&doc.tokens)
            .map_err(|e| anyhow!("encode_doc: {e}"))?;
        fde.values().append_slice(&v);
        fde.append(true);
    }
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as Arc<dyn Array>),
        ("fde", Arc::new(fde.finish()) as Arc<dyn Array>),
    ])
    .context("assemble FDE batch")?;
    backend
        .create_table(name, vec![batch])
        .await
        .with_context(|| format!("create FDE table {name}"))?;
    Ok(name.to_string())
}

/// Exact MaxSim of one query against one doc's tokens: `Σ_i max_j cos(q_i, d_j)`.
fn maxsim_one(query: &[Vec<f32>], doc: &[Vec<f32>]) -> f32 {
    query
        .iter()
        .map(|q| {
            doc.iter()
                .map(|d| cos(q, d))
                .fold(f32::NEG_INFINITY, f32::max)
        })
        .sum()
}

/// Mean recall@k and latency for MUVERA: FDE ANN over-fetch then exact MaxSim re-rank.
#[allow(clippy::too_many_arguments)]
async fn muvera_recall(
    backend: &LanceDbBackend,
    fde_table: &str,
    docs: &[Item],
    queries: &[Item],
    truth: &[Vec<u64>],
    encoder: &FdeEncoder,
    retrieval_k: usize,
    nprobes: Option<usize>,
) -> Result<(f64, f64)> {
    let mut sum = 0.0;
    let start = std::time::Instant::now();
    for (q, t) in queries.iter().zip(truth) {
        let fq = encoder
            .encode_query(&q.tokens)
            .map_err(|e| anyhow!("encode_query: {e}"))?;
        let cand = fde_ann_topk(backend, fde_table, &fq, retrieval_k, nprobes).await?;
        let mut scored: Vec<(u64, f32)> = cand
            .iter()
            .map(|&id| (id, maxsim_one(&q.tokens, &docs[id as usize].tokens)))
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        let got: Vec<u64> = scored.into_iter().take(K).map(|(id, _)| id).collect();
        let tset: BTreeSet<u64> = t.iter().copied().collect();
        let hit = got.iter().filter(|id| tset.contains(id)).count();
        sum += hit as f64 / t.len() as f64;
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0 / queries.len() as f64;
    Ok((sum / queries.len() as f64, ms))
}

/// Single-vector ANN over the FDE column (Dot), returning candidate ids best-first.
async fn fde_ann_topk(
    backend: &LanceDbBackend,
    table: &str,
    fde_query: &[f32],
    retrieval_k: usize,
    nprobes: Option<usize>,
) -> Result<Vec<u64>> {
    let batches: Vec<RecordBatch> = backend
        .vector_search(
            table,
            "fde",
            fde_query,
            retrieval_k,
            DistanceMetric::Dot,
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
    out.sort_by(|a, b| a.1.total_cmp(&b.1));
    Ok(out
        .into_iter()
        .take(retrieval_k)
        .map(|(id, _)| id)
        .collect())
}

/// Reads an `MVEC` binary file: `(dim, items)` where each item is ragged tokens.
///
/// # Errors
/// Returns an error if the file is missing or has a bad header.
fn read_mvec(path: &str) -> Result<(usize, Vec<Item>)> {
    let bytes = std::fs::read(path).with_context(|| format!("read {path}"))?;
    let mut p = 0usize;
    let u32_at = |bytes: &[u8], p: &mut usize| -> u32 {
        let v = u32::from_le_bytes(bytes[*p..*p + 4].try_into().unwrap());
        *p += 4;
        v
    };
    anyhow::ensure!(
        u32_at(&bytes, &mut p) == 0x4D56_4543,
        "bad MVEC magic in {path}"
    );
    let dim = u32_at(&bytes, &mut p) as usize;
    let n = u32_at(&bytes, &mut p) as usize;
    let mut items = Vec::with_capacity(n);
    for _ in 0..n {
        let n_tokens = u32_at(&bytes, &mut p) as usize;
        let mut tokens = Vec::with_capacity(n_tokens);
        for _ in 0..n_tokens {
            let mut tok = Vec::with_capacity(dim);
            for _ in 0..dim {
                tok.push(f32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()));
                p += 4;
            }
            tokens.push(tok);
        }
        items.push(Item { tokens });
    }
    Ok((dim, items))
}

/// Largest divisor of `dim` that keeps sub-vectors small (≤ dim/4-ish), for PQ.
fn pick_num_sub_vectors(dim: usize) -> u32 {
    for cand in [dim / 4, dim / 3, dim / 2, 16, 8, 4] {
        if cand > 0 && dim.is_multiple_of(cand) {
            return cand as u32;
        }
    }
    1
}

/// Cosine similarity of two (unit-norm) vectors == dot product.
fn cos(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Brute-force MaxSim per doc against `query`: `Σ_i max_j cos(q_i, d_j)`, desc.
fn brute_force_maxsim(docs: &[Item], query: &[Vec<f32>]) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = docs
        .iter()
        .enumerate()
        .map(|(id, doc)| {
            let score: f32 = query
                .iter()
                .map(|q| {
                    doc.tokens
                        .iter()
                        .map(|d| cos(q, d))
                        .fold(f32::NEG_INFINITY, f32::max)
                })
                .sum();
            (id as u64, score)
        })
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1));
    scored
}

fn top_k_ids(ranked: Vec<(u64, f32)>) -> Vec<u64> {
    ranked.into_iter().take(K).map(|(id, _)| id).collect()
}

/// Builds and stores a `{id, vector: List<FixedSizeList<Float32, dim>>}` table.
///
/// # Errors
/// Returns an error if Arrow assembly or table creation fails.
async fn build_table(
    backend: &LanceDbBackend,
    name: &str,
    docs: &[Item],
    dim: usize,
) -> Result<String> {
    let ids = UInt64Array::from((0..docs.len() as u64).collect::<Vec<_>>());
    let mut vectors =
        ListBuilder::new(FixedSizeListBuilder::new(Float32Builder::new(), dim as i32));
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

/// Mean `recall@k` and mean query latency (ms) over all queries for a config.
///
/// # Errors
/// Returns an error if any query fails to execute.
async fn avg_recall(
    backend: &LanceDbBackend,
    table: &str,
    queries: &[Item],
    truth: &[Vec<u64>],
    nprobes: Option<usize>,
    refine: Option<u32>,
) -> Result<(f64, f64)> {
    let mut sum = 0.0;
    let start = std::time::Instant::now();
    for (q, t) in queries.iter().zip(truth) {
        let got = query_topk(backend, table, &q.tokens, nprobes, refine).await?;
        let tset: BTreeSet<u64> = t.iter().copied().collect();
        let hit = got.iter().filter(|id| tset.contains(id)).count();
        sum += hit as f64 / t.len() as f64;
    }
    let ms_per_query = start.elapsed().as_secs_f64() * 1000.0 / queries.len() as f64;
    Ok((sum / queries.len() as f64, ms_per_query))
}

/// Runs one multi-vector MaxSim query and returns the top-k ids (best first).
///
/// # Errors
/// Returns an error if the query fails to build, execute, or decode.
async fn query_topk(
    backend: &LanceDbBackend,
    table: &str,
    query: &[Vec<f32>],
    nprobes: Option<usize>,
    refine: Option<u32>,
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
                refine_factor: refine,
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
    out.sort_by(|a, b| a.1.total_cmp(&b.1));
    Ok(out.into_iter().take(K).map(|(id, _)| id).collect())
}

fn report(label: &str, r: &(f64, f64)) {
    println!("{label:>28}: recall@{K} = {:.3}   {:.1} ms/query", r.0, r.1);
}
