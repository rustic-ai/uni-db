// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! ann-benchmarks protocol: **recall@10 vs QPS** on SIFT-1M.
//!
//! The industry currency for an ANN index is the Pareto curve — how much recall
//! you keep at a given query rate, swept across the knob that trades one for the
//! other. `dense_retrieval.rs` reports recall and latency *separately*, on a
//! synthetic corpus, against an oracle of our own construction, from a **single**
//! query vector. This bench is the other experiment:
//!
//! * a **real** corpus (SIFT-1M, 128-d, L2),
//! * **externally defined** ground truth — SIFT ships the true top-100 per query,
//!   so recall is not scored against anything we computed,
//! * **many** queries, so recall is a mean rather than one sample,
//! * **QPS**, not per-query latency, plotted against recall.
//!
//! # Why ≥1M vectors is not negotiable
//!
//! Lance falls back to brute force below a size threshold. `fork_index_recall_bench.rs`
//! reports recall@10 = 1.000 at n=1000 for exactly that reason — the index under
//! test never runs. A recall curve measured on a corpus small enough to brute
//! force is measuring the brute force. The full base set is the default here and
//! a subset is opt-in, loudly labelled.
//!
//! # Ground truth and subsets
//!
//! SIFT's `sift_groundtruth.ivecs` is the true top-100 **over the full million**.
//! Ingest a prefix of the base set and those answers are wrong for the corpus
//! actually present — a neighbour ranked 3rd overall may be absent entirely. So:
//!
//! * full corpus → file ground truth, and recall is externally defined;
//! * subset → ground truth is **recomputed** by brute force over the subset, and
//!   every report line says so, because it is then a weaker claim.
//!
//! Using the file's answers against a subset would silently deflate recall and
//! make an index look broken when it is not. It is refused rather than warned.
//!
//! # Running
//!
//! ```bash
//! python3 scripts/fixtures/fetch.py --only sift1m-base --only sift1m-query \
//!     --only sift1m-groundtruth
//! cargo bench -p uni-db --bench ann_pareto
//!
//! # A quick shape check on a subset (ground truth recomputed, and labelled).
//! ANN_DOCS=50000 ANN_QUERIES=50 cargo bench -p uni-db --bench ann_pareto
//! ```

#![recursion_limit = "256"]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use uni_db::{
    DataType, IndexType, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};

#[path = "common/ann_fixtures.rs"]
mod ann_fixtures;

use ann_fixtures::{fixture, l2_sq, read_fvecs, read_ivecs};

/// SIFT-1M dimensionality.
const DIM: usize = 128;
/// Retrieved per query.
const K: usize = 10;
/// Vectors in the full base set.
const FULL: usize = 1_000_000;
/// Ground-truth neighbours per query in `sift_groundtruth.ivecs`.
const GT_DEPTH: usize = 100;
/// Rows per `bulk_insert_vertices` call.
const BATCH: usize = 10_000;

/// Index families to run, comma-separated (`ANN_INDEXES`). Lets a long sweep be
/// split across invocations, and lets a failure be localized to one family
/// without paying for the others.
fn families() -> Vec<String> {
    std::env::var("ANN_INDEXES")
        .unwrap_or_else(|_| "flat,hnsw,ivf_pq".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .map(|s| s.trim().parse().unwrap_or_else(|_| panic!("{var}: {s:?}")))
        .unwrap_or(default)
}

/// One point on the Pareto curve.
struct Point {
    index: String,
    knob: String,
    recall: f64,
    qps: f64,
}

/// The index under test for a sweep cell.
fn algo(kind: &str) -> VectorAlgo {
    match kind {
        "flat" => VectorAlgo::Flat,
        // `partitions: None` resolves to **one** IVF partition
        // (`index_manager.rs`, `num_partitions.unwrap_or(1)`), whose internal
        // doc calls it an "auto" default. One partition means no IVF pruning is
        // possible, so every query touches a single cell holding the entire
        // corpus and cost grows with n. `ANN_HNSW_PARTITIONS` overrides it.
        "hnsw" => VectorAlgo::Hnsw {
            m: env_usize("ANN_HNSW_M", 16) as u32,
            ef_construction: 100,
            partitions: match env_usize("ANN_HNSW_PARTITIONS", 0) {
                0 => None,
                p => Some(p as u32),
            },
        },
        // Same graph, three payload encodings, so the on-disk index size varies by
        // roughly an order of magnitude while the search work stays comparable.
        // If query cost tracks index size rather than search effort, the
        // per-query index load is what dominates.
        "hnsw_flat" => VectorAlgo::HnswFlat {
            m: 16,
            ef_construction: 100,
            partitions: Some(1024),
        },
        "hnsw_pq" => VectorAlgo::HnswPq {
            m: 16,
            ef_construction: 100,
            sub_vectors: 16,
            partitions: Some(1024),
        },
        // sqrt(N) partitions is the usual IVF rule of thumb; 16 sub-vectors over
        // 128 dims is 8 dims per sub-quantizer.
        "ivf_pq" => VectorAlgo::IvfPq {
            partitions: 1024,
            sub_vectors: 16,
        },
        other => unreachable!("unknown index kind: {other}"),
    }
}

/// Build a flushed, indexed corpus from the first `n` SIFT base vectors.
async fn setup(base: &[Vec<f32>], kind: &str) -> anyhow::Result<Uni> {
    // `query_timeout` defaults to 30s, which is an interactive-latency guard, not
    // a benchmark budget. An exact Flat scan over a million 128-d vectors runs
    // past it -- measured, not assumed: at n=1M the build completes (insert 5.3s,
    // commit 6.1s, flush 10.0s) and the *query* is what trips the deadline. A
    // bench whose job is to time the exact baseline must not be bounded by it.
    let config = UniConfig {
        query_timeout: Duration::from_secs(600),
        // `Uni::flush()` -> `flush_to_l1` drains in-flight async flushes bounded
        // by this field and *discards the result* (`writer.rs`, `let _ =
        // coord.drain(...)`). Its default is 10s. Above roughly 600k rows the
        // drain does not finish, flush returns Ok having broken the barrier its
        // own rustdoc promises, the L1 table is absent, and the index build is
        // skipped as `NotAttempted` -- which is mapped to `Online`. The measured
        // consequence is recall 0.999 with ef_search/nprobes completely inert.
        // Raised here so the corpus is genuinely flushed before indexing; the
        // underlying silent-success is a product defect, recorded in
        // docs/perf/ann-2026-08-25.md.
        drop_fork_drain_timeout: Duration::from_secs(900),
        ..Default::default()
    };
    let db = Uni::temporary().config(config).build().await?;
    db.schema()
        .label("Doc")
        .property("idx", DataType::Int)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: algo(kind),
                // SIFT's ground truth is Euclidean. Scoring an L2 ground truth
                // with a cosine index would not error — it would quietly measure
                // recall against the wrong neighbours.
                metric: VectorMetric::L2,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    // Bulk path, not one CREATE per row: a million single-statement inserts is
    // parse-and-plan bound and would dominate setup.
    //
    // Each stage is timed and announced. A 1M-vector build is minutes of silence
    // otherwise, and when it failed the log said only which *function* died --
    // localizing it cost a three-hour run.
    let t = Instant::now();
    let tx = db.session().tx().await?;
    for (chunk, vectors) in base.chunks(BATCH).enumerate() {
        let props: Vec<HashMap<String, Value>> = vectors
            .iter()
            .enumerate()
            .map(|(j, v)| {
                let mut p = HashMap::new();
                p.insert("idx".to_string(), Value::Int((chunk * BATCH + j) as i64));
                p.insert("emb".to_string(), Value::Vector(v.clone()));
                p
            })
            .collect();
        tx.bulk_insert_vertices("Doc", props).await?;
    }
    eprintln!("[ann]   {kind}: insert {:.1}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    tx.commit().await?;
    eprintln!("[ann]   {kind}: commit {:.1}s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    db.flush().await?;
    eprintln!("[ann]   {kind}: flush {:.1}s", t.elapsed().as_secs_f64());

    // Force the ANN structure over the whole flushed corpus, so the measured
    // query exercises the index rather than a residual brute-force fallback.
    let t = Instant::now();
    db.indexes().rebuild("Doc", false).await?;
    eprintln!("[ann]   {kind}: index {:.1}s", t.elapsed().as_secs_f64());
    Ok(db)
}

/// One top-`K` query, returning base indices.
async fn query_once(db: &Uni, q: &[f32], options: &str) -> anyhow::Result<Vec<u32>> {
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {options}) \
         YIELD node, score RETURN node.idx AS idx"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param("q", Value::Vector(q.to_vec()))
        .param("k", Value::Int(K as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<i64>("idx").unwrap() as u32)
        .collect())
}

/// Print the plan for one vector query, to localize where the time goes.
async fn explain(db: &Uni, q: &[f32], options: &str) -> anyhow::Result<()> {
    let cypher = format!(
        "EXPLAIN CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {options}) \
         YIELD node, score RETURN node.idx AS idx"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param("q", Value::Vector(q.to_vec()))
        .param("k", Value::Int(K as i64))
        .fetch_all()
        .await?;
    for r in rows {
        for (c, v) in r.columns().iter().zip(r.values()) {
            println!("[ann][plan] {c} = {v:?}");
        }
    }
    Ok(())
}

/// Mean recall@K and QPS over `queries`, measured in one pass.
async fn measure(
    db: &Uni,
    queries: &[Vec<f32>],
    truth: &[Vec<u32>],
    options: &str,
) -> anyhow::Result<(f64, f64)> {
    let mut hits = 0usize;
    let start = Instant::now();
    for (qi, q) in queries.iter().enumerate() {
        let got = query_once(db, q, options).await?;
        let want: std::collections::HashSet<u32> = truth[qi].iter().take(K).copied().collect();
        hits += got.iter().filter(|i| want.contains(i)).count();
    }
    let elapsed = start.elapsed().as_secs_f64();
    let recall = hits as f64 / (queries.len() * K) as f64;
    let qps = queries.len() as f64 / elapsed;
    Ok((recall, qps))
}

/// Brute-force top-`GT_DEPTH` under L2 over the subset actually ingested.
fn recompute_truth(base: &[Vec<f32>], queries: &[Vec<f32>]) -> Vec<Vec<u32>> {
    queries
        .iter()
        .map(|q| {
            let mut scored: Vec<(u32, f64)> = base
                .iter()
                .enumerate()
                .map(|(i, d)| (i as u32, l2_sq(q, d)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            scored.truncate(GT_DEPTH);
            scored.into_iter().map(|(i, _)| i).collect()
        })
        .collect()
}

fn main() {
    // No criterion harness: criterion measures per-iteration latency, and the
    // quantity here is queries-per-second over a fixed query set alongside the
    // recall of those same queries. Both come from one pass, so the timing loop
    // is written directly rather than borrowed.
    if std::env::args().any(|a| a == "--test") {
        eprintln!("[ann] --test mode: this bench measures a curve, not a smoke; skipping");
        return;
    }

    let n = env_usize("ANN_DOCS", FULL);
    let nq = env_usize("ANN_QUERIES", 100);
    let rt = Runtime::new().unwrap();

    let base_path = fixture("sift1m-base");
    let query_path = fixture("sift1m-query");
    let gt_path = fixture("sift1m-groundtruth");

    eprintln!("[ann] reading corpus…");
    let base = read_fvecs(&base_path, DIM, Some(n));
    assert_eq!(
        base.len(),
        n,
        "asked for {n} base vectors, read {}",
        base.len()
    );
    let queries = read_fvecs(&query_path, DIM, Some(nq));
    assert_eq!(queries.len(), nq);

    // Ground truth: external when the whole corpus is present, recomputed
    // otherwise. Never the file's answers against a prefix.
    let (truth, truth_kind) = if n == FULL {
        (read_ivecs(&gt_path, GT_DEPTH, Some(nq)), "external (SIFT)")
    } else {
        eprintln!(
            "[ann] corpus is a {n}-vector prefix, not the full {FULL}; SIFT's ground truth does \
             not describe it. Recomputing by brute force…"
        );
        (recompute_truth(&base, &queries), "recomputed (subset)")
    };
    assert_eq!(truth.len(), nq);

    let mut points: Vec<Point> = Vec::new();

    // Exact baseline. This is also the experiment's own correctness check: if a
    // Flat index does not agree with the ground truth, then the metric, the
    // ingest order or the truth source is misaligned and every other number
    // below is meaningless.
    let fams = families();
    assert!(
        fams.contains(&"flat".to_string()),
        "the flat cell is the experiment's correctness check (it validates metric, ingest order \
         and truth source); ANN_INDEXES must include it"
    );

    eprintln!("[ann] building flat…");
    let flat = rt.block_on(setup(&base, "flat")).unwrap();
    // The exact cell is an anchor and a floor, not a curve. At 1M it costs tens
    // of seconds per query, so it runs over a prefix of the query set by default
    // -- enough to establish recall alignment without spending an hour on a
    // single point. The count is reported, never implied.
    let nq_flat = env_usize("ANN_FLAT_QUERIES", nq.min(10));
    let flat_queries = &queries[..nq_flat];
    let flat_truth = &truth[..nq_flat];
    let (flat_recall, flat_qps) = rt
        .block_on(measure(&flat, flat_queries, flat_truth, "{}"))
        .unwrap();
    println!(
        "[ann] corpus=sift1m n={n} queries={nq_flat} truth={truth_kind} index=flat \
         recall@{K}={flat_recall:.4} qps={flat_qps:.3}"
    );
    points.push(Point {
        index: "flat".into(),
        knob: format!("exact, {nq_flat} queries"),
        recall: flat_recall,
        qps: flat_qps,
    });
    drop(flat);

    // HNSW, swept on the query-time beam width.
    for fam in ["hnsw", "hnsw_flat", "hnsw_pq"] {
        if fams.contains(&fam.to_string()) {
            eprintln!("[ann] building {fam}…");
            let hnsw = rt.block_on(setup(&base, fam)).unwrap();
            for ef in [10usize, 25, 50, 100, 200, 400] {
                let opts = format!("{{ef_search: {ef}}}");
                if std::env::var("ANN_EXPLAIN").is_ok() && ef == 100 {
                    println!("[ann][plan] ===== {fam} ef_search={ef} =====");
                    let _ = rt.block_on(explain(&hnsw, &queries[0], &opts));
                }
                let (recall, qps) = rt
                    .block_on(measure(&hnsw, &queries, &truth, &opts))
                    .unwrap();
                println!(
                    "[ann] corpus=sift1m n={n} queries={nq} truth={truth_kind} index={fam} \
             ef_search={ef} recall@{K}={recall:.4} qps={qps:.1}"
                );
                points.push(Point {
                    index: fam.into(),
                    knob: format!("ef_search={ef}"),
                    recall,
                    qps,
                });
            }
            drop(hnsw);
        }
    }

    // IVF_PQ, swept on probe count.
    if fams.contains(&"ivf_pq".to_string()) {
        eprintln!("[ann] building ivf_pq…");
        let ivf = rt.block_on(setup(&base, "ivf_pq")).unwrap();
        for np in [1usize, 4, 16, 64, 128] {
            let opts = format!("{{nprobes: {np}}}");
            if std::env::var("ANN_EXPLAIN").is_ok() && np == 16 {
                println!("[ann][plan] ===== ivf_pq nprobes={np} =====");
                let _ = rt.block_on(explain(&ivf, &queries[0], &opts));
            }
            let (recall, qps) = rt.block_on(measure(&ivf, &queries, &truth, &opts)).unwrap();
            println!(
                "[ann] corpus=sift1m n={n} queries={nq} truth={truth_kind} index=ivf_pq \
             nprobes={np} recall@{K}={recall:.4} qps={qps:.1}"
            );
            points.push(Point {
                index: "ivf_pq".into(),
                knob: format!("nprobes={np}"),
                recall,
                qps,
            });
        }
        drop(ivf);
    }

    report(&points, n, nq, truth_kind, flat_recall);
}

fn report(points: &[Point], n: usize, nq: usize, truth_kind: &str, flat_recall: f64) {
    println!("\n## ANN Pareto — SIFT-1M\n");
    println!("corpus = sift1m, n = {n}, queries = {nq}, K = {K}, truth = {truth_kind}\n");
    println!("| index | knob | recall@{K} | QPS |");
    println!("|---|---|---:|---:|");
    for p in points {
        println!(
            "| {} | {} | {:.4} | {:.1} |",
            p.index, p.knob, p.recall, p.qps
        );
    }
    println!();

    // --- non-vacuity ------------------------------------------------------
    //
    // Three ways this bench could print a table describing nothing.

    // 1. The exact index must agree with the ground truth. This validates the
    //    metric, the ingest order and the truth source in one assertion. Not 1.0
    //    exactly: ties at the K'th distance can legitimately reorder.
    if flat_recall < 0.99 {
        eprintln!(
            "[ann] VACUOUS: a Flat (exact) index recalls only {flat_recall:.4} against the \
             ground truth. The metric, the ingest order or the truth source is misaligned, so \
             every recall below is measuring the wrong thing."
        );
        std::process::exit(1);
    }

    // 2. The knob must move recall. If every cell of a swept index returns the
    //    same recall, the knob is not reaching the engine and the "curve" is a
    //    flat line drawn through one measurement repeated.
    for family in ["hnsw", "ivf_pq"] {
        let rs: Vec<f64> = points
            .iter()
            .filter(|p| p.index == family)
            .map(|p| p.recall)
            .collect();
        // A family excluded by ANN_INDEXES contributes no points; that is a
        // deliberate narrowing, not a vacuous sweep.
        if rs.len() < 2 {
            continue;
        }
        let spread = rs.iter().cloned().fold(f64::MIN, f64::max)
            - rs.iter().cloned().fold(f64::MAX, f64::min);
        if spread == 0.0 {
            eprintln!(
                "[ann] VACUOUS: every {family} cell returned recall {:.4}. The sweep knob is not \
                 changing the search, so this is one measurement repeated, not a curve.",
                rs[0]
            );
            std::process::exit(1);
        }
    }

    // 3. A corpus small enough for Lance to brute-force makes every ANN cell
    //    exact, which is the `fork_index_recall_bench.rs` trap. Perfect recall
    //    across a whole family at every knob setting is that signature.
    for family in ["hnsw", "ivf_pq"] {
        let ran: Vec<&Point> = points.iter().filter(|p| p.index == family).collect();
        let all_perfect = !ran.is_empty() && ran.iter().all(|p| p.recall >= 0.9999);
        if all_perfect && n < FULL {
            eprintln!(
                "[ann] SUSPECT: every {family} cell is exact at n={n}. Below the full corpus \
                 Lance may brute-force, in which case the index under test never ran. Re-run at \
                 n={FULL} before citing these numbers."
            );
            std::process::exit(1);
        }
    }

    // Name what was actually checked. An earlier revision printed "both swept
    // families moved recall" unconditionally -- including on a run where
    // ANN_INDEXES excluded both, so the witness asserted something it had not
    // looked at. A summary that overstates its own coverage is the same defect
    // class this bench exists to catch.
    let swept: Vec<&str> = ["hnsw", "ivf_pq"]
        .into_iter()
        .filter(|f| points.iter().filter(|p| p.index == *f).count() >= 2)
        .collect();
    if swept.is_empty() {
        println!(
            "[ann] non-vacuity: flat recall {flat_recall:.4} confirms metric/truth alignment. No \
             family was swept, so no recall/QPS trade-off was measured -- this run is an anchor \
             only, not a curve."
        );
    } else {
        println!(
            "[ann] non-vacuity: flat recall {flat_recall:.4} confirms metric/truth alignment; \
             {} moved recall across the sweep.",
            swept.join(" and ")
        );
    }
}
