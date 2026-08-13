// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! E2E correctness tests for the dense-vector (KNN) index — parity with the
//! gold-standard sparse suite (`sparse_index.rs`).
//!
//! A `Vector` column + a `Flat` (exact, brute-force) Cosine index, queried via
//! `uni.vector.query`. The doc titled `"target"` has `emb == query`, so it is the
//! unique cosine maximizer; the engine's reported `score` is the EXACT
//! `(2 - cosine_distance) / 2 == (1 + cosine_similarity) / 2`, validated against
//! an independent brute-force oracle over the same corpus. Covers the backfill and
//! create-before-ingest build paths, the L0 (no-flush) union path, L0 update /
//! tombstone (MVCC) visibility, snapshot isolation, restart durability, the Cypher
//! DDL build path, and dense-property projection.
//!
//! `Flat` is chosen deliberately: it is an exact KNN, so the score is oracle-exact
//! on every path (no ANN approximation), matching the sparse suite's bar.

use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

/// Vector dimensionality — small enough for fast tests, large enough that random
/// `[-1, 1]` vectors are near-orthogonal so the `target` self-match (cos = 1) is a
/// clear, unique maximizer.
const DIM: usize = 16;

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
    /// A component in `[-1, 1)`, so cosine similarity spans the full range and
    /// random docs cluster near orthogonal (score ≈ 0.5).
    fn component(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

/// A dense vector.
type Dense = Vec<f32>;

fn random_dense(rng: &mut Rng) -> Dense {
    (0..DIM).map(|_| rng.component()).collect()
}

/// The fixed query vector (distinct non-trivial components).
fn query_vec() -> Dense {
    (0..DIM).map(|i| ((i as f32) - 7.5) / 8.0).collect()
}

fn vec_value(v: &Dense) -> Value {
    Value::Vector(v.clone())
}

/// Brute-force cosine-derived score ground truth in f64: `(1 + cos(q, d)) / 2`,
/// the exact value [`uni_query_functions::similar_to::calculate_score`] produces
/// for a `Cosine` metric (Lance returns `1 - cos` as the distance). Higher is
/// better; range `[0, 1]`; a self-match scores `1.0`.
fn dense_score_oracle(q: &Dense, d: &Dense) -> f64 {
    let dot: f64 = q.iter().zip(d).map(|(&x, &y)| x as f64 * y as f64).sum();
    let nq = q.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
    let nd = d.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
    let cos = if nq == 0.0 || nd == 0.0 {
        0.0
    } else {
        dot / (nq * nd)
    };
    (1.0 + cos) / 2.0
}

/// Deterministic corpus: doc `n/2` titled `target` has `emb == query` (the unique
/// cosine maximizer); the rest are random vectors. Returned so a test can compute
/// the oracle over the exact same data.
fn build_corpus(n: usize, seed: u64) -> Vec<(String, Dense)> {
    let q = query_vec();
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            if i == n / 2 {
                ("target".to_string(), q.clone())
            } else {
                (format!("doc{i}"), random_dense(&mut rng))
            }
        })
        .collect()
}

async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index("emb", flat_cosine())
        .apply()
        .await?;
    Ok(())
}

/// Schema with the vector column but NO index (for create-before-ingest / backfill,
/// where the index is added separately).
async fn define_schema_no_index(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .apply()
        .await?;
    Ok(())
}

async fn add_index(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .index("emb", flat_cosine())
        .apply()
        .await?;
    Ok(())
}

/// The exact, brute-force KNN index used throughout (oracle-exact scores).
fn flat_cosine() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: None,
    })
}

async fn insert_docs(db: &Uni, docs: &[(String, Dense)], flush: bool) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    for (title, dense) in docs {
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(title.clone()))
            .param("emb", vec_value(dense))
            .run()
            .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(())
}

/// Run `uni.vector.query` for the standard query and return `(title, score)` in
/// engine rank order.
async fn query_results(db: &Uni, k: usize) -> anyhow::Result<Vec<(String, f64)>> {
    let rows = db
        .session()
        .query_with(
            "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", vec_value(&query_vec()))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

/// Assert engine results match the brute-force oracle: every returned doc carries
/// its EXACT cosine score, results are descending, and the top score equals the
/// oracle maximum (the `target` self-match at `1.0`).
fn assert_matches_oracle(engine: &[(String, f64)], corpus: &[(String, Dense)]) {
    const EPS: f64 = 1e-3;
    let q = query_vec();
    let oracle: std::collections::HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, d)| (t.as_str(), dense_score_oracle(&q, d)))
        .collect();

    for (title, score) in engine {
        let want = oracle
            .get(title.as_str())
            .unwrap_or_else(|| panic!("engine returned a title not in the corpus: {title:?}"));
        assert!(
            (score - want).abs() < EPS,
            "exact cosine-score mismatch for {title:?}: engine={score} oracle={want}"
        );
    }
    for w in engine.windows(2) {
        assert!(
            w[0].1 >= w[1].1 - EPS,
            "results not in descending score order: {engine:?}"
        );
    }
    let oracle_max = oracle.values().cloned().fold(f64::MIN, f64::max);
    if let Some((_, top)) = engine.first() {
        assert!(
            (top - oracle_max).abs() < EPS,
            "top score {top} != oracle max {oracle_max}"
        );
    }
}

// ---------------------------------------------------------------------------
// Build paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dense_backfill_then_query_matches_oracle() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    let corpus = build_corpus(60, 0xD1CE_5EED);
    insert_docs(&db, &corpus, true).await?; // flush so the backfill scan sees rows
    add_index(&db).await?; // backfill build path

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "target (emb == query) must rank first: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn dense_create_before_ingest_matches_oracle() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?; // index created on empty label
    let corpus = build_corpus(60, 0xBEEF_F00D);
    insert_docs(&db, &corpus, true).await?; // flush populates the index incrementally

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "flush-maintained index should retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn dense_index_via_cypher_create_vector_index() -> anyhow::Result<()> {
    // `CREATE VECTOR INDEX ... OPTIONS{type:'flat', metric:'cosine'}` builds a dense
    // KNN index (the default modality), create-before-ingest.
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE VECTOR INDEX emb_idx FOR (d:Doc) ON (d.emb) \
         OPTIONS {type: 'flat', metric: 'cosine'}",
    )
    .await?;
    tx.commit().await?;

    let corpus = build_corpus(50, 0xC17E_0095);
    insert_docs(&db, &corpus, true).await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "Cypher-created dense index must retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

// ---------------------------------------------------------------------------
// L0 union / flush equivalence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dense_l0_only_no_flush_matches_oracle() -> anyhow::Result<()> {
    // No flush: all rows live in L0. The query path must union L0 candidates and
    // re-score them, so results are correct without an index dataset on disk.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(40, 0x0A0A_0A0A);
    insert_docs(&db, &corpus, false).await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "L0-only query must find the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn dense_flush_equivalence() -> anyhow::Result<()> {
    // The ranking must be identical before and after a flush (consistency across
    // the L0 and flushed-index paths).
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(50, 0xF1F1_F1F1);
    insert_docs(&db, &corpus, false).await?;
    let before = query_results(&db, 10).await?;
    db.flush().await?;
    let after = query_results(&db, 10).await?;

    let names_before: Vec<&str> = before.iter().map(|(t, _)| t.as_str()).collect();
    let names_after: Vec<&str> = after.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        names_before, names_after,
        "ranking changed across flush: {names_before:?} vs {names_after:?}"
    );
    assert_matches_oracle(&after, &corpus);
    Ok(())
}

// ---------------------------------------------------------------------------
// L0 MVCC visibility — update / tombstone
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dense_l0_update_last_writer_wins() -> anyhow::Result<()> {
    // Flush a corpus, then in L0 overwrite `target`'s emb with `-query` (cosine
    // -1 → score 0). The stale flushed vector would still rank it first, but the
    // re-score must use the fresh L0 value → it drops out of the top.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(30, 0xABCD_1234);
    insert_docs(&db, &corpus, true).await?;

    let anti: Dense = query_vec().iter().map(|&x| -x).collect();
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (d:Doc {title: 'target'}) SET d.emb = $emb")
        .param("emb", vec_value(&anti))
        .run()
        .await?;
    tx.commit().await?;

    let results = query_results(&db, 10).await?;
    let target_score = results.iter().find(|(t, _)| t == "target").map(|(_, s)| *s);
    // target is now anti-parallel to the query → score ≈ 0; with random docs near
    // 0.5 it falls out of the top-k entirely.
    assert!(
        target_score.map(|s| s < 0.1).unwrap_or(true),
        "after L0 update target should no longer rank near the query: {target_score:?}"
    );
    assert_ne!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "anti-parallel target must not rank first: {results:?}"
    );
    Ok(())
}

#[tokio::test]
async fn dense_l0_tombstone_hides_flushed_doc() -> anyhow::Result<()> {
    // Flush a corpus, then delete `target` in L0. A tombstone must hide it from
    // results even though its flushed vector still maximizes the query.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(30, 0x5555_AAAA);
    insert_docs(&db, &corpus, true).await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 'target'}) DELETE d")
        .await?;
    tx.commit().await?;

    let results = query_results(&db, 10).await?;
    assert!(
        !results.iter().any(|(t, _)| t == "target"),
        "tombstoned target must not appear in results: {results:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Restart / reopen durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dense_persists_across_reopen() -> anyhow::Result<()> {
    // Build + flush the index, drop the db, reopen the same on-disk path, and
    // assert the query returns an identical, oracle-exact ranking.
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let corpus = build_corpus(60, 0xD00D_FEED);

    let before = {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &corpus, true).await?;
        let r = query_results(&db, 10).await?;
        assert_eq!(r.first().map(|(t, _)| t.as_str()), Some("target"));
        assert_matches_oracle(&r, &corpus);
        drop(db);
        r
    };

    let db = Uni::open(path).build().await?;
    let after = query_results(&db, 10).await?;
    let names_before: Vec<&str> = before.iter().map(|(t, _)| t.as_str()).collect();
    let names_after: Vec<&str> = after.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        names_before, names_after,
        "ranking changed across reopen: {names_before:?} vs {names_after:?}"
    );
    assert_matches_oracle(&after, &corpus);
    Ok(())
}

#[tokio::test]
async fn dense_wal_replay_after_reopen_unflushed_delta() -> anyhow::Result<()> {
    // Durability of UNFLUSHED dense writes through the WAL. Flush a base batch
    // first (creates the manifest), then commit a delta — including `target` —
    // WITHOUT flushing, then drop + reopen. The recovered rows land in L0 and the
    // vector read path unions them, so the query returns `target` first with EXACT
    // oracle scores (no index rebuild).
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let base: Vec<(String, Dense)> = (0..20)
        .map(|i| (format!("base{i}"), random_dense(&mut rng)))
        .collect();
    let mut delta: Vec<(String, Dense)> = vec![("target".to_string(), query_vec())];
    for i in 0..5 {
        delta.push((format!("delta{i}"), random_dense(&mut rng)));
    }

    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &base, true).await?; // flush → manifest
        insert_docs(&db, &delta, false).await?; // commit only, in WAL
        let r = query_results(&db, 10).await?;
        assert_eq!(r.first().map(|(t, _)| t.as_str()), Some("target"));
        drop(db);
    }

    let db = Uni::open(path).build().await?;
    let after = query_results(&db, 10).await?;
    assert_eq!(
        after.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "after WAL recovery, recovered dense data must be queryable with no rebuild: {after:?}"
    );
    let full: Vec<(String, Dense)> = base.into_iter().chain(delta).collect();
    assert_matches_oracle(&after, &full);
    Ok(())
}

// ---------------------------------------------------------------------------
// MVCC snapshot isolation
// ---------------------------------------------------------------------------

/// Run `uni.vector.query` inside a transaction's pinned snapshot.
async fn query_results_tx(tx: &uni_db::Transaction, k: usize) -> anyhow::Result<Vec<String>> {
    let rows = tx
        .query_with(
            "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", vec_value(&query_vec()))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_snapshot_isolates_reader_from_concurrent_insert() -> anyhow::Result<()> {
    // A reader transaction pins its snapshot at begin. A concurrent writer inserts
    // a new doc that maximizes the query and commits. The reader, querying within
    // its pinned snapshot, must NOT see the new doc.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(20, 0x5A4B_3C2D_1E0F_9876);
    insert_docs(&db, &corpus, true).await?;

    let s_r = db.session();
    let tx_r = s_r.tx().await?;
    let before = query_results_tx(&tx_r, 50).await?;
    assert!(
        before.contains(&"target".to_string()),
        "snapshot sees the seed corpus"
    );

    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String("late_arrival".to_string()))
            .param("emb", vec_value(&query_vec()))
            .run()
            .await?;
        tx_w.commit().await?;
    }

    let after = query_results_tx(&tx_r, 50).await?;
    assert!(
        !after.contains(&"late_arrival".to_string()),
        "reader snapshot must be isolated from the concurrent insert: {after:?}"
    );

    let live = query_results(&db, 50).await?;
    assert!(
        live.iter().any(|(t, _)| t == "late_arrival"),
        "live view should see the committed insert: {live:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Result-surface — projecting a dense vector in RETURN
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// HNSW query-time `ef_search` tuning
// ---------------------------------------------------------------------------

/// An approximate HNSW Cosine index. Unlike `Flat`, its recall depends on the
/// search-time beam width `ef_search`, so it exercises the query-time knob.
fn hnsw_cosine() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Hnsw {
            m: 16,
            ef_construction: 100,
            partitions: None,
        },
        metric: VectorMetric::Cosine,
        embedding: None,
    })
}

/// Recall@k of the engine's returned titles against the brute-force cosine-oracle
/// top-k for `q` over the same corpus.
fn recall_at_k(q: &Dense, engine: &[(String, f64)], corpus: &[(String, Dense)], k: usize) -> f64 {
    let mut scored: Vec<(&str, f64)> = corpus
        .iter()
        .map(|(t, d)| (t.as_str(), dense_score_oracle(q, d)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let truth: std::collections::HashSet<&str> = scored.iter().take(k).map(|(t, _)| *t).collect();
    let hit = engine
        .iter()
        .filter(|(t, _)| truth.contains(t.as_str()))
        .count();
    hit as f64 / truth.len().max(1) as f64
}

/// Run `uni.vector.query` for `q` with an explicit options literal
/// (e.g. `{ef_search: 512}`), returning `(title, score)` in engine rank order.
async fn query_results_opts(
    db: &Uni,
    q: &Dense,
    k: usize,
    options: &str,
) -> anyhow::Result<Vec<(String, f64)>> {
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {options}) \
         YIELD node, score RETURN node.title AS title, score"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param("q", vec_value(q))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

/// Mean recall@k across many deterministic probe queries.
///
/// A single recall@k sample is quantized in `1/k` steps and — because the HNSW
/// graph build is nondeterministic — occasionally lands on `1.0` even for the
/// default beam, which made a strict `high > low` assertion flaky (~1/4 runs).
/// Averaging over [`PROBE_QUERIES`] independent probes concentrates the mean so
/// the low-vs-wide beam separation is stable across graph builds.
async fn mean_recall(
    db: &Uni,
    corpus: &[(String, Dense)],
    queries: &[Dense],
    k: usize,
    options: &str,
) -> anyhow::Result<f64> {
    let mut total = 0.0;
    for q in queries {
        let engine = query_results_opts(db, q, k, options).await?;
        total += recall_at_k(q, &engine, corpus, k);
    }
    Ok(total / queries.len() as f64)
}

/// Number of probe queries the HNSW recall means are averaged over.
const PROBE_QUERIES: usize = 32;

/// Deterministic random probe queries (NOT planted in the corpus, so a narrow
/// beam has no trivially reachable exact match to get lucky on).
fn probe_queries(seed: u64) -> Vec<Dense> {
    let mut rng = Rng(seed);
    (0..PROBE_QUERIES).map(|_| random_dense(&mut rng)).collect()
}

#[tokio::test]
async fn dense_hnsw_ef_search_raises_recall() -> anyhow::Result<()> {
    // On an APPROXIMATE HNSW index, a narrow search beam under-explores the graph
    // and misses true neighbors; a wide `ef_search` must recover recall — proving
    // the query-time knob is plumbed end-to-end (regression: `ef_search` was
    // previously unparsed and never reached lancedb's `.ef()`, silently pinning
    // recall to the tiny default of `1.5 * k`).
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index("emb", hnsw_cosine())
        .apply()
        .await?;

    // ~1000 docs: large enough that HNSW with a narrow beam demonstrably misses
    // neighbors (the regime the recall bench surfaced at recall ≈ 0.6).
    let corpus = build_corpus(1000, 0xEF5E_A4C8);
    insert_docs(&db, &corpus, true).await?;
    db.indexes().rebuild("Doc", false).await?;

    // Mean recall over many probes: a single-query recall@10 sample is quantized
    // in 0.1 steps and — because the HNSW graph build is nondeterministic —
    // occasionally hit 1.000 even for the narrow beam, so the old strict
    // `high > low` flaked (~1/4 runs). Both beams go through the `ef_search`
    // knob (minimal `k` vs wide 512): if the option ever stops reaching the
    // index search again, both searches are identical and the gap is exactly 0.
    //
    // Thresholds calibrated over 30 independent builds (2026-07-01):
    // low ∈ [0.77, 0.89], high = 0.9875 constant, gap ∈ [0.097, 0.216]
    // (gap mean 0.163, σ 0.028 → 0.05 is ~4σ safe; high has 0.0375 headroom).
    let k = 10;
    let queries = probe_queries(0xBEA7_5EED);
    let low = mean_recall(&db, &corpus, &queries, k, "{ef_search: 10}").await?;
    let high = mean_recall(&db, &corpus, &queries, k, "{ef_search: 512}").await?;

    assert!(
        high - low >= 0.05,
        "ef_search must widen the beam and raise mean HNSW recall: ef_search=10 \
         mean recall@{k}={low:.3} vs ef_search=512 mean recall@{k}={high:.3} \
         (a zero gap means the knob never reached the index search)"
    );
    assert!(
        high >= 0.95,
        "a wide ef_search should recover strong recall: got mean {high:.3} \
         (narrow beam {low:.3})"
    );
    Ok(())
}

/// **The ANN directional oracle.** Recall must be non-decreasing in `ef_search`
/// across a whole ladder, not merely between two hand-picked endpoints.
///
/// # Why a directional law rather than bag equality
///
/// Every other oracle here compares result *bags*. ANN knobs cannot be tested
/// that way: `ef_search` changes which neighbours come back **by design**, so
/// bag equality across two beam widths would fail on a perfectly healthy engine.
/// That is precisely why these knobs are excluded from the DQP levers. What
/// survives is a directional law — a wider beam explores more of the graph, so
/// it may not return *worse* recall.
///
/// # Why non-decreasing with a tolerance, and not strictly increasing
///
/// The HNSW graph build is nondeterministic and a single recall@k sample is
/// quantized in `1/k` steps, which is what made an earlier strict `high > low`
/// assertion flake in about one run in four. Two defences, both inherited from
/// that fix:
///
/// * every rung is a **mean over [`PROBE_QUERIES`] probes**, not one query;
/// * adjacent rungs are compared with [`RUNG_TOLERANCE`] slack, because two
///   neighbouring beam widths can legitimately invert by a hair.
///
/// The tolerance is what keeps this from being a flake generator. What keeps it
/// from being vacuous is the separate end-to-end gap assertion: slack alone
/// would let a completely unplumbed knob pass, since a constant series is
/// trivially non-decreasing.
#[tokio::test]
async fn dense_hnsw_recall_is_monotone_in_ef_search() -> anyhow::Result<()> {
    /// Slack allowed between two *adjacent* rungs.
    ///
    /// From the calibration recorded on `dense_hnsw_ef_search_raises_recall`:
    /// mean-recall σ ≈ 0.028 over 30 builds, so 0.05 is comfortably outside the
    /// noise while still rejecting a genuine inversion.
    const RUNG_TOLERANCE: f64 = 0.05;
    /// End-to-end separation the ladder must achieve, matching the two-point
    /// test's threshold. This is the non-vacuity guard.
    const MIN_END_TO_END_GAIN: f64 = 0.05;

    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index("emb", hnsw_cosine())
        .apply()
        .await?;

    let corpus = build_corpus(1000, 0xEF5E_A4C8);
    insert_docs(&db, &corpus, true).await?;
    db.indexes().rebuild("Doc", false).await?;

    let k = 10;
    let queries = probe_queries(0xBEA7_5EED);
    let ladder = [10usize, 20, 40, 80, 160, 320];

    let mut recalls = Vec::with_capacity(ladder.len());
    for ef in ladder {
        let r = mean_recall(&db, &corpus, &queries, k, &format!("{{ef_search: {ef}}}")).await?;
        recalls.push(r);
    }

    let series: Vec<String> = ladder
        .iter()
        .zip(&recalls)
        .map(|(ef, r)| format!("{ef}:{r:.3}"))
        .collect();
    let series = series.join(" ");

    for w in 1..ladder.len() {
        let (prev, cur) = (recalls[w - 1], recalls[w]);
        assert!(
            cur >= prev - RUNG_TOLERANCE,
            "recall fell when the beam widened: ef_search {} -> {} took mean \
             recall@{k} {prev:.3} -> {cur:.3}, a drop of {:.3} beyond the {:.2} \
             tolerance. A wider beam explores a superset of the graph, so this \
             is a real inversion, not noise.\n  series: {series}",
            ladder[w - 1],
            ladder[w],
            prev - cur,
            RUNG_TOLERANCE,
        );
    }

    let gain = recalls[recalls.len() - 1] - recalls[0];
    assert!(
        gain >= MIN_END_TO_END_GAIN,
        "the ladder gained only {gain:.3} recall from ef_search {} to {}. \
         Monotonicity alone is satisfied by a constant series, so without a real \
         gain this oracle would pass even if `ef_search` never reached the index \
         search at all — which is exactly the regression the two-point test was \
         written for.\n  series: {series}",
        ladder[0],
        ladder[ladder.len() - 1],
    );
    Ok(())
}

#[tokio::test]
async fn dense_property_projection_in_return() -> anyhow::Result<()> {
    // `RETURN d.emb` must materialise the dense `Value::Vector` result column (not
    // fall back to a Utf8 string column). Covers both the L0 and flushed paths.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let q = query_vec();
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'p', emb: $emb})")
        .param("emb", vec_value(&q))
        .run()
        .await?;
    tx.commit().await?;

    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        let rows = db
            .session()
            .query("MATCH (d:Doc {title: 'p'}) RETURN d.emb AS emb")
            .await?;
        assert_eq!(rows.rows().len(), 1);
        match rows.rows()[0].value("emb") {
            Some(Value::Vector(v)) => {
                assert_eq!(v, &q, "projected dense vector (flush={flush})");
            }
            other => panic!("expected projected Vector (flush={flush}), got {other:?}"),
        }
    }
    Ok(())
}
