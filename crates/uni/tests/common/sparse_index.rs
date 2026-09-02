// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! E2E correctness tests for the scored sparse-vector index (issue #95, set B/C).
//!
//! A `SparseVector` column + a sparse index scored by dot product, queried via
//! `uni.sparse.query`. The doc titled `"target"` has `emb == query`, so it is
//! the unique dot-product maximizer; the engine's reported `score` is the EXACT
//! `sparse_dot`, validated against an independent brute-force oracle over the
//! same corpus. Covers the backfill and create-before-ingest build paths, the
//! L0 (no-flush) union path, and L0 update / tombstone (MVCC) visibility.

use std::collections::{BTreeMap, HashMap};
use uni_db::{DataType, IndexType, Uni, Value};

const VOCAB: usize = 1000;

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
    fn weight(&mut self) -> f32 {
        // Positive weights in [0.1, ~1.1), matching learned-sparse (ReLU) output.
        ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) + 0.1
    }
    fn term(&mut self) -> u32 {
        (self.next_u64() % VOCAB as u64) as u32
    }
}

/// A sparse vector as parallel sorted-unique `(indices, values)`.
type Sparse = (Vec<u32>, Vec<f32>);

fn random_sparse(rng: &mut Rng, nnz: usize) -> Sparse {
    let mut m: BTreeMap<u32, f32> = BTreeMap::new();
    while m.len() < nnz {
        let t = rng.term();
        let w = rng.weight();
        m.insert(t, w);
    }
    (m.keys().copied().collect(), m.values().copied().collect())
}

/// The fixed query sparse vector (distinct terms, positive weights).
fn query_vec() -> Sparse {
    (vec![1, 5, 9, 42, 77], vec![1.0, 2.0, 3.0, 0.5, 1.5])
}

fn sv_value((indices, values): &Sparse) -> Value {
    Value::SparseVector {
        indices: indices.clone(),
        values: values.clone(),
    }
}

/// Brute-force dot-product ground truth in f64.
fn sparse_dot_oracle(q: &Sparse, d: &Sparse) -> f64 {
    let qm: HashMap<u32, f64> = q.0.iter().zip(&q.1).map(|(&t, &w)| (t, w as f64)).collect();
    d.0.iter()
        .zip(&d.1)
        .filter_map(|(&t, &w)| qm.get(&t).map(|qw| qw * w as f64))
        .sum()
}

/// Deterministic corpus: doc `n/2` titled `target` has `emb == query` (the
/// unique dot maximizer); the rest are random sparse vectors. Returned so a test
/// can compute the oracle over the exact same data.
fn build_corpus(n: usize, seed: u64) -> Vec<(String, Sparse)> {
    let q = query_vec();
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            if i == n / 2 {
                ("target".to_string(), q.clone())
            } else {
                (format!("doc{i}"), random_sparse(&mut rng, 8))
            }
        })
        .collect()
}

async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::sparse(VOCAB))
        .apply()
        .await?;
    Ok(())
}

/// Schema with the sparse column but NO index (for create-before-ingest, where
/// the index is added separately).
async fn define_schema_no_index(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .apply()
        .await?;
    Ok(())
}

async fn add_index(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .index("emb", IndexType::sparse(VOCAB))
        .apply()
        .await?;
    Ok(())
}

/// Schema + sparse index with an explicit `quantize` setting. The default-on
/// (quantized) path is exercised by [`define_schema`]; `quantize = false` stores
/// lossless f32 postings (the legacy / back-compat layout).
async fn define_schema_quantize(db: &Uni, quantize: bool) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize,
                embedding: None,
            },
        )
        .apply()
        .await?;
    Ok(())
}

async fn insert_docs(db: &Uni, docs: &[(String, Sparse)], flush: bool) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    for (title, sparse) in docs {
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(title.clone()))
            .param("emb", sv_value(sparse))
            .run()
            .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(())
}

/// Run `uni.sparse.query` for the standard query and return `(title, score)` in
/// engine rank order.
async fn query_results(db: &Uni, k: usize) -> anyhow::Result<Vec<(String, f64)>> {
    let q = query_vec();
    let rows = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", sv_value(&q))
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

/// Assert engine results match the brute-force oracle: every returned doc
/// carries its EXACT dot score, results are descending, and the top score
/// equals the oracle maximum (the `target`).
fn assert_matches_oracle(engine: &[(String, f64)], corpus: &[(String, Sparse)]) {
    const EPS: f64 = 1e-3;
    let q = query_vec();
    let oracle: HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, s)| (t.as_str(), sparse_dot_oracle(&q, s)))
        .collect();

    for (title, score) in engine {
        let want = oracle
            .get(title.as_str())
            .unwrap_or_else(|| panic!("engine returned a title not in the corpus: {title:?}"));
        assert!(
            (score - want).abs() < EPS,
            "exact dot-score mismatch for {title:?}: engine={score} oracle={want}"
        );
    }
    // Descending order.
    for w in engine.windows(2) {
        assert!(
            w[0].1 >= w[1].1 - EPS,
            "results not in descending score order: {engine:?}"
        );
    }
    // Top == oracle maximum (the target self-match).
    let oracle_max = oracle.values().cloned().fold(f64::MIN, f64::max);
    if let Some((_, top)) = engine.first() {
        assert!(
            (top - oracle_max).abs() < EPS,
            "top score {top} != oracle max {oracle_max}"
        );
    }
}

#[tokio::test]
async fn sparse_backfill_then_query_matches_oracle() -> anyhow::Result<()> {
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
async fn sparse_create_before_ingest_matches_oracle() -> anyhow::Result<()> {
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
async fn sparse_quantize_false_is_lossless() -> anyhow::Result<()> {
    // `quantize:false` stores f32 postings (the legacy / back-compat read path).
    let db = Uni::temporary().build().await?;
    define_schema_quantize(&db, false).await?;
    let corpus = build_corpus(60, 0x10C5_1E55);
    insert_docs(&db, &corpus, true).await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "lossless sparse index must retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_quantized_and_lossless_agree() -> anyhow::Result<()> {
    // 8-bit quantization is ≈ lossless: the quantized (default) index and the
    // lossless index return the same ranked candidates on the same corpus, and
    // both carry the exact `sparse_dot` score (the orchestration layer always
    // re-scores from the lossless stored vector, so quantization can only ever
    // perturb the candidate set within the over-fetch margin).
    let corpus = build_corpus(80, 0x5EED_0095);

    let build_and_query = |quantize: bool| {
        let corpus = corpus.clone();
        async move {
            let db = Uni::temporary().build().await?;
            define_schema_quantize(&db, quantize).await?;
            insert_docs(&db, &corpus, true).await?;
            query_results(&db, 10).await
        }
    };

    let quant = build_and_query(true).await?;
    let lossless = build_and_query(false).await?;

    let quant_titles: Vec<&str> = quant.iter().map(|(t, _)| t.as_str()).collect();
    let lossless_titles: Vec<&str> = lossless.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        quant_titles, lossless_titles,
        "quantized vs lossless ranking diverged: {quant:?} vs {lossless:?}"
    );
    assert_matches_oracle(&quant, &corpus);
    assert_matches_oracle(&lossless, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_index_via_create_index_proc_quantize_false() -> anyhow::Result<()> {
    // Procedure index-creation path — `uni.schema.createIndex` with
    // `{type:'sparse', quantize:false}` (create-before-ingest) — confirming the
    // `quantize` option is threaded through `ddl_procedures`.
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CALL uni.schema.createIndex('Doc', 'emb', {type: 'sparse', quantize: false})")
        .await?;
    tx.commit().await?;

    let corpus = build_corpus(50, 0xDD15_0095);
    insert_docs(&db, &corpus, true).await?; // flush populates the index incrementally

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "procedure-created sparse index must retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_index_via_create_index_proc_after_flush() -> anyhow::Result<()> {
    // Regression for the create-after-flush count-guard fix: creating the sparse
    // index via the procedure AFTER rows are flushed must backfill them (the raw
    // dataset count reads 0 for the flushed LanceDB table, so the build must not
    // be gated by it).
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    let corpus = build_corpus(50, 0xAF7E_0095);
    insert_docs(&db, &corpus, true).await?; // flush BEFORE the index exists

    let tx = db.session().tx().await?;
    tx.execute("CALL uni.schema.createIndex('Doc', 'emb', {type: 'sparse'})")
        .await?;
    tx.commit().await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "procedure index built after flush must backfill + retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

/// Create a sparse index via the Cypher `CREATE VECTOR INDEX … OPTIONS{type:'sparse'}`
/// statement (the routing fixed for #95).
async fn create_sparse_index_cypher(db: &Uni, opts: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE VECTOR INDEX emb_idx FOR (d:Doc) ON (d.emb) OPTIONS {opts}"
    ))
    .await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn sparse_index_via_cypher_create_vector_index() -> anyhow::Result<()> {
    // `CREATE VECTOR INDEX ... OPTIONS{type:'sparse'}` must build a SPARSE index
    // (not a dense one), create-before-ingest.
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    create_sparse_index_cypher(&db, "{type: 'sparse'}").await?;

    let corpus = build_corpus(50, 0xC17E_0095);
    insert_docs(&db, &corpus, true).await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "Cypher-created sparse index must retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_index_via_cypher_create_after_flush() -> anyhow::Result<()> {
    // Same Cypher statement, but created AFTER the rows are flushed: the executor
    // routes to `create_sparse_vector_index`, which backfills via the backend.
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    let corpus = build_corpus(50, 0xC17F_0095);
    insert_docs(&db, &corpus, true).await?; // flush BEFORE the index exists
    create_sparse_index_cypher(&db, "{type: 'sparse'}").await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "Cypher index built after flush must backfill + retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_index_via_cypher_quantize_false() -> anyhow::Result<()> {
    // The `quantize` option is threaded through the Cypher routing too.
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    create_sparse_index_cypher(&db, "{type: 'sparse', quantize: false}").await?;

    let corpus = build_corpus(50, 0xC180_0095);
    insert_docs(&db, &corpus, true).await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "Cypher quantize:false sparse index must retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_l0_only_no_flush_matches_oracle() -> anyhow::Result<()> {
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
async fn sparse_flush_equivalence() -> anyhow::Result<()> {
    // The ranking must be identical before and after a flush (scale consistency
    // across the L0 and flushed-index paths).
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

#[tokio::test]
async fn sparse_l0_update_last_writer_wins() -> anyhow::Result<()> {
    // Flush a corpus, then in L0 overwrite `target`'s emb with a vector that no
    // longer matches the query. The stale flushed posting still makes it a
    // candidate, but the re-score uses the fresh L0 value → it drops out of top.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(30, 0xABCD_1234);
    insert_docs(&db, &corpus, true).await?;

    // Overwrite target with a disjoint sparse vector (no query terms).
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (d:Doc {title: 'target'}) SET d.emb = $emb")
        .param(
            "emb",
            Value::SparseVector {
                indices: vec![500, 600, 700],
                values: vec![1.0, 1.0, 1.0],
            },
        )
        .run()
        .await?;
    tx.commit().await?;

    let results = query_results(&db, 10).await?;
    let target_score = results.iter().find(|(t, _)| t == "target").map(|(_, s)| *s);
    // target now shares no terms with the query → score 0 (or absent from top-k).
    assert!(
        target_score.map(|s| s.abs() < 1e-6).unwrap_or(true),
        "after L0 update target should no longer match the query: {target_score:?}"
    );
    Ok(())
}

#[tokio::test]
async fn sparse_l0_tombstone_hides_flushed_doc() -> anyhow::Result<()> {
    // Flush a corpus, then delete `target` in L0. A tombstone must hide it from
    // results even though its flushed posting still matches the query.
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
// Set G — restart / reopen durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparse_persists_across_reopen() -> anyhow::Result<()> {
    // Build + flush the index, drop the db, reopen the same on-disk path, and
    // assert the query returns an identical, oracle-exact ranking — the flushed
    // postings dataset and schema-registered index survive a restart.
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

    // Reopen: WAL/manifest recovery restores the flushed dataset + index config.
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
async fn sparse_wal_replay_after_reopen_unflushed_delta() -> anyhow::Result<()> {
    // Durability of UNFLUSHED sparse writes through the WAL (tagged-msgpack CV
    // codec, TAG_SPARSE_VECTOR). The engine requires a snapshot manifest to
    // reopen, so we flush a base batch first (creates the manifest), then commit
    // a second batch — including `target` — WITHOUT flushing, then drop + reopen.
    //
    // This is the sparse-specific concern: the sparse VALUE must survive WAL
    // recovery — the CV codec round-trips the `SparseVector` weights through the
    // WAL (not a degraded untagged-serde `Map`), so the recovered query returns
    // `target` first with EXACT oracle scores.
    //
    // NOTE: no index rebuild is needed here. WAL recovery does not repopulate the
    // postings index, but it does not have to: the recovered rows land in L0 and
    // the `sparse_rerank` read path unions live L0 candidates. The companion test
    // `sparse_recovered_delta_queryable_without_rebuild` pins both that L0-union
    // path AND the flush-time re-index (L1-only) path explicitly.
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    // Base batch: random docs only (no target), flushed → creates the manifest.
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let base: Vec<(String, Sparse)> = (0..20)
        .map(|i| (format!("base{i}"), random_sparse(&mut rng, 8)))
        .collect();
    // Delta batch: the target (emb == query) + a few randoms, committed unflushed.
    let mut delta: Vec<(String, Sparse)> = vec![("target".to_string(), query_vec())];
    for i in 0..5 {
        delta.push((format!("delta{i}"), random_sparse(&mut rng, 8)));
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

    // The WAL serializes mutation values through the explicit CV codec, so the
    // unflushed delta — including `target`'s sparse vector — recovers as a
    // genuine `Value::SparseVector` (not a degraded untagged-serde `Map`). No
    // index rebuild: the recovered rows are in L0 and the sparse read path unions
    // them, so the query returns `target` first with EXACT oracle scores — which
    // is only possible if the recovered weights survived intact through the WAL.
    let after = query_results(&db, 10).await?;
    assert_eq!(
        after.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "after WAL recovery, recovered sparse data must be queryable with no rebuild: {after:?}"
    );
    let full: Vec<(String, Sparse)> = base.into_iter().chain(delta).collect();
    assert_matches_oracle(&after, &full);
    Ok(())
}

// ---------------------------------------------------------------------------
// Set D — MVCC snapshot isolation
// ---------------------------------------------------------------------------

/// Run `uni.sparse.query` inside a transaction's pinned snapshot.
async fn query_results_tx(tx: &uni_db::Transaction, k: usize) -> anyhow::Result<Vec<String>> {
    let q = query_vec();
    let rows = tx
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", sv_value(&q))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_snapshot_isolates_reader_from_concurrent_insert() -> anyhow::Result<()> {
    // A reader transaction pins its snapshot at begin. A concurrent writer
    // inserts a new doc that matches the query and commits. The reader, querying
    // within its pinned snapshot, must NOT see the new doc.
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

    // Concurrent writer inserts a brand-new matching doc and commits.
    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String("late_arrival".to_string()))
            .param("emb", sv_value(&query_vec()))
            .run()
            .await?;
        tx_w.commit().await?;
    }

    // The reader's pinned snapshot must not surface the post-begin insert.
    let after = query_results_tx(&tx_r, 50).await?;
    assert!(
        !after.contains(&"late_arrival".to_string()),
        "reader snapshot must be isolated from the concurrent insert: {after:?}"
    );

    // A fresh session (live view) DOES see it.
    let live = query_results(&db, 50).await?;
    assert!(
        live.iter().any(|(t, _)| t == "late_arrival"),
        "live view should see the committed insert: {live:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Result-surface — projecting a sparse property in RETURN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparse_property_projection_in_return() -> anyhow::Result<()> {
    // `RETURN d.emb` must materialise the sparse `Struct{indices,values}` result
    // column (not fall back to a Utf8 string column). Covers both the L0 and the
    // flushed read paths.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let (qi, qv) = query_vec();
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'p', emb: $emb})")
        .param("emb", sv_value(&(qi.clone(), qv.clone())))
        .run()
        .await?;
    tx.commit().await?;

    // Read once from L0, then again after a flush — both must round-trip.
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
            Some(Value::SparseVector { indices, values }) => {
                assert_eq!(indices, &qi, "projected sparse indices (flush={flush})");
                assert_eq!(values, &qv, "projected sparse values (flush={flush})");
            }
            other => panic!("expected projected SparseVector (flush={flush}), got {other:?}"),
        }
    }
    Ok(())
}

/// Regression: a sparse query reflects a recovered-but-unflushed delta WITHOUT
/// any explicit index rebuild — disproving the long-standing "secondary indexes
/// must be rebuilt after WAL recovery" belief that was previously asserted (only
/// as a comment) by `sparse_wal_replay_after_reopen_unflushed_delta`.
///
/// Two mechanisms make a rebuild unnecessary, and this test pins BOTH:
///   [A] L0-union read path — after reopen, the recovered rows live in L0 and the
///       `sparse_rerank` orchestration unions them (`collect_l0_label_candidates`),
///       so the query is correct with the postings index still cold.
///   [B] flush re-indexes — the next flush recomputes the sparse-index delta from a
///       FULL L0 scan (`writer.rs` `flush_stream_l1`), so the recovered rows are
///       written into the L1 postings index. After a SECOND reopen (WAL truncated,
///       L0 empty) the query is served purely from the L1 index — the only way it
///       can still find `target` is if that flush actually maintained the index.
#[tokio::test]
async fn sparse_recovered_delta_queryable_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let base: Vec<(String, Sparse)> = (0..20)
        .map(|i| (format!("base{i}"), random_sparse(&mut rng, 8)))
        .collect();
    let mut delta: Vec<(String, Sparse)> = vec![("target".to_string(), query_vec())];
    for i in 0..5 {
        delta.push((format!("delta{i}"), random_sparse(&mut rng, 8)));
    }
    let full: Vec<(String, Sparse)> = base.iter().cloned().chain(delta.iter().cloned()).collect();

    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &base, true).await?; // flush -> manifest
        insert_docs(&db, &delta, false).await?; // commit only, in WAL
        drop(db);
    }

    // [A] Reopen and query with the postings index still cold (NO rebuild, NO
    // flush). The recovered delta is served via the L0-union path.
    let db = Uni::open(path).build().await?;
    let after_reopen = query_results(&db, 10).await?;
    assert_eq!(
        after_reopen.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "recovered delta must be queryable via the L0-union path with no rebuild: {after_reopen:?}"
    );
    assert_matches_oracle(&after_reopen, &full);

    // [B] Flush (re-indexes the recovered rows into L1) then reopen again. The
    // WAL is now truncated and L0 is empty, so the query is served purely from
    // the L1 postings index — still with NO explicit rebuild.
    db.flush().await?;
    drop(db);

    let db = Uni::open(path).build().await?;
    let from_l1_index = query_results(&db, 10).await?;
    assert_eq!(
        from_l1_index.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "flush after recovery must maintain the L1 sparse index (no rebuild): {from_l1_index:?}"
    );
    assert_matches_oracle(&from_l1_index, &full);
    Ok(())
}

/// Regression: a recovered-but-unflushed UPDATE to an indexed sparse column is
/// honored without a rebuild — the stale flushed posting must NOT win.
///
/// Was flaky under parallel load with Lance's own "Retryable commit conflict …
/// preempted by concurrent transaction", and passed when run alone. The cause
/// was not in the test: `SparseIndex::write_postings` committed to Lance
/// without going through `retry_on_lance_conflict`, so a losing commit reached
/// the caller instead of being retried — as did the three sibling secondary
/// index writers. Retrying here would have hidden a defect users could hit.
///
/// `target` is flushed as the exact query match (the high-scoring L1 posting),
/// then overwritten in an unflushed commit with a disjoint vector. After a crash
/// (drop) + reopen the recovered L0 value must override the stale L1 posting, so
/// `target` drops out of the results. Pins both the L0-union path [A] and the
/// post-flush L1-only path [B] (the flush must rewrite the posting, not append).
#[tokio::test]
async fn sparse_recovered_update_overrides_stale_posting_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let corpus = build_corpus(20, 0x1357_9BDF);

    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &corpus, true).await?; // flush -> target indexed as the exact match

        // Overwrite target with a disjoint vector (no query terms), unflushed.
        let tx = db.session().tx().await?;
        tx.execute_with("MATCH (d:Doc {title: 'target'}) SET d.emb = $emb")
            .param(
                "emb",
                Value::SparseVector {
                    indices: vec![500, 600, 700],
                    values: vec![1.0, 1.0, 1.0],
                },
            )
            .run()
            .await?;
        tx.commit().await?; // committed, NOT flushed
        drop(db);
    }

    // [A] L0-union path: the recovered update overrides the stale L1 posting.
    let assert_target_dropped = |results: &[(String, f64)], stage: &str| {
        let score = results.iter().find(|(t, _)| t == "target").map(|(_, s)| *s);
        assert!(
            score.map(|s| s.abs() < 1e-6).unwrap_or(true),
            "[{stage}] recovered update ignored — stale high-scoring posting won: {score:?}"
        );
    };

    let db = Uni::open(path).build().await?;
    assert_target_dropped(&query_results(&db, 10).await?, "L0-union");

    // [B] L1-only path: the flush must rewrite target's posting to the new value.
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert_target_dropped(&query_results(&db, 10).await?, "L1-index");
    Ok(())
}

/// Regression: a recovered-but-unflushed DELETE of an indexed sparse doc is
/// honored without a rebuild — the flushed posting must not resurrect it.
///
/// `target` is flushed as the exact query match, then deleted in an unflushed
/// commit. After a crash (drop) + reopen the recovered tombstone must hide it
/// (L0-union path [A]); after flushing the tombstone and reopening again the doc
/// must be gone from the L1 index too (post-flush L1-only path [B]).
#[tokio::test]
async fn sparse_recovered_delete_hides_doc_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let corpus = build_corpus(20, 0x2468_ACE0);

    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &corpus, true).await?; // flush -> target indexed

        let tx = db.session().tx().await?;
        tx.execute("MATCH (d:Doc {title: 'target'}) DELETE d")
            .await?;
        tx.commit().await?; // committed, NOT flushed
        drop(db);
    }

    // [A] L0-union path: the recovered tombstone hides the flushed posting.
    let db = Uni::open(path).build().await?;
    let after_reopen = query_results(&db, 10).await?;
    assert!(
        !after_reopen.iter().any(|(t, _)| t == "target"),
        "recovered tombstone must hide the flushed posting (no rebuild): {after_reopen:?}"
    );

    // [B] L1-only path: the flush must remove target's posting from the L1 index.
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    let from_l1_index = query_results(&db, 10).await?;
    assert!(
        !from_l1_index.iter().any(|(t, _)| t == "target"),
        "flush after recovery must drop the deleted doc from the L1 index (no rebuild): {from_l1_index:?}"
    );
    Ok(())
}

/// Attempt to CREATE a `Doc` carrying `emb`, returning the write `Result` so a
/// test can assert acceptance or rejection of a (possibly malformed) value.
async fn try_insert_emb(db: &Uni, title: &str, emb: Value) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
        .param("title", Value::String(title.to_string()))
        .param("emb", emb)
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn sparse_malformed_value_is_rejected_not_panicked() -> anyhow::Result<()> {
    // Regression for issue #95: a malformed `Value::SparseVector` supplied through the
    // query-parameter surface previously reached the WAL value codec and `.expect()`-
    // panicked the commit/write task. Ingest validation must now reject it as a clean
    // `TypeError`, and canonicalize (rather than reject) merely-unsorted/duplicate input.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;

    // (a) Non-finite weight → rejected.
    assert!(
        try_insert_emb(
            &db,
            "nan",
            Value::SparseVector {
                indices: vec![1, 5],
                values: vec![f32::NAN, 2.0]
            },
        )
        .await
        .is_err(),
        "a NaN weight must be rejected at ingest"
    );

    // (b) Length mismatch → rejected.
    assert!(
        try_insert_emb(
            &db,
            "lenmismatch",
            Value::SparseVector {
                indices: vec![1, 2, 3],
                values: vec![1.0]
            },
        )
        .await
        .is_err(),
        "a length-mismatched sparse vector must be rejected at ingest"
    );

    // (c) Term id at/beyond the declared term space (dimensions = VOCAB) → rejected.
    assert!(
        try_insert_emb(
            &db,
            "outofrange",
            Value::SparseVector {
                indices: vec![VOCAB as u32],
                values: vec![1.0]
            },
        )
        .await
        .is_err(),
        "a term id >= dimensions must be rejected at ingest"
    );

    // (d) Unsorted + duplicate term ids → accepted and canonicalized (sorted, summed).
    try_insert_emb(
        &db,
        "canon",
        Value::SparseVector {
            indices: vec![9, 1, 9],
            values: vec![1.0, 2.0, 0.5],
        },
    )
    .await?;
    db.flush().await?;
    let rows = db
        .session()
        .query("MATCH (d:Doc {title: 'canon'}) RETURN d.emb AS emb")
        .await?;
    match rows.rows()[0].value("emb") {
        Some(Value::SparseVector { indices, values }) => {
            assert_eq!(indices, &vec![1u32, 9], "canonicalized indices (sorted)");
            assert_eq!(
                values,
                &vec![2.0f32, 1.5],
                "canonicalized values (summed dup)"
            );
        }
        other => panic!("expected canonicalized SparseVector, got {other:?}"),
    }

    // The database is still alive and writable after the rejected writes (no panic).
    insert_docs(&db, &build_corpus(10, 0xA11_5EED), true).await?;
    Ok(())
}

/// Recall@k of the engine's returned titles against the oracle's true top-k.
fn recall_at_k(engine: &[(String, f64)], corpus: &[(String, Sparse)], k: usize) -> f64 {
    let q = query_vec();
    let mut ranked: Vec<(&str, f64)> = corpus
        .iter()
        .map(|(t, s)| (t.as_str(), sparse_dot_oracle(&q, s)))
        .filter(|(_, s)| *s > 0.0)
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(b.0)));
    let truth: std::collections::HashSet<&str> = ranked.iter().take(k).map(|(t, _)| *t).collect();
    if truth.is_empty() {
        return 1.0;
    }
    let got = engine
        .iter()
        .filter(|(t, _)| truth.contains(t.as_str()))
        .count();
    got as f64 / truth.len() as f64
}

/// A corpus with controlled query-term overlap so there is a non-trivial oracle
/// top-k the candidate stage must preserve: each doc draws a random subset of the
/// query's terms (scaled weights) plus random noise terms.
fn build_overlap_corpus(n: usize, seed: u64) -> Vec<(String, Sparse)> {
    let q = query_vec();
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            let mut m: BTreeMap<u32, f32> = BTreeMap::new();
            // 1..=5 query terms, weighted, so dot scores spread across the corpus.
            let take = 1 + (rng.next_u64() as usize % q.0.len());
            for &t in q.0.iter().take(take) {
                m.insert(t, rng.weight());
            }
            for _ in 0..6 {
                m.insert(rng.term(), rng.weight());
            }
            (
                format!("d{i}"),
                (m.keys().copied().collect(), m.values().copied().collect()),
            )
        })
        .collect()
}

#[tokio::test]
async fn sparse_recall_at_10_is_perfect_on_overlap_corpus() -> anyhow::Result<()> {
    // Closes the headline test gap (issue #95 SP-TG-1): the benchmark *printed*
    // recall@10 but never asserted it, so a regression in the `k * over_fetch`
    // candidate cutoff or in quantization could silently drop recall below 1.0.
    // This asserts recall@10 == 1.0 against the exact f64 oracle over a few-thousand
    // doc corpus with real query-term overlap, on both the quantized (default) and
    // lossless index layouts.
    for quantize in [true, false] {
        let db = Uni::temporary().build().await?;
        define_schema_quantize(&db, quantize).await?;
        let corpus = build_overlap_corpus(2000, 0x5ECA_11ED ^ quantize as u64);
        insert_docs(&db, &corpus, true).await?;

        let results = query_results(&db, 10).await?;
        let recall = recall_at_k(&results, &corpus, 10);
        assert!(
            (recall - 1.0).abs() < f64::EPSILON,
            "recall@10 regressed (quantize={quantize}): {recall} (engine={results:?})"
        );
        assert_matches_oracle(&results, &corpus);
    }
    Ok(())
}
