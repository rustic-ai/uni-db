// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Whether a *vector* or *full-text* search consulted its index (#175).
//!
//! The scalar half of #175 is covered by [`super::issue_175_index_consulted`].
//! This is the remaining third: `vector_search` and `full_text_search` reach
//! Lance through `nearest()` / `full_text_search()`, build no `ScanRequest`, and
//! so never touched the scan-path callback. A vector query reported
//! `index_scans = 0` whether it ran a real ANN search or a brute-force scan.
//!
//! # Why not the scan path's predicate
//!
//! `attach_scan_stats` decides "consulted" by OR-ing `indices_loaded`,
//! `parts_loaded` and `index_comparisons`. That is sound for a plain scan — a
//! scanner that never calls `nearest()` can only produce scalar-index nodes —
//! and unsound here, because `vector_search` sets `prefilter(true)` with a SQL
//! filter, so a *scalar* index serving the prefilter lights all three terms
//! while the KNN runs brute force.
//!
//! Measured on the fixture below, no vector index, filter on a Hash-indexed
//! column: `indices_loaded = 1`, `index_comparisons = 4096`, and no
//! `partitions_searched` at all. The OR would have called that an ANN search.
//! `a_scalar_prefilter_is_not_a_vector_index` is that case, and it is the only
//! test here that separates the two predicates.
//!
//! # The denominator
//!
//! Every negative asserts `searches_reported > 0` beside the zero. Without that
//! pairing, deleting the callback makes every negative pass.
//!
//! # Two things worth knowing before reading a zero
//!
//! **An index built before ingest is not an index.** Applying a vector index to
//! an empty table leaves Lance nothing to train on and it plans a flat scan
//! forever after — `indices_loaded = 0`, every row scanned. Every fixture here
//! ingests first and indexes second, and
//! `an_index_built_before_ingest_is_not_consulted` pins that this is visible
//! rather than silent. It is the likeliest way for one of these tests to become
//! vacuous.
//!
//! **`Flat` is still an index.** Lance builds `VectorAlgo::Flat` as a
//! single-partition IVF, so it reports one partition searched. The counter
//! answers "was a vector index consulted", not "was the search approximate".

use uni_db::api::schema::{IndexType, ScalarType, VectorAlgo, VectorIndexCfg, VectorMetric};
use uni_db::{DataType, QueryResult, Uni, Value};

const DIM: usize = 16;
/// Small enough to keep the suite fast; measured to produce a real ANN plan.
const N: usize = 500;

fn embedding(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i % DIM] = 1.0;
    v[(i + 1) % DIM] = 0.5;
    v
}

fn ivf() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::IvfFlat { partitions: 4 },
        metric: VectorMetric::Cosine,
        embedding: None,
    })
}

fn flat() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: None,
    })
}

/// `N` docs with a vector and a tag. The vector index, when asked for, is built
/// **after** ingest — see the module note.
async fn fixture(vector_index: Option<IndexType>, hash_on_tag: bool) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Doc")
        .property("tag", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .apply()
        .await
        .unwrap();

    let tx = db.session().tx().await.unwrap();
    for i in 0..N {
        let v: Vec<String> = embedding(i).iter().map(|f| f.to_string()).collect();
        tx.execute(&format!(
            "CREATE (:Doc {{tag:'t{}', emb:[{}]}})",
            i % 10,
            v.join(",")
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    if let Some(ix) = vector_index {
        db.schema()
            .label("Doc")
            .index("emb", ix)
            .apply()
            .await
            .unwrap();
    }
    if hash_on_tag {
        db.schema()
            .label("Doc")
            .index("tag", IndexType::Scalar(ScalarType::Hash))
            .apply()
            .await
            .unwrap();
    }
    db.flush().await.unwrap();
    db
}

async fn vector_query(db: &Uni, filter: Option<&str>) -> QueryResult {
    let f = match filter {
        Some(x) => Value::String(x.to_string()),
        None => Value::Null,
    };
    db.session()
        .query_with(
            "CALL uni.vector.query('Doc', 'emb', $q, 5, $f, null, {}) \
             YIELD node, score RETURN node.tag AS tag",
        )
        .param("q", Value::Vector(embedding(3)))
        .param("f", f)
        .fetch_all()
        .await
        .unwrap()
}

/// The positive. An IVF index built against real data is consulted.
#[tokio::test]
async fn a_vector_query_over_an_ivf_index_consults_it() {
    let db = fixture(Some(ivf()), false).await;
    let m = vector_query(&db, None).await.metrics().clone();
    assert!(
        m.searches_reported > 0,
        "no search reported at all — the stats callback is not wired"
    );
    assert!(
        m.vector_index_scans >= 1,
        "an IVF-indexed vector query consulted no index (searches_reported={}). \
         Lance may have renamed `partitions_searched`, or the index was not \
         built against the data.",
        m.searches_reported
    );
}

/// The negative. Same corpus, same query, no vector index.
#[tokio::test]
async fn a_vector_query_without_an_index_consults_none() {
    let db = fixture(None, false).await;
    let m = vector_query(&db, None).await.metrics().clone();
    assert!(m.searches_reported > 0, "the search must still report");
    assert_eq!(
        m.vector_index_scans, 0,
        "reported a vector index on a table that has none"
    );
}

/// **The load-bearing test.** No vector index, but the filter rides a
/// Hash-indexed scalar column, so Lance's `indices_loaded` / `index_comparisons`
/// both rise while the KNN itself is brute force. The scan path's OR predicate
/// would report an index search here; `partitions_searched` does not.
#[tokio::test]
async fn a_scalar_prefilter_is_not_a_vector_index() {
    let db = fixture(None, true).await;
    let m = vector_query(&db, Some("tag = 't3'"))
        .await
        .metrics()
        .clone();
    assert!(m.searches_reported > 0, "the search must still report");
    assert_eq!(
        m.vector_index_scans, 0,
        "a scalar index serving the prefilter was counted as a vector index \
         search — this is the confound the whole predicate exists to reject"
    );
}

/// Both indexes present: the scalar prefilter must not *suppress* the vector
/// count either. The mirror of the test above, so neither direction is assumed.
#[tokio::test]
async fn a_prefiltered_query_over_an_indexed_vector_still_counts() {
    let db = fixture(Some(ivf()), true).await;
    let m = vector_query(&db, Some("tag = 't3'"))
        .await
        .metrics()
        .clone();
    assert!(m.vector_index_scans >= 1, "the vector index was consulted");
}

/// `Flat` is a brute-force *index*, and Lance builds it as a single-partition
/// IVF — so it counts as consulted. Named so a future reader does not "fix" it:
/// the counter reports whether an index was searched, not whether the search
/// was approximate.
#[tokio::test]
async fn a_flat_index_counts_as_consulted() {
    let db = fixture(Some(flat()), false).await;
    let m = vector_query(&db, None).await.metrics().clone();
    assert!(m.searches_reported > 0);
    assert_eq!(
        m.vector_index_scans, 1,
        "a Flat index is one partition, and searching it is still consulting an index"
    );
}

/// An index applied to an empty table has nothing to train on, and Lance plans a
/// flat scan from then on. That is the likeliest way for the positive tests here
/// to quietly stop testing anything, so it is pinned as an observable outcome
/// rather than left as a comment.
#[tokio::test]
async fn an_index_built_before_ingest_is_not_consulted() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Doc")
        .property("tag", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index("emb", ivf())
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    for i in 0..N {
        let v: Vec<String> = embedding(i).iter().map(|f| f.to_string()).collect();
        tx.execute(&format!(
            "CREATE (:Doc {{tag:'t{}', emb:[{}]}})",
            i % 10,
            v.join(",")
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    let m = vector_query(&db, None).await.metrics().clone();
    assert!(m.searches_reported > 0, "the search must still report");
    assert_eq!(
        m.vector_index_scans, 0,
        "an index declared before ingest was never built, so it cannot be consulted"
    );
}

/// The falsifiability pair, asserted together: one query that must move the
/// counter and one that must not. A counter that is always zero and one that is
/// always nonzero are equally useless, and only this shape rules out both.
#[tokio::test]
async fn the_vector_counter_is_not_a_constant() {
    let with = fixture(Some(ivf()), false).await;
    let without = fixture(None, false).await;
    let a = vector_query(&with, None).await.metrics().clone();
    let b = vector_query(&without, None).await.metrics().clone();
    assert!(a.searches_reported > 0 && b.searches_reported > 0);
    assert!(a.vector_index_scans > 0, "the counter never rises");
    assert_eq!(b.vector_index_scans, 0, "the counter never falls");
}

/// `index_scans` counts scalar `ScanRequest` scans and must stay out of this.
/// Folding ANN into it would silently change every existing assertion about it.
#[tokio::test]
async fn a_vector_search_does_not_move_the_scalar_counter() {
    let db = fixture(Some(ivf()), false).await;
    let m = vector_query(&db, None).await.metrics().clone();
    assert!(m.vector_index_scans >= 1);
    assert_eq!(
        m.index_scans, 0,
        "a vector search moved the scalar-scan counter"
    );
}

// ── Full-text search ────────────────────────────────────────────────────────

async fn fts_fixture(with_index: bool) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Doc")
        .property("body", DataType::String)
        .apply()
        .await
        .unwrap();
    let tx = db.session().tx().await.unwrap();
    for i in 0..N {
        tx.execute(&format!("CREATE (:Doc {{body:'alpha beta doc{i} gamma'}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();
    if with_index {
        db.schema()
            .label("Doc")
            .index("body", IndexType::FullText)
            .apply()
            .await
            .unwrap();
        db.flush().await.unwrap();
    }
    db
}

async fn fts_query(db: &Uni) -> QueryResult {
    db.session()
        .query(
            "CALL uni.fts.query('Doc', 'body', 'gamma', 5, null, null, {}) \
             YIELD node RETURN node.body AS b",
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn an_fts_query_over_an_inverted_index_consults_it() {
    let db = fts_fixture(true).await;
    let m = fts_query(&db).await.metrics().clone();
    assert!(
        m.searches_reported > 0,
        "no search reported — the stats callback is not wired"
    );
    assert!(
        m.fts_index_scans >= 1,
        "an inverted-indexed FTS query consulted no index. Lance may have \
         renamed `partitions_searched`."
    );
}

/// FTS still answers without an index, by scanning — so this is a live negative
/// rather than an error path. Note Lance reports the metric here as *present and
/// zero*, where the vector path omits it entirely; the predicate has to read
/// both as "not consulted".
#[tokio::test]
async fn an_fts_query_without_an_index_consults_none() {
    let db = fts_fixture(false).await;
    let m = fts_query(&db).await.metrics().clone();
    assert!(m.searches_reported > 0, "the search must still report");
    assert_eq!(
        m.fts_index_scans, 0,
        "reported an index that does not exist"
    );
}

#[tokio::test]
async fn the_fts_counter_is_not_a_constant() {
    let with = fts_fixture(true).await;
    let without = fts_fixture(false).await;
    let a = fts_query(&with).await.metrics().clone();
    let b = fts_query(&without).await.metrics().clone();
    assert!(a.searches_reported > 0 && b.searches_reported > 0);
    assert!(a.fts_index_scans > 0, "the counter never rises");
    assert_eq!(b.fts_index_scans, 0, "the counter never falls");
}

