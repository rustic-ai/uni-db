// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A multi-vector (`List<Vector>`) search's `filter` holds on every path
//! (issue #343), and unflushed changes to the best matches do not shrink the
//! answer.
//!
//! The first fix for #343 dropped unflushed rows that fail the filter. Two
//! cases remained: once such an update was flushed, the old row version stayed
//! in the append-only table and the search still matched it; and rows dropped
//! after the flushed fetch were not replaced, so a search returned fewer than
//! `k` rows, or none, when the best matches had unflushed changes.
//!
//! Corpus convention: the query is the tokens `[e0, e1]`; doc `id` carries
//! `[e0 + off, e1 + off]` with `off` growing with `id`, so a lower `id` always
//! scores higher.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(multivector_search_filter)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{
    DataType, IndexType, Session, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};

const DIM: usize = 8;
const MUVERA: &str = "{type: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'flat'}";

/// Which index, if any, the `mv` column gets.
#[derive(Clone, Copy)]
enum Index {
    None,
    Flat,
    Muvera,
}

async fn open(index: Index) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            disable_fork_sweeper: true,
            disable_fork_index_builder: true,
            ..Default::default()
        })
        .build()
        .await?;
    let label = db.schema().label("Doc").property("id", DataType::Int);
    let label = label.property(
        "mv",
        DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
    );
    match index {
        Index::Flat => {
            label
                .index(
                    "mv",
                    IndexType::Vector(VectorIndexCfg {
                        algorithm: VectorAlgo::Flat,
                        metric: VectorMetric::Cosine,
                        embedding: None,
                    }),
                )
                .apply()
                .await?
        }
        Index::None | Index::Muvera => label.apply().await?,
    };
    if matches!(index, Index::Muvera) {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "CREATE VECTOR INDEX mv_idx FOR (d:Doc) ON (d.mv) OPTIONS {MUVERA}"
        ))
        .await?;
        tx.commit().await?;
    }
    Ok(db)
}

/// Unit axis `axis`, pushed off it by `off` along the last dimension.
fn token(axis: usize, off: f64) -> String {
    let mut v = vec![0.0_f64; DIM];
    v[axis] = 1.0;
    v[DIM - 1] = off;
    format!("{v:?}")
}

fn tokens(id: i64) -> String {
    let off = 0.02 * id as f64;
    format!("[{}, {}]", token(0, off), token(1, off))
}

fn query() -> String {
    format!("[{}, {}]", token(0, 0.0), token(1, 0.0))
}

async fn run(session: &Session, stmt: &str) -> Result<()> {
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn write(session: &Session, ids: impl IntoIterator<Item = i64>) -> Result<()> {
    let tx = session.tx().await?;
    for id in ids {
        tx.execute(&format!("CREATE (:Doc {{id: {id}, mv: {}}})", tokens(id)))
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

fn call(k: usize, filter: Option<&str>) -> String {
    let filter = filter.map_or("null".to_string(), |f| format!("'{f}'"));
    format!(
        "CALL uni.vector.query('Doc', 'mv', {}, {k}, {filter}) \
         YIELD node, score RETURN node.id AS id ORDER BY score DESC, id",
        query()
    )
}

fn ids_of(result: &uni_db::QueryResult) -> Vec<i64> {
    result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect()
}

/// Doc ids a search returns, best score first.
async fn ranked(session: &Session, k: usize, filter: Option<&str>) -> Result<Vec<i64>> {
    Ok(ids_of(&session.query(&call(k, filter)).await?))
}

/// Doc ids a filtered search returns, ascending.
async fn ids(session: &Session, filter: &str) -> Result<Vec<i64>> {
    let mut out = ranked(session, 10, Some(filter)).await?;
    out.sort_unstable();
    Ok(out)
}

/// Unflushed, flushed, and updated-then-flushed rows, on each kind of index.
#[tokio::test]
async fn multivector_search_filter_on_each_index_kind() -> Result<()> {
    for (name, index) in [
        ("no index", Index::None),
        ("flat", Index::Flat),
        ("muvera", Index::Muvera),
    ] {
        let db = open(index).await?;
        let s = db.session();
        write(&s, 1..=4).await?;
        assert_eq!(ids(&s, "id >= 3").await?, [3, 4], "{name}: unflushed");
        db.flush().await?;
        assert_eq!(ids(&s, "id >= 3").await?, [3, 4], "{name}: flushed");

        run(&s, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;
        assert_eq!(ids(&s, "id = 3").await?, [0; 0], "{name}: updated");
        db.flush().await?;
        assert_eq!(ids(&s, "id = 3").await?, [0; 0], "{name}: updated, flushed");
        assert_eq!(ids(&s, "id = 30").await?, [30], "{name}: current value");
    }
    Ok(())
}

/// The issue's own shape: an indexed column and a `_vid IN (…)` allow-list
/// whose size is `k`, with the best match outside it.
#[tokio::test]
async fn multivector_search_filter_by_vid_allow_list() -> Result<()> {
    let db = open(Index::Flat).await?;
    let s = db.session();
    write(&s, 1..=7).await?;
    let rows = s
        .query("MATCH (d:Doc) WHERE d.id >= 2 RETURN id(d) AS vid")
        .await?;
    let vids: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("vid").unwrap().to_string())
        .collect();
    let filter = format!("_vid IN ({})", vids.join(","));
    for stage in ["unflushed", "flushed"] {
        assert_eq!(
            ranked(&s, 6, Some(&filter)).await?,
            [2, 3, 4, 5, 6, 7],
            "{stage}"
        );
        db.flush().await?;
    }
    Ok(())
}

/// Unflushed changes to the best flushed matches do not shrink the answer:
/// each one can displace a flushed candidate, so that many more are fetched.
#[tokio::test]
async fn multivector_search_returns_k_when_top_matches_changed_unflushed() -> Result<()> {
    for (name, index) in [("no index", Index::None), ("muvera", Index::Muvera)] {
        let db = open(index).await?;
        let s = db.session();
        write(&s, 1..=40).await?;
        db.flush().await?;
        run(&s, "MATCH (d:Doc) WHERE d.id <= 20 SET d.id = d.id + 1000").await?;
        assert_eq!(
            ranked(&s, 2, Some("id < 1000")).await?,
            [21, 22],
            "{name}: updated out of the filter"
        );

        let db = open(index).await?;
        let s = db.session();
        write(&s, 1..=40).await?;
        db.flush().await?;
        run(&s, "MATCH (d:Doc) WHERE d.id <= 20 DETACH DELETE d").await?;
        assert_eq!(ranked(&s, 2, None).await?, [21, 22], "{name}: deleted");
    }
    Ok(())
}

/// A transaction's own uncommitted writes are filtered like any unflushed row.
#[tokio::test]
async fn multivector_search_filter_sees_uncommitted_writes_in_their_transaction() -> Result<()> {
    let db = open(Index::None).await?;
    let s = db.session();
    write(&s, 1..=3).await?;
    db.flush().await?;

    let tx = s.tx().await?;
    tx.execute("MATCH (d:Doc {id: 2}) SET d.id = 20").await?;
    let inside = ids_of(&tx.query(&call(10, Some("id >= 2 AND id <= 3"))).await?);
    assert_eq!(inside, [3], "inside the transaction");
    assert_eq!(ids(&s, "id >= 2 AND id <= 3").await?, [2, 3], "outside");
    Ok(())
}

/// A fork has no multi-vector index; its candidates come from a branch scan,
/// which must also read each vertex's latest row only.
#[tokio::test]
async fn multivector_search_filter_on_a_fork_after_flushing_an_update() -> Result<()> {
    let db = open(Index::None).await?;
    let primary = db.session();
    write(&primary, 1..=3).await?;
    db.flush().await?;

    let fork = primary.fork("mv_filter_flushed").await?;
    run(&fork, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;
    assert_eq!(
        ids(&fork, "id = 3").await?,
        [0; 0],
        "fork, unflushed update"
    );
    fork.flush().await?;
    assert_eq!(ids(&fork, "id = 3").await?, [0; 0], "fork, flushed update");
    assert_eq!(ids(&fork, "id = 30").await?, [30], "fork, current value");
    assert_eq!(ids(&primary, "id = 3").await?, [3], "primary");
    Ok(())
}

/// Every unflushed row is scored, however many there are. The candidate cap
/// bounds the flushed hits only: applied to the union, it dropped an arbitrary
/// share of the unflushed rows, the best matches among them.
#[tokio::test]
async fn multivector_search_scores_every_unflushed_row() -> Result<()> {
    // (flushed, unflushed): the widened flushed fetch plus the unflushed rows
    // pass the cap; the unflushed rows alone pass it.
    for (flushed, unflushed) in [(1200, 600), (50, 1500)] {
        let db = open(Index::None).await?;
        let s = db.session();
        write(&s, 5001..=5000 + flushed).await?;
        db.flush().await?;
        write(&s, 1..=unflushed).await?;
        assert_eq!(
            ranked(&s, 10, None).await?,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "{flushed} flushed, {unflushed} unflushed"
        );
    }
    Ok(())
}
