// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A sparse search's `filter` argument scopes its results (issue #344).
//!
//! `uni.sparse.query` used to reject a filter, and the sparse arm of
//! `uni.search` ignored it, so a filtered fused search returned rows the filter
//! excluded. The sparse index holds postings only and cannot evaluate a
//! predicate; the filter is resolved to the vids it allows and the index scores
//! only those, so the top `k` is exact among the allowed rows.
//!
//! Corpus convention: the query is term 1 with weight 1.0, and doc `id` carries
//! term 1 with weight `1 / id`, so a lower `id` always scores higher.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(sparse_search_filter)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, IndexType, Session, Uni, UniConfig, Value};

const VOCAB: usize = 16;

async fn open() -> Result<Uni> {
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
    db.schema()
        .label("Doc")
        .property("id", DataType::Int)
        .property("body", DataType::String)
        .property("sp", DataType::SparseVector { dimensions: VOCAB })
        .index("sp", IndexType::sparse(VOCAB))
        .apply()
        .await?;
    Ok(db)
}

fn query() -> Value {
    Value::SparseVector {
        indices: vec![1],
        values: vec![1.0],
    }
}

/// Write docs `ids` through `session` in one transaction.
async fn write(session: &Session, ids: &[i64]) -> Result<()> {
    let tx = session.tx().await?;
    for id in ids {
        tx.execute_with("CREATE (:Doc {id: $id, body: 'foo', sp: $sp})")
            .param("id", Value::Int(*id))
            .param(
                "sp",
                Value::SparseVector {
                    indices: vec![1, 7],
                    values: vec![1.0 / *id as f32, 0.1],
                },
            )
            .run()
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn run(session: &Session, stmt: &str) -> Result<()> {
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

/// Doc ids a call returns, best score first.
async fn ranked(session: &Session, call: &str) -> Result<Vec<i64>> {
    let result = session
        .query_with(&format!(
            "{call} YIELD node, score RETURN node.id AS id ORDER BY score DESC, id"
        ))
        .param("q", query())
        .fetch_all()
        .await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect())
}

/// Doc ids a call returns, ascending.
async fn ids(session: &Session, call: &str) -> Result<Vec<i64>> {
    let mut out = ranked(session, call).await?;
    out.sort_unstable();
    Ok(out)
}

fn sparse_query(k: usize, filter: &str) -> String {
    format!("CALL uni.sparse.query('Doc', 'sp', $q, {k}, '{filter}', null, {{}})")
}

fn search_sparse(k: usize, filter: &str) -> String {
    format!(
        "CALL uni.search('Doc', {{sparse: 'sp'}}, '', null, {k}, '{filter}', {{sparse_query: $q}})"
    )
}

fn search_fused(k: usize, filter: &str) -> String {
    format!(
        "CALL uni.search('Doc', {{fts: 'body', sparse: 'sp'}}, 'foo', null, {k}, '{filter}', \
         {{sparse_query: $q}})"
    )
}

/// Every sparse entry point returns exactly the ids `filter` allows.
async fn check(session: &Session, stage: &str, filter: &str, want: &[i64]) -> Result<()> {
    for (name, call) in [
        ("sparse.query", sparse_query(10, filter)),
        ("search sparse arm", search_sparse(10, filter)),
        ("search fts + sparse", search_fused(10, filter)),
    ] {
        assert_eq!(
            ids(session, &call).await?,
            want,
            "{stage}: {name}, {filter}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sparse_search_filter_on_unflushed_flushed_and_mixed_rows() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3, 4]).await?;
    check(&s, "unflushed", "id >= 3", &[3, 4]).await?;
    db.flush().await?;
    check(&s, "flushed", "id >= 3", &[3, 4]).await?;
    write(&s, &[5, 6]).await?;
    check(&s, "mixed", "id >= 3 AND id <= 5", &[3, 4, 5]).await?;
    check(&s, "nothing allowed", "id > 100", &[]).await?;
    Ok(())
}

/// A filtered-out row that scores best takes no slot from a row the filter
/// allows: the top `k` is ranked among the allowed rows, not cut from the
/// unfiltered top `k`.
#[tokio::test]
async fn sparse_search_filter_keeps_top_k_slots_for_allowed_rows() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]).await?;
    for stage in ["unflushed", "flushed"] {
        // Docs 1..=10 outscore 11 and 12, the only ones allowed; with the
        // default 4x over-fetch a post-filter of the top 8 would return nothing.
        assert_eq!(
            ranked(&s, &sparse_query(2, "id >= 11")).await?,
            [11, 12],
            "{stage}: sparse.query"
        );
        assert_eq!(
            ranked(&s, &search_sparse(2, "id >= 11")).await?,
            [11, 12],
            "{stage}: search sparse arm"
        );
        db.flush().await?;
    }
    Ok(())
}

/// The allow-list form a caller uses to scope a search to known vertices.
#[tokio::test]
async fn sparse_search_filter_by_vid_allow_list() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3, 4]).await?;
    let rows = s
        .query("MATCH (d:Doc) WHERE d.id IN [3, 4] RETURN id(d) AS vid")
        .await?;
    let vids: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("vid").unwrap().to_string())
        .collect();
    let filter = format!("_vid IN ({})", vids.join(","));
    check(&s, "unflushed", &filter, &[3, 4]).await?;
    db.flush().await?;
    check(&s, "flushed", &filter, &[3, 4]).await?;
    Ok(())
}

/// An unflushed update moves a flushed row out of, or into, the filter.
#[tokio::test]
async fn sparse_search_filter_follows_unflushed_updates() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3]).await?;
    db.flush().await?;
    run(&s, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;
    check(&s, "moved out", "id = 3", &[]).await?;
    check(&s, "moved in", "id = 30", &[30]).await?;
    Ok(())
}

/// A superseded flushed row version that still passes the filter does not
/// admit a vertex whose latest row fails it; a deleted vertex is not admitted.
#[tokio::test]
async fn sparse_search_filter_reads_latest_flushed_row_only() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3]).await?;
    db.flush().await?;
    run(&s, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;
    run(&s, "MATCH (d:Doc {id: 2}) DETACH DELETE d").await?;
    db.flush().await?;
    check(&s, "superseded", "id = 3", &[]).await?;
    check(&s, "current", "id = 30", &[30]).await?;
    check(&s, "deleted", "id <= 3", &[1]).await?;
    Ok(())
}

/// A fork has no sparse index of its own; its candidates come from a branch
/// scan, which the filter scopes the same way.
#[tokio::test]
async fn sparse_search_filter_on_a_fork() -> Result<()> {
    let db = open().await?;
    let primary = db.session();
    write(&primary, &[1, 2, 3]).await?;
    db.flush().await?;

    let fork = primary.fork("sparse_filter").await?;
    write(&fork, &[4, 5]).await?;
    fork.flush().await?;
    write(&fork, &[6]).await?;
    run(&fork, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;

    check(&fork, "fork", "id >= 2 AND id <= 5", &[2, 4, 5]).await?;
    check(&fork, "fork unflushed", "id >= 6", &[6, 30]).await?;
    check(&primary, "primary", "id >= 2", &[2, 3]).await?;
    Ok(())
}

/// A search over inline arguments, for a transaction (which takes no builder
/// parameters here): doc ids, best score first.
fn inline_sparse_query(k: usize, filter: &str) -> String {
    format!(
        "CALL uni.sparse.query('Doc', 'sp', {{indices: [1], values: [1.0]}}, {k}, '{filter}', \
         null, {{}}) YIELD node, score RETURN node.id AS id ORDER BY score DESC, id"
    )
}

/// Unflushed changes to the best flushed matches do not shrink the answer:
/// each one can displace a flushed candidate, so that many more are fetched.
#[tokio::test]
async fn sparse_search_filter_returns_k_when_top_matches_changed_unflushed() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &(1..=40).collect::<Vec<_>>()).await?;
    db.flush().await?;
    run(&s, "MATCH (d:Doc) WHERE d.id <= 20 SET d.id = d.id + 1000").await?;
    assert_eq!(
        ranked(&s, &sparse_query(2, "id < 1000")).await?,
        [21, 22],
        "sparse.query, updated out of the filter"
    );
    assert_eq!(
        ranked(&s, &search_sparse(2, "id < 1000")).await?,
        [21, 22],
        "search sparse arm, updated out of the filter"
    );
    Ok(())
}

/// The same without a filter: unflushed deletes of the best matches.
#[tokio::test]
async fn sparse_search_returns_k_when_top_matches_deleted_unflushed() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &(1..=40).collect::<Vec<_>>()).await?;
    db.flush().await?;
    run(&s, "MATCH (d:Doc) WHERE d.id <= 20 DETACH DELETE d").await?;
    assert_eq!(
        ranked(&s, "CALL uni.sparse.query('Doc', 'sp', $q, 2)").await?,
        [21, 22]
    );
    assert_eq!(
        ranked(&s, &sparse_query(2, "id >= 1")).await?,
        [21, 22],
        "with a filter"
    );
    Ok(())
}

/// A pinned session filters the rows of its snapshot, not later versions.
#[tokio::test]
async fn sparse_search_filter_on_a_pinned_snapshot() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3]).await?;
    db.flush().await?;
    let snapshot = db.create_snapshot("before").await?;
    run(&s, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;
    write(&s, &[4]).await?;
    db.flush().await?;

    let mut pinned = db.session();
    pinned.pin_to_version(&snapshot).await?;
    check(&pinned, "pinned", "id >= 2", &[2, 3]).await?;
    check(&s, "live", "id >= 2", &[2, 4, 30]).await?;
    Ok(())
}

/// A transaction's own uncommitted writes are filtered like any unflushed row.
#[tokio::test]
async fn sparse_search_filter_sees_uncommitted_writes_in_their_transaction() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3]).await?;
    db.flush().await?;

    let tx = s.tx().await?;
    tx.execute("MATCH (d:Doc {id: 2}) SET d.id = 20").await?;
    let got: Vec<i64> = tx
        .query(&inline_sparse_query(10, "id >= 2 AND id <= 3"))
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect();
    assert_eq!(got, [3], "inside the transaction");
    check(
        &s,
        "outside the transaction",
        "id >= 2 AND id <= 3",
        &[2, 3],
    )
    .await?;
    Ok(())
}

/// A fork that flushes an update keeps the old row version on its branch.
#[tokio::test]
async fn sparse_search_filter_on_a_fork_after_flushing_an_update() -> Result<()> {
    let db = open().await?;
    let primary = db.session();
    write(&primary, &[1, 2, 3]).await?;
    db.flush().await?;

    let fork = primary.fork("sparse_filter_flushed").await?;
    run(&fork, "MATCH (d:Doc {id: 3}) SET d.id = 30").await?;
    fork.flush().await?;
    check(&fork, "fork, superseded", "id = 3", &[]).await?;
    check(&fork, "fork, current", "id = 30", &[30]).await?;
    check(&primary, "primary", "id = 3", &[3]).await?;
    Ok(())
}

/// `threshold` and an unflushed delete both compose with the filter.
#[tokio::test]
async fn sparse_search_filter_with_threshold_and_unflushed_delete() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, &[1, 2, 3, 4]).await?;
    db.flush().await?;
    // Scores are 1 / id: 0.5 and 0.33 pass the filter, only 0.5 the threshold.
    assert_eq!(
        ranked(
            &s,
            "CALL uni.sparse.query('Doc', 'sp', $q, 10, 'id >= 2', 0.4, {})"
        )
        .await?,
        [2]
    );
    run(&s, "MATCH (d:Doc {id: 3}) DETACH DELETE d").await?;
    check(&s, "deleted unflushed", "id >= 3", &[4]).await?;
    Ok(())
}

/// A filter most of a large table passes resolves the latest row versions by
/// reading the version column whole rather than by a list of vids; the answer
/// is the same.
#[tokio::test]
async fn sparse_search_filter_passing_most_of_a_large_table() -> Result<()> {
    const ROWS: i64 = 9000;
    let db = open().await?;
    let s = db.session();
    let rows = (1..=ROWS)
        .map(|id| {
            std::collections::HashMap::from([
                ("id".to_string(), Value::Int(id)),
                ("body".to_string(), Value::String("foo".to_string())),
                (
                    "sp".to_string(),
                    Value::SparseVector {
                        indices: vec![1, 7],
                        values: vec![1.0 / id as f32, 0.1],
                    },
                ),
            ])
        })
        .collect();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices("Doc", rows).await?;
    tx.commit().await?;
    db.flush().await?;
    run(&s, "MATCH (d:Doc {id: 2}) SET d.id = 20000").await?;
    db.flush().await?;

    assert_eq!(
        ranked(&s, &sparse_query(3, &format!("id <= {ROWS}"))).await?,
        [1, 3, 4],
        "broad filter: the superseded row of the moved doc is not matched"
    );
    assert_eq!(
        ranked(&s, &sparse_query(3, "id = 20000")).await?,
        [20000],
        "selective filter"
    );
    Ok(())
}
