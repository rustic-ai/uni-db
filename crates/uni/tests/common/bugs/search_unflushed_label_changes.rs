// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A search follows an unflushed label change, as `MATCH` does.
//!
//! `REMOVE n:L` and `SET n:L` on a flushed vertex leave L0 holding its new
//! labels and no property row. A search of `L` took its unflushed candidates
//! from L0 property rows, so until the next flush the multi-vector, sparse and
//! full-text searches still returned a vertex that had left the label, and
//! every search missed one that had joined it.
//!
//! Each check compares the search with `MATCH (n:Doc)` over the same data.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(search_unflushed_label_changes)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, IndexType, Session, Uni, UniConfig};

const DIM: usize = 4;

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    for label in ["Doc", "Other"] {
        db.schema()
            .label(label)
            .property("id", DataType::Int)
            .property("body", DataType::String)
            .property("emb", DataType::Vector { dimensions: 2 })
            .property(
                "mv",
                DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
            )
            .property("sp", DataType::SparseVector { dimensions: 16 })
            .index("sp", IndexType::sparse(16))
            .apply()
            .await?;
    }
    Ok(db)
}

async fn run(session: &Session, stmt: &str) -> Result<()> {
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn write(session: &Session, label: &str, ids: &[i64]) -> Result<()> {
    let tx = session.tx().await?;
    for id in ids {
        let off = 0.1 * *id as f64;
        tx.execute_with(&format!(
            "CREATE (:{label} {{id: {id}, body: 'foo', emb: [1.0, {off}], \
             mv: [[1.0, 0.0, 0.0, {off}], [0.0, 1.0, 0.0, {off}]], sp: $sp}})"
        ))
        .param(
            "sp",
            uni_db::Value::SparseVector {
                indices: vec![1],
                values: vec![1.0 / *id as f32],
            },
        )
        .run()
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn ids(session: &Session, cypher: &str) -> Result<Vec<i64>> {
    let mut out: Vec<i64> = session
        .query(cypher)
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id").unwrap())
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// Every search of `Doc` returns the vertices `MATCH (n:Doc)` does, with and
/// without a filter that all of them pass.
async fn check(session: &Session, stage: &str, want: &[i64]) -> Result<()> {
    let oracle = ids(session, "MATCH (node:Doc) RETURN node.id AS id").await?;
    assert_eq!(oracle, want, "{stage}: MATCH");
    for filter in ["null", "'id >= 1'"] {
        for (name, call) in [
            (
                "dense",
                format!("uni.vector.query('Doc', 'emb', [1.0, 0.0], 10, {filter})"),
            ),
            (
                "multi-vector",
                format!(
                    "uni.vector.query('Doc', 'mv', \
                     [[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]], 10, {filter})"
                ),
            ),
            (
                "sparse",
                format!(
                    "uni.sparse.query('Doc', 'sp', {{indices: [1], values: [1.0]}}, 10, \
                     {filter}, null, {{}})"
                ),
            ),
            (
                "full-text",
                format!("uni.fts.query('Doc', 'body', 'foo', 10, {filter})"),
            ),
        ] {
            let got = ids(
                session,
                &format!("CALL {call} YIELD node RETURN node.id AS id"),
            )
            .await?;
            assert_eq!(got, want, "{stage}: {name}, filter {filter}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn search_unflushed_label_changes_removal() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, "Doc", &[1, 2, 3]).await?;
    db.flush().await?;
    check(&s, "baseline", &[1, 2, 3]).await?;

    run(&s, "MATCH (d:Doc {id: 2}) REMOVE d:Doc").await?;
    check(&s, "label removed, unflushed", &[1, 3]).await?;
    db.flush().await?;
    check(&s, "label removed, flushed", &[1, 3]).await?;
    Ok(())
}

#[tokio::test]
async fn search_unflushed_label_changes_addition() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, "Doc", &[1, 2]).await?;
    write(&s, "Other", &[3, 4]).await?;
    db.flush().await?;
    check(&s, "baseline", &[1, 2]).await?;

    run(&s, "MATCH (o:Other {id: 3}) SET o:Doc").await?;
    check(&s, "label added, unflushed", &[1, 2, 3]).await?;
    db.flush().await?;
    check(&s, "label added, flushed", &[1, 2, 3]).await?;
    Ok(())
}

/// A label added and removed again before a flush leaves no trace, and one
/// removed and added again stays.
#[tokio::test]
async fn search_unflushed_label_changes_follow_the_newest_change() -> Result<()> {
    let db = open().await?;
    let s = db.session();
    write(&s, "Doc", &[1, 2]).await?;
    write(&s, "Other", &[3]).await?;
    db.flush().await?;

    run(&s, "MATCH (o:Other {id: 3}) SET o:Doc").await?;
    run(&s, "MATCH (o:Other {id: 3}) REMOVE o:Doc").await?;
    run(&s, "MATCH (d:Doc {id: 2}) REMOVE d:Doc").await?;
    run(&s, "MATCH (d {id: 2}) SET d:Doc").await?;
    check(&s, "added then removed; removed then added", &[1, 2]).await?;
    Ok(())
}
