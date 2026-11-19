// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A search's `filter` argument applies to unflushed rows as well as flushed
//! ones.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(search_filter_on_unflushed)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::Int)
        .property("emb", DataType::Vector { dimensions: 2 })
        .property("body", DataType::String)
        .property(
            "mv",
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
        )
        .apply()
        .await?;
    Ok(db)
}

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn ids(db: &Uni, call: &str) -> Result<Vec<i64>> {
    let result = db
        .session()
        .query(&format!("{call} YIELD node RETURN node.id AS id"))
        .await?;
    let mut out: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => *i,
            other => panic!("id: {other:?}"),
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

const SEED: &str = "CREATE \
    (:Doc {id: 1, emb: [1.0, 0.0], body: 'foo a', mv: [[1.0, 0.0], [0.0, 1.0]]}), \
    (:Doc {id: 2, emb: [0.9, 0.1], body: 'foo b', mv: [[0.9, 0.1], [0.1, 0.9]]}), \
    (:Doc {id: 3, emb: [0.0, 1.0], body: 'foo c', mv: [[0.8, 0.2], [0.2, 0.8]]})";
const MORE: &str = "CREATE \
    (:Doc {id: 4, emb: [1.0, 0.1], body: 'foo d', mv: [[0.7, 0.3], [0.3, 0.7]]}), \
    (:Doc {id: 5, emb: [0.8, 0.2], body: 'foo e', mv: [[0.6, 0.4], [0.4, 0.6]]})";

async fn check(db: &Uni, stage: &str, filter: &str, want: &[i64]) -> Result<()> {
    let vector = format!("CALL uni.vector.query('Doc', 'emb', [1.0, 0.0], 10, '{filter}')");
    let text = format!("CALL uni.fts.query('Doc', 'body', 'foo', 10, '{filter}')");
    assert_eq!(ids(db, &vector).await?, want, "{stage}: vector, {filter}");
    assert_eq!(ids(db, &text).await?, want, "{stage}: text, {filter}");
    // The multi-vector path unions unflushed rows above the storage layer and
    // has to drop the filtered-out ones itself (#343).
    let multi =
        format!("CALL uni.vector.query('Doc', 'mv', [[1.0, 0.0], [0.0, 1.0]], 10, '{filter}')");
    assert_eq!(ids(db, &multi).await?, want, "{stage}: multi, {filter}");
    Ok(())
}

#[tokio::test]
async fn search_filter_on_unflushed_rows() -> Result<()> {
    let db = open().await?;
    run(&db, SEED).await?;
    check(&db, "unflushed", "id = 2", &[2]).await?;
    db.flush().await?;
    check(&db, "flushed", "id = 2", &[2]).await?;
    run(&db, MORE).await?;
    check(&db, "mixed", "id >= 2 AND id <= 4", &[2, 3, 4]).await?;
    Ok(())
}

/// An unflushed update that takes a flushed row out of the filter removes it.
#[tokio::test]
async fn search_filter_on_unflushed_updates() -> Result<()> {
    let db = open().await?;
    run(&db, SEED).await?;
    db.flush().await?;
    run(&db, "MATCH (d:Doc {id: 2}) SET d.id = 20").await?;
    check(&db, "updated", "id = 2", &[]).await?;
    // Flushing the update leaves the old row version in the table, where the
    // search index still matches it; only the vertex's latest row counts.
    db.flush().await?;
    check(&db, "updated and flushed", "id = 2", &[]).await?;
    check(&db, "updated and flushed", "id = 20", &[20]).await?;
    Ok(())
}

/// A filtered-out unflushed row that scores best takes no slot from a row the
/// filter allows, when `k` is the number of allowed rows (#343).
#[tokio::test]
async fn multivector_filter_keeps_top_k_slots_for_allowed_rows() -> Result<()> {
    let db = open().await?;
    run(&db, SEED).await?;
    // Doc 1 is the exact match for the query and is the one filtered out.
    let call = "CALL uni.vector.query('Doc', 'mv', [[1.0, 0.0], [0.0, 1.0]], 2, 'id <> 1')";
    assert_eq!(ids(&db, call).await?, [2, 3], "unflushed");
    db.flush().await?;
    assert_eq!(ids(&db, call).await?, [2, 3], "flushed");
    Ok(())
}
