// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A build that rewrites the label table to fill a derived column (a keyed
//! index's key, a JSON full-text index's text) keeps every other Lance index
//! on the table.
//!
//! The rewrite is `WriteMode::Overwrite`, and Lance drops every index on the
//! dataset when it overwrites. A `CREATE INDEX` on a DATETIME over flushed
//! rows left the table with only the new key index: the defaults (`_vid`,
//! `_uid`, `ext_id`) and every declared index were gone until the next flush
//! happened to rebuild them. A rebuild and a semantic compaction rewrote the
//! table again in the middle of restoring its indexes, so they ended the same
//! way.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(rewrite_keeps_indexes)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use std::collections::BTreeSet;
use uni_db::{Uni, UniConfig};

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

/// The Lance indexes on the `Doc` table.
async fn lance_indexes(db: &Uni) -> Result<BTreeSet<String>> {
    Ok(db
        .storage()
        .head_backend()
        .list_indexes("vertices_Doc")
        .await?
        .into_iter()
        .map(|i| i.name)
        .collect())
}

/// The index lookups the scan of `query` made.
async fn index_hits(db: &Uni, query: &str) -> Result<usize> {
    let (_, profile) = db.session().query_with(query).profile().await?;
    Ok(profile
        .runtime_stats
        .iter()
        .filter(|s| s.operator == "GraphScanExec")
        .filter_map(|s| s.index_hits)
        .sum())
}

async fn count(db: &Uni, query: &str) -> Result<usize> {
    Ok(db.session().query(query).await?.len())
}

/// Every index in `before` is still on the table, and the plain, keyed and
/// JSON full-text lookups still answer, the first two through their index.
async fn assert_kept(db: &Uni, stage: &str, before: &BTreeSet<String>) -> Result<BTreeSet<String>> {
    let now = lance_indexes(db).await?;
    let lost: Vec<&String> = before.difference(&now).collect();
    assert!(
        lost.is_empty(),
        "{stage}: lost Lance indexes {lost:?}; now {now:?}"
    );
    let by_id = "MATCH (d:Doc) WHERE d.id = 3 RETURN d.id";
    assert_eq!(count(db, by_id).await?, 1, "{stage}");
    assert!(
        index_hits(db, by_id).await? > 0,
        "{stage}: the id lookup no longer reads its index"
    );
    let by_title = "MATCH (d:Doc) WHERE d.doc.title CONTAINS 'graph' RETURN d.id";
    assert_eq!(count(db, by_title).await?, 50, "{stage}");
    Ok(now)
}

#[tokio::test]
async fn rewrite_keeps_indexes_across_create_rebuild_and_compaction() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_string_lossy().as_ref())
        .config(config())
        .build()
        .await?;
    run(&db, "CREATE LABEL Doc (id INT, ts DATETIME, doc JSON)").await?;
    run(&db, "CREATE INDEX idx_id FOR (d:Doc) ON (d.id)").await?;
    run(
        &db,
        "UNWIND range(1, 50) AS i CREATE (:Doc {id: i, \
         ts: datetime({epochSeconds: 1767225600 + i}), doc: {title: 'graph ' + toString(i)}})",
    )
    .await?;
    db.flush().await?;
    let before = lance_indexes(&db).await?;
    for name in ["_vid_idx", "_uid_idx", "ext_id_idx", "idx_id"] {
        assert!(before.contains(name), "{name} missing before: {before:?}");
    }

    // A JSON full-text index over flushed rows rewrites the table.
    run(&db, "CREATE JSON FULLTEXT INDEX dfts FOR (d:Doc) ON doc").await?;
    let before = assert_kept(&db, "JSON full-text create", &before).await?;
    assert!(before.contains("dfts"), "{before:?}");

    // So does a DATETIME index over flushed rows.
    run(&db, "CREATE INDEX idx_ts FOR (d:Doc) ON (d.ts)").await?;
    let before = assert_kept(&db, "DATETIME create", &before).await?;
    assert!(before.contains("idx_ts__dt"), "{before:?}");
    let by_ts = "MATCH (d:Doc) WHERE d.ts > datetime('2026-01-01T00:00:10Z') RETURN d.id";
    assert_eq!(count(&db, by_ts).await?, 40);
    assert!(index_hits(&db, by_ts).await? > 0, "the key index is read");

    // A rebuild rewrites it once per derived column.
    db.indexes().rebuild("Doc", false).await?;
    let before = assert_kept(&db, "rebuild", &before).await?;

    // A semantic compaction replaces it, then restores its indexes.
    run(&db, "MATCH (d:Doc {id: 50}) SET d.id = 5000").await?;
    db.flush().await?;
    let stats = uni_db::store::storage::compaction::Compactor::new(db.storage())
        .compact_vertices("Doc")
        .await?;
    assert!(
        stats.ran,
        "the compaction must run for this test to mean anything"
    );
    assert_kept(&db, "semantic compaction", &before).await?;
    assert_eq!(count(&db, by_ts).await?, 40);
    assert!(index_hits(&db, by_ts).await? > 0, "the key index is read");
    Ok(())
}
