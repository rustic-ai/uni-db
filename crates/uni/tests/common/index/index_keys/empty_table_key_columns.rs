// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A keyed index created over a label table that exists but holds no rows
//! still adds its key columns to that table.
//!
//! A fork writing a label primary never flushed creates an empty table for it
//! on primary. The key backfill over it rewrote nothing, the replace kept the
//! old schema, and the catalog registered the key columns anyway: every later
//! primary flush of the label failed with "has no system column '__key_…'",
//! across reopens.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(empty_table_key_columns)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::{Uni, UniConfig};

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        disable_fork_sweeper: true,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

#[tokio::test]
async fn empty_table_key_columns_reach_an_empty_label_table() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config()).build().await?;
    run(&db, "CREATE LABEL Mix (id INT, ts DATETIME, v BTIC)").await?;
    run(&db, "CREATE LABEL Other (id INT)").await?;
    run(&db, "CREATE (:Other {id: 1})").await?;
    db.flush().await?;
    // The fork's first write to Mix leaves an empty Mix table on primary.
    let fork = db.session().fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Mix {id: 50, v: btic('1985')})")
        .await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);
    let storage = db.storage();
    let backend = storage.head_backend();
    assert!(backend.table_exists("vertices_Mix").await?);
    assert_eq!(backend.count_rows("vertices_Mix", None).await?, 0);

    run(&db, "CREATE INDEX idx_ts FOR (e:Mix) ON (e.ts)").await?;
    run(&db, "CREATE INDEX idx_v FOR (e:Mix) ON (e.v)").await?;
    let columns: Vec<String> = backend
        .get_table_schema("vertices_Mix")
        .await?
        .expect("the table exists")
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for key in ["__key_dt_ts", "__key_lo_v", "__key_hi_v"] {
        assert!(
            columns.iter().any(|c| c == key),
            "{key} missing from {columns:?}"
        );
    }

    run(
        &db,
        "CREATE (:Mix {id: 1, ts: datetime('2020-01-01T00:00:00Z'), v: btic('1970')})",
    )
    .await?;
    db.flush().await?;
    drop(storage);
    db.shutdown().await?;

    let db = Uni::open(&path).config(config()).build().await?;
    let rows = db
        .session()
        .query(
            "MATCH (e:Mix) WHERE e.ts = datetime('2020-01-01T00:00:00Z') \
             AND btic_overlaps(e.v, btic('1960/1980')) RETURN e.id",
        )
        .await?;
    assert_eq!(rows.len(), 1);
    run(&db, "CREATE (:Mix {id: 2, v: btic('1971')})").await?;
    db.flush().await?;
    Ok(())
}
