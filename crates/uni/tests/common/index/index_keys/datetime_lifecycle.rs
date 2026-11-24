// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T3: a DATETIME index's lifecycle. Declared before data and after it (the
//! key column is backfilled), dropped (the key column unregistered), rebuilt,
//! and reopened. An index recorded `Failed` before keys existed is upgraded at
//! the next writable open (`upgrade_outside_flush`).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(datetime_lifecycle)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::Uni;

const QUERY: &str =
    "MATCH (e:Event) WHERE e.ts >= datetime('2026-01-01T12:00:00Z') RETURN e.id AS id";

async fn ids(db: &Uni) -> Result<Vec<i64>> {
    let rows = db.session().query(QUERY).await?;
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

async fn status(db: &Uni, index: &str) -> Result<Option<String>> {
    Ok(db
        .get_label_info("Event")
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == index))
        .map(|i| i.status))
}

fn key_registered(db: &Uni) -> bool {
    db.storage()
        .schema_manager()
        .schema()
        .properties
        .get("Event")
        .is_some_and(|p| p.contains_key("__key_dt_ts"))
}

fn keyed(db: &Uni, index: &str) -> bool {
    db.storage()
        .schema_manager()
        .get_index(index)
        .is_some_and(|def| match def {
            uni_db::core::schema::IndexDefinition::Scalar(cfg) => cfg.key.is_some(),
            _ => false,
        })
}

async fn used(db: &Uni) -> Result<bool> {
    let explain = db.session().query_with(QUERY).explain().await?;
    Ok(explain
        .index_usage
        .iter()
        .any(|u| u.property == "ts" && u.used))
}

async fn insert(db: &Uni, from: i64) -> Result<()> {
    run(
        db,
        &format!(
            "CREATE (:Event {{id: {from}, ts: datetime('2026-01-01T12:00:00Z')}}), \
                    (:Event {{id: {}, ts: datetime('2025-01-01T12:00:00Z')}})",
            from + 1
        ),
    )
    .await
}

#[tokio::test]
async fn datetime_lifecycle_declared_before_and_after_data() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, "CREATE LABEL Event (id INT, ts DATETIME)").await?;
    run(&db, "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)").await?;
    assert!(keyed(&db, "idx_ts"));
    assert!(key_registered(&db), "registered at declaration");
    insert(&db, 1).await?;
    db.flush().await?;
    assert_eq!(status(&db, "idx_ts").await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1]);
    assert!(used(&db).await?);

    // Drop: the key column goes with the index; reads and writes keep working.
    run(&db, "DROP INDEX idx_ts").await?;
    assert!(!key_registered(&db), "the key column outlived its index");
    insert(&db, 3).await?;
    db.flush().await?;
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(!used(&db).await?);

    // Re-created over flushed rows, those written while dropped included.
    run(&db, "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)").await?;
    assert_eq!(status(&db, "idx_ts").await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(used(&db).await?);

    // Rebuilt, synchronously.
    db.indexes().rebuild("Event", false).await?;
    assert_eq!(status(&db, "idx_ts").await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1, 3]);
    Ok(())
}

#[tokio::test]
async fn datetime_lifecycle_survives_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    {
        let db = Uni::open(&path).build().await?;
        run(&db, "CREATE LABEL Event (id INT, ts DATETIME)").await?;
        insert(&db, 1).await?;
        db.flush().await?;
        run(&db, "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)").await?;
        insert(&db, 3).await?;
        db.flush().await?;
        assert_eq!(ids(&db).await?, vec![1, 3]);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).build().await?;
    assert!(keyed(&db, "idx_ts"));
    assert_eq!(status(&db, "idx_ts").await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(used(&db).await?);
    insert(&db, 5).await?;
    assert_eq!(ids(&db).await?, vec![1, 3, 5], "unflushed after reopen");
    db.flush().await?;
    assert_eq!(ids(&db).await?, vec![1, 3, 5]);
    Ok(())
}
