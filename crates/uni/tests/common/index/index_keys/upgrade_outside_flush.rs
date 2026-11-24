// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An index that needs a table rewrite to become keyed is upgraded at the
//! next writable open, never inside a flush or on a fork.
//!
//! The upgrade used to run at the next flush (`ensure_declared_indexes`). On
//! a fork the key backfill is refused, so every fork flush of the label
//! failed. On primary the backfill joined the flush's unit, and the catalog
//! save naming the key column ran ahead of the manifest naming the rewritten
//! table.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(upgrade_outside_flush)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::{Uni, UniConfig};

const QUERY: &str =
    "MATCH (e:Event) WHERE e.ts >= datetime('2026-01-01T12:00:00Z') RETURN e.id AS id";

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

fn keyed(db: &Uni) -> bool {
    matches!(
        db.storage().schema_manager().get_index("idx_ts"),
        Some(uni_db::core::schema::IndexDefinition::Scalar(cfg)) if cfg.key.is_some()
    )
}

fn key_registered(db: &Uni) -> bool {
    db.storage()
        .schema_manager()
        .schema()
        .properties
        .get("Event")
        .is_some_and(|p| p.contains_key("__key_dt_ts"))
}

async fn status(db: &Uni) -> Result<Option<String>> {
    Ok(db
        .get_label_info("Event")
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == "idx_ts"))
        .map(|i| i.status))
}

async fn ids(session: &uni_db::Session, query: &str) -> Result<Vec<i64>> {
    let rows = session.query(query).await?;
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

async fn used(db: &Uni) -> Result<bool> {
    let explain = db.session().query_with(QUERY).explain().await?;
    Ok(explain
        .index_usage
        .iter()
        .any(|u| u.property == "ts" && u.used))
}

/// An index declared while `ts` was undeclared, then `ts` typed DATETIME:
/// recorded without a key, over a flushed table.
async fn declare_unkeyed(db: &Uni) -> Result<()> {
    run(db, "CREATE LABEL Event (id INT)").await?;
    run(db, "CREATE (:Event {id: 0})").await?;
    db.flush().await?;
    run(db, "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)").await?;
    run(db, "ALTER LABEL Event ADD PROPERTY ts DATETIME").await?;
    assert!(!keyed(db), "the scenario needs an unkeyed index");
    Ok(())
}

#[tokio::test]
async fn upgrade_outside_flush_fork_flush_succeeds_and_keeps_rows() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    declare_unkeyed(&db).await?;
    let fork = db.session().fork("f").await?;
    for id in [1, 2] {
        let tx = fork.tx().await?;
        tx.execute(&format!(
            "CREATE (:Event {{id: {id}, ts: datetime('2026-01-01T12:00:00Z')}})"
        ))
        .await?;
        tx.commit().await?;
        fork.flush().await?;
    }
    assert_eq!(
        ids(&fork, "MATCH (e:Event) RETURN e.id AS id").await?,
        vec![0, 1, 2]
    );
    assert_eq!(ids(&fork, QUERY).await?, vec![1, 2]);
    drop(fork);
    let fork = db.session().fork("f").await?;
    assert_eq!(
        ids(&fork, "MATCH (e:Event) RETURN e.id AS id").await?,
        vec![0, 1, 2],
        "the reopened fork keeps its rows"
    );

    // A primary flush does not upgrade it either: the catalog never names a
    // key column the flush's manifest does not.
    run(
        &db,
        "CREATE (:Event {id: 7, ts: datetime('2026-01-01T12:00:00Z')})",
    )
    .await?;
    db.flush().await?;
    assert!(
        !keyed(&db) && !key_registered(&db),
        "upgraded inside a flush"
    );
    assert_eq!(ids(&db.session(), QUERY).await?, vec![7]);
    Ok(())
}

/// The state a #356-era catalog holds (unkeyed and `Failed` on a DATETIME
/// property) is upgraded by the next writable open, and a read-only open
/// leaves it alone.
#[tokio::test]
async fn upgrade_outside_flush_runs_at_the_next_writable_open() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    {
        let db = Uni::open(&path).config(config()).build().await?;
        declare_unkeyed(&db).await?;
        db.storage()
            .schema_manager()
            .update_index_metadata("idx_ts", |m| {
                m.status = uni_db::core::schema::IndexStatus::Failed;
            })?;
        db.storage().schema_manager().save().await?;
        run(
            &db,
            "CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')}), \
                    (:Event {id: 2, ts: datetime('2025-01-01T12:00:00Z')})",
        )
        .await?;
        db.flush().await?;
        assert!(!keyed(&db), "a flush must not upgrade it");
        assert_eq!(status(&db).await?, Some("FAILED".into()));
        db.shutdown().await?;
    }
    {
        let db = Uni::open(&path)
            .config(config())
            .read_only()
            .build()
            .await?;
        assert!(!keyed(&db), "a read-only open upgraded it");
        assert_eq!(status(&db).await?, Some("FAILED".into()));
        assert_eq!(ids(&db.session(), QUERY).await?, vec![1]);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert!(keyed(&db), "the writable open must key it");
    assert!(key_registered(&db));
    assert_eq!(status(&db).await?, Some("ONLINE".into()));
    assert_eq!(ids(&db.session(), QUERY).await?, vec![1]);
    assert!(used(&db).await?);
    // The upgrade's rewrite kept the label's default indexes.
    let indexes: Vec<String> = db
        .storage()
        .head_backend()
        .list_indexes("vertices_Event")
        .await?
        .into_iter()
        .map(|i| i.name)
        .collect();
    assert!(
        indexes.iter().any(|n| n == "_vid_idx") && indexes.iter().any(|n| n == "idx_ts__dt"),
        "{indexes:?}"
    );
    Ok(())
}
