// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! EXPLAIN reports a keyed index as the physical planner uses it: on a fork
//! and a pinned snapshot (`VERSION AS OF`) the scan bounds no key (the index
//! cannot vouch for those views), yet EXPLAIN reported the keyed index `used`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(keyed_pushdown_views)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::{Session, Uni, UniConfig};

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

async fn ids(session: &Session, query: &str) -> Result<Vec<i64>> {
    let rows = session.query(query).await?;
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

async fn ts_used(session: &Session, query: &str) -> Result<bool> {
    let explain = session.query_with(query).explain().await?;
    Ok(explain
        .index_usage
        .iter()
        .any(|u| u.property == "ts" && u.used))
}

async fn insert(db: &Uni) -> Result<()> {
    run(
        db,
        "CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')}), \
                (:Event {id: 2, ts: datetime('2025-01-01T12:00:00Z')})",
    )
    .await
}

#[tokio::test]
async fn keyed_pushdown_views_explain_unused_on_fork_and_pinned_snapshot() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    run(&db, "CREATE LABEL Event (id INT, ts DATETIME)").await?;
    run(&db, "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)").await?;
    insert(&db).await?;
    db.flush().await?;
    let primary = db.session();
    assert!(ts_used(&primary, QUERY).await?, "primary bounds the key");

    let snapshot = db.create_snapshot("s").await?;
    let pinned = format!("{QUERY} VERSION AS OF '{snapshot}'");
    assert!(
        !ts_used(&primary, &pinned).await?,
        "a pinned snapshot bounds no key, so EXPLAIN must not report it used"
    );
    assert_eq!(ids(&primary, &pinned).await?, vec![1]);

    let fork = primary.fork("f").await?;
    assert!(
        !ts_used(&fork, QUERY).await?,
        "a fork bounds no key, so EXPLAIN must not report it used"
    );
    assert_eq!(ids(&fork, QUERY).await?, vec![1]);
    Ok(())
}
