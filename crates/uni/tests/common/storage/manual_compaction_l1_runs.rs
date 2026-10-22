// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A manual full compaction relieves write throttling.
//!
//! Measured on df777cb3e before the fix: `compaction().compact(label)`,
//! `compaction().compact(edge_type)` and `CALL uni.admin.compact()` all
//! optimised tables, the flush-generation count (`l1_runs`) that drives the
//! write throttle stayed at 3, and the next `CREATE` timed out at the hard
//! limit. Only a background pass reset the count. `compact()` also ran no
//! semantic compaction (Tier 2) and took neither the compaction guard nor the
//! directory lock, so it could run beside a background pass.
//!
//! Decided: a full manual compaction is the background pass -- same tiers,
//! guard and lock -- and lowers `l1_runs` by the runs it saw at its start.
//! Scoped `compact_label` / `compact_edge_type` leave the count alone.

// Rust guideline compliant

use std::time::Duration;

use uni_common::config::WriteThrottleConfig;
use uni_db::{DataType, Uni, UniConfig};

/// Hard limit 3; background compaction enabled but never ticking, so only a
/// manual pass can lower the count.
fn config() -> UniConfig {
    let mut config = UniConfig::default();
    config.compaction.enabled = true;
    config.compaction.check_interval = Duration::from_secs(3600);
    config.throttle = WriteThrottleConfig {
        soft_limit: 2,
        hard_limit: 3,
        base_delay: Duration::from_millis(1),
    };
    config.query_timeout = Duration::from_secs(20);
    config
}

/// `Item {n}` and `R`, then `runs` flush generations.
async fn db_with_runs(path: &str, runs: usize) -> Uni {
    let db = Uni::open(path).config(config()).build().await.unwrap();
    db.schema()
        .label("Item")
        .property("n", DataType::Int64)
        .apply()
        .await
        .unwrap();
    db.schema()
        .edge_type("R", &["Item"], &["Item"])
        .apply()
        .await
        .unwrap();
    for i in 0..runs {
        let tx = db.session().tx().await.unwrap();
        tx.execute(&format!(
            "CREATE (a:Item {{n: {i}}})-[:R]->(:Item {{n: {}}})",
            i + 100
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();
    }
    assert_eq!(
        db.metrics().l1_run_count,
        runs,
        "premise: flush generations"
    );
    db
}

/// Scoped compactions leave `l1_runs` alone; `uni.admin.compact()` subtracts
/// the runs it folded, and a writer stalled at the hard limit proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_full_compaction_unblocks_a_throttled_writer() {
    let dir = tempfile::tempdir().unwrap();
    let db = std::sync::Arc::new(db_with_runs(dir.path().to_str().unwrap(), 3).await);

    db.compaction().compact("Item").await.unwrap();
    db.compaction().compact("R").await.unwrap();
    assert_eq!(
        db.metrics().l1_run_count,
        3,
        "scoped compactions do not fold deltas, so they must not lower the count"
    );

    // A writer at the hard limit stalls.
    let writer = {
        let db = std::sync::Arc::clone(&db);
        tokio::spawn(async move {
            let tx = db.session().tx().await?;
            tx.execute("CREATE (:Item {n: 999})").await?;
            tx.commit().await?;
            Ok::<_, uni_db::UniError>(())
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !writer.is_finished(),
        "premise: the write must be stalled at the hard limit"
    );

    let rows = db.session().run("CALL uni.admin.compact()").await.unwrap();
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(
        db.metrics().l1_run_count,
        0,
        "a full manual compaction must fold the runs it saw"
    );

    let unblocked = tokio::time::timeout(Duration::from_secs(5), writer).await;
    assert!(
        matches!(unblocked, Ok(Ok(Ok(())))),
        "the stalled writer must proceed after the manual compaction: {unblocked:?}"
    );
    let count = db
        .session()
        .query("MATCH (n:Item) RETURN count(n) AS c")
        .await
        .unwrap();
    assert_eq!(count.rows()[0].get::<i64>("c").unwrap(), 7);
    let traversed = db
        .session()
        .query("MATCH (:Item)-[r:R]->(:Item) RETURN count(r) AS c")
        .await
        .unwrap();
    assert_eq!(
        traversed.rows()[0].get::<i64>("c").unwrap(),
        3,
        "the semantic pass must keep every edge"
    );
}

/// `compact()` waits for a running pass instead of overlapping it: two
/// manual passes started together both complete, one after the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_manual_compactions_serialize() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_runs(dir.path().to_str().unwrap(), 2).await;
    let storage = db.storage();
    let before = storage.compaction_status().unwrap().total_compactions;
    let (a, b) = tokio::join!(storage.compact(), storage.compact());
    a.unwrap();
    b.unwrap();
    let status = storage.compaction_status().unwrap();
    assert_eq!(status.total_compactions, before + 2);
    assert!(!status.compaction_in_progress);
    assert_eq!(status.l1_runs, 0);
}
