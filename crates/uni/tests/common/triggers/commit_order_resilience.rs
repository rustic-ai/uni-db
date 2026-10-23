// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Commit notifications reach watchers and CDC in commit (version) order.
//!
//! A commit becomes visible under the writer's serialization lock but is
//! announced after the lock is released, so a second commit can finish in
//! between. The announcement order must still be the commit order: CDC builds
//! each batch's LSN range from the notification and checkpoints the newest
//! LSN, so an out-of-order pair regresses the checkpoint, and a consumer
//! resuming from it would skip the commit delivered late.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, Uni};

use super::event_support::{attach_cdc, wait_for_batches};

async fn ab_db() -> Result<Arc<Uni>> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    Ok(Arc::new(db))
}

/// Assert that `versions` (as announced) is strictly increasing and is exactly
/// the set of `committed` versions.
fn assert_in_commit_order(what: &str, announced: &[u64], committed: &[u64]) {
    let mut sorted = committed.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        announced, sorted,
        "{what}: announced versions {announced:?}, commits returned {committed:?}"
    );
}

/// Deterministic: commit A pauses after its commit point, commit B completes
/// and announces, then A resumes. Both the watch stream and CDC must still see
/// A before B, with A's own version.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_paused_before_notify_is_still_announced_first_resilience() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    let db = ab_db().await?;
    let cdc = attach_cdc(&db, "CREATE (:A {x: -1})").await?;
    let mut watch = db.session().watch();

    // The first commit to reach the seam parks there until released; every
    // later one passes straight through.
    let armed = Arc::new(AtomicBool::new(true));
    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let paused_tx = parking_lot::Mutex::new(paused_tx);
    let release_rx = parking_lot::Mutex::new(release_rx);
    {
        let armed = Arc::clone(&armed);
        fail::cfg_callback("commit::before-notify", move || {
            if armed.swap(false, Ordering::SeqCst) {
                paused_tx.lock().send(()).unwrap();
                // Bounded so a regression cannot hang the suite.
                let _ = release_rx.lock().recv_timeout(Duration::from_secs(30));
            }
        })
        .unwrap();
    }

    let db_a = Arc::clone(&db);
    let a = tokio::spawn(async move {
        let tx = db_a.session().tx().await?;
        tx.execute("CREATE (:A {x: 1})").await?;
        anyhow::Ok(tx.commit().await?.version)
    });
    tokio::task::spawn_blocking(move || paused_rx.recv_timeout(Duration::from_secs(30))).await??;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {x: 2})").await?;
    let b_version = tx.commit().await?.version;

    release_tx.send(())?;
    let a_version = a.await??;
    fail::remove("commit::before-notify");

    let mut announced = Vec::new();
    for _ in 0..2 {
        let n = tokio::time::timeout(Duration::from_secs(10), watch.next())
            .await?
            .expect("watch stream open");
        announced.push(n.version);
    }
    wait_for_batches(&cdc.delivered, 2).await;
    let lsns: Vec<u64> = cdc.delivered.lock().iter().map(|b| b.lsn_end.0).collect();

    // A reached its commit point first, so it owns the lower version.
    assert!(
        a_version < b_version,
        "commit A returned version {a_version}, B returned {b_version}; watch announced \
         {announced:?}, CDC lsn_end {lsns:?}"
    );
    assert_in_commit_order("watch", &announced, &[a_version, b_version]);
    assert_in_commit_order("CDC lsn_end", &lsns, &[a_version, b_version]);
    let checkpoints: Vec<u64> = cdc.checkpoints.lock().iter().map(|c| c.0).collect();
    assert!(
        checkpoints.windows(2).all(|w| w[0] <= w[1]),
        "CDC checkpoint regressed: {checkpoints:?}"
    );
    Ok(())
}

/// Many concurrent committers: every commit is announced once, with its own
/// version, in version order. Not deterministic (the deterministic case is the
/// failpoint test above), but runs in every lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_commits_are_announced_in_version_order() -> Result<()> {
    const WRITERS: i64 = 8;
    const PER_WRITER: i64 = 25;
    let db = ab_db().await?;
    let cdc = attach_cdc(&db, "CREATE (:A {x: -1})").await?;
    let mut watch = db.session().watch();

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let db = Arc::clone(&db);
        handles.push(tokio::spawn(async move {
            let mut versions = Vec::new();
            for i in 0..PER_WRITER {
                let tx = db.session().tx().await?;
                tx.execute(&format!("CREATE (:A {{x: {}}})", w * 1000 + i))
                    .await?;
                versions.push(tx.commit().await?.version);
            }
            anyhow::Ok(versions)
        }));
    }
    let mut committed = Vec::new();
    for h in handles {
        committed.extend(h.await??);
    }
    let total = committed.len();
    assert_eq!(
        committed.iter().collect::<HashSet<_>>().len(),
        total,
        "two commits returned the same version: {committed:?}"
    );

    let mut announced = Vec::new();
    for _ in 0..total {
        let n = tokio::time::timeout(Duration::from_secs(10), watch.next())
            .await?
            .expect("watch stream open");
        assert_eq!(n.dropped_before, 0, "fixture: the watcher lagged");
        announced.push(n.version);
    }
    assert_in_commit_order("watch", &announced, &committed);

    wait_for_batches(&cdc.delivered, total).await;
    let lsns: Vec<u64> = cdc.delivered.lock().iter().map(|b| b.lsn_end.0).collect();
    assert_in_commit_order("CDC lsn_end", &lsns, &committed);
    Ok(())
}
