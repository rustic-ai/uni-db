// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background work must be bounded and joinable (W1 follow-ups).
//!
//! * A background index rebuild started a new manager with its own worker on
//!   every call. The worker loops until shutdown, so each call left one more
//!   live task behind, and the managers overwrote each other's persisted
//!   task state.
//! * Async-flush stream tasks were awaited only by the coordinator's own
//!   `shutdown`, which `Drop for Uni` does not call, so a stream in flight at
//!   drop kept writing into the directory after `drop` returned, and
//!   `background_task_count` never counted it.

use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

// Rust guideline compliant

/// Repeated background rebuilds share one manager and one worker, and that
/// worker completes every queued task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_rebuilds_share_one_worker() -> Result<()> {
    let mut cfg = UniConfig::default();
    cfg.index_rebuild.worker_check_interval = Duration::from_millis(20);
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("N")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE INDEX idx_n_x FOR (n:N) ON (n.x)")
        .await?;
    tx.execute("UNWIND range(1, 50) AS i CREATE (:N {x: i})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let baseline = db.background_task_count();
    const CALLS: usize = 5;
    for _ in 0..CALLS {
        db.indexes().rebuild("N", true).await?;
    }
    let added = db.background_task_count().saturating_sub(baseline);
    assert!(
        added <= 1,
        "{CALLS} background rebuilds left {added} live tasks behind (one worker per call)"
    );
    // The one worker does the queued work, and `rebuild_status` sees it.
    wait_until_rebuilds_complete(&db).await?;
    db.indexes().rebuild("N", true).await?;
    wait_until_rebuilds_complete(&db).await?;
    assert!(db.background_task_count().saturating_sub(baseline) <= 1);

    db.shutdown_in_place().await?;
    assert_eq!(
        db.background_task_count(),
        0,
        "a rebuild worker outlived shutdown"
    );
    Ok(())
}

/// Polls `rebuild_status` until every task has completed.
async fn wait_until_rebuilds_complete(db: &Uni) -> Result<()> {
    let t = Instant::now();
    loop {
        let tasks = db.indexes().rebuild_status().await?;
        anyhow::ensure!(!tasks.is_empty(), "no rebuild task is tracked");
        if tasks
            .iter()
            .all(|t| t.status == uni_store::storage::IndexRebuildStatus::Completed)
        {
            return Ok(());
        }
        anyhow::ensure!(
            t.elapsed() < Duration::from_secs(30),
            "rebuild tasks did not complete: {tasks:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `drop` waits for an async-flush stream that is in flight, and the stream
/// counts as a background task while it runs.
///
/// The stream is held open by a failpoint callback that sleeps; "the stream
/// got past the sleep before `drop` returned" is what proves the wait.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_waits_for_an_in_flight_flush_stream() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SEAM: &str = "flush::after-rotate-before-lance";
    const HOLD: Duration = Duration::from_millis(1500);

    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let mut cfg = UniConfig::default();
    cfg.async_flush_enabled = true;
    cfg.auto_flush_threshold = 1;
    cfg.auto_flush_interval = None;
    let db = Uni::open(&path).config(cfg).build().await?;
    db.schema()
        .label("N")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let baseline = db.background_task_count();

    let entered = Arc::new(AtomicUsize::new(0));
    let left = Arc::new(AtomicUsize::new(0));
    {
        let (entered, left) = (Arc::clone(&entered), Arc::clone(&left));
        fail::cfg_callback(SEAM, move || {
            entered.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(HOLD);
            left.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    }

    // One committed row crosses `auto_flush_threshold`: the commit rotates L0
    // and spawns the stream without awaiting it.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {x: 1})").await?;
    tx.commit().await?;
    let t = Instant::now();
    while entered.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "the commit never started a flush stream; the test would prove nothing"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(
        db.background_task_count() > baseline,
        "the in-flight flush stream is not counted as a background task"
    );

    drop(db);
    let waited = left.load(Ordering::SeqCst);
    fail::remove(SEAM);
    assert_eq!(
        waited, 1,
        "drop returned while an async-flush stream was still in flight"
    );
    Ok(())
}
