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

/// Opens a database with an indexed label, plus a fork whose bulk load left a
/// background index-rebuild worker running against the fork's storage.
async fn db_with_fork_rebuild_worker(path: &str) -> Result<(Uni, uni_db::Session, usize)> {
    use std::collections::HashMap;

    let mut cfg = UniConfig::default();
    cfg.index_rebuild.worker_check_interval = Duration::from_millis(20);
    let db = Uni::open(path).config(cfg).build().await?;
    db.schema()
        .label("N")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE INDEX idx_n_x FOR (n:N) ON (n.x)")
        .await?;
    tx.execute("CREATE (:N {x: 0})").await?;
    tx.commit().await?;
    db.flush().await?;
    let baseline = db.background_task_count();

    let fork = db.session().fork("f").await?;
    {
        let tx = fork.tx().await?;
        let mut bulk = tx.bulk_writer().async_indexes(true).build()?;
        let rows: Vec<HashMap<String, uni_db::Value>> = (1..=20)
            .map(|i| HashMap::from([("x".to_string(), uni_db::Value::Int(i))]))
            .collect();
        bulk.insert_vertices("N", rows).await?;
        let stats = bulk.commit().await?;
        anyhow::ensure!(
            stats.indexes_pending,
            "the fork's bulk load scheduled no background rebuild; the test would prove nothing"
        );
    }
    Ok((db, fork, baseline))
}

/// A background worker started on a fork is a task of the database: counted by
/// the primary and stopped by its shutdown even while the fork session lives.
///
/// Fork and snapshot `UniInner`s each got a fresh `ShutdownHandle` that no
/// shutdown path signalled or awaited, so the worker was invisible to
/// `background_task_count` and kept looping after `shutdown_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_worker_is_counted_and_stopped_by_primary_shutdown() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let (db, fork, baseline) = db_with_fork_rebuild_worker(dir.path().to_str().unwrap()).await?;
    assert!(
        db.background_task_count() > baseline,
        "the fork's rebuild worker is not counted as a background task"
    );

    db.shutdown_in_place().await?;
    assert_eq!(
        db.background_task_count(),
        0,
        "a fork's rebuild worker outlived the database's shutdown"
    );
    drop(fork);
    Ok(())
}

/// Dropping the last session of a fork stops the workers that fork started.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_worker_stops_when_the_fork_is_dropped() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let (db, fork, baseline) = db_with_fork_rebuild_worker(dir.path().to_str().unwrap()).await?;
    assert!(
        db.background_task_count() > baseline,
        "the fork's rebuild worker is not counted as a background task"
    );

    drop(fork);
    let t = Instant::now();
    while db.background_task_count() > baseline {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "the fork's rebuild worker kept running after the fork was dropped"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    db.shutdown_in_place().await?;
    Ok(())
}

/// Bulk loads that defer their index rebuild share the database's one rebuild
/// manager and worker, like `indexes().rebuild(.., true)` does.
///
/// Each async-index bulk commit built its own `IndexRebuildManager` and
/// started its own worker, which loops until shutdown, so every such load
/// left one more live task behind, and the managers overwrote each other's
/// persisted task state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_loads_with_async_indexes_share_one_worker() -> Result<()> {
    use std::collections::HashMap;

    let mut cfg = UniConfig::default();
    cfg.index_rebuild.worker_check_interval = Duration::from_millis(20);
    let dir = tempfile::TempDir::new()?;
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(cfg)
        .build()
        .await?;
    db.schema()
        .label("N")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE INDEX idx_n_x FOR (n:N) ON (n.x)")
        .await?;
    tx.execute("CREATE (:N {x: 0})").await?;
    tx.commit().await?;
    db.flush().await?;
    // The first rebuild starts the database's one worker.
    db.indexes().rebuild("N", true).await?;
    wait_until_rebuilds_complete(&db).await?;
    let baseline = db.background_task_count();

    const LOADS: i64 = 3;
    for load in 0..LOADS {
        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().async_indexes(true).build()?;
        let rows: Vec<HashMap<String, uni_db::Value>> = (1..=10)
            .map(|i| HashMap::from([("x".to_string(), uni_db::Value::Int(load * 100 + i))]))
            .collect();
        bulk.insert_vertices("N", rows).await?;
        let stats = bulk.commit().await?;
        anyhow::ensure!(stats.indexes_pending, "load {load} scheduled no rebuild");
        tx.commit().await?;
    }
    let added = db.background_task_count().saturating_sub(baseline);
    assert_eq!(
        added, 0,
        "{LOADS} async-index bulk loads left {added} live tasks behind (one worker per load)"
    );
    // The one worker runs the queued rebuilds, and `rebuild_status` sees them.
    wait_until_rebuilds_complete(&db).await?;
    db.shutdown_in_place().await?;
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

/// Fail point inside `IndexRebuildManager::save_state`.
#[cfg(feature = "failpoints")]
const SAVE_STATE: &str = "index_rebuild::save-state";

/// A label `N` with an index over `n.x`, built over `rows` flushed rows.
#[cfg(feature = "failpoints")]
async fn indexed_label(db: &Uni, rows: i64) -> Result<()> {
    db.schema()
        .label("N")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(1, {rows}) AS i CREATE (:N {{x: i}})"
    ))
    .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE INDEX idx_n_x FOR (n:N) ON (n.x)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

/// A failed rebuild's retry is not armed as a task of its own.
///
/// `handle_rebuild_failure` used to spawn a detached task that slept for
/// `retry_delay`, then re-queued the task and wrote the state file. Nothing
/// tracked it, so it wrote into the directory after the database had shut
/// down. The retry is now the worker's job, on its own tick.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_rebuild_writes_nothing_after_shutdown() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uni_store::storage::IndexRebuildStatus;

    const RETRY: Duration = Duration::from_secs(1);
    let saves = Arc::new(AtomicUsize::new(0));
    {
        let saves = Arc::clone(&saves);
        fail::cfg_callback(SAVE_STATE, move || {
            saves.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    }
    fail::cfg("index_rebuild::execute", "return").unwrap();

    let mut cfg = UniConfig::default();
    cfg.index_rebuild.worker_check_interval = Duration::from_millis(20);
    cfg.index_rebuild.retry_delay = RETRY;
    let dir = tempfile::TempDir::new()?;
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(cfg)
        .build()
        .await?;
    indexed_label(&db, 10).await?;
    db.indexes().rebuild("N", true).await?;
    let t = Instant::now();
    loop {
        let tasks = db.indexes().rebuild_status().await?;
        if tasks.iter().any(|t| t.status == IndexRebuildStatus::Failed) {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "the injected failure never failed a rebuild task: {tasks:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    db.shutdown_in_place().await?;
    let at_shutdown = saves.load(Ordering::SeqCst);
    tokio::time::sleep(RETRY * 2).await;
    let after = saves.load(Ordering::SeqCst);
    fail::remove(SAVE_STATE);
    fail::remove("index_rebuild::execute");
    assert_eq!(
        after, at_shutdown,
        "the rebuild state was written after shutdown returned: a retry outlived its database"
    );
    Ok(())
}

/// The post-flush rebuild check queues work without a task of its own.
///
/// It used to persist the queued tasks from a detached `tokio::spawn`, which
/// nothing tracked: a slow write finished after the database had shut down.
/// The first state write after the flush is held for `HOLD` here; it must
/// complete before shutdown returns.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_flush_rebuild_scheduling_writes_nothing_after_shutdown() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const HOLD: Duration = Duration::from_secs(2);
    let mut cfg = UniConfig::default();
    cfg.index_rebuild.auto_rebuild_enabled = true;
    // The worker's first tick is immediate; the next is out of reach, so the
    // only worker write in the test is its exit write.
    cfg.index_rebuild.worker_check_interval = Duration::from_secs(3600);
    let dir = tempfile::TempDir::new()?;
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(cfg)
        .build()
        .await?;
    indexed_label(&db, 10).await?;

    let slow = Arc::new(AtomicBool::new(true));
    let completed = Arc::new(AtomicUsize::new(0));
    {
        let (slow, completed) = (Arc::clone(&slow), Arc::clone(&completed));
        fail::cfg_callback(SAVE_STATE, move || {
            if slow.swap(false, Ordering::SeqCst) {
                std::thread::sleep(HOLD);
            }
            completed.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    }
    // Growth past `growth_trigger_ratio` (default 0.5) of the built row count
    // makes the flush queue a rebuild.
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(11, 30) AS i CREATE (:N {x: i})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let queued = db.indexes().rebuild_status().await?;
    assert!(
        !queued.is_empty(),
        "the flush queued no rebuild; the test would prove nothing"
    );

    db.shutdown_in_place().await?;
    let at_shutdown = completed.load(Ordering::SeqCst);
    tokio::time::sleep(HOLD + Duration::from_secs(1)).await;
    let after = completed.load(Ordering::SeqCst);
    fail::remove(SAVE_STATE);
    assert_eq!(
        after, at_shutdown,
        "the rebuild state was written after shutdown returned: the post-flush \
         scheduling ran in an untracked task"
    );
    Ok(())
}

/// A slow background flush does not time out user commits.
///
/// The time-based auto-flush task called `flush_to_l1`, which holds the
/// commit lock (`flush_lock`) across the whole Lance write. A commit waits for
/// that lock for at most `commit_timeout`, so whenever one background flush
/// took longer than that (a loaded machine, a slow disk, a debug build) a user
/// commit failed with `CommitTimeout`, though nothing else was committing.
/// The commit path's own flush trigger rotates under the lock and writes
/// Lance outside it; the background task now does the same.
///
/// The Lance write is slowed past `commit_timeout` by a fail point.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_background_flush_does_not_time_out_commits() -> Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const SEAM: &str = "flush::after-rotate-before-lance";
    const HOLD: Duration = Duration::from_millis(2500);

    let dir = tempfile::TempDir::new()?;
    let mut cfg = UniConfig::default();
    cfg.async_flush_enabled = true;
    cfg.auto_flush_threshold = usize::MAX / 2;
    cfg.auto_flush_interval = Some(Duration::from_millis(200));
    cfg.commit_timeout = Duration::from_secs(1);
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(cfg)
        .build()
        .await?;
    db.schema()
        .label("N")
        .property("x", DataType::Int64)
        .apply()
        .await?;

    let slow = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicUsize::new(0));
    {
        let (slow, entered) = (Arc::clone(&slow), Arc::clone(&entered));
        fail::cfg_callback(SEAM, move || {
            if slow.swap(false, Ordering::SeqCst) {
                entered.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(HOLD);
            }
        })
        .unwrap();
    }

    // One unflushed row, then no commits: the next background tick past the
    // interval is what flushes it. The flush first restarts the interval, so
    // the commit itself (which checks the same interval) does not flush.
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {x: 1})").await?;
    tx.commit().await?;
    slow.store(true, Ordering::SeqCst);
    // Waits on this thread rather than on a tokio timer: while the held flush
    // blocks a worker, timer wake-ups can be delayed until it returns, and
    // the wait would then end only after the window it is meant to catch.
    tokio::task::block_in_place(|| {
        let t = Instant::now();
        while entered.load(Ordering::SeqCst) == 0 {
            assert!(
                t.elapsed() < Duration::from_secs(10),
                "no background flush reached {SEAM}; the test would prove nothing"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    // The background flush is inside its Lance write now.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {x: 2})").await?;
    let started = Instant::now();
    let res = tx.commit().await;
    let took = started.elapsed();
    fail::remove(SEAM);
    assert!(
        res.is_ok(),
        "a commit failed after {took:?} while a background flush was writing: {:?}",
        res.err()
    );
    db.shutdown_in_place().await?;
    Ok(())
}
