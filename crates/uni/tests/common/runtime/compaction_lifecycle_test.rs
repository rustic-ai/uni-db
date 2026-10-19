// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Background compaction must not outlive its database (W1).
//!
//! Before the fix, `Drop for Uni` only broadcast shutdown: a pass in flight
//! kept rewriting the directory after its owner was gone. Removing the
//! directory then panicked the detached task, and reopening the path at once
//! ran two compactors over it — measured as an overlap in 19/19
//! drop-and-reopen iterations, with the live handle answering a reverse
//! traversal with no rows in 4 of them.
//!
//! "A pass is in flight" is observed through the directory's compaction lock
//! (`CompactionDirLock`): a pass holds it for its whole duration, so a failed
//! `try_acquire` proves one is running, and a successful one proves none is.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::{Uni, UniConfig, Value};
use uni_store::storage::compaction::CompactionDirLock;

// Rust guideline compliant

/// Compaction that starts on the first tick after any flush and on any
/// non-empty delta, so a reopened handle compacts at once too.
fn aggressive() -> UniConfig {
    let mut c = UniConfig::default();
    c.compaction.enabled = true;
    c.compaction.check_interval = Duration::from_millis(50);
    c.compaction.max_l1_runs = 1;
    c.compaction.max_l1_size_bytes = 1;
    c
}

const V: usize = 1000;
const E: usize = 4000;

fn storage_root(db_path: &Path) -> PathBuf {
    db_path.join("storage")
}

/// Bulk-loads `V` vertices and `E` edges keyed from `base`, then flushes.
async fn load(db: &Uni, base: usize, ddl: bool) -> Result<()> {
    if ddl {
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL P (k INT)").await?;
        tx.execute("CREATE EDGE TYPE E (w INT) FROM P TO P").await?;
        tx.commit().await?;
    }
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let props: Vec<HashMap<String, Value>> = (0..V)
        .map(|i| HashMap::from([("k".to_string(), Value::Int((base + i) as i64))]))
        .collect();
    let vids = bulk.insert_vertices("P", props).await?;
    let edges: Vec<uni_bulk::EdgeData> = (0..E)
        .map(|i| {
            let a = (i * 7919) % V;
            let b = (i * 104_729 + 13) % V;
            uni_bulk::EdgeData::new(
                vids[a],
                vids[b],
                HashMap::from([("w".to_string(), Value::Int((base + i) as i64))]),
            )
        })
        .collect();
    bulk.insert_edges("E", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

async fn scalar(db: &Uni, q: &str) -> Result<i64> {
    let r = db.session().query(q).await?;
    Ok(r.rows()[0].get::<i64>("x")?)
}

/// Edge count, weight sum and agreement of both traversal directions for
/// `batches` loads made by [`load`] with bases `0, 100_000, ...`.
async fn check_integrity(db: &Uni, batches: usize) -> Result<()> {
    let ev = (V * batches) as i64;
    let ee = (E * batches) as i64;
    let nv = scalar(db, "MATCH (n:P) RETURN count(n) AS x").await?;
    anyhow::ensure!(nv == ev, "vertex count {nv} != {ev}");
    let ne = scalar(db, "MATCH ()-[r:E]->() RETURN count(r) AS x").await?;
    anyhow::ensure!(ne == ee, "edge count {ne} != {ee}");
    let wsum: i64 = (0..batches)
        .map(|b| (0..E as i64).map(|i| (b * 100_000) as i64 + i).sum::<i64>())
        .sum();
    let ws = scalar(db, "MATCH ()-[r:E]->() RETURN sum(r.w) AS x").await?;
    anyhow::ensure!(ws == wsum, "weight sum {ws} != {wsum}");
    let fwd = scalar(
        db,
        "MATCH (a:P)-[r:E]->(b:P) RETURN sum(a.k * 1000003 + b.k) AS x",
    )
    .await?;
    let bwd = scalar(
        db,
        "MATCH (b:P)<-[r:E]-(a:P) RETURN sum(a.k * 1000003 + b.k) AS x",
    )
    .await?;
    anyhow::ensure!(fwd == bwd, "fwd {fwd} and bwd {bwd} adjacency disagree");
    let fc = scalar(db, "MATCH (a:P)-[:E]->(b:P) RETURN count(*) AS x").await?;
    let bc = scalar(db, "MATCH (b:P)<-[:E]-(a:P) RETURN count(*) AS x").await?;
    anyhow::ensure!(
        (fc, bc) == (ee, ee),
        "traversal counts fwd={fc} bwd={bc}, expected {ee}"
    );
    Ok(())
}

/// Waits until a compaction pass holds the directory's lock.
async fn wait_for_pass_in_flight(root: &Path) -> Result<()> {
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(20) {
        if CompactionDirLock::try_acquire(root)?.is_none() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    anyhow::bail!("no compaction pass started within 20s; the test would prove nothing")
}

/// Counts panics on any thread, including detached tokio tasks, which a test
/// otherwise never sees.
fn install_panic_counter() -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        seen.fetch_add(1, Ordering::SeqCst);
        previous(info);
    }));
    count
}

/// Dropping a database mid-pass stops the pass before `drop` returns, and
/// deleting the directory afterwards panics nothing.
///
/// Covers both waiting branches of `Drop for Uni`: a drop on a runtime worker
/// (`block_in_place`) and a drop on a plain thread with no runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_while_compacting_stops_the_pass_and_never_panics() -> Result<()> {
    let panics = install_panic_counter();
    for on_plain_thread in [false, true] {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap().to_string();
        let root = storage_root(dir.path());
        let db = Uni::open(&path).config(aggressive()).build().await?;
        load(&db, 0, true).await?;
        wait_for_pass_in_flight(&root).await?;

        if on_plain_thread {
            std::thread::spawn(move || drop(db)).join().unwrap();
        } else {
            drop(db);
        }
        assert!(
            CompactionDirLock::try_acquire(&root)?.is_some(),
            "a compaction pass was still running after drop returned \
             (plain thread: {on_plain_thread})"
        );

        // What the panicking runs did: remove the directory under the pass.
        drop(dir);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(
            panics.load(Ordering::SeqCst),
            0,
            "a background task panicked after the database was dropped and its \
             directory removed (plain thread: {on_plain_thread})"
        );
    }
    Ok(())
}

/// `shutdown_in_place` returns with no tracked task alive and no pass running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_leaves_no_background_task_alive() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let root = storage_root(dir.path());
    let db = Uni::open(&path).config(aggressive()).build().await?;
    load(&db, 0, true).await?;
    wait_for_pass_in_flight(&root).await?;
    assert!(
        db.background_task_count() > 0,
        "nothing was tracked to begin with"
    );

    db.shutdown_in_place().await?;
    assert_eq!(
        db.background_task_count(),
        0,
        "a tracked task outlived shutdown"
    );
    assert!(
        CompactionDirLock::try_acquire(&root)?.is_some(),
        "a compaction pass was still running after shutdown returned"
    );
    Ok(())
}

/// `shutdown_in_place` cancels a pass in flight instead of waiting for it to
/// run to completion.
///
/// The pass is held at a safe point by a fail point that waits for the
/// compaction token to be cancelled, for at most `HOLD`. Released by the
/// cancel, the pass stops at that safe point; without the cancel, shutdown
/// would signal the background loop, which does not look at the signal until
/// the pass ends, and wait out the whole hold.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_the_pass_in_flight() -> Result<()> {
    const SEAM: &str = "compaction::safe-point::before-adjacency";
    const HOLD: Duration = Duration::from_secs(10);

    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let root = storage_root(dir.path());
    let db = Uni::open(&path).config(aggressive()).build().await?;

    let entered = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    {
        let token = db.storage().compaction_cancel_token().clone();
        let (entered, cancelled) = (Arc::clone(&entered), Arc::clone(&cancelled));
        fail::cfg_callback(SEAM, move || {
            entered.fetch_add(1, Ordering::SeqCst);
            let t = Instant::now();
            while t.elapsed() < HOLD {
                if token.is_cancelled() {
                    cancelled.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        })
        .unwrap();
    }
    load(&db, 0, true).await?;
    let t = Instant::now();
    while entered.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "no compaction pass reached {SEAM}; the test would prove nothing"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let t = Instant::now();
    db.shutdown_in_place().await?;
    let took = t.elapsed();
    fail::remove(SEAM);
    assert_eq!(
        cancelled.load(Ordering::SeqCst),
        1,
        "shutdown did not cancel the pass in flight; it waited {took:?} for it to finish"
    );
    assert!(took < HOLD, "shutdown took {took:?}");
    assert!(
        CompactionDirLock::try_acquire(&root)?.is_some(),
        "a compaction pass was still running after shutdown returned"
    );
    Ok(())
}

/// A writable open waits while another handle compacts the same directory,
/// since that pass's table overwrites would discard this handle's writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writable_open_waits_for_a_foreign_compaction() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let root = storage_root(dir.path());
    Uni::open(&path).build().await?.shutdown().await?;

    let foreign = CompactionDirLock::try_acquire(&root)?.expect("lock is free after shutdown");
    let open = tokio::spawn({
        let path = path.clone();
        async move { Uni::open(&path).build().await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !open.is_finished(),
        "a writable open went ahead while another handle held the compaction lock"
    );
    drop(foreign);
    let db = tokio::time::timeout(Duration::from_secs(30), open)
        .await
        .expect("open did not proceed after the lock was released")??;
    db.shutdown().await?;
    Ok(())
}

/// Drop a handle mid-pass and reopen the path at once, as the perf probe's
/// `build()` did. The store must stay intact for the live handle and after a
/// clean reopen. Not a timing test: the guarantees come from the drop join and
/// the directory lock, and the pass is asserted to be in flight at the drop.
///
/// One iteration: the reopened handle's pass costs ~20 s in a debug build
/// (a per-EID `main_edges` check over every delta), and the deterministic
/// tests above carry the revert checks. The wrong answer this guards against
/// showed in 4 of 19 iterations before the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_and_reopen_mid_compaction_keeps_the_store_intact() -> Result<()> {
    {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap().to_string();
        let root = storage_root(dir.path());
        {
            let a = Uni::open(&path).config(aggressive()).build().await?;
            load(&a, 0, true).await?;
            wait_for_pass_in_flight(&root).await?;
        }
        let b = Uni::open(&path).config(aggressive()).build().await?;
        load(&b, 100_000, false).await?;
        b.compaction().wait().await?;
        check_integrity(&b, 2).await?;
        b.shutdown().await?;

        let mut cfg = UniConfig::default();
        cfg.compaction.enabled = false;
        let c = Uni::open(&path).config(cfg).build().await?;
        check_integrity(&c, 2).await?;
        c.shutdown().await?;
    }
    Ok(())
}
