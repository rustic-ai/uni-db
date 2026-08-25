// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience: crash / recovery / abort-cleanup for SSI.
//!
//! These validate the durability boundary the design stakes its correctness on —
//! *validation happens before the WAL is touched, and the WAL flush is the commit
//! point* — end-to-end through a real close-and-reopen (the WAL replays from
//! disk), plus the abort-cleanup invariants (no leaked locks, pins, or registry
//! entries).
//!
//! The crash-injection tests (gated behind the `failpoints` feature) drive a
//! commit to panic at a precise seam (`commit::after-validate` /
//! `after-wal-flush` / `after-merge`), then reopen and assert **atomicity**: the
//! recovered value is all-or-nothing (`0` or the written value), never a partial
//! or corrupt state, and the database stays usable. A crash before the WAL touch
//! must recover NOTHING (no resurrection). Because the commit panicked before
//! `commit()` returned, the transaction was never acknowledged to the caller, so
//! whether a mid-commit crash recovers the value is unspecified — only that it is
//! atomic. Run with `--features ssi,failpoints`. Each test owns its failpoint and
//! runs in its own process under nextest, so the global registry does not bleed.

#[cfg(feature = "failpoints")]
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::ssi_support::reopen::DiskHarness;
use crate::ssi_support::schedule::{assert_committed, assert_serialization_conflict};

/// Sets up the `C(id, n)` schema and seeds `x = 0` on a freshly-opened db.
async fn init_schema_and_seed(db: &Uni) -> Result<()> {
    db.schema()
        .label("C")
        .property("id", DataType::String)
        .property("n", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:C {id: 'x', n: 0})").await?;
    tx.commit().await?;
    Ok(())
}

async fn read_n(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (c:C {id: 'x'}) RETURN c.n AS n")
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

// ── Reopen / recovery (no fault injection) ───────────────────────────────────

/// Baseline: a committed write survives close-and-reopen (WAL replay).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_write_survives_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 5").await?;
        tx.commit().await?;
        db.flush().await?;
    }
    let db = h.open().await?;
    assert_eq!(read_n(&db).await?, 5, "committed write lost across reopen");
    Ok(())
}

/// The central correctness claim, end-to-end: a transaction aborted by SSI
/// validation leaves NO trace after a real reopen — its mutations never reached
/// the WAL (validation runs before the WAL append). The winner's write persists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_aborted_tx_leaves_no_trace_through_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;

        let (sa, sb) = (db.session(), db.session());
        let ta = sa.tx().await?;
        let tb = sb.tx().await?; // snapshot before ta commits

        ta.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
        tb.execute("MATCH (c:C {id: 'x'}) SET c.n = 2").await?;

        assert_committed(ta.commit().await); // winner
        assert_serialization_conflict(tb.commit().await); // loser aborts

        db.flush().await?;
    }
    // Reopen: only the winner (n = 1) is durable; the aborted writer's n = 2
    // never touched the WAL, so it cannot resurrect on replay.
    let db = h.open().await?;
    assert_eq!(
        read_n(&db).await?,
        1,
        "aborted transaction resurrected after reopen"
    );
    Ok(())
}

/// After a reopen the in-memory commit registry is empty and conflict detection
/// resumes correctly: a fresh pair of concurrent transactions still conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_detection_resumes_after_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    let db = h.open().await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
    tb.execute("MATCH (c:C {id: 'x'}) SET c.n = 2").await?;
    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);
    Ok(())
}

// ── Abort cleanup ────────────────────────────────────────────────────────────

/// An aborted commit leaves no residue: it does not freeze a generation, the
/// FOR UPDATE lock map stays empty, and the database keeps accepting commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_leaves_no_residue() -> Result<()> {
    let h = DiskHarness::new()?;
    let db = h.open().await?;
    init_schema_and_seed(&db).await?;
    let writer = db.writer().expect("disk db has a writer");

    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
    tb.execute("MATCH (c:C {id: 'x'}) SET c.n = 2").await?;
    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);

    // The aborted transaction leaves no FOR UPDATE lock entries behind...
    assert_eq!(
        writer.for_update_lock_count(),
        0,
        "an abort must not leak FOR UPDATE lock entries"
    );
    // ...and the database is unharmed: a subsequent commit succeeds and the
    // value reflects only the winner plus the new write.
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 9").await?;
    assert_committed(tx.commit().await);
    assert_eq!(read_n(&db).await?, 9);
    Ok(())
}

/// A transaction older than the retained commit history aborts conservatively
/// with a (retriable) serialization conflict rather than silently missing a
/// possible conflict. Ignored by default: it commits 4097+ transactions to push
/// the long-running reader past the 4096-entry registry capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "slow: commits >4096 transactions to exceed the OCC registry capacity"]
async fn long_transaction_past_registry_capacity_aborts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    init_schema_and_seed(&db).await?;
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("CREATE (:C {id: 'long', n: 0})").await?;
        tx.commit().await?;
    }

    // A long-running reader pins an old read sequence.
    let s_long = db.session();
    let long = s_long.tx().await?;
    long.query("MATCH (c:C {id: 'long'}) RETURN c.n").await?;

    // Churn past the registry capacity with disjoint committed writes.
    for i in 0..4097 {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(&format!("CREATE (:C {{id: 'churn{i}', n: 0}})"))
            .await?;
        tx.commit().await?;
    }

    // The long reader now writes and commits: its read sequence predates the
    // retained history, so it must abort conservatively.
    long.execute("MATCH (c:C {id: 'long'}) SET c.n = 1").await?;
    assert_serialization_conflict(long.commit().await);
    Ok(())
}

// ── Crash-mid-commit atomicity (requires `failpoints`) ───────────────────────

/// Helper: run a `SET c.n = <val>` commit that is expected to panic at the
/// configured failpoint. Returns once the panicking task has been joined.
#[cfg(feature = "failpoints")]
async fn commit_that_crashes(db: Arc<Uni>, val: i64) {
    let res = tokio::spawn(async move {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute(&format!("MATCH (c:C {{id: 'x'}}) SET c.n = {val}"))
            .await
            .unwrap();
        tx.commit().await
    })
    .await;
    assert!(
        res.is_err(),
        "commit task should have panicked at the failpoint"
    );
}

/// After a mid-commit crash + reopen, the value is atomic (`0` or `val`, never a
/// partial state) and the database still accepts new writes. Usability is probed
/// with a *fresh* node (not the recovered one) to avoid any WAL-replay node
/// identity ambiguity for `x`.
#[cfg(feature = "failpoints")]
async fn assert_atomic_and_usable(db: &Uni, val: i64) -> Result<()> {
    let recovered = read_n(db).await?;
    assert!(
        recovered == 0 || recovered == val,
        "non-atomic recovery: n = {recovered} (expected 0 or {val})"
    );
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:C {id: 'probe', n: 1})").await?;
    assert_committed(tx.commit().await);
    let r = db
        .session()
        .query("MATCH (c:C {id: 'probe'}) RETURN c.n AS n")
        .await?;
    assert_eq!(
        r.rows()[0].value("n"),
        Some(&Value::Int(1)),
        "database unusable after crash recovery"
    );
    Ok(())
}

/// A crash AFTER validation but BEFORE the WAL append recovers nothing: the
/// transaction never became durable.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_validate_recovers_nothing() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    {
        let db = Arc::new(h.open().await?);
        fail::cfg("commit::after-validate", "panic").unwrap();
        commit_that_crashes(db.clone(), 42).await;
        fail::remove("commit::after-validate");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_n(&db).await?,
        0,
        "a crash before the WAL flush must leave no trace"
    );
    Ok(())
}

/// A crash AFTER the WAL flush but before the L0 merge is atomic on reopen — the
/// transaction was not acknowledged, so it recovers wholly or not at all, never
/// partially.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_wal_flush_is_atomic() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    {
        let db = Arc::new(h.open().await?);
        fail::cfg("commit::after-wal-flush", "panic").unwrap();
        commit_that_crashes(db.clone(), 7).await;
        fail::remove("commit::after-wal-flush");
        drop(db);
    }
    let db = h.open().await?;
    assert_atomic_and_usable(&db, 7).await
}

/// A crash AFTER the L0 merge but before the in-memory registry record is also
/// atomic on reopen (the registry is rebuilt empty, so it plays no part in
/// durability).
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_merge_is_atomic() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    {
        let db = Arc::new(h.open().await?);
        fail::cfg("commit::after-merge", "panic").unwrap();
        commit_that_crashes(db.clone(), 3).await;
        fail::remove("commit::after-merge");
        drop(db);
    }
    let db = h.open().await?;
    assert_atomic_and_usable(&db, 3).await
}

/// Lost-commit regression: a crash mid-flush (panic AFTER the L0 rotation but
/// BEFORE the Lance write) followed by a graceful close must not drop an
/// acknowledged commit that was sitting in the rotated buffer.
///
/// The failed flush leaves its buffer in `pending_flush` (WAL retains the data);
/// the subsequent shutdown flush must not truncate that buffer's WAL nor publish
/// a `wal_high_water_mark` past it. The bug keyed both off the pending buffer's
/// HIGH watermark (`wal_lsn_at_flush`) instead of its START watermark, so the
/// shutdown flush deleted the committed `n = 5` segment and checkpointed past it
/// — the value vanished on reopen. (Under a real, `Drop`-less crash the WAL was
/// already durable, so only the graceful-close path lost data.)
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_during_flush_preserves_committed_unflushed_commit() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = Arc::new(h.open().await?);
        init_schema_and_seed(&db).await?; // commits n = 0
        db.flush().await?; // n = 0 durable in L1
        // An acknowledged, unflushed commit — the value the crashing flush is
        // mid-rotating when it panics.
        {
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 5").await?;
            tx.commit().await?;
        }
        fail::cfg("flush::after-rotate-before-lance", "panic").unwrap();
        let db_f = db.clone();
        let res = tokio::spawn(async move { db_f.flush().await }).await;
        fail::remove("flush::after-rotate-before-lance");
        assert!(res.is_err(), "flush task should have panicked at the seam");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_n(&db).await?,
        5,
        "acknowledged commit lost across crash-during-flush + graceful reopen"
    );
    Ok(())
}

/// A corrupt (torn) WAL segment at the TAIL must not block reopen
/// (architecture review §2.5): the torn segment belongs to a commit that was
/// never acknowledged, so recovery skips it with a warning and replays
/// everything before it. Before the tail-vs-middle policy existed, ANY
/// corrupt segment hard-failed the whole recovery and left the database
/// unopenable after a simple crash.
#[tokio::test]
async fn corrupt_wal_tail_does_not_block_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        // Pin the post-flush `n = 5` commit to the WAL tail: disable the
        // time-based auto-flush so a background flush cannot promote it into
        // L1. Otherwise, under heavy load, ≥ `auto_flush_interval` can elapse
        // between `flush()` and the commit, the commit trips the time-based
        // auto-flush, and the spawned flush (which `Drop` does not drain)
        // finalizes `n = 5` into L1 — so corrupting the WAL tail no longer
        // reverts it and the reopen observes `n = 5` (a flaky failure).
        let cfg = uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        };
        let db = h.open_with(cfg).await?;
        init_schema_and_seed(&db).await?; // commits n = 0
        // Flush so a snapshot manifest exists (n = 0 reaches L1).
        db.flush().await?;
        // A post-flush commit that will become the WAL tail.
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 5").await?;
        tx.commit().await?;
        drop(db);
    }

    // Simulate a torn write: overwrite the highest-LSN segment with garbage.
    let wal_dir = std::path::PathBuf::from(h.uri()).join("wal");
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&wal_dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    segments.sort();
    let tail = segments.last().expect("at least one WAL segment");
    std::fs::write(tail, b"torn-by-power-loss")?;

    // Reopen succeeds; the corrupt tail's commit (n = 5) is gone, the
    // commit before it (n = 0) survives.
    let db = h.open().await?;
    assert_eq!(
        read_n(&db).await?,
        0,
        "intact segments before the torn tail must replay"
    );
    Ok(())
}

// ── The same seams under a real process abort ────────────────────────────────
//
// The tests above panic mid-commit and then drop the `Uni`, which still runs
// the shutdown flush. These re-run the same four seams under `SIGABRT`, where
// nothing runs: no unwinding, no `Drop`, no final flush. See
// `common/crash_harness.rs` for why that distinction matters and how the child
// process works.
//
// Both are kept. Graceful close is a real path with its own regression history,
// and `crash_during_flush_preserves_committed_unflushed_commit` documents that
// its bug is reachable *only* there.

/// The child half of the abort tests in this file.
///
/// Rebuilds the pre-crash state from scratch at the path the parent chose, arms
/// the seam to abort, and drives the operation into it. Never returns.
///
/// Runs as a no-op when the environment is absent — the CI failpoint lane
/// passes `--run-ignored all`, so it gets executed directly with nothing set.
/// The parent tests are the ones that assert.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal: child process entry point for the abort harness"]
async fn ssi_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();

    // Seed and close cleanly, so the pre-crash state is genuinely durable and
    // the abort below is the only ungraceful event in the run.
    {
        let db = Uni::open(&uri).build().await.unwrap();
        init_schema_and_seed(&db).await.unwrap();
        db.flush().await.unwrap();
        if scenario == "during-flush" {
            // An acknowledged commit that is NOT yet in L1: the thing whose
            // survival across a crash-during-flush is under test.
            let s = db.session();
            let tx = s.tx().await.unwrap();
            tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 5")
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(&uri).build().await.unwrap();

    // The control case for the whole harness: arm a real seam that this
    // operation never evaluates, then run the operation and exit cleanly.
    // `run_child` must reject that. Without it, "the child died of SIGABRT"
    // would not establish *where* it died, and several parent assertions
    // (n == 0, n == 5) would pass just as happily against a child that
    // aborted the instant the seam was armed.
    if scenario == "unreached-seam" {
        crate::crash_harness::abort_at("fork::drop-after-begin");
        set_n(&db, 1).await;
        db.shutdown().await.unwrap();
        return;
    }

    match scenario.as_str() {
        "after-validate" => {
            crate::crash_harness::abort_at("commit::after-validate");
            set_n(&db, 42).await;
        }
        "after-wal-flush" => {
            crate::crash_harness::abort_at("commit::after-wal-flush");
            set_n(&db, 7).await;
        }
        "after-merge" => {
            crate::crash_harness::abort_at("commit::after-merge");
            set_n(&db, 3).await;
        }
        "during-flush" => {
            crate::crash_harness::abort_at("flush::after-rotate-before-lance");
            let _ = db.flush().await;
        }
        other => crate::crash_harness::unknown_scenario("ssi_abort_child", other),
    }
    panic!("the operation returned; the seam for '{scenario}' was never reached");
}

/// Drives a `SET c.n = val` commit that is expected to abort the process.
#[cfg(feature = "failpoints")]
async fn set_n(db: &Uni, val: i64) {
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute(&format!("MATCH (c:C {{id: 'x'}}) SET c.n = {val}"))
        .await
        .unwrap();
    let _ = tx.commit().await;
}

/// Opens the crashed database and returns it, for the abort tests' assertions.
#[cfg(feature = "failpoints")]
async fn reopen_after_abort(path: &std::path::Path) -> Result<Uni> {
    Ok(Uni::open(path.to_string_lossy().as_ref()).build().await?)
}

/// Abort sibling of [`crash_after_validate_recovers_nothing`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_after_validate_recovers_nothing() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "ssi_resilience::ssi_abort_child",
        "after-validate",
        &uri,
    )
    .await;

    let db = reopen_after_abort(&uri).await?;
    assert_eq!(
        read_n(&db).await?,
        0,
        "a crash before the WAL flush must leave no trace"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`crash_after_wal_flush_is_atomic`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_after_wal_flush_is_atomic() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "ssi_resilience::ssi_abort_child",
        "after-wal-flush",
        &uri,
    )
    .await;

    let db = reopen_after_abort(&uri).await?;
    assert_atomic_and_usable(&db, 7).await?;
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`crash_after_merge_is_atomic`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_after_merge_is_atomic() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async("ssi_resilience::ssi_abort_child", "after-merge", &uri)
        .await;

    let db = reopen_after_abort(&uri).await?;
    assert_atomic_and_usable(&db, 3).await?;
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`crash_during_flush_preserves_committed_unflushed_commit`].
///
/// The sibling's own docs note the loss it regresses was reachable only on the
/// graceful-close path — under a real abort the WAL was already durable, so
/// this asserts the acknowledged commit survives.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_during_flush_preserves_committed_unflushed_commit() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async("ssi_resilience::ssi_abort_child", "during-flush", &uri)
        .await;

    let db = reopen_after_abort(&uri).await?;
    assert_eq!(
        read_n(&db).await?,
        5,
        "an acknowledged commit was lost across a crash during flush"
    );
    db.shutdown().await?;
    Ok(())
}
