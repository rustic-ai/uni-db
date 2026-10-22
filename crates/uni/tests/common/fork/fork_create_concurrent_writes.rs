// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Review M1: fork-point capture must be atomic w.r.t. concurrent parent
//! writes.
//!
//! `create_fork_2pc` flushes the parent and then captures the allocator
//! HWM and per-dataset Lance versions. Before the fix these three reads
//! were not atomic: a concurrent parent commit that crossed the
//! auto-flush threshold in the window let the fork (a) branch off a Lance
//! version containing post-fork-point parent rows and (b) bootstrap its
//! allocator to a stale HWM, so the fork's first writes re-used VIDs that
//! already existed in `base_paths` and Lance read-merge shadowed one side
//! or the other.
//!
//! The fix captures HWM + per-dataset versions under one held
//! `flush_lock` (`Writer::flush_and_capture_fork_point`).
//!
//! ## Detector
//! We avoid pinning the exact (inherently racy) fork point. Instead we
//! rely on a collision-invariant that holds regardless of which side wins
//! a VID merge: **each fork-local write must increase the fork's visible
//! row count by exactly one.** A stale-HWM collision makes a fork write
//! land on a VID already present in `base_paths`, so the net count either
//! does not move (fork write shadowed) or replaces a base row (base row
//! shadowed) — both break the `+1` invariant. We also assert each marker
//! is individually queryable with its exact `seq`.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use uni_db::{DataType, Uni, UniConfig};

/// Rows a fork writes per iteration; `>1` so a single collision is caught.
const FORK_WRITES_PER_ITER: usize = 5;
/// Forks created per run; enough to shake the flush/capture window.
const FORK_ITERS: usize = 15;

async fn run_race(uri: &str, async_flush: bool) {
    // A low auto-flush threshold turns ordinary parent commits into
    // frequent flushes, which is what opens the fork-point capture
    // window. The generous commit timeout keeps the racing writer from
    // failing on the brief `flush_lock` hold rather than on a real bug.
    let config = UniConfig {
        auto_flush_threshold: 4,
        async_flush_enabled: async_flush,
        commit_timeout: Duration::from_secs(120),
        ..Default::default()
    };

    let db = Uni::open(uri).config(config).build().await.unwrap();
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .property("seq", DataType::Int64)
        .apply()
        .await
        .unwrap();

    // Seed a base row so `vertices_Item` exists on disk at fork-point.
    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:Item {kind: 'seed', seq: -1})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    // Background parent writer: commit primary rows as fast as possible so
    // flushes race fork creation. `next_seq` is shared so seqs stay unique
    // and monotonic across the run.
    let stop = Arc::new(AtomicBool::new(false));
    let next_seq = Arc::new(AtomicI64::new(0));
    let writer_session = db.session();
    let writer_stop = stop.clone();
    let writer_seq = next_seq.clone();
    let writer = tokio::spawn(async move {
        while !writer_stop.load(Ordering::Relaxed) {
            let s = writer_seq.fetch_add(1, Ordering::Relaxed);
            let tx = writer_session.tx().await.unwrap();
            tx.execute(&format!("CREATE (:Item {{kind: 'p', seq: {s}}})"))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
    });

    for i in 0..FORK_ITERS {
        let forked = session.fork(format!("m1_race_{i}")).await.unwrap();

        // Count the fork's view immediately, then write our own marker
        // rows and re-count. The delta must be exactly the number written.
        let before = forked
            .query("MATCH (n:Item) RETURN n")
            .await
            .unwrap()
            .rows()
            .len();

        let ftx = forked.tx().await.unwrap();
        for j in 0..FORK_WRITES_PER_ITER {
            // A marker seq well outside the parent's range so it can never
            // be confused with a leaked parent row.
            let mseq = 1_000_000 + (i * FORK_WRITES_PER_ITER + j) as i64;
            ftx.execute(&format!(
                "CREATE (:Item {{kind: 'fmark_{i}_{j}', seq: {mseq}}})"
            ))
            .await
            .unwrap();
        }
        ftx.commit().await.unwrap();

        let after = forked
            .query("MATCH (n:Item) RETURN n")
            .await
            .unwrap()
            .rows()
            .len();

        assert_eq!(
            after,
            before + FORK_WRITES_PER_ITER,
            "fork m1_race_{i} (async_flush={async_flush}): writing {FORK_WRITES_PER_ITER} \
             rows changed the visible count from {before} to {after}; a VID collision with \
             post-fork-point parent rows shadowed a write (M1 fork-point capture race)"
        );

        // Each marker must be present exactly once with its exact seq.
        // Matching on both `kind` and `seq` asserts the write persisted
        // intact and was not replaced by a colliding base row.
        for j in 0..FORK_WRITES_PER_ITER {
            let mseq = 1_000_000 + (i * FORK_WRITES_PER_ITER + j) as i64;
            let rows = forked
                .query(&format!(
                    "MATCH (n:Item {{kind: 'fmark_{i}_{j}', seq: {mseq}}}) RETURN n"
                ))
                .await
                .unwrap();
            assert_eq!(
                rows.rows().len(),
                1,
                "fork marker fmark_{i}_{j} (async_flush={async_flush}) not visible exactly \
                 once with seq={mseq}; fork write was shadowed by a base row (M1)"
            );
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    db.shutdown().await.unwrap();
}

/// **Load sensitivity, root-caused:** with `auto_flush_threshold: 4` the
/// writer loop flushes every few commits, and under machine load the
/// background compaction (10 s tick) falls behind, so the flush-generation
/// count reaches the write throttle's soft band (16..32 runs). The soft
/// throttle used to be one uninterruptible sleep of `10 ms × 2^(runs − 16)`:
/// 40.96 s at 28 runs, 81.92 s at 29. That outlived the 30 s statement
/// timeout, so the background `CREATE` failed with `Timeout { 30000 }` even
/// after compaction had reset the count. Run as 16 parallel copies it failed
/// in about half of them. Every failing run had taken that sleep and no
/// passing run had throttled.
///
/// The soft band now re-checks the count every 100 ms and caps its wait at
/// 1.6 s (`Writer::check_write_pressure`; pinned by
/// `storage::write_backpressure_test` here and the `soft_throttle_*` tests in
/// uni-store). A red run of this test is therefore no longer explained by
/// load alone: look for a write stalled at the hard limit (32 runs, logged
/// as "Write stalled"), which still waits for compaction without a time
/// bound.
///
/// The repository asks for this note at the test site rather than in a tracker,
/// because the next person to see it red will be looking here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_point_atomic_under_concurrent_parent_writes_async_flush() {
    let dir = tempfile::tempdir().unwrap();
    run_race(dir.path().to_str().unwrap(), true).await;
}

/// Same load-sensitivity note as the async-flush twin above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_point_atomic_under_concurrent_parent_writes_sync_flush() {
    let dir = tempfile::tempdir().unwrap();
    run_race(dir.path().to_str().unwrap(), false).await;
}
