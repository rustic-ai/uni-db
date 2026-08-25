// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience: crash / recovery / WAL replay for the sparse-vector index
//! (issue #95, test set F).
//!
//! These extend `ssi_resilience.rs` with a `SparseVector` payload. The
//! sparse-specific durability claim is that a committed sparse mutation is
//! persisted through the explicit Cypher-value codec (`TAG_SPARSE_VECTOR`), so a
//! crash-then-reopen replays it from the WAL and decodes it **losslessly** — the
//! durability fix that closed the lossy untagged-serde path. The crash-injection
//! tests (gated behind the `failpoints` feature) drive a commit / flush to panic
//! at a precise seam, then reopen and assert atomicity and decode fidelity.
//!
//! WAL replay does not repopulate the sparse postings index, but no rebuild is
//! needed: the recovered rows land in L0 and the `sparse_rerank` read path
//! unions live L0 candidates, so the index query is correct with the postings
//! still cold (see `recovery_index_no_rebuild.rs` for the cross-index regression).
//! Run with `--features failpoints`. Each test owns its failpoint and runs in
//! its own process under nextest, so the global registry does not bleed.

#[cfg(feature = "failpoints")]
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value};

use crate::ssi_support::reopen::DiskHarness;

const VOCAB: usize = 1000;

/// The fixed sparse vector seeded as `target` and reused as the query — `target`
/// is its own exact dot-product maximizer.
fn target_emb() -> Value {
    Value::SparseVector {
        indices: vec![1, 5, 9, 42],
        values: vec![1.0, 2.0, 3.0, 0.5],
    }
}

/// `Doc(title, emb: SparseVector)` + a scored sparse index over `emb`.
async fn define_sparse_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::sparse(VOCAB))
        .apply()
        .await?;
    Ok(())
}

/// Insert one `Doc` with the given title and sparse embedding (own transaction).
async fn insert_doc(db: &Uni, title: &str, emb: Value) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, emb: $emb})")
        .param("t", Value::String(title.to_string()))
        .param("emb", emb)
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Read back the `emb` of the doc titled `title`, if it survived recovery.
async fn read_emb(db: &Uni, title: &str) -> Result<Option<Value>> {
    let r = db
        .session()
        .query_with("MATCH (d:Doc {title: $t}) RETURN d.emb AS emb")
        .param("t", Value::String(title.to_string()))
        .fetch_all()
        .await?;
    Ok(r.rows().first().and_then(|row| row.value("emb").cloned()))
}

/// The title of the top-1 sparse match to `target_emb()` via `uni.sparse.query`.
#[cfg(feature = "failpoints")]
async fn top_sparse_title(db: &Uni) -> Result<Option<String>> {
    let r = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, 1, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", target_emb())
        .fetch_all()
        .await?;
    Ok(r.rows().first().and_then(|row| match row.value("title") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }))
}

/// Drive a sparse `CREATE` + commit that is expected to panic at an armed
/// failpoint (the spawned task aborts, so the join returns `Err`).
#[cfg(feature = "failpoints")]
async fn sparse_create_that_crashes(db: Arc<Uni>, title: &'static str) {
    let res = tokio::spawn(async move {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute_with("CREATE (:Doc {title: $t, emb: $emb})")
            .param("t", Value::String(title.to_string()))
            .param("emb", target_emb())
            .run()
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

/// A sparse mutation made durable in the WAL (committed but not flushed to L1)
/// survives a later mid-commit crash and decodes losslessly through the CV codec
/// on replay. This is the sparse analogue of `crash_after_wal_flush_is_atomic`,
/// targeting the value-fidelity guarantee rather than a scalar count.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_committed_value_survives_crash_recovery() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_sparse_schema(&db).await?;
        // A flushed baseline gives recovery a snapshot manifest to replay onto.
        insert_doc(
            &db,
            "base",
            Value::SparseVector {
                indices: vec![2, 3],
                values: vec![0.7, 0.7],
            },
        )
        .await?;
        db.flush().await?;
        // `target` is committed (durable in the WAL) but NOT flushed, so recovery
        // must replay it from the WAL.
        insert_doc(&db, "target", target_emb()).await?;
        // A later transaction crashes after its WAL flush; the already-durable
        // `target` commit must survive the crash + reopen intact.
        let db = Arc::new(db);
        fail::cfg("commit::after-wal-flush", "panic").unwrap();
        sparse_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-wal-flush");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "target").await?,
        Some(target_emb()),
        "committed sparse value corrupted or lost across crash recovery"
    );
    // Usable post-recovery WITHOUT a rebuild: the recovered doc is in L0 and the
    // sparse read path unions it, so it is the top sparse match with the index
    // still cold.
    assert_eq!(
        top_sparse_title(&db).await?.as_deref(),
        Some("target"),
        "recovered sparse doc not retrievable via the L0-union path (no rebuild)"
    );
    Ok(())
}

/// A sparse `CREATE` that crashes AFTER validation but BEFORE the WAL append
/// recovers nothing: the mutation never became durable, so no half-written
/// `SparseVector` resurrects on replay.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_crash_before_wal_recovers_nothing() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_sparse_schema(&db).await?;
        insert_doc(
            &db,
            "base",
            Value::SparseVector {
                indices: vec![2, 3],
                values: vec![0.7, 0.7],
            },
        )
        .await?;
        db.flush().await?;
        let db = Arc::new(db);
        fail::cfg("commit::after-validate", "panic").unwrap();
        sparse_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-validate");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "doomed").await?,
        None,
        "a sparse write that crashed before the WAL flush must leave no trace"
    );
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(Value::SparseVector {
            indices: vec![2, 3],
            values: vec![0.7, 0.7],
        }),
        "the durable baseline sparse doc must survive"
    );
    Ok(())
}

/// A crash mid-flush (panic after the L0 rotation, before the Lance write) loses
/// no committed data: both the flushed `base` and the committed-but-unflushed
/// `delta` (which sat in the rotated buffer the crash abandoned) survive reopen,
/// each exactly once, and the index rebuilds cleanly. Targets the
/// `flush::after-rotate-before-lance` seam.
///
/// Regression for the lost-commit durability bug: a graceful close after a
/// failed flush truncated the delta's WAL segment and published a
/// `wal_high_water_mark` past it (WAL truncation + checkpoint keyed off the
/// pending buffer's HIGH watermark instead of its START watermark), so an
/// acknowledged commit vanished on reopen. See `l0_manager::min_pending_wal_lsn_start`.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_crash_during_flush_loses_no_committed_data() -> Result<()> {
    let h = DiskHarness::new()?;
    let delta_emb = Value::SparseVector {
        indices: vec![1, 5],
        values: vec![0.4, 0.4],
    };
    {
        let db = Arc::new(h.open().await?);
        define_sparse_schema(&db).await?;
        // `base` is an exact match for the query, flushed durably before the crash.
        insert_doc(&db, "base", target_emb()).await?;
        db.flush().await?;
        // `delta` is committed (acknowledged) but unflushed — it is the doc the
        // crashing flush is mid-rotating when it panics.
        insert_doc(&db, "delta", delta_emb.clone()).await?;
        // Flush panics at the seam: L0 is rotated to pending but never written to
        // Lance. The rotated buffer's WAL data must NOT be truncated nor
        // checkpointed past on the subsequent graceful close.
        fail::cfg("flush::after-rotate-before-lance", "panic").unwrap();
        let dbf = db.clone();
        let res = tokio::spawn(async move { dbf.flush().await }).await;
        fail::remove("flush::after-rotate-before-lance");
        assert!(res.is_err(), "flush task should have panicked at the seam");
        drop(db);
    }
    let db = h.open().await?;
    // Both the flushed base and the committed-but-unflushed delta survive.
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(target_emb()),
        "flushed sparse doc lost across crash-during-flush"
    );
    assert_eq!(
        read_emb(&db, "delta").await?,
        Some(delta_emb),
        "committed-but-unflushed sparse doc lost across crash-during-flush (lost-commit regression)"
    );
    // Neither is double-applied by the partial-flush + WAL-replay interplay.
    for title in ["base", "delta"] {
        let n = db
            .session()
            .query_with("MATCH (d:Doc {title: $t}) RETURN d.title AS title")
            .param("t", Value::String(title.to_string()))
            .fetch_all()
            .await?
            .rows()
            .len();
        assert_eq!(
            n, 1,
            "{title} double-applied across crash-during-flush recovery"
        );
    }
    // The sparse query is correct over the recovered rows with NO rebuild: both
    // base (flushed, indexed in L1) and delta (recovered into L0) are unioned by
    // the read path.
    assert_eq!(
        top_sparse_title(&db).await?.as_deref(),
        Some("base"),
        "recovered top sparse match wrong after crash-during-flush (no rebuild)"
    );
    Ok(())
}

/// Issue #132: a STALLED async flush stream (modelling a lost-wakeup in the
/// sparse Lance read-modify-write) must NOT permanently wedge the flush
/// pipeline. Pre-fix, the stalled stream never submits its rotate-seq, so the
/// finalizer wedges, back-pressure permits saturate, and the next commits fall
/// to the inline flush path and block forever on `flush_lock`. With the
/// flush-stream timeout the stall becomes a data-safe *failure*: the finalizer
/// advances, the permit releases, and later commits + an explicit flush all
/// complete — and no committed data is lost (the stalled flush's rows stay in
/// L0/WAL and remain queryable).
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_sparse_flush_stream_recovers_not_wedges() -> Result<()> {
    let h = DiskHarness::new()?;
    // Flush on every commit via the ASYNC path (the one the timeout guards);
    // a 500ms stream timeout converts the stall quickly; no time-based trigger.
    let cfg = uni_db::UniConfig {
        async_flush_enabled: true,
        flush_stream_timeout: std::time::Duration::from_millis(500),
        auto_flush_threshold: 1,
        auto_flush_min_mutations: 1,
        auto_flush_interval: None,
        max_pending_flushes: 2,
        ..Default::default()
    };
    let db = h.open_with(cfg).await?;
    define_sparse_schema(&db).await?;

    // Arm a PERSISTENT async stall: EVERY flush stream sleeps far past the
    // timeout. Each async flush therefore times out into a data-safe failure,
    // frees its permit, and a later commit retries — but no flush ever
    // completes, so all committed data must remain live in L0/WAL. Armed AFTER
    // schema setup so the stalls hit async (timeout-guarded) flushes.
    fail::cfg("flush::stream-async-stall", "return").unwrap();

    // Commit enough docs to SATURATE the flush pipeline (both permits held by
    // stalled flushes). Pre-fix, the (max_pending+1)th commit falls to the
    // blocking inline flush path, hits the same stall with no timeout, and hangs
    // holding `flush_lock` → the whole runtime parks. Post-fix, a saturated
    // async pipeline SKIPs the flush (retry later) so every commit completes,
    // and stalled async flushes are bounded by `flush_stream_timeout`. Bound the
    // wall clock so a regression fails loudly instead of hanging CI.
    const N: usize = 6;
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for i in 0..N {
            insert_doc(&db, &format!("doc-{i}"), target_emb()).await?;
        }
        anyhow::Ok(())
    })
    .await;
    fail::remove("flush::stream-async-stall");
    assert!(
        recovered.is_ok(),
        "issue #132 regression: flush pipeline wedged — a commit hung after the pipeline stalled"
    );
    recovered.expect("bounded above")?;

    // Data-safety: with EVERY flush failing, all committed docs must still be
    // queryable from the L0/WAL union (finalize_failure retains them).
    for i in 0..N {
        assert_eq!(
            read_emb(&db, &format!("doc-{i}")).await?,
            Some(target_emb()),
            "doc-{i} lost while every flush was failing (finalize_failure must retain L0/WAL)"
        );
    }
    Ok(())
}

/// A torn (corrupt) WAL segment at the TAIL must not block reopen: the torn
/// segment belongs to an unacknowledged sparse commit, so recovery skips it and
/// replays everything before it — the flushed baseline sparse doc survives.
/// Mirrors `ssi_resilience::corrupt_wal_tail_does_not_block_reopen` with a
/// sparse payload (no failpoints required).
#[tokio::test]
async fn sparse_corrupt_wal_tail_skipped_on_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    let base_emb = Value::SparseVector {
        indices: vec![2, 3],
        values: vec![0.7, 0.7],
    };
    {
        // Disable time-based auto-flush so the post-flush tail commit cannot be
        // promoted to L1 by a background flush (which would survive tail
        // corruption and make the assertion flaky).
        let cfg = uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        };
        let db = h.open_with(cfg).await?;
        define_sparse_schema(&db).await?;
        insert_doc(&db, "base", base_emb.clone()).await?;
        db.flush().await?;
        // A post-flush sparse commit that becomes the WAL tail.
        insert_doc(&db, "tail", target_emb()).await?;
        drop(db);
    }

    // Simulate a torn write: overwrite the highest-LSN WAL segment with garbage.
    let wal_dir = std::path::PathBuf::from(h.uri()).join("wal");
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&wal_dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    segments.sort();
    let tail = segments.last().expect("at least one WAL segment");
    std::fs::write(tail, b"torn-by-power-loss")?;

    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(base_emb),
        "the flushed baseline sparse doc before the torn tail must replay"
    );
    assert_eq!(
        read_emb(&db, "tail").await?,
        None,
        "the torn-tail sparse commit must not resurrect"
    );
    Ok(())
}

// ── The same seams under a real process abort ────────────────────────────────
//
// Siblings of the three crash tests above, run under `SIGABRT` in a child
// process instead of a panic followed by a `Drop` that still flushes. See
// `common/crash_harness.rs`.

/// The baseline sparse vector used where the panic-path tests inline it.
#[cfg(feature = "failpoints")]
fn base_emb() -> Value {
    Value::SparseVector {
        indices: vec![2, 3],
        values: vec![0.7, 0.7],
    }
}

/// The committed-but-unflushed sparse vector for the during-flush scenario.
#[cfg(feature = "failpoints")]
fn delta_emb() -> Value {
    Value::SparseVector {
        indices: vec![1, 5],
        values: vec![0.4, 0.4],
    }
}

/// The child half of the abort tests in this file. Never returns.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal: child process entry point for the abort harness"]
async fn sparse_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();

    {
        let db = Uni::open(&uri).build().await.unwrap();
        define_sparse_schema(&db).await.unwrap();
        match scenario.as_str() {
            "during-flush" => {
                insert_doc(&db, "base", target_emb()).await.unwrap();
                db.flush().await.unwrap();
                insert_doc(&db, "delta", delta_emb()).await.unwrap();
            }
            _ => {
                insert_doc(&db, "base", base_emb()).await.unwrap();
                db.flush().await.unwrap();
                if scenario == "after-wal-flush" {
                    insert_doc(&db, "target", target_emb()).await.unwrap();
                }
            }
        }
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(&uri).build().await.unwrap();
    match scenario.as_str() {
        "after-wal-flush" => {
            crate::crash_harness::abort_at("commit::after-wal-flush");
            let _ = insert_doc(&db, "doomed", target_emb()).await;
        }
        "after-validate" => {
            crate::crash_harness::abort_at("commit::after-validate");
            let _ = insert_doc(&db, "doomed", target_emb()).await;
        }
        "during-flush" => {
            crate::crash_harness::abort_at("flush::after-rotate-before-lance");
            let _ = db.flush().await;
        }
        other => crate::crash_harness::unknown_scenario("sparse_abort_child", other),
    }
    panic!("the operation returned; the seam for '{scenario}' was never reached");
}

/// Abort sibling of [`sparse_committed_value_survives_crash_recovery`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_abort_committed_value_survives_recovery() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "sparse_resilience::sparse_abort_child",
        "after-wal-flush",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        read_emb(&db, "target").await?,
        Some(target_emb()),
        "committed sparse value corrupted or lost across abort recovery"
    );
    assert_eq!(
        top_sparse_title(&db).await?.as_deref(),
        Some("target"),
        "recovered sparse doc not retrievable via the L0-union path (no rebuild)"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`sparse_crash_before_wal_recovers_nothing`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_abort_before_wal_recovers_nothing() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "sparse_resilience::sparse_abort_child",
        "after-validate",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        read_emb(&db, "doomed").await?,
        None,
        "a sparse write that aborted before the WAL flush must leave no trace"
    );
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(base_emb()),
        "the durable baseline sparse doc must survive"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`sparse_crash_during_flush_loses_no_committed_data`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_abort_during_flush_loses_no_committed_data() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "sparse_resilience::sparse_abort_child",
        "during-flush",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(target_emb()),
        "flushed sparse doc lost across abort-during-flush"
    );
    assert_eq!(
        read_emb(&db, "delta").await?,
        Some(delta_emb()),
        "committed-but-unflushed sparse doc lost across abort-during-flush"
    );
    db.shutdown().await?;
    Ok(())
}
