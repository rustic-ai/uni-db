// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience: crash / recovery / WAL replay for the dense-vector (KNN) index —
//! parity with `sparse_resilience.rs`.
//!
//! The dense-specific durability claim is that a committed dense mutation is
//! persisted through the explicit Cypher-value codec and replays from the WAL
//! losslessly as a typed `Value::Vector`. The crash-injection tests (gated behind
//! the `failpoints` feature) drive a commit / flush to panic at a precise seam,
//! then reopen and assert atomicity + decode fidelity. WAL replay does not
//! repopulate the flushed Lance dataset, but no rebuild is needed: the recovered
//! rows land in L0 and the dense read path unions live L0 candidates (the fix in
//! `cb00e8b34`), so the query is correct with the dataset still cold.
//!
//! Run with `--features failpoints`. Each test owns its failpoint and runs in its
//! own process under nextest, so the global registry does not bleed.

#[cfg(feature = "failpoints")]
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

use crate::ssi_support::reopen::DiskHarness;

const DIM: usize = 8;

/// The fixed dense vector seeded as `target` and reused as the query — `target`
/// is its own exact cosine maximizer (score 1.0).
fn target_emb() -> Value {
    Value::Vector(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8])
}

/// A distinct baseline vector (not the query).
fn base_emb() -> Value {
    Value::Vector(vec![-0.8, 0.7, -0.6, 0.5, -0.4, 0.3, -0.2, 0.1])
}

/// The doomed doc's vector — anti-parallel to [`target_emb`], so its cosine
/// similarity to the query is -1 and it can never outrank `target`.
///
/// It must not be the query vector. `commit::after-wal-flush` is the commit
/// point, so the doomed write is durable and *does* come back on replay. Giving
/// it `target_emb()` left two docs at distance 0 and made the top-1 assertions
/// below a tie broken by replay and scan order.
fn doomed_emb() -> Value {
    Value::Vector(vec![-0.1, -0.2, -0.3, -0.4, -0.5, -0.6, -0.7, -0.8])
}

/// `Doc(title, emb: Vector)` + an exact `Flat`/Cosine index over `emb`.
async fn define_dense_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;
    Ok(())
}

/// Insert one `Doc` with the given title and dense embedding (own transaction).
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

/// The title of the top-1 dense match to `target_emb()` via `uni.vector.query`.
#[cfg(feature = "failpoints")]
async fn top_dense_title(db: &Uni) -> Result<Option<String>> {
    let r = db
        .session()
        .query_with(
            "CALL uni.vector.query('Doc', 'emb', $q, 1, null, null, {}) \
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

/// Drive a dense `CREATE` + commit that is expected to panic at an armed failpoint.
#[cfg(feature = "failpoints")]
async fn dense_create_that_crashes(db: Arc<Uni>, title: &'static str) {
    let res = tokio::spawn(async move {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute_with("CREATE (:Doc {title: $t, emb: $emb})")
            .param("t", Value::String(title.to_string()))
            .param("emb", doomed_emb())
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

/// A dense mutation made durable in the WAL (committed but not flushed) survives a
/// later mid-commit crash and decodes losslessly through the CV codec on replay.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_committed_value_survives_crash_recovery() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_dense_schema(&db).await?;
        insert_doc(&db, "base", base_emb()).await?;
        db.flush().await?;
        insert_doc(&db, "target", target_emb()).await?;
        let db = Arc::new(db);
        fail::cfg("commit::after-wal-flush", "panic").unwrap();
        dense_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-wal-flush");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "target").await?,
        Some(target_emb()),
        "committed dense value corrupted or lost across crash recovery"
    );
    // The seam IS the commit point, so the doomed write is durable and must come
    // back as well. Nothing asserted this before, which is how it went unnoticed
    // that the doomed doc joins `target` in the ranking below.
    assert_eq!(
        read_emb(&db, "doomed").await?,
        Some(doomed_emb()),
        "a dense write already durable in the WAL at the crash must recover"
    );
    // Usable post-recovery WITHOUT a rebuild: the recovered doc is in L0 and the
    // dense read path unions it, so it is the top match with the dataset cold.
    assert_eq!(
        top_dense_title(&db).await?.as_deref(),
        Some("target"),
        "recovered dense doc not retrievable via the L0-union path (no rebuild)"
    );
    Ok(())
}

/// A dense `CREATE` that crashes AFTER validation but BEFORE the WAL append
/// recovers nothing: the mutation never became durable.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_crash_before_wal_recovers_nothing() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_dense_schema(&db).await?;
        insert_doc(&db, "base", base_emb()).await?;
        db.flush().await?;
        let db = Arc::new(db);
        fail::cfg("commit::after-validate", "panic").unwrap();
        dense_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-validate");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "doomed").await?,
        None,
        "a dense write that crashed before the WAL flush must leave no trace"
    );
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(base_emb()),
        "the durable baseline dense doc must survive"
    );
    Ok(())
}

/// A crash mid-flush (panic after L0 rotation, before the Lance write) loses no
/// committed data: both the flushed `base` and the committed-but-unflushed `delta`
/// survive reopen, each exactly once.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_crash_during_flush_loses_no_committed_data() -> Result<()> {
    let h = DiskHarness::new()?;
    let delta_emb = Value::Vector(vec![0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]);
    {
        let db = Arc::new(h.open().await?);
        define_dense_schema(&db).await?;
        insert_doc(&db, "base", target_emb()).await?;
        db.flush().await?;
        insert_doc(&db, "delta", delta_emb.clone()).await?;
        fail::cfg("flush::after-rotate-before-lance", "panic").unwrap();
        let dbf = db.clone();
        let res = tokio::spawn(async move { dbf.flush().await }).await;
        fail::remove("flush::after-rotate-before-lance");
        assert!(res.is_err(), "flush task should have panicked at the seam");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(target_emb()),
        "flushed dense doc lost across crash-during-flush"
    );
    assert_eq!(
        read_emb(&db, "delta").await?,
        Some(delta_emb),
        "committed-but-unflushed dense doc lost across crash-during-flush (lost-commit regression)"
    );
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
    assert_eq!(
        top_dense_title(&db).await?.as_deref(),
        Some("base"),
        "recovered top dense match wrong after crash-during-flush (no rebuild)"
    );
    Ok(())
}

/// A torn (corrupt) WAL segment at the TAIL must not block reopen: recovery skips
/// the unacknowledged tail commit and replays everything before it.
#[tokio::test]
async fn dense_corrupt_wal_tail_skipped_on_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let cfg = uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        };
        let db = h.open_with(cfg).await?;
        define_dense_schema(&db).await?;
        insert_doc(&db, "base", base_emb()).await?;
        db.flush().await?;
        insert_doc(&db, "tail", target_emb()).await?;
        drop(db);
    }

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
        Some(base_emb()),
        "the flushed baseline dense doc before the torn tail must replay"
    );
    assert_eq!(
        read_emb(&db, "tail").await?,
        None,
        "the torn-tail dense commit must not resurrect"
    );
    Ok(())
}

// ── The same seams under a real process abort ────────────────────────────────
//
// Siblings of the three crash tests above, run under `SIGABRT` in a child
// process instead of a panic followed by a `Drop` that still flushes. See
// `common/crash_harness.rs`.

/// The child half of the abort tests in this file. Never returns.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal: child process entry point for the abort harness"]
async fn dense_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();
    let delta_emb = Value::Vector(vec![0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]);

    {
        let db = Uni::open(&uri).build().await.unwrap();
        define_dense_schema(&db).await.unwrap();
        match scenario.as_str() {
            "during-flush" => {
                insert_doc(&db, "base", target_emb()).await.unwrap();
                db.flush().await.unwrap();
                insert_doc(&db, "delta", delta_emb).await.unwrap();
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
            let _ = insert_doc(&db, "doomed", doomed_emb()).await;
        }
        "after-validate" => {
            crate::crash_harness::abort_at("commit::after-validate");
            let _ = insert_doc(&db, "doomed", doomed_emb()).await;
        }
        "during-flush" => {
            crate::crash_harness::abort_at("flush::after-rotate-before-lance");
            let _ = db.flush().await;
        }
        other => crate::crash_harness::unknown_scenario("dense_abort_child", other),
    }
    panic!("the operation returned; the seam for '{scenario}' was never reached");
}

/// Abort sibling of [`dense_committed_value_survives_crash_recovery`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_abort_committed_value_survives_recovery() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "dense_resilience::dense_abort_child",
        "after-wal-flush",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        read_emb(&db, "target").await?,
        Some(target_emb()),
        "committed dense value corrupted or lost across abort recovery"
    );
    assert_eq!(
        top_dense_title(&db).await?.as_deref(),
        Some("target"),
        "recovered dense doc not retrievable via the L0-union path (no rebuild)"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`dense_crash_before_wal_recovers_nothing`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_abort_before_wal_recovers_nothing() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "dense_resilience::dense_abort_child",
        "after-validate",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        read_emb(&db, "doomed").await?,
        None,
        "a dense write that aborted before the WAL flush must leave no trace"
    );
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(base_emb()),
        "the durable baseline dense doc must survive"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`dense_crash_during_flush_loses_no_committed_data`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_abort_during_flush_loses_no_committed_data() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    let delta_emb = Value::Vector(vec![0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]);
    crate::crash_harness::run_child_async(
        "dense_resilience::dense_abort_child",
        "during-flush",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(target_emb()),
        "flushed dense doc lost across abort-during-flush"
    );
    assert_eq!(
        read_emb(&db, "delta").await?,
        Some(delta_emb),
        "committed-but-unflushed dense doc lost across abort-during-flush"
    );
    for title in ["base", "delta"] {
        let n = db
            .session()
            .query_with("MATCH (d:Doc {title: $t}) RETURN d.title AS title")
            .param("t", Value::String(title.to_string()))
            .fetch_all()
            .await?
            .rows()
            .len();
        assert_eq!(n, 1, "{title} double-applied across abort recovery");
    }
    db.shutdown().await?;
    Ok(())
}
