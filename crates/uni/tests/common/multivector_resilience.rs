// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience: crash / recovery / WAL replay for multi-vector (ColBERT / MaxSim)
//! search — parity with `sparse_resilience.rs` / `dense_resilience.rs`.
//!
//! The payload is a `List<Vector>` token set. Durability is asserted by existence
//! plus exact-MaxSim top-match (the `target` self-matches at 2.0): if the token
//! list were corrupted or lost across recovery, `target` would not rank first.
//! This sidesteps the multi-vector value-representation surface while still pinning
//! that the committed value survives the crash + replay. The recovered rows land in
//! L0 and the multi-vector read path unions + re-scores them, so the query is
//! correct with no index rebuild.
//!
//! Run with `--features failpoints`. Each test owns its failpoint and runs in its
//! own process under nextest, so the global registry does not bleed.

#[cfg(feature = "failpoints")]
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::ssi_support::reopen::DiskHarness;

const DIM: usize = 8;

fn basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

/// Query / `target` tokens `[e0, e1]` — self-MaxSim 2.0 (the unique maximizer).
fn target_tokens() -> Vec<Vec<f32>> {
    vec![basis(0), basis(1)]
}

/// A distinct baseline token set (orthogonal to the query → MaxSim 0).
fn base_tokens() -> Vec<Vec<f32>> {
    vec![basis(4), basis(5)]
}

/// The doomed doc's tokens — orthogonal to [`target_tokens`], so MaxSim against
/// the query is 0 and it can never outrank `target`.
///
/// They must not be the query tokens: `commit::after-wal-flush` is the commit
/// point, so the doomed write is durable and comes back on replay, and reusing
/// `target_tokens()` made the top-1 assertions a scan-order tie at MaxSim 2.0.
fn doomed_tokens() -> Vec<Vec<f32>> {
    vec![basis(6), basis(7)]
}

fn to_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

fn cypher_lit(tokens: &[Vec<f32>]) -> String {
    let toks: Vec<String> = tokens
        .iter()
        .map(|t| {
            let nums: Vec<String> = t.iter().map(|x| format!("{x:?}")).collect();
            format!("[{}]", nums.join(","))
        })
        .collect();
    format!("[{}]", toks.join(","))
}

/// `Doc(title, tokens: List<Vector>)` (no index — `uni.vector.query` runs the
/// exact MaxSim rerank directly over the token column).
async fn define_multi_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;
    Ok(())
}

async fn insert_doc(db: &Uni, title: &str, tokens: &[Vec<f32>]) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, tokens: $toks})")
        .param("t", Value::String(title.to_string()))
        .param("toks", to_value(tokens))
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Number of `Doc` rows titled `title` (0 = absent, 1 = present exactly once).
async fn doc_count(db: &Uni, title: &str) -> Result<usize> {
    Ok(db
        .session()
        .query_with("MATCH (d:Doc {title: $t}) RETURN d.title AS title")
        .param("t", Value::String(title.to_string()))
        .fetch_all()
        .await?
        .rows()
        .len())
}

/// Issue #132 (multi-vector / MUVERA flush path): a persistently STALLED flush
/// stream must NOT wedge the pipeline. Mirrors the sparse variant in
/// `sparse_resilience.rs` — the flush-stream timeout + skip-on-saturation fix is
/// schema-agnostic, but this exercises the multi-vector flush columns named in
/// the issue. Every flush fails (stalls → times out), so all committed docs must
/// stay live in L0/WAL while commits keep succeeding (no runtime park).
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_multivec_flush_stream_recovers_not_wedges() -> Result<()> {
    let h = DiskHarness::new()?;
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
    define_multi_schema(&db).await?;
    fail::cfg("flush::stream-async-stall", "return").unwrap();
    const N: usize = 6;
    let recovered = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for i in 0..N {
            insert_doc(&db, &format!("doc-{i}"), &target_tokens()).await?;
        }
        anyhow::Ok(())
    })
    .await;
    fail::remove("flush::stream-async-stall");
    assert!(
        recovered.is_ok(),
        "issue #132 regression: multi-vector flush pipeline wedged under a persistent stall"
    );
    recovered.expect("bounded above")?;
    for i in 0..N {
        assert_eq!(
            doc_count(&db, &format!("doc-{i}")).await?,
            1,
            "doc-{i} lost while every multi-vector flush was failing"
        );
    }
    Ok(())
}

/// The title of the top-1 MaxSim match to `target_tokens()` via `uni.vector.query`.
async fn top_multi_title(db: &Uni) -> Result<Option<String>> {
    let lit = cypher_lit(&target_tokens());
    let r = db
        .session()
        .query(&format!(
            "CALL uni.vector.query('Doc', 'tokens', {lit}, 1, null, null, {{}}) \
             YIELD node, score RETURN node.title AS title"
        ))
        .await?;
    Ok(r.rows().first().and_then(|row| match row.value("title") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }))
}

/// Drive a multi-vector `CREATE` + commit that is expected to panic at a failpoint.
#[cfg(feature = "failpoints")]
async fn multi_create_that_crashes(db: Arc<Uni>, title: &'static str) {
    let res = tokio::spawn(async move {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute_with("CREATE (:Doc {title: $t, tokens: $toks})")
            .param("t", Value::String(title.to_string()))
            .param("toks", to_value(&doomed_tokens()))
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

/// A committed multi-vector mutation durable in the WAL survives a later mid-commit
/// crash and replays intact — `target` self-matches first after recovery.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multivector_committed_value_survives_crash_recovery() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_multi_schema(&db).await?;
        insert_doc(&db, "base", &base_tokens()).await?;
        db.flush().await?;
        insert_doc(&db, "target", &target_tokens()).await?;
        let db = Arc::new(db);
        fail::cfg("commit::after-wal-flush", "panic").unwrap();
        multi_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-wal-flush");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        doc_count(&db, "target").await?,
        1,
        "committed multi-vector doc lost across crash recovery"
    );
    // The seam IS the commit point, so the doomed write is durable and must come
    // back as well; it simply must not outrank `target`.
    assert_eq!(
        doc_count(&db, "doomed").await?,
        1,
        "a multi-vector write already durable in the WAL at the crash must recover"
    );
    assert_eq!(
        top_multi_title(&db).await?.as_deref(),
        Some("target"),
        "recovered multi-vector tokens corrupted (target no longer self-matches) — no rebuild path"
    );
    Ok(())
}

/// A multi-vector `CREATE` that crashes AFTER validation but BEFORE the WAL append
/// recovers nothing.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multivector_crash_before_wal_recovers_nothing() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_multi_schema(&db).await?;
        insert_doc(&db, "base", &base_tokens()).await?;
        db.flush().await?;
        let db = Arc::new(db);
        fail::cfg("commit::after-validate", "panic").unwrap();
        multi_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-validate");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        doc_count(&db, "doomed").await?,
        0,
        "a multi-vector write that crashed before the WAL flush must leave no trace"
    );
    assert_eq!(
        doc_count(&db, "base").await?,
        1,
        "the durable baseline multi-vector doc must survive"
    );
    Ok(())
}

/// A crash mid-flush loses no committed data: both flushed `base` and
/// committed-but-unflushed `delta` survive reopen, each exactly once.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multivector_crash_during_flush_loses_no_committed_data() -> Result<()> {
    let h = DiskHarness::new()?;
    let delta_tokens = vec![basis(2), basis(3)];
    {
        let db = Arc::new(h.open().await?);
        define_multi_schema(&db).await?;
        insert_doc(&db, "base", &target_tokens()).await?; // exact match for the query
        db.flush().await?;
        insert_doc(&db, "delta", &delta_tokens).await?;
        fail::cfg("flush::after-rotate-before-lance", "panic").unwrap();
        let dbf = db.clone();
        let res = tokio::spawn(async move { dbf.flush().await }).await;
        fail::remove("flush::after-rotate-before-lance");
        assert!(res.is_err(), "flush task should have panicked at the seam");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        doc_count(&db, "base").await?,
        1,
        "flushed multi-vector doc lost across crash-during-flush"
    );
    assert_eq!(
        doc_count(&db, "delta").await?,
        1,
        "committed-but-unflushed multi-vector doc lost across crash-during-flush (lost-commit regression)"
    );
    assert_eq!(
        top_multi_title(&db).await?.as_deref(),
        Some("base"),
        "recovered top multi-vector match wrong after crash-during-flush (no rebuild)"
    );
    Ok(())
}

/// A torn WAL tail must not block reopen: recovery skips the unacknowledged tail
/// commit and replays everything before it.
#[tokio::test]
async fn multivector_corrupt_wal_tail_skipped_on_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let cfg = uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        };
        let db = h.open_with(cfg).await?;
        define_multi_schema(&db).await?;
        insert_doc(&db, "base", &base_tokens()).await?;
        db.flush().await?;
        insert_doc(&db, "tail", &target_tokens()).await?;
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
        doc_count(&db, "base").await?,
        1,
        "the flushed baseline multi-vector doc before the torn tail must replay"
    );
    assert_eq!(
        doc_count(&db, "tail").await?,
        0,
        "the torn-tail multi-vector commit must not resurrect"
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
async fn multivector_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();
    let delta_tokens = vec![basis(2), basis(3)];

    {
        let db = Uni::open(&uri).build().await.unwrap();
        define_multi_schema(&db).await.unwrap();
        match scenario.as_str() {
            "during-flush" => {
                insert_doc(&db, "base", &target_tokens()).await.unwrap();
                db.flush().await.unwrap();
                insert_doc(&db, "delta", &delta_tokens).await.unwrap();
            }
            _ => {
                insert_doc(&db, "base", &base_tokens()).await.unwrap();
                db.flush().await.unwrap();
                if scenario == "after-wal-flush" {
                    insert_doc(&db, "target", &target_tokens()).await.unwrap();
                }
            }
        }
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(&uri).build().await.unwrap();
    match scenario.as_str() {
        "after-wal-flush" => {
            crate::crash_harness::abort_at("commit::after-wal-flush");
            let _ = insert_doc(&db, "doomed", &doomed_tokens()).await;
        }
        "after-validate" => {
            crate::crash_harness::abort_at("commit::after-validate");
            let _ = insert_doc(&db, "doomed", &doomed_tokens()).await;
        }
        "during-flush" => {
            crate::crash_harness::abort_at("flush::after-rotate-before-lance");
            let _ = db.flush().await;
        }
        other => crate::crash_harness::unknown_scenario("multivector_abort_child", other),
    }
    panic!("the operation returned; the seam for '{scenario}' was never reached");
}

/// Abort sibling of [`multivector_committed_value_survives_crash_recovery`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multivector_abort_committed_value_survives_recovery() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "multivector_resilience::multivector_abort_child",
        "after-wal-flush",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        doc_count(&db, "target").await?,
        1,
        "committed multivector doc lost across abort recovery"
    );
    assert_eq!(
        top_multi_title(&db).await?.as_deref(),
        Some("target"),
        "recovered multivector doc not retrievable via the L0-union path"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`multivector_crash_before_wal_recovers_nothing`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multivector_abort_before_wal_recovers_nothing() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "multivector_resilience::multivector_abort_child",
        "after-validate",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        doc_count(&db, "doomed").await?,
        0,
        "a multivector write that aborted before the WAL flush must leave no trace"
    );
    assert_eq!(
        doc_count(&db, "base").await?,
        1,
        "the durable baseline multivector doc must survive"
    );
    db.shutdown().await?;
    Ok(())
}

/// Abort sibling of [`multivector_crash_during_flush_loses_no_committed_data`].
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multivector_abort_during_flush_loses_no_committed_data() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().join("db");
    crate::crash_harness::run_child_async(
        "multivector_resilience::multivector_abort_child",
        "during-flush",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
    assert_eq!(
        doc_count(&db, "base").await?,
        1,
        "flushed multivector doc lost across abort-during-flush"
    );
    assert_eq!(
        doc_count(&db, "delta").await?,
        1,
        "committed-but-unflushed multivector doc lost across abort-during-flush"
    );
    db.shutdown().await?;
    Ok(())
}
