// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Durability + primary-isolation tests for a fork that has WRITTEN and FLUSHED
//! (production-readiness review C1, M2, H5).
//!
//! These exercise the failure modes that the happy-path soak (`fork_writes_soak.rs`)
//! cannot catch because it always flushes the *primary* last (healing any
//! regression) and asserts only row counts, never version-HWM monotonicity.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

#[cfg(feature = "failpoints")]
use std::sync::Arc;

/// Read the version high-water-mark published in the *global* `catalog/latest`
/// pointer (the primary's snapshot), straight off the local filesystem. A fork
/// flush must never change this (review C1).
fn global_latest_version_hwm(root: &std::path::Path) -> Result<u64> {
    // The snapshot catalog is rooted under `{db}/storage/catalog/`.
    let catalog = root.join("storage").join("catalog");
    let latest = std::fs::read_to_string(catalog.join("latest"))?;
    let id = latest.trim();
    let manifest = std::fs::read_to_string(catalog.join("manifests").join(format!("{id}.json")))?;
    let v: serde_json::Value = serde_json::from_str(&manifest)?;
    Ok(v["version_high_water_mark"].as_u64().unwrap_or(u64::MAX))
}

/// Synchronous-flush config so flush()/drop are deterministic for assertions.
fn sync_flush_config() -> UniConfig {
    UniConfig {
        async_flush_enabled: false,
        ..Default::default()
    }
}

/// C1: a forked session's flush must NOT publish its (small) version HWM into
/// the global `catalog/latest`, which the primary reads on reopen. Before the
/// fix the fork shared the primary's `SnapshotManager`, so the fork flush
/// overwrote `catalog/latest` with the fork's manifest → on reopen the primary's
/// version counter regressed → silent lost updates.
#[tokio::test]
async fn fork_flush_does_not_poison_primary_catalog_latest() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let root = dir.path().to_path_buf();
    let uri = root.to_str().unwrap().to_string();

    // 1. Primary: create + many updates to push the version HWM high, then flush.
    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .property("v", DataType::Int)
            .apply()
            .await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("CREATE (:Person {name: 'p', v: 0})").await?;
        tx.commit().await?;
        for i in 1..=10 {
            let tx = s.tx().await?;
            tx.execute(&format!("MATCH (n:Person {{name: 'p'}}) SET n.v = {i}"))
                .await?;
            tx.commit().await?;
        }
        db.flush().await?;
        db.shutdown().await?;
    }

    let primary_hwm = global_latest_version_hwm(&root)?;
    assert!(
        primary_hwm > 0,
        "primary must publish a non-zero version HWM"
    );

    // 2. Reopen, fork, write on the fork, flush the FORK as the LAST writer
    //    (no healing primary flush), drop the fork.
    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        let s = db.session();
        let fork = s.fork("scn").await?;
        // Fork creation legitimately flushes the PARENT (the #97 flush-before-branch),
        // which may bump the global HWM. Capture it here; the fork's OWN flush below
        // must not change it.
        let hwm_after_fork_create = global_latest_version_hwm(&root)?;
        assert!(
            hwm_after_fork_create >= primary_hwm,
            "fork-create parent flush must not regress the global HWM"
        );

        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'fork-only', v: 99})")
            .await?;
        tx.commit().await?;
        fork.flush().await?; // the fork is the last thing to flush
        drop(fork);
        db.drop_fork("scn").await?;

        // The DIRECT C1 assertion: the fork's flush must not touch the global
        // pointer the primary reads on reopen.
        assert_eq!(
            global_latest_version_hwm(&root)?,
            hwm_after_fork_create,
            "fork flush must not change the global catalog/latest version HWM (review C1)"
        );

        db.shutdown().await?;
    }

    // 3. Reopen the primary and prove no corruption: all rows visible and an
    //    update to an existing row is visible (not shadowed by a regressed
    //    version), and the fork-only row did not leak into the primary.
    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (n:Person {name: 'p'}) SET n.v = 12345")
            .await?;
        tx.commit().await?;
        let v = s
            .query("MATCH (n:Person {name: 'p'}) RETURN n.v AS v")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("v").ok());
        assert_eq!(
            v,
            Some(12345),
            "post-reopen update must be visible (version counter must not have regressed)"
        );
        let leaked = s
            .query("MATCH (n:Person {name: 'fork-only'}) RETURN count(n) AS c")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(-1);
        assert_eq!(leaked, 0, "fork-only row must not leak into the primary");
        db.shutdown().await?;
    }

    Ok(())
}

/// M2/H5 wiring: a fork's committed writes — some flushed, some still in the
/// fork WAL — survive a full DB close+reopen exactly once. Exercises the
/// per-fork snapshot load + WAL replay path introduced for M2 (fork reopen now
/// replays from the fork's own `wal_high_water_mark`, not unconditionally 0).
#[tokio::test]
async fn fork_committed_writes_survive_db_reopen_exactly_once() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        db.schema()
            .label("N")
            .property("i", DataType::Int)
            .apply()
            .await?;
        let s = db.session();
        let fork = s.fork("scn").await?;
        for i in 0..5 {
            let tx = fork.tx().await?;
            tx.execute(&format!("CREATE (:N {{i: {i}}})")).await?;
            tx.commit().await?;
        }
        fork.flush().await?; // first 5 are flushed to the fork's branch
        for i in 5..8 {
            let tx = fork.tx().await?;
            tx.execute(&format!("CREATE (:N {{i: {i}}})")).await?;
            tx.commit().await?; // these 3 stay in the fork WAL
        }
        drop(fork);
        db.shutdown().await?;
    }

    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        let s = db.session();
        let fork = s.fork("scn").await?;
        let c = fork
            .query("MATCH (n:N) RETURN count(n) AS c")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(-1);
        assert_eq!(
            c, 8,
            "reopened fork must see all committed rows exactly once (no loss, no double-apply)"
        );
        drop(fork);
        db.shutdown().await?;
    }

    Ok(())
}

/// M2/durability: a crash AFTER the fork's branch write + snapshot publish but
/// BEFORE WAL truncation must recover to exactly-once on reopen — no data loss
/// and no gross duplication. The per-fork `wal_high_water_mark` (published with
/// the fork snapshot, review M2) gates the replay so already-flushed segments
/// are not re-applied; for pure inserts replay is additionally idempotent (L0
/// shadows by version), so this test guards the recovery wiring and the
/// no-loss/no-duplication property rather than serving as a count fail-before.
///
/// Ignored by default: drives a crash failpoint and must run serially.
/// Run with: `cargo nextest run -p uni-db --features failpoints --run-ignored all -E 'test(fork_flush_crash_before_truncate_no_double_apply)'`
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_flush_crash_before_truncate_no_double_apply() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();

    // Seed a fork with committed rows (in its WAL).
    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        db.schema()
            .label("N")
            .property("i", DataType::Int)
            .apply()
            .await?;
        let s = db.session();
        let fork = s.fork("scn").await?;
        for i in 0..6 {
            let tx = fork.tx().await?;
            tx.execute(&format!("CREATE (:N {{i: {i}}})")).await?;
            tx.commit().await?;
        }
        drop(fork);
        db.shutdown().await?;
    }

    // Reopen, then flush the fork with a crash injected between complete_flush
    // and WAL truncation (branch + snapshot durable, WAL NOT truncated).
    {
        let db = Arc::new(Uni::open(&uri).config(sync_flush_config()).build().await?);
        let dbc = db.clone();
        fail::cfg("flush::after-complete-before-cache-clear", "panic").unwrap();
        let res = tokio::spawn(async move {
            let s = dbc.session();
            let fork = s.fork("scn").await.unwrap();
            fork.flush().await
        })
        .await;
        fail::remove("flush::after-complete-before-cache-clear");
        assert!(
            res.is_err(),
            "fork flush should have panicked at the failpoint"
        );
        drop(db);
    }

    // Reopen: the 6 rows must be present exactly once, not 12.
    {
        let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
        let s = db.session();
        let fork = s.fork("scn").await?;
        let c = fork
            .query("MATCH (n:N) RETURN count(n) AS c")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(-1);
        assert_eq!(
            c, 6,
            "interrupted-truncate reopen must not double-apply flushed rows (review M2)"
        );

        // A subsequent clean flush must also not inflate the count (catches
        // Append-duplication of already-flushed rows).
        fork.flush().await?;
        let c2 = fork
            .query("MATCH (n:N) RETURN count(n) AS c")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(-1);
        assert_eq!(c2, 6, "re-flush after recovery must not duplicate rows");
        drop(fork);
        db.shutdown().await?;
    }

    Ok(())
}

// ── The same seam under a real process abort ─────────────────────────────────

/// The child half of the abort test in this file. Never returns.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal: child process entry point for the abort harness"]
async fn fork_durability_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();
    if scenario != "flush-before-truncate" {
        crate::crash_harness::unknown_scenario("fork_durability_abort_child", &scenario);
    }

    {
        let db = Uni::open(&uri)
            .config(sync_flush_config())
            .build()
            .await
            .unwrap();
        db.schema()
            .label("N")
            .property("i", DataType::Int)
            .apply()
            .await
            .unwrap();
        let s = db.session();
        let fork = s.fork("scn").await.unwrap();
        for i in 0..6 {
            let tx = fork.tx().await.unwrap();
            tx.execute(&format!("CREATE (:N {{i: {i}}})"))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        drop(fork);
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(&uri)
        .config(sync_flush_config())
        .build()
        .await
        .unwrap();
    crate::crash_harness::abort_at("flush::after-complete-before-cache-clear");
    let s = db.session();
    let fork = s.fork("scn").await.unwrap();
    let _ = fork.flush().await;
    panic!("fork flush returned; the seam was never reached");
}

/// Abort sibling of [`fork_flush_crash_before_truncate_no_double_apply`].
///
/// Under a real abort the WAL is not truncated *and* no shutdown flush runs, so
/// this is the stricter form of the exactly-once claim.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_abort_before_truncate_no_double_apply() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_path_buf();
    crate::crash_harness::run_child_async(
        "fork::fork_durability::fork_durability_abort_child",
        "flush-before-truncate",
        &uri,
    )
    .await;

    let db = Uni::open(uri.to_string_lossy().as_ref())
        .config(sync_flush_config())
        .build()
        .await?;
    let s = db.session();
    let fork = s.fork("scn").await?;
    assert_eq!(
        node_count(&fork).await?,
        6,
        "interrupted-truncate reopen must not double-apply flushed rows"
    );
    fork.flush().await?;
    assert_eq!(
        node_count(&fork).await?,
        6,
        "re-flush after recovery must not duplicate rows"
    );
    drop(fork);
    db.shutdown().await?;
    Ok(())
}

/// `count(n)` over label `N`, for the abort test's before/after assertions.
#[cfg(feature = "failpoints")]
async fn node_count(session: &uni_db::Session) -> Result<i64> {
    Ok(session
        .query("MATCH (n:N) RETURN count(n) AS c")
        .await?
        .rows()
        .first()
        .and_then(|r| r.get::<i64>("c").ok())
        .unwrap_or(-1))
}
