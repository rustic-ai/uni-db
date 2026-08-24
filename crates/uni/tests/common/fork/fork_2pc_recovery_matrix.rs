// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 9: crash inside every fork 2PC window and prove recovery lands.
//!
//! The fork create and drop paths are multi-step, and each step is durable.
//! Until now a crash *between* steps was only simulated indirectly — by
//! hand-building a `Pending` or `Tombstoned` registry, or by faulting Lance
//! from outside through `UNI_FORK_INJECT_FAIL_AFTER`. Neither reaches the
//! windows around the registry writes, the allocator bootstrap, or the two PUTs
//! inside `finish_create`, and the env-var faults cannot fail the *Nth* call —
//! they fail every one, so a partially-deleted fork was unreachable.
//!
//! # The invariant
//!
//! `recover_forks` runs from `Uni::open` before any session handle is exposed,
//! and its three passes promise: `Pending` is always rolled back (never
//! forward), `Tombstoned` is always completed, and orphan tombstones are swept.
//! So after a reopen:
//!
//! > every fork is **`Active` or absent** — never `Pending`, never
//! > `Tombstoned`.
//!
//! Each test below crashes at one seam and asserts exactly that, plus whatever
//! is specific to the window.
//!
//! # Why these are `#[ignore]`d
//!
//! fail-rs's registry is process-global, so a test that arms a seam cannot run
//! concurrently with one that reads it. The CI `failpoints` job passes
//! `--run-ignored all`.
//!
//! Run locally with:
//! `cargo nextest run -p uni-db --features failpoints --run-ignored all -E 'test(fork_2pc)'`

// Rust guideline compliant

#![cfg(feature = "failpoints")]

use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig};

fn sync_flush_config() -> UniConfig {
    UniConfig {
        async_flush_enabled: false,
        ..Default::default()
    }
}

/// Two labels, so a fork owns more than one branch.
///
/// The mid-loop seam needs at least two: crashing after the first delete is
/// what produces a *partially* deleted fork, and with one dataset there is no
/// such state.
async fn seeded(uri: &str) -> Result<Uni> {
    let db = Uni::open(uri).config(sync_flush_config()).build().await?;
    for label in ["A", "B"] {
        db.schema()
            .label(label)
            .property("i", DataType::Int)
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {i: 1}), (:B {i: 2})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Crash at `seam` while running `op`, without shutting the database down.
///
/// `drop` rather than `shutdown` is the point: a graceful close runs the
/// shutdown flush, which is precisely what a crash does not do.
async fn crash_at<F, Fut>(uri: &str, seam: &str, action: &str, op: F) -> Result<()>
where
    F: FnOnce(Arc<Uni>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let db = Arc::new(Uni::open(uri).config(sync_flush_config()).build().await?);
    let dbc = db.clone();
    fail::cfg(seam, action).unwrap();
    let res = tokio::spawn(async move { op(dbc).await }).await;
    fail::remove(seam);
    assert!(res.is_err(), "{seam}: expected a panic at the failpoint");
    drop(db);
    Ok(())
}

/// The phase's acceptance criterion, asserted after every reopen.
async fn assert_no_torn_state(db: &Uni) -> Result<()> {
    for info in db.list_forks().await {
        let status = format!("{:?}", info.status);
        assert_eq!(
            status, "Active",
            "fork {:?} survived recovery in a torn state: {status}",
            info.name
        );
    }
    Ok(())
}

/// Files still namespaced by a fork id — WAL segments, the id allocator, and
/// fork-scoped snapshot manifests.
///
/// **Files, not directories.** `delete_fork_artifacts` goes through
/// `ObjectStore`, which deletes objects; on a local filesystem the now-empty
/// `catalog/forks/{id}/` and `wal_forks/{id}/` directories survive. Testing
/// `Path::exists` therefore reports residue for a fork that was cleaned up
/// perfectly — which it did, until this was corrected. Same rule as
/// `fork_drop_cleanup::file_tree_contains`, which this mirrors.
fn fork_residue(root: &std::path::Path, fork_id: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        if p.is_file() && p.to_string_lossy().contains(fork_id) {
            found.push(p.to_string_lossy().into_owned());
        }
        if let Ok(rd) = std::fs::read_dir(&p) {
            for e in rd.flatten() {
                stack.push(e.path());
            }
        }
    }
    found
}

// ---------------------------------------------------------------------------
// Drop path
// ---------------------------------------------------------------------------

/// Crash right after `begin_drop`: tombstone durable, nothing else done.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_after_begin_drop_recovers() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let fork_id = {
        let db = seeded(&uri).await?;
        let f = db.session().fork("victim").await?;
        drop(f);
        let id = db
            .list_forks()
            .await
            .into_iter()
            .find(|f| f.name == "victim")
            .expect("fork present")
            .id
            .to_string();
        db.shutdown().await?;
        id
    };

    crash_at(&uri, "fork::drop-after-begin", "panic", |db| async move {
        let _ = db.drop_fork("victim").await;
    })
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;
    assert!(
        db.list_forks().await.iter().all(|f| f.name != "victim"),
        "recovery must complete the tombstoned drop"
    );
    assert!(
        fork_residue(dir.path(), &fork_id).is_empty(),
        "recovery left residue: {:?}",
        fork_residue(dir.path(), &fork_id)
    );
    db.shutdown().await?;
    Ok(())
}

/// Crash **after one branch is already deleted** — the partially-deleted fork.
///
/// `1*off->panic` skips the first iteration and crashes on the second, which is
/// the state the env-var fault injection cannot produce: it fails every delete,
/// so the fork is always fully intact or fully gone.
///
/// This is the phase's second acceptance criterion: the tombstone must survive
/// so recovery can finish the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_mid_delete_loop_keeps_the_tombstone() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let fork_id = {
        let db = seeded(&uri).await?;
        let f = db.session().fork("halfway").await?;
        drop(f);
        let id = db
            .list_forks()
            .await
            .into_iter()
            .find(|f| f.name == "halfway")
            .expect("fork present")
            .id
            .to_string();
        db.shutdown().await?;
        id
    };

    crash_at(
        &uri,
        "fork::drop-mid-delete-loop",
        "1*off->panic",
        |db| async move {
            let _ = db.drop_fork("halfway").await;
        },
    )
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;
    assert!(
        db.list_forks().await.iter().all(|f| f.name != "halfway"),
        "a partially-deleted fork must still complete on reopen"
    );
    assert!(
        fork_residue(dir.path(), &fork_id).is_empty(),
        "recovery left residue after a partial delete: {:?}",
        fork_residue(dir.path(), &fork_id)
    );
    db.shutdown().await?;
    Ok(())
}

/// Crash between the artifact sweep and `finish_drop`.
///
/// The window the ordering fix created, and the one that proves it safe: the
/// artifacts are already gone while the tombstone still anchors recovery, so
/// the reopen finishes the drop and the idempotent sweep re-runs harmlessly.
///
/// Before the fix the order was reversed, and the equivalent crash orphaned the
/// WAL directory and id allocator permanently — no tombstone, no registry
/// entry, nothing for `recover_forks` to act on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_after_artifacts_leaves_no_residue() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let fork_id = {
        let db = seeded(&uri).await?;
        let f = db.session().fork("swept").await?;
        let tx = f.tx().await?;
        tx.execute("CREATE (:A {i: 99})").await?;
        tx.commit().await?;
        drop(f);
        let id = db
            .list_forks()
            .await
            .into_iter()
            .find(|f| f.name == "swept")
            .expect("fork present")
            .id
            .to_string();
        db.shutdown().await?;
        id
    };

    crash_at(
        &uri,
        "fork::drop-after-artifacts",
        "panic",
        |db| async move {
            let _ = db.drop_fork("swept").await;
        },
    )
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;
    assert!(
        fork_residue(dir.path(), &fork_id).is_empty(),
        "the fork's WAL dir or id allocator leaked: {:?}",
        fork_residue(dir.path(), &fork_id)
    );
    db.shutdown().await?;
    Ok(())
}

/// Crash after every branch is deleted but before `finish_drop`.
///
/// The complement of the mid-loop case: there the fork is half-deleted, here it
/// is fully deleted but still Tombstoned. Both must converge on the same place,
/// and the tombstone is what carries the second one there — without it the
/// registry entry would be the only trace and `recover_forks` would have no
/// artifact list to sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_before_finish_drop_recovers() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let fork_id = {
        let db = seeded(&uri).await?;
        let f = db.session().fork("halfway").await?;
        let tx = f.tx().await?;
        tx.execute("CREATE (:A {i: 7})").await?;
        tx.commit().await?;
        drop(f);
        let id = db
            .list_forks()
            .await
            .into_iter()
            .find(|f| f.name == "halfway")
            .expect("fork present")
            .id
            .to_string();
        db.shutdown().await?;
        id
    };

    crash_at(&uri, "fork::drop-before-finish", "panic", |db| async move {
        let _ = db.drop_fork("halfway").await;
    })
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;
    assert!(
        db.list_forks().await.iter().all(|f| f.name != "halfway"),
        "recovery must complete the tombstoned drop"
    );
    assert!(
        fork_residue(dir.path(), &fork_id).is_empty(),
        "recovery left residue: {:?}",
        fork_residue(dir.path(), &fork_id)
    );
    db.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Create path
// ---------------------------------------------------------------------------

/// Crash right after `begin_create`: a `Pending` entry, no allocator, no
/// branches. Recovery always rolls a `Pending` fork back, never forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_after_begin_create_rolls_back() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    {
        let db = seeded(&uri).await?;
        db.shutdown().await?;
    }

    crash_at(&uri, "fork::create-after-begin", "panic", |db| async move {
        let _ = db.session().fork("halfborn").await;
    })
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;
    assert!(
        db.list_forks().await.iter().all(|f| f.name != "halfborn"),
        "a Pending fork must be rolled back, not left visible"
    );
    // The name must be reusable — a rolled-back create that left the name
    // claimed would be indistinguishable from a leak to the caller.
    let f = db.session().fork("halfborn").await?;
    drop(f);
    db.shutdown().await?;
    Ok(())
}

/// Crash after the id allocator is written but before any branch exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_after_allocator_rolls_back() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    {
        let db = seeded(&uri).await?;
        db.shutdown().await?;
    }

    crash_at(
        &uri,
        "fork::create-after-allocator",
        "panic",
        |db| async move {
            let _ = db.session().fork("allocated").await;
        },
    )
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;
    assert!(
        db.list_forks().await.iter().all(|f| f.name != "allocated"),
        "a Pending fork must be rolled back"
    );
    db.shutdown().await?;
    Ok(())
}

/// Crash between `finish_create`'s two PUTs: the registry says `Active` while
/// the schema overlay file does not exist yet.
///
/// Benign by design — `load_schema_overlay` returns an empty delta on any read
/// failure — but that is an assumption worth asserting rather than trusting.
/// The fork must be usable after the reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn fork_2pc_crash_between_finish_create_puts_is_usable() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    {
        let db = seeded(&uri).await?;
        db.shutdown().await?;
    }

    crash_at(&uri, "fork::create-mid-finish", "panic", |db| async move {
        let _ = db.session().fork("overlayless").await;
    })
    .await?;

    let db = Uni::open(&uri).config(sync_flush_config()).build().await?;
    assert_no_torn_state(&db).await?;

    // Active with no overlay file: the fork must still open and read.
    if db
        .list_forks()
        .await
        .iter()
        .any(|f| f.name == "overlayless")
    {
        let fork = db.session().fork("overlayless").await?;
        let n = fork.query("MATCH (n:A) RETURN count(n) AS c").await?;
        assert_eq!(
            n.rows().len(),
            1,
            "a fork whose overlay PUT was interrupted must still be readable"
        );
        drop(fork);
    }
    db.shutdown().await?;
    Ok(())
}
