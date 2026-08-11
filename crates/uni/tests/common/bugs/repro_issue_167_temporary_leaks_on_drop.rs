// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #167 — a temporary database dropped **without** an explicit
//! `shutdown()` usually leaves its `uni_mem_*` directory behind.
//!
//! `shutdown_reaps_scratch_dir` already pins the `shutdown()` path, where
//! `shutdown_async()` awaits the tracked background tasks and `reap_scratch_dir`
//! retries the removal. Nothing covers the path a caller reaches by simply
//! letting the handle go out of scope, which is what `db = Uni.temporary()`
//! without a `with` block does from Python.
//!
//! On that path `Drop for Uni` calls `ShutdownHandle::shutdown_blocking`, which
//! despite the name only sends a broadcast and returns — it awaits nothing. The
//! only remaining cleanup is `TempDir`'s own `Drop`: a single un-retried
//! `remove_dir_all` whose error is discarded. `remove_dir_all` walks then
//! unlinks, so a background flush or manifest write landing mid-walk yields
//! `ENOTEMPTY` and the directory survives silently. Hence the reported
//! nondeterminism: the removal exists, it just often loses the race.
//!
//! Asserted on the observable property — nothing left behind — rather than on
//! an internal path, matching `shutdown_reaps_scratch_dir`.

// Rust guideline compliant

use uni_db::Uni;

/// Cycles enough times that a per-drop leak rate of even a few percent is
/// overwhelmingly likely to show. The reported rate is ~70%.
const CYCLES: usize = 40;

#[tokio::test]
async fn drop_without_shutdown_leaves_no_scratch_directory_behind() -> anyhow::Result<()> {
    // A private TMPDIR, so a concurrent test's databases cannot be miscounted.
    // Safe here: nextest runs each test in its own process.
    let root = tempfile::tempdir()?;
    unsafe { std::env::set_var("TMPDIR", root.path()) };

    for _ in 0..CYCLES {
        let db = Uni::in_memory().build().await?;
        // Deliberately no `db.shutdown()` — this is the path under test.
        drop(db);
    }

    let stranded = drain(root.path()).await?;
    assert!(
        stranded.is_empty(),
        "dropping a temporary database without shutdown() left {}/{CYCLES} scratch \
         directories behind: {stranded:?}",
        stranded.len()
    );
    Ok(())
}

/// The same, with a schema applied — the issue reports the rate is unchanged,
/// confirming this is not about whether the database was ever really opened.
#[tokio::test]
async fn drop_after_schema_apply_leaves_no_scratch_directory_behind() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    unsafe { std::env::set_var("TMPDIR", root.path()) };

    for _ in 0..CYCLES {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("X")
            .property("n", uni_db::DataType::String)
            .apply()
            .await?;
        drop(db);
    }

    let stranded = drain(root.path()).await?;
    assert!(
        stranded.is_empty(),
        "dropping a temporary database with a schema left {}/{CYCLES} scratch \
         directories behind: {stranded:?}",
        stranded.len()
    );
    Ok(())
}

/// Lists surviving scratch directories, allowing a bounded wait first.
///
/// Teardown on the bare-drop path is inherently asynchronous: `Drop for Uni`
/// signals the background tasks and returns, and the directory is removed once
/// the last of them releases its claim. So the most recently dropped database
/// legitimately outlives its `drop` by a few milliseconds. What must not happen
/// is a directory surviving *indefinitely*, which is what the issue reports —
/// hence a bounded poll rather than an unconditional sleep.
async fn drain(root: &std::path::Path) -> anyhow::Result<Vec<String>> {
    let mut stranded = Vec::new();
    for _ in 0..100 {
        stranded = std::fs::read_dir(root)?
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("uni_mem_"))
            .collect();
        if stranded.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(stranded)
}

/// `shutdown()` must remove the directory *before* the handle is dropped.
///
/// `shutdown_in_place` awaits every background task and can therefore reap
/// eagerly. Leaving it to the guard's `Drop` instead would keep the directory
/// alive for the lifetime of the handle — which for a Python
/// `with Uni.temporary() as db:` block means past the end of the block, until
/// garbage collection runs. Asserted with the handle deliberately still alive.
#[tokio::test]
async fn shutdown_reaps_before_the_handle_is_dropped() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    unsafe { std::env::set_var("TMPDIR", root.path()) };

    let db = Uni::in_memory().build().await?;
    let path = std::path::PathBuf::from(db.uri());
    db.shutdown_in_place().await?;

    assert!(
        !path.exists(),
        "shutdown() must remove {} while the handle is still alive",
        path.display()
    );
    drop(db);
    Ok(())
}
