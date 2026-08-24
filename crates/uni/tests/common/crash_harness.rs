// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A child-process abort harness: crash simulation that actually crashes.
//!
//! # Why this exists
//!
//! Every other "crash" test in this repo panics inside a `tokio::spawn` and then
//! drops the `Uni`. That is not a crash. `Drop for Uni` broadcasts the shutdown
//! signal, and the auto-flush task responds by running a full `flush_to_l1` —
//! the same call `shutdown_in_place` makes. `auto_flush_interval` defaults to
//! `Some(5s)`, so it is armed in essentially every one of those tests.
//!
//! So they validate *graceful-close* atomicity, which is strictly weaker than
//! crash atomicity, and the two have already diverged in practice: #167's
//! shutdown-triggered final flush *recreating* a directory was a `Drop`-path
//! behaviour. `ssi_resilience.rs` concedes in prose that the bug one of its
//! tests regresses was reachable only on the graceful-close path.
//!
//! They are also racy rather than merely weak. `Uni::drop` does not await that
//! final flush, so it races the reopen — which is why one test has to set
//! `auto_flush_interval: None` to stop a flake.
//!
//! A `SIGABRT` has none of that. No unwinding, no destructors, no atexit
//! handlers, no buffered writes. What survives is exactly what was fsynced.
//!
//! # How it works
//!
//! fail-rs has no abort action — its grammar is `off`, `return`, `sleep`,
//! `panic`, `print`, `pause`, `yield`, `delay` and nothing else. But
//! [`fail::cfg_callback`] takes an arbitrary `Fn()`, and the bare
//! `fail_point!("name")` form runs whatever task is registered. So
//! [`abort_at`] makes any existing seam abort the process, with **no change to
//! production code**.
//!
//! An abort kills the whole test binary, so it has to happen somewhere else.
//! `CARGO_BIN_EXE_*` is not available (uni-db has no bin targets), so the child
//! is this same integration binary, re-invoked through `std::env::current_exe`
//! and pointed at a single `#[ignore]`d entry-point test.
//!
//! ```text
//!   parent test                       child process
//!   ───────────                       ─────────────
//!   TempDir::new()
//!   run_child(entry, scenario, path) ─────▶ child_env() -> (scenario, path)
//!                                           rebuild pre-crash state at `path`
//!                                           abort_at(seam)
//!                                           ...drive the operation...
//!                                           <SIGABRT>
//!   assert signal == SIGABRT   ◀───────────
//!   Uni::open(path); assert ...
//! ```
//!
//! The parent owns the `TempDir` and passes a path, so the directory outlives
//! the child and is cleaned up normally when the parent finishes.
//!
//! # Reading a failure
//!
//! The dangerous outcome is not a child that fails — it is a child that *exits
//! cleanly*, meaning the seam was never reached and the parent's post-reopen
//! assertions ran against a database nothing ever crashed. [`run_child`] treats
//! that as an explicit failure and prints the child's stderr, because it is the
//! one failure mode that would otherwise pass silently.
//!
//! If a child hangs, nextest's `slow-timeout` (60s x 3, see
//! `.config/nextest.toml`) is the backstop; this harness adds no timeout of its
//! own.

// Rust guideline compliant

#![cfg(all(unix, feature = "failpoints"))]

use std::path::{Path, PathBuf};
use std::process::Command;

/// `SIGABRT`. Hardcoded because `libc` is not a dev-dependency of this crate
/// and pulling one in for a single constant is not worth it.
pub const ABORT_SIGNAL: i32 = 6;

/// Names the scenario the child should run. Set only on the child `Command`,
/// never exported into the parent's own environment.
const SCENARIO_VAR: &str = "UNI_CRASH_SCENARIO";

/// Where the child should open (or create) its database.
const PATH_VAR: &str = "UNI_CRASH_DB_PATH";

/// Reads the child-side environment.
///
/// Returns `None` in the parent, and in the `--run-ignored all` pass that runs
/// the entry-point tests directly with nothing set — an entry point that sees
/// `None` has nothing to do and returns.
pub fn child_env() -> Option<(String, PathBuf)> {
    let scenario = std::env::var(SCENARIO_VAR).ok()?;
    let path = std::env::var(PATH_VAR).ok()?;
    Some((scenario, PathBuf::from(path)))
}

/// Arms `seam` to abort the process instead of panicking.
///
/// Uses [`fail::cfg_callback`] rather than an action string because fail-rs has
/// no abort action. Call this in the child only — it will take down whatever
/// process evaluates the seam.
pub fn abort_at(seam: &str) {
    fail::cfg_callback(seam.to_string(), || {
        // Flush nothing, run nothing. That is the point.
        std::process::abort()
    })
    .unwrap_or_else(|e| panic!("failed to arm abort at {seam}: {e}"));
}

/// Runs `entry_test` in a child process and asserts it died of `SIGABRT`.
///
/// `entry_test` is the full nextest/libtest path of an `#[ignore]`d entry-point
/// test, e.g. `"ssi_resilience::ssi_abort_child"`.
///
/// # Panics
///
/// On any outcome other than death by `SIGABRT`, including a clean exit — see
/// the module docs on why a clean exit is the failure mode worth shouting
/// about.
pub fn run_child(entry_test: &str, scenario: &str, db_path: &Path) {
    let exe = std::env::current_exe().expect("current_exe");
    let output = Command::new(&exe)
        .args([entry_test, "--exact", "--nocapture", "--ignored"])
        .env(SCENARIO_VAR, scenario)
        .env(PATH_VAR, db_path)
        .output()
        .unwrap_or_else(|e| panic!("failed to spawn child {}: {e}", exe.display()));

    use std::os::unix::process::ExitStatusExt;
    if output.status.signal() == Some(ABORT_SIGNAL) {
        return;
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    if output.status.success() {
        panic!(
            "child for scenario '{scenario}' exited cleanly — the seam was \
             never reached, so nothing crashed and any assertion the parent \
             makes after this is vacuous.\n\
             --- child stdout ---\n{stdout}\n--- child stderr ---\n{stderr}"
        );
    }
    panic!(
        "child for scenario '{scenario}' did not abort: status {:?}, signal {:?}\n\
         --- child stdout ---\n{stdout}\n--- child stderr ---\n{stderr}",
        output.status.code(),
        output.status.signal(),
    );
}

/// [`run_child`] from an async test, without blocking a runtime worker.
///
/// A panic inside the blocking task is re-raised here rather than surfacing as
/// an opaque `JoinError`, so the diagnostics [`run_child`] builds survive.
pub async fn run_child_async(entry_test: &str, scenario: &str, db_path: &Path) {
    let (entry, scenario, path) = (
        entry_test.to_string(),
        scenario.to_string(),
        db_path.to_path_buf(),
    );
    match tokio::task::spawn_blocking(move || run_child(&entry, &scenario, &path)).await {
        Ok(()) => {}
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => panic!("child task failed to run: {e}"),
    }
}

/// Fails an entry point that was handed a scenario it does not know.
///
/// A silent fall-through here would turn the parent test into a no-op that
/// still passes, which is the exact defect this harness exists to remove.
pub fn unknown_scenario(entry: &str, scenario: &str) -> ! {
    panic!(
        "entry point '{entry}' has no scenario named '{scenario}'. \
         The parent test and the child dispatch table have diverged."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The harness must not pass when the child fails to abort.
    ///
    /// A scenario name the entry point does not know makes the child panic
    /// instead of aborting. That is the shape of every divergence between a
    /// parent test and a child dispatch table — a typo, a renamed scenario, a
    /// parent pointed at the wrong entry point — and it must be loud, because
    /// a harness that shrugged here would let a parent's post-reopen
    /// assertions run against a database that never crashed.
    ///
    /// This is the harness testing itself; without it, `run_child`'s failure
    /// path is the one part of this file nothing exercises.
    /// The seam, and only the seam, is what kills the child.
    ///
    /// The child arms a real failpoint that its operation never evaluates, runs
    /// the operation, and exits 0. If `run_child` accepted that, then "the
    /// child died of SIGABRT" would say nothing about *where* it died — and
    /// parent assertions like `n == 0` or "the seeded doc survived" would pass
    /// just as happily against a child that aborted the moment the seam was
    /// armed, before doing any work at all.
    ///
    /// So this is the test that makes the other twenty-one mean something.
    #[test]
    fn a_child_whose_seam_is_never_reached_fails_loudly() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let outcome = std::panic::catch_unwind(|| {
            run_child(
                "ssi_resilience::ssi_abort_child",
                "unreached-seam",
                &dir.path().join("db"),
            )
        });
        let err = outcome.expect_err("run_child must fail when the seam is never reached");
        let msg = panic_message(&err);
        assert!(
            msg.contains("exited cleanly"),
            "expected an 'exited cleanly' diagnostic, got: {msg}"
        );
    }

    fn panic_message(err: &Box<dyn std::any::Any + Send>) -> &str {
        err.downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| err.downcast_ref::<&str>().copied())
            .unwrap_or("")
    }

    #[test]
    fn a_child_that_does_not_abort_fails_loudly() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let outcome = std::panic::catch_unwind(|| {
            run_child(
                "ssi_resilience::ssi_abort_child",
                "no-such-scenario",
                &dir.path().join("db"),
            )
        });
        let err = outcome.expect_err("run_child must fail when the child does not abort");
        let msg = panic_message(&err);
        assert!(
            msg.contains("did not abort"),
            "expected a 'did not abort' diagnostic, got: {msg}"
        );
    }
}
