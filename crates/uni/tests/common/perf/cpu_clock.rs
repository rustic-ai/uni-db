// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! CPU time used by this process, for bounds on how much work a query did.
//!
//! A wall-clock ceiling on a short query measures the machine as much as the
//! query: beside a full workspace run, a query that does 50 ms of work can be
//! off-CPU for ten times that, and the ceiling fails with nothing wrong. CPU
//! time counts only the cycles spent, so contention does not inflate it. For a
//! query stopped by a wall-clock deadline it goes *down* under load, since the
//! query gets fewer cycles before the deadline arrives.
//!
//! The clock is process-wide (`CLOCK_PROCESS_CPUTIME_ID`): every thread,
//! including ones that have exited, such as the scoped threads a per-row
//! sub-plan runs on. nextest runs each test in its own process, so the delta
//! across a query is that query's work plus whatever the test's own database
//! does in the background meanwhile. Under `cargo test`, which runs tests as
//! threads of one process, the delta also includes concurrently running tests,
//! so a bound built on it is only exact under nextest.

// Rust guideline compliant

use std::time::Duration;

/// CPU time this process has used so far, across all its threads.
///
/// # Panics
///
/// When the process CPU clock cannot be read, which on Linux it always can.
pub fn process_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable `timespec` for the duration of the
    // call, and `CLOCK_PROCESS_CPUTIME_ID` is a clock id the platform defines.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_PROCESS_CPUTIME_ID) failed");
    Duration::new(
        u64::try_from(ts.tv_sec).expect("CPU time is non-negative"),
        u32::try_from(ts.tv_nsec).expect("nanoseconds fit in u32"),
    )
}

/// Wall time and process CPU time spent across one future.
pub struct Spent {
    /// Elapsed wall-clock time.
    pub wall: Duration,
    /// Process CPU time used meanwhile; see the module documentation.
    pub cpu: Duration,
}

/// Run `fut`, returning its output and the [`Spent`] on it.
pub async fn measure<T>(fut: impl std::future::Future<Output = T>) -> (T, Spent) {
    let cpu_before = process_cpu_time();
    let started = std::time::Instant::now();
    let out = fut.await;
    let wall = started.elapsed();
    let cpu = process_cpu_time().saturating_sub(cpu_before);
    (out, Spent { wall, cpu })
}
