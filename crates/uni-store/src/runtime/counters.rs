// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Per-query execution counters.
//!
//! These exist to answer one question about a single query: **which execution
//! path did it actually take?** That is the question a differential-query-plans
//! oracle has to answer before it can claim a comparison was meaningful — see
//! `docs/proposals/test_harness_implementation_plan_2026-08-12.md` — and it is
//! also the question a human debugging "why is this slow" asks first.
//!
//! # Why a shared counter object rather than a metrics facade
//!
//! `uni-store` already emits `metrics`-crate counters on the write path, but
//! those route to a **process-global** recorder with no in-process reader in
//! library code. They cannot be attributed to one query, which is exactly what is
//! needed here: two queries running concurrently must not pollute each other's
//! counts.
//!
//! So this is an `Arc<QueryCounters>` created once per query and threaded down
//! through the three carriers that already cross the relevant layer boundaries:
//! [`QueryContext`](super::context::QueryContext) into storage,
//! `ScanRequest` into the backend, and `GraphExecutionContext` into the
//! DataFusion operators.
//!
//! # Counting granularity
//!
//! Every increment site adds a **batch's worth** of rows or counts one scan —
//! never one row at a time. A relaxed atomic add per batch is negligible against
//! the work of producing the batch, which is why these are always on rather than
//! hidden behind a config flag.
//!
//! # Why not the loom/shuttle atomic shim
//!
//! [`super::sync`] aliases `AtomicU64` to loom's or shuttle's instrumented twin
//! so the OCC commit core can be model-checked. These counters are pure
//! statistics and are not part of that core; routing them through the shim would
//! make every query construct model-instrumented state, which loom forbids
//! outside a model closure. Plain `std` atomics are the correct choice here.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters accumulated while a single query executes.
///
/// All methods take `&self` and use `Ordering::Relaxed`: these are statistics,
/// never synchronization. A count read after execution completes is exact
/// because the read happens-after every increment via the executor's own
/// completion, not because of the ordering on any individual add.
#[derive(Debug, Default)]
pub struct QueryCounters {
    l0_rows: AtomicU64,
    storage_rows: AtomicU64,
    rows_scanned: AtomicU64,
    branch_scans: AtomicU64,
    snapshot_reads: AtomicU64,
}

impl QueryCounters {
    /// A fresh, all-zero counter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `n` rows served from an L0 buffer (in-memory, unflushed).
    pub fn add_l0_rows(&self, n: usize) {
        self.l0_rows.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Records `n` rows served from L1 / Lance storage.
    pub fn add_storage_rows(&self, n: usize) {
        self.storage_rows.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Records `n` rows examined by a scan, before filtering and projection.
    pub fn add_rows_scanned(&self, n: usize) {
        self.rows_scanned.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Records that a scan **executed** against a fork's Lance branch.
    ///
    /// This must only be called where the branch read actually happens, never
    /// where a branch is merely selected: `BranchedBackend` resolves a branch per
    /// call and falls back to primary when the table has none, so a session with
    /// fork scope active can still read primary. A counter incremented at
    /// selection time would report a fork read that never occurred.
    pub fn add_branch_scan(&self) {
        self.branch_scans.fetch_add(1, Ordering::Relaxed);
    }

    /// Records that a read **executed** against a pinned snapshot version.
    ///
    /// Same rule as [`Self::add_branch_scan`]: increment where the version
    /// filter is applied, not where the pin is configured.
    pub fn add_snapshot_read(&self) {
        self.snapshot_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// Rows served from L0.
    pub fn l0_rows(&self) -> u64 {
        self.l0_rows.load(Ordering::Relaxed)
    }

    /// Rows served from L1 / Lance storage.
    pub fn storage_rows(&self) -> u64 {
        self.storage_rows.load(Ordering::Relaxed)
    }

    /// Rows examined by scans.
    pub fn rows_scanned(&self) -> u64 {
        self.rows_scanned.load(Ordering::Relaxed)
    }

    /// Scans executed against a fork branch.
    pub fn branch_scans(&self) -> u64 {
        self.branch_scans.load(Ordering::Relaxed)
    }

    /// Reads executed against a pinned snapshot.
    pub fn snapshot_reads(&self) -> u64 {
        self.snapshot_reads.load(Ordering::Relaxed)
    }

    /// Folds another counter set into this one.
    ///
    /// Used where a query fans out into sub-executions that each carry their own
    /// counters (for example a procedure call building its own graph context).
    pub fn merge_from(&self, other: &QueryCounters) {
        self.add_l0_rows(other.l0_rows() as usize);
        self.add_storage_rows(other.storage_rows() as usize);
        self.add_rows_scanned(other.rows_scanned() as usize);
        self.branch_scans
            .fetch_add(other.branch_scans(), Ordering::Relaxed);
        self.snapshot_reads
            .fetch_add(other.snapshot_reads(), Ordering::Relaxed);
    }

    /// Resets every counter to zero, for reuse across executions.
    pub fn reset(&self) {
        self.l0_rows.store(0, Ordering::Relaxed);
        self.storage_rows.store(0, Ordering::Relaxed);
        self.rows_scanned.store(0, Ordering::Relaxed);
        self.branch_scans.store(0, Ordering::Relaxed);
        self.snapshot_reads.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::QueryCounters;
    use std::sync::Arc;

    #[test]
    fn counters_accumulate_and_reset() {
        let c = QueryCounters::new();
        assert_eq!(c.l0_rows(), 0, "a fresh counter set is zero");
        c.add_l0_rows(3);
        c.add_l0_rows(4);
        c.add_storage_rows(10);
        c.add_rows_scanned(17);
        c.add_branch_scan();
        c.add_snapshot_read();
        assert_eq!(c.l0_rows(), 7);
        assert_eq!(c.storage_rows(), 10);
        assert_eq!(c.rows_scanned(), 17);
        assert_eq!(c.branch_scans(), 1);
        assert_eq!(c.snapshot_reads(), 1);
        c.reset();
        assert_eq!(c.l0_rows(), 0);
        assert_eq!(c.branch_scans(), 0);
    }

    #[test]
    fn counters_are_shared_through_arc() {
        // The whole design depends on clones of the Arc observing one shared set
        // of counts — a clone that forked its state would silently under-report.
        let c = Arc::new(QueryCounters::new());
        let clone = Arc::clone(&c);
        clone.add_l0_rows(5);
        assert_eq!(c.l0_rows(), 5, "an Arc clone must share the counters");
    }

    #[test]
    fn merge_from_sums_both_sides() {
        let a = QueryCounters::new();
        a.add_l0_rows(2);
        a.add_branch_scan();
        let b = QueryCounters::new();
        b.add_l0_rows(3);
        b.add_branch_scan();
        a.merge_from(&b);
        assert_eq!(a.l0_rows(), 5);
        assert_eq!(a.branch_scans(), 2);
    }
}
