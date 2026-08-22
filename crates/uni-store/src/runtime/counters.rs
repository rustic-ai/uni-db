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
//! The crate-private `runtime::sync` module aliases `AtomicU64` to loom's or
//! shuttle's instrumented twin
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
    index_scans: AtomicU64,
    index_comparisons: AtomicU64,
    lance_iops: AtomicU64,
    scans_reported: AtomicU64,
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

    /// Records one completed Lance scan and what its executed plan reported
    /// about index work.
    ///
    /// Called from the scanner's execution-stats callback, which Lance invokes
    /// after walking the metrics of the plan it actually ran — so this observes
    /// execution, never configuration. `comparisons` is Lance's
    /// `index_comparisons`; `consulted` is whether the plan reported any index
    /// activity at all.
    ///
    /// Always bumps [`Self::scans_reported`], including when nothing was
    /// consulted. That is the point: it is the denominator that makes a zero in
    /// [`Self::index_scans`] mean something.
    pub fn add_lance_scan(&self, consulted: bool, comparisons: usize, iops: usize) {
        self.scans_reported.fetch_add(1, Ordering::Relaxed);
        if consulted {
            self.index_scans.fetch_add(1, Ordering::Relaxed);
        }
        self.index_comparisons
            .fetch_add(comparisons as u64, Ordering::Relaxed);
        self.lance_iops.fetch_add(iops as u64, Ordering::Relaxed);
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

    /// Lance scans whose executed plan reported index work.
    ///
    /// **Nonzero proves an index was searched. Zero does not prove one was
    /// not.** Lance's `indices_loaded` counts index *loads from storage* and is
    /// documented not to fire when the index is already in memory, and a BTree
    /// lookup whose key falls outside every page range reports zero
    /// comparisons. Read this as a positive signal only.
    pub fn index_scans(&self) -> u64 {
        self.index_scans.load(Ordering::Relaxed)
    }

    /// Sum of Lance's `index_comparisons` over the scans in this query.
    ///
    /// A work proxy whose unit depends on the index type, so the magnitude is
    /// never worth asserting exactly — only its presence or absence.
    pub fn index_comparisons(&self) -> u64 {
        self.index_comparisons.load(Ordering::Relaxed)
    }

    /// Storage I/O operations Lance performed across the scans in this query.
    ///
    /// Measured to fall with the number of fragments a table holds — about five
    /// per fragment on a full scan — which is what makes it a witness that a
    /// compaction changed how a query executed, and not merely that one ran.
    ///
    /// It counts *physical* reads, so it is sensitive to caching in a way the
    /// row counters are not. Today every scan opens a fresh `Dataset`, so it is
    /// stable; a future dataset cache would shrink it on a warm read, exactly as
    /// one already would for `indices_loaded`. A consumer that needs "did this
    /// query touch storage at all" should not use this.
    pub fn lance_iops(&self) -> u64 {
        self.lance_iops.load(Ordering::Relaxed)
    }

    /// Lance scans for which the execution-stats callback fired at all.
    ///
    /// The anti-vacuity denominator. Without it, `index_scans == 0` is
    /// ambiguous between *no scan ran*, *a scan consulted no index*, and *the
    /// callback was never wired* — so a negative assertion would be satisfied
    /// by the very regression it exists to catch. Assert this is nonzero in the
    /// same breath as asserting `index_scans` is zero.
    pub fn scans_reported(&self) -> u64 {
        self.scans_reported.load(Ordering::Relaxed)
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
        self.index_scans
            .fetch_add(other.index_scans(), Ordering::Relaxed);
        self.index_comparisons
            .fetch_add(other.index_comparisons(), Ordering::Relaxed);
        self.lance_iops
            .fetch_add(other.lance_iops(), Ordering::Relaxed);
        self.scans_reported
            .fetch_add(other.scans_reported(), Ordering::Relaxed);
    }

    /// Resets every counter to zero, for reuse across executions.
    pub fn reset(&self) {
        self.l0_rows.store(0, Ordering::Relaxed);
        self.storage_rows.store(0, Ordering::Relaxed);
        self.rows_scanned.store(0, Ordering::Relaxed);
        self.branch_scans.store(0, Ordering::Relaxed);
        self.snapshot_reads.store(0, Ordering::Relaxed);
        self.index_scans.store(0, Ordering::Relaxed);
        self.index_comparisons.store(0, Ordering::Relaxed);
        self.lance_iops.store(0, Ordering::Relaxed);
        self.scans_reported.store(0, Ordering::Relaxed);
    }

    /// A by-value copy of every counter, for handing across a crate boundary.
    ///
    /// Named fields rather than a tuple deliberately: the executor snapshot
    /// used to be a positional `(usize, usize, usize, u64, u64)` destructured
    /// at the far end, and with eight counters that is four-plus interchangeable
    /// `u64` slots where a mis-order type-checks and silently mis-assigns one
    /// counter to another — a mistake invisible in review that would surface as
    /// a product bug in whichever lever reads the wrong field.
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            l0_rows: self.l0_rows(),
            storage_rows: self.storage_rows(),
            rows_scanned: self.rows_scanned(),
            branch_scans: self.branch_scans(),
            snapshot_reads: self.snapshot_reads(),
            index_scans: self.index_scans(),
            index_comparisons: self.index_comparisons(),
            lance_iops: self.lance_iops(),
            scans_reported: self.scans_reported(),
        }
    }
}

/// An immutable, by-value reading of a [`QueryCounters`] set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CounterSnapshot {
    /// Rows served from an L0 buffer.
    pub l0_rows: u64,
    /// Rows served from L1 / Lance storage.
    pub storage_rows: u64,
    /// Rows examined by scans, before filtering and projection.
    pub rows_scanned: u64,
    /// Scans executed against a fork branch.
    pub branch_scans: u64,
    /// Reads executed against a pinned snapshot.
    pub snapshot_reads: u64,
    /// Lance scans whose executed plan reported index work.
    pub index_scans: u64,
    /// Sum of Lance's `index_comparisons` across those scans.
    pub index_comparisons: u64,
    /// Sum of Lance's `iops` across those scans. See
    /// [`QueryCounters::lance_iops`] for what it is and is not good for.
    pub lance_iops: u64,
    /// Lance scans for which the stats callback fired at all.
    pub scans_reported: u64,
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

    /// A scan that consulted no index still counts as *reported*.
    ///
    /// That separation is the whole point of `scans_reported`: without it a zero
    /// in `index_scans` cannot distinguish "a scan ran and used no index" from
    /// "no scan was ever observed", and every negative assertion built on it
    /// would be satisfied by an unwired callback.
    #[test]
    fn a_scan_without_an_index_still_reports() {
        let c = QueryCounters::new();
        c.add_lance_scan(false, 0, 3);
        assert_eq!(c.index_scans(), 0);
        assert_eq!(c.scans_reported(), 1, "the scan itself must be counted");

        c.add_lance_scan(true, 12, 7);
        assert_eq!(c.index_scans(), 1);
        assert_eq!(c.index_comparisons(), 12);
        assert_eq!(c.scans_reported(), 2);
        // Accumulated across both scans, and independent of `consulted`:
        // physical I/O happens whether or not an index was involved.
        assert_eq!(c.lance_iops(), 10);
    }

    /// Every field must survive `merge_from` and be cleared by `reset`.
    ///
    /// Adding a counter means five edits — field, adder, getter, `merge_from`,
    /// `reset` — and nothing but this test enforces the last two. A counter
    /// missing from `merge_from` silently under-reports whenever a query fans
    /// out into sub-executions; one missing from `reset` leaks across reuse.
    #[test]
    fn every_counter_merges_and_resets() {
        let a = QueryCounters::new();
        let b = QueryCounters::new();
        b.add_l0_rows(1);
        b.add_storage_rows(2);
        b.add_rows_scanned(3);
        b.add_branch_scan();
        b.add_snapshot_read();
        b.add_lance_scan(true, 4, 2);
        a.merge_from(&b);

        let m = a.snapshot();
        assert_eq!(
            m,
            crate::runtime::counters::CounterSnapshot {
                l0_rows: 1,
                storage_rows: 2,
                rows_scanned: 3,
                branch_scans: 1,
                snapshot_reads: 1,
                index_scans: 1,
                index_comparisons: 4,
                lance_iops: 2,
                scans_reported: 1,
            },
            "a counter is missing from merge_from"
        );

        a.reset();
        assert_eq!(
            a.snapshot(),
            crate::runtime::counters::CounterSnapshot::default(),
            "a counter is missing from reset"
        );
    }
}
