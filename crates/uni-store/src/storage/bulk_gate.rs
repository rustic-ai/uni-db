// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The bulk-load visibility gate: one bulk load becomes visible all at once.
//!
//! A bulk load publishes its staged rows table by table -- per-label vertex
//! tables, `vertices`, `main_edges`, then each edge type's fwd and bwd delta
//! tables -- and each publish is its own Lance commit. Lance has no
//! cross-table commit, so without a gate a reader in the middle of a publish
//! saw a label scan return the new vertices while a label-less scan did not,
//! new vertices with none of their edges, or fwd edges without their bwd
//! twins.
//!
//! Every row of one load carries the same `_version` (the bulk writer reserves
//! one version per load). While a load publishes, its version is *hidden*: the
//! `StorageManager` read tree conjoins `NOT (_version IN hidden)` onto every L1
//! read, and the adjacency CSR is built under the same filter and is installed
//! only if the gate did not change while it was being built. When every table
//! is published the version is released in one step, under the gate's lock,
//! together with the invalidation of the cached CSRs that lack the load.
//!
//! # Scope
//!
//! The gate is in-memory and per `StorageManager` family (a database handle and
//! the transaction and snapshot views derived from it). Another process, and
//! another handle opened on the same path (a read-only reader beside a writer),
//! read the tables directly and are not gated: they can observe a load part way
//! through its publish.
//!
//! Semantic compaction (Tier 2) folds delta rows into L2 adjacency, whose rows
//! carry the compaction's version, and clears the deltas; run while a version
//! is hidden it would expose part of the load through L2. It therefore takes
//! the gate's exclusion lock for writing, and every hold takes it for reading,
//! so the two never overlap.
//!
//! A fork cut while a version is hidden would branch off the tables part way
//! through the publish and keep that partial load for good: its branches are
//! cut at the versions read then, and it reads with a gate of its own. Fork
//! creation therefore waits for the gate to open ([`BulkVisibilityGate::wait_open`])
//! and re-reads the epoch after capturing its fork point; a change means a
//! publish began during the capture, and the capture is retried.

// Rust guideline compliant

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::types::{FilterExpr, Scalar, cols};

/// Which row versions an L1 read may return.
///
/// Combines a snapshot ceiling (`_version <= hwm`, from a pinned snapshot or a
/// transaction pin) with the versions the [`BulkVisibilityGate`] hides. The
/// default admits every row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowVisibility {
    hwm: Option<u64>,
    hidden: Arc<[u64]>,
}

impl RowVisibility {
    /// Visibility with ceiling `hwm` and the `hidden` versions excluded.
    #[must_use]
    pub fn new(hwm: Option<u64>, hidden: Arc<[u64]>) -> Self {
        Self { hwm, hidden }
    }

    /// Visibility with ceiling `hwm` and nothing hidden.
    #[must_use]
    pub fn at_most(hwm: Option<u64>) -> Self {
        Self {
            hwm,
            hidden: Arc::from(Vec::new()),
        }
    }

    /// The snapshot ceiling, if any.
    #[must_use]
    pub fn hwm(&self) -> Option<u64> {
        self.hwm
    }

    /// The versions hidden by in-flight bulk publishes.
    #[must_use]
    pub fn hidden(&self) -> &[u64] {
        &self.hidden
    }

    /// Whether a row stamped `version` is visible.
    #[must_use]
    pub fn admits(&self, version: u64) -> bool {
        self.hwm.is_none_or(|hwm| version <= hwm) && !self.hidden.contains(&version)
    }

    /// The predicate an L1 read conjoins, or `None` when every row is visible.
    #[must_use]
    pub fn filter(&self) -> Option<FilterExpr> {
        let hide = (!self.hidden.is_empty()).then(|| {
            FilterExpr::negate(FilterExpr::one_of(
                cols::VERSION,
                self.hidden.iter().map(|v| Scalar::UInt(*v)),
            ))
        });
        match (self.hwm.map(FilterExpr::version_at_most), hide) {
            (Some(a), Some(b)) => Some(FilterExpr::all([a, b])),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// `filter` restricted to the visible versions.
    #[must_use]
    pub fn bound(&self, filter: FilterExpr) -> FilterExpr {
        match self.filter() {
            Some(v) => FilterExpr::all([filter, v]),
            None => filter,
        }
    }

    /// The optional `extra` filter restricted to the visible versions.
    #[must_use]
    pub fn combine(&self, extra: Option<&FilterExpr>) -> Option<FilterExpr> {
        match (self.filter(), extra) {
            (Some(v), Some(f)) => Some(FilterExpr::all([v, f.clone()])),
            (Some(v), None) => Some(v),
            (None, Some(f)) => Some(f.clone()),
            (None, None) => None,
        }
    }
}

/// Converts a version argument into a [`RowVisibility`].
///
/// Lets the main-table readers take either a bare ceiling (`Option<u64>`, for
/// write-side reads that are not gated) or a full [`RowVisibility`].
pub trait IntoRowVisibility {
    /// Performs the conversion.
    fn into_row_visibility(self) -> RowVisibility;
}

impl IntoRowVisibility for Option<u64> {
    fn into_row_visibility(self) -> RowVisibility {
        RowVisibility::at_most(self)
    }
}

impl IntoRowVisibility for RowVisibility {
    fn into_row_visibility(self) -> RowVisibility {
        self
    }
}

impl IntoRowVisibility for &RowVisibility {
    fn into_row_visibility(self) -> RowVisibility {
        self.clone()
    }
}

#[derive(Debug, Default)]
struct GateState {
    /// Hidden version -> number of holds on it.
    hidden: BTreeMap<u64, usize>,
    /// `hidden`'s keys, shared with readers without copying.
    snapshot: Arc<[u64]>,
    /// Bumped on every change, so a reader can tell that what it built under
    /// one state is stale under the next.
    epoch: u64,
}

impl GateState {
    fn changed(&mut self) {
        self.snapshot = self.hidden.keys().copied().collect::<Vec<_>>().into();
        self.epoch = self.epoch.wrapping_add(1);
    }
}

/// Hides the rows of bulk loads whose publish is in flight; see the module
/// docs.
#[derive(Debug, Default)]
pub struct BulkVisibilityGate {
    state: parking_lot::RwLock<GateState>,
    /// Holds read it, semantic compaction writes it.
    exclusion: Arc<tokio::sync::RwLock<()>>,
    /// Woken whenever the last hidden version is shown.
    opened: tokio::sync::Notify,
}

impl BulkVisibilityGate {
    /// An open gate: nothing hidden.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The versions currently hidden.
    #[must_use]
    pub fn hidden(&self) -> Arc<[u64]> {
        Arc::clone(&self.state.read().snapshot)
    }

    /// The current epoch and hidden versions, read together.
    #[must_use]
    pub fn snapshot(&self) -> (u64, Arc<[u64]>) {
        let state = self.state.read();
        (state.epoch, Arc::clone(&state.snapshot))
    }

    /// Whether any version is hidden.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        !self.state.read().hidden.is_empty()
    }

    /// The gate's current epoch; it changes whenever a version is hidden or
    /// shown.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.state.read().epoch
    }

    /// Waits until no version is hidden, for at most `timeout`, and returns
    /// the epoch read together with the open state; `None` if a version was
    /// still hidden when the time ran out.
    ///
    /// For a reader that must not observe any publish part way through (fork
    /// creation, whose branches would keep a partial load): read the epoch
    /// here, do the work, and compare [`Self::epoch`] afterwards. An equal
    /// epoch means no publish began in between, since every publish hides its
    /// version before its first table and shows it after its last.
    pub async fn wait_open(&self, timeout: Duration) -> Option<u64> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.opened.notified();
            tokio::pin!(notified);
            // Registered before the state is read, so a release between the
            // read and the await still wakes this waiter.
            notified.as_mut().enable();
            {
                let state = self.state.read();
                if state.hidden.is_empty() {
                    return Some(state.epoch);
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return None;
            }
        }
    }

    /// Runs `install` if the gate is still at `epoch`, holding the gate so
    /// that no change can land between the check and the install. Returns
    /// `None` without running it otherwise.
    ///
    /// For a cache built under one gate state (the adjacency CSR): installing
    /// it after the state changed would serve rows the gate now hides, or miss
    /// rows it now shows, until the next invalidation.
    pub fn install_if_current<R>(&self, epoch: u64, install: impl FnOnce() -> R) -> Option<R> {
        let state = self.state.read();
        (state.epoch == epoch).then(install)
    }

    /// Hides `version` until the returned hold is released.
    ///
    /// Waits for a running semantic compaction to finish first, and keeps one
    /// from starting while the hold lives.
    pub async fn hide(self: &Arc<Self>, version: u64) -> GateHold {
        let exclusion = Arc::clone(&self.exclusion).read_owned().await;
        {
            let mut state = self.state.write();
            *state.hidden.entry(version).or_insert(0) += 1;
            state.changed();
        }
        GateHold {
            gate: Some(Arc::clone(self)),
            version,
            exclusion: Some(exclusion),
        }
    }

    /// Exclusive access for semantic compaction, or `None` while a hold lives.
    #[must_use]
    pub fn try_exclusive(&self) -> Option<tokio::sync::OwnedRwLockWriteGuard<()>> {
        Arc::clone(&self.exclusion).try_write_owned().ok()
    }

    fn unhide(&self, version: u64, under_lock: impl FnOnce()) {
        let mut state = self.state.write();
        if let Some(n) = state.hidden.get_mut(&version) {
            *n -= 1;
            if *n == 0 {
                state.hidden.remove(&version);
            }
        }
        state.changed();
        under_lock();
        if state.hidden.is_empty() {
            self.opened.notify_waiters();
        }
    }
}

/// Keeps one bulk load's version hidden; see [`BulkVisibilityGate::hide`].
///
/// [`Self::release`] shows the load. Dropping the hold without releasing it
/// leaves the version hidden for the life of the process -- a publish that was
/// abandoned part way must not become visible part way; the next open
/// completes it -- but lets semantic compaction run again.
#[derive(Debug)]
pub struct GateHold {
    gate: Option<Arc<BulkVisibilityGate>>,
    version: u64,
    exclusion: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

impl GateHold {
    /// The hidden version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Shows the load, running `under_lock` while the gate is still locked so
    /// that no reader observes the new state before it has run.
    ///
    /// `under_lock` must not block or take the gate; it is for dropping
    /// in-memory caches built without the load.
    pub fn release(mut self, under_lock: impl FnOnce()) {
        if let Some(gate) = self.gate.take() {
            gate.unhide(self.version, under_lock);
        }
        self.exclusion.take();
    }
}

impl Drop for GateHold {
    fn drop(&mut self) {
        if self.gate.is_some() {
            tracing::warn!(
                version = self.version,
                "bulk load visibility hold dropped before its publish completed; the load stays \
                 hidden in this process and is completed at the next open"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_visibility_has_no_filter() {
        assert!(RowVisibility::default().filter().is_none());
        assert!(RowVisibility::default().admits(7));
    }

    #[test]
    fn hidden_versions_are_filtered_and_not_admitted() {
        let v = RowVisibility::new(Some(10), Arc::from(vec![4u64]));
        assert!(v.admits(3));
        assert!(!v.admits(4));
        assert!(!v.admits(11));
        let sql = v.filter().unwrap().to_sql().unwrap();
        assert!(sql.contains("_version"), "{sql}");
        assert!(sql.contains("NOT"), "{sql}");
    }

    #[tokio::test]
    async fn hold_hides_until_released_and_excludes_compaction() {
        let gate = Arc::new(BulkVisibilityGate::new());
        let (e0, _) = gate.snapshot();
        let hold = gate.hide(5).await;
        assert_eq!(&*gate.hidden(), &[5]);
        assert!(gate.try_exclusive().is_none(), "a hold excludes compaction");
        let (e1, _) = gate.snapshot();
        assert_ne!(e0, e1);
        assert!(gate.install_if_current(e0, || ()).is_none());
        let mut ran = false;
        hold.release(|| ran = true);
        assert!(ran);
        assert!(gate.hidden().is_empty());
        assert!(gate.try_exclusive().is_some());
    }

    #[tokio::test]
    async fn wait_open_returns_at_once_when_nothing_is_hidden() {
        let gate = Arc::new(BulkVisibilityGate::new());
        let epoch = gate.wait_open(Duration::ZERO).await;
        assert_eq!(epoch, Some(gate.epoch()));
    }

    #[tokio::test]
    async fn wait_open_times_out_while_a_version_is_hidden() {
        let gate = Arc::new(BulkVisibilityGate::new());
        let hold = gate.hide(3).await;
        assert_eq!(gate.wait_open(Duration::from_millis(50)).await, None);
        hold.release(|| ());
        assert!(gate.wait_open(Duration::ZERO).await.is_some());
    }

    #[tokio::test]
    async fn wait_open_wakes_when_the_last_hold_is_released() {
        let gate = Arc::new(BulkVisibilityGate::new());
        let first = gate.hide(3).await;
        let second = gate.hide(4).await;
        let before = gate.epoch();
        let waiter = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move { gate.wait_open(Duration::from_secs(30)).await })
        };
        first.release(|| ());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "one version is still hidden");
        second.release(|| ());
        let epoch = waiter.await.unwrap().expect("opened before the deadline");
        assert_ne!(epoch, before, "the epoch moved with every change");
        assert_eq!(epoch, gate.epoch());
    }

    #[tokio::test]
    async fn an_epoch_read_at_open_detects_a_publish_that_began_later() {
        let gate = Arc::new(BulkVisibilityGate::new());
        let epoch = gate.wait_open(Duration::ZERO).await.unwrap();
        let hold = gate.hide(8).await;
        assert_ne!(gate.epoch(), epoch, "a publish started after the read");
        hold.release(|| ());
        assert_ne!(
            gate.epoch(),
            epoch,
            "and finished: still not the same state"
        );
    }

    #[tokio::test]
    async fn dropped_hold_keeps_the_version_hidden_but_frees_compaction() {
        let gate = Arc::new(BulkVisibilityGate::new());
        drop(gate.hide(9).await);
        assert_eq!(&*gate.hidden(), &[9]);
        assert!(gate.try_exclusive().is_some());
    }
}
