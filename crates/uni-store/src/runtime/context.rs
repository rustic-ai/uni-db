// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::counters::QueryCounters;
use crate::runtime::l0::L0Buffer;
use crate::storage::bulk_gate::RowVisibility;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct QueryContext {
    pub l0: Arc<RwLock<L0Buffer>>,
    pub transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
    /// L0 buffers currently being flushed to L1.
    /// These remain visible to reads until flush completes successfully.
    pub pending_flush_l0s: Vec<Arc<RwLock<L0Buffer>>>,
    pub deadline: Option<Instant>,
    /// Cooperative cancellation token. Checked alongside the deadline in
    /// `check_timeout()`.
    pub cancellation_token: Option<CancellationToken>,
    /// Per-query execution counters, shared with the executor that built this
    /// context.
    ///
    /// `Arc`, not a plain field, because a `QueryContext` is **rebuilt per call**
    /// by `Executor::get_context` and cloned freely down the read path — a
    /// by-value counter would be dropped on the floor at every hop. `None` for
    /// contexts constructed outside a query (recovery, compaction, admin), where
    /// there is no result to attribute counts to.
    pub counters: Option<Arc<QueryCounters>>,
    /// What an L1 read on this query's behalf may return, when the query runs
    /// against a pinned view (a transaction, or a time-travel snapshot).
    ///
    /// The property manager is shared and sits on LIVE storage, so its own
    /// visibility has no ceiling inside a transaction; without this, a
    /// property value committed and flushed after the transaction began would
    /// be read in place of the one its snapshot holds. `None`: live reads.
    pub l1_visibility: Option<RowVisibility>,
}

impl QueryContext {
    pub fn new(l0: Arc<RwLock<L0Buffer>>) -> Self {
        Self {
            l0,
            transaction_l0: None,
            pending_flush_l0s: Vec::new(),
            deadline: None,
            cancellation_token: None,
            counters: None,
            l1_visibility: None,
        }
    }

    pub fn new_with_tx(
        l0: Arc<RwLock<L0Buffer>>,
        transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
    ) -> Self {
        Self {
            l0,
            transaction_l0,
            pending_flush_l0s: Vec::new(),
            deadline: None,
            cancellation_token: None,
            counters: None,
            l1_visibility: None,
        }
    }

    pub fn new_with_pending(
        l0: Arc<RwLock<L0Buffer>>,
        transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
        pending_flush_l0s: Vec<Arc<RwLock<L0Buffer>>>,
    ) -> Self {
        Self {
            l0,
            transaction_l0,
            pending_flush_l0s,
            deadline: None,
            cancellation_token: None,
            counters: None,
            l1_visibility: None,
        }
    }

    pub fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
    }

    pub fn set_cancellation_token(&mut self, token: CancellationToken) {
        self.cancellation_token = Some(token);
    }

    /// Bounds this query's L1 reads to a pinned view's visibility
    /// ([`Self::l1_visibility`]); `None` leaves them live.
    pub fn set_l1_visibility(&mut self, visibility: Option<RowVisibility>) {
        self.l1_visibility = visibility;
    }

    /// Attaches the per-query counter set.
    pub fn set_counters(&mut self, counters: Arc<QueryCounters>) {
        self.counters = Some(counters);
    }

    /// Records `n` rows served from an L0 buffer, if counting is active.
    pub fn count_l0_rows(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_l0_rows(n);
        }
    }

    /// Records `n` rows served from L1 / Lance storage, if counting is active.
    pub fn count_storage_rows(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_storage_rows(n);
        }
    }

    /// Records `n` rows examined by a scan, if counting is active.
    pub fn count_rows_scanned(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_rows_scanned(n);
        }
    }

    pub fn check_timeout(&self) -> anyhow::Result<()> {
        if let Some(ref token) = self.cancellation_token
            && token.is_cancelled()
        {
            return Err(anyhow::anyhow!("Query cancelled"));
        }
        if let Some(deadline) = self.deadline
            && Instant::now() > deadline
        {
            return Err(anyhow::anyhow!("Query timed out"));
        }
        Ok(())
    }
}
