// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A [`MemoryPool`] wrapper that records the query's reserved high-water mark.
//!
//! `PROFILE` reported `peak_memory_bytes: 0` for every query, because nothing
//! measured it. DataFusion's pools expose only the *current* reservation
//! (`MemoryPool::reserved`), and a query's pool is gone by the time the
//! profile is assembled, so the peak has to be observed while it happens.
//!
//! The wrapper delegates every call to the pool it wraps — the limit, the
//! refusal and its message are the inner pool's — and mirrors each successful
//! grow and shrink into the query's [`QueryCounters`]. A statement builds one
//! wrapped pool, shared by every physical plan it builds — sub-plans, per-row
//! mutation plans and procedures' inner queries included (see
//! `StatementMemory`) — so the running total is exactly what the statement
//! holds reserved, and its maximum is the peak.
//!
//! The figure is *reserved* memory, not RSS: an allocation that never asks the
//! pool is invisible to it, exactly as it is invisible to `max_query_memory`.

use std::fmt;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use uni_store::QueryCounters;

/// Delegating [`MemoryPool`] that tracks the reserved high-water mark.
#[derive(Debug)]
pub(crate) struct PeakTrackingPool {
    inner: Arc<dyn MemoryPool>,
    counters: Arc<QueryCounters>,
}

impl PeakTrackingPool {
    /// Wrap `inner`, recording reservations into `counters`.
    pub(crate) fn new(inner: Arc<dyn MemoryPool>, counters: Arc<QueryCounters>) -> Self {
        Self { inner, counters }
    }
}

impl fmt::Display for PeakTrackingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl MemoryPool for PeakTrackingPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.counters.add_reserved_bytes(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.counters.sub_reserved_bytes(shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        // Recorded only on success: a refused reservation was never held.
        self.inner.try_grow(reservation, additional)?;
        self.counters.add_reserved_bytes(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};

    #[test]
    fn records_the_peak_and_delegates_the_limit() {
        let counters = Arc::new(QueryCounters::new());
        let pool: Arc<dyn MemoryPool> = Arc::new(PeakTrackingPool::new(
            Arc::new(GreedyMemoryPool::new(1_000)),
            Arc::clone(&counters),
        ));
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1_000)));

        let a = MemoryConsumer::new("a").register(&pool);
        let b = MemoryConsumer::new("b").register(&pool);
        a.try_grow(600).unwrap();
        b.try_grow(300).unwrap();
        // Refused by the inner pool, and therefore not counted.
        assert!(b.try_grow(500).is_err());
        a.shrink(500);
        b.grow(200);
        assert_eq!(pool.reserved(), 600);
        assert_eq!(counters.peak_reserved_bytes(), 900);

        drop(a);
        drop(b);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn two_pools_sharing_counters_report_their_combined_peak() {
        // A query's sub-plans build their own pools; what the query holds is
        // the sum across them, not either pool's own figure.
        let counters = Arc::new(QueryCounters::new());
        let outer: Arc<dyn MemoryPool> = Arc::new(PeakTrackingPool::new(
            Arc::new(UnboundedMemoryPool::default()),
            Arc::clone(&counters),
        ));
        let inner: Arc<dyn MemoryPool> = Arc::new(PeakTrackingPool::new(
            Arc::new(UnboundedMemoryPool::default()),
            Arc::clone(&counters),
        ));
        let x = MemoryConsumer::new("x").register(&outer);
        let y = MemoryConsumer::new("y").register(&inner);
        x.grow(400);
        y.grow(300);
        y.free();
        x.grow(100);
        assert_eq!(counters.peak_reserved_bytes(), 700);
    }
}
