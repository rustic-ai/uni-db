// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::l0::L0Buffer;
use crate::runtime::wal::WriteAheadLog;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct L0Manager {
    // The current active L0 buffer.
    // Outer RwLock protects the Arc (swapping L0s).
    // Inner RwLock protects the L0Buffer content (concurrent reads/writes).
    current: RwLock<Arc<RwLock<L0Buffer>>>,
    // L0 buffers currently being flushed to L1.
    // These remain visible to reads until flush completes successfully.
    // This prevents data loss if L1 writes fail after rotation.
    pending_flush: RwLock<Vec<Arc<RwLock<L0Buffer>>>>,
}

impl L0Manager {
    pub fn new(start_version: u64, wal: Option<Arc<WriteAheadLog>>) -> Self {
        let l0 = L0Buffer::new(start_version, wal);
        Self {
            current: RwLock::new(Arc::new(RwLock::new(l0))),
            pending_flush: RwLock::new(Vec::new()),
        }
    }

    /// Get the current L0 buffer.
    pub fn get_current(&self) -> Arc<RwLock<L0Buffer>> {
        self.current.read().clone()
    }

    /// Get all L0 buffers that should be visible to reads.
    /// This includes the current L0 plus any L0s being flushed.
    pub fn get_all_readable(&self) -> Vec<Arc<RwLock<L0Buffer>>> {
        let current = self.get_current();
        let pending = self.pending_flush.read().clone();
        let mut all = vec![current];
        all.extend(pending);
        all
    }

    /// Get L0 buffers currently being flushed (for QueryContext).
    pub fn get_pending_flush(&self) -> Vec<Arc<RwLock<L0Buffer>>> {
        self.pending_flush.read().clone()
    }

    /// Rotate L0. Returns the OLD L0 buffer.
    /// The new L0 is initialized with `next_version` and `new_wal`.
    pub fn rotate(
        &self,
        next_version: u64,
        new_wal: Option<Arc<WriteAheadLog>>,
    ) -> Arc<RwLock<L0Buffer>> {
        let mut guard = self.current.write();
        let old_l0 = guard.clone();

        let new_l0 = L0Buffer::new(next_version, new_wal);
        *guard = Arc::new(RwLock::new(new_l0));

        old_l0
    }

    /// Begin flush: rotate L0 and add old L0 to pending flush list.
    /// The old L0 remains visible to reads until `complete_flush` is called.
    /// Returns the old L0 buffer to be flushed.
    pub fn begin_flush(
        &self,
        next_version: u64,
        new_wal: Option<Arc<WriteAheadLog>>,
    ) -> Arc<RwLock<L0Buffer>> {
        let old_l0 = self.rotate(next_version, new_wal);
        self.pending_flush.write().push(old_l0.clone());
        old_l0
    }

    /// Complete flush: remove the flushed L0 from pending list.
    /// Call this only after L1 writes have succeeded.
    pub fn complete_flush(&self, l0: &Arc<RwLock<L0Buffer>>) {
        let mut pending = self.pending_flush.write();
        pending.retain(|x| !Arc::ptr_eq(x, l0));
    }

    /// Get the minimum WAL LSN across all pending flush L0s.
    /// WAL truncation should not go past this LSN to preserve data for pending flushes.
    /// Returns None if no pending flushes exist.
    pub fn min_pending_wal_lsn(&self) -> Option<u64> {
        let pending = self.pending_flush.read();
        if pending.is_empty() {
            return None;
        }
        pending
            .iter()
            .map(|l0_arc| {
                let l0 = l0_arc.read();
                l0.wal_lsn_at_flush
            })
            .min()
    }
}
