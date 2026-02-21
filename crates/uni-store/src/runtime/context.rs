// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::l0::L0Buffer;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct QueryContext {
    pub l0: Arc<RwLock<L0Buffer>>,
    pub transaction_l0: Option<Arc<RwLock<L0Buffer>>>,
    /// L0 buffers currently being flushed to L1.
    /// These remain visible to reads until flush completes successfully.
    pub pending_flush_l0s: Vec<Arc<RwLock<L0Buffer>>>,
    pub deadline: Option<Instant>,
}

impl QueryContext {
    pub fn new(l0: Arc<RwLock<L0Buffer>>) -> Self {
        Self {
            l0,
            transaction_l0: None,
            pending_flush_l0s: Vec::new(),
            deadline: None,
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
        }
    }

    pub fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = Some(deadline);
    }

    pub fn check_timeout(&self) -> anyhow::Result<()> {
        if let Some(deadline) = self.deadline
            && Instant::now() > deadline
        {
            return Err(anyhow::anyhow!("Query timed out"));
        }
        Ok(())
    }
}
