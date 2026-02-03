// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::time::{Duration, SystemTime};

#[derive(Debug, Clone, Copy)]
pub enum CompactionTask {
    ByRunCount,
    BySize,
    ByAge,
}

#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub files_compacted: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub duration: Duration,
    pub crdt_merges: usize,
}

#[derive(Debug, Clone)]
pub struct CompactionStatus {
    pub l1_runs: usize,
    pub l1_size_bytes: u64,
    pub oldest_l1_age: Duration,
    pub compaction_in_progress: bool,
    pub compaction_pending: usize,
    pub last_compaction: Option<SystemTime>,
    pub total_compactions: u64,
    pub total_bytes_compacted: u64,
}

impl Default for CompactionStatus {
    fn default() -> Self {
        Self {
            l1_runs: 0,
            l1_size_bytes: 0,
            oldest_l1_age: Duration::from_secs(0),
            compaction_in_progress: false,
            compaction_pending: 0,
            last_compaction: None,
            total_compactions: 0,
            total_bytes_compacted: 0,
        }
    }
}
