// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod backend;
pub mod cloud;
pub mod compaction;
pub mod fork;
pub mod runtime;
pub mod storage;
pub mod store_utils;
pub mod snapshot {
    pub mod manager;
}

pub use backend::StorageBackend;
#[cfg(feature = "lance-backend")]
pub use backend::lance::LanceDbBackend;
pub use backend::types::VectorQueryOpts;
pub use compaction::{CompactionStats, CompactionStatus};
pub use runtime::context::QueryContext;
pub use runtime::counters::QueryCounters;
pub use runtime::property_manager::PropertyManager;
pub use runtime::writer::{ForkPoint, Writer};
pub use snapshot::manager::SnapshotManager;
pub use storage::manager::collect_l0_label_candidates;

/// loom/shuttle model-checking harness for the OCC commit core. Compiled only
/// under `--features loom` / `--features shuttle`; see `runtime/sync.rs`.
#[cfg(any(feature = "loom", feature = "shuttle"))]
pub mod occ_loom_model;
