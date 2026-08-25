// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod context;
pub mod counters;
/// Embedding-capability mapping (task→heads) and per-alias head requirements,
/// shared by open-time validation and write-time auto-embed routing.
pub mod embed_caps;
pub mod flush_coordinator;
pub mod id_allocator;
pub mod id_reservoir;
pub mod l0;
pub mod l0_manager;
pub mod l0_visibility;
pub mod occ;
pub mod property_manager;
/// Concurrency-primitive shim: aliases to `std` normally, `loom`/`shuttle` under
/// their features, so the OCC commit core can be model-checked. See `sync.rs`.
pub(crate) mod sync;
pub mod vid_remapper;
pub mod wal;
pub mod working_graph;
pub mod writer;

pub use l0::L0Buffer;
pub use l0_manager::L0Manager;
// Threads through the executor as `Option<SnapshotView>`; only ever constructed
// when a transaction pins a snapshot (`UniConfig::ssi_enabled`, default on).
// See `L0Manager::pin_snapshot`.
pub use l0_manager::SnapshotView;
pub use property_manager::PropertyManager;
pub use vid_remapper::{EidRemapper, VidRemapper};
// Re-export SimpleGraph from uni-common
pub use context::QueryContext;
pub use counters::QueryCounters;
pub use id_reservoir::{DEFAULT_RESERVOIR_BATCH, TxIdReservoir};
pub use uni_common::graph::simple_graph::{Direction, SimpleGraph};
pub use wal::WriteAheadLog;
pub use working_graph::WorkingGraph;
pub use writer::{ForkPoint, Writer};

use uni_common::core::id::{Eid, Vid};

/// Vertex data - TOPOLOGY ONLY
#[derive(Clone, Copy, Debug)]
pub struct VertexData {
    pub vid: Vid,
}

/// Edge data - TOPOLOGY ONLY
#[derive(Clone, Copy, Debug)]
pub struct EdgeData {
    pub eid: Eid,
    pub edge_type: u32,
}
