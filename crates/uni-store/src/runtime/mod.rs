// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod context;
pub mod id_allocator;
pub mod l0;
pub mod l0_manager;
pub mod l0_visibility;
pub mod property_manager;
pub mod vid_remapper;
pub mod wal;
pub mod working_graph;
pub mod writer;

pub use l0::L0Buffer;
pub use l0_manager::L0Manager;
pub use property_manager::PropertyManager;
pub use vid_remapper::{EidRemapper, VidRemapper};
// Re-export SimpleGraph from uni-common
pub use context::QueryContext;
pub use uni_common::graph::simple_graph::{Direction, SimpleGraph};
pub use wal::WriteAheadLog;
pub use working_graph::WorkingGraph;
pub use writer::Writer;

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
