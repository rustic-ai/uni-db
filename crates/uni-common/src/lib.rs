// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod api {
    pub mod error;
}

pub mod config;
pub mod sync;

pub mod core {
    pub mod edge_type;
    pub mod id;
    pub mod schema;
    pub mod snapshot;
}

pub mod graph {
    pub mod simple_graph;
}

// Re-exports for convenience
pub use api::error::{Result, UniError};
pub use config::{CloudStorageConfig, UniConfig};
pub use core::edge_type::EdgeTypeId;
pub use core::id::{Eid, UniId, Vid};
pub use core::schema::{CrdtType, DataType, Schema};
pub use graph::simple_graph::SimpleGraph;

pub type Properties = std::collections::HashMap<String, serde_json::Value>;
