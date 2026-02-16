// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod api {
    pub mod error;
}

pub mod config;
pub mod cypher_value_codec;
pub mod sync;
pub mod value;

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
#[doc(inline)]
pub use value::{Edge, FromValue, Node, Path, TemporalType, TemporalValue, Value};

/// String-keyed property map using [`Value`] for type-preserving storage.
pub type Properties = std::collections::HashMap<String, Value>;
