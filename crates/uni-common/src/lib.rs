// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod api {
    pub mod error;
}

pub mod config;
pub mod cypher_value_codec;
pub mod datetime;
pub mod muvera;
pub mod sync;
pub mod value;
pub mod vector_index_opts;

pub mod core {
    pub mod check_constraint;
    pub mod edge_type;
    pub mod fork;
    pub mod id;
    pub mod schema;
    pub mod snapshot;
}

pub mod graph {
    pub mod simple_graph;
}

// Re-exports for convenience
pub use api::error::{
    GRAPH_COMPUTE_INCOMPLETE_TAG, GraphComputeIncomplete, GraphComputeIncompleteReason,
    LocyIncomplete, LocyIncompleteReason, Result, UniError,
};
pub use config::{CloudStorageConfig, UniConfig};
pub use core::edge_type::EdgeTypeId;
pub use core::fork::{ForkId, ForkInfo, ForkRegistryFile, ForkStatus, SchemaDelta};
pub use core::id::{Eid, UniId, Vid};
pub use core::schema::{CrdtType, DataType, Schema};
pub use graph::simple_graph::SimpleGraph;
pub use uni_btic;
pub use uni_sparse_vector;
#[doc(inline)]
pub use value::{Edge, FromValue, Node, Path, TemporalType, TemporalValue, Value, cmp_i64_f64};

/// String-keyed property map using [`Value`] for type-preserving storage.
pub type Properties = std::collections::HashMap<String, Value>;
