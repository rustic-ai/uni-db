// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! # Uni - Embedded Graph Database
//!
//! Uni is an embedded, object-store-backed graph database with OpenCypher queries,
//! columnar analytics, and vector search.

pub mod api;

pub use api::builder::PropertiesBuilder;
pub use api::schema::{IndexType, ScalarType, VectorAlgo, VectorIndexCfg, VectorMetric};
pub use api::sync::UniSync;
pub use api::transaction::Transaction;
pub use api::{Uni, UniBuilder};

// Re-exports from internal crates
pub use uni_common::{CrdtType, DataType, Eid, Result, Schema, UniConfig, UniError, UniId, Vid};
pub use uni_query::{
    Edge, ExecuteResult, ExplainOutput, FromValue, Node, Path, ProfileOutput, QueryResult, Row,
    Value,
};

#[cfg(feature = "storage-internals")]
pub use uni_store::storage::StorageManager;

#[cfg(feature = "snapshot-internals")]
pub use uni_common::core::snapshot::SnapshotManifest;
#[cfg(feature = "snapshot-internals")]
pub use uni_store::snapshot::manager::SnapshotManager;

// Re-export crates
pub use uni_algo as algo_crate;
pub use uni_common as common;
pub use uni_query as query_crate;
pub use uni_store as store;

// Module aliases for internal crate access
pub mod core {
    pub use crate::common::core::*;
}

pub mod storage {
    pub use crate::store::storage::*;
    // Fix for tests expecting IndexManager in storage root or similar?
    // tests use uni_db::storage::manager::StorageManager.
    // crate::store::storage has manager.
}

pub mod runtime {
    pub use crate::store::runtime::*;
}

pub mod query {
    pub use crate::query_crate::query::*;
}

pub mod algo {
    // Tests use uni_db::algo::* (from src/algo).
    // uni-algo has `algo` module.
    pub use crate::algo_crate::algo::*;
}
