// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Pluggable storage backend abstraction.
//!
//! This module defines the [`StorageBackend`] trait that all storage backends
//! must implement, along with supporting types for queries, writes, and
//! index management.

pub mod branched;
pub mod branching;
pub mod capabilities;
pub mod eval;
#[cfg(feature = "lance-backend")]
pub mod fts_analyzer;
#[cfg(feature = "lance-backend")]
pub mod lance;
#[cfg(feature = "lance-backend")]
pub mod lance_branch;
#[cfg(feature = "lance-backend")]
pub mod lance_directory;
#[cfg(feature = "lance-backend")]
pub(crate) mod lance_fts;
#[cfg(feature = "lance-backend")]
pub(crate) use lance_fts::score_documents;

/// Refuse scored search when no BM25 implementation is compiled in.
#[cfg(not(feature = "lance-backend"))]
pub(crate) async fn score_documents(
    _column: &str,
    _query: &str,
    _docs: Vec<(u64, String)>,
    check: impl Fn() -> anyhow::Result<()> + Clone + Send + Sync + 'static,
) -> anyhow::Result<Vec<(u64, f32)>> {
    check()?;
    anyhow::bail!("Schemaless full-text scoring requires BM25 support")
}
pub mod table_names;
pub mod traits;
pub mod types;

pub use branching::ForkBranching;
pub use capabilities::{FullTextSearchCapability, ScalarIndexCapability, VectorSearchCapability};
pub use traits::StorageBackend;
pub use types::*;
