// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host traits that invert the dependency between the fork diff/promote
//! engine and uni-db.
//!
//! The diff/promote logic needs to run Cypher queries against a session, read
//! the storage manager's UID indexes, and bulk-insert promoted rows into a
//! primary transaction. All of those are uni-db types, so the engine is made
//! generic over these traits and uni-db supplies the implementations
//! (`Session: ForkQueryHost`, `Transaction: ForkPromoteSink`).

use std::sync::Arc;

use uni_common::Properties;
use uni_common::Result;
use uni_common::core::id::Vid;
use uni_store::storage::manager::StorageManager;

/// Read-side host for the diff engine and promote scans.
///
/// Implemented by `uni_db::Session`.
#[async_trait::async_trait]
pub trait ForkQueryHost: Send + Sync {
    /// Run a read-only Cypher query and return its result.
    async fn query(&self, cypher: &str) -> Result<uni_query::QueryResult>;

    /// The session's storage manager (used to read the shared UID index).
    fn storage(&self) -> Arc<StorageManager>;

    /// The session's schema manager (labels + edge types to diff over).
    fn schema(&self) -> Arc<uni_common::core::schema::SchemaManager>;
}

/// Write-side host for `run_promote` — the primary-targeted transaction
/// that promoted rows are bulk-inserted into.
///
/// Implemented by `uni_db::Transaction`. The edge sink takes
/// `(src_vid, dst_vid, properties)` tuples (not `uni_bulk::EdgeData`), so
/// uni-fork does not need to depend on uni-bulk.
#[async_trait::async_trait]
pub trait ForkPromoteSink: Send + Sync {
    /// Bulk-insert promoted vertices; returns the allocated VIDs in order.
    async fn bulk_insert_vertices(&self, label: &str, rows: Vec<Properties>) -> Result<Vec<Vid>>;

    /// Overwrite an existing primary vertex's properties in place, keeping
    /// its VID. Used by the upsert path when a fork edit resolves to an
    /// existing primary vertex by `(label, ext_id)`.
    async fn update_vertex_properties(
        &self,
        label: &str,
        vid: Vid,
        props: Properties,
    ) -> Result<()>;

    /// Called once before a batch of [`Self::update_vertex_properties`]
    /// calls with every vid the batch may update, so the sink can read what
    /// the updates need (each vertex's stored `created_at`) in one go
    /// instead of once per update. The default does nothing.
    async fn prepare_vertex_updates(&self, vids: &[Vid]) -> Result<()> {
        let _ = vids;
        Ok(())
    }

    /// Soft-delete an existing primary vertex. Used by delete-promotion
    /// when a vertex present at the fork point was removed on the fork.
    async fn delete_vertex(&self, label: &str, vid: Vid) -> Result<()>;

    /// Bulk-insert promoted edges between resolved primary endpoints.
    async fn bulk_insert_edges(
        &self,
        edge_type: &str,
        edges: Vec<(Vid, Vid, Properties)>,
    ) -> Result<()>;
}
