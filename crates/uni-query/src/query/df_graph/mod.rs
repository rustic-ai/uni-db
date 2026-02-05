// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Custom graph operators for DataFusion execution.
//!
//! This module provides DataFusion `ExecutionPlan` implementations for graph-specific
//! operations that cannot be expressed in standard relational algebra:
//!
//! - [`GraphScanExec`]: Scans vertices/edges with property materialization
//! - [`GraphExtIdLookupExec`]: Looks up a vertex by external ID
//! - [`GraphTraverseExec`]: Single-hop edge traversal using CSR adjacency
//! - `GraphVariableLengthTraverseExec`: Multi-hop BFS traversal
//! - [`GraphShortestPathExec`]: Shortest path computation
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │      DataFusion ExecutionPlan Tree      │
//! ├─────────────────────────────────────────┤
//! │  ProjectionExec (DataFusion)            │
//! │       │                                 │
//! │  FilterExec (DataFusion)                │
//! │       │                                 │
//! │  GraphTraverseExec (CUSTOM)             │
//! │       │                                 │
//! │  GraphScanExec (CUSTOM)                 │
//! │       │                                 │
//! │  UniTableProvider + UniMergeExec        │
//! └─────────────────────────────────────────┘
//! ```
//!
//! Graph operators use [`GraphExecutionContext`] to access:
//! - AdjacencyManager for O(1) neighbor lookups
//! - L0 buffers for uncommitted edge visibility
//! - Property manager for lazy property loading

pub mod ext_id_lookup;
pub mod scan;
pub mod shortest_path;
pub mod traverse;
pub mod unwind;
pub mod vector_knn;

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Instant;
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::adjacency_manager::AdjacencyManager;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

pub use ext_id_lookup::GraphExtIdLookupExec;
pub use scan::GraphScanExec;
pub use shortest_path::GraphShortestPathExec;
pub use traverse::{GraphTraverseExec, GraphTraverseMainExec};
pub use unwind::GraphUnwindExec;
pub use vector_knn::GraphVectorKnnExec;

/// Shared context for graph operators.
///
/// Provides access to graph-specific resources needed during query execution:
/// - CSR adjacency cache for fast neighbor lookups
/// - L0 buffers for MVCC visibility of uncommitted changes
/// - Property manager for lazy-loading vertex/edge properties
/// - Storage manager for schema and dataset access
///
/// # Example
///
/// ```ignore
/// let ctx = GraphExecutionContext::new(
///     storage_manager,
///     l0_buffer,
///     property_manager,
/// );
///
/// // Get neighbors with L0 overlay
/// let neighbors = ctx.get_neighbors(vid, edge_type_id, Direction::Outgoing);
/// ```
pub struct GraphExecutionContext {
    /// Storage manager for schema and dataset access.
    storage: Arc<StorageManager>,

    /// L0 visibility context for MVCC.
    l0_context: L0Context,

    /// Property manager for lazy property loading.
    property_manager: Arc<PropertyManager>,

    /// Query timeout deadline.
    deadline: Option<Instant>,
}

impl std::fmt::Debug for GraphExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphExecutionContext")
            .field("l0_context", &self.l0_context)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// L0 buffer visibility context for MVCC reads.
///
/// Maintains references to all L0 buffers that should be visible to a query:
/// - Current L0: The active write buffer
/// - Transaction L0: Buffer for the current transaction (if any)
/// - Pending flush L0s: Buffers being flushed to disk (still visible to reads)
///
/// The visibility order is: pending flush L0s (oldest first) → current L0 → transaction L0.
#[derive(Clone)]
pub struct L0Context {
    /// Current active L0 buffer.
    pub current_l0: Option<Arc<RwLock<L0Buffer>>>,

    /// Transaction-local L0 buffer (if in a transaction).
    pub transaction_l0: Option<Arc<RwLock<L0Buffer>>>,

    /// L0 buffers pending flush to disk.
    /// These remain visible until flush completes.
    pub pending_flush_l0s: Vec<Arc<RwLock<L0Buffer>>>,
}

impl std::fmt::Debug for L0Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0Context")
            .field("current_l0", &self.current_l0.is_some())
            .field("transaction_l0", &self.transaction_l0.is_some())
            .field("pending_flush_l0s_count", &self.pending_flush_l0s.len())
            .finish()
    }
}

impl L0Context {
    /// Create an empty L0 context with no buffers.
    pub fn empty() -> Self {
        Self {
            current_l0: None,
            transaction_l0: None,
            pending_flush_l0s: Vec::new(),
        }
    }

    /// Create L0 context with just a current buffer.
    pub fn with_current(l0: Arc<RwLock<L0Buffer>>) -> Self {
        Self {
            current_l0: Some(l0),
            transaction_l0: None,
            pending_flush_l0s: Vec::new(),
        }
    }

    /// Create L0 context from a query context.
    pub fn from_query_context(ctx: &QueryContext) -> Self {
        Self {
            current_l0: Some(ctx.l0.clone()),
            transaction_l0: ctx.transaction_l0.clone(),
            pending_flush_l0s: ctx.pending_flush_l0s.clone(),
        }
    }

    /// Iterate over all L0 buffers in visibility order.
    /// Order: pending flush L0s (oldest first), then current L0, then transaction L0.
    pub fn iter_l0_buffers(&self) -> impl Iterator<Item = &Arc<RwLock<L0Buffer>>> {
        self.pending_flush_l0s
            .iter()
            .chain(self.current_l0.iter())
            .chain(self.transaction_l0.iter())
    }
}

impl GraphExecutionContext {
    /// Create a new graph execution context.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage manager for schema and dataset access
    /// * `l0` - Current L0 buffer for MVCC visibility
    /// * `property_manager` - Manager for lazy property loading
    pub fn new(
        storage: Arc<StorageManager>,
        l0: Arc<RwLock<L0Buffer>>,
        property_manager: Arc<PropertyManager>,
    ) -> Self {
        Self {
            storage,
            l0_context: L0Context::with_current(l0),
            property_manager,
            deadline: None,
        }
    }

    /// Create context with full L0 visibility.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage manager for schema and dataset access
    /// * `l0_context` - L0 visibility context with all buffers
    /// * `property_manager` - Manager for lazy property loading
    pub fn with_l0_context(
        storage: Arc<StorageManager>,
        l0_context: L0Context,
        property_manager: Arc<PropertyManager>,
    ) -> Self {
        Self {
            storage,
            l0_context,
            property_manager,
            deadline: None,
        }
    }

    /// Create context from a query context.
    pub fn from_query_context(
        storage: Arc<StorageManager>,
        query_ctx: &QueryContext,
        property_manager: Arc<PropertyManager>,
    ) -> Self {
        Self {
            storage,
            l0_context: L0Context::from_query_context(query_ctx),
            property_manager,
            deadline: query_ctx.deadline,
        }
    }

    /// Set query timeout deadline.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Check if the query has timed out.
    ///
    /// # Errors
    ///
    /// Returns an error if the deadline has passed.
    pub fn check_timeout(&self) -> anyhow::Result<()> {
        if let Some(deadline) = self.deadline
            && Instant::now() > deadline
        {
            return Err(anyhow::anyhow!("Query timed out"));
        }
        Ok(())
    }

    /// Get a reference to the storage manager.
    pub fn storage(&self) -> &Arc<StorageManager> {
        &self.storage
    }

    /// Get a reference to the adjacency manager.
    pub fn adjacency_manager(&self) -> Arc<AdjacencyManager> {
        self.storage.adjacency_manager()
    }

    /// Get a reference to the property manager.
    pub fn property_manager(&self) -> &Arc<PropertyManager> {
        &self.property_manager
    }

    /// Get a reference to the L0 context.
    pub fn l0_context(&self) -> &L0Context {
        &self.l0_context
    }

    /// Create a query context for property manager calls.
    ///
    /// If there is no current L0 buffer (e.g., for snapshot queries), creates an empty one.
    pub fn query_context(&self) -> QueryContext {
        let l0 = self
            .l0_context
            .current_l0
            .clone()
            .unwrap_or_else(|| Arc::new(RwLock::new(L0Buffer::new(0, None))));

        QueryContext {
            l0,
            transaction_l0: self.l0_context.transaction_l0.clone(),
            pending_flush_l0s: self.l0_context.pending_flush_l0s.clone(),
            deadline: self.deadline,
        }
    }

    /// Ensure adjacency CSRs are warmed for the given edge types and direction.
    ///
    /// This loads any missing CSR data from storage into the adjacency manager
    /// so that subsequent `get_neighbors` calls return complete results.
    /// Skips warming if the adjacency manager already has data (Main CSR or
    /// active overlay) for the edge type, avoiding duplicate entries.
    pub async fn ensure_adjacency_warmed(
        &self,
        edge_type_ids: &[u16],
        direction: Direction,
    ) -> anyhow::Result<()> {
        let am = self.adjacency_manager();
        let version = self.storage.version_high_water_mark();
        for &etype_id in edge_type_ids {
            // Skip if AM already has data (CSR or overlay) for this edge type.
            // The overlay contains edges from dual-write (Writer), so warming
            // would duplicate them.
            if !am.is_active_for(etype_id, direction) {
                for &dir in direction.expand() {
                    self.storage.warm_adjacency(etype_id, dir, version).await?;
                }
            }
        }
        Ok(())
    }

    /// Create a boxed warming future for use in DataFusion stream state machines.
    ///
    /// Wraps `ensure_adjacency_warmed` into a `Pin<Box<dyn Future<Output = DFResult<()>> + Send>>`
    /// suitable for polling in stream `poll_next` implementations.
    pub fn warming_future(
        self: &Arc<Self>,
        edge_type_ids: Vec<u16>,
        direction: Direction,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = datafusion::common::Result<()>> + Send>>
    {
        let ctx = self.clone();
        Box::pin(async move {
            ctx.ensure_adjacency_warmed(&edge_type_ids, direction)
                .await
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        })
    }

    /// Get neighbors for a vertex, merging CSR and all L0 buffers.
    ///
    /// This implements the MVCC visibility rules:
    /// 1. Load from CSR (L2 + L1 merged, auto-warms on cache miss)
    /// 2. Overlay pending flush L0s (oldest to newest)
    /// 3. Overlay current L0
    /// 4. Overlay transaction L0 (if present)
    /// 5. Filter tombstones (handled by overlay)
    ///
    /// # Arguments
    ///
    /// * `vid` - Source vertex ID
    /// * `edge_type` - Edge type ID to traverse
    /// * `direction` - Traversal direction (Outgoing, Incoming, or Both)
    ///
    /// # Returns
    ///
    /// Vector of (neighbor VID, edge ID) pairs.
    pub fn get_neighbors(&self, vid: Vid, edge_type: u16, direction: Direction) -> Vec<(Vid, Eid)> {
        let am = self.adjacency_manager();
        let version_hwm = self.storage.version_high_water_mark();

        // Use AdjacencyManager which reads Main CSR + overlay (dual-write).
        // For snapshot queries, filter by version via StorageManager delegate.
        let mut neighbors = if let Some(hwm) = version_hwm {
            self.storage
                .get_neighbors_at_version(vid, edge_type, direction, hwm)
        } else {
            am.get_neighbors(vid, edge_type, direction)
        };

        // Overlay transaction L0 if present (transaction edges bypass Writer/AM).
        if version_hwm.is_none()
            && let Some(tx_l0) = &self.l0_context.transaction_l0
        {
            let tx_guard = tx_l0.read();
            overlay_l0_neighbors(
                vid,
                edge_type,
                direction,
                &tx_guard,
                &mut neighbors,
                version_hwm,
            );
        }

        neighbors
    }

    /// Get neighbors for multiple vertices in batch.
    ///
    /// More efficient than calling `get_neighbors` repeatedly as it amortizes
    /// lock acquisition for L0 buffers.
    ///
    /// # Arguments
    ///
    /// * `vids` - Source vertex IDs
    /// * `edge_type` - Edge type ID to traverse
    /// * `direction` - Traversal direction
    ///
    /// # Returns
    ///
    /// Vector of (source VID, neighbor VID, edge ID) triples.
    pub fn get_neighbors_batch(
        &self,
        vids: &[Vid],
        edge_type: u16,
        direction: Direction,
    ) -> Vec<(Vid, Vid, Eid)> {
        let am = self.adjacency_manager();
        let version_hwm = self.storage.version_high_water_mark();

        let tx_guard = self.l0_context.transaction_l0.as_ref().map(|l0| l0.read());

        let mut results = Vec::new();

        for &vid in vids {
            let mut neighbors = if let Some(hwm) = version_hwm {
                self.storage
                    .get_neighbors_at_version(vid, edge_type, direction, hwm)
            } else {
                am.get_neighbors(vid, edge_type, direction)
            };

            // Overlay transaction L0 if present
            if version_hwm.is_none()
                && let Some(ref tx_guard) = tx_guard
            {
                overlay_l0_neighbors(
                    vid,
                    edge_type,
                    direction,
                    tx_guard,
                    &mut neighbors,
                    version_hwm,
                );
            }

            for (neighbor, eid) in neighbors {
                results.push((vid, neighbor, eid));
            }
        }

        results
    }
}

/// Overlay L0 buffer neighbors onto existing neighbor list.
///
/// Adds new edges from L0 and removes tombstoned edges.
/// Filters by version if a snapshot boundary is provided.
fn overlay_l0_neighbors(
    vid: Vid,
    edge_type: u16,
    direction: Direction,
    l0: &L0Buffer,
    neighbors: &mut Vec<(Vid, Eid)>,
    version_hwm: Option<u64>,
) {
    use std::collections::HashMap;
    use uni_common::graph::simple_graph::Direction as SimpleDirection;

    // Convert to map for efficient updates
    let mut neighbor_map: HashMap<Eid, Vid> = neighbors.drain(..).map(|(v, e)| (e, v)).collect();

    // Determine which directions to query from L0
    let directions: &[SimpleDirection] = match direction {
        Direction::Outgoing => &[SimpleDirection::Outgoing],
        Direction::Incoming => &[SimpleDirection::Incoming],
        Direction::Both => &[SimpleDirection::Outgoing, SimpleDirection::Incoming],
    };

    // Get L0 neighbors for each direction
    for &simple_dir in directions {
        for (neighbor, eid, version) in l0.get_neighbors(vid, edge_type, simple_dir) {
            // Skip edges beyond snapshot boundary
            if version_hwm.is_some_and(|hwm| version > hwm) {
                continue;
            }

            // Apply insert or check tombstone
            if l0.is_tombstoned(eid) {
                neighbor_map.remove(&eid);
            } else {
                neighbor_map.insert(eid, neighbor);
            }
        }
    }

    // Convert back to vec
    *neighbors = neighbor_map.into_iter().map(|(e, v)| (v, e)).collect();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l0_context_empty() {
        let ctx = L0Context::empty();
        assert!(ctx.current_l0.is_none());
        assert!(ctx.transaction_l0.is_none());
        assert!(ctx.pending_flush_l0s.is_empty());
    }
}
