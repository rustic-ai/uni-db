// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shadow CSR for time-travel deleted edge tracking.
//!
//! Stores edges that have been deleted from the Main CSR along with
//! their version lifecycle (created_version, deleted_version). Only
//! queried during snapshot/time-travel reads — never on the hot path.

use crate::storage::direction::Direction;
use dashmap::DashMap;
use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};

/// A deleted edge with version range for time-travel reconstruction.
#[derive(Clone, Debug)]
pub struct ShadowEdge {
    /// Neighbor vertex ID.
    pub neighbor_vid: Vid,
    /// Edge ID.
    pub eid: Eid,
    /// Edge type ID (bit 31 = 0 for schema'd, 1 for schemaless).
    pub edge_type: u32,
    /// Version at which this edge was created.
    pub created_version: u64,
    /// Version at which this edge was deleted.
    pub deleted_version: u64,
}

/// Shadow CSR storing deleted edges with their version lifecycle.
///
/// Only queried during snapshot/time-travel reads. Uses `HashMap`
/// rather than packed CSR because deleted edges are typically few,
/// append-heavy, and never on the regular query hot path.
pub struct ShadowCsr {
    /// `(edge_type, direction) -> vid -> Vec<ShadowEdge>`.
    /// Edge type is u32 with bit 31 = 0 for schema'd, 1 for schemaless.
    entries: DashMap<(u32, Direction), HashMap<Vid, Vec<ShadowEdge>>>,
}

impl ShadowCsr {
    /// Creates an empty shadow CSR.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Records a deleted edge with its version lifecycle.
    pub fn add_deleted_edge(&self, src_vid: Vid, edge: ShadowEdge, direction: Direction) {
        self.entries
            .entry((edge.edge_type, direction))
            .or_default()
            .entry(src_vid)
            .or_default()
            .push(edge);
    }

    /// Returns edges that were alive at the given `version`.
    ///
    /// An edge is considered alive at `version` when
    /// `created_version <= version < deleted_version`.
    pub fn get_entries_at_version(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
        version: u64,
    ) -> Vec<(Vid, Eid)> {
        let mut result = Vec::new();

        if let Some(map) = self.entries.get(&(edge_type, direction))
            && let Some(edges) = map.get(&vid)
        {
            for edge in edges {
                if edge.created_version <= version && edge.deleted_version > version {
                    result.push((edge.neighbor_vid, edge.eid));
                }
            }
        }

        result
    }

    /// Returns raw shadow entries for a vertex (all versions).
    pub fn get_entries(&self, vid: Vid, edge_type: u32, direction: Direction) -> Vec<ShadowEdge> {
        if let Some(map) = self.entries.get(&(edge_type, direction))
            && let Some(edges) = map.get(&vid)
        {
            return edges.clone();
        }
        Vec::new()
    }

    /// Garbage-collects shadow entries no longer needed.
    ///
    /// Removes entries where `deleted_version <= oldest_active_snapshot_version`,
    /// since no active snapshot can reference those edges.
    pub fn gc(&self, oldest_active_snapshot_version: u64) {
        for mut entry in self.entries.iter_mut() {
            let map = entry.value_mut();
            for edge_list in map.values_mut() {
                edge_list.retain(|e| e.deleted_version > oldest_active_snapshot_version);
            }
            // Remove empty vertex entries
            map.retain(|_, edges| !edges.is_empty());
        }
    }
}

impl Default for ShadowCsr {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShadowCsr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total_edges: usize = self
            .entries
            .iter()
            .map(|e| e.value().values().map(|v| v.len()).sum::<usize>())
            .sum();
        f.debug_struct("ShadowCsr")
            .field("buckets", &self.entries.len())
            .field("total_edges", &total_edges)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_shadow_edge(neighbor: u64, eid: u64, created: u64, deleted: u64) -> ShadowEdge {
        ShadowEdge {
            neighbor_vid: Vid::new(neighbor),
            eid: Eid::new(eid),
            edge_type: 1,
            created_version: created,
            deleted_version: deleted,
        }
    }

    #[test]
    fn test_add_and_query() {
        let shadow = ShadowCsr::new();
        let src = Vid::new(1);

        // Edge created at v1, deleted at v5
        shadow.add_deleted_edge(src, make_shadow_edge(2, 100, 1, 5), Direction::Outgoing);

        // At v3 the edge should be visible
        let result = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 3);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], (Vid::new(2), Eid::new(100)));

        // At v5 the edge is deleted
        let result = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 5);
        assert!(result.is_empty());

        // At v0 the edge doesn't exist yet
        let result = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_gc_removes_old_entries() {
        let shadow = ShadowCsr::new();
        let src = Vid::new(1);

        shadow.add_deleted_edge(src, make_shadow_edge(2, 100, 1, 3), Direction::Outgoing);
        shadow.add_deleted_edge(src, make_shadow_edge(3, 101, 2, 10), Direction::Outgoing);

        // GC with oldest snapshot at v5 — first entry (deleted_version=3) gets removed
        shadow.gc(5);

        let entries = shadow.get_entries(src, 1, Direction::Outgoing);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].eid, Eid::new(101));
    }

    #[test]
    fn test_empty_shadow() {
        let shadow = ShadowCsr::new();
        let result = shadow.get_entries_at_version(Vid::new(0), 1, Direction::Outgoing, 5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_multiple_edges_same_vertex() {
        let shadow = ShadowCsr::new();
        let src = Vid::new(1);

        shadow.add_deleted_edge(src, make_shadow_edge(2, 100, 1, 5), Direction::Outgoing);
        shadow.add_deleted_edge(src, make_shadow_edge(3, 101, 2, 8), Direction::Outgoing);

        // At v4: both alive
        let result = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 4);
        assert_eq!(result.len(), 2);

        // At v6: only second alive
        let result = shadow.get_entries_at_version(src, 1, Direction::Outgoing, 6);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, Eid::new(101));
    }
}
