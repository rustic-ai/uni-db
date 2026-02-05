// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! L0-csr overlay segments for in-memory edge mutations.
//!
//! Edges are dual-written to both the data L0 (SimpleGraph) and the
//! L0-csr overlay. The overlay keeps adjacency data current without
//! needing to rebuild the CSR after flush.

use crate::storage::direction::Direction;
use dashmap::DashMap;
use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};

/// Tombstone for a deleted edge in the L0-csr overlay.
#[derive(Clone, Debug)]
pub struct OverlayTombstone {
    /// Deleted edge ID.
    pub eid: Eid,
    /// Source vertex of the deleted edge.
    pub src_vid: Vid,
    /// Destination vertex of the deleted edge.
    pub dst_vid: Vid,
    /// Edge type ID (bit 31 = 0 for schema'd, 1 for schemaless).
    pub edge_type: u32,
    /// Version at which deletion occurred.
    pub version: u64,
}

/// Key for overlay adjacency lookups: `(edge_type_id, direction)`.
/// Edge type is u32 with bit 31 = 0 for schema'd, 1 for schemaless.
type OverlayKey = (u32, Direction);

/// Per-vertex neighbor list: `vid -> [(neighbor_vid, eid, version)]`.
type NeighborMap = HashMap<Vid, Vec<(Vid, Eid, u64)>>;

/// Active L0-csr overlay segment receiving concurrent writes.
///
/// New edge inserts and deletes are written here concurrently with the
/// data L0. The segment can be frozen into a [`FrozenCsrSegment`] for
/// compaction into the Main CSR.
pub struct L0CsrSegment {
    /// Adjacency lists for inserted edges: `(edge_type, direction) -> vid -> [(neighbor, eid, version)]`.
    pub(crate) inserts: DashMap<OverlayKey, NeighborMap>,
    /// Tombstones for deleted edges, keyed by edge ID.
    pub(crate) tombstones: DashMap<Eid, OverlayTombstone>,
}

impl L0CsrSegment {
    /// Creates an empty overlay segment.
    pub fn new() -> Self {
        Self {
            inserts: DashMap::new(),
            tombstones: DashMap::new(),
        }
    }

    /// Records an edge insertion in the overlay.
    pub fn insert_edge(
        &self,
        src: Vid,
        dst: Vid,
        eid: Eid,
        edge_type: u32,
        version: u64,
        direction: Direction,
    ) {
        self.inserts
            .entry((edge_type, direction))
            .or_default()
            .entry(src)
            .or_default()
            .push((dst, eid, version));
    }

    /// Records a tombstone for a deleted edge.
    pub fn add_tombstone(&self, eid: Eid, src: Vid, dst: Vid, edge_type: u32, version: u64) {
        self.tombstones.insert(
            eid,
            OverlayTombstone {
                eid,
                src_vid: src,
                dst_vid: dst,
                edge_type,
                version,
            },
        );
    }

    /// Returns neighbors for a vertex, applying tombstones.
    pub fn get_neighbors(&self, vid: Vid, edge_type: u32, direction: Direction) -> Vec<(Vid, Eid)> {
        let mut result = Vec::new();

        if let Some(adj) = self.inserts.get(&(edge_type, direction))
            && let Some(neighbors) = adj.get(&vid)
        {
            for &(neighbor, eid, _version) in neighbors {
                if !self.tombstones.contains_key(&eid) {
                    result.push((neighbor, eid));
                }
            }
        }

        result
    }

    /// Returns neighbors visible at a specific version, applying tombstones.
    pub fn get_neighbors_at_version(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
        version: u64,
    ) -> Vec<(Vid, Eid)> {
        let mut result = Vec::new();

        if let Some(adj) = self.inserts.get(&(edge_type, direction))
            && let Some(neighbors) = adj.get(&vid)
        {
            for &(neighbor, eid, ver) in neighbors {
                let tombstoned = self
                    .tombstones
                    .get(&eid)
                    .is_some_and(|ts| ts.version <= version);
                if ver <= version && !tombstoned {
                    result.push((neighbor, eid));
                }
            }
        }

        result
    }

    /// Checks whether this segment has any insert entries for the given type and direction.
    pub fn has_entries_for(&self, edge_type: u32, direction: Direction) -> bool {
        self.inserts.contains_key(&(edge_type, direction))
    }

    /// Freezes this segment into an immutable [`FrozenCsrSegment`].
    ///
    /// The active segment is consumed and its data moved into plain
    /// `HashMap`s for lock-free read access during compaction.
    pub fn freeze(self) -> FrozenCsrSegment {
        FrozenCsrSegment {
            inserts: self.inserts.into_iter().collect(),
            tombstones: self.tombstones.into_iter().collect(),
        }
    }
}

impl Default for L0CsrSegment {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for L0CsrSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0CsrSegment")
            .field("insert_buckets", &self.inserts.len())
            .field("tombstones", &self.tombstones.len())
            .finish()
    }
}

/// Frozen (immutable) L0-csr segment awaiting compaction.
///
/// Same data as [`L0CsrSegment`] but in plain `HashMap`s for
/// lock-free shared read access.
#[derive(Debug)]
pub struct FrozenCsrSegment {
    /// Adjacency lists for inserted edges.
    pub(crate) inserts: HashMap<OverlayKey, NeighborMap>,
    /// Tombstones for deleted edges.
    pub(crate) tombstones: HashMap<Eid, OverlayTombstone>,
}

impl FrozenCsrSegment {
    /// Returns neighbors for a vertex, applying tombstones.
    pub fn get_neighbors(&self, vid: Vid, edge_type: u32, direction: Direction) -> Vec<(Vid, Eid)> {
        let mut result = Vec::new();

        if let Some(adj) = self.inserts.get(&(edge_type, direction))
            && let Some(neighbors) = adj.get(&vid)
        {
            for &(neighbor, eid, _version) in neighbors {
                if !self.tombstones.contains_key(&eid) {
                    result.push((neighbor, eid));
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get_neighbors() {
        let segment = L0CsrSegment::new();
        let src = Vid::new(1);
        let dst = Vid::new(2);
        let eid = Eid::new(100);

        segment.insert_edge(src, dst, eid, 1, 1, Direction::Outgoing);

        let neighbors = segment.get_neighbors(src, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], (dst, eid));
    }

    #[test]
    fn test_tombstone_excludes_edge() {
        let segment = L0CsrSegment::new();
        let src = Vid::new(1);
        let dst = Vid::new(2);
        let eid = Eid::new(100);

        segment.insert_edge(src, dst, eid, 1, 1, Direction::Outgoing);
        segment.add_tombstone(eid, src, dst, 1, 2);

        let neighbors = segment.get_neighbors(src, 1, Direction::Outgoing);
        assert!(neighbors.is_empty());
    }

    #[test]
    fn test_multiple_inserts_accumulate() {
        let segment = L0CsrSegment::new();
        let src = Vid::new(1);

        segment.insert_edge(src, Vid::new(2), Eid::new(100), 1, 1, Direction::Outgoing);
        segment.insert_edge(src, Vid::new(3), Eid::new(101), 1, 1, Direction::Outgoing);

        let neighbors = segment.get_neighbors(src, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn test_different_directions_are_separate() {
        let segment = L0CsrSegment::new();
        let src = Vid::new(1);
        let dst = Vid::new(2);

        segment.insert_edge(src, dst, Eid::new(100), 1, 1, Direction::Outgoing);
        segment.insert_edge(dst, src, Eid::new(100), 1, 1, Direction::Incoming);

        let out = segment.get_neighbors(src, 1, Direction::Outgoing);
        assert_eq!(out.len(), 1);

        let inc = segment.get_neighbors(dst, 1, Direction::Incoming);
        assert_eq!(inc.len(), 1);

        // No cross-contamination
        assert!(
            segment
                .get_neighbors(src, 1, Direction::Incoming)
                .is_empty()
        );
    }

    #[test]
    fn test_freeze_preserves_data() {
        let segment = L0CsrSegment::new();
        let src = Vid::new(1);
        let dst = Vid::new(2);
        let eid = Eid::new(100);

        segment.insert_edge(src, dst, eid, 1, 1, Direction::Outgoing);
        segment.add_tombstone(Eid::new(999), Vid::new(5), Vid::new(6), 2, 3);

        let frozen = segment.freeze();

        let neighbors = frozen.get_neighbors(src, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], (dst, eid));
        assert!(frozen.tombstones.contains_key(&Eid::new(999)));
    }

    #[test]
    fn test_empty_segment() {
        let segment = L0CsrSegment::new();
        assert!(
            segment
                .get_neighbors(Vid::new(0), 1, Direction::Outgoing)
                .is_empty()
        );
    }
}
