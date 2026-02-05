// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Simple adjacency list graph for L0Buffer.
//!
//! This replaces gryf's DeltaGraph with a minimal implementation that provides:
//! - O(1) vertex lookup
//! - O(1) edge lookup by Eid
//! - O(degree) neighbor iteration
//! - Safe edge deletion without panics

use crate::core::id::{Eid, Vid};
use fxhash::FxBuildHasher;
use std::collections::HashMap;

/// Edge entry stored in adjacency lists.
///
/// The `edge_type` field uses the [`EdgeTypeId`](crate::core::edge_type::EdgeTypeId) encoding.
#[derive(Clone, Copy, Debug)]
pub struct EdgeEntry {
    pub eid: Eid,
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub edge_type: u32,
}

/// Type alias for FxHashMap (faster hashing for integer keys)
type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

/// Simple directed graph with adjacency lists.
///
/// Stores outgoing and incoming edge lists per vertex for O(degree) traversal,
/// plus a direct Eid-to-edge map for O(1) edge lookup.
#[derive(Debug)]
pub struct SimpleGraph {
    /// Set of vertices in the graph
    vertices: FxHashMap<Vid, ()>,
    /// Outgoing edges per vertex: src_vid -> [EdgeEntry]
    outgoing: FxHashMap<Vid, Vec<EdgeEntry>>,
    /// Incoming edges per vertex: dst_vid -> [EdgeEntry]
    incoming: FxHashMap<Vid, Vec<EdgeEntry>>,
    /// Direct edge lookup by Eid for O(1) access
    edge_map: FxHashMap<Eid, EdgeEntry>,
}

/// Direction for neighbor traversal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
}

impl Default for SimpleGraph {
    fn default() -> Self {
        Self {
            vertices: HashMap::with_hasher(FxBuildHasher::default()),
            outgoing: HashMap::with_hasher(FxBuildHasher::default()),
            incoming: HashMap::with_hasher(FxBuildHasher::default()),
            edge_map: HashMap::with_hasher(FxBuildHasher::default()),
        }
    }
}

impl SimpleGraph {
    /// Creates a new empty graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new graph with pre-allocated capacity.
    pub fn with_capacity(vertices: usize, edges: usize) -> Self {
        Self {
            vertices: HashMap::with_capacity_and_hasher(vertices, FxBuildHasher::default()),
            outgoing: HashMap::with_capacity_and_hasher(vertices, FxBuildHasher::default()),
            incoming: HashMap::with_capacity_and_hasher(vertices, FxBuildHasher::default()),
            edge_map: HashMap::with_capacity_and_hasher(edges, FxBuildHasher::default()),
        }
    }

    /// Adds a vertex to the graph. Returns true if the vertex was newly added.
    pub fn add_vertex(&mut self, vid: Vid) -> bool {
        if self.vertices.contains_key(&vid) {
            return false;
        }
        self.vertices.insert(vid, ());
        true
    }

    /// Removes a vertex and all its edges from the graph.
    pub fn remove_vertex(&mut self, vid: Vid) {
        self.vertices.remove(&vid);

        // Remove outgoing edges from this vertex
        if let Some(edges) = self.outgoing.remove(&vid) {
            // Also remove from incoming lists of neighbors and edge_map
            for edge in edges {
                self.edge_map.remove(&edge.eid);
                if let Some(incoming_list) = self.incoming.get_mut(&edge.dst_vid) {
                    incoming_list.retain(|e| e.eid != edge.eid);
                }
            }
        }

        // Remove incoming edges to this vertex
        if let Some(edges) = self.incoming.remove(&vid) {
            // Also remove from outgoing lists of neighbors and edge_map
            for edge in edges {
                self.edge_map.remove(&edge.eid);
                if let Some(outgoing_list) = self.outgoing.get_mut(&edge.src_vid) {
                    outgoing_list.retain(|e| e.eid != edge.eid);
                }
            }
        }
    }

    /// Checks if a vertex exists in the graph.
    pub fn contains_vertex(&self, vid: Vid) -> bool {
        self.vertices.contains_key(&vid)
    }

    /// Returns the number of vertices in the graph.
    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    /// Adds an edge to the graph. Vertices are implicitly created if they don't exist.
    pub fn add_edge(&mut self, src_vid: Vid, dst_vid: Vid, eid: Eid, edge_type: u32) {
        // Ensure vertices exist
        self.add_vertex(src_vid);
        self.add_vertex(dst_vid);

        self.add_edge_unchecked(src_vid, dst_vid, eid, edge_type);
    }

    /// Adds an edge without checking if vertices exist. Use when vertices are pre-added.
    #[inline]
    pub fn add_edge_unchecked(&mut self, src_vid: Vid, dst_vid: Vid, eid: Eid, edge_type: u32) {
        let entry = EdgeEntry {
            eid,
            src_vid,
            dst_vid,
            edge_type,
        };

        // Add to outgoing list
        self.outgoing.entry(src_vid).or_default().push(entry);

        // Add to incoming list
        self.incoming.entry(dst_vid).or_default().push(entry);

        // Add to edge map for O(1) lookup
        self.edge_map.insert(eid, entry);
    }

    /// Removes an edge by its ID. Returns the removed edge entry if found.
    ///
    /// Uses O(1) lookup via edge_map, then O(degree) removal from adjacency lists.
    pub fn remove_edge(&mut self, eid: Eid) -> Option<EdgeEntry> {
        // O(1) lookup via edge_map
        let entry = self.edge_map.remove(&eid)?;

        // Remove from outgoing list
        if let Some(outgoing_list) = self.outgoing.get_mut(&entry.src_vid) {
            outgoing_list.retain(|e| e.eid != eid);
        }

        // Remove from incoming list
        if let Some(incoming_list) = self.incoming.get_mut(&entry.dst_vid) {
            incoming_list.retain(|e| e.eid != eid);
        }

        Some(entry)
    }

    /// Returns neighbors in the specified direction.
    /// O(degree) complexity.
    pub fn neighbors(&self, vid: Vid, direction: Direction) -> &[EdgeEntry] {
        let map = match direction {
            Direction::Outgoing => &self.outgoing,
            Direction::Incoming => &self.incoming,
        };
        map.get(&vid).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Returns an iterator over all edges in the graph.
    pub fn edges(&self) -> impl Iterator<Item = &EdgeEntry> {
        self.outgoing.values().flat_map(|v| v.iter())
    }

    /// Returns the edge with the given Eid in O(1) time.
    pub fn edge(&self, eid: Eid) -> Option<&EdgeEntry> {
        self.edge_map.get(&eid)
    }

    /// Checks if a vertex exists and returns it (identity).
    pub fn vertex(&self, vid: Vid) -> Option<Vid> {
        if self.vertices.contains_key(&vid) {
            Some(vid)
        } else {
            None
        }
    }

    /// Returns the total number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.outgoing.values().map(|v| v.len()).sum()
    }

    /// Returns an iterator over all vertices in the graph.
    pub fn vertices(&self) -> impl Iterator<Item = Vid> + '_ {
        self.vertices.keys().copied()
    }

    /// Clears the graph, removing all vertices and edges.
    pub fn clear(&mut self) {
        self.vertices.clear();
        self.outgoing.clear();
        self.incoming.clear();
        self.edge_map.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove_vertex() {
        let mut g = SimpleGraph::new();
        let v1 = Vid::new(1);
        let v2 = Vid::new(2);

        assert!(g.add_vertex(v1));
        assert!(!g.add_vertex(v1)); // Already exists
        assert!(g.add_vertex(v2));

        assert_eq!(g.vertex_count(), 2);
        assert!(g.contains_vertex(v1));

        g.remove_vertex(v1);
        assert_eq!(g.vertex_count(), 1);
        assert!(!g.contains_vertex(v1));
    }

    #[test]
    fn test_add_remove_edge() {
        let mut g = SimpleGraph::new();
        let v1 = Vid::new(1);
        let v2 = Vid::new(2);
        let v3 = Vid::new(3);
        let e1 = Eid::new(101);
        let e2 = Eid::new(102);

        g.add_edge(v1, v2, e1, 1);
        g.add_edge(v1, v3, e2, 1);

        assert_eq!(g.edge_count(), 2);

        // Check outgoing neighbors of v1
        let out = g.neighbors(v1, Direction::Outgoing);
        assert_eq!(out.len(), 2);

        // Check incoming neighbors of v2
        let inc = g.neighbors(v2, Direction::Incoming);
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].src_vid, v1);

        // Remove edge
        let removed = g.remove_edge(e1);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().dst_vid, v2);

        assert_eq!(g.edge_count(), 1);
        assert_eq!(g.neighbors(v1, Direction::Outgoing).len(), 1);
        assert_eq!(g.neighbors(v2, Direction::Incoming).len(), 0);
    }

    #[test]
    fn test_remove_vertex_with_edges() {
        let mut g = SimpleGraph::new();
        let v1 = Vid::new(1);
        let v2 = Vid::new(2);
        let v3 = Vid::new(3);
        let e1 = Eid::new(101);
        let e2 = Eid::new(102);

        g.add_edge(v1, v2, e1, 1);
        g.add_edge(v2, v3, e2, 1);

        // Remove v2, should clean up edges to/from it
        g.remove_vertex(v2);

        assert_eq!(g.vertex_count(), 2);
        assert_eq!(g.edge_count(), 0);
        assert_eq!(g.neighbors(v1, Direction::Outgoing).len(), 0);
        assert_eq!(g.neighbors(v3, Direction::Incoming).len(), 0);
    }

    #[test]
    fn test_edge_iteration() {
        let mut g = SimpleGraph::new();
        let v1 = Vid::new(1);
        let v2 = Vid::new(2);
        let v3 = Vid::new(3);
        let e1 = Eid::new(101);
        let e2 = Eid::new(102);

        g.add_edge(v1, v2, e1, 1);
        g.add_edge(v2, v3, e2, 2);

        let edges: Vec<_> = g.edges().collect();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn test_neighbor_iteration_after_deletion() {
        // This is the key test - ensure we don't panic after edge deletion
        let mut g = SimpleGraph::new();
        let v1 = Vid::new(1);
        let v2 = Vid::new(2);
        let v3 = Vid::new(3);
        let e1 = Eid::new(101);
        let e2 = Eid::new(102);

        g.add_edge(v1, v2, e1, 1);
        g.add_edge(v1, v3, e2, 1);

        // Delete one edge
        g.remove_edge(e1);

        // Iterate neighbors - should not panic (unlike gryf)
        let neighbors = g.neighbors(v1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].dst_vid, v3);
    }

    #[test]
    fn test_edge_lookup_o1() {
        // Test O(1) edge lookup via edge_map
        let mut g = SimpleGraph::new();
        let v1 = Vid::new(1);
        let v2 = Vid::new(2);
        let v3 = Vid::new(3);
        let e1 = Eid::new(101);
        let e2 = Eid::new(102);

        g.add_edge(v1, v2, e1, 1);
        g.add_edge(v2, v3, e2, 2);

        // O(1) lookup should work
        let edge = g.edge(e1);
        assert!(edge.is_some());
        let edge = edge.unwrap();
        assert_eq!(edge.src_vid, v1);
        assert_eq!(edge.dst_vid, v2);
        assert_eq!(edge.edge_type, 1);

        // Lookup non-existent edge
        let non_existent = Eid::new(999);
        assert!(g.edge(non_existent).is_none());

        // After removal, lookup should return None
        g.remove_edge(e1);
        assert!(g.edge(e1).is_none());
        assert!(g.edge(e2).is_some()); // e2 should still exist
    }
}
