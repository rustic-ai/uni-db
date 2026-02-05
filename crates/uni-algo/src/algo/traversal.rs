// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Zero-copy graph traversal directly on AdjacencyManager.
//!
//! For light algorithms that don't need the full projection overhead.

use std::collections::{HashMap, HashSet, VecDeque};
use uni_common::core::id::{Eid, Vid};
use uni_store::storage::adjacency_manager::AdjacencyManager;
use uni_store::storage::direction::Direction;

/// Zero-copy traversal on storage primitives.
///
/// Use this for light algorithms like BFS, DFS, or point-to-point shortest path
/// where projection overhead isn't justified.
pub struct DirectTraversal<'a> {
    am: &'a AdjacencyManager,
    edge_types: Vec<u32>,
}

impl<'a> DirectTraversal<'a> {
    /// Create a new direct traversal context.
    pub fn new(am: &'a AdjacencyManager, edge_types: Vec<u32>) -> Self {
        Self { am, edge_types }
    }

    /// Iterate neighbors of a vertex in the given direction.
    pub fn neighbors(&self, vid: Vid, direction: Direction) -> Vec<(Vid, Eid)> {
        let mut result = Vec::new();

        for &edge_type in &self.edge_types {
            let neighbors = self.am.get_neighbors(vid, edge_type, direction);
            result.extend(neighbors);
        }

        result
    }

    /// BFS from a source vertex.
    pub fn bfs(&self, source: Vid, direction: Direction) -> BfsIterator<'_> {
        BfsIterator::new(self, source, direction)
    }

    /// Find shortest path between source and target using bidirectional BFS.
    ///
    /// Returns the path as a sequence of VIDs and EIDs, or None if no path exists.
    pub fn shortest_path(&self, source: Vid, target: Vid, direction: Direction) -> Option<Path> {
        self.shortest_path_with_hops(source, target, direction, 0, u32::MAX)
    }

    /// Find shortest path with hop constraints using unidirectional BFS.
    ///
    /// Returns the path only if its length is within [min_hops, max_hops].
    /// Uses unidirectional BFS with depth tracking for efficiency with hop constraints.
    ///
    /// # Arguments
    /// * `source` - Starting vertex
    /// * `target` - Destination vertex
    /// * `direction` - Edge traversal direction
    /// * `min_hops` - Minimum path length (number of edges)
    /// * `max_hops` - Maximum path length (number of edges)
    pub fn shortest_path_with_hops(
        &self,
        source: Vid,
        target: Vid,
        direction: Direction,
        min_hops: u32,
        max_hops: u32,
    ) -> Option<Path> {
        // Handle special case: source == target
        if source == target {
            if min_hops == 0 {
                return Some(Path {
                    vertices: vec![source],
                    edges: Vec::new(),
                });
            } else {
                // Need at least min_hops edges, but source == target with no edges is 0 hops
                return None;
            }
        }

        // Invalid configuration
        if min_hops > max_hops {
            return None;
        }

        // Use unidirectional BFS with depth tracking for hop constraints
        // This is simpler and allows us to track depth precisely
        let mut visited: HashMap<Vid, (Vid, Eid, u32)> = HashMap::default(); // vid -> (parent, edge, depth)
        let mut frontier: VecDeque<(Vid, u32)> = VecDeque::new(); // (vid, depth)

        frontier.push_back((source, 0));
        visited.insert(source, (source, Eid::new(0), 0)); // Source has no parent, depth 0

        while let Some((current, depth)) = frontier.pop_front() {
            // Stop expanding if we've reached max_hops
            if depth >= max_hops {
                continue;
            }

            for (neighbor, eid) in self.neighbors(current, direction) {
                if visited.contains_key(&neighbor) {
                    continue;
                }

                let new_depth = depth + 1;
                visited.insert(neighbor, (current, eid, new_depth));

                if neighbor == target {
                    // Found target - check if path length is within bounds
                    if new_depth >= min_hops && new_depth <= max_hops {
                        return Some(self.reconstruct_path_from_visited(source, target, &visited));
                    } else if new_depth < min_hops {
                        // Path too short, but we found the shortest path
                        // Since BFS finds shortest first, any other path will be longer
                        // So if shortest is too short, we need to continue searching
                        // Actually, in BFS the first path found IS the shortest, so we can't find
                        // a longer path that still satisfies min_hops unless we use DFS or
                        // enumerate all paths. For simplicity, return None if shortest is too short.
                        return None;
                    } else {
                        // Path too long (shouldn't happen since we stop at max_hops)
                        return None;
                    }
                }

                frontier.push_back((neighbor, new_depth));
            }
        }

        None
    }

    /// Reconstruct path from visited map (unidirectional BFS).
    fn reconstruct_path_from_visited(
        &self,
        source: Vid,
        target: Vid,
        visited: &HashMap<Vid, (Vid, Eid, u32)>,
    ) -> Path {
        let mut vertices = vec![target];
        let mut edges = Vec::new();
        let mut current = target;

        while current != source {
            if let Some(&(parent, eid, _)) = visited.get(&current) {
                edges.push(eid);
                vertices.push(parent);
                current = parent;
            } else {
                break;
            }
        }

        vertices.reverse();
        edges.reverse();

        Path { vertices, edges }
    }

    /// Find all shortest paths between source and target using BFS.
    ///
    /// Returns all paths that have the minimum length (number of edges).
    /// This is different from `shortest_path_with_hops` which returns only one path.
    ///
    /// # Arguments
    /// * `source` - Starting vertex
    /// * `target` - Destination vertex
    /// * `direction` - Edge traversal direction
    /// * `min_hops` - Minimum path length (number of edges)
    /// * `max_hops` - Maximum path length (number of edges)
    pub fn all_shortest_paths_with_hops(
        &self,
        source: Vid,
        target: Vid,
        direction: Direction,
        min_hops: u32,
        max_hops: u32,
    ) -> Vec<Path> {
        // Handle special case: source == target
        if source == target {
            if min_hops == 0 {
                return vec![Path {
                    vertices: vec![source],
                    edges: Vec::new(),
                }];
            } else {
                return Vec::new();
            }
        }

        // Invalid configuration
        if min_hops > max_hops {
            return Vec::new();
        }

        // Phase 1: BFS to find distances from source and to target
        // This allows us to know which edges lie on shortest paths
        let dist_from_source = self.bfs_distances(source, direction, max_hops);

        // If target is unreachable or too far, return empty
        let shortest_dist = match dist_from_source.get(&target) {
            Some(&d) if d >= min_hops && d <= max_hops => d,
            Some(&d) if d < min_hops => return Vec::new(), // Shortest path too short
            _ => return Vec::new(),                        // Unreachable
        };

        // Phase 2: DFS to enumerate all paths of the shortest length
        let mut all_paths = Vec::new();
        let mut current_path = vec![source];
        let mut current_edges = Vec::new();
        let mut visited = HashSet::new();
        visited.insert(source);

        self.enumerate_shortest_paths(
            source,
            target,
            direction,
            shortest_dist,
            0,
            &dist_from_source,
            &mut current_path,
            &mut current_edges,
            &mut visited,
            &mut all_paths,
        );

        all_paths
    }

    /// BFS to compute distances from source to all reachable vertices.
    fn bfs_distances(
        &self,
        source: Vid,
        direction: Direction,
        max_depth: u32,
    ) -> HashMap<Vid, u32> {
        let mut distances: HashMap<Vid, u32> = HashMap::default();
        let mut frontier: VecDeque<(Vid, u32)> = VecDeque::new();

        frontier.push_back((source, 0));
        distances.insert(source, 0);

        while let Some((current, depth)) = frontier.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for (neighbor, _eid) in self.neighbors(current, direction) {
                if let std::collections::hash_map::Entry::Vacant(e) = distances.entry(neighbor) {
                    let new_depth = depth + 1;
                    e.insert(new_depth);
                    frontier.push_back((neighbor, new_depth));
                }
            }
        }

        distances
    }

    /// DFS to enumerate all shortest paths.
    #[allow(clippy::too_many_arguments)]
    fn enumerate_shortest_paths(
        &self,
        current: Vid,
        target: Vid,
        direction: Direction,
        target_dist: u32,
        current_dist: u32,
        dist_from_source: &HashMap<Vid, u32>,
        current_path: &mut Vec<Vid>,
        current_edges: &mut Vec<Eid>,
        visited: &mut HashSet<Vid>,
        all_paths: &mut Vec<Path>,
    ) {
        // Found target at correct distance
        if current == target && current_dist == target_dist {
            all_paths.push(Path {
                vertices: current_path.clone(),
                edges: current_edges.clone(),
            });
            return;
        }

        // Pruning: if current_dist >= target_dist, we can't reach target
        if current_dist >= target_dist {
            return;
        }

        // Explore neighbors that lie on shortest paths
        for (neighbor, eid) in self.neighbors(current, direction) {
            // Check if this neighbor is on a shortest path:
            // dist_from_source[neighbor] == current_dist + 1
            if let Some(&neighbor_dist) = dist_from_source.get(&neighbor)
                && neighbor_dist == current_dist + 1
                && !visited.contains(&neighbor)
            {
                visited.insert(neighbor);
                current_path.push(neighbor);
                current_edges.push(eid);

                self.enumerate_shortest_paths(
                    neighbor,
                    target,
                    direction,
                    target_dist,
                    current_dist + 1,
                    dist_from_source,
                    current_path,
                    current_edges,
                    visited,
                    all_paths,
                );

                current_path.pop();
                current_edges.pop();
                visited.remove(&neighbor);
            }
        }
    }
}

/// BFS iterator yielding (vid, distance) pairs.
pub struct BfsIterator<'a> {
    traversal: &'a DirectTraversal<'a>,
    frontier: VecDeque<(Vid, u32)>,
    visited: HashSet<Vid>,
    direction: Direction,
}

impl<'a> BfsIterator<'a> {
    fn new(traversal: &'a DirectTraversal<'a>, source: Vid, direction: Direction) -> Self {
        let mut frontier = VecDeque::new();
        let mut visited = HashSet::default();

        frontier.push_back((source, 0));
        visited.insert(source);

        Self {
            traversal,
            frontier,
            visited,
            direction,
        }
    }
}

impl Iterator for BfsIterator<'_> {
    type Item = (Vid, u32);

    fn next(&mut self) -> Option<Self::Item> {
        let (current, distance) = self.frontier.pop_front()?;

        // Enqueue neighbors
        for (neighbor, _eid) in self.traversal.neighbors(current, self.direction) {
            if self.visited.insert(neighbor) {
                self.frontier.push_back((neighbor, distance + 1));
            }
        }

        Some((current, distance))
    }
}

/// Path representation for shortest path results.
#[derive(Debug, Clone)]
pub struct Path {
    /// Vertices in the path (source to target)
    pub vertices: Vec<Vid>,
    /// Edges in the path
    pub edges: Vec<Eid>,
}

impl Path {
    /// Length of the path (number of edges)
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Whether the path is empty (source == target)
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full traversal tests require mocking AdjacencyManager
    // These tests focus on the Path data structure and hop constraint logic

    #[test]
    fn test_path_length() {
        let path = Path {
            vertices: vec![Vid::new(0), Vid::new(1), Vid::new(2)],
            edges: vec![Eid::new(0), Eid::new(1)],
        };

        assert_eq!(path.len(), 2);
        assert!(!path.is_empty());
    }

    #[test]
    fn test_path_empty() {
        // Zero-length path (source == target)
        let path = Path {
            vertices: vec![Vid::new(0)],
            edges: vec![],
        };

        assert_eq!(path.len(), 0);
        assert!(path.is_empty());
    }

    #[test]
    fn test_path_single_hop() {
        let path = Path {
            vertices: vec![Vid::new(0), Vid::new(1)],
            edges: vec![Eid::new(0)],
        };

        assert_eq!(path.len(), 1);
        assert!(!path.is_empty());
    }

    // Tests for hop constraint validation logic
    // These test the bounds checking that would be used in shortest_path_with_hops

    #[test]
    fn test_hop_constraint_validation() {
        // Test helper for validating hop constraints
        fn is_valid_path_length(path_len: u32, min_hops: u32, max_hops: u32) -> bool {
            path_len >= min_hops && path_len <= max_hops
        }

        // Path length 3, constraints [1, 5] -> valid
        assert!(is_valid_path_length(3, 1, 5));

        // Path length 0 (source==target), constraints [0, 5] -> valid
        assert!(is_valid_path_length(0, 0, 5));

        // Path length 0, constraints [1, 5] -> invalid (too short)
        assert!(!is_valid_path_length(0, 1, 5));

        // Path length 6, constraints [1, 5] -> invalid (too long)
        assert!(!is_valid_path_length(6, 1, 5));

        // Path length 5, constraints [5, 5] -> valid (exact match)
        assert!(is_valid_path_length(5, 5, 5));

        // Invalid constraint: min > max
        assert!(!is_valid_path_length(3, 5, 2));
    }

    #[test]
    fn test_hop_constraint_edge_cases() {
        fn is_valid_path_length(path_len: u32, min_hops: u32, max_hops: u32) -> bool {
            min_hops <= max_hops && path_len >= min_hops && path_len <= max_hops
        }

        // Unbounded max (u32::MAX)
        assert!(is_valid_path_length(1000, 1, u32::MAX));

        // Zero min_hops with zero-length path
        assert!(is_valid_path_length(0, 0, 10));

        // Boundary conditions
        assert!(is_valid_path_length(1, 1, 1)); // Exactly 1 hop
        assert!(!is_valid_path_length(2, 1, 1)); // 2 hops when max is 1
        assert!(!is_valid_path_length(0, 1, 1)); // 0 hops when min is 1
    }
}
