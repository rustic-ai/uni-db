// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Maximum Bipartite Matching Algorithm (Hopcroft-Karp).
//!
//! Finds the maximum matching in a bipartite graph.
//! Returns the matching edges and count.
//! Requires the graph to be bipartite.

use crate::algo::GraphProjection;
use crate::algo::algorithms::{Algorithm, BipartiteCheck, BipartiteCheckConfig};
use uni_common::core::id::Vid;

pub struct MaximumMatching;

#[derive(Debug, Clone, Default)]
pub struct MaximumMatchingConfig {}

pub struct MaximumMatchingResult {
    pub match_count: usize,
    pub matching: Vec<(Vid, Vid)>,
}

impl Algorithm for MaximumMatching {
    type Config = MaximumMatchingConfig;
    type Result = Result<MaximumMatchingResult, String>;

    fn name() -> &'static str {
        "max_matching"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return Ok(MaximumMatchingResult {
                match_count: 0,
                matching: Vec::new(),
            });
        }

        // 1. Check Bipartite
        let check = BipartiteCheck::run(graph, BipartiteCheckConfig::default());
        if !check.is_bipartite {
            return Err("Graph is not bipartite".to_string());
        }

        // Split into U (color 0) and V (color 1)
        // partition is Vec<(Vid, u8)> but we need slots.
        // We can reconstruct color map by slot.
        // BipartiteCheck actually returns `partition: Vec<(Vid, u8)>`.
        // We should probably modify BipartiteCheck to return slot map or re-run logic.
        // Or map Vid back to slot.
        let mut color = vec![0u8; n];
        for (vid, c) in check.partition {
            if let Some(slot) = graph.to_slot(vid) {
                color[slot as usize] = c; // 0 or 1
            }
        }

        let mut pair_u = vec![None; n]; // Pair for u in U (stores v in V)
        let mut pair_v = vec![None; n]; // Pair for v in V (stores u in U)
        let mut dist = vec![u32::MAX; n];

        let u_nodes: Vec<usize> = (0..n).filter(|&i| color[i] == 0).collect();

        let mut matching_size = 0;

        loop {
            // BFS
            let mut queue = std::collections::VecDeque::new();
            for &u in &u_nodes {
                if pair_u[u].is_none() {
                    dist[u] = 0;
                    queue.push_back(u);
                } else {
                    dist[u] = u32::MAX;
                }
            }
            let mut dist_null = u32::MAX;

            while let Some(u) = queue.pop_front() {
                if dist[u] < dist_null {
                    for &v_u32 in graph.out_neighbors(u as u32) {
                        let v = v_u32 as usize;
                        // Since we treat graph as undirected for bipartite matching,
                        // we must ensure we only traverse edges between partition sets.
                        // Bipartite check ensures edges are only between 0 and 1.
                        // But `out_neighbors` might be directed.
                        // If graph is directed, do we treat as undirected?
                        // Standard matching is on undirected edges.
                        // If U->V, OK. If V->U?
                        // Hopcroft-Karp usually formulated on U->V.
                        // If we have edges in both directions, we might double count or traverse wrong.
                        // We should only consider edges from U to V?
                        // If `out_neighbors` contains V->U, we should ignore?
                        // Since we iterate `u` in `u_nodes` (Set U), `out_neighbors` are neighbors of U.
                        // They must be in V.

                        if let Some(next_u) = pair_v[v] {
                            if dist[next_u] == u32::MAX {
                                dist[next_u] = dist[u] + 1;
                                queue.push_back(next_u);
                            }
                        } else {
                            dist_null = dist[u] + 1;
                        }
                    }

                    // Also check in_neighbors if undirected?
                    // GraphProjection might have `include_reverse`.
                    // If we assume undirected connectivity, we need to check both.
                    // If `u` in U, neighbors are in V.
                    if graph.has_reverse() {
                        for &v_u32 in graph.in_neighbors(u as u32) {
                            let v = v_u32 as usize;
                            if let Some(next_u) = pair_v[v] {
                                if dist[next_u] == u32::MAX {
                                    dist[next_u] = dist[u] + 1;
                                    queue.push_back(next_u);
                                }
                            } else {
                                dist_null = dist[u] + 1;
                            }
                        }
                    }
                }
            }

            if dist_null == u32::MAX {
                break;
            }

            // DFS
            for &u in &u_nodes {
                if pair_u[u].is_none() && dfs(u, graph, &mut pair_u, &mut pair_v, &dist) {
                    matching_size += 1;
                }
            }
        }

        let mut matching = Vec::new();
        for u in u_nodes {
            if let Some(v) = pair_u[u] {
                matching.push((graph.to_vid(u as u32), graph.to_vid(v as u32)));
            }
        }

        Ok(MaximumMatchingResult {
            match_count: matching_size,
            matching,
        })
    }
}

fn dfs(
    u: usize,
    graph: &GraphProjection,
    pair_u: &mut [Option<usize>],
    pair_v: &mut [Option<usize>],
    dist: &[u32],
) -> bool {
    if dist[u] == u32::MAX {
        return false;
    }

    let mut neighbors = Vec::new();
    neighbors.extend_from_slice(graph.out_neighbors(u as u32));
    if graph.has_reverse() {
        neighbors.extend_from_slice(graph.in_neighbors(u as u32));
    }

    for &v_u32 in &neighbors {
        let v = v_u32 as usize;
        // Check if dist logic holds
        let _next_dist = if let Some(next_u) = pair_v[v] {
            dist[next_u]
        } else {
            u32::MAX // null node
        };

        // Target condition: next_dist == dist[u] + 1
        // If pair_v[v] is None, dist[null] is conceptually dist[u]+1 if we reached free node.
        // Wait, standard DFS logic:

        let proceed = if let Some(next_u) = pair_v[v] {
            dist[next_u] == dist[u] + 1 && dfs(next_u, graph, pair_u, pair_v, dist)
        } else {
            true // Found free vertex, augmenting path found
        };

        if proceed {
            pair_v[v] = Some(u);
            pair_u[u] = Some(v);
            return true;
        }
    }

    // Mark as visited/useless for this phase
    // In standard HK, we reset dist to infinity? No, just don't visit again.
    // Usually dist is not modified in DFS.

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_matching_simple() {
        // 0 -> 1, 2 -> 3
        // Bipartite: {0, 2} and {1, 3}
        // Matching: (0,1), (2,3) -> size 2
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2), Vid::from(3)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(2), Vid::from(3))];
        let graph = build_test_graph(vids, edges);

        let result = MaximumMatching::run(&graph, MaximumMatchingConfig::default()).unwrap();
        assert_eq!(result.match_count, 2);
    }
}
