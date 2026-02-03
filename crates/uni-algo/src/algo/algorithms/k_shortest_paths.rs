// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! K-Shortest Paths Algorithm (Yen's Algorithm).
//!
//! Finds the K shortest loop-less paths from source to target.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use uni_common::core::id::Vid;

pub struct KShortestPaths;

#[derive(Debug, Clone)]
pub struct KShortestPathsConfig {
    pub source: Vid,
    pub target: Vid,
    pub k: usize,
}

impl Default for KShortestPathsConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            target: Vid::from(0),
            k: 1,
        }
    }
}

pub struct KShortestPathsResult {
    pub paths: Vec<(Vec<Vid>, f64)>, // (path, cost)
}

impl Algorithm for KShortestPaths {
    type Config = KShortestPathsConfig;
    type Result = KShortestPathsResult;

    fn name() -> &'static str {
        "k_shortest_paths"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source_slot = match graph.to_slot(config.source) {
            Some(s) => s,
            None => return KShortestPathsResult { paths: Vec::new() },
        };
        let target_slot = match graph.to_slot(config.target) {
            Some(s) => s,
            None => return KShortestPathsResult { paths: Vec::new() },
        };

        if config.k == 0 {
            return KShortestPathsResult { paths: Vec::new() };
        }

        let mut a: Vec<(Vec<u32>, f64)> = Vec::new();

        // 1. First shortest path
        let (path0, cost0) = match run_dijkstra(graph, source_slot, target_slot, &HashSet::new()) {
            Some(res) => res,
            None => return KShortestPathsResult { paths: Vec::new() },
        };
        a.push((path0, cost0));

        let mut b: BinaryHeap<Reverse<(u64, Vec<u32>)>> = BinaryHeap::new();

        // 2. Iterate for k
        for k in 1..config.k {
            let prev_path = &a[k - 1].0;

            // The spur node ranges from the first node to the next-to-last node in the previous k-shortest path.
            for i in 0..prev_path.len() - 1 {
                let spur_node = prev_path[i];
                let root_path = &prev_path[0..=i];
                let root_path_cost = calculate_path_cost(graph, root_path);

                let mut forbidden_edges = HashSet::new();

                for (p_path, _) in &a {
                    if p_path.len() > i && &p_path[0..=i] == root_path {
                        forbidden_edges.insert((p_path[i], p_path[i + 1]));
                    }
                }

                // Remove root path nodes from graph (except spur node) to ensure loopless
                // We simulate this by checking if neighbor is in root_path (excluding spur)
                // Actually Yen's usually ensures loopless.
                // Standard implementation: disable nodes in root path.

                // Run Dijkstra from spur node to target
                if let Some((spur_path, spur_cost)) = run_dijkstra_with_constraints(
                    graph,
                    spur_node,
                    target_slot,
                    &forbidden_edges,
                    root_path, // forbidden nodes are root_path[0..i] (excluding spur which is root_path[i])
                ) {
                    let mut total_path = root_path[0..i].to_vec();
                    total_path.extend(spur_path);
                    let total_cost = root_path_cost + spur_cost; // root_path excludes last edge to spur? No, root_path includes spur.
                    // Wait, root_path includes spur. path cost calculation logic needs to be precise.

                    // Logic check:
                    // root_path = [s, ..., spur]
                    // spur_path = [spur, ..., t]
                    // total = [s, ..., spur, ..., t]
                    // cost = cost(s..spur) + cost(spur..t)

                    // Using bits for f64 ordering in heap
                    let entry = Reverse((total_cost.to_bits(), total_path));

                    // Ideally verify uniqueness before pushing to heap, but B handles sorting.
                    // Duplicate paths might be generated.
                    // We should check if path is already in B?
                    // BinaryHeap doesn't support contains.
                    // Typically B is a set or we push and then dedup when popping.

                    // Optimization: check if already in B? Too slow.
                    // Just push.
                    b.push(entry);
                }
            }

            if b.is_empty() {
                break;
            }

            // Extract best from B
            // Need to handle duplicates
            let mut best_path = None;

            while let Some(Reverse((cost_bits, path))) = b.pop() {
                let cost = f64::from_bits(cost_bits);
                // Check if path is already in A
                let exists = a.iter().any(|(p, _)| p == &path);
                if !exists {
                    best_path = Some((path, cost));
                    break;
                }
            }

            if let Some(bp) = best_path {
                a.push(bp);
            } else {
                break;
            }
        }

        let mapped_paths = a
            .into_iter()
            .map(|(path, cost)| {
                let vids = path.iter().map(|&slot| graph.to_vid(slot)).collect();
                (vids, cost)
            })
            .collect();

        KShortestPathsResult {
            paths: mapped_paths,
        }
    }
}

fn calculate_path_cost(graph: &GraphProjection, path: &[u32]) -> f64 {
    let mut cost = 0.0;
    for i in 0..path.len() - 1 {
        let u = path[i];
        let v = path[i + 1];
        // Find edge weight
        // Linear scan of neighbors
        let neighbors = graph.out_neighbors(u);
        let mut weight = 1.0;
        if graph.has_weights() {
            for (idx, &n) in neighbors.iter().enumerate() {
                if n == v {
                    weight = graph.out_weight(u, idx);
                    break;
                }
            }
        }
        cost += weight;
    }
    cost
}

fn run_dijkstra(
    graph: &GraphProjection,
    source: u32,
    target: u32,
    forbidden_edges: &HashSet<(u32, u32)>,
) -> Option<(Vec<u32>, f64)> {
    run_dijkstra_with_constraints(graph, source, target, forbidden_edges, &[])
}

fn run_dijkstra_with_constraints(
    graph: &GraphProjection,
    source: u32,
    target: u32,
    forbidden_edges: &HashSet<(u32, u32)>,
    forbidden_nodes: &[u32],
) -> Option<(Vec<u32>, f64)> {
    let n = graph.vertex_count();
    let mut dist = vec![f64::INFINITY; n];
    let mut prev = vec![None; n];
    let mut heap = BinaryHeap::new();

    dist[source as usize] = 0.0;
    heap.push(Reverse((0.0f64.to_bits(), source)));

    let forbidden_nodes_set: HashSet<u32> = forbidden_nodes.iter().cloned().collect();

    while let Some(Reverse((d_bits, u))) = heap.pop() {
        let d = f64::from_bits(d_bits);
        if d > dist[u as usize] {
            continue;
        }
        if u == target {
            break;
        }

        for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
            if forbidden_nodes_set.contains(&v) {
                continue;
            }
            if forbidden_edges.contains(&(u, v)) {
                continue;
            }

            let weight = if graph.has_weights() {
                graph.out_weight(u, i)
            } else {
                1.0
            };
            let new_dist = d + weight;

            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                prev[v as usize] = Some(u);
                heap.push(Reverse((new_dist.to_bits(), v)));
            }
        }
    }

    if dist[target as usize] == f64::INFINITY {
        return None;
    }

    let mut path = Vec::new();
    let mut curr = Some(target);
    while let Some(slot) = curr {
        path.push(slot);
        if slot == source {
            break;
        }
        curr = prev[slot as usize];
    }
    path.reverse();
    Some((path, dist[target as usize]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_ksp_simple() {
        // 0 -> 1 -> 3 (cost 2)
        // 0 -> 2 -> 3 (cost 2)
        // 0 -> 3 (cost 10 - not possible in unit weight without custom builder)
        // Let's rely on hop count as cost (1.0).
        // 0->1->3 (2.0)
        // 0->2->3 (2.0)

        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2), Vid::from(3)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(3)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(2), Vid::from(3)),
        ];
        let graph = build_test_graph(vids, edges);

        let config = KShortestPathsConfig {
            source: Vid::from(0),
            target: Vid::from(3),
            k: 2,
        };

        let result = KShortestPaths::run(&graph, config);
        assert_eq!(result.paths.len(), 2);
        assert_eq!(result.paths[0].1, 2.0);
        assert_eq!(result.paths[1].1, 2.0);
    }
}
