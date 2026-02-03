// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A* Search Algorithm.
//!
//! A* is an informed search algorithm, or a best-first search, meaning that it is formulated in terms of weighted graphs:
//! starting from a specific starting node of a graph, it aims to find a path to the given goal node having the smallest cost.
//! It uses a heuristic function `h(n)` to estimate the cost from node `n` to the goal.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use uni_common::core::id::Vid;

pub struct AStar;

#[derive(Debug, Clone)]
pub struct AStarConfig {
    pub source: Vid,
    pub target: Vid,
    /// Heuristic values h(n) for each node n.
    /// If a node is missing, h(n) is assumed to be 0.0.
    pub heuristic: HashMap<Vid, f64>,
}

impl Default for AStarConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            target: Vid::from(0),
            heuristic: HashMap::new(),
        }
    }
}

pub struct AStarResult {
    pub distance: Option<f64>,
    pub path: Option<Vec<Vid>>,
    pub visited_count: usize,
}

impl Algorithm for AStar {
    type Config = AStarConfig;
    type Result = AStarResult;

    fn name() -> &'static str {
        "astar"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source_slot = match graph.to_slot(config.source) {
            Some(slot) => slot,
            None => {
                return AStarResult {
                    distance: None,
                    path: None,
                    visited_count: 0,
                };
            }
        };

        let target_slot = match graph.to_slot(config.target) {
            Some(slot) => slot,
            None => {
                return AStarResult {
                    distance: None,
                    path: None,
                    visited_count: 0,
                };
            }
        };

        let n = graph.vertex_count();
        // g_score[u] is the cost of the cheapest path from source to u currently known.
        let mut g_score = vec![f64::INFINITY; n];
        let mut prev: Vec<Option<u32>> = vec![None; n];

        // Priority queue stores (f_score, u), where f_score = g_score[u] + h(u).
        // We use Reverse for min-heap behavior.
        // Storing bits for f64 ordering.
        let mut heap = BinaryHeap::new();

        g_score[source_slot as usize] = 0.0;
        let h_source = config.heuristic.get(&config.source).copied().unwrap_or(0.0);
        let f_source = 0.0 + h_source;

        heap.push(Reverse((f_source.to_bits(), source_slot)));

        let mut visited_count = 0;

        while let Some(Reverse((f_bits, u))) = heap.pop() {
            let f_current = f64::from_bits(f_bits);

            // If we reached the target, we are done (if heuristic is consistent/monotone).
            // A* with consistent heuristic guarantees optimal path first time target is popped.
            if u == target_slot {
                let dist = g_score[u as usize];

                // Reconstruct path
                let mut path = Vec::new();
                let mut curr = Some(u);
                while let Some(slot) = curr {
                    path.push(graph.to_vid(slot));
                    if slot == source_slot {
                        break;
                    }
                    curr = prev[slot as usize];
                }
                path.reverse();

                return AStarResult {
                    distance: Some(dist),
                    path: Some(path),
                    visited_count,
                };
            }

            // Lazy deletion / check if we found a better g_score already that makes this entry stale
            // f_current is the f_score stored in heap.
            // Current best f would be g_score[u] + h(u).
            // If f_current > g_score[u] + h(u), then this heap entry is stale.
            let h_u = config
                .heuristic
                .get(&graph.to_vid(u))
                .copied()
                .unwrap_or(0.0);
            if f_current > g_score[u as usize] + h_u {
                continue;
            }

            visited_count += 1;

            for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
                let weight = if graph.has_weights() {
                    graph.out_weight(u, i)
                } else {
                    1.0
                };

                let tentative_g = g_score[u as usize] + weight;

                if tentative_g < g_score[v as usize] {
                    prev[v as usize] = Some(u);
                    g_score[v as usize] = tentative_g;

                    let h_v = config
                        .heuristic
                        .get(&graph.to_vid(v))
                        .copied()
                        .unwrap_or(0.0);
                    let f_v = tentative_g + h_v;

                    heap.push(Reverse((f_v.to_bits(), v)));
                }
            }
        }

        // Path not found
        AStarResult {
            distance: None,
            path: None,
            visited_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_astar_simple() {
        // 0 -> 1 -> 2
        // Weights 1.0
        // Heuristic: h(0)=2, h(1)=1, h(2)=0
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges); // weights default to 1.0 if not set?
        // GraphProjection out_weights is Option. build_test_graph sets None.
        // AStar assumes 1.0 if no weights.

        let mut heuristic = HashMap::new();
        heuristic.insert(Vid::from(0), 2.0);
        heuristic.insert(Vid::from(1), 1.0);
        heuristic.insert(Vid::from(2), 0.0);

        let config = AStarConfig {
            source: Vid::from(0),
            target: Vid::from(2),
            heuristic,
        };

        let result = AStar::run(&graph, config);

        assert_eq!(result.distance, Some(2.0));
        assert!(result.path.is_some());
        let path = result.path.unwrap();
        assert_eq!(path, vec![Vid::from(0), Vid::from(1), Vid::from(2)]);
    }

    #[test]
    fn test_astar_heuristic_guides() {
        // 0 -> 1 -> 3 (cost 1+1=2)
        // 0 -> 2 -> 3 (cost 1+10=11)
        // Heuristic favors 2? h(1)=100, h(2)=0
        // A* should still find optimal path 0->1->3 even if heuristic initially guides to 2?
        // Actually, if heuristic is admissible (h(n) <= true cost), it finds optimal.
        // If h(1)=100, it's > true cost (1), so not admissible.
        // But let's check if it explores 2 first.

        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2), Vid::from(3)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(3)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(2), Vid::from(3)),
        ];

        // We need weighted graph for this test to be interesting, but build_test_graph is unweighted.
        // With unweighted, both paths are len 2.
        // Let's use heuristic to guide order.

        let graph = build_test_graph(vids, edges);

        // h(1) = 0.5, h(2) = 0.1
        // f(1) = g(1)+h(1) = 1 + 0.5 = 1.5
        // f(2) = g(2)+h(2) = 1 + 0.1 = 1.1
        // Should expand 2 first.

        let mut heuristic = HashMap::new();
        heuristic.insert(Vid::from(1), 0.5);
        heuristic.insert(Vid::from(2), 0.1);
        heuristic.insert(Vid::from(3), 0.0);

        let config = AStarConfig {
            source: Vid::from(0),
            target: Vid::from(3),
            heuristic,
        };

        let result = AStar::run(&graph, config);
        assert_eq!(result.distance, Some(2.0));
        // Path could be either, but A* with correct heuristic finds optimal.
    }
}
