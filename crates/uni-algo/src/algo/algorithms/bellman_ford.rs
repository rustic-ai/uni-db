// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bellman-Ford Algorithm.
//!
//! Computes shortest paths from a source node to all other nodes in a weighted graph.
//! Unlike Dijkstra, it handles negative edge weights.
//! Detects negative cycles.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct BellmanFord;

#[derive(Debug, Clone)]
pub struct BellmanFordConfig {
    pub source: Vid,
}

impl Default for BellmanFordConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
        }
    }
}

pub struct BellmanFordResult {
    pub distances: Vec<(Vid, f64)>,
    pub has_negative_cycle: bool,
}

impl Algorithm for BellmanFord {
    type Config = BellmanFordConfig;
    type Result = BellmanFordResult;

    fn name() -> &'static str {
        "bellman_ford"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source_slot = match graph.to_slot(config.source) {
            Some(slot) => slot,
            None => {
                return BellmanFordResult {
                    distances: Vec::new(),
                    has_negative_cycle: false,
                };
            }
        };

        let n = graph.vertex_count();
        let mut dist = vec![f64::INFINITY; n];
        dist[source_slot as usize] = 0.0;

        // Relax V-1 times
        for _ in 0..n - 1 {
            let mut changed = false;
            for u in 0..n {
                if dist[u] == f64::INFINITY {
                    continue;
                }

                // Iterate over edges (u, v)
                for (i, &v_slot) in graph.out_neighbors(u as u32).iter().enumerate() {
                    let v = v_slot as usize;
                    let weight = if graph.has_weights() {
                        graph.out_weight(u as u32, i)
                    } else {
                        1.0
                    };

                    if dist[u] + weight < dist[v] {
                        dist[v] = dist[u] + weight;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        // Check for negative cycles
        let mut has_negative_cycle = false;
        for u in 0..n {
            if dist[u] == f64::INFINITY {
                continue;
            }
            for (i, &v_slot) in graph.out_neighbors(u as u32).iter().enumerate() {
                let v = v_slot as usize;
                let weight = if graph.has_weights() {
                    graph.out_weight(u as u32, i)
                } else {
                    1.0
                };

                if dist[u] + weight < dist[v] {
                    has_negative_cycle = true;
                    break;
                }
            }
            if has_negative_cycle {
                break;
            }
        }

        if has_negative_cycle {
            BellmanFordResult {
                distances: Vec::new(),
                has_negative_cycle: true,
            }
        } else {
            let results = dist
                .into_iter()
                .enumerate()
                .filter(|(_, d)| *d < f64::INFINITY)
                .map(|(slot, d)| (graph.to_vid(slot as u32), d))
                .collect();

            BellmanFordResult {
                distances: results,
                has_negative_cycle: false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_bellman_ford_simple() {
        // 0 -> 1 (1.0), 1 -> 2 (1.0)
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges); // weight 1.0

        let config = BellmanFordConfig {
            source: Vid::from(0),
        };
        let result = BellmanFord::run(&graph, config);

        assert!(!result.has_negative_cycle);
        let dists: std::collections::HashMap<_, _> = result.distances.into_iter().collect();
        assert_eq!(dists[&Vid::from(0)], 0.0);
        assert_eq!(dists[&Vid::from(1)], 1.0);
        assert_eq!(dists[&Vid::from(2)], 2.0);
    }

    // Note: build_test_graph doesn't support custom weights yet.
    // We would need a custom builder test utility to test negative cycles properly.
    // For now, testing logic structure with default weights.
}
