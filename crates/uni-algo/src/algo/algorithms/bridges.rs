// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bridge Detection Algorithm.
//!
//! Finds bridges (cut-edges) in the graph. Removing a bridge increases the number of connected components.
//! This implementation treats the graph as undirected if `include_reverse` was used in projection.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct Bridges;

#[derive(Debug, Clone, Default)]
pub struct BridgesConfig {}

pub struct BridgesResult {
    pub bridges: Vec<(Vid, Vid)>,
}

impl Algorithm for Bridges {
    type Config = BridgesConfig;
    type Result = BridgesResult;

    fn name() -> &'static str {
        "bridges"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return BridgesResult {
                bridges: Vec::new(),
            };
        }

        let mut disc = vec![u32::MAX; n];
        let mut low = vec![u32::MAX; n];
        let mut time = 0;
        let mut bridges = Vec::new();

        for i in 0..n {
            if disc[i] == u32::MAX {
                find_bridges(
                    i as u32,
                    u32::MAX, // no parent
                    graph,
                    &mut disc,
                    &mut low,
                    &mut time,
                    &mut bridges,
                );
            }
        }

        let mapped_bridges = bridges
            .into_iter()
            .map(|(u, v)| (graph.to_vid(u), graph.to_vid(v)))
            .collect();

        BridgesResult {
            bridges: mapped_bridges,
        }
    }
}

fn find_bridges(
    u: u32,
    p: u32, // parent
    graph: &GraphProjection,
    disc: &mut [u32],
    low: &mut [u32],
    time: &mut u32,
    bridges: &mut Vec<(u32, u32)>,
) {
    disc[u as usize] = *time;
    low[u as usize] = *time;
    *time += 1;

    // Helper to process neighbor
    let mut process_neighbor = |v: u32| {
        if v == p {
            return;
        }
        if disc[v as usize] != u32::MAX {
            low[u as usize] = std::cmp::min(low[u as usize], disc[v as usize]);
        } else {
            find_bridges(v, u, graph, disc, low, time, bridges);
            low[u as usize] = std::cmp::min(low[u as usize], low[v as usize]);
            if low[v as usize] > disc[u as usize] {
                bridges.push((u, v));
            }
        }
    };

    // Iterate out neighbors
    for &v in graph.out_neighbors(u) {
        process_neighbor(v);
    }

    // Iterate in neighbors (if available, treating as undirected)
    if graph.has_reverse() {
        for &v in graph.in_neighbors(u) {
            process_neighbor(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_bridges_simple() {
        // 0 - 1 - 2
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        // Undirected: need both directions for build_test_graph usually,
        // or ensure algorithm treats directed as undirected.
        // Our algorithm does treat directed edges.
        // If we want undirected behavior test, we should provide edges such that graph connects.
        // 0->1, 1->2.
        // Bridges: (0,1), (1,2)
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges); // directed

        let result = Bridges::run(&graph, BridgesConfig::default());
        assert_eq!(result.bridges.len(), 2);
    }

    #[test]
    fn test_bridges_cycle() {
        // 0 - 1 - 2 - 0 (Triangle)
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(0)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = Bridges::run(&graph, BridgesConfig::default());
        assert_eq!(result.bridges.len(), 0);
    }

    #[test]
    fn test_bridges_dumbbell() {
        // 0-1-2-3, 2-4-5-2 (Loop at end)
        // 0->1 (bridge)
        // 1->2 (bridge)
        // 2->3 (bridge)
        // 2->4, 4->5, 5->2 (cycle)
        // Expected bridges: (0,1), (1,2), (2,3)
        // Note: graph is directed in test utils.

        let vids = (0..6).map(Vid::from).collect();
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(3)),
            (Vid::from(2), Vid::from(4)),
            (Vid::from(4), Vid::from(5)),
            (Vid::from(5), Vid::from(2)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = Bridges::run(&graph, BridgesConfig::default());
        assert_eq!(result.bridges.len(), 3);
        // Check contents?
    }
}
