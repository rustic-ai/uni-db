// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cycle Detection Algorithm.
//!
//! Uses DFS to detect cycles in a directed graph.
//! Returns `true` and the nodes involved in the first detected cycle.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct CycleDetection;

#[derive(Debug, Clone, Default)]
pub struct CycleDetectionConfig {}

pub struct CycleDetectionResult {
    pub has_cycle: bool,
    pub cycle_nodes: Vec<Vid>,
}

impl Algorithm for CycleDetection {
    type Config = CycleDetectionConfig;
    type Result = CycleDetectionResult;

    fn name() -> &'static str {
        "cycle_detection"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return CycleDetectionResult {
                has_cycle: false,
                cycle_nodes: Vec::new(),
            };
        }

        // 0: unvisited, 1: visiting, 2: visited
        let mut state = vec![0u8; n];
        let mut parent = vec![None; n];

        for v in 0..n {
            if state[v] == 0
                && let Some(cycle) = find_cycle(v as u32, graph, &mut state, &mut parent)
            {
                let cycle_vids = cycle.into_iter().map(|idx| graph.to_vid(idx)).collect();
                return CycleDetectionResult {
                    has_cycle: true,
                    cycle_nodes: cycle_vids,
                };
            }
        }

        CycleDetectionResult {
            has_cycle: false,
            cycle_nodes: Vec::new(),
        }
    }
}

fn find_cycle(
    u: u32,
    graph: &GraphProjection,
    state: &mut [u8],
    parent: &mut [Option<u32>],
) -> Option<Vec<u32>> {
    state[u as usize] = 1; // visiting

    for &v in graph.out_neighbors(u) {
        if state[v as usize] == 1 {
            // Cycle detected! Backtrack from u to v using parent pointers
            let mut cycle = Vec::new();
            cycle.push(v);
            let mut curr = u;
            while curr != v {
                cycle.push(curr);
                curr = parent[curr as usize].unwrap();
            }
            cycle.push(v); // Close the loop
            cycle.reverse();
            return Some(cycle);
        } else if state[v as usize] == 0 {
            parent[v as usize] = Some(u);
            if let Some(cycle) = find_cycle(v, graph, state, parent) {
                return Some(cycle);
            }
        }
    }

    state[u as usize] = 2; // visited
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_cycle_detection_no_cycle() {
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges);

        let result = CycleDetection::run(&graph, CycleDetectionConfig::default());
        assert!(!result.has_cycle);
    }

    #[test]
    fn test_cycle_detection_simple_cycle() {
        // 0 -> 1 -> 0
        let vids = vec![Vid::from(0), Vid::from(1)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(0))];
        let graph = build_test_graph(vids, edges);

        let result = CycleDetection::run(&graph, CycleDetectionConfig::default());
        assert!(result.has_cycle);
        assert_eq!(result.cycle_nodes.len(), 3); // 0, 1, 0

        // Cycle nodes should be a valid cycle
        let first = result.cycle_nodes[0];
        let last = result.cycle_nodes[result.cycle_nodes.len() - 1];
        assert_eq!(first, last);
    }
}
