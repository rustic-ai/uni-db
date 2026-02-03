// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Topological Sort Algorithm.
//!
//! Uses Kahn's algorithm to produce a linear ordering of vertices such that
//! for every directed edge from u to v, vertex u comes before v in the ordering.
//!
//! If the graph contains a cycle, the topological sort is not possible and
//! `has_cycle` will be true.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct TopologicalSort;

#[derive(Debug, Clone, Default)]
pub struct TopologicalSortConfig {}

pub struct TopologicalSortResult {
    pub sorted_nodes: Vec<Vid>,
    pub has_cycle: bool,
}

impl Algorithm for TopologicalSort {
    type Config = TopologicalSortConfig;
    type Result = TopologicalSortResult;

    fn name() -> &'static str {
        "topological_sort"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return TopologicalSortResult {
                sorted_nodes: Vec::new(),
                has_cycle: false,
            };
        }

        // 1. Compute in-degrees
        let mut in_degree = vec![0u32; n];
        for v in 0..n as u32 {
            for &neighbor in graph.out_neighbors(v) {
                in_degree[neighbor as usize] += 1;
            }
        }

        // 2. Initialize queue with nodes having in-degree 0
        let mut queue = std::collections::VecDeque::new();
        for (v, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                queue.push_back(v as u32);
            }
        }

        let mut sorted_indices = Vec::with_capacity(n);

        // 3. Process
        while let Some(u) = queue.pop_front() {
            sorted_indices.push(u);

            for &v in graph.out_neighbors(u) {
                in_degree[v as usize] -= 1;
                if in_degree[v as usize] == 0 {
                    queue.push_back(v);
                }
            }
        }

        // 4. Check for cycles
        if sorted_indices.len() != n {
            TopologicalSortResult {
                sorted_nodes: Vec::new(),
                has_cycle: true,
            }
        } else {
            let sorted_nodes = sorted_indices
                .into_iter()
                .map(|idx| graph.to_vid(idx))
                .collect();
            TopologicalSortResult {
                sorted_nodes,
                has_cycle: false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_topological_sort_dag() {
        // 0 -> 1 -> 2
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];

        let graph = build_test_graph(vids, edges);

        let result = TopologicalSort::run(&graph, TopologicalSortConfig::default());

        assert!(!result.has_cycle);
        assert_eq!(result.sorted_nodes.len(), 3);

        let pos0 = result
            .sorted_nodes
            .iter()
            .position(|&v| v == Vid::from(0))
            .unwrap();
        let pos1 = result
            .sorted_nodes
            .iter()
            .position(|&v| v == Vid::from(1))
            .unwrap();
        let pos2 = result
            .sorted_nodes
            .iter()
            .position(|&v| v == Vid::from(2))
            .unwrap();

        assert!(pos0 < pos1);
        assert!(pos1 < pos2);
    }

    #[test]
    fn test_topological_sort_cycle() {
        // 0 -> 1 -> 0
        let vids = vec![Vid::from(0), Vid::from(1)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(0))];

        let graph = build_test_graph(vids, edges);

        let result = TopologicalSort::run(&graph, TopologicalSortConfig::default());

        assert!(result.has_cycle);
        assert!(result.sorted_nodes.is_empty());
    }
}
