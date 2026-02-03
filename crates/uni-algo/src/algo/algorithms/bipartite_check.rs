// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bipartite Check Algorithm.
//!
//! Checks if a graph is bipartite (2-colorable) using BFS.
//! Returns `true` if bipartite, and the partition (0 or 1) for each node.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct BipartiteCheck;

#[derive(Debug, Clone, Default)]
pub struct BipartiteCheckConfig {}

pub struct BipartiteCheckResult {
    pub is_bipartite: bool,
    pub partition: Vec<(Vid, u8)>, // (node, color: 0 or 1)
}

impl Algorithm for BipartiteCheck {
    type Config = BipartiteCheckConfig;
    type Result = BipartiteCheckResult;

    fn name() -> &'static str {
        "bipartite_check"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return BipartiteCheckResult {
                is_bipartite: true,
                partition: Vec::new(),
            };
        }

        // 0: uncolored, 1: color A, 2: color B
        let mut colors = vec![0u8; n];
        let mut is_bipartite = true;

        for start_node in 0..n {
            if colors[start_node] != 0 {
                continue;
            }

            let mut queue = std::collections::VecDeque::new();
            queue.push_back(start_node as u32);
            colors[start_node] = 1;

            while let Some(u) = queue.pop_front() {
                let current_color = colors[u as usize];
                let next_color = if current_color == 1 { 2 } else { 1 };

                // Check out-neighbors (treat as undirected for bipartite check typically,
                // but if strictly directed, we only check forward edges?
                // Bipartite usually implies underlying undirected graph structure.
                // If strictly directed, a cycle might not matter if directions align.
                // Standard definition: "vertices can be divided into two disjoint and independent sets"
                // This usually applies to undirected edges.
                // Let's assume undirected semantics by checking both directions if possible,
                // or just outgoing if directed graph semantics.
                // Uni graphs are directed. But bipartite check usually implies structure.
                // Let's check outgoing edges. If graph represents undirected, user should have added reverse edges.
                // Actually, `GraphProjection` can have reverse edges if requested.
                // If `include_reverse` is false, we only check outgoing.

                for &v in graph.out_neighbors(u) {
                    if colors[v as usize] == 0 {
                        colors[v as usize] = next_color;
                        queue.push_back(v);
                    } else if colors[v as usize] == current_color {
                        is_bipartite = false;
                        // Don't break immediately if we want to color the rest of the component?
                        // But result is invalid.
                        // We can stop or continue. Let's continue to fill partition map best-effort or stop?
                        // The result says "is_bipartite: false". Partition might be partial/invalid.
                        // Let's continue to process this component to finish coloring for debug, but mark false.
                    }
                }

                // If we had reverse edges (undirected check), we would check in_neighbors too.
                if graph.has_reverse() {
                    for &v in graph.in_neighbors(u) {
                        if colors[v as usize] == 0 {
                            colors[v as usize] = next_color;
                            queue.push_back(v);
                        } else if colors[v as usize] == current_color {
                            is_bipartite = false;
                        }
                    }
                }
            }
        }

        let partition = if is_bipartite {
            colors
                .into_iter()
                .enumerate()
                .filter(|(_, c)| *c != 0)
                .map(|(i, c)| (graph.to_vid(i as u32), c - 1))
                .collect()
        } else {
            Vec::new() // Or return partial partition?
        };

        BipartiteCheckResult {
            is_bipartite,
            partition,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_bipartite_check_true() {
        // 0 -> 1, 1 -> 2 (0 and 2 are color A, 1 is color B)
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges);

        let result = BipartiteCheck::run(&graph, BipartiteCheckConfig::default());
        assert!(result.is_bipartite);
        assert_eq!(result.partition.len(), 3);
    }

    #[test]
    fn test_bipartite_check_false_triangle() {
        // 0 -> 1 -> 2 -> 0
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(0)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = BipartiteCheck::run(&graph, BipartiteCheckConfig::default());
        assert!(!result.is_bipartite);
    }
}
