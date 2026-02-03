// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Minimum Spanning Tree (MST) Algorithm.
//!
//! Uses Kruskal's algorithm to find the Minimum Spanning Tree of a weighted graph.
//! Treating graph as undirected (if include_reverse is true, we dedup edges; if not, we treat directed as undirected structure).
//! Returns the edges in the MST and total weight.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct MinimumSpanningTree;

#[derive(Debug, Clone, Default)]
pub struct MstConfig {
    // If true, treats graph as undirected by considering u->v and v->u as same edge.
    // If false, treats directed edges, but MST is usually defined for undirected.
    // Kruskal's works on edges.
}

pub struct MstResult {
    pub edges: Vec<(Vid, Vid, f64)>, // (u, v, weight)
    pub total_weight: f64,
}

impl Algorithm for MinimumSpanningTree {
    type Config = MstConfig;
    type Result = MstResult;

    fn name() -> &'static str {
        "mst"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return MstResult {
                edges: Vec::new(),
                total_weight: 0.0,
            };
        }

        // Collect all edges
        // If we want to treat graph as undirected, we should dedup (u, v) and (v, u).
        // Standard approach: normalize (min, max).
        let mut edges = Vec::new();
        for u in 0..n as u32 {
            for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
                // If treating as undirected, only add if u < v to avoid duplicates
                // assuming symmetry. If not symmetric, Kruskal's on directed graph
                // produces Minimum Spanning Forest (Arborescence is different).
                // Let's assume undirected MST on the underlying graph structure.
                if u < v {
                    let weight = if graph.has_weights() {
                        graph.out_weight(u, i)
                    } else {
                        1.0
                    };
                    edges.push((u, v, weight));
                }
            }
        }

        // Sort by weight
        edges.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());

        // Union-Find
        let mut parent: Vec<u32> = (0..n as u32).collect();
        let mut rank: Vec<u8> = vec![0; n];

        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                parent[x as usize] = parent[parent[x as usize] as usize];
                x = parent[x as usize];
            }
            x
        }

        fn union(parent: &mut [u32], rank: &mut [u8], x: u32, y: u32) -> bool {
            let px = find(parent, x);
            let py = find(parent, y);
            if px == py {
                return false;
            }
            match rank[px as usize].cmp(&rank[py as usize]) {
                std::cmp::Ordering::Less => parent[px as usize] = py,
                std::cmp::Ordering::Greater => parent[py as usize] = px,
                std::cmp::Ordering::Equal => {
                    parent[py as usize] = px;
                    rank[px as usize] += 1;
                }
            }
            true
        }

        let mut mst_edges = Vec::new();
        let mut total_weight = 0.0;

        for (u, v, w) in edges {
            if union(&mut parent, &mut rank, u, v) {
                mst_edges.push((graph.to_vid(u), graph.to_vid(v), w));
                total_weight += w;
            }
        }

        MstResult {
            edges: mst_edges,
            total_weight,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_mst_simple() {
        // 0-1 (1.0), 1-2 (2.0), 0-2 (10.0)
        // MST should be (0,1) and (1,2) => weight 3.0
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(0), Vid::from(2)),
        ];
        // build_test_graph does not support weights yet.
        // I need to update build_test_graph or create a new helper.
        // Or I can modify GraphProjection field directly since it's pub(crate) and I'm in same crate.

        let mut graph = build_test_graph(vids, edges);
        // Inject weights manually
        // Edges in build_test_graph are added in order of iteration over `edges`.
        // Order: (0,1), (1,2), (0,2).
        // Node 0: out_neighbors [1, 2]
        // Node 1: out_neighbors [2]
        // Node 2: []
        // We need to match this structure.

        // 0->1 (idx 0 for node 0) -> weight 1.0
        // 0->2 (idx 1 for node 0) -> weight 10.0
        // 1->2 (idx 0 for node 1) -> weight 2.0

        // Flattened weights vector for GraphProjection:
        // Node 0 edges, then Node 1 edges, etc.
        // Node 0: [1.0, 10.0]
        // Node 1: [2.0]
        // Node 2: []

        graph.out_weights = Some(vec![1.0, 10.0, 2.0]);

        let result = MinimumSpanningTree::run(&graph, MstConfig::default());
        assert_eq!(result.total_weight, 3.0);
        assert_eq!(result.edges.len(), 2);
    }
}
