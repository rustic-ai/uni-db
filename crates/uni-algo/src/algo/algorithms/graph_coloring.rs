// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph Coloring Algorithm (DSatur).
//!
//! Assigns colors (integers) to vertices such that no two adjacent vertices share the same color.
//! Uses DSatur heuristic.

use crate::algo::algorithms::Algorithm;

use crate::algo::GraphProjection;

use std::collections::HashSet;

use uni_common::core::id::Vid;

pub struct GraphColoring;

#[derive(Debug, Clone, Default)]

pub struct GraphColoringConfig {}

pub struct GraphColoringResult {
    pub coloring: Vec<(Vid, u32)>,

    pub chromatic_number: u32,
}

impl Algorithm for GraphColoring {
    type Config = GraphColoringConfig;

    type Result = GraphColoringResult;

    fn name() -> &'static str {
        "graph_coloring"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();

        if n == 0 {
            return GraphColoringResult {
                coloring: Vec::new(),

                chromatic_number: 0,
            };
        }

        let mut colors = vec![None; n];

        let mut saturation = vec![0; n]; // Count of distinct neighbor colors

        let mut neighbor_colors: Vec<HashSet<u32>> = vec![HashSet::new(); n];

        let mut degree = vec![0; n];

        for (i, d) in degree.iter_mut().enumerate().take(n) {
            *d = graph.out_degree(i as u32)
                + if graph.has_reverse() {
                    graph.in_degree(i as u32)
                } else {
                    0
                };
        }

        let mut uncolored_count = n;

        while uncolored_count > 0 {
            // Select vertex: max saturation, then max degree

            let mut best_u = None;

            let mut max_sat = -1;

            let mut max_deg = -1;

            for i in 0..n {
                if colors[i].is_none() {
                    let sat = saturation[i] as i32;

                    let deg = degree[i] as i32;

                    if sat > max_sat || (sat == max_sat && deg > max_deg) {
                        max_sat = sat;

                        max_deg = deg;

                        best_u = Some(i);
                    }
                }
            }

            let u = best_u.unwrap();

            // Assign lowest available color

            let mut color = 0;

            while neighbor_colors[u].contains(&color) {
                color += 1;
            }

            colors[u] = Some(color);

            uncolored_count -= 1;

            // Update neighbors

            let mut update_neighbor = |v: u32| {
                let v = v as usize;

                if colors[v].is_none() && neighbor_colors[v].insert(color) {
                    saturation[v] += 1;
                }
            };

            for &v in graph.out_neighbors(u as u32) {
                update_neighbor(v);
            }

            if graph.has_reverse() {
                for &v in graph.in_neighbors(u as u32) {
                    update_neighbor(v);
                }
            }
        }

        let max_color = colors.iter().filter_map(|c| *c).max().unwrap_or(0);
        let result_coloring = colors
            .into_iter()
            .enumerate()
            .map(|(i, c)| (graph.to_vid(i as u32), c.unwrap()))
            .collect();

        GraphColoringResult {
            coloring: result_coloring,
            chromatic_number: max_color + 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_coloring_triangle() {
        // Triangle needs 3 colors
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(0)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(1)),
            (Vid::from(2), Vid::from(0)),
            (Vid::from(0), Vid::from(2)),
        ];
        let graph = build_test_graph(vids, edges); // Assuming include_reverse logic if graph directed?
        // DSatur treats adjacency. My implementation checks out and in neighbors.

        let result = GraphColoring::run(&graph, GraphColoringConfig::default());
        assert_eq!(result.chromatic_number, 3);
    }
}
