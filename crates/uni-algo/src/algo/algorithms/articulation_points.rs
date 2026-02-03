// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Articulation Points Algorithm.
//!
//! Finds articulation points (cut vertices) in the graph. Removing an articulation point
//! increases the number of connected components.
//! This implementation treats the graph as undirected if `include_reverse` was used in projection.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct ArticulationPoints;

#[derive(Debug, Clone, Default)]
pub struct ArticulationPointsConfig {}

pub struct ArticulationPointsResult {
    pub points: Vec<Vid>,
}

impl Algorithm for ArticulationPoints {
    type Config = ArticulationPointsConfig;
    type Result = ArticulationPointsResult;

    fn name() -> &'static str {
        "articulation_points"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return ArticulationPointsResult { points: Vec::new() };
        }

        let mut disc = vec![u32::MAX; n];
        let mut low = vec![u32::MAX; n];
        let mut time = 0;
        let mut is_ap = vec![false; n];

        for i in 0..n {
            if disc[i] == u32::MAX {
                find_aps(
                    i as u32,
                    u32::MAX, // no parent
                    graph,
                    &mut disc,
                    &mut low,
                    &mut time,
                    &mut is_ap,
                );
            }
        }

        let points = is_ap
            .into_iter()
            .enumerate()
            .filter(|(_, is_ap)| *is_ap)
            .map(|(i, _)| graph.to_vid(i as u32))
            .collect();

        ArticulationPointsResult { points }
    }
}

fn find_aps(
    u: u32,
    p: u32, // parent
    graph: &GraphProjection,
    disc: &mut [u32],
    low: &mut [u32],
    time: &mut u32,
    is_ap: &mut [bool],
) {
    let mut children = 0;
    disc[u as usize] = *time;
    low[u as usize] = *time;
    *time += 1;

    let mut process_neighbor = |v: u32| {
        if v == p {
            return;
        }
        if disc[v as usize] != u32::MAX {
            low[u as usize] = std::cmp::min(low[u as usize], disc[v as usize]);
        } else {
            children += 1;
            find_aps(v, u, graph, disc, low, time, is_ap);
            low[u as usize] = std::cmp::min(low[u as usize], low[v as usize]);
            if p != u32::MAX && low[v as usize] >= disc[u as usize] {
                is_ap[u as usize] = true;
            }
        }
    };

    // Iterate out neighbors
    for &v in graph.out_neighbors(u) {
        process_neighbor(v);
    }

    // Iterate in neighbors
    if graph.has_reverse() {
        for &v in graph.in_neighbors(u) {
            process_neighbor(v);
        }
    }

    if p == u32::MAX && children > 1 {
        is_ap[u as usize] = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_articulation_points_line() {
        // 0 - 1 - 2
        // 1 is AP.
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges);

        let result = ArticulationPoints::run(&graph, ArticulationPointsConfig::default());
        assert_eq!(result.points.len(), 1);
        assert_eq!(result.points[0], Vid::from(1));
    }

    #[test]
    fn test_articulation_points_cycle() {
        // 0 - 1 - 2 - 0
        // No APs.
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(0)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = ArticulationPoints::run(&graph, ArticulationPointsConfig::default());
        assert_eq!(result.points.len(), 0);
    }
}
