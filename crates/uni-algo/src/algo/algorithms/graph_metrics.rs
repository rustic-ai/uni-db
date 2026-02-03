// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph Metrics Algorithm.
//!
//! Computes structural metrics:
//! - Eccentricity for each node
//! - Diameter (max eccentricity)
//! - Radius (min eccentricity)
//! - Center (nodes with min eccentricity)
//! - Periphery (nodes with max eccentricity)
//!
//! Uses BFS (unweighted) or Dijkstra (weighted) from every node.
//! Complexity: O(V * (V + E)) or O(V * (V + E) * log V).

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use uni_common::core::id::Vid;

pub struct GraphMetrics;

#[derive(Debug, Clone, Default)]
pub struct GraphMetricsConfig {}

pub struct GraphMetricsResult {
    pub diameter: f64,
    pub radius: f64,
    pub eccentricities: Vec<(Vid, f64)>,
    pub center: Vec<Vid>,
    pub periphery: Vec<Vid>,
}

impl Algorithm for GraphMetrics {
    type Config = GraphMetricsConfig;
    type Result = GraphMetricsResult;

    fn name() -> &'static str {
        "graph_metrics"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return GraphMetricsResult {
                diameter: 0.0,
                radius: 0.0,
                eccentricities: Vec::new(),
                center: Vec::new(),
                periphery: Vec::new(),
            };
        }

        // Compute eccentricity for all nodes in parallel
        let eccentricities: Vec<f64> = (0..n)
            .into_par_iter()
            .map(|i| compute_eccentricity(graph, i as u32))
            .collect();

        // Filter out INFINITY (disconnected components)
        // Usually metrics are defined for connected component or max of components.
        // If graph is disconnected, eccentricity is technically infinity?
        // Standard definition usually considers the connected component or returns infinity.
        // For practical purposes, let's ignore unreachable nodes (or max reachable distance).
        // Convention: Eccentricity is max distance to any *reachable* node? No, standard is max distance.
        // If not connected, diameter is infinity.
        // Let's stick to max reachable distance (or 0 if isolated) to avoid infinity messing up stats?
        // Or better: Max distance in the component of v.

        let valid_eccs: Vec<f64> = eccentricities
            .iter()
            .copied()
            .filter(|&e| e < f64::INFINITY)
            .collect();

        if valid_eccs.is_empty() {
            return GraphMetricsResult {
                diameter: 0.0,
                radius: 0.0,
                eccentricities: Vec::new(),
                center: Vec::new(),
                periphery: Vec::new(),
            };
        }

        let diameter = valid_eccs.iter().fold(0.0f64, |a, &b| a.max(b));
        let radius = valid_eccs.iter().fold(f64::INFINITY, |a, &b| a.min(b));

        let mut center = Vec::new();
        let mut periphery = Vec::new();
        let mut ecc_mapped = Vec::new();

        for (i, &ecc) in eccentricities.iter().enumerate() {
            if ecc < f64::INFINITY {
                ecc_mapped.push((graph.to_vid(i as u32), ecc));
                if (ecc - radius).abs() < 1e-9 {
                    center.push(graph.to_vid(i as u32));
                }
                if (ecc - diameter).abs() < 1e-9 {
                    periphery.push(graph.to_vid(i as u32));
                }
            }
        }

        GraphMetricsResult {
            diameter,
            radius,
            eccentricities: ecc_mapped,
            center,
            periphery,
        }
    }
}

fn compute_eccentricity(graph: &GraphProjection, start: u32) -> f64 {
    // Dijkstra (handles weighted and unweighted if weights=1.0)
    let n = graph.vertex_count();
    let mut dist = vec![f64::INFINITY; n];
    let mut heap = BinaryHeap::new();

    dist[start as usize] = 0.0;
    heap.push(Reverse((0.0f64.to_bits(), start)));

    let mut max_dist = 0.0;

    while let Some(Reverse((d_bits, u))) = heap.pop() {
        let d = f64::from_bits(d_bits);
        if d > dist[u as usize] {
            continue;
        }
        if d > max_dist {
            max_dist = d;
        }

        for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
            let weight = if graph.has_weights() {
                graph.out_weight(u, i)
            } else {
                1.0
            };
            let new_dist = d + weight;
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                heap.push(Reverse((new_dist.to_bits(), v)));
            }
        }
    }

    // Check if we visited all? If graph is disconnected, max_dist is max of connected component.
    // If we strictly define eccentricity as max distance to ANY node, it's infinity if disconnected.
    // But for "Graph Metrics" in tools like Gephi/Neo4j, usually it's per component or largest component.
    // Let's return max_dist (max reachable).

    max_dist
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_metrics_line() {
        // 0-1-2-3
        // Ecc: 0->3, 1->2, 2->2, 3->3
        // Radius: 2 (nodes 1, 2)
        // Diameter: 3
        // Center: 1, 2
        // Periphery: 0, 3

        let vids = (0..4).map(Vid::from).collect();
        // Undirected structure via bidirectional edges for testing metrics logic
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(0)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(1)),
            (Vid::from(2), Vid::from(3)),
            (Vid::from(3), Vid::from(2)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = GraphMetrics::run(&graph, GraphMetricsConfig::default());

        assert_eq!(result.diameter, 3.0);
        assert_eq!(result.radius, 2.0);

        assert!(result.center.contains(&Vid::from(1)));
        assert!(result.center.contains(&Vid::from(2)));
        assert_eq!(result.center.len(), 2);

        assert!(result.periphery.contains(&Vid::from(0)));
        assert!(result.periphery.contains(&Vid::from(3)));
        assert_eq!(result.periphery.len(), 2);
    }
}
