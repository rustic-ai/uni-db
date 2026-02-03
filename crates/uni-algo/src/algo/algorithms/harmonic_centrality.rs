// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Harmonic Centrality Algorithm.
//!
//! A variant of Closeness Centrality that deals with infinite distances by summing
//! the inverse of distances: H(u) = sum(1 / d(u, v)) for v != u.
//! Uses BFS (unweighted) or Dijkstra (weighted) from every node.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use uni_common::core::id::Vid;

pub struct HarmonicCentrality;

#[derive(Debug, Clone, Default)]
pub struct HarmonicCentralityConfig {}

pub struct HarmonicCentralityResult {
    pub scores: Vec<(Vid, f64)>,
}

impl Algorithm for HarmonicCentrality {
    type Config = HarmonicCentralityConfig;
    type Result = HarmonicCentralityResult;

    fn name() -> &'static str {
        "harmonic_centrality"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return HarmonicCentralityResult { scores: Vec::new() };
        }

        let scores: Vec<(Vid, f64)> = (0..n)
            .into_par_iter()
            .map(|i| {
                let score = compute_harmonic_score(graph, i as u32);
                (graph.to_vid(i as u32), score)
            })
            .collect();

        HarmonicCentralityResult { scores }
    }
}

fn compute_harmonic_score(graph: &GraphProjection, start: u32) -> f64 {
    let n = graph.vertex_count();
    let mut dist = vec![f64::INFINITY; n];
    let mut heap = BinaryHeap::new();

    dist[start as usize] = 0.0;
    heap.push(Reverse((0.0f64.to_bits(), start)));

    let mut sum_inv_dist = 0.0;

    while let Some(Reverse((d_bits, u))) = heap.pop() {
        let d = f64::from_bits(d_bits);
        if d > dist[u as usize] {
            continue;
        }

        if u != start && d > 0.0 {
            sum_inv_dist += 1.0 / d;
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

    sum_inv_dist
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_harmonic_centrality_simple() {
        // 0 -> 1 -> 2
        // H(0) = 1/1 + 1/2 = 1.5
        // H(1) = 1/1 = 1.0
        // H(2) = 0
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_test_graph(vids, edges);

        let result = HarmonicCentrality::run(&graph, HarmonicCentralityConfig::default());
        let map: std::collections::HashMap<_, _> = result.scores.into_iter().collect();

        assert!((map[&Vid::from(0)] - 1.5).abs() < 1e-6);
        assert!((map[&Vid::from(1)] - 1.0).abs() < 1e-6);
        assert!((map[&Vid::from(2)] - 0.0).abs() < 1e-6);
    }
}
