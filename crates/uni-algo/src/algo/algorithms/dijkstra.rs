// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Dijkstra's Shortest Path Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use uni_common::core::id::Vid;

pub struct Dijkstra;

#[derive(Debug, Clone)]
pub struct DijkstraConfig {
    pub source: Vid,
    pub target: Option<Vid>,
    pub max_distance: Option<f64>,
}

impl Default for DijkstraConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            target: None,
            max_distance: None,
        }
    }
}

pub struct DijkstraResult {
    pub distances: Vec<(Vid, f64)>,
    pub path: Option<Vec<Vid>>,
}

impl Algorithm for Dijkstra {
    type Config = DijkstraConfig;
    type Result = DijkstraResult;

    fn name() -> &'static str {
        "shortestPath"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source_slot = match graph.to_slot(config.source) {
            Some(slot) => slot,
            None => {
                return DijkstraResult {
                    distances: Vec::new(),
                    path: None,
                };
            }
        };

        let n = graph.vertex_count();
        let mut dist = vec![f64::INFINITY; n];
        let mut prev: Vec<Option<u32>> = vec![None; n];
        let mut heap = BinaryHeap::new();

        dist[source_slot as usize] = 0.0;
        heap.push(Reverse((0.0f64.to_bits(), source_slot)));

        let target_slot = config.target.and_then(|t| graph.to_slot(t));

        while let Some(Reverse((d_bits, u))) = heap.pop() {
            let d = f64::from_bits(d_bits);
            if d > dist[u as usize] {
                continue;
            }

            // Early exit for point-to-point
            if target_slot == Some(u) {
                break;
            }

            // Max distance cutoff
            if let Some(max_d) = config.max_distance
                && d > max_d
            {
                continue;
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
                    prev[v as usize] = Some(u);
                    heap.push(Reverse((new_dist.to_bits(), v)));
                }
            }
        }

        // Reconstruct path if target specified
        let mut path = None;
        if let Some(t_slot) = target_slot
            && dist[t_slot as usize] < f64::INFINITY
        {
            let mut p = Vec::new();
            let mut curr = Some(t_slot);
            while let Some(slot) = curr {
                p.push(graph.to_vid(slot));
                if slot == source_slot {
                    break;
                }
                curr = prev[slot as usize];
            }
            p.reverse();
            path = Some(p);
        }

        let results = dist
            .into_iter()
            .enumerate()
            .filter(|(_, d)| *d < f64::INFINITY)
            .map(|(slot, d)| (graph.to_vid(slot as u32), d))
            .collect();

        DijkstraResult {
            distances: results,
            path,
        }
    }
}
