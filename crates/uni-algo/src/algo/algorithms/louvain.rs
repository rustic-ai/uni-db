// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Louvain Community Detection Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::collections::HashMap;
use uni_common::core::id::Vid;

pub struct Louvain;

#[derive(Debug, Clone)]
pub struct LouvainConfig {
    pub resolution: f64,
    pub max_iterations: usize,
    pub min_modularity_gain: f64,
}

impl Default for LouvainConfig {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            max_iterations: 10,
            min_modularity_gain: 1e-4,
        }
    }
}

pub struct LouvainResult {
    pub communities: Vec<(Vid, u64)>,
    pub modularity: f64,
    pub community_count: usize,
}

impl Algorithm for Louvain {
    type Config = LouvainConfig;
    type Result = LouvainResult;

    fn name() -> &'static str {
        "louvain"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return LouvainResult {
                communities: Vec::new(),
                modularity: 0.0,
                community_count: 0,
            };
        }

        // Initialize: each node in its own community
        let mut community: Vec<u32> = (0..n as u32).collect();

        // Total edge weight (m)
        // For unweighted graph, m = edge_count / 2 (since each edge is counted twice if bidirectional)
        // But Uni GraphProjection might be directed.
        // Louvain usually works on undirected graphs.
        // We treat it as undirected by summing all out_degrees.
        let mut m: f64 = 0.0;
        let mut node_weights = vec![0.0; n];
        for v in 0..n as u32 {
            let mut deg = graph.out_degree(v) as f64;
            if graph.has_reverse() {
                deg += graph.in_degree(v) as f64;
            }
            m += deg;
            node_weights[v as usize] = deg;
        }
        m /= 2.0;

        if m == 0.0 {
            return LouvainResult {
                communities: community
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| (graph.to_vid(i as u32), c as u64))
                    .collect(),
                modularity: 0.0,
                community_count: n,
            };
        }

        // Track community total weights (Sigma_tot)
        let mut community_weights = node_weights.clone();

        for _ in 0..config.max_iterations {
            let mut improved = false;

            // Phase 1: Local moves
            for v in 0..n as u32 {
                let v_idx = v as usize;
                let current_comm = community[v_idx];
                let v_weight = node_weights[v_idx];

                // Find neighbor communities and weights to them (k_i,in)
                let mut neighbor_comm_weights: HashMap<u32, f64> = HashMap::new();
                for &u in graph.out_neighbors(v) {
                    let u_comm = community[u as usize];
                    *neighbor_comm_weights.entry(u_comm).or_insert(0.0) += 1.0;
                }
                if graph.has_reverse() {
                    for &u in graph.in_neighbors(v) {
                        let u_comm = community[u as usize];
                        *neighbor_comm_weights.entry(u_comm).or_insert(0.0) += 1.0;
                    }
                }

                let mut best_comm = current_comm;
                let mut max_gain = 0.0;

                // Remove v from current community
                community_weights[current_comm as usize] -= v_weight;

                for (&target_comm, &k_i_in) in &neighbor_comm_weights {
                    let target_comm_weight = community_weights[target_comm as usize];

                    // Modularity gain formula:
                    // delta_Q = [ (Sigma_tot + k_i,in) / 2m - ((Sigma_tot + k_i)/2m)^2 ] - [ Sigma_tot/2m - (Sigma_tot/2m)^2 - (k_i/2m)^2 ]
                    // Simplified: delta_Q = (1/2m) * (k_i,in - (Sigma_tot * k_i) / m)
                    let gain =
                        k_i_in - (target_comm_weight * v_weight * config.resolution) / (2.0 * m);

                    if gain > max_gain {
                        max_gain = gain;
                        best_comm = target_comm;
                    }
                }

                if max_gain > config.min_modularity_gain && best_comm != current_comm {
                    community[v_idx] = best_comm;
                    improved = true;
                }

                // Add v to best community
                community_weights[community[v_idx] as usize] += v_weight;
            }

            if !improved {
                break;
            }
        }

        // Final modularity calculation
        let q = compute_modularity(graph, &community, m, config.resolution);

        // Map back to VIDs and renumber communities
        let mut comm_map: HashMap<u32, u64> = HashMap::new();
        let mut next_id = 0u64;
        let mut results = Vec::with_capacity(n);
        for (i, &comm) in community.iter().enumerate() {
            let id = *comm_map.entry(comm).or_insert_with(|| {
                let val = next_id;
                next_id += 1;
                val
            });
            results.push((graph.to_vid(i as u32), id));
        }

        LouvainResult {
            communities: results,
            modularity: q,
            community_count: comm_map.len(),
        }
    }
}

fn compute_modularity(graph: &GraphProjection, community: &[u32], m: f64, resolution: f64) -> f64 {
    let n = graph.vertex_count();
    let mut q = 0.0;

    // Sum over communities
    let mut comm_internal_weights: HashMap<u32, f64> = HashMap::new();
    let mut comm_total_weights: HashMap<u32, f64> = HashMap::new();

    for v in 0..n as u32 {
        let v_comm = community[v as usize];
        let v_deg = graph.out_degree(v) as f64;
        *comm_total_weights.entry(v_comm).or_insert(0.0) += v_deg;

        for &u in graph.out_neighbors(v) {
            if community[u as usize] == v_comm {
                *comm_internal_weights.entry(v_comm).or_insert(0.0) += 1.0;
            }
        }
    }

    for (&comm, &internal) in &comm_internal_weights {
        let total = comm_total_weights[&comm];
        q += (internal / (2.0 * m)) - resolution * (total / (2.0 * m)).powi(2);
    }

    q
}
