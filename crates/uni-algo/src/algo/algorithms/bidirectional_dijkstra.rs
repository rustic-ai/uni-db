// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bidirectional Dijkstra Algorithm.
//!
//! Performs Dijkstra's algorithm from both source and target simultaneously.
//! Requires `include_reverse` in GraphProjection.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use uni_common::core::id::Vid;

pub struct BidirectionalDijkstra;

#[derive(Debug, Clone)]
pub struct BidirectionalDijkstraConfig {
    pub source: Vid,
    pub target: Vid,
}

impl Default for BidirectionalDijkstraConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            target: Vid::from(0),
        }
    }
}

pub struct BidirectionalDijkstraResult {
    pub distance: Option<f64>,
    // Path reconstruction is more complex for bidirectional, omitted for basic version
    // but can be added if we store parent pointers.
    // Let's implement distance first.
}

impl Algorithm for BidirectionalDijkstra {
    type Config = BidirectionalDijkstraConfig;
    type Result = BidirectionalDijkstraResult;

    fn name() -> &'static str {
        "bidirectional_dijkstra"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        if !graph.has_reverse() {
            // Fallback or error? Usually should error if algorithm requires reverse.
            // But we can just return None or panic.
            panic!("Bidirectional Dijkstra requires graph projection with reverse edges");
        }

        let source_slot = match graph.to_slot(config.source) {
            Some(s) => s,
            None => return BidirectionalDijkstraResult { distance: None },
        };
        let target_slot = match graph.to_slot(config.target) {
            Some(s) => s,
            None => return BidirectionalDijkstraResult { distance: None },
        };

        if source_slot == target_slot {
            return BidirectionalDijkstraResult {
                distance: Some(0.0),
            };
        }

        let n = graph.vertex_count();
        let mut dist_fwd = vec![f64::INFINITY; n];
        let mut dist_bwd = vec![f64::INFINITY; n];

        let mut heap_fwd = BinaryHeap::new();
        let mut heap_bwd = BinaryHeap::new();

        dist_fwd[source_slot as usize] = 0.0;
        heap_fwd.push(Reverse((0.0f64.to_bits(), source_slot)));

        dist_bwd[target_slot as usize] = 0.0;
        heap_bwd.push(Reverse((0.0f64.to_bits(), target_slot)));

        let mut mu = f64::INFINITY; // Best path cost found so far

        while !heap_fwd.is_empty() && !heap_bwd.is_empty() {
            // Check termination: top_fwd + top_bwd >= mu
            let min_fwd = f64::from_bits(heap_fwd.peek().unwrap().0.0);
            let min_bwd = f64::from_bits(heap_bwd.peek().unwrap().0.0);

            if min_fwd + min_bwd >= mu {
                break;
            }

            // Expand smaller frontier or alternate
            if heap_fwd.len() <= heap_bwd.len() {
                // Forward step
                let Reverse((d_bits, u)) = heap_fwd.pop().unwrap();
                let d = f64::from_bits(d_bits);

                if d > dist_fwd[u as usize] {
                    continue;
                }

                for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
                    let weight = if graph.has_weights() {
                        graph.out_weight(u, i)
                    } else {
                        1.0
                    };
                    let new_dist = d + weight;
                    if new_dist < dist_fwd[v as usize] {
                        dist_fwd[v as usize] = new_dist;
                        heap_fwd.push(Reverse((new_dist.to_bits(), v)));

                        // Check connection
                        if dist_bwd[v as usize] < f64::INFINITY {
                            mu = mu.min(new_dist + dist_bwd[v as usize]);
                        }
                    }
                }
            } else {
                // Backward step
                let Reverse((d_bits, u)) = heap_bwd.pop().unwrap();
                let d = f64::from_bits(d_bits);

                if d > dist_bwd[u as usize] {
                    continue;
                }

                for &v in graph.in_neighbors(u) {
                    // For reverse step, weight is edge v->u.
                    // We need to find weight of v->u.
                    // GraphProjection in_neighbors doesn't store weights directly?
                    // We might need to look up v's out_edges to find u.
                    // This is inefficient.
                    // GraphProjection should probably store in_weights if needed.
                    // If no weights, assume 1.0.
                    let weight = if graph.has_weights() {
                        // TODO: efficiently get weight for v -> u
                        // Linear scan of v's out_neighbors
                        let mut w = 1.0;
                        // This is O(degree), making backward search slower on dense graphs.
                        // Ideally GraphProjection stores in_weights.
                        // For now, assume 1.0 or implement scan.
                        let neighbors = graph.out_neighbors(v);
                        for (idx, &n) in neighbors.iter().enumerate() {
                            if n == u {
                                w = graph.out_weight(v, idx);
                                break;
                            }
                        }
                        w
                    } else {
                        1.0
                    };

                    let new_dist = d + weight;
                    if new_dist < dist_bwd[v as usize] {
                        dist_bwd[v as usize] = new_dist;
                        heap_bwd.push(Reverse((new_dist.to_bits(), v)));

                        // Check connection
                        if dist_fwd[v as usize] < f64::INFINITY {
                            mu = mu.min(dist_fwd[v as usize] + new_dist);
                        }
                    }
                }
            }
        }

        BidirectionalDijkstraResult {
            distance: if mu < f64::INFINITY { Some(mu) } else { None },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::{GraphProjection, IdMap};

    fn build_graph_with_reverse(vids: Vec<Vid>, edges: Vec<(Vid, Vid)>) -> GraphProjection {
        // Custom builder to ensure in_neighbors are populated
        // Reusing test_utils logic but adding in_neighbors
        let mut id_map = IdMap::with_capacity(vids.len());
        for vid in &vids {
            id_map.insert(*vid);
        }
        let vertex_count = id_map.len();

        let mut out_adj: Vec<Vec<u32>> = vec![Vec::new(); vertex_count];
        let mut in_adj: Vec<Vec<u32>> = vec![Vec::new(); vertex_count];

        for (src, dst) in &edges {
            let u = id_map.to_slot(*src).unwrap();
            let v = id_map.to_slot(*dst).unwrap();
            out_adj[u as usize].push(v);
            in_adj[v as usize].push(u);
        }

        let mut out_offsets = vec![0u32; vertex_count + 1];
        let mut out_neighbors = Vec::new();
        for i in 0..vertex_count {
            out_offsets[i + 1] = out_offsets[i] + out_adj[i].len() as u32;
            out_neighbors.extend_from_slice(&out_adj[i]);
        }

        let mut in_offsets = vec![0u32; vertex_count + 1];
        let mut in_neighbors = Vec::new();
        for i in 0..vertex_count {
            in_offsets[i + 1] = in_offsets[i] + in_adj[i].len() as u32;
            in_neighbors.extend_from_slice(&in_adj[i]);
        }

        GraphProjection {
            vertex_count,
            out_offsets,
            out_neighbors,
            in_offsets,
            in_neighbors,
            out_weights: None,
            id_map,
            _node_labels: Vec::new(),
            _edge_types: Vec::new(),
        }
    }

    #[test]
    fn test_bi_dijkstra_simple() {
        // 0 -> 1 -> 2
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let graph = build_graph_with_reverse(vids, edges);

        let config = BidirectionalDijkstraConfig {
            source: Vid::from(0),
            target: Vid::from(2),
        };

        let result = BidirectionalDijkstra::run(&graph, config);
        assert_eq!(result.distance, Some(2.0));
    }
}
