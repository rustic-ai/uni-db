// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Ford-Fulkerson Algorithm (Edmonds-Karp implementation).
//!
//! Computes maximum flow from source to sink using BFS to find augmenting paths.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct FordFulkerson;

#[derive(Debug, Clone)]
pub struct FordFulkersonConfig {
    pub source: Vid,
    pub sink: Vid,
}

impl Default for FordFulkersonConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            sink: Vid::from(0),
        }
    }
}

pub struct FordFulkersonResult {
    pub max_flow: f64,
}

#[derive(Clone, Copy)]
struct Edge {
    to: usize,
    rev: usize,
    cap: f64,
    flow: f64,
}

impl Algorithm for FordFulkerson {
    type Config = FordFulkersonConfig;
    type Result = FordFulkersonResult;

    fn name() -> &'static str {
        "ford_fulkerson"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source = match graph.to_slot(config.source) {
            Some(s) => s as usize,
            None => return FordFulkersonResult { max_flow: 0.0 },
        };
        let sink = match graph.to_slot(config.sink) {
            Some(s) => s as usize,
            None => return FordFulkersonResult { max_flow: 0.0 },
        };

        if source == sink {
            return FordFulkersonResult {
                max_flow: f64::INFINITY,
            };
        }

        let n = graph.vertex_count();
        let mut adj: Vec<Vec<Edge>> = (0..n).map(|_| Vec::new()).collect();

        // Build residual graph
        for u in 0..n {
            for (i, &v_u32) in graph.out_neighbors(u as u32).iter().enumerate() {
                let v = v_u32 as usize;
                let cap = if graph.has_weights() {
                    graph.out_weight(u as u32, i)
                } else {
                    1.0
                };

                let a_len = adj[u].len();
                let b_len = adj[v].len();

                adj[u].push(Edge {
                    to: v,
                    rev: b_len,
                    cap,
                    flow: 0.0,
                });
                adj[v].push(Edge {
                    to: u,
                    rev: a_len,
                    cap: 0.0,
                    flow: 0.0,
                });
            }
        }

        let mut max_flow = 0.0;

        loop {
            // BFS for augmenting path
            let mut parent = vec![None; n]; // (u, edge_index)
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(source);

            // Just to track visited
            let mut visited = vec![false; n];
            visited[source] = true;

            let mut found_sink = false;

            while let Some(u) = queue.pop_front() {
                if u == sink {
                    found_sink = true;
                    break;
                }

                for (idx, e) in adj[u].iter().enumerate() {
                    if !visited[e.to] && e.cap - e.flow > 1e-9 {
                        visited[e.to] = true;
                        parent[e.to] = Some((u, idx));
                        queue.push_back(e.to);
                    }
                }
            }

            if !found_sink {
                break;
            }

            // Path reconstruction and bottleneck
            let mut path_flow = f64::INFINITY;
            let mut v = sink;
            while v != source {
                let (u, idx) = parent[v].unwrap();
                let e = &adj[u][idx];
                path_flow = path_flow.min(e.cap - e.flow);
                v = u;
            }

            // Augment
            v = sink;
            while v != source {
                let (u, idx) = parent[v].unwrap();
                adj[u][idx].flow += path_flow;
                let rev_idx = adj[u][idx].rev;
                adj[v][rev_idx].flow -= path_flow;
                v = u;
            }

            max_flow += path_flow;
        }

        FordFulkersonResult { max_flow }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_ford_fulkerson_simple() {
        // 0 -> 1 (cap 10), 1 -> 2 (cap 5)
        // Max flow 0->2 is 5.
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let mut graph = build_test_graph(vids, edges);
        graph.out_weights = Some(vec![10.0, 5.0]);

        let config = FordFulkersonConfig {
            source: Vid::from(0),
            sink: Vid::from(2),
        };

        let result = FordFulkerson::run(&graph, config);
        assert_eq!(result.max_flow, 5.0);
    }
}
