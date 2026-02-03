// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Dinic's Max Flow Algorithm.
//!
//! Computes maximum flow from source to sink.
//! Edge weights in GraphProjection are treated as capacities.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct Dinic;

#[derive(Debug, Clone)]
pub struct DinicConfig {
    pub source: Vid,
    pub sink: Vid,
}

impl Default for DinicConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            sink: Vid::from(0),
        }
    }
}

pub struct DinicResult {
    pub max_flow: f64,
}

#[derive(Clone, Copy)]
struct Edge {
    to: usize,
    rev: usize, // index of reverse edge in adj[to]
    cap: f64,
    flow: f64,
}

impl Algorithm for Dinic {
    type Config = DinicConfig;
    type Result = DinicResult;

    fn name() -> &'static str {
        "dinic"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source = match graph.to_slot(config.source) {
            Some(s) => s as usize,
            None => return DinicResult { max_flow: 0.0 },
        };
        let sink = match graph.to_slot(config.sink) {
            Some(s) => s as usize,
            None => return DinicResult { max_flow: 0.0 },
        };

        if source == sink {
            return DinicResult {
                max_flow: f64::INFINITY,
            };
        }

        let n = graph.vertex_count();
        let mut adj: Vec<Vec<Edge>> = (0..n).map(|_| Vec::new()).collect();

        // Build residual graph
        // For every edge u->v with cap C, add u->v (cap C, flow 0) and v->u (cap 0, flow 0).
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
                    cap: 0.0, // Back edge capacity 0
                    flow: 0.0,
                });
            }
        }

        let mut max_flow = 0.0;
        let mut level = vec![0; n];

        loop {
            // BFS to build Level Graph
            level.fill(i32::MAX);
            level[source] = 0;
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(source);

            while let Some(u) = queue.pop_front() {
                for e in &adj[u] {
                    if e.cap - e.flow > 1e-9 && level[e.to] == i32::MAX {
                        level[e.to] = level[u] + 1;
                        queue.push_back(e.to);
                    }
                }
            }

            if level[sink] == i32::MAX {
                break;
            }

            // DFS to push blocking flow
            let mut ptr = vec![0; n];
            while let Some(pushed) = dfs(source, sink, f64::INFINITY, &mut adj, &level, &mut ptr) {
                if pushed == 0.0 {
                    break;
                }
                max_flow += pushed;
            }
        }

        DinicResult { max_flow }
    }
}

fn dfs(
    u: usize,
    sink: usize,
    pushed: f64,
    adj: &mut [Vec<Edge>],
    level: &[i32],
    ptr: &mut [usize],
) -> Option<f64> {
    if pushed == 0.0 || u == sink {
        return Some(pushed);
    }

    for i in ptr[u]..adj[u].len() {
        ptr[u] = i;
        let e = &adj[u][i];
        if level[u] + 1 != level[e.to] || e.cap - e.flow <= 1e-9 {
            continue;
        }

        let tr = dfs(e.to, sink, pushed.min(e.cap - e.flow), adj, level, ptr);
        if let Some(tr) = tr {
            if tr == 0.0 {
                continue;
            }
            adj[u][i].flow += tr;
            let rev = adj[u][i].rev;
            let to = adj[u][i].to;
            adj[to][rev].flow -= tr;
            return Some(tr);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_dinic_simple() {
        // 0 -> 1 (cap 10), 1 -> 2 (cap 5)
        // Max flow 0->2 is 5.
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];
        let mut graph = build_test_graph(vids, edges);
        // Inject weights
        graph.out_weights = Some(vec![10.0, 5.0]);

        let config = DinicConfig {
            source: Vid::from(0),
            sink: Vid::from(2),
        };

        let result = Dinic::run(&graph, config);
        assert_eq!(result.max_flow, 5.0);
    }
}
