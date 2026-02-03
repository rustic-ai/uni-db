// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Elementary Circuits Algorithm (Johnson's Algorithm).
//!
//! Finds all elementary circuits (cycles) in a directed graph.
//! Uses Johnson's algorithm with SCC decomposition optimization.

use crate::algo::GraphProjection;
use crate::algo::algorithms::{Algorithm, Scc, SccConfig};
use std::collections::{HashMap, HashSet};
use uni_common::core::id::Vid;

pub struct ElementaryCircuits;

#[derive(Debug, Clone)]
pub struct ElementaryCircuitsConfig {
    pub min_length: usize,
    pub max_length: usize,
    pub limit: usize,
}

impl Default for ElementaryCircuitsConfig {
    fn default() -> Self {
        Self {
            min_length: 2,  // Simple cycle > 1 node? Or self loop allowed? Usually > 0.
            max_length: 10, // Limit depth to avoid explosion
            limit: 1000,
        }
    }
}

pub struct ElementaryCircuitsResult {
    pub cycles: Vec<Vec<Vid>>,
}

impl Algorithm for ElementaryCircuits {
    type Config = ElementaryCircuitsConfig;
    type Result = ElementaryCircuitsResult;

    fn name() -> &'static str {
        "elementary_circuits"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        // 1. Decompose into SCCs
        // We only care about SCCs with size > 1 or self-loop.
        let scc_res = Scc::run(graph, SccConfig::default());

        let mut scc_map: HashMap<u64, Vec<u32>> = HashMap::new();
        for (vid, cid) in scc_res.components {
            if let Some(slot) = graph.to_slot(vid) {
                scc_map.entry(cid).or_default().push(slot);
            }
        }

        let mut cycles = Vec::new();
        let mut count = 0;

        for nodes in scc_map.values() {
            if nodes.len() < 2 {
                // Check self loop
                if nodes.len() == 1 {
                    let u = nodes[0];
                    if graph.out_neighbors(u).contains(&u) && config.min_length <= 1 {
                        cycles.push(vec![graph.to_vid(u)]);
                        count += 1;
                    }
                }
                continue;
            }

            if count >= config.limit {
                break;
            }

            // Run Johnson's on this SCC
            // Subgraph induced by `nodes`.
            // We need mapping from global slot to local index 0..sub_n
            // Or just work with global slots but filtered.

            // To properly implement Johnson, we need to order nodes.
            let mut scc_nodes = nodes.clone();
            scc_nodes.sort(); // Process in order

            for (i, &start_node) in scc_nodes.iter().enumerate() {
                if count >= config.limit {
                    break;
                }

                // Subgraph induced by {v | v >= start_node}
                let sub_scc = &scc_nodes[i..];

                // DFS state
                let mut stack = Vec::new();
                let mut blocked = HashSet::new();
                let mut block_map: HashMap<u32, HashSet<u32>> = HashMap::new();

                let mut ctx = CircuitCtx {
                    graph,
                    scc_nodes: sub_scc,
                    stack: &mut stack,
                    blocked: &mut blocked,
                    block_map: &mut block_map,
                    cycles: &mut cycles,
                    config: &config,
                    count: &mut count,
                };

                circuit(start_node, start_node, &mut ctx);
            }
        }

        ElementaryCircuitsResult { cycles }
    }
}

struct CircuitCtx<'a> {
    graph: &'a GraphProjection,
    scc_nodes: &'a [u32],
    stack: &'a mut Vec<Vid>,
    blocked: &'a mut HashSet<u32>,
    block_map: &'a mut HashMap<u32, HashSet<u32>>,
    cycles: &'a mut Vec<Vec<Vid>>,
    config: &'a ElementaryCircuitsConfig,
    count: &'a mut usize,
}

fn circuit(u: u32, start_node: u32, ctx: &mut CircuitCtx) -> bool {
    if *ctx.count >= ctx.config.limit {
        return false;
    }

    let vid = ctx.graph.to_vid(u);
    ctx.stack.push(vid);
    ctx.blocked.insert(u);
    let mut found = false;

    // Neighbors in SCC
    for &v in ctx.graph.out_neighbors(u) {
        if !ctx.scc_nodes.contains(&v) {
            continue;
        }

        if v == start_node {
            if ctx.stack.len() >= ctx.config.min_length && ctx.stack.len() <= ctx.config.max_length
            {
                ctx.cycles.push(ctx.stack.clone());
                *ctx.count += 1;
                found = true;
            }
        } else if !ctx.blocked.contains(&v)
            && ctx.stack.len() < ctx.config.max_length
            && circuit(v, start_node, ctx)
        {
            found = true;
        }
    }

    if found {
        unblock(u, ctx.blocked, ctx.block_map);
    } else {
        for &v in ctx.graph.out_neighbors(u) {
            if ctx.scc_nodes.contains(&v) {
                ctx.block_map.entry(v).or_default().insert(u);
            }
        }
    }

    ctx.stack.pop();
    found
}

fn unblock(u: u32, blocked: &mut HashSet<u32>, block_map: &mut HashMap<u32, HashSet<u32>>) {
    blocked.remove(&u);
    if let Some(list) = block_map.remove(&u) {
        for w in list {
            if blocked.contains(&w) {
                unblock(w, blocked, block_map);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_elementary_circuits() {
        // 0->1->2->0 (Cycle 1)
        // 1->2->1 (Cycle 2 - nested)
        // 0->1, 1->2, 2->0, 2->1

        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(0)),
            (Vid::from(2), Vid::from(1)),
        ];
        let graph = build_test_graph(vids, edges);

        let config = ElementaryCircuitsConfig {
            min_length: 2,
            ..Default::default()
        };
        let result = ElementaryCircuits::run(&graph, config);

        assert_eq!(result.cycles.len(), 2);
        // Cycles: [0,1,2], [1,2]
    }
}
