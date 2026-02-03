// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Maximal Cliques Algorithm (Bron-Kerbosch with Pivot).
//!
//! Finds all maximal cliques in an undirected graph.
//! A maximal clique is a subgraph where every node is connected to every other node,
//! and it cannot be extended by adding another node.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::collections::HashSet;
use uni_common::core::id::Vid;

pub struct MaximalCliques;

#[derive(Debug, Clone)]
pub struct MaximalCliquesConfig {
    pub min_size: usize,
}

impl Default for MaximalCliquesConfig {
    fn default() -> Self {
        Self { min_size: 2 }
    }
}

pub struct MaximalCliquesResult {
    pub cliques: Vec<Vec<Vid>>,
}

impl Algorithm for MaximalCliques {
    type Config = MaximalCliquesConfig;
    type Result = MaximalCliquesResult;

    fn name() -> &'static str {
        "maximal_cliques"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return MaximalCliquesResult {
                cliques: Vec::new(),
            };
        }

        // Convert graph to adjacency sets for fast lookup O(1) or O(d).
        // GraphProjection is CSR. To check u-v edge, we scan neighbors of u.
        // For dense graphs, bitset/matrix is better.
        // For sparse graphs, sorted adjacency list allows O(log d) or intersection.
        // Bron-Kerbosch mainly iterates neighbors.
        // P intersection N(u).
        // GraphProjection neighbors are usually sorted? No guarantee.

        // Treat graph as undirected: u-v exists if u->v OR v->u.
        // We can build a local adjacency list `adj` where adj[u] contains v if connected.

        let mut adj = vec![HashSet::new(); n];
        for u in 0..n {
            for &v_u32 in graph.out_neighbors(u as u32) {
                let v = v_u32 as usize;
                if u != v {
                    adj[u].insert(v);
                    adj[v].insert(u);
                }
            }
            if graph.has_reverse() {
                for &v_u32 in graph.in_neighbors(u as u32) {
                    let v = v_u32 as usize;
                    if u != v {
                        adj[u].insert(v);
                        adj[v].insert(u);
                    }
                }
            }
        }

        let mut cliques = Vec::new();
        let r = HashSet::new();
        let p: HashSet<usize> = (0..n).collect();
        let x = HashSet::new();

        bron_kerbosch_pivot(&adj, r, p, x, config.min_size, &mut cliques);

        let mapped_cliques = cliques
            .into_iter()
            .map(|clique| {
                clique
                    .into_iter()
                    .map(|idx| graph.to_vid(idx as u32))
                    .collect()
            })
            .collect();

        MaximalCliquesResult {
            cliques: mapped_cliques,
        }
    }
}

fn bron_kerbosch_pivot(
    adj: &[HashSet<usize>],
    r: HashSet<usize>,
    mut p: HashSet<usize>,
    mut x: HashSet<usize>,
    min_size: usize,
    cliques: &mut Vec<Vec<usize>>,
) {
    if p.is_empty() && x.is_empty() {
        if r.len() >= min_size {
            cliques.push(r.into_iter().collect());
        }
        return;
    }

    // Pivot selection: choose u in P U X that maximizes |P intersect N(u)|
    let u_pivot = p
        .union(&x)
        .max_by_key(|&u| p.iter().filter(|&v| adj[*u].contains(v)).count())
        .copied();

    // If no pivot (P U X empty), we are done (handled by base case).
    // If P U X not empty, u_pivot exists.
    let u = u_pivot.unwrap();

    // P \ N(u)
    let candidates: Vec<usize> = p.iter().filter(|&v| !adj[u].contains(v)).copied().collect();

    for v in candidates {
        let mut new_r = r.clone();
        new_r.insert(v);

        let new_p: HashSet<usize> = p.iter().filter(|&n| adj[v].contains(n)).copied().collect();

        let new_x: HashSet<usize> = x.iter().filter(|&n| adj[v].contains(n)).copied().collect();

        bron_kerbosch_pivot(adj, new_r, new_p, new_x, min_size, cliques);

        p.remove(&v);
        x.insert(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_cliques_triangle() {
        // 0-1, 1-2, 2-0 (Triangle)
        // Clique: {0, 1, 2}
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(0)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = MaximalCliques::run(&graph, MaximalCliquesConfig::default());
        assert_eq!(result.cliques.len(), 1);
        assert_eq!(result.cliques[0].len(), 3);
    }

    #[test]
    fn test_cliques_two_triangles() {
        // 0-1-2-0 (Triangle 1)
        // 2-3-4-2 (Triangle 2)
        // Joint at 2.
        // Cliques: {0,1,2}, {2,3,4}
        let vids = (0..5).map(Vid::from).collect();
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(0)),
            (Vid::from(2), Vid::from(3)),
            (Vid::from(3), Vid::from(4)),
            (Vid::from(4), Vid::from(2)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = MaximalCliques::run(&graph, MaximalCliquesConfig::default());
        assert_eq!(result.cliques.len(), 2);
    }
}
