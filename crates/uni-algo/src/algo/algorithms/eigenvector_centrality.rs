// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Eigenvector Centrality Algorithm.
//!
//! Measures the influence of a node in a network.
//! Uses power iteration method.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct EigenvectorCentrality;

#[derive(Debug, Clone)]
pub struct EigenvectorCentralityConfig {
    pub max_iterations: usize,
    pub tolerance: f64,
}

impl Default for EigenvectorCentralityConfig {
    fn default() -> Self {
        Self {
            max_iterations: 100,
            tolerance: 1e-6,
        }
    }
}

pub struct EigenvectorCentralityResult {
    pub scores: Vec<(Vid, f64)>,
    pub iterations: usize,
}

impl Algorithm for EigenvectorCentrality {
    type Config = EigenvectorCentralityConfig;
    type Result = EigenvectorCentralityResult;

    fn name() -> &'static str {
        "eigenvector_centrality"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return EigenvectorCentralityResult {
                scores: Vec::new(),
                iterations: 0,
            };
        }

        let mut x = vec![1.0 / (n as f64).sqrt(); n]; // Initial vector (normalized)
        let mut next_x = vec![0.0; n];
        let mut iterations = 0;

        for iter in 0..config.max_iterations {
            iterations = iter + 1;
            next_x.fill(0.0);

            // Matrix multiplication: next_x = A^T * x (using incoming edges)
            // Or A * x if directed means "influence flows from u to v"?
            // Eigenvector usually defined on adjacency A. x_v = 1/lambda * sum(A_uv * x_u)
            // If u -> v (u influences v), then v's score depends on u's score.
            // So we iterate incoming edges of v.
            // Or iterate outgoing edges of u and add to v.

            // Using push method (outgoing edges) is cache friendly for CSR.
            for (u, &x_u) in x.iter().enumerate().take(n) {
                if x_u == 0.0 {
                    continue;
                }
                for (i, &v_u32) in graph.out_neighbors(u as u32).iter().enumerate() {
                    let weight = if graph.has_weights() {
                        graph.out_weight(u as u32, i)
                    } else {
                        1.0
                    };
                    next_x[v_u32 as usize] += x_u * weight;
                }
            }

            // Normalize (L2 norm)
            let mut norm_sq = 0.0;
            for val in &next_x {
                norm_sq += val * val;
            }
            let norm = norm_sq.sqrt();

            if norm == 0.0 {
                // Graph likely has no edges or sink trap
                break;
            }

            for val in &mut next_x {
                *val /= norm;
            }

            // Check convergence
            let mut diff = 0.0;
            for i in 0..n {
                diff += (next_x[i] - x[i]).abs();
            }

            x.copy_from_slice(&next_x);

            if diff < config.tolerance {
                break;
            }
        }

        let scores = x
            .into_iter()
            .enumerate()
            .map(|(i, s)| (graph.to_vid(i as u32), s))
            .collect();

        EigenvectorCentralityResult { scores, iterations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_eigenvector_centrality_star() {
        // Use a non-bipartite graph to avoid power iteration oscillation.
        // Triangle 0-1-2 plus node 3 connected to 0.
        // 0-1, 1-2, 2-0, 0-3
        // 0 has degree 3 (neighbors 1,2,3). 1,2 degree 2. 3 degree 1.
        let vids2 = vec![Vid::from(0), Vid::from(1), Vid::from(2), Vid::from(3)];
        let edges2 = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(0)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(2), Vid::from(1)),
            (Vid::from(2), Vid::from(0)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(0), Vid::from(3)),
            (Vid::from(3), Vid::from(0)),
        ];
        let graph2 = build_test_graph(vids2, edges2);
        let result2 = EigenvectorCentrality::run(&graph2, EigenvectorCentralityConfig::default());
        let map2: std::collections::HashMap<_, _> = result2.scores.into_iter().collect();

        // 0 should be highest
        assert!(map2[&Vid::from(0)] > map2[&Vid::from(3)]);
        assert!(map2[&Vid::from(0)] > map2[&Vid::from(1)]);
    }
}
