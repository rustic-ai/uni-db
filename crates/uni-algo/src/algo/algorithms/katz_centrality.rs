// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Katz Centrality Algorithm.
//!
//! Measures influence by taking into account total number of walks between nodes.
//! x = alpha * A * x + beta.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct KatzCentrality;

#[derive(Debug, Clone)]
pub struct KatzCentralityConfig {
    pub alpha: f64,
    pub beta: f64,
    pub max_iterations: usize,
    pub tolerance: f64,
}

impl Default for KatzCentralityConfig {
    fn default() -> Self {
        Self {
            alpha: 0.1, // Should be < 1/lambda_max
            beta: 1.0,
            max_iterations: 100,
            tolerance: 1e-6,
        }
    }
}

pub struct KatzCentralityResult {
    pub scores: Vec<(Vid, f64)>,
    pub iterations: usize,
}

impl Algorithm for KatzCentrality {
    type Config = KatzCentralityConfig;
    type Result = KatzCentralityResult;

    fn name() -> &'static str {
        "katz_centrality"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return KatzCentralityResult {
                scores: Vec::new(),
                iterations: 0,
            };
        }

        let mut x = vec![config.beta; n]; // Start with beta
        let mut next_x = vec![0.0; n];
        let mut iterations = 0;

        for iter in 0..config.max_iterations {
            iterations = iter + 1;
            // next_x = beta + alpha * A^T * x
            // Initialize with beta
            next_x.fill(config.beta);

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
                    next_x[v_u32 as usize] += config.alpha * x_u * weight;
                }
            }

            // Normalize? Katz usually converges if alpha < 1/lambda.
            // Normalization (L2) prevents overflow if alpha is large.
            // Standard Katz doesn't normalize per step, but converges to (I - alpha*A)^-1 * beta.
            // Let's check convergence.

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

        KatzCentralityResult { scores, iterations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_katz_centrality_dag() {
        // 1 -> 0
        // x0 = beta + alpha * x1
        // x1 = beta
        // Expected: x1 = 1.0, x0 = 1.0 + 0.1 * 1.0 = 1.1

        let vids = vec![Vid::from(0), Vid::from(1)];
        let edges = vec![(Vid::from(1), Vid::from(0))];
        let graph = build_test_graph(vids, edges);

        let config = KatzCentralityConfig {
            alpha: 0.1,
            beta: 1.0,
            ..Default::default()
        };

        let result = KatzCentrality::run(&graph, config);
        let map: std::collections::HashMap<_, _> = result.scores.into_iter().collect();

        assert!((map[&Vid::from(1)] - 1.0).abs() < 1e-6);
        assert!((map[&Vid::from(0)] - 1.1).abs() < 1e-6);
    }
}
