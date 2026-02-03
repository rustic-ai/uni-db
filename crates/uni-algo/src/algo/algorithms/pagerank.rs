// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! PageRank Centrality Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use uni_common::core::id::Vid;

pub struct PageRank;

#[derive(Debug, Clone)]
pub struct PageRankConfig {
    pub damping_factor: f64,
    pub max_iterations: usize,
    pub tolerance: f64,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            damping_factor: 0.85,
            max_iterations: 20,
            tolerance: 1e-6,
        }
    }
}

pub struct PageRankResult {
    pub scores: Vec<(Vid, f64)>,
    pub iterations: usize,
    pub converged: bool,
}

impl Algorithm for PageRank {
    type Config = PageRankConfig;
    type Result = PageRankResult;

    fn name() -> &'static str {
        "pageRank"
    }

    fn needs_reverse() -> bool {
        true
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return PageRankResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
            };
        }

        let d = config.damping_factor;
        let base = (1.0 - d) / n as f64;

        let mut scores = vec![1.0 / n as f64; n];
        let mut next = vec![0.0; n];

        let mut iterations = 0;
        let mut converged = false;

        for iter in 0..config.max_iterations {
            iterations = iter + 1;

            // Parallel iteration over vertices
            next.par_iter_mut().enumerate().for_each(|(v, score)| {
                let sum: f64 = graph
                    .in_neighbors(v as u32)
                    .iter()
                    .map(|&u| {
                        let out_deg = graph.out_degree(u);
                        if out_deg > 0 {
                            scores[u as usize] / out_deg as f64
                        } else {
                            // Dangling node logic - standard PageRank distributes their score evenly
                            // but simpler impl often just ignores or handles via sink correction.
                            // For MVP, we ignore them in the sum (effectively base distributes them).
                            0.0
                        }
                    })
                    .sum();
                *score = base + d * sum;
            });

            // Convergence check
            let diff: f64 = scores
                .par_iter()
                .zip(next.par_iter())
                .map(|(a, b)| (a - b).abs())
                .sum();

            std::mem::swap(&mut scores, &mut next);

            if diff < config.tolerance {
                converged = true;
                break;
            }
        }

        // Map results back to VIDs
        let results = scores
            .into_iter()
            .enumerate()
            .map(|(slot, score)| (graph.to_vid(slot as u32), score))
            .collect();

        PageRankResult {
            scores: results,
            iterations,
            converged,
        }
    }
}
