// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Closeness Centrality Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use std::collections::VecDeque;
use uni_common::core::id::Vid;

pub struct Closeness;

#[derive(Debug, Clone, Default)]
pub struct ClosenessConfig {
    pub wasserman_faust: bool, // Improved formula for disconnected graphs
}

pub struct ClosenessResult {
    pub scores: Vec<(Vid, f64)>,
}

impl Algorithm for Closeness {
    type Config = ClosenessConfig;
    type Result = ClosenessResult;

    fn name() -> &'static str {
        "closeness"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return ClosenessResult { scores: Vec::new() };
        }

        let mut scores = vec![0.0; n];

        // Parallel BFS from every node
        scores.par_iter_mut().enumerate().for_each(|(s, score)| {
            let mut q = VecDeque::with_capacity(n);
            let mut d = vec![-1; n];

            d[s] = 0;
            q.push_back(s as u32);

            let mut sum_dist = 0;
            let mut reached = 0;

            while let Some(u) = q.pop_front() {
                let dist_u = d[u as usize];

                // Skip self in sum
                if u as usize != s {
                    sum_dist += dist_u;
                    reached += 1;
                }

                for &v in graph.out_neighbors(u) {
                    if d[v as usize] == -1 {
                        d[v as usize] = dist_u + 1;
                        q.push_back(v);
                    }
                }
            }

            if sum_dist > 0 {
                if config.wasserman_faust {
                    // WF = (reached / (n-1)) * (reached / sum_dist)
                    //    = reached^2 / ((n-1) * sum_dist)
                    if n > 1 {
                        *score = (reached as f64).powi(2) / ((n - 1) as f64 * sum_dist as f64);
                    }
                } else {
                    // Standard = reached / sum_dist (normalized by n-1 usually implies 1/(avg_dist))
                    // Standard def: 1 / avg_dist = 1 / (sum_dist / (n-1)) = (n-1) / sum_dist
                    // But if not connected, standard is 0 or only component.
                    // Neo4j uses: (reached / (n-1)) / (sum_dist / reached) = reached^2 / ((n-1)sum)
                    // Which is effectively Wasserman-Faust for component?

                    // Let's stick to standard harmonic closeness or normalized closeness.
                    // Normalized: (n-1) / sum_dist
                    if n > 1 {
                        *score = (n - 1) as f64 / sum_dist as f64;
                    }
                }
            }
        });

        let results = scores
            .into_iter()
            .enumerate()
            .map(|(i, s)| (graph.to_vid(i as u32), s))
            .collect();

        ClosenessResult { scores: results }
    }
}
