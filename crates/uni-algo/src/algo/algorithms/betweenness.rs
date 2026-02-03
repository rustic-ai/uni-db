// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Betweenness Centrality Algorithm (Brandes').

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use std::collections::VecDeque;
use uni_common::core::id::Vid;

pub struct Betweenness;

#[derive(Debug, Clone)]
pub struct BetweennessConfig {
    pub normalize: bool,
    pub sampling_size: Option<usize>, // If None, exact computation (all nodes)
}

impl Default for BetweennessConfig {
    fn default() -> Self {
        Self {
            normalize: true,
            sampling_size: None,
        }
    }
}

pub struct BetweennessResult {
    pub scores: Vec<(Vid, f64)>,
}

impl Algorithm for Betweenness {
    type Config = BetweennessConfig;
    type Result = BetweennessResult;

    fn name() -> &'static str {
        "betweenness"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return BetweennessResult { scores: Vec::new() };
        }

        // Determine source nodes (all or sample)
        let mut sources: Vec<u32> = (0..n as u32).collect();
        if let Some(size) = config.sampling_size
            && size < n
        {
            use rand::seq::SliceRandom;
            let mut rng = rand::thread_rng();
            sources.shuffle(&mut rng);
            sources.truncate(size);
        }

        // Parallel execution of Brandes' algorithm
        // Use fold/reduce to accumulate scores thread-locally, avoiding Mutex contention.
        let mut cb = sources
            .par_iter()
            .fold(
                || vec![0.0; n],
                |mut acc_cb, &s| {
                    // Stack S, queue Q
                    let mut s_stack = Vec::with_capacity(n);
                    let mut q = VecDeque::with_capacity(n);

                    // Path counts (sigma) and distances (d)
                    let mut d: Vec<i32> = vec![-1; n];
                    let mut sigma: Vec<u64> = vec![0; n];
                    let mut p: Vec<Vec<u32>> = vec![Vec::new(); n]; // Predecessors

                    sigma[s as usize] = 1;
                    d[s as usize] = 0;
                    q.push_back(s);

                    // BFS
                    while let Some(v) = q.pop_front() {
                        s_stack.push(v);
                        let dist_v = d[v as usize];

                        for &w in graph.out_neighbors(v) {
                            // Path discovery
                            if d[w as usize] < 0 {
                                d[w as usize] = dist_v + 1;
                                q.push_back(w);
                            }
                            // Path counting
                            if d[w as usize] == dist_v + 1 {
                                sigma[w as usize] += sigma[v as usize];
                                p[w as usize].push(v);
                            }
                        }
                    }

                    // Accumulation
                    let mut delta = vec![0.0; n];
                    while let Some(w) = s_stack.pop() {
                        for &v in &p[w as usize] {
                            if sigma[w as usize] > 0 {
                                delta[v as usize] += (sigma[v as usize] as f64
                                    / sigma[w as usize] as f64)
                                    * (1.0 + delta[w as usize]);
                            }
                        }
                        if w != s {
                            acc_cb[w as usize] += delta[w as usize];
                        }
                    }
                    acc_cb
                },
            )
            .reduce(
                || vec![0.0; n],
                |mut a, b| {
                    for (x, y) in a.iter_mut().zip(b.iter()) {
                        *x += y;
                    }
                    a
                },
            );

        // Normalize
        if config.normalize && n > 2 {
            // For directed graphs: normalizer = 1 / ((n-1)(n-2))
            // For undirected: 2 / ((n-1)(n-2))
            // We assume directed here matching GraphProjection structure unless we detect otherwise,
            // but standard algo.betweenness usually normalizes for directed.
            let norm_factor = 1.0 / ((n - 1) as f64 * (n - 2) as f64);
            for score in cb.iter_mut() {
                *score *= norm_factor;
            }
        }

        let results = cb
            .into_iter()
            .enumerate()
            .map(|(i, score)| (graph.to_vid(i as u32), score))
            .collect();

        BetweennessResult { scores: results }
    }
}
