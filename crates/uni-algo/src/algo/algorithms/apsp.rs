// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! All Pairs Shortest Path Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::Mutex;
use uni_common::core::id::Vid;

pub struct AllPairsShortestPath;

#[derive(Debug, Clone, Default)]
pub struct AllPairsShortestPathConfig;

pub struct AllPairsShortestPathResult {
    pub distances: Vec<(Vid, Vid, u32)>, // (source, target, distance)
}

impl Algorithm for AllPairsShortestPath {
    type Config = AllPairsShortestPathConfig;
    type Result = AllPairsShortestPathResult;

    fn name() -> &'static str {
        "allPairsShortestPath"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return AllPairsShortestPathResult {
                distances: Vec::new(),
            };
        }

        // Limit n? O(V(V+E)).

        let mut results = Vec::new();
        let results_mutex = Mutex::new(&mut results);

        (0..n as u32).into_par_iter().for_each(|s| {
            let mut q = VecDeque::with_capacity(n);
            let mut d = vec![-1; n];

            d[s as usize] = 0;
            q.push_back(s);

            let mut local_results = Vec::new();

            while let Some(u) = q.pop_front() {
                let dist_u = d[u as usize];

                if u != s {
                    local_results.push((s, u, dist_u as u32));
                }

                for &v in graph.out_neighbors(u) {
                    if d[v as usize] == -1 {
                        d[v as usize] = dist_u + 1;
                        q.push_back(v);
                    }
                }
            }

            if !local_results.is_empty() {
                let mut guard = results_mutex.lock().unwrap_or_else(|e| e.into_inner());
                let src_vid = graph.to_vid(s);
                for (_src_idx, tgt_idx, dist) in local_results {
                    // src_idx is s
                    guard.push((src_vid, graph.to_vid(tgt_idx), dist));
                }
            }
        });

        AllPairsShortestPathResult { distances: results }
    }
}
