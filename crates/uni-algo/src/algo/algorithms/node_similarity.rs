// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Node Similarity Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use fxhash::FxHashMap;
use rayon::prelude::*;
use std::sync::Mutex;
use uni_common::core::id::Vid;

pub struct NodeSimilarity;

#[derive(Debug, Clone)]
pub struct NodeSimilarityConfig {
    pub similarity_metric: SimilarityMetric,
    pub similarity_cutoff: f64,
    pub top_k: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SimilarityMetric {
    Jaccard,
    Overlap,
    Cosine,
}

impl Default for NodeSimilarityConfig {
    fn default() -> Self {
        Self {
            similarity_metric: SimilarityMetric::Jaccard,
            similarity_cutoff: 0.1,
            top_k: 10,
        }
    }
}

pub struct NodeSimilarityResult {
    pub similar_pairs: Vec<(Vid, Vid, f64)>,
}

impl Algorithm for NodeSimilarity {
    type Config = NodeSimilarityConfig;
    type Result = NodeSimilarityResult;

    fn name() -> &'static str {
        "nodeSimilarity"
    }

    fn needs_reverse() -> bool {
        true
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return NodeSimilarityResult {
                similar_pairs: Vec::new(),
            };
        }

        // We compute similarity based on OUTGOING neighbors.
        // Two nodes are similar if they point to the same targets.
        // Intersection is computed by iterating over TARGETS and looking at their INCOMING neighbors.

        // Map (u, v) -> intersection_count
        // Using partitioned approach to allow parallelism and reduce contention?
        // Or just straightforward MapReduce.

        // Naive approach: Map<(u32, u32), u32> can become huge.
        // Better: Process per-node?
        // No, processing by target is efficient for intersection.

        // Let's implement a chunked approach or simple parallel accumulation if memory permits.
        // Given we are in-memory, we assume we can fit `E * avg_degree` roughly?
        // Let's use a concurrent map (DashMap would be good, but we use Mutex<HashMap> for std).

        // Optimization: iterate source nodes `u`. For each `u`, collect neighbors `N(u)`.
        // Then for each `n` in `N(u)`, find other `v` in `in_neighbors(n)`.
        // Accumulate `intersection[v]`.
        // Compute similarity for `u` vs all `v`. Keep TopK for `u`.

        // This is O(V * D * D_in).

        let mut results = Vec::new();
        let results_mutex = Mutex::new(&mut results);

        (0..n as u32).into_par_iter().for_each(|u| {
            let u_out = graph.out_neighbors(u);
            let degree_u = u_out.len() as f64;

            if degree_u == 0.0 {
                return;
            }

            let mut intersections: FxHashMap<u32, u32> = FxHashMap::default();

            for &target in u_out {
                for &v in graph.in_neighbors(target) {
                    if v != u {
                        *intersections.entry(v).or_insert(0) += 1;
                    }
                }
            }

            let mut local_results = Vec::new();

            for (v, count) in intersections {
                let degree_v = graph.out_degree(v) as f64;
                let overlap = count as f64;

                let score = match config.similarity_metric {
                    SimilarityMetric::Jaccard => overlap / (degree_u + degree_v - overlap),
                    SimilarityMetric::Overlap => overlap / f64::min(degree_u, degree_v),
                    SimilarityMetric::Cosine => overlap / (degree_u * degree_v).sqrt(),
                };

                if score >= config.similarity_cutoff {
                    local_results.push((v, score));
                }
            }

            // Top K
            local_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            local_results.truncate(config.top_k);

            if !local_results.is_empty() {
                let mut guard = results_mutex
                    .lock()
                    .expect("Results mutex poisoned - a thread panicked while holding it");
                let u_vid = graph.to_vid(u);
                for (v, score) in local_results {
                    guard.push((u_vid, graph.to_vid(v), score));
                }
            }
        });

        NodeSimilarityResult {
            similar_pairs: results,
        }
    }
}
