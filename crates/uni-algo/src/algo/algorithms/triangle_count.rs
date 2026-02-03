// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Triangle Count Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use uni_common::core::id::Vid;

pub struct TriangleCount;

#[derive(Debug, Clone, Default)]
pub struct TriangleCountConfig;

pub struct TriangleCountResult {
    pub global_count: u64,
    pub node_counts: Vec<(Vid, u64)>,
}

impl Algorithm for TriangleCount {
    type Config = TriangleCountConfig;
    type Result = TriangleCountResult;

    fn name() -> &'static str {
        "triangleCount"
    }

    fn needs_reverse() -> bool {
        // Triangle count typically treats graph as undirected to find {u, v, w} set.
        // If directed, we look for cycles.
        // Standard algo.triangleCount is undirected.
        true
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return TriangleCountResult {
                global_count: 0,
                node_counts: Vec::new(),
            };
        }

        let mut node_counts = vec![0u64; n];

        // Parallel iteration
        // To avoid double counting, we iterate u, then v in N(u) where v > u.
        // Then intersection of N(u) and N(v) gives w.
        // If w > v, we found a unique triangle {u, v, w}.
        // But we want per-node counts.
        // Each triangle {u, v, w} contributes +1 to count(u), count(v), count(w).
        // Since we iterate u < v < w, we can atomically add.
        // Or we can just compute per-node: for u, iterate all pairs v, w in N(u) and check edge? O(d^2).
        // Better: iterate u, neighbors v. intersection = N(u) & N(v).
        // For each w in intersection, we have triangle.

        // Let's use sorted adjacency lists for fast intersection.
        // GraphProjection stores neighbors but not necessarily sorted.
        // Sorting is needed.

        // Pre-processing: Sort neighbor lists (or create sorted view).
        // Since we can't modify GraphProjection easily in-place (it's immutable ref),
        // we might just collect and sort, or use HashSet.
        // HashSet is slower but easier.
        // Given typically small degrees, simple scan might be okay?
        // Let's create sorted adjacency for efficient intersection.

        // Actually, we can just parallelize over `u` and accumulate local counts.
        // `count(u)` is number of triangles u participates in.
        // `count(u) = sum_{v in N(u)} |N(u) intersect N(v)| / 2`.

        // This works for undirected.

        let adj: Vec<Vec<u32>> = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut neighbors = Vec::new();
                neighbors.extend_from_slice(graph.out_neighbors(i as u32));
                if graph.has_reverse() {
                    // Merge in_neighbors, dedup
                    let in_n = graph.in_neighbors(i as u32);
                    neighbors.extend_from_slice(in_n);
                    neighbors.sort_unstable();
                    neighbors.dedup();
                } else {
                    neighbors.sort_unstable();
                }
                neighbors
            })
            .collect();

        // Now compute counts
        // To update node_counts in parallel without atomics per node, we can just return counts?
        // Actually, `count(u)` depends on `u`'s neighbors.
        // We can compute `count(u)` independently.

        node_counts
            .par_iter_mut()
            .enumerate()
            .for_each(|(u, count)| {
                let neighbors = &adj[u];
                let mut local_triangles = 0;

                // For each pair of neighbors, check if they are connected
                // Using intersection is faster if degree is large: sum_{v in N(u)} |N(u) intersect N(v)|
                // Then divide by 2 because each edge (v, w) is counted twice (once for v, once for w).

                for &v in neighbors {
                    let neighbors_v = &adj[v as usize];
                    // Intersect sorted lists
                    local_triangles += intersect_sorted_len(neighbors, neighbors_v);
                }

                *count = local_triangles as u64 / 2;
            });

        let total: u64 = node_counts.iter().sum::<u64>() / 3; // Each triangle counted 3 times (once per node)

        let results = node_counts
            .into_iter()
            .enumerate()
            .map(|(i, c)| (graph.to_vid(i as u32), c))
            .collect();

        TriangleCountResult {
            global_count: total,
            node_counts: results,
        }
    }
}

fn intersect_sorted_len(a: &[u32], b: &[u32]) -> usize {
    let mut i = 0;
    let mut j = 0;
    let mut count = 0;

    // Branchless intersection
    // Compiler auto-vectorization friendly
    while i < a.len() && j < b.len() {
        let va = a[i];
        let vb = b[j];

        let le = va <= vb;
        let ge = va >= vb;

        count += (le && ge) as usize;
        i += le as usize;
        j += ge as usize;
    }
    count
}
