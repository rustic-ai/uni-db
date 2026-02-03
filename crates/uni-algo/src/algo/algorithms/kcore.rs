// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! K-Core Decomposition Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct KCore;

#[derive(Debug, Clone, Default)]
pub struct KCoreConfig {
    pub k: Option<usize>, // If Some, returns only nodes in k-core. If None, returns core number for all.
}

pub struct KCoreResult {
    pub core_numbers: Vec<(Vid, u32)>,
}

impl Algorithm for KCore {
    type Config = KCoreConfig;
    type Result = KCoreResult;

    fn name() -> &'static str {
        "kCore"
    }

    fn needs_reverse() -> bool {
        // K-core is typically defined for undirected graphs.
        true
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return KCoreResult {
                core_numbers: Vec::new(),
            };
        }

        // 1. Compute degrees (undirected)
        let mut degrees = vec![0u32; n];
        let mut max_degree = 0;

        for (v, deg_ref) in degrees.iter_mut().enumerate() {
            let mut deg = graph.out_degree(v as u32);
            if graph.has_reverse() {
                deg += graph.in_degree(v as u32);
            }
            *deg_ref = deg;
            if deg > max_degree {
                max_degree = deg;
            }
        }
        // 2. Bucket sort
        // bins[d] contains list of vertices with degree d
        // We need efficient move.
        // Array of list heads? Doubly linked list for vertices?
        // Standard O(V+E) implementation uses:
        // - pos[v]: position of v in sorted array
        // - vert[i]: vertex at position i in sorted array
        // - bin[d]: starting position of degree d in sorted array

        let mut vert = vec![0u32; n];
        let mut pos = vec![0usize; n];
        let mut bin = vec![0usize; max_degree as usize + 1];

        // Histogram
        for &d in &degrees {
            bin[d as usize] += 1;
        }

        // Cumulative sum for start positions
        let mut start = 0;
        for b in &mut bin {
            let num = *b;
            *b = start;
            start += num;
        }

        // Fill array
        for v in 0..n {
            let d = degrees[v] as usize;
            pos[v] = bin[d];
            vert[pos[v]] = v as u32;
            bin[d] += 1;
        }

        // Restore bin starts (shift right)
        for d in (1..=max_degree as usize).rev() {
            bin[d] = bin[d - 1];
        }
        bin[0] = 0;

        // 3. Peeling
        // Iterate through vertices in sorted order
        for i in 0..n {
            let v = vert[i]; // vertex with smallest current degree
            let deg_v = degrees[v as usize];

            // Iterate neighbors
            // We need unique neighbors to avoid double decrementing if u->v and v->u exist
            let mut neighbors = Vec::new();
            neighbors.extend_from_slice(graph.out_neighbors(v));
            if graph.has_reverse() {
                neighbors.extend_from_slice(graph.in_neighbors(v));
            }
            neighbors.sort_unstable();
            neighbors.dedup();

            for &u in &neighbors {
                let u_idx = u as usize;
                if degrees[u_idx] > deg_v {
                    let deg_u = degrees[u_idx] as usize;
                    let pos_u = pos[u_idx];
                    let pos_w = bin[deg_u];
                    let w = vert[pos_w]; // First vertex with degree deg_u

                    // Swap u with w (move u to start of bin)
                    if u != w {
                        pos[u_idx] = pos_w;
                        pos[w as usize] = pos_u;
                        vert[pos_u] = w;
                        vert[pos_w] = u;
                    }

                    // Increment bin start for deg_u (effectively removing u from bin deg_u)
                    bin[deg_u] += 1;
                    // u is now at the end of bin[deg_u-1] (conceptually) or effectively shifted
                    degrees[u_idx] -= 1;
                }
            }
        }

        let mut results = Vec::new();
        if let Some(k) = config.k {
            for (v, &deg) in degrees.iter().enumerate() {
                if deg >= k as u32 {
                    results.push((graph.to_vid(v as u32), deg));
                }
            }
        } else {
            for (v, &deg) in degrees.iter().enumerate() {
                results.push((graph.to_vid(v as u32), deg));
            }
        }

        KCoreResult {
            core_numbers: results,
        }
    }
}
