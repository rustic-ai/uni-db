// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Weakly Connected Components (WCC) Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::collections::HashMap;
use uni_common::core::id::Vid;

pub struct Wcc;

#[derive(Debug, Clone, Default)]
pub struct WccConfig {
    pub min_component_size: Option<usize>,
}

pub struct WccResult {
    pub components: Vec<(Vid, u64)>, // (node, component_id)
    pub component_count: usize,
}

impl Algorithm for Wcc {
    type Config = WccConfig;
    type Result = WccResult;

    fn name() -> &'static str {
        "wcc"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return WccResult {
                components: Vec::new(),
                component_count: 0,
            };
        }

        // Union-Find with path compression
        let mut parent: Vec<u32> = (0..n as u32).collect();
        let mut rank: Vec<u8> = vec![0; n];

        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                parent[x as usize] = parent[parent[x as usize] as usize]; // path compression
                x = parent[x as usize];
            }
            x
        }

        fn union(parent: &mut [u32], rank: &mut [u8], x: u32, y: u32) {
            let px = find(parent, x);
            let py = find(parent, y);
            if px == py {
                return;
            }
            // Union by rank
            match rank[px as usize].cmp(&rank[py as usize]) {
                std::cmp::Ordering::Less => parent[px as usize] = py,
                std::cmp::Ordering::Greater => parent[py as usize] = px,
                std::cmp::Ordering::Equal => {
                    parent[py as usize] = px;
                    rank[px as usize] += 1;
                }
            }
        }

        // Process all edges
        for v in 0..n as u32 {
            for &u in graph.out_neighbors(v) {
                union(&mut parent, &mut rank, v, u);
            }
        }

        // Assign contiguous component IDs
        let mut comp_map: HashMap<u32, u64> = HashMap::default();
        let mut next_id = 0u64;

        let mut results = Vec::with_capacity(n);
        for slot in 0..n as u32 {
            let root = find(&mut parent, slot);
            let cid = *comp_map.entry(root).or_insert_with(|| {
                let id = next_id;
                next_id += 1;
                id
            });
            results.push((graph.to_vid(slot), cid));
        }

        // Filter by min size if specified
        if let Some(min_size) = config.min_component_size {
            let mut sizes = HashMap::new();
            for (_, cid) in &results {
                *sizes.entry(*cid).or_insert(0) += 1;
            }
            results.retain(|(_, cid)| *sizes.get(cid).unwrap() >= min_size);
        }

        WccResult {
            component_count: comp_map.len(),
            components: results,
        }
    }
}
