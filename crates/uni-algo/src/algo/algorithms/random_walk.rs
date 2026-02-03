// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Random Walk Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rand::prelude::*;
use rayon::prelude::*;
use std::sync::Mutex;
use uni_common::core::id::Vid;

pub struct RandomWalk;

#[derive(Debug, Clone, Default)]
pub struct RandomWalkConfig {
    pub walk_length: usize,
    pub walks_per_node: usize,
    pub start_nodes: Vec<Vid>, // If empty, all nodes
    pub return_param: f64,     // p (1/p)
    pub in_out_param: f64,     // q (1/q)
}

pub struct RandomWalkResult {
    pub walks: Vec<Vec<Vid>>,
}

impl Algorithm for RandomWalk {
    type Config = RandomWalkConfig;
    type Result = RandomWalkResult;

    fn name() -> &'static str {
        "randomWalk"
    }

    fn needs_reverse() -> bool {
        // Only if we need to know previous neighbors for Node2Vec (checking if neighbor is connected to prev)
        // For simple random walk, no.
        // Implementing simple random walk first.
        false
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return RandomWalkResult { walks: Vec::new() };
        }

        let start_slots: Vec<u32> = if config.start_nodes.is_empty() {
            (0..n as u32).collect()
        } else {
            config
                .start_nodes
                .iter()
                .filter_map(|&vid| graph.to_slot(vid))
                .collect()
        };

        let mut walks = Vec::new();
        let walks_mutex = Mutex::new(&mut walks);

        // Chunking to avoid massive mutex contention or single result vector resizing
        start_slots.par_iter().for_each(|&start_node| {
            let mut local_walks = Vec::with_capacity(config.walks_per_node);
            let mut rng = rand::thread_rng();

            for _ in 0..config.walks_per_node {
                let mut walk = Vec::with_capacity(config.walk_length + 1);
                let mut curr = start_node;
                walk.push(graph.to_vid(curr));

                for _ in 0..config.walk_length {
                    let neighbors = graph.out_neighbors(curr);
                    if neighbors.is_empty() {
                        break;
                    }
                    // Simple uniform random walk
                    let next = neighbors.choose(&mut rng).unwrap();
                    curr = *next;
                    walk.push(graph.to_vid(curr));
                }
                local_walks.push(walk);
            }

            let mut guard = walks_mutex.lock().unwrap_or_else(|e| e.into_inner());
            guard.extend(local_walks);
        });

        RandomWalkResult { walks }
    }
}
