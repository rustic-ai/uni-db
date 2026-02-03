// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Degree Centrality Algorithm.
//!
//! Measures the centrality of a node based on its degree (number of connections).
//! Can compute in-degree, out-degree, or total degree.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct DegreeCentrality;

#[derive(Debug, Clone)]
pub struct DegreeCentralityConfig {
    pub direction: DegreeDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DegreeDirection {
    Incoming,
    Outgoing,
    Both,
}

impl Default for DegreeCentralityConfig {
    fn default() -> Self {
        Self {
            direction: DegreeDirection::Outgoing,
        }
    }
}

pub struct DegreeCentralityResult {
    pub scores: Vec<(Vid, f64)>,
}

impl Algorithm for DegreeCentrality {
    type Config = DegreeCentralityConfig;
    type Result = DegreeCentralityResult;

    fn name() -> &'static str {
        "degree_centrality"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return DegreeCentralityResult { scores: Vec::new() };
        }

        let mut scores = Vec::with_capacity(n);

        for i in 0..n as u32 {
            let degree = match config.direction {
                DegreeDirection::Outgoing => graph.out_degree(i),
                DegreeDirection::Incoming => {
                    if graph.has_reverse() {
                        graph.in_degree(i)
                    } else {
                        // Fallback or error?
                        // If no reverse edges, in-degree is 0? Or unknown?
                        // Usually GraphProjection should have reverse if needed.
                        0
                    }
                }
                DegreeDirection::Both => {
                    let out_d = graph.out_degree(i);
                    let in_d = if graph.has_reverse() {
                        graph.in_degree(i)
                    } else {
                        0
                    };
                    out_d + in_d
                }
            };
            scores.push((graph.to_vid(i), degree as f64));
        }

        DegreeCentralityResult { scores }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_degree_centrality_outgoing() {
        // 0 -> 1, 0 -> 2
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(0), Vid::from(2))];
        let graph = build_test_graph(vids, edges);

        let config = DegreeCentralityConfig {
            direction: DegreeDirection::Outgoing,
        };
        let result = DegreeCentrality::run(&graph, config);

        let map: std::collections::HashMap<_, _> = result.scores.into_iter().collect();
        assert_eq!(map[&Vid::from(0)], 2.0);
        assert_eq!(map[&Vid::from(1)], 0.0);
        assert_eq!(map[&Vid::from(2)], 0.0);
    }
}
