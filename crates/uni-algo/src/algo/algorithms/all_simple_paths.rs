// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! All Simple Paths Algorithm.
//!
//! Finds all simple (loop-less) paths between source and target.
//! Depth-First Search with backtracking.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use uni_common::core::id::Vid;

pub struct AllSimplePaths;

#[derive(Debug, Clone)]
pub struct AllSimplePathsConfig {
    pub source: Vid,
    pub target: Vid,
    pub min_depth: usize,
    pub max_depth: usize,
    pub limit: usize, // Max paths to return
}

impl Default for AllSimplePathsConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            target: Vid::from(0),
            min_depth: 0,
            max_depth: 5,
            limit: 100,
        }
    }
}

pub struct AllSimplePathsResult {
    pub paths: Vec<Vec<Vid>>,
}

impl Algorithm for AllSimplePaths {
    type Config = AllSimplePathsConfig;
    type Result = AllSimplePathsResult;

    fn name() -> &'static str {
        "all_simple_paths"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let source_slot = match graph.to_slot(config.source) {
            Some(s) => s,
            None => return AllSimplePathsResult { paths: Vec::new() },
        };
        let target_slot = match graph.to_slot(config.target) {
            Some(s) => s,
            None => return AllSimplePathsResult { paths: Vec::new() },
        };

        let mut paths = Vec::new();
        let mut current_path = Vec::with_capacity(config.max_depth + 1);
        let mut visited = vec![false; graph.vertex_count()];

        current_path.push(source_slot);
        visited[source_slot as usize] = true;

        dfs(
            graph,
            source_slot,
            target_slot,
            &config,
            &mut current_path,
            &mut visited,
            &mut paths,
        );

        let mapped_paths = paths
            .into_iter()
            .map(|path| path.into_iter().map(|idx| graph.to_vid(idx)).collect())
            .collect();

        AllSimplePathsResult {
            paths: mapped_paths,
        }
    }
}

fn dfs(
    graph: &GraphProjection,
    u: u32,
    target: u32,
    config: &AllSimplePathsConfig,
    path: &mut Vec<u32>,
    visited: &mut [bool],
    paths: &mut Vec<Vec<u32>>,
) {
    if paths.len() >= config.limit {
        return;
    }

    let depth = path.len() - 1; // path includes source
    if depth > config.max_depth {
        return;
    }

    if u == target {
        if depth >= config.min_depth {
            paths.push(path.clone());
        }
        // Continue searching? Simple path ends at target?
        // Or can go through target? No, simple path contains nodes only once.
        // We can't go through target and come back to target because target is visited.
        // But we could continue from target to find longer paths if target wasn't the goal?
        // But here target IS the goal.
        return;
    }

    for &v in graph.out_neighbors(u) {
        if !visited[v as usize] {
            visited[v as usize] = true;
            path.push(v);
            dfs(graph, v, target, config, path, visited, paths);
            path.pop();
            visited[v as usize] = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    #[test]
    fn test_simple_paths_diamond() {
        // 0->1->3, 0->2->3
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2), Vid::from(3)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(3)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(2), Vid::from(3)),
        ];
        let graph = build_test_graph(vids, edges);

        let config = AllSimplePathsConfig {
            source: Vid::from(0),
            target: Vid::from(3),
            max_depth: 5,
            limit: 10,
            ..Default::default()
        };

        let result = AllSimplePaths::run(&graph, config);
        assert_eq!(result.paths.len(), 2);
    }
}
