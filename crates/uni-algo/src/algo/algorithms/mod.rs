// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Core algorithm trait and common utilities.

use crate::algo::GraphProjection;

/// Core trait for all graph algorithms.
pub trait Algorithm: Send + Sync {
    /// Algorithm parameters.
    type Config: Default + Clone + Send + 'static;
    /// Result type.
    type Result: Send + 'static;

    /// Algorithm identifier.
    fn name() -> &'static str;

    /// Execute algorithm on a projection.
    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result;

    /// Whether this algorithm requires reverse edges.
    fn needs_reverse() -> bool {
        false
    }

    /// Whether this algorithm requires edge weights.
    fn needs_weights() -> bool {
        false
    }
}

mod pagerank;
pub use pagerank::{PageRank, PageRankConfig, PageRankResult};

mod wcc;
pub use wcc::{Wcc, WccConfig, WccResult};

mod dijkstra;
pub use dijkstra::{Dijkstra, DijkstraConfig, DijkstraResult};

mod louvain;
pub use louvain::{Louvain, LouvainConfig, LouvainResult};

mod label_propagation;
pub use label_propagation::{LabelPropagation, LabelPropagationConfig, LabelPropagationResult};

mod betweenness;
pub use betweenness::{Betweenness, BetweennessConfig, BetweennessResult};

mod node_similarity;
pub use node_similarity::{
    NodeSimilarity, NodeSimilarityConfig, NodeSimilarityResult, SimilarityMetric,
};

mod closeness;
pub use closeness::{Closeness, ClosenessConfig, ClosenessResult};

mod triangle_count;
pub use triangle_count::{TriangleCount, TriangleCountConfig, TriangleCountResult};

mod kcore;
pub use kcore::{KCore, KCoreConfig, KCoreResult};

mod random_walk;
pub use random_walk::{RandomWalk, RandomWalkConfig, RandomWalkResult};

mod apsp;
pub use apsp::{AllPairsShortestPath, AllPairsShortestPathConfig, AllPairsShortestPathResult};

mod scc;
pub use scc::{Scc, SccConfig, SccResult};

mod topological_sort;
pub use topological_sort::{TopologicalSort, TopologicalSortConfig, TopologicalSortResult};

mod cycle_detection;
pub use cycle_detection::{CycleDetection, CycleDetectionConfig, CycleDetectionResult};

mod bipartite_check;
pub use bipartite_check::{BipartiteCheck, BipartiteCheckConfig, BipartiteCheckResult};

mod bridges;
pub use bridges::{Bridges, BridgesConfig, BridgesResult};

mod articulation_points;
pub use articulation_points::{
    ArticulationPoints, ArticulationPointsConfig, ArticulationPointsResult,
};

mod astar;
pub use astar::{AStar, AStarConfig, AStarResult};

mod bidirectional_dijkstra;
pub use bidirectional_dijkstra::{
    BidirectionalDijkstra, BidirectionalDijkstraConfig, BidirectionalDijkstraResult,
};

mod bellman_ford;
pub use bellman_ford::{BellmanFord, BellmanFordConfig, BellmanFordResult};

mod k_shortest_paths;
pub use k_shortest_paths::{KShortestPaths, KShortestPathsConfig, KShortestPathsResult};

mod mst;
pub use mst::{MinimumSpanningTree, MstConfig, MstResult};

mod max_matching;
pub use max_matching::{MaximumMatching, MaximumMatchingConfig, MaximumMatchingResult};

mod dinic;
pub use dinic::{Dinic, DinicConfig, DinicResult};

mod ford_fulkerson;
pub use ford_fulkerson::{FordFulkerson, FordFulkersonConfig, FordFulkersonResult};

mod graph_metrics;
pub use graph_metrics::{GraphMetrics, GraphMetricsConfig, GraphMetricsResult};

mod degree_centrality;
pub use degree_centrality::{
    DegreeCentrality, DegreeCentralityConfig, DegreeCentralityResult, DegreeDirection,
};

mod harmonic_centrality;
pub use harmonic_centrality::{
    HarmonicCentrality, HarmonicCentralityConfig, HarmonicCentralityResult,
};

mod eigenvector_centrality;
pub use eigenvector_centrality::{
    EigenvectorCentrality, EigenvectorCentralityConfig, EigenvectorCentralityResult,
};

mod katz_centrality;
pub use katz_centrality::{KatzCentrality, KatzCentralityConfig, KatzCentralityResult};

mod maximal_cliques;
pub use maximal_cliques::{MaximalCliques, MaximalCliquesConfig, MaximalCliquesResult};

mod all_simple_paths;
pub use all_simple_paths::{AllSimplePaths, AllSimplePathsConfig, AllSimplePathsResult};

mod elementary_circuits;
pub use elementary_circuits::{
    ElementaryCircuits, ElementaryCircuitsConfig, ElementaryCircuitsResult,
};

mod graph_coloring;
pub use graph_coloring::{GraphColoring, GraphColoringConfig, GraphColoringResult};
