// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph Algorithm Engine
//!
//! This module provides native graph algorithm implementations for Uni.
//!
//! # Architecture
//!
//! Two execution paths are supported:
//!
//! - **DirectTraversal**: Zero-copy traversal on `AdjacencyManager` + `L0Buffer`.
//!   Best for light algorithms (BFS, single shortest path).
//!
//! - **GraphProjection**: Materialized dense CSR for heavy algorithms.
//!   Best for iterative algorithms (PageRank, WCC, Louvain).
//!
//! # Example
//!
//! ```ignore
//! use uni_db::algo::{ProjectionBuilder, PageRank, Algorithm};
//!
//! // Build projection
//! let projection = ProjectionBuilder::new(storage, cache, l0)
//!     .node_labels(&["Person"])
//!     .edge_types(&["KNOWS"])
//!     .include_reverse(true)
//!     .build()
//!     .await?;
//!
//! // Run algorithm
//! let result = PageRank::run(&projection, Default::default());
//! ```

mod id_map;
pub mod projection;
mod traversal;

pub mod algorithms;
pub mod cypher;
pub mod procedure_template;
pub mod procedures;

pub use id_map::IdMap;
pub use projection::{GraphProjection, ProjectionBuilder, ProjectionConfig};
pub use traversal::{BfsIterator, DirectTraversal, Path};

#[cfg(test)]
pub mod test_utils;

use std::collections::HashMap;
use std::sync::Arc;

/// Algorithm registry for procedure dispatch
pub struct AlgorithmRegistry {
    procedures: HashMap<String, Arc<dyn procedures::AlgoProcedure>>,
}

impl AlgorithmRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            procedures: HashMap::default(),
        };

        // Register built-in algorithms
        registry.register(cypher::PageRankProcedure::default());
        registry.register(cypher::WccProcedure::default());
        registry.register(cypher::ShortestPathProcedure);
        registry.register(cypher::LouvainProcedure::default());
        registry.register(cypher::LabelPropagationProcedure::default());
        registry.register(cypher::BetweennessProcedure::default());
        registry.register(cypher::NodeSimilarityProcedure::default());
        registry.register(cypher::ClosenessProcedure::default());
        registry.register(cypher::TriangleCountProcedure::default());
        registry.register(cypher::KCoreProcedure::default());
        registry.register(cypher::RandomWalkProcedure::default());
        registry.register(cypher::AllPairsShortestPathProcedure::default());
        registry.register(cypher::SccProcedure::default());
        registry.register(cypher::TopologicalSortProcedure::default());
        registry.register(cypher::CycleDetectionProcedure::default());
        registry.register(cypher::BipartiteCheckProcedure::default());
        registry.register(cypher::BridgesProcedure::default());
        registry.register(cypher::ArticulationPointsProcedure::default());
        registry.register(cypher::AStarProcedure);
        registry.register(cypher::BellmanFordProcedure::default());
        registry.register(cypher::BidirectionalDijkstraProcedure::default());
        registry.register(cypher::KShortestPathsProcedure::default());
        registry.register(cypher::MstProcedure::default());
        registry.register(cypher::MaxMatchingProcedure::default());
        registry.register(cypher::DinicProcedure::default());
        registry.register(cypher::FordFulkersonProcedure::default());
        registry.register(cypher::GraphMetricsProcedure::default());
        registry.register(cypher::DiameterProcedure::default());
        registry.register(cypher::DegreeCentralityProcedure::default());
        registry.register(cypher::HarmonicCentralityProcedure::default());
        registry.register(cypher::EigenvectorCentralityProcedure::default());
        registry.register(cypher::KatzCentralityProcedure::default());
        registry.register(cypher::MaximalCliquesProcedure::default());
        registry.register(cypher::AllSimplePathsProcedure);
        registry.register(cypher::ElementaryCircuitsProcedure::default());
        registry.register(cypher::GraphColoringProcedure::default());

        registry
    }

    pub fn register<P: procedures::AlgoProcedure + 'static>(&mut self, proc: P) {
        self.procedures
            .insert(proc.name().to_string(), Arc::new(proc));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn procedures::AlgoProcedure>> {
        self.procedures.get(name).cloned()
    }

    pub fn list(&self) -> Vec<&str> {
        self.procedures.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for AlgorithmRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for algorithm execution
#[derive(Debug, Clone)]
pub struct AlgorithmConfig {
    /// Maximum memory for all projections (bytes)
    pub max_projection_memory: usize,
    /// Maximum vertices per projection
    pub max_vertices: usize,
    /// Warn if L0 exceeds this fraction of graph
    pub l0_warning_threshold: f64,
}

impl Default for AlgorithmConfig {
    fn default() -> Self {
        Self {
            max_projection_memory: 1 << 30, // 1 GB
            max_vertices: 100_000_000,      // 100M
            l0_warning_threshold: 0.1,      // 10%
        }
    }
}
