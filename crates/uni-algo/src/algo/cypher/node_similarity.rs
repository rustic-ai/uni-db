// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.nodeSimilarity procedure implementation.

use crate::algo::algorithms::{Algorithm, NodeSimilarity, NodeSimilarityConfig, SimilarityMetric};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct NodeSimilarityAdapter;

impl GraphAlgoAdapter for NodeSimilarityAdapter {
    const NAME: &'static str = "uni.algo.nodeSimilarity";
    type Algo = NodeSimilarity;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("metric", ValueType::String, Some(json!("JACCARD"))),
            ("similarityCutoff", ValueType::Float, Some(json!(0.1))),
            ("topK", ValueType::Int, Some(json!(10))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("node1", ValueType::Int),
            ("node2", ValueType::Int),
            ("similarity", ValueType::Float),
        ]
    }

    fn to_config(args: Vec<Value>) -> NodeSimilarityConfig {
        let metric_str = args[0].as_str().unwrap_or("JACCARD");
        let metric = match metric_str.to_uppercase().as_str() {
            "OVERLAP" => SimilarityMetric::Overlap,
            "COSINE" => SimilarityMetric::Cosine,
            _ => SimilarityMetric::Jaccard,
        };

        NodeSimilarityConfig {
            similarity_metric: metric,
            similarity_cutoff: args[1].as_f64().unwrap_or(0.1),
            top_k: args[2].as_u64().unwrap_or(10) as usize,
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .similar_pairs
            .into_iter()
            .map(|(u, v, score)| AlgoResultRow {
                values: vec![json!(u.as_u64()), json!(v.as_u64()), json!(score)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type NodeSimilarityProcedure = GenericAlgoProcedure<NodeSimilarityAdapter>;
