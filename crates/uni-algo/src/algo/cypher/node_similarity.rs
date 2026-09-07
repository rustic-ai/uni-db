// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.nodeSimilarity procedure implementation.

use crate::algo::algorithms::{Algorithm, NodeSimilarity, NodeSimilarityConfig, SimilarityMetric};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
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

    fn to_config(args: Vec<Value>) -> Result<NodeSimilarityConfig> {
        let metric_str = args[0].as_str().unwrap_or("JACCARD");
        // An unknown spelling used to fall through to Jaccard, so `metric:'COSIN'`
        // silently returned Jaccard scores under a Cosine-shaped request.
        let metric = match metric_str.to_uppercase().as_str() {
            "JACCARD" => SimilarityMetric::Jaccard,
            "OVERLAP" => SimilarityMetric::Overlap,
            "COSINE" => SimilarityMetric::Cosine,
            other => {
                return Err(anyhow!(
                    "unknown `metric` {other:?} for uni.algo.nodeSimilarity; \
                     accepted values are JACCARD, OVERLAP, COSINE (case-insensitive)"
                ));
            }
        };

        Ok(NodeSimilarityConfig {
            similarity_metric: metric,
            similarity_cutoff: args[1].as_f64().unwrap_or(0.1),
            top_k: args[2].as_u64().unwrap_or(10) as usize,
        })
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
}

pub type NodeSimilarityProcedure = GenericAlgoProcedure<NodeSimilarityAdapter>;

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for(metric: Value) -> Result<NodeSimilarityConfig> {
        NodeSimilarityAdapter::to_config(vec![metric, json!(0.1), json!(10)])
    }

    #[test]
    fn unknown_metric_errors_instead_of_falling_back_to_jaccard() {
        // A3: `metric:'COSIN'` used to silently return Jaccard scores.
        for (name, want) in [
            ("JACCARD", SimilarityMetric::Jaccard),
            ("overlap", SimilarityMetric::Overlap),
            ("Cosine", SimilarityMetric::Cosine),
        ] {
            let cfg = config_for(json!(name)).unwrap_or_else(|e| panic!("{name} must parse: {e}"));
            assert_eq!(cfg.similarity_metric, want, "metric {name}");
        }

        let err = config_for(json!("COSIN")).expect_err("a typo'd metric must error");
        let msg = err.to_string();
        assert!(msg.contains("COSIN"), "error must echo the input: {msg}");
        assert!(
            msg.contains("JACCARD") && msg.contains("OVERLAP") && msg.contains("COSINE"),
            "error must list the accepted values: {msg}"
        );
    }
}
