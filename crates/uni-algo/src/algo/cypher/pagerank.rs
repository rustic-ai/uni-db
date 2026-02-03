// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.pageRank procedure implementation.

use crate::algo::algorithms::{Algorithm, PageRank, PageRankConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct PageRankAdapter;

impl GraphAlgoAdapter for PageRankAdapter {
    const NAME: &'static str = "uni.algo.pageRank";
    type Algo = PageRank;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("dampingFactor", ValueType::Float, Some(json!(0.85))),
            ("maxIterations", ValueType::Int, Some(json!(20))),
            ("tolerance", ValueType::Float, Some(json!(1e-6))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> PageRankConfig {
        PageRankConfig {
            damping_factor: args[0].as_f64().unwrap(),
            max_iterations: args[1].as_u64().unwrap() as usize,
            tolerance: args[2].as_f64().unwrap(),
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .scores
            .into_iter()
            .map(|(vid, score)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(score)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type PageRankProcedure = GenericAlgoProcedure<PageRankAdapter>;
