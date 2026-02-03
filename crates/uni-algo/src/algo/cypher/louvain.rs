// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.louvain procedure implementation.

use crate::algo::algorithms::{Algorithm, Louvain, LouvainConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct LouvainAdapter;

impl GraphAlgoAdapter for LouvainAdapter {
    const NAME: &'static str = "uni.algo.louvain";
    type Algo = Louvain;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("resolution", ValueType::Float, Some(json!(1.0))),
            ("maxIterations", ValueType::Int, Some(json!(10))),
            ("minModularityGain", ValueType::Float, Some(json!(1e-4))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("communityId", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> LouvainConfig {
        LouvainConfig {
            resolution: args[0].as_f64().unwrap(),
            max_iterations: args[1].as_u64().unwrap() as usize,
            min_modularity_gain: args[2].as_f64().unwrap(),
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .communities
            .into_iter()
            .map(|(vid, cid)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(cid)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type LouvainProcedure = GenericAlgoProcedure<LouvainAdapter>;
