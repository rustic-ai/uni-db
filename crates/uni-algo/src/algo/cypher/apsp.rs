// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.allPairsShortestPath procedure implementation.

use crate::algo::algorithms::{Algorithm, AllPairsShortestPath, AllPairsShortestPathConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct AllPairsShortestPathAdapter;

impl GraphAlgoAdapter for AllPairsShortestPathAdapter {
    const NAME: &'static str = "uni.algo.allPairsShortestPath";
    type Algo = AllPairsShortestPath;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("sourceNodeId", ValueType::Int),
            ("targetNodeId", ValueType::Int),
            ("distance", ValueType::Int),
        ]
    }

    fn to_config(_args: Vec<Value>) -> AllPairsShortestPathConfig {
        AllPairsShortestPathConfig
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .distances
            .into_iter()
            .map(|(u, v, dist)| AlgoResultRow {
                values: vec![json!(u.as_u64()), json!(v.as_u64()), json!(dist)],
            })
            .collect())
    }
}

pub type AllPairsShortestPathProcedure = GenericAlgoProcedure<AllPairsShortestPathAdapter>;
