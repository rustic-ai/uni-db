// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.maxMatching procedure implementation.

use crate::algo::algorithms::{Algorithm, MaximumMatching, MaximumMatchingConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};

pub struct MaxMatchingAdapter;

impl GraphAlgoAdapter for MaxMatchingAdapter {
    const NAME: &'static str = "uni.algo.maxMatching";
    type Algo = MaximumMatching;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("node1", ValueType::Node),
            ("node2", ValueType::Node),
            ("matchId", ValueType::Int),
        ]
    }

    fn to_config(_args: Vec<Value>) -> MaximumMatchingConfig {
        MaximumMatchingConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        match result {
            Ok(res) => Ok(res
                .matching
                .into_iter()
                .map(|(u, v)| AlgoResultRow {
                    values: vec![json!(u.as_u64()), json!(v.as_u64()), json!(0)],
                })
                .collect()),
            Err(e) => Err(anyhow!("MaxMatching error: {}", e)),
        }
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type MaxMatchingProcedure = GenericAlgoProcedure<MaxMatchingAdapter>;
