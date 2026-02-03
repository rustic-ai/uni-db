// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.closeness procedure implementation.

use crate::algo::algorithms::{Algorithm, Closeness, ClosenessConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct ClosenessAdapter;

impl GraphAlgoAdapter for ClosenessAdapter {
    const NAME: &'static str = "uni.algo.closeness";
    type Algo = Closeness;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("wassermanFaust", ValueType::Bool, Some(json!(false)))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> ClosenessConfig {
        ClosenessConfig {
            wasserman_faust: args[0].as_bool().unwrap_or(false),
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
}

pub type ClosenessProcedure = GenericAlgoProcedure<ClosenessAdapter>;
