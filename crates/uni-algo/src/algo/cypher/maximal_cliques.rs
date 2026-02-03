// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.maximalCliques procedure implementation.

use crate::algo::algorithms::{Algorithm, MaximalCliques, MaximalCliquesConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct MaximalCliquesAdapter;

impl GraphAlgoAdapter for MaximalCliquesAdapter {
    const NAME: &'static str = "uni.algo.maximalCliques";
    type Algo = MaximalCliques;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("minSize", ValueType::Int, Some(json!(2)))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("clique", ValueType::List)]
    }

    fn to_config(args: Vec<Value>) -> MaximalCliquesConfig {
        MaximalCliquesConfig {
            min_size: args[0].as_u64().unwrap_or(2) as usize,
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .cliques
            .into_iter()
            .map(|clique| {
                let clique_json: Vec<Value> =
                    clique.into_iter().map(|v| json!(v.as_u64())).collect();
                AlgoResultRow {
                    values: vec![Value::Array(clique_json)],
                }
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type MaximalCliquesProcedure = GenericAlgoProcedure<MaximalCliquesAdapter>;
