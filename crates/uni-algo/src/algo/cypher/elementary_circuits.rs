// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.elementaryCircuits procedure implementation.

use crate::algo::algorithms::{Algorithm, ElementaryCircuits, ElementaryCircuitsConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct ElementaryCircuitsAdapter;

impl GraphAlgoAdapter for ElementaryCircuitsAdapter {
    const NAME: &'static str = "uni.algo.elementaryCircuits";
    type Algo = ElementaryCircuits;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("limit", ValueType::Int, Some(json!(1000)))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("cycle", ValueType::List)]
    }

    fn to_config(args: Vec<Value>) -> ElementaryCircuitsConfig {
        ElementaryCircuitsConfig {
            limit: args[0].as_u64().unwrap_or(1000) as usize,
            ..Default::default()
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .cycles
            .into_iter()
            .map(|cycle| {
                let cycle_json: Vec<Value> = cycle.into_iter().map(|v| json!(v.as_u64())).collect();
                AlgoResultRow {
                    values: vec![Value::Array(cycle_json)],
                }
            })
            .collect())
    }
}

pub type ElementaryCircuitsProcedure = GenericAlgoProcedure<ElementaryCircuitsAdapter>;
