// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.wcc procedure implementation.

use crate::algo::algorithms::{Algorithm, Wcc, WccConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct WccAdapter;

impl GraphAlgoAdapter for WccAdapter {
    const NAME: &'static str = "uni.algo.wcc";
    type Algo = Wcc;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("minComponentSize", ValueType::Int, Some(json!(1)))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("componentId", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> WccConfig {
        WccConfig {
            min_component_size: Some(args[0].as_u64().unwrap() as usize),
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .components
            .into_iter()
            .map(|(vid, cid)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(cid)],
            })
            .collect())
    }
}

pub type WccProcedure = GenericAlgoProcedure<WccAdapter>;
