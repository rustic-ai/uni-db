// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.scc procedure implementation.

use crate::algo::algorithms::{Algorithm, Scc, SccConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct SccAdapter;

impl GraphAlgoAdapter for SccAdapter {
    const NAME: &'static str = "uni.algo.scc";
    type Algo = Scc;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("componentId", ValueType::Int)]
    }

    fn to_config(_args: Vec<Value>) -> SccConfig {
        SccConfig::default()
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

pub type SccProcedure = GenericAlgoProcedure<SccAdapter>;
