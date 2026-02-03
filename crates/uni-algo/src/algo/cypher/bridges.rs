// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.bridges procedure implementation.

use crate::algo::algorithms::{Algorithm, Bridges, BridgesConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct BridgesAdapter;

impl GraphAlgoAdapter for BridgesAdapter {
    const NAME: &'static str = "uni.algo.bridges";
    type Algo = Bridges;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("source", ValueType::Node), ("target", ValueType::Node)]
    }

    fn to_config(_args: Vec<Value>) -> BridgesConfig {
        BridgesConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .bridges
            .into_iter()
            .map(|(u, v)| AlgoResultRow {
                values: vec![json!(u.as_u64()), json!(v.as_u64())],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type BridgesProcedure = GenericAlgoProcedure<BridgesAdapter>;
