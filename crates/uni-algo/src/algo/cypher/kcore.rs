// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.kCore procedure implementation.

use crate::algo::algorithms::{Algorithm, KCore, KCoreConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct KCoreAdapter;

impl GraphAlgoAdapter for KCoreAdapter {
    const NAME: &'static str = "uni.algo.kCore";
    type Algo = KCore;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("k", ValueType::Int, Some(Value::Null))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("coreNumber", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> KCoreConfig {
        KCoreConfig {
            k: args[0].as_u64().map(|v| v as usize),
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .core_numbers
            .into_iter()
            .map(|(vid, core)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(core)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type KCoreProcedure = GenericAlgoProcedure<KCoreAdapter>;
