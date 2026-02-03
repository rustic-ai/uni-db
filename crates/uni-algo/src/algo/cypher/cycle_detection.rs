// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.hasCycle procedure implementation.

use crate::algo::algorithms::{Algorithm, CycleDetection, CycleDetectionConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct CycleDetectionAdapter;

impl GraphAlgoAdapter for CycleDetectionAdapter {
    const NAME: &'static str = "uni.algo.hasCycle";
    type Algo = CycleDetection;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("hasCycle", ValueType::Bool),
            ("cycleNodes", ValueType::List),
        ]
    }

    fn to_config(_args: Vec<Value>) -> CycleDetectionConfig {
        CycleDetectionConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        let cycle_nodes_json: Vec<Value> = result
            .cycle_nodes
            .into_iter()
            .map(|vid| json!(vid.as_u64()))
            .collect();

        Ok(vec![AlgoResultRow {
            values: vec![json!(result.has_cycle), Value::Array(cycle_nodes_json)],
        }])
    }
}

pub type CycleDetectionProcedure = GenericAlgoProcedure<CycleDetectionAdapter>;
