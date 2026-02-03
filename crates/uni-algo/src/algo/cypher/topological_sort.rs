// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.topoSort procedure implementation.

use crate::algo::algorithms::{Algorithm, TopologicalSort, TopologicalSortConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};

pub struct TopologicalSortAdapter;

impl GraphAlgoAdapter for TopologicalSortAdapter {
    const NAME: &'static str = "uni.algo.topoSort";
    type Algo = TopologicalSort;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("order", ValueType::Int)]
    }

    fn to_config(_args: Vec<Value>) -> TopologicalSortConfig {
        TopologicalSortConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        if result.has_cycle {
            return Err(anyhow!(
                "Graph has a cycle, topological sort is not possible"
            ));
        }

        Ok(result
            .sorted_nodes
            .into_iter()
            .enumerate()
            .map(|(i, vid)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(i as i64)],
            })
            .collect())
    }
}

pub type TopologicalSortProcedure = GenericAlgoProcedure<TopologicalSortAdapter>;
