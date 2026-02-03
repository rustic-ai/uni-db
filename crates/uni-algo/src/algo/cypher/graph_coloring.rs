// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.graphColoring procedure implementation.

use crate::algo::algorithms::{Algorithm, GraphColoring, GraphColoringConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct GraphColoringAdapter;

impl GraphAlgoAdapter for GraphColoringAdapter {
    const NAME: &'static str = "uni.algo.graphColoring";
    type Algo = GraphColoring;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("color", ValueType::Int)]
    }

    fn to_config(_args: Vec<Value>) -> GraphColoringConfig {
        GraphColoringConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .coloring
            .into_iter()
            .map(|(vid, color)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(color)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type GraphColoringProcedure = GenericAlgoProcedure<GraphColoringAdapter>;
