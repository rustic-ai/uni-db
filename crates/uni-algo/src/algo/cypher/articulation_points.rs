// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.articulationPoints procedure implementation.

use crate::algo::algorithms::{Algorithm, ArticulationPoints, ArticulationPointsConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct ArticulationPointsAdapter;

impl GraphAlgoAdapter for ArticulationPointsAdapter {
    const NAME: &'static str = "uni.algo.articulationPoints";
    type Algo = ArticulationPoints;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("node", ValueType::Node)]
    }

    fn to_config(_args: Vec<Value>) -> ArticulationPointsConfig {
        ArticulationPointsConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .points
            .into_iter()
            .map(|vid| AlgoResultRow {
                values: vec![json!(vid.as_u64())],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type ArticulationPointsProcedure = GenericAlgoProcedure<ArticulationPointsAdapter>;
