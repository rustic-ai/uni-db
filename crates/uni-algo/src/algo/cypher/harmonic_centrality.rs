// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.harmonicCentrality procedure implementation.

use crate::algo::algorithms::{Algorithm, HarmonicCentrality, HarmonicCentralityConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct HarmonicCentralityAdapter;

impl GraphAlgoAdapter for HarmonicCentralityAdapter {
    const NAME: &'static str = "uni.algo.harmonicCentrality";
    type Algo = HarmonicCentrality;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("centrality", ValueType::Float)]
    }

    fn to_config(_args: Vec<Value>) -> HarmonicCentralityConfig {
        HarmonicCentralityConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .scores
            .into_iter()
            .map(|(vid, score)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(score)],
            })
            .collect())
    }
}

pub type HarmonicCentralityProcedure = GenericAlgoProcedure<HarmonicCentralityAdapter>;
