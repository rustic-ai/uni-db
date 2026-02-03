// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.labelPropagation procedure implementation.

use crate::algo::algorithms::{Algorithm, LabelPropagation, LabelPropagationConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct LabelPropagationAdapter;

impl GraphAlgoAdapter for LabelPropagationAdapter {
    const NAME: &'static str = "uni.algo.labelPropagation";
    type Algo = LabelPropagation;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("maxIterations", ValueType::Int, Some(json!(10))),
            ("write", ValueType::Bool, Some(json!(false))),
            ("writeProperty", ValueType::String, Some(json!("community"))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("communityId", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> LabelPropagationConfig {
        LabelPropagationConfig {
            max_iterations: args[0].as_u64().unwrap() as usize,
            write: args[1].as_bool().unwrap(),
            write_property: args[2].as_str().unwrap().to_string(),
            seed_property: None,
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .communities
            .into_iter()
            .map(|(vid, cid)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(cid)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type LabelPropagationProcedure = GenericAlgoProcedure<LabelPropagationAdapter>;
