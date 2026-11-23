// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.mst procedure implementation.

use crate::algo::algorithms::{Algorithm, MinimumSpanningTree, MstConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct MstAdapter;

impl GraphAlgoAdapter for MstAdapter {
    const NAME: &'static str = "uni.algo.mst";
    type Algo = MinimumSpanningTree;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("weightProperty", ValueType::String, None)]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("source", ValueType::Int),
            ("target", ValueType::Int),
            ("weight", ValueType::Float),
        ]
    }

    fn to_config(_args: Vec<Value>) -> Result<MstConfig> {
        Ok(MstConfig {})
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .edges
            .into_iter()
            .map(|(u, v, w)| AlgoResultRow {
                values: vec![json!(u.as_u64()), json!(v.as_u64()), json!(w)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        false
    }

    fn weight_arg_index() -> Option<usize> {
        Some(0)
    }
}

pub type MstProcedure = GenericAlgoProcedure<MstAdapter>;
