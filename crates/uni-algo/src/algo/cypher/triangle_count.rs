// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.triangleCount procedure implementation.

use crate::algo::algorithms::{Algorithm, TriangleCount, TriangleCountConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct TriangleCountAdapter;

impl GraphAlgoAdapter for TriangleCountAdapter {
    const NAME: &'static str = "uni.algo.triangleCount";
    type Algo = TriangleCount;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("nodeId", ValueType::Int),
            ("triangleCount", ValueType::Int),
        ]
    }

    fn to_config(_args: Vec<Value>) -> TriangleCountConfig {
        TriangleCountConfig
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .node_counts
            .into_iter()
            .map(|(vid, count)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(count)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type TriangleCountProcedure = GenericAlgoProcedure<TriangleCountAdapter>;
