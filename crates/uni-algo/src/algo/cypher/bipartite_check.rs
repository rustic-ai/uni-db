// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.isBipartite procedure implementation.

use crate::algo::algorithms::{Algorithm, BipartiteCheck, BipartiteCheckConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct BipartiteCheckAdapter;

impl GraphAlgoAdapter for BipartiteCheckAdapter {
    const NAME: &'static str = "uni.algo.isBipartite";
    type Algo = BipartiteCheck;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("isBipartite", ValueType::Bool),
            ("partition", ValueType::Map),
        ]
    }

    fn to_config(_args: Vec<Value>) -> BipartiteCheckConfig {
        BipartiteCheckConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        let mut partition_map = serde_json::Map::new();
        if result.is_bipartite {
            for (vid, color) in result.partition {
                partition_map.insert(vid.to_string(), json!(color));
            }
        }

        Ok(vec![AlgoResultRow {
            values: vec![json!(result.is_bipartite), Value::Object(partition_map)],
        }])
    }

    fn include_reverse() -> bool {
        true
    }
}

pub type BipartiteCheckProcedure = GenericAlgoProcedure<BipartiteCheckAdapter>;
