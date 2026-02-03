// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.randomWalk procedure implementation.

use crate::algo::algorithms::{Algorithm, RandomWalk, RandomWalkConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use uni_common::core::id::Vid;

pub struct RandomWalkAdapter;

impl GraphAlgoAdapter for RandomWalkAdapter {
    const NAME: &'static str = "uni.algo.randomWalk";
    type Algo = RandomWalk;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("walkLength", ValueType::Int, Some(json!(5))),
            ("walksPerNode", ValueType::Int, Some(json!(1))),
            ("startNodes", ValueType::List, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("path", ValueType::List)]
    }

    fn to_config(args: Vec<Value>) -> RandomWalkConfig {
        let walk_length = args[0].as_u64().unwrap_or(5) as usize;
        let walks_per_node = args[1].as_u64().unwrap_or(1) as usize;

        let start_nodes = if args[2].is_null() {
            Vec::new()
        } else {
            let list = args[2].as_array().unwrap();
            let mut vids = Vec::new();
            for val in list {
                if let Ok(v) = vid_from_value(val) {
                    vids.push(v);
                }
            }
            vids
        };

        RandomWalkConfig {
            walk_length,
            walks_per_node,
            start_nodes,
            return_param: 1.0,
            in_out_param: 1.0,
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .walks
            .into_iter()
            .map(|walk| {
                let path: Vec<Value> = walk.iter().map(|v| json!(v.as_u64())).collect();
                AlgoResultRow {
                    values: vec![Value::Array(path)],
                }
            })
            .collect())
    }
}

fn vid_from_value(val: &Value) -> Result<Vid> {
    if let Some(s) = val.as_str()
        && let Ok(vid) = s.parse::<Vid>()
    {
        return Ok(vid);
    }
    if let Some(v) = val.as_u64() {
        return Ok(Vid::from(v));
    }
    Err(anyhow!("Invalid Vid format"))
}

pub type RandomWalkProcedure = GenericAlgoProcedure<RandomWalkAdapter>;
