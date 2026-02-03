// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.kShortestPaths procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, KShortestPaths, KShortestPathsConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};
use uni_common::core::id::Vid;

pub struct KShortestPathsAdapter;

impl GraphAlgoAdapter for KShortestPathsAdapter {
    const NAME: &'static str = "uni.algo.kShortestPaths";
    type Algo = KShortestPaths;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("startNode", ValueType::Node, None),
            ("endNode", ValueType::Node, None),
            ("k", ValueType::Int, None),
            ("weightProperty", ValueType::String, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("path", ValueType::List),
            ("cost", ValueType::Float),
            ("rank", ValueType::Int),
        ]
    }

    fn to_config(args: Vec<Value>) -> KShortestPathsConfig {
        KShortestPathsConfig {
            source: Vid::from(args[0].as_u64().unwrap_or(0)),
            target: Vid::from(args[1].as_u64().unwrap_or(0)),
            k: args[2].as_u64().unwrap_or(1) as usize,
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .paths
            .into_iter()
            .enumerate()
            .map(|(i, (path, cost))| {
                let path_json: Vec<Value> = path.into_iter().map(|v| json!(v.as_u64())).collect();
                AlgoResultRow {
                    values: vec![Value::Array(path_json), json!(cost), json!(i + 1)],
                }
            })
            .collect())
    }

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[3].as_str() {
            builder = builder.weight_property(prop);
        }
        builder
    }
}

pub type KShortestPathsProcedure = GenericAlgoProcedure<KShortestPathsAdapter>;
