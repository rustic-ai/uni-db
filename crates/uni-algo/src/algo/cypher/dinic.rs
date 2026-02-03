// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.maxFlow procedure implementation (using Dinic).

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, Dinic, DinicConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};
use uni_common::core::id::Vid;

pub struct DinicAdapter;

impl GraphAlgoAdapter for DinicAdapter {
    const NAME: &'static str = "uni.algo.maxFlow";
    type Algo = Dinic;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("sourceNode", ValueType::Node, None),
            ("sinkNode", ValueType::Node, None),
            ("capacityProperty", ValueType::String, None),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("maxFlow", ValueType::Float), ("flowEdges", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> DinicConfig {
        DinicConfig {
            source: Vid::from(args[0].as_u64().unwrap_or(0)),
            sink: Vid::from(args[1].as_u64().unwrap_or(0)),
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vec![AlgoResultRow {
            values: vec![json!(result.max_flow), json!(0)],
        }])
    }

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[2].as_str() {
            builder = builder.weight_property(prop);
        }
        builder
    }
}

pub type DinicProcedure = GenericAlgoProcedure<DinicAdapter>;
