// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.bidirectionalDijkstra procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, BidirectionalDijkstra, BidirectionalDijkstraConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};
use uni_common::core::id::Vid;

pub struct BidirectionalDijkstraAdapter;

impl GraphAlgoAdapter for BidirectionalDijkstraAdapter {
    const NAME: &'static str = "uni.algo.bidirectionalDijkstra";
    type Algo = BidirectionalDijkstra;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("startNode", ValueType::Node, None),
            ("endNode", ValueType::Node, None),
            ("weightProperty", ValueType::String, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("distance", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> BidirectionalDijkstraConfig {
        BidirectionalDijkstraConfig {
            source: Vid::from(args[0].as_u64().unwrap_or(0)),
            target: Vid::from(args[1].as_u64().unwrap_or(0)),
        }
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        let mut rows = Vec::new();
        if let Some(d) = result.distance {
            rows.push(AlgoResultRow {
                values: vec![json!(d)],
            });
        }
        Ok(rows)
    }

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[2].as_str() {
            builder = builder.weight_property(prop);
        }
        builder.include_reverse(true)
    }
}

pub type BidirectionalDijkstraProcedure = GenericAlgoProcedure<BidirectionalDijkstraAdapter>;
