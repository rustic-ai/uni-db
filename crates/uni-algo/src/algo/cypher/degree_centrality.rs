// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.degreeCentrality procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{
    Algorithm, DegreeCentrality, DegreeCentralityConfig, DegreeDirection,
};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct DegreeCentralityAdapter;

impl GraphAlgoAdapter for DegreeCentralityAdapter {
    const NAME: &'static str = "uni.algo.degreeCentrality";
    type Algo = DegreeCentrality;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("direction", ValueType::String, Some(json!("OUTGOING")))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> DegreeCentralityConfig {
        let direction_str = args[0].as_str().unwrap_or("OUTGOING").to_uppercase();
        let direction = match direction_str.as_str() {
            "INCOMING" | "IN" => DegreeDirection::Incoming,
            "BOTH" => DegreeDirection::Both,
            _ => DegreeDirection::Outgoing,
        };

        DegreeCentralityConfig { direction }
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

    fn customize_projection(builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        let direction_str = args[0].as_str().unwrap_or("OUTGOING").to_uppercase();
        let include_reverse = matches!(direction_str.as_str(), "INCOMING" | "IN" | "BOTH");
        builder.include_reverse(include_reverse)
    }
}

pub type DegreeCentralityProcedure = GenericAlgoProcedure<DegreeCentralityAdapter>;
