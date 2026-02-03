// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.katzCentrality procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, KatzCentrality, KatzCentralityConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct KatzCentralityAdapter;

impl GraphAlgoAdapter for KatzCentralityAdapter {
    const NAME: &'static str = "uni.algo.katzCentrality";
    type Algo = KatzCentrality;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("alpha", ValueType::Float, Some(json!(0.1))),
            ("beta", ValueType::Float, Some(json!(1.0))),
            ("maxIterations", ValueType::Int, Some(json!(100))),
            ("tolerance", ValueType::Float, Some(json!(1e-6))),
            ("weightProperty", ValueType::String, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> KatzCentralityConfig {
        KatzCentralityConfig {
            alpha: args[0].as_f64().unwrap_or(0.1),
            beta: args[1].as_f64().unwrap_or(1.0),
            max_iterations: args[2].as_u64().unwrap_or(100) as usize,
            tolerance: args[3].as_f64().unwrap_or(1e-6),
        }
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

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[4].as_str() {
            builder = builder.weight_property(prop);
        }
        builder
    }
}

pub type KatzCentralityProcedure = GenericAlgoProcedure<KatzCentralityAdapter>;
