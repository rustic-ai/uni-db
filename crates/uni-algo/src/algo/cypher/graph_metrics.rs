// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.metrics procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, GraphMetrics, GraphMetricsConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct GraphMetricsAdapter;

impl GraphAlgoAdapter for GraphMetricsAdapter {
    const NAME: &'static str = "uni.algo.metrics";
    type Algo = GraphMetrics;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("weightProperty", ValueType::String, Some(Value::Null))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("diameter", ValueType::Float),
            ("radius", ValueType::Float),
            ("center", ValueType::List),
            ("periphery", ValueType::List),
        ]
    }

    fn to_config(_args: Vec<Value>) -> GraphMetricsConfig {
        GraphMetricsConfig {}
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        let center_json: Vec<Value> = result
            .center
            .into_iter()
            .map(|v| json!(v.as_u64()))
            .collect();
        let periphery_json: Vec<Value> = result
            .periphery
            .into_iter()
            .map(|v| json!(v.as_u64()))
            .collect();

        Ok(vec![AlgoResultRow {
            values: vec![
                json!(result.diameter),
                json!(result.radius),
                Value::Array(center_json),
                Value::Array(periphery_json),
            ],
        }])
    }

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[0].as_str() {
            builder = builder.weight_property(prop);
        }
        builder
    }
}

pub type GraphMetricsProcedure = GenericAlgoProcedure<GraphMetricsAdapter>;
