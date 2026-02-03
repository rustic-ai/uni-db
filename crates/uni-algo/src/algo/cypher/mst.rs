// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.mst procedure implementation.

use crate::algo::ProjectionBuilder;
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
            ("source", ValueType::Node),
            ("target", ValueType::Node),
            ("weight", ValueType::Float),
        ]
    }

    fn to_config(_args: Vec<Value>) -> MstConfig {
        MstConfig {}
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

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[0].as_str() {
            builder = builder.weight_property(prop);
        }
        builder
    }
}

pub type MstProcedure = GenericAlgoProcedure<MstAdapter>;
