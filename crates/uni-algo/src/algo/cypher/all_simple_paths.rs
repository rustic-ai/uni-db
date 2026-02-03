// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.allSimplePaths procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, AllSimplePaths, AllSimplePathsConfig};
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::Result;
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::{Value, json};
use uni_common::core::id::Vid;

pub struct AllSimplePathsProcedure;

impl AlgoProcedure for AllSimplePathsProcedure {
    fn name(&self) -> &str {
        "uni.algo.allSimplePaths"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                ("startNode", ValueType::Node),
                ("endNode", ValueType::Node),
                ("relationshipTypes", ValueType::List),
                ("maxLength", ValueType::Int),
            ],
            optional_args: vec![("nodeLabels", ValueType::List, Value::Null)],
            yields: vec![("path", ValueType::List)],
        }
    }

    fn execute(
        &self,
        ctx: AlgoContext,
        args: Vec<Value>,
    ) -> BoxStream<'static, Result<AlgoResultRow>> {
        let signature = self.signature();
        let args = match signature.validate_args(args) {
            Ok(a) => a,
            Err(e) => return stream::once(async { Err(e) }).boxed(),
        };

        let start_vid_u64 = match &args[0] {
            Value::String(s) => s.parse::<u64>().unwrap_or(0),
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => return stream::once(async { Err(anyhow::anyhow!("Invalid start node")) }).boxed(),
        };

        let end_vid_u64 = match &args[1] {
            Value::String(s) => s.parse::<u64>().unwrap_or(0),
            Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => return stream::once(async { Err(anyhow::anyhow!("Invalid end node")) }).boxed(),
        };

        let edge_types = args[2]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        let max_len = args[3].as_u64().unwrap() as usize;

        let node_labels = if args[4].is_null() {
            Vec::new()
        } else {
            args[4]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };

        let start_vid = Vid::from(start_vid_u64);
        let end_vid = Vid::from(end_vid_u64);

        let stream = async_stream::try_stream! {
            let schema = ctx.storage.schema_manager().schema();

            if !node_labels.is_empty() {
                for label in &node_labels {
                    if !schema.labels.contains_key(label) {
                        Err(anyhow::anyhow!("Label '{}' not found", label))?;
                    }
                }
            }
            for etype in &edge_types {
                if !schema.edge_types.contains_key(etype) {
                    Err(anyhow::anyhow!("Edge type '{}' not found", etype))?;
                }
            }

            let mut builder = ProjectionBuilder::new(ctx.storage.clone())
                .l0_manager(ctx.l0_manager.clone())
                .edge_types(&edge_types.iter().map(|s| s.as_str()).collect::<Vec<_>>());

            if !node_labels.is_empty() {
                builder = builder.node_labels(&node_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>());
            }

            let projection = builder.build().await?;

            let config = AllSimplePathsConfig {
                source: start_vid,
                target: end_vid,
                max_depth: max_len,
                limit: 1000,
                min_depth: 0,
            };

            let result = tokio::task::spawn_blocking(move || {
                AllSimplePaths::run(&projection, config)
            }).await?;

            for path in result.paths {
                let path_json: Vec<Value> = path.into_iter().map(|v| json!(v.as_u64())).collect();
                yield AlgoResultRow {
                    values: vec![
                        Value::Array(path_json),
                    ],
                };
            }
        };

        Box::pin(stream)
    }
}
