// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.allSimplePaths procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, AllSimplePaths, AllSimplePathsConfig};
use crate::algo::procedure_template::{arg_string_list, arg_u64, err_stream, parse_vid_arg};
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::Result;
use futures::stream::BoxStream;
use serde_json::{Value, json};

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

    fn wants_native_terminals(&self) -> bool {
        true
    }

    fn execute_with_native_terminals(
        &self,
        ctx: AlgoContext,
        args: Vec<Value>,
    ) -> BoxStream<'static, Result<AlgoResultRow>> {
        let signature = self.signature();
        let args = match signature.validate_args(args) {
            Ok(a) => a,
            Err(e) => return err_stream(e),
        };

        // Parse every terminal up front; bad input now surfaces a clear
        // error instead of the old `unwrap_or(0)` / `unwrap()`.
        let (start_vid, end_vid, edge_types, max_len, node_labels) = match (|| {
            let node_labels = if args[4].is_null() {
                Vec::new()
            } else {
                arg_string_list(&args, 4, "nodeLabels")?
            };
            Ok((
                parse_vid_arg(&args[0], "startNode")?,
                parse_vid_arg(&args[1], "endNode")?,
                arg_string_list(&args, 2, "relationshipTypes")?,
                arg_u64(&args, 3, "maxLength")? as usize,
                node_labels,
            ))
        })() {
            Ok(parsed) => parsed,
            Err(e) => return err_stream(e),
        };

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
                .property_manager(ctx.property_manager.clone())
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
