// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.astar procedure implementation.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{AStar, AStarConfig, Algorithm};
use crate::algo::procedure_template::{arg_str, err_stream, parse_vid_arg};
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::Result;
use futures::stream::BoxStream;
use serde_json::{Value, json};
use std::collections::HashMap;
use uni_common::core::id::Vid;

pub struct AStarProcedure;

impl AlgoProcedure for AStarProcedure {
    fn name(&self) -> &str {
        "uni.algo.astar"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                ("startNode", ValueType::Node),
                ("endNode", ValueType::Node),
                ("edgeType", ValueType::String),
                ("heuristicProperty", ValueType::String),
            ],
            optional_args: vec![],
            yields: vec![
                ("path", ValueType::List), // List of VIDs
                ("cost", ValueType::Float),
            ],
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
        // error instead of the old `unwrap_or(0)` (silent vertex-0 routing)
        // or `unwrap()` (panic).
        let (start_vid, end_vid, edge_type, heuristic_prop) = match (|| {
            Ok((
                parse_vid_arg(&args[0], "startNode")?,
                parse_vid_arg(&args[1], "endNode")?,
                arg_str(&args, 2, "edgeType")?.to_string(),
                arg_str(&args, 3, "heuristicProperty")?.to_string(),
            ))
        })() {
            Ok(parsed) => parsed,
            Err(e) => return err_stream(e),
        };

        let stream = async_stream::try_stream! {
            let schema = ctx.storage.schema_manager().schema();

            if !schema.edge_types.contains_key(&edge_type) {
                Err(anyhow::anyhow!("Edge type '{}' not found", edge_type))?;
            }

            let edge_meta = schema.edge_types.get(&edge_type).unwrap();
            let mut labels = edge_meta.src_labels.clone();
            labels.extend(edge_meta.dst_labels.clone());
            labels.sort();
            labels.dedup();

            // 1. Build Projection
            let projection = ProjectionBuilder::new(ctx.storage.clone())
                .l0_manager(ctx.l0_manager.clone())
                .property_manager(ctx.property_manager.clone())
                .node_labels(&labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
                .edge_types(&[&edge_type])
                .build()
                .await?;

            // 2. Load Heuristic Property, through the same L0 view and
            //    property manager as the projection: without the view an
            //    unflushed heuristic value read as missing.
            let prop_manager = ctx.property_manager();
            let l0_view = ctx.l0_manager.as_ref().map(|m| m.pin_snapshot());
            let query_ctx = l0_view.as_ref().map(|v| v.query_context());

            let mut heuristic = HashMap::new();
            let vids: Vec<Vid> = projection.vertices().map(|(_, vid)| vid).collect();

            for chunk in vids.chunks(1000) {
                let props_map = prop_manager.get_batch_vertex_props(chunk, &[&heuristic_prop], query_ctx.as_ref()).await?;
                for (vid, props) in props_map {
                    if let Some(val) = props.get(&heuristic_prop)
                        && let Some(f) = val.as_f64() {
                            heuristic.insert(vid, f);
                        }
                }
            }

            let config = AStarConfig {
                source: start_vid,
                target: end_vid,
                heuristic,
            };

            let result = tokio::task::spawn_blocking(move || {
                AStar::run(&projection, config)
            }).await?;

            if let (Some(path), Some(cost)) = (result.path, result.distance) {
                let path_json: Vec<Value> = path.into_iter().map(|v| json!(v.as_u64())).collect();
                yield AlgoResultRow {
                    values: vec![Value::Array(path_json), json!(cost)],
                };
            }
        };

        Box::pin(stream)
    }
}
