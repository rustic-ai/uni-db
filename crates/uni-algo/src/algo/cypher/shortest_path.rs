// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.shortestPath procedure implementation.

use crate::algo::DirectTraversal;
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::{Result, anyhow};
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::{Value, json};
use uni_common::core::id::Vid;
use uni_store::storage::direction::Direction;

pub struct ShortestPathProcedure;

impl AlgoProcedure for ShortestPathProcedure {
    fn name(&self) -> &str {
        "uni.algo.shortestPath"
    }

    fn signature(&self) -> ProcedureSignature {
        ProcedureSignature {
            args: vec![
                ("sourceNode", ValueType::Node),
                ("targetNode", ValueType::Node),
                ("relationshipTypes", ValueType::List),
            ],
            optional_args: Vec::new(),
            yields: vec![
                ("nodeIds", ValueType::List),
                ("edgeIds", ValueType::List),
                ("length", ValueType::Int),
            ],
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

        let source_vid = match vid_from_value(&args[0]) {
            Ok(v) => v,
            Err(e) => return stream::once(async move { Err(e) }).boxed(),
        };
        let target_vid = match vid_from_value(&args[1]) {
            Ok(v) => v,
            Err(e) => return stream::once(async move { Err(e) }).boxed(),
        };
        let edge_types_str: Vec<String> = args[2]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        // Use stream::once with an async block for the single result
        let result_stream = async move {
            // 1. Resolve edge types and warm adjacency
            let schema = ctx.storage.schema_manager().schema();
            let mut edge_type_ids = Vec::new();

            for type_name in &edge_types_str {
                let meta = schema
                    .edge_types
                    .get(type_name)
                    .ok_or_else(|| anyhow!("Edge type {} not found", type_name))?;
                edge_type_ids.push(meta.id);

                let edge_ver = ctx.storage.get_edge_version_by_id(meta.id);

                // Warm Outgoing
                ctx.storage
                    .warm_adjacency(meta.id, Direction::Outgoing, edge_ver)
                    .await?;

                // Warm Incoming
                ctx.storage
                    .warm_adjacency(meta.id, Direction::Incoming, edge_ver)
                    .await?;
            }

            let am = ctx.storage.adjacency_manager();
            let traversal = DirectTraversal::new(&am, edge_type_ids);

            if let Some(path) = traversal.shortest_path(source_vid, target_vid, Direction::Outgoing)
            {
                Ok(Some(AlgoResultRow {
                    values: vec![
                        json!(path.vertices.iter().map(|v| v.as_u64()).collect::<Vec<_>>()),
                        json!(path.edges.iter().map(|e| e.as_u64()).collect::<Vec<_>>()),
                        json!(path.len()),
                    ],
                }))
            } else {
                Ok(None)
            }
        };

        // Convert the async block to a stream, filtering out None results
        stream::once(result_stream)
            .filter_map(|res: Result<Option<AlgoResultRow>>| async move {
                match res {
                    Ok(Some(row)) => Some(Ok(row)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .boxed()
    }
}

fn vid_from_value(val: &Value) -> Result<Vid> {
    // In the new storage model, VIDs are pure auto-increment integers
    if let Some(s) = val.as_str() {
        // Try parsing as simple integer first
        if let Ok(id) = s.parse::<u64>() {
            return Ok(Vid::new(id));
        }
        // Legacy format "label:offset" - parse and combine
        let parts: Vec<_> = s.split(':').collect();
        if parts.len() == 2
            && let (Ok(l), Ok(o)) = (parts[0].parse::<u16>(), parts[1].parse::<u64>())
        {
            // Legacy: combine label and offset for backward compat
            return Ok(Vid::new((l as u64) << 48 | o));
        }
    }
    if let Some(v) = val.as_u64() {
        return Ok(Vid::from(v));
    }
    Err(anyhow!("Invalid Vid format: {:?}", val))
}
