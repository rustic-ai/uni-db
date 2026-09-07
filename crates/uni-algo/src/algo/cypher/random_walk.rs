// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.randomWalk procedure implementation.

use crate::algo::algorithms::{Algorithm, RandomWalk, RandomWalkConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, parse_vid_arg};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use uni_common::core::id::Vid;

pub struct RandomWalkAdapter;

impl GraphAlgoAdapter for RandomWalkAdapter {
    const NAME: &'static str = "uni.algo.randomWalk";
    type Algo = RandomWalk;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("walkLength", ValueType::Int, Some(json!(5))),
            ("walksPerNode", ValueType::Int, Some(json!(1))),
            ("startNodes", ValueType::List, Some(Value::Null)),
            // node2vec second-order bias: p (returnFactor) / q (inOutFactor).
            // Both 1.0 => unbiased first-order walk.
            ("returnFactor", ValueType::Float, Some(json!(1.0))),
            ("inOutFactor", ValueType::Float, Some(json!(1.0))),
            // Optional RNG seed; null => deterministic default seed.
            ("seed", ValueType::Int, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("path", ValueType::List)]
    }

    fn to_config(args: Vec<Value>) -> Result<RandomWalkConfig> {
        let walk_length = args[0].as_u64().unwrap_or(5) as usize;
        let walks_per_node = args[1].as_u64().unwrap_or(1) as usize;

        let start_nodes = if args[2].is_null() {
            Vec::new()
        } else {
            let list = args[2]
                .as_array()
                .ok_or_else(|| anyhow!("`startNodes` must be a list of node ids"))?;
            // A dropped bad entry used to shrink the list; an all-bad list became
            // empty, which downstream means "start from every node" — so
            // randomWalk(startNodes: ['abc']) walked the whole graph.
            list.iter()
                .enumerate()
                .map(|(i, val)| parse_vid_arg(val, &format!("startNodes[{i}]")))
                .collect::<Result<Vec<Vid>>>()?
        };

        let return_param = args.get(3).and_then(Value::as_f64).unwrap_or(1.0);
        let in_out_param = args.get(4).and_then(Value::as_f64).unwrap_or(1.0);
        let seed = args.get(5).and_then(Value::as_u64);

        Ok(RandomWalkConfig {
            walk_length,
            walks_per_node,
            start_nodes,
            return_param,
            in_out_param,
            seed,
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .walks
            .into_iter()
            .map(|walk| {
                let path: Vec<Value> = walk.iter().map(|v| json!(v.as_u64())).collect();
                AlgoResultRow {
                    values: vec![Value::Array(path)],
                }
            })
            .collect())
    }
}

pub type RandomWalkProcedure = GenericAlgoProcedure<RandomWalkAdapter>;

#[cfg(test)]
mod tests {
    use super::*;

    fn args(start_nodes: Value) -> Vec<Value> {
        vec![
            json!(5),
            json!(1),
            start_nodes,
            json!(1.0),
            json!(1.0),
            Value::Null,
        ]
    }

    #[test]
    fn bad_start_node_errors_instead_of_walking_the_whole_graph() {
        // A2: a dropped entry left an empty start list, which downstream means
        // "start from every node" — so startNodes:['abc'] walked the whole graph.
        let cfg = RandomWalkAdapter::to_config(args(json!([3, "5"])))
            .expect("valid start nodes must parse");
        assert_eq!(cfg.start_nodes, vec![Vid::from(3u64), Vid::from(5u64)]);

        let err = RandomWalkAdapter::to_config(args(json!(["abc"])))
            .expect_err("an unparsable startNodes entry must error");
        assert!(
            err.to_string().contains("startNodes[0]"),
            "error must name the offending entry: {err}"
        );

        // A single bad entry among good ones must not be silently dropped either.
        let err = RandomWalkAdapter::to_config(args(json!([3, "abc"])))
            .expect_err("one bad entry must error");
        assert!(
            err.to_string().contains("startNodes[1]"),
            "error must name the offending index: {err}"
        );

        // null still means "no explicit start nodes".
        let cfg = RandomWalkAdapter::to_config(args(Value::Null)).expect("null is allowed");
        assert!(cfg.start_nodes.is_empty());
    }
}
