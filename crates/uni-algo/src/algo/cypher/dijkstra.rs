// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.algo.dijkstra` procedure implementation.
//!
//! Weighted single-source shortest paths over a [`GraphProjection`].
//! Distinct from [`super::ShortestPathProcedure`] (`uni.algo.shortestPath`)
//! which performs unweighted BFS; this procedure honors edge weights via
//! the configurable `weightProperty` projection knob.

use crate::algo::algorithms::{Algorithm, Dijkstra, DijkstraConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, parse_vid_arg};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};

pub struct DijkstraAdapter;

impl GraphAlgoAdapter for DijkstraAdapter {
    const NAME: &'static str = "uni.algo.dijkstra";
    type Algo = Dijkstra;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("startNode", ValueType::Node, None),
            ("endNode", ValueType::Node, Some(Value::Null)),
            ("weightProperty", ValueType::String, Some(Value::Null)),
            ("maxDistance", ValueType::Float, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        // One row per result. When `endNode` is supplied and reached, a
        // single row is emitted carrying the path; otherwise one row per
        // reachable node with an empty `path` is emitted (SSSP mode).
        vec![
            ("nodeId", ValueType::Int),
            ("distance", ValueType::Float),
            ("path", ValueType::List),
        ]
    }

    fn to_config(args: Vec<Value>) -> Result<DijkstraConfig> {
        let target = if args[1].is_null() {
            None
        } else {
            Some(parse_vid_arg(&args[1], "endNode")?)
        };
        let max_distance = args[3].as_f64();
        Ok(DijkstraConfig {
            source: parse_vid_arg(&args[0], "startNode")?,
            target,
            max_distance,
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        // `Dijkstra::run` now returns a `Result`: propagate weight-validation
        // errors (e.g. a negative edge) into the adapter's `anyhow::Result`.
        let result = result.map_err(|e| anyhow!(e))?;
        if let Some(path) = result.path {
            let target = match path.last() {
                Some(v) => *v,
                None => return Ok(Vec::new()),
            };
            let distance = result
                .distances
                .iter()
                .find_map(|(vid, d)| (*vid == target).then_some(*d))
                .unwrap_or(0.0);
            let path_json: Vec<Value> = path.into_iter().map(|v| json!(v.as_u64())).collect();
            return Ok(vec![AlgoResultRow {
                values: vec![
                    json!(target.as_u64()),
                    json!(distance),
                    Value::Array(path_json),
                ],
            }]);
        }
        let rows = result
            .distances
            .into_iter()
            .map(|(vid, d)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(d), Value::Array(Vec::new())],
            })
            .collect();
        Ok(rows)
    }

    fn include_reverse() -> bool {
        false
    }

    fn weight_arg_index() -> Option<usize> {
        Some(2)
    }
}

pub type DijkstraProcedure = GenericAlgoProcedure<DijkstraAdapter>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::algorithms::DijkstraResult;
    use uni_common::core::id::Vid;

    #[test]
    fn map_result_emits_path_row_when_target_reached() {
        let result = DijkstraResult {
            distances: vec![
                (Vid::from(0), 0.0),
                (Vid::from(1), 3.5),
                (Vid::from(2), 5.0),
            ],
            path: Some(vec![Vid::from(0), Vid::from(1), Vid::from(2)]),
        };
        let rows = DijkstraAdapter::map_result(Ok(result)).expect("map_result must succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], json!(2_u64));
        assert_eq!(rows[0].values[1], json!(5.0));
        assert_eq!(
            rows[0].values[2],
            Value::Array(vec![json!(0_u64), json!(1_u64), json!(2_u64)])
        );
    }

    #[test]
    fn map_result_emits_one_row_per_reachable_node_in_sssp_mode() {
        let result = DijkstraResult {
            distances: vec![
                (Vid::from(0), 0.0),
                (Vid::from(1), 1.0),
                (Vid::from(2), 2.0),
            ],
            path: None,
        };
        let rows = DijkstraAdapter::map_result(Ok(result)).expect("map_result must succeed");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert!(matches!(&row.values[2], Value::Array(a) if a.is_empty()));
        }
    }

    #[test]
    fn to_config_treats_null_endnode_as_sssp() {
        let cfg =
            DijkstraAdapter::to_config(vec![json!(7_u64), Value::Null, Value::Null, Value::Null])
                .expect("config must build");
        assert_eq!(cfg.source.as_u64(), 7);
        assert!(cfg.target.is_none());
        assert!(cfg.max_distance.is_none());
    }
}
