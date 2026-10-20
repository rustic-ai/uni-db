// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Leaf modules extracted from `uni-query` to isolate them from the
//! planner/executor strongly-connected component.
//!
//! Editing any module here recompiles only this crate, not the 79k-LOC
//! `uni-query` SCC downstream. `uni-query` re-exports everything here so
//! downstream callers keep using `uni_query::query::<name>::*`.

pub mod custom_functions;
pub use uni_common::cypher_sum;
pub mod cypher_type_coerce;
pub mod datetime;
pub mod df_expr;
pub mod df_udfs;
pub mod expr_eval;
pub mod function_props;
pub mod fusion;
pub mod java_regex;
pub mod percentile;
pub mod pushdown;
pub mod rewrite;
pub mod similar_to;
pub mod spatial;
