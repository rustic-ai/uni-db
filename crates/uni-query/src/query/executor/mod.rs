// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Query execution engine.
//!
//! Translates logical plans into concrete read/write operations against
//! Uni storage, including DataFusion-based columnar execution, procedure
//! dispatch, and result normalization.

pub mod core;
pub mod ddl_procedures;
pub mod path_builder;
pub mod plan_shape;
pub mod plugin_adapter;
pub mod procedure;
pub mod procedure_host;
pub mod read;
pub mod result_normalizer;
pub mod write;

pub use self::core::Executor;
// `custom_functions` was extracted to `uni-query-functions` (leaf module).
// Re-export for back-compat so existing `uni_query::query::executor::*`
// paths keep working.
pub use path_builder::PathBuilder;
pub use result_normalizer::ResultNormalizer;
pub use uni_query_functions::custom_functions::{CustomFunctionRegistry, CustomScalarFn};
