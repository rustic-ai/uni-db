// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod algo;

pub use algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
pub use algo::projection::{GraphProjection, ProjectionBuilder, ProjectionConfig};
pub use algo::{AlgorithmConfig, AlgorithmRegistry};
