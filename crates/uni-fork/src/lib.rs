// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork diff/promote engine and background maintenance for uni-db.
//!
//! This crate holds the reusable *logic* of the uni-db fork subsystem:
//!
//! - [`types`] — the public diff/promote value types (`ForkDiff`,
//!   `PromotePattern`, `PromoteReport`, …).
//! - [`diff`] — [`compute_diff`] and [`run_promote`], generic over the
//!   [`host`] traits.
//! - [`host`] — `ForkQueryHost` / `ForkPromoteSink`, implemented by uni-db for
//!   its `Session`/`Transaction` types.
//! - [`maintenance`] — the TTL sweeper and fork-local index-builder task
//!   skeletons, generic over `maintenance::ForkMaintenanceHost`.
//!
//! The fork *drivers* that construct `Session`/`UniInner` (`fork.rs`,
//! `fork_schema.rs`, the `Uni::*` fork orchestration) stay in uni-db to avoid a
//! dependency cycle; they delegate to this crate's engine.

pub mod diff;
pub mod host;
pub mod maintenance;
pub mod types;

pub use diff::{SharedLineage, compute_diff, compute_diff_with_lineage, run_promote};
pub use host::{ForkPromoteSink, ForkQueryHost};
pub use maintenance::{ForkMaintenanceHost, spawn_index_builder, spawn_sweeper};
pub use types::{
    ConflictPolicy, DiffEdge, DiffVertex, EdgeDiff, EdgePropertyChange, ForkDiff, PromoteBaseline,
    PromoteOptions, PromotePattern, PromoteReport, PropertyChange, VertexDiff,
    VertexPropertyChange,
};
