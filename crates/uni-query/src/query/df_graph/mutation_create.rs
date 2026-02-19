// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher CREATE clauses.
//!
//! Thin wrapper around [`MutationExec`] with a typed constructor that builds
//! the correct [`MutationKind::Create`] variant.

use super::mutation_common::{MutationContext, MutationExec, MutationKind};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uni_cypher::ast::Pattern;

/// Type alias for a CREATE mutation execution plan.
pub type MutationCreateExec = MutationExec;

/// Create a new `MutationExec` configured for a CREATE clause.
pub fn new_create_exec(
    input: Arc<dyn ExecutionPlan>,
    pattern: Pattern,
    mutation_ctx: Arc<MutationContext>,
) -> MutationCreateExec {
    MutationExec::new(
        input,
        MutationKind::Create { pattern },
        "MutationCreateExec",
        mutation_ctx,
    )
}
