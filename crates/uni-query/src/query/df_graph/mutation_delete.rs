// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher DELETE and DETACH DELETE clauses.
//!
//! Thin wrapper around [`MutationExec`] with a typed constructor that builds
//! the correct [`MutationKind::Delete`] variant.

use super::mutation_common::{MutationContext, MutationExec, MutationKind};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uni_cypher::ast::Expr;

/// Type alias for a DELETE mutation execution plan.
pub type MutationDeleteExec = MutationExec;

/// Create a new `MutationExec` configured for a DELETE clause.
pub fn new_delete_exec(
    input: Arc<dyn ExecutionPlan>,
    items: Vec<Expr>,
    detach: bool,
    mutation_ctx: Arc<MutationContext>,
) -> MutationDeleteExec {
    MutationExec::new(
        input,
        MutationKind::Delete { items, detach },
        "MutationDeleteExec",
        mutation_ctx,
    )
}
