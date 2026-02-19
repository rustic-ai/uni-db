// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher REMOVE clauses.
//!
//! Thin wrapper around [`MutationExec`] with a typed constructor that builds
//! the correct [`MutationKind::Remove`] variant.

use super::mutation_common::{MutationContext, MutationExec, MutationKind};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uni_cypher::ast::RemoveItem;

/// Type alias for a REMOVE mutation execution plan.
pub type MutationRemoveExec = MutationExec;

/// Create a new `MutationExec` configured for a REMOVE clause.
pub fn new_remove_exec(
    input: Arc<dyn ExecutionPlan>,
    items: Vec<RemoveItem>,
    mutation_ctx: Arc<MutationContext>,
) -> MutationRemoveExec {
    MutationExec::new(
        input,
        MutationKind::Remove { items },
        "MutationRemoveExec",
        mutation_ctx,
    )
}
