// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher SET clauses.
//!
//! Thin wrapper around [`MutationExec`] with a typed constructor that builds
//! the correct [`MutationKind::Set`] variant.

use super::mutation_common::{MutationContext, MutationExec, MutationKind};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uni_cypher::ast::SetItem;

/// Type alias for a SET mutation execution plan.
pub type MutationSetExec = MutationExec;

/// Create a new `MutationExec` configured for a SET clause.
pub fn new_set_exec(
    input: Arc<dyn ExecutionPlan>,
    items: Vec<SetItem>,
    mutation_ctx: Arc<MutationContext>,
) -> MutationSetExec {
    MutationExec::new(
        input,
        MutationKind::Set { items },
        "MutationSetExec",
        mutation_ctx,
    )
}
