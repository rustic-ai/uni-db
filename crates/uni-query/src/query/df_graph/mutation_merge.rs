// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher MERGE clauses.
//!
//! Thin wrapper around [`MutationExec`] with a typed constructor that builds
//! the correct [`MutationKind::Merge`] variant.

use super::mutation_common::{MutationContext, MutationExec, MutationKind};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uni_cypher::ast::{Pattern, SetClause};

/// Type alias for a MERGE mutation execution plan.
pub type MutationMergeExec = MutationExec;

/// Create a new `MutationExec` configured for a MERGE clause.
pub fn new_merge_exec(
    input: Arc<dyn ExecutionPlan>,
    pattern: Pattern,
    on_match: Option<SetClause>,
    on_create: Option<SetClause>,
    mutation_ctx: Arc<MutationContext>,
) -> MutationMergeExec {
    MutationExec::new(
        input,
        MutationKind::Merge {
            pattern,
            on_match,
            on_create,
        },
        "MutationMergeExec",
        mutation_ctx,
    )
}
