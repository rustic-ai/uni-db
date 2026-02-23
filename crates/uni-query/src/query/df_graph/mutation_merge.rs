// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher MERGE clauses.
//!
//! Re-exports [`new_merge_exec`] from `mutation_common` and provides a type
//! alias for backward compatibility.

pub use super::mutation_common::{MutationExec as MutationMergeExec, new_merge_exec};
