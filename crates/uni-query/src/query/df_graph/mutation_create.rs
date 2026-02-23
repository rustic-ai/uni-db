// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion ExecutionPlan for Cypher CREATE clauses.
//!
//! Re-exports [`new_create_exec`] from `mutation_common` and provides a type
//! alias for backward compatibility.

pub use super::mutation_common::{MutationExec as MutationCreateExec, new_create_exec};
