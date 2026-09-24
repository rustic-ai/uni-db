// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Error types for the Locy native execution engine.

use std::fmt;

/// The phrase a [`LocyRuntimeError::MemoryLimitExceeded`] renders with.
///
/// The fixpoint raises that error as a `DataFusionError::Execution` string, so
/// it reaches the API layer as text; this is what the API layer matches to
/// classify it as a memory refusal rather than a failed program. It is
/// deliberately distinct from Cypher's "Query exceeded memory limit", which is
/// a different budget (`max_query_memory`, not `max_derived_bytes`).
pub const DERIVED_BYTES_LIMIT_MARKER: &str = "exceeded max_derived_bytes";

/// Runtime errors specific to Locy evaluation.
#[derive(Debug)]
pub enum LocyRuntimeError {
    /// Fixpoint iteration did not converge within the allowed limit.
    NonConvergence { iterations: usize },
    /// A monotonic aggregate detected a decrease in value.
    MonotonicViolation { rule: String, column: String },
    /// Stratification detected a cycle through negation.
    NegationCycle { rules: Vec<String> },
    /// A derived relation exceeded its memory budget.
    MemoryLimitExceeded {
        rule: String,
        bytes: usize,
        limit: usize,
    },
}

impl fmt::Display for LocyRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonConvergence { iterations } => {
                write!(
                    f,
                    "fixpoint did not converge: max iterations ({iterations}) exceeded"
                )
            }
            Self::MonotonicViolation { rule, column } => {
                write!(f, "monotonic violation in rule '{rule}', column '{column}'")
            }
            Self::NegationCycle { rules } => {
                write!(
                    f,
                    "negation cycle detected among rules: {}",
                    rules.join(", ")
                )
            }
            Self::MemoryLimitExceeded { rule, bytes, limit } => {
                write!(
                    f,
                    "rule '{rule}' {DERIVED_BYTES_LIMIT_MARKER} memory limit ({bytes} bytes > {limit} byte limit)"
                )
            }
        }
    }
}

impl std::error::Error for LocyRuntimeError {}
