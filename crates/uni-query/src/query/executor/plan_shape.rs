// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Assertions over which **physical** operators a query actually ran.
//!
//! # Why this exists
//!
//! A coverage run on 2026-08-14 found `df_graph/vid_lookup_join.rs` at **0 of
//! 441 executable lines** — while a dedicated 15-test suite written for that
//! operator was passing. Those tests assert result *bags*, and the operator's
//! whole contract is to be bag-identical to the `HashJoinExec` it replaces. So
//! no bag assertion can ever distinguish "the optimization fired and is
//! correct" from "the optimization silently fell back". The operator sat behind
//! six silent `return Ok(None)` guards, and nothing turned red.
//!
//! That is a general hazard, not one operator's bad luck: an optimization with
//! a correctness-preserving fallback is *by construction* invisible to
//! result-only tests.
//!
//! # `EXPLAIN` cannot answer this — use `PROFILE`
//!
//! [`ExplainOutput::plan_text`](crate::query::planner::ExplainOutput) is
//! `format!("{:#?}", plan)` over a **`LogicalPlan`**
//! (`planner.rs::explain_logical_plan`), produced before physical planning. It
//! can never contain a physical operator name, so
//! `plan_text.contains("VidLookupJoinExec")` is false for every query — a
//! positive assertion fails for the wrong reason and a negative one passes
//! vacuously.
//!
//! The observable that answers the question is
//! [`ProfileOutput::runtime_stats`](super::core::ProfileOutput), whose
//! `operator` field is `plan.name()` collected while walking the executed
//! physical tree (`core.rs::collect_plan_metrics`).
//!
//! Note `PROFILE` **executes** the query, so a passing positive assertion both
//! proves emission and exercises the operator.

use super::core::ProfileOutput;

/// Physical operator names from a profiled query, in post-order.
#[must_use]
pub fn op_names(profile: &ProfileOutput) -> Vec<String> {
    profile
        .runtime_stats
        .iter()
        .map(|s| s.operator.clone())
        .collect()
}

/// Asserts `op` ran.
///
/// # Panics
///
/// Panics if no operator in `ops` equals `op`.
pub fn assert_uses(ops: &[String], op: &str, ctx: &str) {
    assert!(
        ops.iter().any(|o| o == op),
        "{ctx}: expected the plan to use `{op}`, but it did not.\n  \
         operators actually run: {ops:?}\n  \
         An optimization guarded by a silent fallback looks identical in the \
         result bag whether or not it fired — which is why this asserts the \
         operator and not the rows."
    );
}

/// Asserts `op` did **not** run — the negative twin of [`assert_uses`].
///
/// Mandatory for any operator that has a fallback. Without it, a planner change
/// that makes the guard always-false leaves the positive assertion as the only
/// witness, and a single failing test is easier to "fix" than to understand.
///
/// # Panics
///
/// Panics if any operator in `ops` equals `op`.
pub fn assert_avoids(ops: &[String], op: &str, ctx: &str) {
    assert!(
        !ops.iter().any(|o| o == op),
        "{ctx}: expected the plan to avoid `{op}`, but it ran.\n  \
         operators actually run: {ops:?}"
    );
}

/// Asserts at least one of `any_of` ran, for operators with several observable
/// names (e.g. `MutationExec` reports one of five `display_name`s).
///
/// # Panics
///
/// Panics if none of `any_of` appears in `ops`.
pub fn assert_uses_any(ops: &[String], any_of: &[&str], ctx: &str) {
    assert!(
        ops.iter().any(|o| any_of.contains(&o.as_str())),
        "{ctx}: expected the plan to use one of {any_of:?}, but it used none.\n  \
         operators actually run: {ops:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    /// Comparison must be **exact equality, never substring**.
    ///
    /// `GraphTraverseExec` is a prefix of `GraphTraverseMainExec`, and
    /// `GraphVariableLengthTraverseExec` of
    /// `GraphVariableLengthTraverseMainExec`. A `contains`-based check — the
    /// idiom this module replaces — would report the wrong operator as present
    /// and silently vouch for a path that never ran.
    #[test]
    fn matching_is_exact_not_substring() {
        let running = ops(&["GraphTraverseMainExec"]);
        assert_uses(&running, "GraphTraverseMainExec", "ctx");
        // The prefix did NOT run, and must not be reported as having run.
        assert_avoids(&running, "GraphTraverseExec", "ctx");
    }

    #[test]
    fn uses_any_accepts_one_of_several_display_names() {
        let running = ops(&["GraphScanExec", "MutationSetExec"]);
        assert_uses_any(&running, &["MutationSetExec", "MutationDeleteExec"], "ctx");
    }

    #[test]
    #[should_panic(expected = "expected the plan to use `VidLookupJoinExec`")]
    fn assert_uses_fails_when_the_operator_did_not_run() {
        assert_uses(&ops(&["HashJoinExec"]), "VidLookupJoinExec", "ctx");
    }

    #[test]
    #[should_panic(expected = "expected the plan to avoid `HashJoinExec`")]
    fn assert_avoids_fails_when_the_operator_ran() {
        assert_avoids(&ops(&["HashJoinExec"]), "HashJoinExec", "ctx");
    }
}
