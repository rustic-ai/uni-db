// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session-level wrappers for physical-operator assertions.
//!
//! The comparison core lives in `uni_query::plan_shape` so both this crate's
//! tests and `uni-query`'s can use it — `uni-query` cannot dev-depend on `uni`
//! (that is a dependency cycle), but `uni` depends on `uni-query`, so the
//! string logic goes in the lower crate and each side keeps a thin wrapper that
//! knows how to obtain a `ProfileOutput`.
//!
//! # Retrofit recipe
//!
//! The work-list is `docs/testing/silent-downgrades-2026-08-15.md`, which
//! catalogues the 29 planner sites where an optimization falls back to a
//! result-identical path. Note it also records what this module **cannot** reach:
//! five of those sites are logical-plan rewrites that never produce a distinct
//! physical operator name, so [`assert_plan_uses`] has nothing to match on.
//!
//! 1. Find a query that should emit `FooExec`, and read the guard conditions at
//!    its construction site in `df_planner.rs` — the fixture shape is usually
//!    load-bearing (a bare-variable projection, a `WHERE` on the probe side, or
//!    running inside a transaction can each silently defeat an optimization).
//! 2. Write the test **next to the feature it belongs to**, not here.
//! 3. Call [`assert_plan_uses`].
//! 4. **If the operator is an optimization with a fallback, the negative twin is
//!    mandatory**: a second query outside the guard conditions, asserting the
//!    operator is absent *and* the fallback present. Template:
//!    `sparse_scoring.rs:277/296`.
//! 5. Keep the existing result assertions. `assert_plan_uses` proves it ran;
//!    the bag proves it ran *correctly*. Neither substitutes for the other.
//! 6. Flip the row in `plan_shape/registry.rs` to `Proven` in the same change —
//!    the gate fails until you do.
//!
//! Note `profile()` **executes** the query, so these run real work; for
//! mutations use a throwaway `Uni::in_memory()`.

pub mod gate;
pub mod registry;

use uni_db::Session;
use uni_query::plan_shape;

/// Physical operator names from profiling `query`.
///
/// # Panics
///
/// Panics if the query fails to execute — `profile()` runs it.
pub async fn plan_ops(session: &Session, query: &str) -> Vec<String> {
    let (_result, profile) = session
        .query_with(query)
        .profile()
        .await
        .unwrap_or_else(|e| panic!("profile failed for `{query}`: {e}"));
    plan_shape::op_names(&profile)
}

/// Asserts `query` runs physical operator `op`.
///
/// # Panics
///
/// Panics if the query fails, or if `op` is absent from the executed plan.
pub async fn assert_plan_uses(session: &Session, query: &str, op: &str) {
    let ops = plan_ops(session, query).await;
    plan_shape::assert_uses(&ops, op, query);
}

/// Asserts `query` does **not** run physical operator `op`.
///
/// # Panics
///
/// Panics if the query fails, or if `op` is present in the executed plan.
pub async fn assert_plan_avoids(session: &Session, query: &str, op: &str) {
    let ops = plan_ops(session, query).await;
    plan_shape::assert_avoids(&ops, op, query);
}
