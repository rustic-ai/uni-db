//! DQP — Differential Query Plans oracle (Phase 0: feasibility measurement).
//!
//! DQP is the third SQLancer oracle, and the one this repo lacks. TLP and NoREC
//! are *self*-differential: they compare a query against a semantically
//! equivalent rewrite of itself, executed under the **same** configuration. If
//! the storage path is wrong, both sides are wrong identically and the oracle
//! passes. DQP instead holds the query fixed and varies the **execution path**:
//!
//! ```text
//! bag(Q under config A) == bag(Q under config B)
//! ```
//!
//! Any inequality is a bug in exactly one of the two paths.
//!
//! # What is here today
//!
//! **Phase 0 only.** This module currently contains no oracle, no `Lever` trait
//! and no driver — only [`seed`], a tiered fixture builder, and [`feasibility`],
//! a measurement suite whose output sets the parameters the oracle will be built
//! against. Building the oracle before measuring is how the two preceding
//! revisions of the proposal ended up committing to fixture sizes and activation
//! witnesses that do not survive contact with the source.
//!
//! See `docs/proposals/test_harness_implementation_plan_2026-08-12.md`.

pub mod counters;
pub mod feasibility;
pub mod seed;
