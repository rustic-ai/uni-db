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
//! # Layout
//!
//! - [`lever`] — the `Lever` trait, and the `Witness` that proves a comparison
//!   was not vacuous.
//! - [`driver`] — `drive_prepared`, the Tier-2 driver, plus the activation-rate
//!   floor and row budget every run is held to.
//! - [`fork_lever`] — lever 1, primary vs a pristine fork.
//! - [`pinned_lever`] — lever 2, a live read vs a pinned time-travel read.
//! - [`seed`] — the tiered fixture builder.
//! - [`admissibility`], [`identity`], [`counters`], [`feasibility`] — the
//!   preconditions. These are
//!   not scaffolding to be deleted: each pins a fact the oracle depends on
//!   (fork-boundary row identity, counter observability, fixture cost) that was
//!   an assumption until it was measured.
//!
//! See `docs/proposals/test_harness_implementation_plan_2026-08-12.md`.

pub mod admissibility;
pub mod counters;
pub mod driver;
pub mod feasibility;
pub mod fork_lever;
pub mod identity;
pub mod lever;
pub mod pinned_lever;
pub mod seed;
