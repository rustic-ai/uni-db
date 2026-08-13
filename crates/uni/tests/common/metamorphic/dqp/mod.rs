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
//! - [`stateful`] — `drive_stateful`, the Tier-1 driver, whose loop inverts so a
//!   global irreversible transition is applied once per *batch* rather than once
//!   per case. Plus `replay_stateful`, which reproduces a failure from its seed
//!   because proptest's shrinker cannot survive the transition.
//! - [`fork_lever`] — lever 1, primary vs a pristine fork (Tier 2).
//! - [`pinned_lever`] — lever 2, a live read vs a pinned time-travel read (Tier 2).
//! - [`flush_lever`] — lever 3, a mixed L0+L1 read vs the same data flushed
//!   (Tier 1).
//! - [`plan_cache_lever`] — lever 4, a fresh plan vs a cached one. Listed as
//!   Tier-1 in the plan; measured to be Tier-2, because the plan cache belongs to
//!   the `Session` rather than the database.
//! - [`seed`] — the tiered fixture builder.
//! - [`admissibility`], [`identity`], [`counters`], [`feasibility`],
//!   [`transition_probe`] — the preconditions. These are
//!   not scaffolding to be deleted: each pins a fact the oracle depends on
//!   (fork-boundary row identity, counter observability, fixture cost,
//!   which transitions are observable at all) that was an assumption until it
//!   was measured.
//!
//! # Two levers are deferred, with the measurement as the reason
//!
//! The plan called for index-absent-vs-present and pre-vs-post-compaction levers.
//! [`transition_probe`] measured both and neither can be built today: no counter
//! anywhere in `QueryMetrics` moves across either transition, the logical plan is
//! byte-identical before and after an index is created, and `CompactionStats`
//! returns hardcoded literals for every field but `duration`. A lever without an
//! activation witness is exactly the vacuous test this oracle exists to prevent,
//! so the honest move is to defer them until the observability exists. The probes
//! stay as the standing evidence, and fail loudly if that changes.
//!
//! See `docs/proposals/test_harness_implementation_plan_2026-08-12.md`.

pub mod admissibility;
pub mod counters;
pub mod driver;
pub mod feasibility;
pub mod flush_lever;
pub mod fork_lever;
pub mod identity;
pub mod lever;
pub mod pinned_lever;
pub mod plan_cache_lever;
pub mod seed;
pub mod stateful;
pub mod transition_probe;
