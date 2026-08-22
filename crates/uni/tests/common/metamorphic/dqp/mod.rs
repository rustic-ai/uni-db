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
//! - [`index_lever`] — the same query with and without a scalar index on the
//!   column it filters. Tier-1, and the only lever restricted to a single case
//!   kind: index consultation needs an `=`/`IN` on the indexed column, which
//!   only `CaseKind::Pushdown` guarantees.
//! - [`seed`] — the tiered fixture builder.
//! - [`admissibility`], [`identity`], [`counters`], [`feasibility`],
//!   [`transition_probe`] — the preconditions. These are
//!   not scaffolding to be deleted: each pins a fact the oracle depends on
//!   (fork-boundary row identity, counter observability, fixture cost,
//!   which transitions are observable at all) that was an assumption until it
//!   was measured.
//!
//! # Both deferred levers now ship, each on a measurement
//!
//! The plan called for index-absent-vs-present and pre-vs-post-compaction levers,
//! and [`transition_probe`] measured that neither could be built: no counter in
//! `QueryMetrics` moved across either transition. A lever without an activation
//! witness is exactly the vacuous test this oracle exists to prevent, so the
//! honest move was to defer them until the observability existed.
//!
//! **The index lever is now built** ([`index_lever`]). #175 supplied its witness:
//! `QueryMetrics::index_scans` reports whether the *executed* plan consulted a
//! scalar index, sourced from Lance's execution-stats callback. Note the probe
//! that justified the deferral had itself been passing for the wrong reason — it
//! created a `BTree` index and probed it with a range predicate, a shape that
//! cannot reach pushdown at all, so it observed nothing and concluded nothing was
//! observable. It has been rebuilt around the shape that does reach it.
//!
//! **The compaction lever is now built too** ([`compaction_lever`]), and its
//! deferral expired in two stages worth keeping straight.
//!
//! #172 made `CompactionStats` report measured work, which made a real
//! compaction distinguishable from a no-op *at run level*. That alone did not
//! unblock the lever: `activated` takes two [`lever::Witness`]es, which are
//! per-*query*, so a run-level number has nothing to say about an individual
//! case. Run-level facts belong in `check_invariants`, and that is where
//! `CompactionStats` ended up — as a precondition rejecting a batch whose
//! transition merged nothing.
//!
//! What unblocked it was a per-query counter. `QueryMetrics::lance_iops` comes
//! from the `iops` field Lance's execution-stats callback had been delivering
//! and `attach_scan_stats` had been discarding. Measured before being relied
//! on: merging five fragments takes a full scan from 25 iops to 5, merging two
//! takes it from 10 to 5 — unanimous across shapes and repeats, and scaling
//! with how much was merged rather than merely reporting that something was.
//!
//! So all six levers ship. **No lever remains deferred.**
//!
//! See `docs/proposals/test_harness_implementation_plan_2026-08-12.md`.

pub mod admissibility;
pub mod cert;
pub mod compaction_lever;
pub mod counters;
pub mod driver;
pub mod feasibility;
pub mod flush_lever;
pub mod fork_lever;
pub mod identity;
pub mod index_lever;
pub mod lever;
pub mod pinned_lever;
pub mod plan_cache_lever;
pub mod seed;
pub mod stateful;
pub mod tier3_probe;
pub mod transition_probe;
pub mod vid_determinism;
