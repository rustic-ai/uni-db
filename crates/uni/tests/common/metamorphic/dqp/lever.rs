//! The `Lever` abstraction: one query, two execution paths, one comparison.
//!
//! A lever names a pair of *result-neutral* execution configurations — primary
//! and a pristine fork, flushed and unflushed, cold and warm plan cache — and
//! knows how to run a query down each. The oracle's claim is that for any query
//! `Q`, `bag(Q under A) == bag(Q under B)`, and any inequality is a bug in
//! exactly one of the two paths.
//!
//! # Why `activated` has no default body
//!
//! A differential oracle whose two sides happen to exercise the *same* machinery
//! is vacuously green: it compares a path against itself, forever, and reports
//! success. That failure mode is silent by construction — nothing in the output
//! distinguishes "the paths agree" from "there was only ever one path".
//!
//! So every lever must supply an **activation witness**: an observable predicate
//! asserting that side A and side B genuinely diverged. `activated` is therefore
//! a required method. A lever author who has not thought about what proves their
//! two sides differ cannot accidentally ship one that never checks.
//!
//! The repo has a live example of the failure this prevents:
//! `fork_index_recall_bench.rs` reports recall@10 = 1.000 because at n = 1000
//! Lance brute-forces and the index under test never runs.
//!
//! # Why the witness is counters and not plan text
//!
//! Rev 1 of the proposal required that the two sides produce *different logical
//! plans*. That rule is invalid: plan-cache cold-vs-warm is identical by
//! construction, and L0-vs-L1 and pre-vs-post-compaction can plan identically
//! while reading entirely different machinery. A universal plan-difference check
//! would reject precisely the levers most worth testing.
//!
//! The witness is instead a snapshot of the per-query execution counters added in
//! Phase 1, which observe **what executed** rather than what was configured.

use uni_cypher::ast::Query;
use uni_db::Session;
use uni_query::QueryResult;

use crate::diff::{RowBag, bag};
use crate::querygen::render::render;

/// What one side of a comparison actually did, beyond producing rows.
///
/// Every field is a straight copy from `QueryResult::metrics()`. Phase 1 exists
/// so that this struct needs no logic: if a counter here is wrong, the bug is in
/// the instrumentation, not the oracle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Witness {
    /// Rows served from an L0 buffer (in-memory, unflushed).
    pub l0_reads: usize,
    /// Rows served from L1 / Lance storage.
    pub storage_reads: usize,
    /// Rows examined by scans, before filtering and projection.
    pub rows_scanned: usize,
    /// Scans that executed against a fork's Lance branch.
    pub branch_scans: u64,
    /// Reads that executed against a pinned time-travel snapshot.
    pub snapshot_reads: u64,
    /// Plan-cache hits attributable to this query.
    ///
    /// **Not** from `QueryMetrics`. `QueryResult::plan_cache_hit` is set at
    /// exactly one site — `impl_query.rs:808`, on the *write* path — so it is
    /// permanently `false` for a read and cannot witness anything; a test in
    /// `feasibility.rs` pins that. The observable that does work is a delta on
    /// `SessionMetrics::plan_cache_hits`, which only [`observe_cached`] can take
    /// because it needs the session on both sides of the call.
    ///
    /// So this field is `0` under plain [`observe`], and non-zero only where a
    /// lever has deliberately measured it. A lever must not read it unless it
    /// produced its own sides with [`observe_cached`].
    pub plan_cache_hits: u64,
    /// Lance scans whose executed plan reported scalar-index work.
    ///
    /// Carried so an index lever can witness on it. Deliberately **not** folded
    /// into any existing lever's `activated`: index consultation is
    /// data-dependent per generated query — only an `=`/`IN` on an indexed
    /// column reaches the pushdown path — so adding it to the fork, pinned,
    /// flush or plan-cache predicates would drop them from 100% activation to
    /// whatever fraction of generated cases carries such a predicate, and fail
    /// the run's activation floor for a reason unrelated to the lever.
    pub index_scans: u64,
}

/// One side's result: the bag to compare, and the evidence of how it was
/// produced.
#[derive(Debug)]
pub struct Observed {
    /// The multiset of rows, for `bag_eq`.
    pub bag: RowBag,
    /// What machinery this side exercised.
    pub witness: Witness,
}

/// Snapshots the execution counters travelling with a result.
pub fn witness_of(r: &QueryResult) -> Witness {
    let m = r.metrics();
    Witness {
        l0_reads: m.l0_reads,
        storage_reads: m.storage_reads,
        rows_scanned: m.rows_scanned,
        branch_scans: m.branch_scans,
        snapshot_reads: m.snapshot_reads,
        plan_cache_hits: 0,
        index_scans: m.index_scans,
    }
}

/// Runs `q` on `session`, returning both the bag and the witness.
///
/// The session is passed in rather than a `Uni` because several levers depend on
/// *which* session runs the query — a fork session, a pinned session, or one
/// whose plan cache has been warmed. `metamorphic::run_bag` opens a fresh
/// session per call, which is correct for TLP/NoREC and wrong here.
///
/// # Errors
///
/// Returns any error from query execution.
pub async fn observe(session: &Session, q: &Query) -> anyhow::Result<Observed> {
    let cypher = render(q);
    let result = session.query(&cypher).await?;
    Ok(Observed {
        witness: witness_of(&result),
        bag: bag(&result),
    })
}

/// [`observe`], additionally measuring the query's plan-cache hits.
///
/// Takes a `SessionMetrics` snapshot either side of the query and records the
/// difference, which is the only way to attribute a cache hit to one execution:
/// `SessionMetrics` counters are cumulative over the session's life, and the
/// per-result flag is write-path-only.
///
/// Costs two extra metric reads per call, so it is a separate function rather
/// than folded into [`observe`] — only the plan-cache lever needs it, and every
/// other lever runs 20 000 cases.
///
/// # Errors
///
/// Returns any error from query execution.
pub async fn observe_cached(session: &Session, q: &Query) -> anyhow::Result<Observed> {
    let cypher = render(q);
    let before = session.metrics().plan_cache_hits;
    let result = session.query(&cypher).await?;
    let after = session.metrics().plan_cache_hits;
    let mut witness = witness_of(&result);
    witness.plan_cache_hits = after.saturating_sub(before);
    Ok(Observed {
        witness,
        bag: bag(&result),
    })
}

/// A pair of result-neutral execution paths, plus proof they differ.
///
/// Deliberately **not** dyn-compatible: native `async fn` in traits is stable at
/// this workspace's MSRV but cannot be used behind `dyn`. That costs nothing
/// here — each lever gets its own test function, exactly as the TLP oracles do,
/// so the driver is generic over `L: Lever` and never needs `Box<dyn Lever>`.
/// The alternative, `#[async_trait]`, would mean adding `async-trait` to
/// `crates/uni` `[dev-dependencies]`; it is currently a normal dependency only,
/// so integration tests cannot see it.
pub trait Lever {
    /// Short name, used in failure messages and the activation report.
    fn name(&self) -> &'static str;

    /// The reference execution path.
    async fn side_a(&self, q: &Query) -> anyhow::Result<Observed>;

    /// The path under test.
    async fn side_b(&self, q: &Query) -> anyhow::Result<Observed>;

    /// Did the two sides genuinely exercise different machinery?
    ///
    /// **No default body, on purpose.** See the module docs: a lever that cannot
    /// answer this is indistinguishable from one that compares a path against
    /// itself.
    fn activated(&self, a: &Witness, b: &Witness) -> bool;

    /// Anything else that must hold for this lever's comparisons to mean
    /// something. Called before and after the case loop.
    ///
    /// The pinned lever uses it to confirm no write advanced the live version
    /// mid-run: if one did, the two sides legitimately saw different data and
    /// the run must be **rejected rather than reported** — a passing comparison
    /// would be luck and a failing one would be a false alarm.
    ///
    /// A default body is right here, unlike [`Self::activated`], which
    /// deliberately has none. Having no extra precondition is a legitimate and
    /// common case, and the default means "nothing further to check" rather than
    /// "skip the thing that makes this valid" — the activation witness still
    /// governs vacuity either way.
    ///
    /// # Errors
    ///
    /// Returns an error describing the violated invariant.
    async fn check_invariants(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
